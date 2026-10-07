package leaderforward

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/osvaldoandrade/codeq/pkg/domain"
)

const stateContextKey = "codeq.leaderforward"

// requestState is the per-request forwarding context stored in gin.
type requestState struct {
	fwd         *Forwarder
	body        []byte
	overflow    bool
	waitSeconds int
}

// routeKind selects the pre-dispatch leadership rule of a route.
type routeKind int

const (
	// kindSingle routes write one task (or one topic). They forward up front
	// only when one peer leads every group; otherwise they run locally and
	// the not-leader error of the exact group they touched is forwarded.
	kindSingle routeKind = iota
	// kindGated routes (claims and batches) are decided before any item:
	// local when this node leads at least one group, forwarded when one
	// peer leads every group, and 503 otherwise.
	kindGated
)

type decision int

const (
	decideLocal decision = iota
	decideForward
	decideUnavailable
)

// view is a snapshot of this node's write leadership across its groups.
type view struct {
	groups  int
	leads   int
	target  string
	unified bool
}

// Single gates a single-task or single-topic write route.
func (f *Forwarder) Single() gin.HandlerFunc { return f.middleware(kindSingle, false) }

// Batch gates a batch write route before any item is processed.
func (f *Forwarder) Batch() gin.HandlerFunc { return f.middleware(kindGated, false) }

// Claim gates the long-poll claim route; the forward timeout grows by the
// request's waitSeconds (capped like the scheduler at 30 s).
func (f *Forwarder) Claim() gin.HandlerFunc { return f.middleware(kindGated, true) }

func (f *Forwarder) middleware(kind routeKind, longPoll bool) gin.HandlerFunc {
	if f == nil {
		return func(c *gin.Context) { c.Next() }
	}
	return func(c *gin.Context) {
		st, err := bufferBody(c)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{errorKey: "invalid body"})
			return
		}
		st.fwd = f
		if longPoll && !st.overflow {
			st.waitSeconds = claimWaitSeconds(st.body)
		}
		c.Set(stateContextKey, st)
		v := f.leadership()
		switch decide(kind, v) {
		case decideForward:
			f.forward(c, st, v.target)
			c.Abort()
		case decideUnavailable:
			f.refuse(c, resultNoLeader, v.target)
		case decideLocal:
			c.Next()
		}
	}
}

func decide(kind routeKind, v view) decision {
	switch {
	case v.groups == 0 || v.leads > 0:
		return decideLocal
	case v.unified:
		return decideForward
	case kind == kindSingle:
		return decideLocal
	default:
		return decideUnavailable
	}
}

// leadership calls RequireWriteLeader on every group. The view is unified
// when this node leads no group and every group names the same non-empty
// leader URL. Under RAFT_MUX_ENABLED with one Pebble shard (both
// installations) there is exactly one group, so the view is that group's.
func (f *Forwarder) leadership() view {
	v := view{groups: len(f.groups), unified: true}
	seen := false
	for _, g := range f.groups {
		err := g.RequireWriteLeader()
		if err == nil {
			v.leads++
			continue
		}
		addr := hintAddr(err)
		switch {
		case !seen:
			v.target, seen = addr, true
		case addr != v.target:
			v.unified = false
		}
	}
	if v.leads > 0 || !seen || v.target == "" {
		v.unified = false
	}
	return v
}

func hintAddr(err error) string {
	var hint domain.LeaderHint
	if errors.As(err, &hint) {
		return hint.LeaderHTTPAddr()
	}
	return ""
}

// bufferBody reads the request body once so it can be replayed to the
// leader. Bodies above MaxBodyBytes stay streamable for local handling but
// are marked as not forwardable.
func bufferBody(c *gin.Context) (*requestState, error) {
	st := &requestState{}
	body := c.Request.Body
	if body == nil || body == http.NoBody {
		return st, nil
	}
	buf, err := io.ReadAll(io.LimitReader(body, MaxBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(buf)) > MaxBodyBytes {
		st.overflow = true
		c.Request.Body = struct {
			io.Reader
			io.Closer
		}{io.MultiReader(bytes.NewReader(buf), body), body}
		return st, nil
	}
	st.body = buf
	c.Request.Body = io.NopCloser(bytes.NewReader(buf))
	return st, nil
}

func claimWaitSeconds(body []byte) int {
	var req struct {
		WaitSeconds int `json:"waitSeconds"`
	}
	if json.Unmarshal(body, &req) != nil || req.WaitSeconds <= 0 {
		return 0
	}
	return min(req.WaitSeconds, maxClaimWaitSeconds)
}

// HandleNotLeader forwards the request when err is a Raft not-leader error
// and reports whether it wrote the response. Controllers call it in place of
// the former 307 redirect. Without a forwarding context (no Raft, or the
// route has no gate) the answer is 503 leader_unavailable, never 307.
func HandleNotLeader(c *gin.Context, err error) bool {
	var hint domain.LeaderHint
	if !errors.As(err, &hint) {
		return false
	}
	st := stateFrom(c)
	if st == nil {
		var none *Forwarder
		none.refuse(c, resultDisabled, "")
		return true
	}
	st.fwd.forward(c, st, hint.LeaderHTTPAddr())
	c.Abort()
	return true
}

func stateFrom(c *gin.Context) *requestState {
	v, ok := c.Get(stateContextKey)
	if !ok {
		return nil
	}
	st, ok := v.(*requestState)
	if !ok || st == nil || st.fwd == nil {
		return nil
	}
	return st
}
