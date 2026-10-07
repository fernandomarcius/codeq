package leaderforward

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	timeoutBody     = `{"error":"leader_forward_timeout"}`
	interruptedBody = `{"error":"leader_forward_interrupted"}`
)

// expectAmbiguous asserts a post-send failure answer: the exact status and
// body, and no Retry-After, so no client is invited to repeat a request the
// leader may already have applied.
func expectAmbiguous(t *testing.T, rec *httptest.ResponseRecorder, status int, code, label string) {
	t.Helper()
	want := `{"error":"` + code + `"}`
	if rec.Code != status || strings.TrimSpace(rec.Body.String()) != want {
		t.Fatalf("%s: status %d body %s, want %d %s", label, rec.Code, rec.Body.String(), status, want)
	}
	if v := rec.Header().Get("Retry-After"); v != "" {
		t.Fatalf("%s: ambiguous answer carries Retry-After %q", label, v)
	}
}

func TestSentThenTimedOutIs504WithoutRetryAfter(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	leader := newLeader(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
		accepted(w, r)
	})
	f := newForwarder(t, leader.URL, []Leadership{follower(leader.URL)}, nil)
	f.timeout = 150 * time.Millisecond
	before := forwardCount(t, testRoute, resultTimeout)
	rec := do(engine(f.Single(), nil), testRoute, `{"command":"c","idempotencyKey":"k"}`, nil)
	expectAmbiguous(t, rec, http.StatusGatewayTimeout, CodeForwardTimeout, "sent then timed out")
	if strings.TrimSpace(rec.Body.String()) != timeoutBody {
		t.Fatalf("body %s", rec.Body.String())
	}
	if got := leader.calls.Load(); got != 1 {
		t.Fatalf("leader received %d requests, want exactly 1 (no transport retry)", got)
	}
	if got := forwardCount(t, testRoute, resultTimeout) - before; got != 1 {
		t.Fatalf("timeout metric delta = %v, want 1", got)
	}
	// The claim route follows the same rule once its extended budget expires.
	claimRoute := testRoute + "/claim"
	beforeClaim := forwardCount(t, claimRoute, resultTimeout)
	rec = do(engine(f.Claim(), nil), claimRoute, `{"waitSeconds":0}`, nil)
	expectAmbiguous(t, rec, http.StatusGatewayTimeout, CodeForwardTimeout, "claim sent then timed out")
	if got := forwardCount(t, claimRoute, resultTimeout) - beforeClaim; got != 1 {
		t.Fatalf("claim timeout metric delta = %v, want 1", got)
	}
}

func TestSentThenInterruptedIs502WithoutRetryAfter(t *testing.T) {
	leader := newLeader(t, func(w http.ResponseWriter, _ *http.Request) {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	})
	f := newForwarder(t, leader.URL, []Leadership{follower(leader.URL)}, nil)
	before := forwardCount(t, testRoute, resultInterrupted)
	rec := do(engine(f.Single(), nil), testRoute, `{}`, nil)
	expectAmbiguous(t, rec, http.StatusBadGateway, CodeForwardInterrupted, "sent then connection lost")
	if strings.TrimSpace(rec.Body.String()) != interruptedBody {
		t.Fatalf("body %s", rec.Body.String())
	}
	if got := leader.calls.Load(); got != 1 {
		t.Fatalf("leader received %d requests, want exactly 1 (no transport retry)", got)
	}
	if got := forwardCount(t, testRoute, resultInterrupted) - before; got != 1 {
		t.Fatalf("interrupted metric delta = %v, want 1", got)
	}
}

// stalledConn accepts no request bytes: Write blocks until the transport
// closes the connection, so the request is never fully sent.
type stalledConn struct {
	net.Conn
	once   sync.Once
	closed chan struct{}
}

func (c *stalledConn) Write([]byte) (int, error) {
	<-c.closed
	return 0, net.ErrClosed
}

func (c *stalledConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// TestTimeoutBeforeRequestIsSentStays503: the leader stops reading in the
// middle of the body, so the transport never accepts the whole request and
// the leader cannot have applied it. The answer stays 503 with Retry-After.
// The body exceeds the transport's 4 KiB write buffer so the stall happens
// before the request is fully handed over.
func TestTimeoutBeforeRequestIsSentStays503(t *testing.T) {
	leader := newLeader(t, accepted)
	dialer := &net.Dialer{}
	f := New(Config{
		PeerHTTPAddrs: map[string]string{selfID: selfURL, "codeq-2": leader.URL},
		SelfID:        selfID,
		Groups:        []Leadership{follower(leader.URL)},
		Timeout:       150 * time.Millisecond,
		Transport: &http.Transport{
			Proxy: nil,
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := dialer.DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				return &stalledConn{Conn: conn, closed: make(chan struct{})}, nil
			},
		},
	})
	body := `{"payload":"` + strings.Repeat("x", 64<<10) + `"}`
	expectUnavailable(t, do(engine(f.Single(), nil), testRoute, body, nil), "timeout before send")
	if got := leader.calls.Load(); got != 0 {
		t.Fatalf("leader handled %d requests, want 0", got)
	}
}

func TestAmbiguousLogsNeverContainCredentials(t *testing.T) {
	var logs strings.Builder
	leader := newLeader(t, func(w http.ResponseWriter, _ *http.Request) {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	})
	f := newForwarder(t, leader.URL, []Leadership{follower(leader.URL)}, &logs)
	do(engine(f.Single(), nil), testRoute, `{"secret":"payload"}`, nil)
	out := logs.String()
	if !strings.Contains(out, `"result":"interrupted"`) || !strings.Contains(out, `"status":502`) {
		t.Fatalf("missing interrupted log line: %s", out)
	}
	for _, secret := range []string{"super-secret-binding-token", "Bearer", "payload"} {
		if strings.Contains(out, secret) {
			t.Fatalf("log leaked %q: %s", secret, out)
		}
	}
}
