package leaderforward

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/osvaldoandrade/codeq/internal/metrics"
)

// Bounded values of the result label of codeq_leader_forward_total.
const (
	resultOK                  = "ok"
	resultUpstreamUnavailable = "upstream_unavailable"
	resultLoop                = "loop"
	resultUnconfigured        = "unconfigured"
	resultNoLeader            = "no_leader"
	resultTooLarge            = "body_too_large"
	resultTimeout             = "timeout"
	resultCanceled            = "canceled"
	resultError               = "error"
	resultRedirect            = "redirect_refused"
	resultInterrupted         = "interrupted"
	resultDisabled            = "disabled"

	logEvent          = "codeq_leader_forward"
	maxLoggedValueLen = 256
	unknownRoute      = "unknown"
)

// forwardedRequestHeaders are the only request headers sent to the leader.
// Authorization and the dev-only X-Role are the leader's authentication
// input; X-Request-Id keeps one correlation ID across both hops.
var forwardedRequestHeaders = []string{"Authorization", "Content-Type", "X-Role"}

// relayedResponseHeaders are the only leader response headers copied back.
var relayedResponseHeaders = []string{"Content-Type", "Retry-After"}

// forward applies the loop, target and size rules, then relays.
func (f *Forwarder) forward(c *gin.Context, st *requestState, target string) {
	switch {
	case c.GetHeader(HeaderForwarded) != "":
		f.refuse(c, resultLoop, target)
	case !f.allowedTarget(target):
		f.refuse(c, resultUnconfigured, target)
	case st.overflow:
		f.record(c, resultTooLarge, target, http.StatusRequestEntityTooLarge, 0)
		c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{errorKey: CodeRequestTooLarge})
	default:
		f.relay(c, st, target)
	}
}

func (f *Forwarder) relay(c *gin.Context, st *requestState, target string) {
	start := time.Now()
	timeout := f.timeout + time.Duration(st.waitSeconds)*time.Second
	ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
	defer cancel()

	req, err := f.newRequest(ctx, c, st, target)
	if err != nil {
		f.refuse(c, resultError, target)
		return
	}
	resp, sent, err := f.send(req)
	if err != nil {
		result := classify(c.Request.Context(), ctx)
		if sent {
			f.ambiguous(c, result, target, time.Since(start))
			return
		}
		f.refuse(c, result, target)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	f.relayResponse(c, resp, target, start)
}

// send performs the forward and reports whether the request was sent: the
// transport accepted every request byte for writing (httptrace WroteRequest
// without error). From then on the leader may have applied the request, so
// a failure is ambiguous. Up to the transport's final write buffer (4 KiB)
// may still be unflushed at that point; that case is treated as sent (fail
// closed). Before it (dial, TLS, connection refused, a leader that stops
// reading mid-body) the leader cannot hold a complete request and the
// answer stays a retryable 503.
func (f *Forwarder) send(req *http.Request) (*http.Response, bool, error) {
	var sent atomic.Bool
	trace := &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				sent.Store(true)
			}
		},
	}
	resp, err := f.client.Do(req.WithContext(httptrace.WithClientTrace(req.Context(), trace)))
	return resp, sent.Load(), err
}

// relayResponse copies the leader's answer, refusing any 3xx.
func (f *Forwarder) relayResponse(c *gin.Context, resp *http.Response, target string, start time.Time) {
	if resp.StatusCode >= http.StatusMultipleChoices && resp.StatusCode < http.StatusBadRequest {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		f.refuse(c, resultRedirect, target)
		return
	}
	for _, name := range relayedResponseHeaders {
		if v := resp.Header.Get(name); v != "" {
			c.Writer.Header().Set(name, v)
		}
	}
	c.Writer.WriteHeader(resp.StatusCode)
	_, copyErr := io.Copy(c.Writer, resp.Body)
	result := resultOK
	if resp.StatusCode == http.StatusServiceUnavailable {
		result = resultUpstreamUnavailable
	}
	if copyErr != nil {
		result = resultError
	}
	f.record(c, result, target, resp.StatusCode, time.Since(start))
	c.Abort()
}

func (f *Forwarder) newRequest(ctx context.Context, c *gin.Context, st *requestState, target string) (*http.Request, error) {
	var body io.Reader = http.NoBody
	if len(st.body) > 0 {
		body = bytes.NewReader(st.body)
	}
	req, err := http.NewRequestWithContext(ctx, c.Request.Method, targetURL(target, c.Request.URL), body)
	if err != nil {
		return nil, err
	}
	for _, name := range forwardedRequestHeaders {
		if v := c.GetHeader(name); v != "" {
			req.Header.Set(name, v)
		}
	}
	if id := c.GetString("request_id"); id != "" {
		req.Header.Set("X-Request-Id", id)
	}
	req.Header.Set(HeaderForwarded, "1")
	return req, nil
}

func classify(parent, ctx context.Context) string {
	switch {
	case parent.Err() != nil:
		return resultCanceled
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return resultTimeout
	default:
		return resultError
	}
}

// ambiguous answers a forward that failed after the whole request was sent:
// the leader may already have applied it, so the answer never invites a
// blind retry (no Retry-After). A timeout gives 504 leader_forward_timeout;
// any other post-send failure (connection reset, leader lost) gives 502
// leader_forward_interrupted. A client retries only with an idempotency key
// or after reading the task state.
func (f *Forwarder) ambiguous(c *gin.Context, result, target string, elapsed time.Duration) {
	status, code := http.StatusBadGateway, CodeForwardInterrupted
	switch result {
	case resultTimeout:
		status, code = http.StatusGatewayTimeout, CodeForwardTimeout
	case resultError:
		result = resultInterrupted
	}
	f.record(c, result, target, status, elapsed)
	c.AbortWithStatusJSON(status, gin.H{errorKey: code})
}

// refuse writes 503 leader_unavailable with Retry-After: 1. It is used only
// when the leader cannot have applied the request: no leader, loop,
// unconfigured target, or a failure before the request was fully sent
// (dial, connect, write). It is safe on a nil Forwarder.
func (f *Forwarder) refuse(c *gin.Context, result, target string) {
	f.record(c, result, target, http.StatusServiceUnavailable, 0)
	c.Header("Retry-After", retryAfterSeconds)
	c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{errorKey: CodeLeaderUnavailable})
}

// record increments the metric and writes at most one log line. The line
// never contains the Authorization header, the body or any token.
func (f *Forwarder) record(c *gin.Context, result, target string, status int, elapsed time.Duration) {
	route := c.FullPath()
	if route == "" {
		route = unknownRoute
	}
	metrics.LeaderForwardTotal.WithLabelValues(route, result).Inc()
	logger := slog.Default()
	if f != nil && f.logger != nil {
		logger = f.logger
	}
	level := slog.LevelWarn
	if result == resultOK {
		level = slog.LevelDebug
	}
	logger.Log(c.Request.Context(), level, "leader forward",
		"event", logEvent,
		"route", route,
		"method", c.Request.Method,
		"result", result,
		"status", status,
		"leader", truncate(target),
		"requestId", truncate(c.GetString("request_id")),
		"durationMs", elapsed.Milliseconds(),
	)
}

func truncate(s string) string {
	if len(s) > maxLoggedValueLen {
		return s[:maxLoggedValueLen]
	}
	return s
}
