package leaderforward

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	_ "github.com/osvaldoandrade/codeq/internal/metrics" // registers codeq_leader_forward_total
	"github.com/osvaldoandrade/codeq/internal/middleware"
)

const (
	testToken       = "Bearer super-secret-binding-token"
	testRoute       = "/v1/codeq/tasks"
	selfID          = "codeq-0"
	peerOneID       = "codeq-1"
	peerOneURL      = "http://codeq-1.codecloud-queue.svc.cluster.local:8080"
	userinfoPeerURL = "http://user:pw@codeq-1:8080"
	selfURL         = "http://codeq-0.codecloud-queue.svc.cluster.local:8080"
	unavailable     = `{"error":"leader_unavailable"}`
)

type fakeGroup struct{ err error }

func (g fakeGroup) RequireWriteLeader() error { return g.err }

type hintErr string

func (h hintErr) Error() string          { return "raft: not leader" }
func (h hintErr) LeaderHTTPAddr() string { return string(h) }

func follower(leaderURL string) Leadership { return fakeGroup{err: hintErr(leaderURL)} }

var leaderGroup Leadership = fakeGroup{}

type seenRequest struct {
	method, uri, body string
	header            http.Header
}

// leaderServer records every request and answers with fn.
type leaderServer struct {
	*httptest.Server
	calls atomic.Int32
	seen  chan seenRequest
}

func newLeader(t *testing.T, fn http.HandlerFunc) *leaderServer {
	t.Helper()
	l := &leaderServer{seen: make(chan seenRequest, 8)}
	l.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l.calls.Add(1)
		b, _ := io.ReadAll(r.Body)
		l.seen <- seenRequest{r.Method, r.URL.RequestURI(), string(b), r.Header.Clone()}
		fn(w, r)
	}))
	t.Cleanup(l.Close)
	return l
}

func accepted(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Location", "http://must-not-leak")
	w.Header().Set("Set-Cookie", "a=b")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"id":"t-1"}`))
}

func newForwarder(t *testing.T, leaderURL string, groups []Leadership, logs io.Writer) *Forwarder {
	t.Helper()
	peers := map[string]string{selfID: selfURL, peerOneID: peerOneURL}
	if leaderURL != "" {
		peers["codeq-2"] = leaderURL
	}
	if logs == nil {
		logs = io.Discard
	}
	return New(Config{
		PeerHTTPAddrs: peers,
		SelfID:        selfID,
		Groups:        groups,
		Logger:        slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Timeout:       2 * time.Second,
	})
}

// engine registers route with the given gate; the local handler answers
// 299 so tests can tell local execution from a relayed answer.
func engine(gate gin.HandlerFunc, local gin.HandlerFunc) *gin.Engine {
	gin.SetMode(gin.TestMode)
	e := gin.New()
	e.Use(middleware.RequestIDMiddleware())
	if local == nil {
		local = func(c *gin.Context) {
			b, _ := io.ReadAll(c.Request.Body)
			c.String(299, "local:%d", len(b))
		}
	}
	e.POST(testRoute, gate, local)
	e.POST(testRoute+"/claim", gate, local)
	return e
}

func do(e *gin.Engine, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Authorization", testToken)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	return rec
}

func expectUnavailable(t *testing.T, rec *httptest.ResponseRecorder, label string) {
	t.Helper()
	if rec.Code != http.StatusServiceUnavailable || strings.TrimSpace(rec.Body.String()) != unavailable {
		t.Fatalf("%s: status %d body %s, want 503 %s", label, rec.Code, rec.Body.String(), unavailable)
	}
	if rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("%s: Retry-After = %q, want 1", label, rec.Header().Get("Retry-After"))
	}
	if rec.Header().Get("Location") != "" {
		t.Fatalf("%s: Location header leaked: %q", label, rec.Header().Get("Location"))
	}
}

func TestForwardPreservesRequestAndRelaysAnswer(t *testing.T) {
	leader := newLeader(t, accepted)
	f := newForwarder(t, leader.URL, []Leadership{follower(leader.URL)}, nil)
	e := engine(f.Single(), func(c *gin.Context) { t.Error("follower ran the controller") })
	body := `{"command":"cflow-executar","payload":{"k":"v"}}`
	rec := do(e, testRoute+"?trace=1&x=%2Fy", body, map[string]string{
		"Cookie": "session=1", "X-Forwarded-For": "203.0.113.9", "X-Request-Id": "req-42", "X-Custom": "nope",
	})
	if rec.Code != http.StatusAccepted || rec.Body.String() != `{"id":"t-1"}` {
		t.Fatalf("relayed %d %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Content-Type") != "application/json" || rec.Header().Get("Location") != "" || rec.Header().Get("Set-Cookie") != "" {
		t.Fatalf("response headers not allow-listed: %v", rec.Header())
	}
	s := <-leader.seen
	if s.method != http.MethodPost || s.uri != testRoute+"?trace=1&x=%2Fy" || s.body != body {
		t.Fatalf("leader saw %s %s %q", s.method, s.uri, s.body)
	}
	if s.header.Get("Authorization") != testToken || s.header.Get(HeaderForwarded) != "1" || s.header.Get("X-Request-Id") != "req-42" {
		t.Fatalf("forwarded headers wrong: %v", s.header)
	}
	for _, h := range []string{"Cookie", "X-Forwarded-For", "X-Custom"} {
		if s.header.Get(h) != "" {
			t.Fatalf("header %s must not be forwarded", h)
		}
	}
}

func TestForwardedRequestIsNeverForwardedAgain(t *testing.T) {
	leader := newLeader(t, accepted)
	f := newForwarder(t, leader.URL, []Leadership{follower(leader.URL)}, nil)
	before := forwardCount(t, testRoute, resultLoop)
	for _, gate := range []gin.HandlerFunc{f.Single(), f.Batch(), f.Claim()} {
		rec := do(engine(gate, nil), testRoute, `{}`, map[string]string{HeaderForwarded: "1"})
		expectUnavailable(t, rec, "loop")
	}
	if leader.calls.Load() != 0 {
		t.Fatalf("leader contacted %d times", leader.calls.Load())
	}
	if got := forwardCount(t, testRoute, resultLoop) - before; got != 3 {
		t.Fatalf("loop metric delta = %v, want 3", got)
	}
}

// forwardCount scrapes codeq_leader_forward_total{route,result} from the
// default registry, as Prometheus would.
func forwardCount(t *testing.T, route, result string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	prefix := `codeq_leader_forward_total{result="` + result + `",route="` + route + `"} `
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if v, ok := strings.CutPrefix(line, prefix); ok {
			n, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			return n
		}
	}
	return 0
}

func TestForwardedHeaderOnLeaderRunsLocally(t *testing.T) {
	f := newForwarder(t, "", []Leadership{leaderGroup}, nil)
	rec := do(engine(f.Single(), nil), testRoute, `{"a":1}`, map[string]string{HeaderForwarded: "1"})
	if rec.Code != 299 || rec.Body.String() != "local:7" {
		t.Fatalf("leader did not run locally: %d %s", rec.Code, rec.Body.String())
	}
}

func TestOnlyExactConfiguredPeersAreTargets(t *testing.T) {
	leader := newLeader(t, accepted)
	cases := map[string]string{
		"unconfigured host": "http://attacker.example:8080",
		"trailing slash":    leader.URL + "/",
		"self":              selfURL,
		"empty hint":        "",
	}
	for name, hint := range cases {
		f := newForwarder(t, leader.URL, []Leadership{follower(hint)}, nil)
		expectUnavailable(t, do(engine(f.Batch(), nil), testRoute, `{}`, nil), name)
	}
	if leader.calls.Load() != 0 {
		t.Fatalf("leader contacted %d times", leader.calls.Load())
	}
}

func TestMalformedPeerURLsAreExcluded(t *testing.T) {
	for _, bad := range []string{"ftp://codeq-1:8080", userinfoPeerURL, "http://codeq-1:8080?x=1", "http://codeq-1:8080#f", "codeq-1:8080", "http://"} {
		f := New(Config{PeerHTTPAddrs: map[string]string{peerOneID: bad}, SelfID: selfID})
		if f.allowedTarget(bad) {
			t.Fatalf("malformed peer %q accepted", bad)
		}
	}
	if !validPeerURL("https://codeq-1.codecloud-queue.svc.cluster.local:8443") {
		t.Fatal("https peer rejected")
	}
}

func TestRedirectFromLeaderIsNeverRelayedOrFollowed(t *testing.T) {
	var hops atomic.Int32
	elsewhere := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hops.Add(1) }))
	defer elsewhere.Close()
	leader := newLeader(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+r.URL.Path, http.StatusTemporaryRedirect)
	})
	f := newForwarder(t, leader.URL, []Leadership{follower(leader.URL)}, nil)
	expectUnavailable(t, do(engine(f.Single(), nil), testRoute, `{}`, nil), "redirect")
	if hops.Load() != 0 {
		t.Fatal("redirect was followed")
	}
}

func TestLeaderUnavailableAnswerIsRelayed(t *testing.T) {
	leader := newLeader(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(unavailable))
	})
	f := newForwarder(t, leader.URL, []Leadership{follower(leader.URL)}, nil)
	expectUnavailable(t, do(engine(f.Single(), nil), testRoute, `{}`, nil), "relayed 503")
}

func TestTransportFailureIsRetryable(t *testing.T) {
	leader := newLeader(t, accepted)
	url := leader.URL
	leader.Close()
	f := newForwarder(t, url, []Leadership{follower(url)}, nil)
	expectUnavailable(t, do(engine(f.Single(), nil), testRoute, `{}`, nil), "closed leader")
}

func TestTimeoutAndClaimWaitExtension(t *testing.T) {
	leader := newLeader(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		accepted(w, r)
	})
	f := newForwarder(t, leader.URL, []Leadership{follower(leader.URL)}, nil)
	f.timeout = 100 * time.Millisecond
	// The request reached the leader before the timeout: ambiguous, 504.
	expectAmbiguous(t, do(engine(f.Single(), nil), testRoute, `{}`, nil), http.StatusGatewayTimeout, CodeForwardTimeout, "timeout")
	// waitSeconds=1 extends the claim budget to 1.1 s.
	rec := do(engine(f.Claim(), nil), testRoute+"/claim", `{"waitSeconds":1}`, nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("claim with wait: %d %s", rec.Code, rec.Body.String())
	}
}

func TestClaimWaitSeconds(t *testing.T) {
	cases := map[string]int{`{"waitSeconds":10}`: 10, `{"waitSeconds":45}`: 30, `{"waitSeconds":-3}`: 0, `{}`: 0, `not json`: 0, ``: 0}
	for body, want := range cases {
		if got := claimWaitSeconds([]byte(body)); got != want {
			t.Fatalf("claimWaitSeconds(%q) = %d, want %d", body, got, want)
		}
	}
}

func TestBodyLimitOnlyAppliesToForwarding(t *testing.T) {
	leader := newLeader(t, accepted)
	big := strings.Repeat("x", int(MaxBodyBytes)+1)
	f := newForwarder(t, leader.URL, []Leadership{follower(leader.URL)}, nil)
	rec := do(engine(f.Single(), nil), testRoute, big, nil)
	if rec.Code != http.StatusRequestEntityTooLarge || !strings.Contains(rec.Body.String(), CodeRequestTooLarge) {
		t.Fatalf("oversize forward: %d %s", rec.Code, rec.Body.String())
	}
	if leader.calls.Load() != 0 {
		t.Fatal("oversize body was forwarded")
	}
	// The leader itself keeps reading the whole body (no new limit).
	local := newForwarder(t, "", []Leadership{leaderGroup}, nil)
	rec = do(engine(local.Single(), nil), testRoute, big, nil)
	if rec.Code != 299 || rec.Body.String() != "local:"+strconv.Itoa(len(big)) {
		t.Fatalf("leader body handling changed: %d %s", rec.Code, rec.Body.String())
	}
	// Exactly MaxBodyBytes is forwarded intact.
	rec = do(engine(f.Single(), nil), testRoute, big[:MaxBodyBytes], nil)
	if rec.Code != http.StatusAccepted || len((<-leader.seen).body) != int(MaxBodyBytes) {
		t.Fatalf("limit-sized body not forwarded: %d", rec.Code)
	}
}

func TestDecisionMatrix(t *testing.T) {
	cases := []struct {
		name string
		kind routeKind
		v    view
		want decision
	}{
		{"no raft", kindGated, view{}, decideLocal},
		{"leader", kindGated, view{groups: 1, leads: 1}, decideLocal},
		{"partial leader", kindGated, view{groups: 4, leads: 1}, decideLocal},
		{"follower unified", kindGated, view{groups: 1, target: "u", unified: true}, decideForward},
		{"single split", kindSingle, view{groups: 4}, decideLocal},
		{"gated split", kindGated, view{groups: 4}, decideUnavailable},
	}
	for _, tc := range cases {
		if got := decide(tc.kind, tc.v); got != tc.want {
			t.Fatalf("%s: decide = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestLeadershipView(t *testing.T) {
	a, b := "http://a:8080", "http://b:8080"
	cases := []struct {
		name    string
		groups  []Leadership
		leads   int
		unified bool
		target  string
	}{
		{"one group follower", []Leadership{follower(a)}, 0, true, a},
		{"mux shards same leader", []Leadership{follower(a), follower(a)}, 0, true, a},
		{"mux shards split leaders", []Leadership{follower(a), follower(b)}, 0, false, a},
		{"leads one shard", []Leadership{leaderGroup, follower(a)}, 1, false, a},
		{"election", []Leadership{follower("")}, 0, false, ""},
		{"opaque error", []Leadership{fakeGroup{err: errors.New("boom")}}, 0, false, ""},
	}
	for _, tc := range cases {
		f := New(Config{Groups: tc.groups})
		v := f.leadership()
		if v.leads != tc.leads || v.unified != tc.unified || v.target != tc.target || v.groups != len(tc.groups) {
			t.Fatalf("%s: view = %+v", tc.name, v)
		}
	}
}

func TestSplitLeadershipGatesBatchesButNotSingleWrites(t *testing.T) {
	split := []Leadership{follower("http://a:8080"), follower("http://b:8080")}
	f := New(Config{Groups: split})
	expectUnavailable(t, do(engine(f.Batch(), nil), testRoute, `{}`, nil), "batch split")
	if rec := do(engine(f.Single(), nil), testRoute, `{}`, nil); rec.Code != 299 {
		t.Fatalf("single write must run locally under split leadership: %d", rec.Code)
	}
}

func TestHandleNotLeader(t *testing.T) {
	leader := newLeader(t, accepted)
	f := newForwarder(t, leader.URL, []Leadership{leaderGroup}, nil)
	// The gate runs locally (leader); the controller then hits a not-leader
	// error, e.g. leadership moved after the gate.
	e := engine(f.Single(), func(c *gin.Context) {
		if !HandleNotLeader(c, hintErr(leader.URL)) {
			t.Error("hint not handled")
		}
	})
	if rec := do(e, testRoute, `{"command":"c"}`, nil); rec.Code != http.StatusAccepted {
		t.Fatalf("reactive forward: %d %s", rec.Code, rec.Body.String())
	}
	if (<-leader.seen).body != `{"command":"c"}` {
		t.Fatal("reactive forward lost the body")
	}
	// Without a gate (no Raft) a hint is a 503, never a 307.
	e = engine(func(c *gin.Context) { c.Next() }, func(c *gin.Context) { HandleNotLeader(c, hintErr(leader.URL)) })
	expectUnavailable(t, do(e, testRoute, `{}`, nil), "no forwarder")
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	if HandleNotLeader(c, errors.New("other")) {
		t.Fatal("non-leader error handled")
	}
}

func TestNilForwarderPassesThrough(t *testing.T) {
	var f *Forwarder
	for _, gate := range []gin.HandlerFunc{f.Single(), f.Batch(), f.Claim()} {
		if rec := do(engine(gate, nil), testRoute, `{}`, nil); rec.Code != 299 {
			t.Fatalf("nil forwarder changed routing: %d", rec.Code)
		}
	}
}

func TestLogsNeverContainCredentials(t *testing.T) {
	var logs bytes.Buffer
	leader := newLeader(t, accepted)
	f := newForwarder(t, leader.URL, []Leadership{follower(leader.URL)}, &logs)
	e := engine(f.Single(), nil)
	do(e, testRoute, `{"secret":"payload"}`, nil)
	do(e, testRoute, `{"secret":"payload"}`, map[string]string{HeaderForwarded: "1"})
	f2 := newForwarder(t, leader.URL, []Leadership{follower("http://attacker.example")}, &logs)
	do(engine(f2.Single(), nil), testRoute, `{}`, nil)
	out := logs.String()
	if !strings.Contains(out, `"event":"codeq_leader_forward"`) || !strings.Contains(out, `"result":"loop"`) {
		t.Fatalf("missing forward log lines: %s", out)
	}
	for _, secret := range []string{"super-secret-binding-token", "Bearer", "payload", "Authorization"} {
		if strings.Contains(out, secret) {
			t.Fatalf("log leaked %q: %s", secret, out)
		}
	}
	if n := strings.Count(out, "\n"); n != 3 {
		t.Fatalf("want one log line per forward decision, got %d: %s", n, out)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("client reset") }

func TestUnreadableBodyIsRejected(t *testing.T) {
	f := New(Config{Groups: []Leadership{leaderGroup}})
	e := engine(f.Single(), nil)
	req := httptest.NewRequest(http.MethodPost, testRoute, errReader{})
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unreadable body: %d", rec.Code)
	}
}
