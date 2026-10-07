package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/osvaldoandrade/codeq/internal/leaderforward"
	_ "github.com/osvaldoandrade/codeq/pkg/auth/static"
	"github.com/osvaldoandrade/codeq/pkg/config"
)

// Platform ADR-0022 C1.5: three in-process Raft voters behind real HTTP
// listeners, wired like the installations (RAFT_MUX_ENABLED=true, one Pebble
// shard, RAFT_PEER_HTTP_ADDRS set). Every call goes through fwdCall, which
// fails the test on any 3xx or Location header: no client ever sees a 307.

const (
	fwdToken       = "dev-token"
	fwdCommand     = "GENERATE_MASTER"
	fwdRetryAfter  = "Retry-After"
	fwdUnavailable = leaderforward.CodeLeaderUnavailable
	fwdBatchPath   = pathTasks + "/batch"
	fwdBatchClaim  = pathClaim + "/batch"
	fwdBatchResult = pathTasks + "/batch/results"
	fwdResults     = "results"
	fwdDelay       = "delaySeconds"
	fwdExtend      = "extendSeconds"
	fwdLease       = "leaseSeconds"
	fwdNumShards   = "numShards"
	fwdOtherTopic  = "other-topic"
	fwdTaskID      = "taskId"
)

type fwdNode struct {
	id      string
	app     *Application
	server  *httptest.Server
	handler atomic.Value
	closed  atomic.Bool
}

func (n *fwdNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h, _ := n.handler.Load().(http.Handler)
	if h == nil {
		http.NotFound(w, r)
		return
	}
	h.ServeHTTP(w, r)
}

func (n *fwdNode) stop() {
	if n.closed.Swap(true) {
		return
	}
	n.server.CloseClientConnections()
	n.server.Close()
	if n.app == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = n.app.TracingShutdown(ctx)
}

type fwdCluster struct {
	t     *testing.T
	nodes []*fwdNode
}

type fwdOptions struct {
	shards    int
	httpPeers bool
	base      func(t *testing.T) *config.Config
}

func staticForwardConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		Timezone: topicTestTimezone, LogLevel: topicTestLogLevel, LogFormat: topicTestLogFormat, Env: topicTestEnvironment,
		DefaultLeaseSeconds: 60, RequeueInspectLimit: 50, LocalArtifactsDir: t.TempDir(), MaxAttemptsDefault: 5,
		BackoffPolicy: topicTestBackoffPolicy, BackoffBaseSeconds: 1, BackoffMaxSeconds: 3,
		WebhookHmacSecret: topicTestSecret, WorkerAudience: topicTestWorkerAudience,
		SubscriptionMinIntervalSeconds: 5, SubscriptionCleanupIntervalSeconds: 60,
		ResultWebhookMaxAttempts: 1, ResultWebhookBaseBackoffSeconds: 1, ResultWebhookMaxBackoffSeconds: 2,
		ProducerAuthProvider: topicTestAuthProvider,
		ProducerAuthConfig:   json.RawMessage(`{"token":"dev-token","subject":"producer-dev","raw":{"role":"ADMIN","tenantId":"dev-tenant"}}`),
		WorkerAuthProvider:   topicTestAuthProvider,
		WorkerAuthConfig: json.RawMessage(`{"token":"dev-token","subject":"worker-dev","eventTypes":["*"],
 "scopes":["codeq:claim","codeq:heartbeat","codeq:abandon","codeq:nack","codeq:result","codeq:subscribe"],"raw":{"tenantId":"dev-tenant"}}`),
		RedisAddr: topicTestRedisAddr,
	}
}

func startForwardCluster(t *testing.T, opts fwdOptions) *fwdCluster {
	t.Helper()
	ports := pickThreeFreePorts(t)
	ids := []string{topicNodeOne, topicNodeTwo, topicNodeThree}
	raftPeers := map[string]string{}
	httpPeers := map[string]string{}
	c := &fwdCluster{t: t, nodes: make([]*fwdNode, len(ids))}
	for i, id := range ids {
		raftPeers[id] = "127.0.0.1:" + ports[i]
		n := &fwdNode{id: id}
		n.server = httptest.NewServer(n)
		httpPeers[id] = n.server.URL
		c.nodes[i] = n
	}
	for _, i := range []int{1, 2, 0} { // followers listen before node-1 bootstraps
		cfg := opts.base(t)
		pcfg, _ := json.Marshal(map[string]any{topicPersistencePathKey: t.TempDir() + "/pebble", fwdNumShards: opts.shards})
		cfg.PersistenceProvider, cfg.PersistenceConfig = topicTestStorageProvider, pcfg
		cfg.Raft = config.RaftConfig{
			Enabled: true, SelfID: ids[i], BindAddr: raftPeers[ids[i]], Bootstrap: i == 0, Peers: raftPeers,
			MuxEnabled: true, TopicCatalogProtocol: "v1",
			HeartbeatMS: 50, ElectionMS: 50, LeaderLeaseMS: 50, CommitMS: 10, ApplyTimeoutSeconds: 3,
		}
		if opts.httpPeers {
			cfg.Raft.PeerHTTPAddrs = httpPeers
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("[%s] validate: %v", ids[i], err)
		}
		app, err := NewApplication(cfg)
		if err != nil {
			t.Fatalf("[%s] app: %v", ids[i], err)
		}
		SetupMappings(app)
		c.nodes[i].app = app
		c.nodes[i].handler.Store(http.Handler(app.Engine))
		// Registered after this node's TempDirs, so (LIFO) the node stops
		// before its Pebble directory is removed.
		node := c.nodes[i]
		t.Cleanup(node.stop)
	}
	return c
}

// settled reports whether every live node knows a leader for every group,
// and returns the per-node led-group counts.
func (c *fwdCluster) settled() ([]int, bool) {
	leads := make([]int, len(c.nodes))
	for i, n := range c.nodes {
		if n.closed.Load() {
			continue
		}
		for _, g := range n.app.RaftGroups {
			id, _ := g.LeaderInfo()
			if id == "" {
				return nil, false
			}
			if g.IsLeader() {
				leads[i]++
			}
		}
	}
	return leads, true
}

// waitLeaderOfAll waits until one live node leads every group and every
// live follower agrees, then returns that node's index.
func (c *fwdCluster) waitLeaderOfAll(timeout time.Duration) int {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if leads, ok := c.settled(); ok {
			for i, n := range leads {
				if n == len(c.nodes[i].app.RaftGroups) && n > 0 && c.followersAgree(i) {
					return i
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	c.t.Fatal("no single leader for every group")
	return -1
}

func (c *fwdCluster) followersAgree(leader int) bool {
	want := c.nodes[leader].server.URL
	for i, n := range c.nodes {
		if i == leader || n.closed.Load() {
			continue
		}
		for _, g := range n.app.RaftGroups {
			if g.LeaderHTTPAddr() != want && n.app.Config.Raft.PeerHTTPAddrs != nil {
				return false
			}
		}
	}
	return true
}

func (c *fwdCluster) others(leader int) []int {
	out := []int{}
	for i, n := range c.nodes {
		if i != leader && !n.closed.Load() {
			out = append(out, i)
		}
	}
	return out
}

type fwdResp struct {
	status int
	header http.Header
	raw    string
	body   map[string]any
}

var noRedirectClient = &http.Client{
	Timeout:       60 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

func (c *fwdCluster) call(node int, method, path, token string, body any, headers map[string]string) fwdResp {
	c.t.Helper()
	var reader io.Reader
	switch b := body.(type) {
	case nil:
	case []byte:
		reader = bytes.NewReader(b)
	default:
		raw, _ := json.Marshal(b)
		reader = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, c.nodes[node].server.URL+path, reader)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := noRedirectClient.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s on %s: %v", method, path, c.nodes[node].id, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	r := fwdResp{status: resp.StatusCode, header: resp.Header, raw: strings.TrimSpace(string(raw))}
	_ = json.Unmarshal(raw, &r.body)
	if r.status >= 300 && r.status < 400 || resp.Header.Get("Location") != "" {
		c.t.Fatalf("%s %s on %s surfaced a redirect: %d %v", method, path, c.nodes[node].id, r.status, resp.Header)
	}
	return r
}

func (c *fwdCluster) expect(r fwdResp, status int, errCode, label string) {
	c.t.Helper()
	if r.status != status {
		c.t.Fatalf("%s: status %d, want %d (body %s)", label, r.status, status, r.raw)
	}
	if errCode != "" && r.body["error"] != errCode {
		c.t.Fatalf("%s: error %v, want %q", label, r.body["error"], errCode)
	}
	if errCode == fwdUnavailable && r.header.Get(fwdRetryAfter) != "1" {
		c.t.Fatalf("%s: Retry-After = %q", label, r.header.Get(fwdRetryAfter))
	}
}

func (c *fwdCluster) create(node int, token, command string) string {
	c.t.Helper()
	r := c.call(node, http.MethodPost, pathTasks, token, map[string]any{keyCommand: command, keyPayload: map[string]any{"k": "v"}}, nil)
	c.expect(r, http.StatusAccepted, "", "create on "+c.nodes[node].id)
	id, _ := r.body["id"].(string)
	return id
}

func (c *fwdCluster) claim(node int, token string, wait int) fwdResp {
	c.t.Helper()
	return c.call(node, http.MethodPost, pathClaim, token, map[string]any{fwdLease: 60, "waitSeconds": wait}, nil)
}

func batchItems(r fwdResp, label string, t *testing.T) []map[string]any {
	t.Helper()
	list, _ := r.body[fwdResults].([]any)
	out := make([]map[string]any, 0, len(list))
	for _, v := range list {
		m, ok := v.(map[string]any)
		if !ok {
			t.Fatalf("%s: malformed item in %s", label, r.raw)
		}
		out = append(out, m)
	}
	return out
}

func TestLeaderForward_FollowerWritesBatchesAndClaims(t *testing.T) {
	c := startForwardCluster(t, fwdOptions{shards: 1, httpPeers: true, base: staticForwardConfig})
	leader := c.waitLeaderOfAll(10 * time.Second)
	f := c.others(leader)

	t.Run("single writes", func(t *testing.T) {
		c.t = t
		id := c.create(f[0], fwdToken, fwdCommand)
		c.expect(c.call(leader, http.MethodGet, pathTasks+"/"+id, fwdToken, nil, nil), http.StatusOK, "", "leader has task")
		r := c.claim(f[0], fwdToken, 0)
		c.expect(r, http.StatusOK, "", "claim via follower")
		if r.body["id"] != id {
			t.Fatalf("claimed %v, want %s", r.body["id"], id)
		}
		taskPath := pathTasks + "/" + id
		c.expect(c.call(f[0], http.MethodPost, taskPath+"/heartbeat", fwdToken, map[string]any{fwdExtend: 30}, nil), http.StatusOK, "", "heartbeat")
		c.expect(c.call(f[1], http.MethodPost, taskPath+"/nack", fwdToken, map[string]any{fwdDelay: 0}, nil), http.StatusOK, "", "nack")
		c.expect(c.claim(f[1], fwdToken, 5), http.StatusOK, "", "re-claim after nack backoff")
		c.expect(c.call(f[1], http.MethodPost, taskPath+"/abandon", fwdToken, nil, nil), http.StatusOK, "", "abandon")
		c.expect(c.claim(f[0], fwdToken, 0), http.StatusOK, "", "claim again")
		c.expect(c.call(f[0], http.MethodPost, taskPath+"/result", fwdToken, map[string]any{keyStatus: statusCompleted, keyResult: okResult}, nil), http.StatusOK, "", "result")
		c.expect(c.call(leader, http.MethodGet, taskPath+"/result", fwdToken, nil, nil), http.StatusOK, "", "leader has result")
	})

	t.Run("batches", func(t *testing.T) {
		c.t = t
		items := make([]any, 0, 3)
		for i := range 3 {
			items = append(items, map[string]any{keyCommand: fwdCommand, keyPayload: i})
		}
		r := c.call(f[0], http.MethodPost, fwdBatchPath, fwdToken, map[string]any{keyTasks: items}, nil)
		c.expect(r, http.StatusOK, "", "batch create via follower")
		for _, it := range batchItems(r, "batch create", t) {
			if it["error"] != nil || it["task"] == nil {
				t.Fatalf("batch create item failed through follower: %s", r.raw)
			}
		}
		r = c.call(f[1], http.MethodPost, fwdBatchClaim, fwdToken, map[string]any{"count": 3, fwdLease: 60}, nil)
		c.expect(r, http.StatusOK, "", "batch claim via follower")
		tasks, _ := r.body[keyTasks].([]any)
		if len(tasks) != 3 || r.body["error"] != nil {
			t.Fatalf("batch claim through follower: %s", r.raw)
		}
		results := make([]any, 0, len(tasks))
		for _, v := range tasks {
			results = append(results, map[string]any{fwdTaskID: v.(map[string]any)["id"], keyStatus: statusCompleted, keyResult: okResult})
		}
		r = c.call(f[0], http.MethodPost, fwdBatchResult, fwdToken, map[string]any{fwdResults: results}, nil)
		c.expect(r, http.StatusOK, "", "batch results via follower")
		for _, it := range batchItems(r, "batch results", t) {
			if it["error"] != nil {
				t.Fatalf("batch result item failed through follower: %s", r.raw)
			}
		}
	})

	t.Run("long-poll claim", func(t *testing.T) {
		c.t = t
		done := make(chan fwdResp, 1)
		start := time.Now()
		go func() { done <- c.claim(f[0], fwdToken, 5) }()
		time.Sleep(600 * time.Millisecond)
		id := c.create(f[1], fwdToken, fwdCommand)
		r := <-done
		c.expect(r, http.StatusOK, "", "long-poll claim via follower")
		if r.body["id"] != id || time.Since(start) >= 5*time.Second {
			t.Fatalf("long-poll returned %v after %s, want %s promptly", r.body["id"], time.Since(start), id)
		}
		c.expect(c.call(f[0], http.MethodPost, pathTasks+"/"+id+"/result", fwdToken, map[string]any{keyStatus: statusCompleted, keyResult: okResult}, nil), http.StatusOK, "", "complete")
	})

	t.Run("loop prevention and authentication", func(t *testing.T) {
		c.t = t
		forwarded := map[string]string{leaderforward.HeaderForwarded: "1"}
		body := map[string]any{keyCommand: fwdCommand, keyPayload: 1}
		for _, path := range []string{pathTasks, fwdBatchPath, pathClaim} {
			c.expect(c.call(f[0], http.MethodPost, path, fwdToken, body, forwarded), http.StatusServiceUnavailable, fwdUnavailable, "re-forward "+path)
		}
		c.expect(c.call(leader, http.MethodPost, pathTasks, fwdToken, body, forwarded), http.StatusAccepted, "", "header on leader")
		c.expect(c.call(leader, http.MethodPost, pathTasks, "wrong", body, forwarded), http.StatusUnauthorized, "", "header carries no authority")
		c.expect(c.call(f[0], http.MethodPost, pathTasks, "wrong", body, nil), http.StatusUnauthorized, "", "follower authenticates first")
		c.expect(c.call(f[0], http.MethodPost, pathTasks, "", body, nil), http.StatusUnauthorized, "", "no token")
	})

	t.Run("body limit", func(t *testing.T) {
		c.t = t
		big := []byte(`{"command":"` + fwdCommand + `","payload":"` + strings.Repeat("x", int(leaderforward.MaxBodyBytes)) + `"}`)
		c.expect(c.call(f[0], http.MethodPost, pathTasks, fwdToken, big, nil), http.StatusRequestEntityTooLarge, leaderforward.CodeRequestTooLarge, "oversize forward")
		// The leader applies no new limit: it reads the whole body and
		// answers with its usual validation (no command), not 413. A valid
		// 16 MiB task is not replicated here to keep 50 ms test elections
		// stable under -race.
		noCommand := []byte(`{"payload":"` + strings.Repeat("x", int(leaderforward.MaxBodyBytes)) + `"}`)
		c.expect(c.call(leader, http.MethodPost, pathTasks, fwdToken, noCommand, nil), http.StatusBadRequest, "invalid body", "leader reads large bodies")
	})

	if got := scrapeForward(t, pathTasks, "ok"); got < 1 {
		t.Fatalf("codeq_leader_forward_total{route=%q,result=\"ok\"} = %v", pathTasks, got)
	}
	if got := scrapeForward(t, pathTasks, "loop"); got < 1 {
		t.Fatalf("codeq_leader_forward_total{route=%q,result=\"loop\"} = %v", pathTasks, got)
	}
}

func scrapeForward(t *testing.T, route, result string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	prefix := fmt.Sprintf("codeq_leader_forward_total{result=%q,route=%q} ", result, route)
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if v, ok := strings.CutPrefix(line, prefix); ok {
			n, _ := strconv.ParseFloat(v, 64)
			return n
		}
	}
	return 0
}

func TestLeaderForward_LeaderLossMidRequestIsRetryable(t *testing.T) {
	c := startForwardCluster(t, fwdOptions{shards: 1, httpPeers: true, base: staticForwardConfig})
	leader := c.waitLeaderOfAll(10 * time.Second)
	f := c.others(leader)

	done := make(chan fwdResp, 1)
	start := time.Now()
	go func() { done <- c.claim(f[0], fwdToken, 8) }()
	time.Sleep(700 * time.Millisecond) // the forwarded long-poll is now waiting on the leader
	c.nodes[leader].stop()
	r := <-done
	c.expect(r, http.StatusServiceUnavailable, fwdUnavailable, "leader lost mid-request")
	if time.Since(start) >= 8*time.Second {
		t.Fatalf("follower waited %s instead of failing fast", time.Since(start))
	}

	// The survivors elect a new leader; a client retry through either
	// survivor then succeeds, still without any 307.
	deadline := time.Now().Add(10 * time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		node := f[attempt%len(f)]
		r = c.call(node, http.MethodPost, pathTasks, fwdToken, map[string]any{keyCommand: fwdCommand, keyPayload: attempt}, nil)
		if r.status == http.StatusAccepted {
			return
		}
		c.expect(r, http.StatusServiceUnavailable, fwdUnavailable, "retry during election")
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no write accepted after failover (last %d %s)", r.status, r.raw)
}

func TestLeaderForward_UnconfiguredPeersAreRefused(t *testing.T) {
	c := startForwardCluster(t, fwdOptions{shards: 1, httpPeers: false, base: staticForwardConfig})
	leader := c.waitLeaderOfAll(10 * time.Second)
	f := c.others(leader)
	body := map[string]any{keyCommand: fwdCommand, keyPayload: 1}
	c.expect(c.call(f[0], http.MethodPost, pathTasks, fwdToken, body, nil), http.StatusServiceUnavailable, fwdUnavailable, "single write")
	c.expect(c.call(f[0], http.MethodPost, fwdBatchPath, fwdToken, map[string]any{keyTasks: []any{body, body}}, nil), http.StatusServiceUnavailable, fwdUnavailable, "batch write")
	c.expect(c.claim(f[1], fwdToken, 1), http.StatusServiceUnavailable, fwdUnavailable, "claim")
	stats := c.call(leader, http.MethodGet, "/v1/codeq/admin/queues/"+fwdCommand, fwdToken, nil, nil)
	c.expect(stats, http.StatusOK, "", "queue stats")
	if stats.body["ready"] != float64(0) {
		t.Fatalf("refused follower writes enqueued tasks: %s", stats.raw)
	}
}

// TestLeaderForward_MuxMultiShard covers RAFT_MUX_ENABLED with four Raft
// groups. With one leader for every group a follower forwards whole
// batches; after failover each survivor keeps the legacy per-item batch
// semantics for the groups it leads, and a node leading no group either
// forwards (one leader for all) or answers 503, never per-item failure.
func TestLeaderForward_MuxMultiShard(t *testing.T) {
	const shards = 4
	c := startForwardCluster(t, fwdOptions{shards: shards, httpPeers: true, base: staticForwardConfig})
	leader := c.waitLeaderOfAll(15 * time.Second)
	items := make([]any, 0, 8)
	for i := range 8 {
		items = append(items, map[string]any{keyCommand: fwdCommand, keyPayload: i})
	}
	for _, f := range c.others(leader) {
		r := c.call(f, http.MethodPost, fwdBatchPath, fwdToken, map[string]any{keyTasks: items}, nil)
		c.expect(r, http.StatusOK, "", "multi-group batch via follower")
		for _, it := range batchItems(r, "multi-group batch", t) {
			if it["error"] != nil {
				t.Fatalf("whole-batch forward produced a per-item failure: %s", r.raw)
			}
		}
	}

	c.nodes[leader].stop()
	deadline := time.Now().Add(15 * time.Second)
	var leads []int
	for ok := false; !ok && time.Now().Before(deadline); {
		time.Sleep(50 * time.Millisecond)
		leads, ok = c.settled()
	}
	if leads == nil {
		t.Fatal("groups did not re-elect")
	}
	t.Logf("groups led per node after failover: %v", leads)
	for _, n := range c.others(leader) {
		r := c.call(n, http.MethodPost, fwdBatchPath, fwdToken, map[string]any{keyTasks: items}, nil)
		t.Logf("%s (leads %d): batch -> %d", c.nodes[n].id, leads[n], r.status)
		switch {
		case leads[n] > 0:
			c.expect(r, http.StatusOK, "", "partial leader keeps per-item semantics")
			for _, it := range batchItems(r, "partial leader", t) {
				if e, _ := it["error"].(string); it["task"] == nil && !strings.Contains(e, "not leader") {
					t.Fatalf("unexpected per-item error: %s", r.raw)
				}
			}
		case r.status == http.StatusOK:
			for _, it := range batchItems(r, "forwarded", t) {
				if it["error"] != nil {
					t.Fatalf("forwarded batch item failed: %s", r.raw)
				}
			}
		default:
			c.expect(r, http.StatusServiceUnavailable, fwdUnavailable, "no group led here and no single leader")
		}
	}
}

// TestLeaderForward_BindingTokensReauthorizedOnLeader proves the leader
// re-runs C1 for forwarded binding-scoped tokens: every refusal that only
// the controller can decide is still returned through a follower, and the
// forwarding header grants nothing.
func TestLeaderForward_BindingTokensReauthorizedOnLeader(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa: %v", err)
	}
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := base64.RawURLEncoding.EncodeToString(key.N.Bytes())
		_, _ = fmt.Fprintf(w, `{"keys":[{"kty":"RSA","kid":%q,"n":%q,"e":"AQAB"}]}`, bindingKid, n)
	}))
	t.Cleanup(jwks.Close)
	s := &bindingSuite{t: t, key: key}
	c := startForwardCluster(t, fwdOptions{shards: 1, httpPeers: true, base: func(t *testing.T) *config.Config {
		s.t = t
		return s.config(jwks.URL)
	}})
	leader := c.waitLeaderOfAll(10 * time.Second)
	f := c.others(leader)
	publish := s.bindingToken(policyPublish, tenantConveste, bindingTopic, "pod-1", nil)
	subscribe := s.bindingToken(policySubscribe, tenantConveste, bindingTopic, "pod-1", nil)
	otherTenant := s.bindingToken(policySubscribe, "other", bindingTopic, "pod-1", nil)

	id := c.create(f[0], publish, bindingTopic)
	r := c.call(f[0], http.MethodPost, pathTasks, publish, map[string]any{keyCommand: fwdOtherTopic, keyPayload: 1}, nil)
	c.expect(r, http.StatusForbidden, "event_type_not_allowed", "other command via follower")
	r = c.call(f[0], http.MethodPost, pathTasks, publish, map[string]any{keyCommand: bindingTopic, keyPayload: 1, keyWebhook: "http://169.254.169.254/"}, nil)
	c.expect(r, http.StatusForbidden, "webhook_not_allowed", "webhook via follower")
	r = c.call(f[1], http.MethodPost, fwdBatchPath, publish, map[string]any{keyTasks: []any{
		map[string]any{keyCommand: bindingTopic, keyPayload: 1}, map[string]any{keyCommand: "other", keyPayload: 1},
	}}, nil)
	c.expect(r, http.StatusForbidden, "event_type_not_allowed", "batch via follower")
	if r.body["index"] != float64(1) {
		t.Fatalf("batch index = %v", r.body["index"])
	}
	c.expect(c.call(f[0], http.MethodGet, "/v1/codeq/admin/queues", publish, nil, nil), http.StatusForbidden, "route_not_allowed", "admin via follower")
	c.expect(c.call(f[0], http.MethodPost, pathClaim, publish, map[string]any{}, nil), http.StatusUnauthorized, "", "publish token claims")

	r = c.claim(f[1], subscribe, 0)
	c.expect(r, http.StatusOK, "", "subscribe claim via follower")
	if r.body["id"] != id || r.body["tenantId"] != tenantConveste {
		t.Fatalf("claimed %s", r.raw)
	}
	hb := pathTasks + "/" + id + "/heartbeat"
	c.expect(c.call(f[0], http.MethodPost, hb, otherTenant, map[string]any{}, nil), http.StatusForbidden, "not-owner", "cross-tenant heartbeat via follower")
	c.expect(c.call(leader, http.MethodPost, hb, otherTenant, map[string]any{}, map[string]string{leaderforward.HeaderForwarded: "1"}), http.StatusForbidden, "not-owner", "forged forward header on leader")
	c.expect(c.call(f[0], http.MethodPost, hb, subscribe, map[string]any{fwdExtend: 30}, nil), http.StatusOK, "", "owner heartbeat via follower")
	c.expect(c.call(f[1], http.MethodPost, pathTasks+"/"+id+"/result", subscribe, map[string]any{keyStatus: statusCompleted, keyResult: okResult}, nil), http.StatusOK, "", "owner result via follower")
}
