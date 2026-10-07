package app

import (
	"context"
	"net/http"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/osvaldoandrade/codeq/internal/producer/producerpb"
)

// The idempotency e2e suite reproduces the cross-repository review probe on
// the production router: an idempotency key must never return another
// tenant's task, for any token kind, and binding keys are namespaced per
// tenant and event type.

const (
	idemOtherTenant  = "othertenant"
	idemLocalTenant  = "local-tenant"
	idemKey          = "order-42"
	idemTopicB       = "cflow-notificar"
	keyIdempotency   = "idempotencyKey"
	keyID            = "id"
	keyTenantID      = "tenantId"
	keyResults       = "results"
	keyTask          = "task"
	pathTasksBatch   = pathTasks + "/batch"
	keyOrder         = "order"
	idemConflictBody = `{"error":"idempotency_conflict"}`
)

func (s *bindingSuite) createWithKey(token, command, key string, payload any) apiResult {
	s.t.Helper()
	return s.call(http.MethodPost, pathTasks, token, map[string]any{keyCommand: command, keyPayload: payload, keyIdempotency: key})
}

func (s *bindingSuite) expectConflictNoBody(r apiResult, label string) {
	s.t.Helper()
	if r.status != http.StatusConflict || r.raw != idemConflictBody {
		s.t.Fatalf("%s: status %d body %s, want 409 %s", label, r.status, r.raw, idemConflictBody)
	}
}

func TestIdempotencyScopeE2E(t *testing.T) {
	s := newBindingSuite(t)
	t.Run("review probe", func(t *testing.T) { s.t = t; s.idempotencyReviewProbe() })
	t.Run("binding namespaces", func(t *testing.T) { s.t = t; s.idempotencyBindingNamespaces() })
	t.Run("batch", func(t *testing.T) { s.t = t; s.idempotencyBatch() })
	t.Run("producer stream", func(t *testing.T) { s.t = t; s.idempotencyProducerStream() })
}

func (s *bindingSuite) idempotencyReviewProbe() {
	secret := map[string]any{keyOrder: "secret-local"}
	first := s.createWithKey(staticProducerTok, bindingTopic, idemKey, secret)
	s.expect(first, http.StatusAccepted, "", "static local-tenant create")
	localID, _ := first.body[keyID].(string)

	// The reviewer's request: a Publish binding token of another tenant with
	// the same key. Its key is namespaced, so it can only create its own task.
	probe := s.createWithKey(s.bindingToken(policyPublish, idemOtherTenant, bindingTopic, "pod-probe", nil), bindingTopic, idemKey, 1)
	s.expect(probe, http.StatusAccepted, "", "binding other tenant")
	if probe.body[keyID] == localID || probe.body[keyTenantID] != idemOtherTenant {
		s.t.Fatalf("binding token received a foreign task: %s", probe.raw)
	}

	// Un-namespaced (legacy) keys of another tenant: 409 and no task body.
	s.expectConflictNoBody(s.createWithKey(s.adminToken(idemOtherTenant), bindingTopic, idemKey, 1), "cross-tenant admin token")
	s.expectConflictNoBody(s.createWithKey(s.adminToken(tenantConveste), "other-command", idemKey, 1), "cross-tenant admin, other command")

	// Same tenant: the idempotent replay is unchanged.
	again := s.createWithKey(staticProducerTok, bindingTopic, idemKey, map[string]any{keyOrder: "changed"})
	s.expect(again, http.StatusAccepted, "", "same-tenant replay")
	if again.body[keyID] != localID {
		s.t.Fatalf("same-tenant replay returned %v, want %s", again.body[keyID], localID)
	}
	replayAdmin := s.createWithKey(s.adminToken(idemLocalTenant), bindingTopic, idemKey, 2)
	s.expect(replayAdmin, http.StatusAccepted, "", "same-tenant replay by another token kind")
	if replayAdmin.body[keyID] != localID {
		s.t.Fatalf("same-tenant admin replay returned %v, want %s", replayAdmin.body[keyID], localID)
	}

	// A NUL in a client key could forge a binding namespace: refused for all.
	forged := s.createWithKey(staticProducerTok, bindingTopic, tenantConveste+"\x00"+bindingTopic+"\x00"+idemKey, 1)
	s.expect(forged, http.StatusBadRequest, "invalid 'idempotencyKey'", "NUL in key")
}

func (s *bindingSuite) idempotencyBindingNamespaces() {
	topicA := s.bindingToken(policyPublish, tenantConveste, bindingTopic, "pod-a", nil)
	topicASibling := s.bindingToken(policyPublish, tenantConveste, bindingTopic, "pod-a2", nil)
	topicB := s.bindingToken(policyPublish, tenantConveste, idemTopicB, "pod-b", nil)
	const key = "shared-key"

	a1 := s.createWithKey(topicA, bindingTopic, key, 1)
	s.expect(a1, http.StatusAccepted, "", "topic A create")
	a2 := s.createWithKey(topicASibling, bindingTopic, key, 2)
	s.expect(a2, http.StatusAccepted, "", "topic A replay by another Pod")
	if a1.body[keyID] != a2.body[keyID] {
		s.t.Fatalf("binding replay within one topic created a new task: %v vs %v", a1.body[keyID], a2.body[keyID])
	}
	b := s.createWithKey(topicB, idemTopicB, key, 3)
	s.expect(b, http.StatusAccepted, "", "topic B same key")
	if b.body[keyID] == a1.body[keyID] || b.body[keyCommand] != idemTopicB {
		s.t.Fatalf("two topics of one tenant collided: %s", b.raw)
	}
	legacy := s.createWithKey(s.adminToken(tenantConveste), bindingTopic, key, 4)
	s.expect(legacy, http.StatusAccepted, "", "legacy key of the same tenant")
	if legacy.body[keyID] == a1.body[keyID] {
		s.t.Fatalf("legacy key collided with a binding namespace: %s", legacy.raw)
	}
}

func (s *bindingSuite) idempotencyBatch() {
	const taken, fresh = "batch-taken", "batch-fresh"
	s.expect(s.createWithKey(staticProducerTok, bindingTopic, taken, map[string]any{keyOrder: "secret-batch"}), http.StatusAccepted, "", "seed")
	item := func(key string) map[string]any {
		return map[string]any{keyCommand: bindingTopic, keyPayload: 1, keyIdempotency: key}
	}
	r := s.call(http.MethodPost, pathTasksBatch, s.adminToken(idemOtherTenant), map[string]any{keyTasks: []any{item(taken), item(fresh)}})
	s.expect(r, http.StatusOK, "", "cross-tenant batch")
	results, _ := r.body[keyResults].([]any)
	if len(results) != 2 {
		s.t.Fatalf("batch results: %s", r.raw)
	}
	conflict, _ := results[0].(map[string]any)
	if conflict["error"] != "idempotency_conflict" || conflict[keyTask] != nil {
		s.t.Fatalf("cross-tenant batch item leaked: %v", conflict)
	}
	created, _ := results[1].(map[string]any)
	if task, _ := created[keyTask].(map[string]any); task == nil || task[keyTenantID] != idemOtherTenant {
		s.t.Fatalf("fresh batch item not created for its own tenant: %v", created)
	}

	publish := s.bindingToken(policyPublish, tenantConveste, bindingTopic, "pod-batch", nil)
	r = s.call(http.MethodPost, pathTasksBatch, publish, map[string]any{keyTasks: []any{item(taken), item(taken)}})
	s.expect(r, http.StatusOK, "", "binding batch")
	results, _ = r.body[keyResults].([]any)
	if len(results) != 2 {
		s.t.Fatalf("binding batch results: %s", r.raw)
	}
	first, _ := results[0].(map[string]any)[keyTask].(map[string]any)
	second, _ := results[1].(map[string]any)[keyTask].(map[string]any)
	if first == nil || second == nil || first[keyID] != second[keyID] || first[keyTenantID] != tenantConveste {
		s.t.Fatalf("binding batch replay not namespaced and idempotent: %s", r.raw)
	}
}

// streamCreate opens a producer stream with token and sends one create.
func (s *bindingSuite) streamCreate(token, key string) *producerpb.CreateAck {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(s.prodAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.t.Fatalf("dial producer: %v", err)
	}
	defer conn.Close()
	stream, err := producerpb.NewProducerStreamClient(conn).Stream(ctx)
	if err != nil {
		s.t.Fatalf("producer stream: %v", err)
	}
	_ = stream.Send(&producerpb.ProducerEvent{Event: &producerpb.ProducerEvent_Hello{Hello: &producerpb.Hello{Token: token}}})
	if _, err := stream.Recv(); err != nil {
		s.t.Fatalf("producer hello: %v", err)
	}
	create := &producerpb.CreateTask{Seq: 1, Command: bindingTopic, Payload: []byte(`1`), IdempotencyKey: key}
	_ = stream.Send(&producerpb.ProducerEvent{Event: &producerpb.ProducerEvent_Create{Create: create}})
	ev, err := stream.Recv()
	if err != nil || ev.GetCreateAck() == nil {
		s.t.Fatalf("producer create ack: %v %v", ev, err)
	}
	return ev.GetCreateAck()
}

func (s *bindingSuite) idempotencyProducerStream() {
	const key = "stream-key"
	seed := s.createWithKey(staticProducerTok, bindingTopic, key, map[string]any{keyOrder: "secret-stream"})
	s.expect(seed, http.StatusAccepted, "", "seed")
	if ack := s.streamCreate(staticProducerTok, key); !ack.Ok || ack.TaskId != seed.body[keyID] {
		s.t.Fatalf("same-tenant stream replay: %v, want %v", ack, seed.body[keyID])
	}
	if ack := s.streamCreate(s.adminToken(idemOtherTenant), key); ack.Ok || ack.TaskId != "" || ack.ErrorMessage != "idempotency_conflict" {
		s.t.Fatalf("cross-tenant stream create: %v, want idempotency_conflict and no task ID", ack)
	}
	if ack := s.streamCreate(staticProducerTok, "a\x00b"); ack.Ok || ack.ErrorMessage != "invalid 'idempotencyKey'" {
		s.t.Fatalf("NUL key on stream: %v", ack)
	}
}
