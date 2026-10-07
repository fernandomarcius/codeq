package topics

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/osvaldoandrade/codeq/internal/core/queuetopic"
	"github.com/osvaldoandrade/codeq/internal/leaderforward"
)

type fakeService struct {
	topic   queuetopic.Topic
	err     error
	created bool
	changed bool
}

const testPhysicalTopicID = "payments.events"

func (s *fakeService) Upsert(_ context.Context, tenantID, topicName string, policy queuetopic.Policy) (queuetopic.Topic, bool, bool, error) {
	s.topic = queuetopic.Topic{TopicID: queuetopic.PhysicalID(tenantID, topicName), TenantID: tenantID, TopicName: topicName, Policy: policy}
	return s.topic, s.created, s.changed, s.err
}

func (s *fakeService) Get(_ context.Context, tenantID, topicName string) (queuetopic.Topic, error) {
	if s.err != nil {
		return queuetopic.Topic{}, s.err
	}
	return queuetopic.Topic{TopicID: queuetopic.PhysicalID(tenantID, topicName), TenantID: tenantID, TopicName: topicName}, nil
}

func (s *fakeService) Delete(_ context.Context, tenantID, topicName string) error {
	s.topic = queuetopic.Topic{TenantID: tenantID, TopicName: topicName}
	return s.err
}

func TestHandlerUpsert(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		service *fakeService
		status  int
	}{
		{name: "create", body: validBody, service: &fakeService{created: true}, status: http.StatusCreated},
		{name: "update", body: validBody, service: &fakeService{changed: true}, status: http.StatusOK},
		{name: "malformed", body: `{`, service: &fakeService{}, status: http.StatusBadRequest},
		{name: "unknown field", body: `{"priorityTiers":[0],"maxAttempts":1,"deadLetterTopicRef":"events-dlq","extra":true}`, service: &fakeService{}, status: http.StatusBadRequest},
		{name: "multiple objects", body: validBody + `{}`, service: &fakeService{}, status: http.StatusBadRequest},
		{name: "validation", body: validBody, service: &fakeService{err: &queuetopic.ValidationError{Field: "topicName", Message: "invalid"}}, status: http.StatusUnprocessableEntity},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := request(t, tt.service, http.MethodPut, "/v1/codeq/admin/topics/events", tt.body)
			if response.Code != tt.status {
				t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
			}
			if tt.status < 300 && tt.service.topic.TopicID != testPhysicalTopicID {
				t.Fatalf("topic = %#v", tt.service.topic)
			}
		})
	}
}

func TestHandlerGetErrorMapping(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
	}{
		{name: "success", status: http.StatusOK},
		{name: "not found", err: &queuetopic.NotFoundError{TopicID: testPhysicalTopicID}, status: http.StatusNotFound},
		{name: "conflict", err: &queuetopic.ConflictError{TopicID: testPhysicalTopicID}, status: http.StatusConflict},
		{name: "unavailable", err: &queuetopic.UnavailableError{Reason: "raft"}, status: http.StatusServiceUnavailable},
		{name: "internal", err: errors.New("backend"), status: http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := request(t, &fakeService{err: tt.err}, http.MethodGet, "/v1/codeq/admin/topics/events", "")
			if response.Code != tt.status {
				t.Fatalf("status = %d, body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestHandlerDeleteRequiresExplicitPolicy(t *testing.T) {
	service := &fakeService{}
	response := request(t, service, http.MethodDelete, "/v1/codeq/admin/topics/events", "")
	if response.Code != http.StatusBadRequest {
		t.Fatalf("missing policy status = %d", response.Code)
	}

	response = request(t, service, http.MethodDelete, "/v1/codeq/admin/topics/events?deletionPolicy=Delete", "")
	if response.Code != http.StatusNoContent || service.topic.TopicName != "events" {
		t.Fatalf("delete status = %d topic=%#v", response.Code, service.topic)
	}

	response = request(t, &fakeService{err: errors.New("backend")}, http.MethodDelete, "/v1/codeq/admin/topics/events?deletionPolicy=Delete", "")
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("delete error status = %d", response.Code)
	}
}

func TestHandlerFollowerWritesNeverRedirect(t *testing.T) {
	for name, url := range map[string]string{"known leader": "http://leader:8080/", "unknown leader": ""} {
		response := request(t, &fakeService{err: &leaderError{url: url}}, http.MethodPut, "/v1/codeq/admin/topics/events?trace=1", validBody)
		if response.Code != http.StatusServiceUnavailable || response.Header().Get("Location") != "" {
			t.Fatalf("%s: status = %d location=%q body=%s", name, response.Code, response.Header().Get("Location"), response.Body.String())
		}
		if response.Header().Get("Retry-After") != "1" || !strings.Contains(response.Body.String(), leaderforward.CodeLeaderUnavailable) {
			t.Fatalf("%s: not a retryable leader_unavailable: %v %s", name, response.Header(), response.Body.String())
		}
	}
}

func TestHandlerForwardsFollowerWritesToConfiguredLeader(t *testing.T) {
	type seen struct{ method, uri, body, forwarded, auth string }
	got := make(chan seen, 1)
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- seen{r.Method, r.URL.RequestURI(), string(b), r.Header.Get(leaderforward.HeaderForwarded), r.Header.Get("Authorization")}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"topicId":"payments.events"}`))
	}))
	defer leader.Close()
	fwd := leaderforward.New(leaderforward.Config{
		PeerHTTPAddrs: map[string]string{"node-1": "http://self.invalid:8080", "node-2": leader.URL},
		SelfID:        "node-1",
	})
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(func(c *gin.Context) { c.Set("tenantID", "payments"); c.Next() })
	handler := NewHandler(&fakeService{err: &leaderError{url: leader.URL}})
	engine.PUT("/v1/codeq/admin/topics/:topicName", fwd.Single(), handler.Upsert)
	req := httptest.NewRequest(http.MethodPut, "/v1/codeq/admin/topics/events?trace=1", strings.NewReader(validBody))
	req.Header.Set("Authorization", "Bearer topic-controller")
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, req)
	if response.Code != http.StatusCreated || response.Body.String() != `{"topicId":"payments.events"}` {
		t.Fatalf("relayed status = %d body=%s", response.Code, response.Body.String())
	}
	s := <-got
	want := seen{http.MethodPut, "/v1/codeq/admin/topics/events?trace=1", validBody, "1", "Bearer topic-controller"}
	if s != want {
		t.Fatalf("leader saw %+v, want %+v", s, want)
	}
}

type leaderError struct{ url string }

func (e *leaderError) Error() string          { return "not leader" }
func (e *leaderError) LeaderHTTPAddr() string { return e.url }

const validBody = `{"priorityTiers":[0,3,5],"maxAttempts":5,"deadLetterTopicRef":"events-dlq","retentionSeconds":3600,"maxConsumers":20}`

func request(t *testing.T, service Service, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(func(c *gin.Context) {
		c.Set("tenantID", "payments")
		c.Next()
	})
	handler := NewHandler(service)
	engine.PUT("/v1/codeq/admin/topics/:topicName", handler.Upsert)
	engine.GET("/v1/codeq/admin/topics/:topicName", handler.Get)
	engine.DELETE("/v1/codeq/admin/topics/:topicName", handler.Delete)
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, req)
	return response
}
