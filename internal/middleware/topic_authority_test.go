package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/osvaldoandrade/codeq/pkg/auth"
	"github.com/osvaldoandrade/codeq/pkg/config"
)

type narrowValidator struct{}

func (narrowValidator) Validate(string) (*auth.Claims, error) {
	return &auth.Claims{Subject: "controller", Scopes: []string{"codeq:topics:manage"}, Raw: map[string]any{"tid": testTenantPayments, "tenant_epoch": "epoch"}}, nil
}

func TestTopicControllerTokenCannotUseTaskOrWorkerAPI(t *testing.T) {
	cfg := &config.Config{AllowProducerAsWorker: true}
	for name, mw := range map[string]gin.HandlerFunc{"producer": AuthMiddleware(narrowValidator{}, cfg), "reader": AnyAuthMiddleware(nil, narrowValidator{}, cfg), "worker": WorkerAuthMiddleware(narrowValidator{}, narrowValidator{}, cfg)} {
		t.Run(name, func(t *testing.T) {
			e := gin.New()
			e.POST("/v1/codeq/tasks", mw, func(c *gin.Context) { c.Status(204) })
			r := httptest.NewRequest(http.MethodPost, "/v1/codeq/tasks", nil)
			r.Header.Set("Authorization", "Bearer test-only")
			w := httptest.NewRecorder()
			e.ServeHTTP(w, r)
			if w.Code == 204 {
				t.Fatal("topic token reached task handler")
			}
		})
	}
}

type topicProofValidator struct{}

func (topicProofValidator) Validate(string) (*auth.Claims, error) {
	return &auth.Claims{Subject: "controller", Scopes: []string{topicManageScope}, Raw: map[string]any{"tid": testTenantPayments, "tenant_epoch": strings.Repeat("a", 64)}}, nil
}

func TestTopicControllerChecksTenantLifetimeOnEveryUse(t *testing.T) {
	tenant, epoch, active := testTenantPayments, strings.Repeat("a", 64), true
	authority := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/internal/codeq/topic-authority" || r.Header.Get("Authorization") != "Bearer test-only" {
			w.WriteHeader(403)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"schemaVersion": "codeq-topic-authority/v1", "tenantId": tenant, "tenantEpoch": epoch, "active": active})
	}))
	defer authority.Close()
	old := topicAuthorityHTTP
	topicAuthorityHTTP = authority.Client()
	defer func() { topicAuthorityHTTP = old }()
	cfg := &config.Config{TopicControllerAuthorityURL: authority.URL + "/v1/internal/codeq/topic-authority"}
	e := gin.New()
	e.GET("/v1/codeq/admin/topics/:topicName", AuthMiddleware(topicProofValidator{}, cfg), RequireTopicAdmin(), func(c *gin.Context) { c.Status(204) })
	request := func() int {
		r := httptest.NewRequest("GET", "/v1/codeq/admin/topics/jobs", nil)
		r.Header.Set("Authorization", "Bearer test-only")
		w := httptest.NewRecorder()
		e.ServeHTTP(w, r)
		return w.Code
	}
	if got := request(); got != 204 {
		t.Fatalf("valid proof: %d", got)
	}
	epoch = strings.Repeat("b", 64)
	if request() != 403 {
		t.Fatal("stale epoch accepted")
	}
	epoch = strings.Repeat("a", 64)
	tenant = "foreign"
	if request() != 403 {
		t.Fatal("cross tenant accepted")
	}
	tenant = testTenantPayments
	active = false
	if request() != 403 {
		t.Fatal("retired tenant accepted")
	}
	cfg.TopicControllerAuthorityURL = ""
	if request() != 403 {
		t.Fatal("disabled authority accepted")
	}
}
