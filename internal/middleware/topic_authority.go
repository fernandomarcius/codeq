package middleware

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/osvaldoandrade/codeq/pkg/auth"
	"github.com/osvaldoandrade/codeq/pkg/config"
)

const topicManageScope = "codeq:topics:manage"

var topicAuthorityHTTP = &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

func topicRoute(c *gin.Context) bool {
	switch c.FullPath() {
	case "/v1/codeq/admin/topics/:topicName":
		return c.Request.Method == http.MethodGet || c.Request.Method == http.MethodPut || c.Request.Method == http.MethodDelete
	case "/v1/codeq/admin/queues/:command":
		return c.Request.Method == http.MethodGet
	default:
		return false
	}
}

func authorizeTopicController(c *gin.Context, cfg *config.Config, claims *auth.Claims, tenantID string) bool {
	if cfg == nil || !topicRoute(c) || len(claims.Scopes) != 1 || len(claims.EventTypes) != 0 {
		return false
	}
	epoch, ok := claims.Raw["tenant_epoch"].(string)
	if !ok || len(epoch) != 64 {
		return false
	}
	headers := c.Request.Header.Values("Authorization")
	if len(headers) != 1 || !strings.HasPrefix(headers[0], "Bearer ") {
		return false
	}
	if !currentTopicProof(c.Request.Context(), cfg.TopicControllerAuthorityURL, headers[0], tenantID, epoch) {
		return false
	}
	c.Set("topicControllerAuthorized", true)
	return true
}

func currentTopicProof(ctx context.Context, raw, bearer, tenantID, epoch string) bool {
	if !validTopicAuthorityURL(raw) {
		return false
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return false
	}
	request.Header.Set("Authorization", bearer)
	response, err := topicAuthorityHTTP.Do(request)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return false
	}
	b, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(b) > 4096 {
		return false
	}
	return validTopicProofBody(b, tenantID, epoch)
}

func validTopicProofBody(b []byte, tenantID, epoch string) bool {
	var proof struct {
		SchemaVersion string `json:"schemaVersion"`
		TenantID      string `json:"tenantId"`
		TenantEpoch   string `json:"tenantEpoch"`
		Active        bool   `json:"active"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(b)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&proof) != nil {
		return false
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return false
	}
	if proof.SchemaVersion != "codeq-topic-authority/v1" || !proof.Active || proof.TenantID != tenantID || proof.TenantEpoch != epoch {
		return false
	}
	return true
}

func validTopicAuthorityURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && validTopicAuthorityOrigin(u) && validTopicAuthorityPath(u)
}

func validTopicAuthorityOrigin(u *url.URL) bool {
	return u.Host != "" && u.User == nil && (u.Scheme == "https" || u.Scheme == "http" && u.Host == "tikti.codecloud-identity.svc.cluster.local")
}

func validTopicAuthorityPath(u *url.URL) bool {
	return u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" && u.RawPath == "" && u.Path == "/v1/internal/codeq/topic-authority"
}

// RequireTopicAdmin accepts a live narrow topic proof or the legacy administrator authority.
func RequireTopicAdmin() gin.HandlerFunc {
	legacy := RequireAdmin()
	return func(c *gin.Context) {
		if authorized, ok := c.Get("topicControllerAuthorized"); ok && authorized == true {
			c.Next()
			return
		}
		legacy(c)
	}
}
