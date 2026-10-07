package authclaims

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/osvaldoandrade/codeq/pkg/auth"
)

var bindingTestNow = time.Unix(1_800_000_000, 0)

const (
	testTenant  = "conveste"
	testTopic   = "cflow-executar"
	testTopicID = testTenant + "." + testTopic
)

const workerScopeString = "codeq:abandon codeq:claim codeq:heartbeat codeq:nack codeq:result"

// bindingClaims builds validated claims the way the JWKS validator does:
// parsed fields plus the raw map.
func bindingClaims(policy BindingPolicy) *auth.Claims {
	aud, scope := BindingProducerAudience, PublishScope
	if policy == PolicySubscribe {
		aud, scope = BindingWorkerAudience, workerScopeString
	}
	raw := map[string]interface{}{
		"iss":         "https://tikti.example",
		"aud":         aud,
		"sub":         "codefoundry:workload:conveste-hostgator:workload-conveste:cflow:6f9a",
		"tid":         testTenant,
		"scope":       scope,
		"eventTypes":  []interface{}{testTopic},
		"cluster_ref": "conveste-hostgator",
		BindingClaim: map[string]interface{}{
			"uid": "7b1f5c2e-1111-4222-8333-944445555666", "generation": float64(3),
			"policy": string(policy), "topicId": testTopicID,
		},
	}
	return claimsFromRaw(raw)
}

func claimsFromRaw(raw map[string]interface{}) *auth.Claims {
	claims := &auth.Claims{Raw: raw, IssuedAt: bindingTestNow.Add(-10 * time.Second), ExpiresAt: bindingTestNow.Add(290 * time.Second)}
	claims.Subject, _ = raw["sub"].(string)
	switch aud := raw["aud"].(type) {
	case string:
		claims.Audience = []string{aud}
	case []interface{}:
		for _, a := range aud {
			if s, ok := a.(string); ok {
				claims.Audience = append(claims.Audience, s)
			}
		}
	}
	if scope, ok := raw["scope"].(string); ok {
		claims.Scopes = strings.Fields(scope)
	}
	if events, ok := raw["eventTypes"].([]interface{}); ok {
		for _, e := range events {
			if s, ok := e.(string); ok {
				claims.EventTypes = append(claims.EventTypes, s)
			}
		}
	}
	return claims
}

func mutate(policy BindingPolicy, change func(raw map[string]interface{})) *auth.Claims {
	claims := bindingClaims(policy)
	change(claims.Raw)
	return claimsFromRaw(claims.Raw)
}

func binding(raw map[string]interface{}) map[string]interface{} {
	return raw[BindingClaim].(map[string]interface{})
}

func TestResolveBindingScopeAcceptsBothPolicies(t *testing.T) {
	for _, policy := range []BindingPolicy{PolicyPublish, PolicySubscribe} {
		scope, err := ResolveBindingScope(bindingClaims(policy), bindingTestNow)
		if err != nil {
			t.Fatalf("%s: unexpected denial: %v", policy, err)
		}
		want := BindingScope{
			UID: "7b1f5c2e-1111-4222-8333-944445555666", Generation: 3, Policy: policy,
			TopicID: testTopicID, TenantID: testTenant, EventType: testTopic,
			Subject: "codefoundry:workload:conveste-hostgator:workload-conveste:cflow:6f9a",
		}
		if *scope != want {
			t.Fatalf("%s: scope = %+v, want %+v", policy, *scope, want)
		}
	}
}

func TestResolveBindingScopeAcceptsScopeOrderAndAudienceArray(t *testing.T) {
	claims := mutate(PolicySubscribe, func(raw map[string]interface{}) {
		raw["scope"] = "codeq:result codeq:nack codeq:heartbeat codeq:claim codeq:abandon"
		raw["aud"] = []interface{}{BindingWorkerAudience}
	})
	if _, err := ResolveBindingScope(claims, bindingTestNow); err != nil {
		t.Fatalf("unexpected denial: %v", err)
	}
}

func TestResolveBindingScopeRefusals(t *testing.T) {
	cases := []struct {
		name   string
		policy BindingPolicy
		change func(raw map[string]interface{})
		reason string
	}{
		{"binding not object", PolicyPublish, func(r map[string]interface{}) { r[BindingClaim] = "x" }, ReasonMalformedBinding},
		{"binding unknown key", PolicyPublish, func(r map[string]interface{}) { binding(r)["extra"] = true }, ReasonMalformedBinding},
		{"binding missing uid", PolicyPublish, func(r map[string]interface{}) { delete(binding(r), "uid") }, ReasonMalformedBinding},
		{"binding empty uid", PolicyPublish, func(r map[string]interface{}) { binding(r)["uid"] = "" }, ReasonMalformedBinding},
		{"binding long uid", PolicyPublish, func(r map[string]interface{}) { binding(r)["uid"] = strings.Repeat("a", 129) }, ReasonMalformedBinding},
		{"binding uid with space", PolicyPublish, func(r map[string]interface{}) { binding(r)["uid"] = "a b" }, ReasonMalformedBinding},
		{"binding zero generation", PolicyPublish, func(r map[string]interface{}) { binding(r)["generation"] = float64(0) }, ReasonMalformedBinding},
		{"binding fractional generation", PolicyPublish, func(r map[string]interface{}) { binding(r)["generation"] = 1.5 }, ReasonMalformedBinding},
		{"binding string generation", PolicyPublish, func(r map[string]interface{}) { binding(r)["generation"] = "1" }, ReasonMalformedBinding},
		{"binding unknown policy", PolicyPublish, func(r map[string]interface{}) { binding(r)["policy"] = "Admin" }, ReasonMalformedBinding},
		{"role claim", PolicyPublish, func(r map[string]interface{}) { r["role"] = "ADMIN" }, ReasonForbiddenClaim},
		{"tenant_epoch claim", PolicyPublish, func(r map[string]interface{}) { r["tenant_epoch"] = "x" }, ReasonForbiddenClaim},
		{"topic_controller claim", PolicySubscribe, func(r map[string]interface{}) { r["topic_controller"] = true }, ReasonForbiddenClaim},
		{"missing tid", PolicyPublish, func(r map[string]interface{}) { delete(r, "tid") }, ReasonTenant},
		{"invalid tid", PolicyPublish, func(r map[string]interface{}) { r["tid"] = "Conveste" }, ReasonTenant},
		{"padded tid", PolicyPublish, func(r map[string]interface{}) { r["tid"] = " conveste" }, ReasonTenant},
		{"tenant alias same value", PolicyPublish, func(r map[string]interface{}) { r["tenantId"] = testTenant }, ReasonTenant},
		{"organization alias", PolicySubscribe, func(r map[string]interface{}) { r["organization_id"] = "other" }, ReasonTenant},
		{"wildcard event type", PolicySubscribe, func(r map[string]interface{}) { r["eventTypes"] = []interface{}{"*"} }, ReasonEventTypes},
		{"two event types", PolicySubscribe, func(r map[string]interface{}) {
			r["eventTypes"] = []interface{}{testTopic, "other"}
		}, ReasonEventTypes},
		{"no event types", PolicyPublish, func(r map[string]interface{}) { r["eventTypes"] = []interface{}{} }, ReasonEventTypes},
		{"hidden non-string event", PolicyPublish, func(r map[string]interface{}) {
			r["eventTypes"] = []interface{}{testTopic, float64(1)}
		}, ReasonEventTypes},
		{"event types not array", PolicyPublish, func(r map[string]interface{}) { r["eventTypes"] = testTopic }, ReasonEventTypes},
		{"event type with dot", PolicyPublish, func(r map[string]interface{}) {
			r["eventTypes"] = []interface{}{testTopicID}
			binding(r)["topicId"] = "conveste.conveste.cflow-executar"
		}, ReasonEventTypes},
		{"topic other tenant", PolicyPublish, func(r map[string]interface{}) { binding(r)["topicId"] = "other.cflow-executar" }, ReasonTopicMismatch},
		{"topic other name", PolicyPublish, func(r map[string]interface{}) { binding(r)["topicId"] = "conveste.other" }, ReasonTopicMismatch},
		{"publish with worker audience", PolicyPublish, func(r map[string]interface{}) { r["aud"] = BindingWorkerAudience }, ReasonAudience},
		{"subscribe with producer audience", PolicySubscribe, func(r map[string]interface{}) { r["aud"] = BindingProducerAudience }, ReasonAudience},
		{"two audiences", PolicyPublish, func(r map[string]interface{}) {
			r["aud"] = []interface{}{BindingProducerAudience, "code-admin-api"}
		}, ReasonAudience},
		{"publish plus admin scope", PolicyPublish, func(r map[string]interface{}) { r["scope"] = "codeq:publish codeq:admin" }, ReasonScopes},
		{"publish duplicate scope", PolicyPublish, func(r map[string]interface{}) { r["scope"] = "codeq:publish codeq:publish" }, ReasonScopes},
		{"subscribe with subscribe scope", PolicySubscribe, func(r map[string]interface{}) {
			r["scope"] = workerScopeString + " codeq:subscribe"
		}, ReasonScopes},
		{"subscribe missing heartbeat", PolicySubscribe, func(r map[string]interface{}) {
			r["scope"] = "codeq:abandon codeq:claim codeq:nack codeq:result"
		}, ReasonScopes},
		{"subscribe with publish scope set", PolicySubscribe, func(r map[string]interface{}) { r["scope"] = PublishScope }, ReasonScopes},
		{"scope array", PolicyPublish, func(r map[string]interface{}) { r["scope"] = []interface{}{PublishScope} }, ReasonScopes},
		{"legacy subject", PolicySubscribe, func(r map[string]interface{}) { r["sub"] = "codecloud-worker" }, ReasonSubject},
		{"bare prefix subject", PolicySubscribe, func(r map[string]interface{}) { r["sub"] = BindingSubjectPrefix }, ReasonSubject},
		{"subject with newline", PolicySubscribe, func(r map[string]interface{}) { r["sub"] = BindingSubjectPrefix + "a\nb" }, ReasonSubject},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertDenied(t, mutate(tc.policy, tc.change), tc.reason)
		})
	}
}

func TestResolveBindingScopeLifetime(t *testing.T) {
	cases := []struct {
		name     string
		iat, exp time.Duration
	}{
		{"lifetime above 300s", -10 * time.Second, 291 * time.Second},
		{"issued in the future", 61 * time.Second, 200 * time.Second},
		{"expired beyond skew", -300 * time.Second, -61 * time.Second},
		{"exp before iat", 0, -1 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claims := bindingClaims(PolicyPublish)
			claims.IssuedAt = bindingTestNow.Add(tc.iat)
			claims.ExpiresAt = bindingTestNow.Add(tc.exp)
			assertDenied(t, claims, ReasonLifetime)
		})
	}
	for _, field := range []string{"iat", "exp"} {
		claims := bindingClaims(PolicyPublish)
		if field == "iat" {
			claims.IssuedAt = time.Time{}
		} else {
			claims.ExpiresAt = time.Time{}
		}
		assertDenied(t, claims, ReasonLifetime)
	}
	exact := bindingClaims(PolicyPublish)
	exact.IssuedAt, exact.ExpiresAt = bindingTestNow, bindingTestNow.Add(BindingMaxLifetime)
	if _, err := ResolveBindingScope(exact, bindingTestNow); err != nil {
		t.Fatalf("exact 300s lifetime refused: %v", err)
	}
}

func TestResolveBindingScopeRefusesPublishWithoutBinding(t *testing.T) {
	claims := bindingClaims(PolicyPublish)
	delete(claims.Raw, BindingClaim)
	if !RequiresBindingScope(claims) {
		t.Fatal("codeq:publish token must require the binding contract")
	}
	assertDenied(t, claims, ReasonMissingBinding)
	if _, err := ResolveBindingScope(nil, bindingTestNow); err == nil {
		t.Fatal("nil claims accepted")
	}
}

func TestResolveBindingScopeRefusesStaticShapedClaims(t *testing.T) {
	// A static validator returns no audience, no expiry and no raw scope.
	claims := bindingClaims(PolicyPublish)
	claims.Audience = nil
	delete(claims.Raw, "aud")
	assertDenied(t, claims, ReasonAudience)
}

func TestRequiresBindingScope(t *testing.T) {
	legacyAdmin := &auth.Claims{Scopes: []string{"codeq:admin"}, Raw: map[string]interface{}{"role": "ADMIN", "tenantId": "local-tenant"}}
	legacyWorker := &auth.Claims{Scopes: strings.Fields(workerScopeString), EventTypes: []string{"*"}, Raw: map[string]interface{}{}}
	topicController := &auth.Claims{Scopes: []string{"codeq:topics:manage"}, Raw: map[string]interface{}{"tenant_epoch": "x"}}
	for name, claims := range map[string]*auth.Claims{"nil": nil, "admin": legacyAdmin, "worker": legacyWorker, "topic": topicController} {
		if RequiresBindingScope(claims) {
			t.Fatalf("%s must not require the binding contract", name)
		}
	}
	markerOnly := &auth.Claims{Raw: map[string]interface{}{BindingClaim: nil}}
	if !RequiresBindingScope(markerOnly) {
		t.Fatal("any codeq_binding claim must require the binding contract")
	}
	assertDenied(t, markerOnly, ReasonMalformedBinding)
}

func assertDenied(t *testing.T, claims *auth.Claims, reason string) {
	t.Helper()
	scope, err := ResolveBindingScope(claims, bindingTestNow)
	if err == nil {
		t.Fatalf("expected denial %q, got scope %+v", reason, scope)
	}
	var denied *BindingDenied
	if !errors.As(err, &denied) || denied.Reason != reason {
		t.Fatalf("denial = %v, want reason %q", err, reason)
	}
	if !strings.HasPrefix(err.Error(), "binding scope denied: ") {
		t.Fatalf("unexpected error text %q", err.Error())
	}
}
