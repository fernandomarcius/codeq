package authclaims

import (
	"errors"
	"math"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/osvaldoandrade/codeq/pkg/auth"
)

// Binding-scoped tokens (platform ADR-0022 C1.1) are short-lived Tikti tokens
// that grant one workload access to exactly one tenant topic. They are
// recognized by the codeq_binding claim, and every token that carries the
// codeq:publish scope must be one of them.
const (
	// BindingClaim is the object claim that marks a binding-scoped token.
	BindingClaim = "codeq_binding"
	// PublishScope is granted only to binding-scoped Publish tokens.
	PublishScope = "codeq:publish"
	// BindingProducerAudience is the only audience of a Publish token.
	BindingProducerAudience = "codeq-producer"
	// BindingWorkerAudience is the only audience of a Subscribe token.
	BindingWorkerAudience = "codeq-worker"
	// BindingSubjectPrefix is the Tikti subject namespace for binding tokens.
	BindingSubjectPrefix = "codefoundry:workload:"
	// BindingMaxLifetime bounds exp-iat of a binding-scoped token.
	BindingMaxLifetime = 300 * time.Second
	// BindingIssuedAtSkew is the ADR-0022 C1.4 clock-skew tolerance.
	BindingIssuedAtSkew = 60 * time.Second

	maxBindingUIDLength   = 128
	maxBindingSubjectSize = 512
	maxSafeJSONInteger    = 1 << 53
)

// BindingPolicy is the ResourceBinding policy encoded in the token.
type BindingPolicy string

const (
	// PolicyPublish grants task creation and reads for one topic.
	PolicyPublish BindingPolicy = "Publish"
	// PolicySubscribe grants the worker lifecycle for one topic.
	PolicySubscribe BindingPolicy = "Subscribe"
)

// Stable, bounded denial reasons. They are used for logs and metric labels
// only; the HTTP body is always binding_scope_denied.
const (
	ReasonMalformedBinding = "malformed_binding"
	ReasonMissingBinding   = "missing_binding"
	ReasonTenant           = "tenant"
	ReasonEventTypes       = "event_types"
	ReasonTopicMismatch    = "topic_mismatch"
	ReasonForbiddenClaim   = "forbidden_claim"
	ReasonAudience         = "audience"
	ReasonScopes           = "scopes"
	ReasonSubject          = "subject"
	ReasonLifetime         = "lifetime"
)

var (
	publishScopes   = []string{PublishScope}
	subscribeScopes = []string{"codeq:abandon", "codeq:claim", "codeq:heartbeat", "codeq:nack", "codeq:result"}
	// forbiddenBindingClaims would let a binding token reach legacy admin,
	// topic-controller or alternate tenant semantics.
	forbiddenBindingClaims = [...]string{"role", "tenant_epoch", "topic_controller"}
	bindingKeys            = map[string]bool{"uid": true, "generation": true, "policy": true, "topicId": true}
	eventTypePattern       = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
)

// ErrStreamNotAllowed refuses binding-scoped tokens on the gRPC streams,
// which are outside the ADR-0022 C1.2 allow-list. Its text is the stable code.
var ErrStreamNotAllowed = errors.New("route_not_allowed")

// BindingDenied reports why a token failed the binding-scope contract.
type BindingDenied struct{ Reason string }

func (e *BindingDenied) Error() string { return "binding scope denied: " + e.Reason }

func deny(reason string) error { return &BindingDenied{Reason: reason} }

// BindingScope is the verified authority of a binding-scoped token.
type BindingScope struct {
	UID        string
	Generation int64
	Policy     BindingPolicy
	TopicID    string
	TenantID   string
	EventType  string
	Subject    string
}

// RequiresBindingScope reports whether a validated token must satisfy the
// binding-scope contract: it carries codeq_binding or the codeq:publish scope.
func RequiresBindingScope(claims *auth.Claims) bool {
	if claims == nil {
		return false
	}
	if _, present := claims.Raw[BindingClaim]; present {
		return true
	}
	return claims.HasScope(PublishScope)
}

// ResolveBindingScope verifies the complete ADR-0022 C1.1 contract and fails
// closed on any deviation. now is the verifier clock.
func ResolveBindingScope(claims *auth.Claims, now time.Time) (*BindingScope, error) {
	if claims == nil {
		return nil, deny(ReasonMissingBinding)
	}
	scope, err := parseBindingObject(claims.Raw)
	if err != nil {
		return nil, err
	}
	checks := []func(*auth.Claims, *BindingScope) error{
		checkForbiddenClaims,
		checkBindingTenant,
		checkBindingEventType,
		checkBindingTopic,
		checkBindingAudience,
		checkBindingScopes,
		checkBindingSubject,
	}
	for _, check := range checks {
		if err := check(claims, scope); err != nil {
			return nil, err
		}
	}
	if err := checkBindingLifetime(claims, now); err != nil {
		return nil, err
	}
	return scope, nil
}

func parseBindingObject(raw map[string]interface{}) (*BindingScope, error) {
	value, present := raw[BindingClaim]
	if !present {
		return nil, deny(ReasonMissingBinding)
	}
	object, ok := value.(map[string]interface{})
	if !ok || !exactBindingKeys(object) {
		return nil, deny(ReasonMalformedBinding)
	}
	uid, uidOK := object["uid"].(string)
	topicID, topicOK := object["topicId"].(string)
	policy, policyOK := object["policy"].(string)
	generation, generationOK := bindingGeneration(object["generation"])
	if !uidOK || !topicOK || !policyOK || !generationOK || !validBindingUID(uid) || !validBindingPolicy(policy) {
		return nil, deny(ReasonMalformedBinding)
	}
	return &BindingScope{UID: uid, Generation: generation, Policy: BindingPolicy(policy), TopicID: topicID}, nil
}

func validBindingPolicy(policy string) bool {
	return BindingPolicy(policy) == PolicyPublish || BindingPolicy(policy) == PolicySubscribe
}

func exactBindingKeys(object map[string]interface{}) bool {
	if len(object) != len(bindingKeys) {
		return false
	}
	for key := range object {
		if !bindingKeys[key] {
			return false
		}
	}
	return true
}

func bindingGeneration(value interface{}) (int64, bool) {
	number, ok := value.(float64)
	if !ok || number < 1 || number > maxSafeJSONInteger || number != math.Trunc(number) {
		return 0, false
	}
	return int64(number), true
}

func validBindingUID(uid string) bool {
	if uid == "" || len(uid) > maxBindingUIDLength || !utf8.ValidString(uid) {
		return false
	}
	return strings.IndexFunc(uid, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) < 0
}

func checkForbiddenClaims(claims *auth.Claims, _ *BindingScope) error {
	for _, name := range forbiddenBindingClaims {
		if _, present := claims.Raw[name]; present {
			return deny(ReasonForbiddenClaim)
		}
	}
	return nil
}

// checkBindingTenant requires exactly the canonical tid claim, untrimmed and
// valid, with no legacy tenant alias present.
func checkBindingTenant(claims *auth.Claims, scope *BindingScope) error {
	for _, name := range tenantClaimNames[1:] {
		if _, present := claims.Raw[name]; present {
			return deny(ReasonTenant)
		}
	}
	tid, ok := claims.Raw[claimTID].(string)
	if !ok || !tenantPattern.MatchString(tid) {
		return deny(ReasonTenant)
	}
	resolved, err := ResolveTenantID(claims)
	if err != nil || resolved != tid {
		return deny(ReasonTenant)
	}
	scope.TenantID = tid
	return nil
}

// checkBindingEventType requires exactly one concrete event type in both the
// raw claim and the parsed claims, so dropped non-string entries cannot hide.
func checkBindingEventType(claims *auth.Claims, scope *BindingScope) error {
	raw, ok := claims.Raw["eventTypes"].([]interface{})
	if !ok || len(raw) != 1 || len(claims.EventTypes) != 1 {
		return deny(ReasonEventTypes)
	}
	eventType, ok := raw[0].(string)
	if !ok || eventType != claims.EventTypes[0] || !eventTypePattern.MatchString(eventType) {
		return deny(ReasonEventTypes)
	}
	scope.EventType = eventType
	return nil
}

func checkBindingTopic(_ *auth.Claims, scope *BindingScope) error {
	if scope.TopicID != scope.TenantID+"."+scope.EventType {
		return deny(ReasonTopicMismatch)
	}
	return nil
}

func checkBindingAudience(claims *auth.Claims, scope *BindingScope) error {
	expected := BindingProducerAudience
	if scope.Policy == PolicySubscribe {
		expected = BindingWorkerAudience
	}
	if len(claims.Audience) != 1 || claims.Audience[0] != expected {
		return deny(ReasonAudience)
	}
	switch aud := claims.Raw["aud"].(type) {
	case string:
		if aud != expected {
			return deny(ReasonAudience)
		}
	case []interface{}:
		if len(aud) != 1 || aud[0] != expected {
			return deny(ReasonAudience)
		}
	default:
		return deny(ReasonAudience)
	}
	return nil
}

func checkBindingScopes(claims *auth.Claims, scope *BindingScope) error {
	expected := publishScopes
	if scope.Policy == PolicySubscribe {
		expected = subscribeScopes
	}
	rawScope, ok := claims.Raw["scope"].(string)
	if !ok || !sameScopeSet(strings.Fields(rawScope), expected) || !sameScopeSet(claims.Scopes, expected) {
		return deny(ReasonScopes)
	}
	return nil
}

// sameScopeSet reports whether got is exactly the expected set, without
// duplicates or extra members. Order is irrelevant.
func sameScopeSet(got, expected []string) bool {
	if len(got) != len(expected) {
		return false
	}
	seen := make(map[string]bool, len(got))
	for _, scope := range got {
		if seen[scope] {
			return false
		}
		seen[scope] = true
	}
	for _, scope := range expected {
		if !seen[scope] {
			return false
		}
	}
	return true
}

func checkBindingSubject(claims *auth.Claims, scope *BindingScope) error {
	subject := claims.Subject
	rest, ok := strings.CutPrefix(subject, BindingSubjectPrefix)
	if !ok || rest == "" || len(subject) > maxBindingSubjectSize || !utf8.ValidString(subject) {
		return deny(ReasonSubject)
	}
	if strings.IndexFunc(subject, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return deny(ReasonSubject)
	}
	scope.Subject = subject
	return nil
}

// checkBindingLifetime requires exp and iat, bounds the issued lifetime to
// BindingMaxLifetime and rejects tokens issued in the future or expired beyond
// the ADR-0022 C1.4 skew. The validator remains the primary expiry check; this
// is defense in depth for validators that treat exp as optional.
func checkBindingLifetime(claims *auth.Claims, now time.Time) error {
	if claims.ExpiresAt.IsZero() || claims.IssuedAt.IsZero() {
		return deny(ReasonLifetime)
	}
	lifetime := claims.ExpiresAt.Sub(claims.IssuedAt)
	if lifetime <= 0 || lifetime > BindingMaxLifetime {
		return deny(ReasonLifetime)
	}
	if claims.IssuedAt.After(now.Add(BindingIssuedAtSkew)) || now.After(claims.ExpiresAt.Add(BindingIssuedAtSkew)) {
		return deny(ReasonLifetime)
	}
	return nil
}
