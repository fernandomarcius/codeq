package controllers

import (
	"regexp"
	"testing"

	"github.com/osvaldoandrade/codeq/internal/authclaims"
)

// labelPattern mirrors the verified tid and eventTypes[0] grammar.
var labelPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// FuzzStorageIdempotencyKey checks the namespace format is injective: two
// accepted (tid, eventType, key) triples share a storage key only when they
// are equal, and a non-empty binding key never equals any accepted legacy key.
func FuzzStorageIdempotencyKey(f *testing.F) {
	f.Add("conveste", "topic-a", "order-42", "conveste", "topic-b", "order-42")
	f.Add("a", "b-c", "d", "a-b", "c", "d")
	f.Add("t", "e", "x", "t", "e", "x")
	f.Fuzz(func(t *testing.T, tid1, evt1, key1, tid2, evt2, key2 string) {
		for _, label := range []string{tid1, evt1, tid2, evt2} {
			if !labelPattern.MatchString(label) {
				return
			}
		}
		if !validIdempotencyKey(key1) || !validIdempotencyKey(key2) || key1 == "" || key2 == "" {
			return
		}
		s1 := &authclaims.BindingScope{TenantID: tid1, EventType: evt1}
		s2 := &authclaims.BindingScope{TenantID: tid2, EventType: evt2}
		k1, k2 := storageIdempotencyKey(s1, key1), storageIdempotencyKey(s2, key2)
		same := tid1 == tid2 && evt1 == evt2 && key1 == key2
		if (k1 == k2) != same {
			t.Fatalf("namespace collision: (%q,%q,%q)=%q vs (%q,%q,%q)=%q", tid1, evt1, key1, k1, tid2, evt2, key2, k2)
		}
		if k1 == storageIdempotencyKey(nil, key2) {
			t.Fatalf("binding key %q equals legacy key %q", k1, key2)
		}
	})
}
