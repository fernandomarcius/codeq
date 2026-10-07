package leaderforward

import (
	"net/url"
	"strings"
	"testing"
)

// FuzzForwardTarget checks the C1.5 target rule for arbitrary leader hints
// and peer values: a hint is a forward target only when it is byte-for-byte
// a configured, well-formed peer URL other than this node's own, and the
// joined target URL keeps the peer's scheme and host.
func FuzzForwardTarget(f *testing.F) {
	seeds := [][2]string{
		{"http://codeq-1.codecloud-queue.svc.cluster.local:8080", "http://codeq-1.codecloud-queue.svc.cluster.local:8080"},
		{"http://codeq-1:8080", "http://codeq-1:8080/"},
		{"http://codeq-1:8080", "http://attacker.example"},
		{"http://user:pw@codeq-1:8080", "http://user:pw@codeq-1:8080"},
		{"https://codeq-2:8443/prefix", "https://codeq-2:8443/prefix"},
		{"", ""},
	}
	for _, s := range seeds {
		f.Add(s[0], s[1], "/v1/codeq/tasks?x=1")
	}
	const self = "http://codeq-0:8080"
	f.Fuzz(func(t *testing.T, peer, hint, requestURI string) {
		fw := New(Config{PeerHTTPAddrs: map[string]string{"codeq-0": self, "codeq-1": peer}, SelfID: "codeq-0"})
		if !fw.allowedTarget(hint) {
			return
		}
		if hint != peer || hint == self || !validPeerURL(peer) {
			t.Fatalf("hint %q accepted with peer %q", hint, peer)
		}
		original, err := url.ParseRequestURI(requestURI)
		if err != nil || !strings.HasPrefix(original.Path, "/") {
			return
		}
		target, err := url.Parse(targetURL(hint, original))
		base, _ := url.Parse(peer)
		if err != nil || target.Scheme != base.Scheme || target.Host != base.Host || target.User != nil {
			t.Fatalf("target %q escaped peer %q (err %v)", targetURL(hint, original), peer, err)
		}
	})
}

// FuzzClaimWaitSeconds checks the claim timeout extension stays within the
// scheduler's long-poll cap for any body.
func FuzzClaimWaitSeconds(f *testing.F) {
	for _, s := range []string{`{"waitSeconds":10}`, `{"waitSeconds":-1}`, `{"waitSeconds":1e9}`, `[]`, ``} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		if got := claimWaitSeconds(body); got < 0 || got > maxClaimWaitSeconds {
			t.Fatalf("claimWaitSeconds(%q) = %d", body, got)
		}
	})
}
