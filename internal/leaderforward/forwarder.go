// Package leaderforward relays /v1/codeq write requests from a Raft follower
// to the current leader in-process (platform ADR-0022 C1.5).
//
// Before this package a follower answered writes with HTTP 307 to the
// leader's in-cluster RAFT_PEER_HTTP_ADDRS URL. Clients outside the cluster
// (workload clusters reaching CodeQ through the regional edge) cannot resolve
// that URL, so a follower now forwards the request itself:
//
//   - the target is accepted only when it is exactly one of the configured
//     RAFT_PEER_HTTP_ADDRS values and is not this node's own URL;
//   - method, path, query, the buffered body and Authorization are preserved,
//     and X-CodeQ-Forwarded: 1 is added;
//   - redirects are never followed and a 3xx answer is never relayed;
//   - a request that already carries X-CodeQ-Forwarded is never forwarded
//     again and gets 503 {"error":"leader_unavailable"} with Retry-After: 1;
//   - a failure after the request was fully sent is ambiguous (the leader
//     may have applied it): 504 leader_forward_timeout or 502
//     leader_forward_interrupted, never with Retry-After.
//
// The header carries no authority. The leader re-runs authentication and
// every authorization rule. No client ever receives HTTP 307.
package leaderforward

import (
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Stable wire values.
const (
	// HeaderForwarded marks a request relayed by a follower. Its value is
	// always "1"; any non-empty value on an incoming request counts.
	HeaderForwarded = "X-CodeQ-Forwarded"

	// CodeLeaderUnavailable is the error code of every 503 this package
	// writes. The leader cannot have applied the request; clients retry
	// after Retry-After.
	CodeLeaderUnavailable = "leader_unavailable"

	// CodeForwardTimeout is the 504 answer when the request reached the
	// leader and no answer came within the forward timeout. The outcome is
	// unknown, so there is no Retry-After.
	CodeForwardTimeout = "leader_forward_timeout"

	// CodeForwardInterrupted is the 502 answer when the request reached the
	// leader and the connection then failed (for example the leader was
	// lost). The outcome is unknown, so there is no Retry-After.
	CodeForwardInterrupted = "leader_forward_interrupted"

	// CodeRequestTooLarge is returned when a request that must be forwarded
	// has a body larger than MaxBodyBytes.
	CodeRequestTooLarge = "request_too_large"

	// DefaultTimeout bounds one forwarded request (C1.5).
	DefaultTimeout = 10 * time.Second

	// MaxBodyBytes is the largest body a follower buffers for forwarding.
	// The leader itself applies no new limit; only a request that must be
	// forwarded and exceeds it is refused with 413.
	MaxBodyBytes int64 = 16 << 20

	// maxClaimWaitSeconds mirrors the scheduler's long-poll cap.
	maxClaimWaitSeconds = 30

	retryAfterSeconds = "1"
	errorKey          = "error"
)

// Leadership reports whether this node may write to one Raft group. It is
// satisfied by *pebble.DB through RequireWriteLeader: nil when this node
// leads the group (or the store is standalone), otherwise an error that
// implements domain.LeaderHint with the leader's configured HTTP URL.
type Leadership interface {
	RequireWriteLeader() error
}

// Config builds a Forwarder.
type Config struct {
	// PeerHTTPAddrs is the configured RAFT_PEER_HTTP_ADDRS map
	// (peer ID -> base URL). Only its values are forward targets.
	PeerHTTPAddrs map[string]string
	// SelfID is this node's Raft ID; its own URL is never a target.
	SelfID string
	// Groups lists every Raft group this process serves, in shard order.
	Groups []Leadership
	// Logger receives one bounded, secret-free line per failed forward.
	Logger *slog.Logger
	// Timeout overrides DefaultTimeout (tests only).
	Timeout time.Duration
	// Transport overrides the HTTP transport (tests only).
	Transport http.RoundTripper
}

// Forwarder relays requests to the Raft leader. A nil *Forwarder is valid:
// its middleware passes requests through and a not-leader error becomes 503.
type Forwarder struct {
	peers   map[string]struct{}
	self    string
	groups  []Leadership
	logger  *slog.Logger
	timeout time.Duration
	client  *http.Client
}

// New validates the configured peers and returns a Forwarder. Peer values
// that are not absolute http(s) base URLs are excluded from the allow-list,
// so a malformed entry can never become a target.
func New(cfg Config) *Forwarder {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	transport := cfg.Transport
	if transport == nil {
		transport = newTransport()
	}
	f := &Forwarder{
		peers:   make(map[string]struct{}, len(cfg.PeerHTTPAddrs)),
		self:    cfg.PeerHTTPAddrs[cfg.SelfID],
		groups:  append([]Leadership(nil), cfg.Groups...),
		logger:  logger,
		timeout: timeout,
		client: &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
	for id, addr := range cfg.PeerHTTPAddrs {
		if !validPeerURL(addr) {
			logger.Warn("raft peer HTTP address excluded from leader forwarding", "event", "codeq_leader_forward_config", "peer", id)
			continue
		}
		if id == cfg.SelfID {
			continue
		}
		f.peers[addr] = struct{}{}
	}
	return f
}

// newTransport never uses an environment proxy, so the bearer token only
// travels to the configured peer.
func newTransport() *http.Transport {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	return &http.Transport{
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   32,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
		DisableCompression:    true,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
	}
}

func validPeerURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	return u.RawQuery == "" && !u.ForceQuery && u.Fragment == ""
}

// allowedTarget reports whether addr is exactly a configured peer URL other
// than this node's own.
func (f *Forwarder) allowedTarget(addr string) bool {
	if addr == "" || addr == f.self {
		return false
	}
	_, ok := f.peers[addr]
	return ok
}

// targetURL joins the peer base URL with the original escaped path and
// query, exactly as the former 307 Location did.
func targetURL(peer string, original *url.URL) string {
	return strings.TrimSuffix(peer, "/") + original.RequestURI()
}
