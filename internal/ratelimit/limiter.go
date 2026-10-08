package ratelimit

import (
	"context"
	"time"
)

// Bucket is the configured rate for one scope.
type Bucket struct {
	RequestsPerMinute int `yaml:"requestsPerMinute"`
	BurstSize         int `yaml:"burstSize"`
}

// Enabled reports whether the bucket should be enforced.
func (b Bucket) Enabled() bool {
	return b.RequestsPerMinute > 0 && b.BurstSize > 0
}

// Decision is the outcome of one Allow call.
type Decision struct {
	Allowed    bool
	RetryAfter time.Duration
}

// Limiter enforces per-subject request budgets. The server uses the
// in-process implementation; there is no external store.
type Limiter interface {
	Allow(ctx context.Context, scope string, subject string, bucket Bucket) (Decision, error)
}
