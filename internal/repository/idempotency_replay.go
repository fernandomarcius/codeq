package repository

import "github.com/osvaldoandrade/codeq/pkg/domain"

// ReplayIdempotent decides what an idempotency-key hit may return. The
// existing task is replayed only to a caller of the same tenant (an exact,
// case-sensitive match, including the empty legacy tenant). Any other caller
// gets domain.ErrIdempotencyConflict and no task, whatever its token kind.
// Same-tenant replay is unchanged: the original task is returned even when
// the replayed request differs (command, payload, priority).
func ReplayIdempotent(existing *domain.Task, tenantID string) (*domain.Task, error) {
	if existing == nil || existing.TenantID != tenantID {
		return nil, domain.ErrIdempotencyConflict
	}
	return existing, nil
}
