package pebble

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bytedance/sonic"
	pebbledb "github.com/cockroachdb/pebble"

	"github.com/osvaldoandrade/codeq/pkg/domain"
)

// waitingDuplicate returns the task that holds the deduplication mapping at
// mappingKey if that task still waits (PENDING, ready or delayed) and belongs
// to the same (tenant, command, key). Any other outcome returns nil and no
// error: the mapping is free or stale (its task was claimed, finished or
// reaped), and the caller overwrites it with the task it creates.
func (r *TaskRepository) waitingDuplicate(mappingKey []byte, cmd domain.Command, tenantID, key string) (*domain.Task, error) {
	id, err := r.db.Get(mappingKey)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("dedupe lookup: %w", err)
	}
	body, err := r.db.Get(KeyTask(string(id)))
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("dedupe task lookup: %w", err)
	}
	var t domain.Task
	if err := sonic.Unmarshal(body, &t); err != nil {
		return nil, fmt.Errorf("dedupe task decode: %w", err)
	}
	if t.Status != domain.StatusPending || t.TenantID != tenantID || t.DeduplicationKey != key ||
		!strings.EqualFold(string(t.Command), string(cmd)) {
		return nil, nil
	}
	return &t, nil
}

// releaseDedupe deletes, inside the batch that moves t out of the waiting
// state, the deduplication mapping t holds. The delete is unconditional: a
// create that finds t waiting returns t instead of writing, so while t waits
// the mapping cannot point at another task, and this batch is the one that
// ends the wait.
func releaseDedupe(b *pebbledb.Batch, t *domain.Task) error {
	if t.DeduplicationKey == "" {
		return nil
	}
	return b.Delete(KeyDedupe(t.Command, t.TenantID, t.DeduplicationKey), nil)
}
