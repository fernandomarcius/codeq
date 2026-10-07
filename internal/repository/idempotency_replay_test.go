package repository

import (
	"errors"
	"testing"
	"time"

	"github.com/osvaldoandrade/codeq/pkg/domain"
)

const (
	replayTenantA = "tenant-a"
	replayTenantB = "tenant-b"
	replayKey     = "order-42"
)

func TestReplayIdempotentOnlySameTenant(t *testing.T) {
	task := &domain.Task{ID: "t-1", TenantID: replayTenantA}
	if got, err := ReplayIdempotent(task, replayTenantA); err != nil || got != task {
		t.Fatalf("same tenant: got %v, %v", got, err)
	}
	cases := map[string]struct {
		task   *domain.Task
		tenant string
	}{
		"other tenant":         {task, replayTenantB},
		"case differs":         {task, "Tenant-A"},
		"legacy empty vs set":  {&domain.Task{ID: "t-2"}, replayTenantA},
		"set vs legacy empty":  {task, ""},
		"missing task (fails)": {nil, replayTenantA},
	}
	for name, tc := range cases {
		got, err := ReplayIdempotent(tc.task, tc.tenant)
		if !errors.Is(err, domain.ErrIdempotencyConflict) || got != nil {
			t.Fatalf("%s: got %v, %v; want nil, ErrIdempotencyConflict", name, got, err)
		}
	}
	legacy := &domain.Task{ID: "t-3"}
	if got, err := ReplayIdempotent(legacy, ""); err != nil || got != legacy {
		t.Fatalf("legacy empty tenant replay: got %v, %v", got, err)
	}
}

func TestRedisIdempotencyIsTenantBound(t *testing.T) {
	ctx, _, _, repo := setupRepo(t)
	cmd := domain.CmdGenerateMaster
	first, err := repo.Enqueue(ctx, cmd, `{"secret":1}`, 0, "", 5, replayKey, time.Time{}, replayTenantA)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Twice: the second call takes the Bloom "maybe present" path.
	for i := range 2 {
		got, err := repo.Enqueue(ctx, cmd, `{}`, 0, "", 5, replayKey, time.Time{}, replayTenantB)
		if !errors.Is(err, domain.ErrIdempotencyConflict) || got != nil {
			t.Fatalf("cross-tenant #%d: got %v, %v; want nil, ErrIdempotencyConflict", i, got, err)
		}
	}
	again, err := repo.Enqueue(ctx, cmd, `{}`, 0, "", 5, replayKey, time.Time{}, replayTenantA)
	if err != nil || again.ID != first.ID {
		t.Fatalf("same-tenant replay: got %v, %v; want %s", again, err, first.ID)
	}
}
