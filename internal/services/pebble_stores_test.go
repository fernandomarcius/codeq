package services

import (
	"testing"
	"time"

	"github.com/osvaldoandrade/codeq/internal/repository"
	pebblerepo "github.com/osvaldoandrade/codeq/internal/repository/pebble"
)

type pebbleStores struct {
	tasks   repository.TaskRepository
	results repository.ResultRepository
	subs    repository.SubscriptionRepository
}

func openPebbleStores(t *testing.T) pebbleStores {
	t.Helper()
	db, err := pebblerepo.Open(pebblerepo.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("pebble open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return pebbleStores{
		tasks:   pebblerepo.NewTaskRepository(db, time.UTC, "exp_full_jitter", 1, 10),
		results: pebblerepo.NewResultRepository(db, time.UTC),
		subs:    pebblerepo.NewSubscriptionRepository(db, time.UTC),
	}
}
