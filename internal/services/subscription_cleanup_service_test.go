package services

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

func TestSubscriptionCleanupStopsOnCancel(t *testing.T) {
	stores := openPebbleStores(t)
	svc := NewSubscriptionCleanupService(stores.subs, slog.Default(), 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		svc.Start(ctx)
		close(done)
	}()
	time.Sleep(1100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup did not return after cancel")
	}
}
