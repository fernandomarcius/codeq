package app

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type raftReadinessVerifier interface{ VerifyReadiness(context.Context) error }

// registerRaftReadiness exposes bounded, data-free local quorum observations.
// It is not a liveness probe: followers and partitioned leaders return 503.
func registerRaftReadiness(app *Application) {
	busy := make(chan struct{}, 1)
	app.Engine.GET("/readyz/raft", func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		select {
		case busy <- struct{}{}:
			defer func() { <-busy }()
		default:
			c.Status(http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		verified := make([]int, 0, len(app.RaftGroups))
		selfID := ""
		for index, group := range app.RaftGroups {
			if index == 0 {
				selfID = group.SelfID()
			}
			verifier, ok := group.(raftReadinessVerifier)
			if ok && group.IsLeader() && verifier.VerifyReadiness(ctx) == nil {
				verified = append(verified, index)
			}
		}
		status := http.StatusServiceUnavailable
		if len(app.RaftGroups) > 0 && len(verified) == len(app.RaftGroups) {
			status = http.StatusOK
		}
		c.JSON(status, gin.H{"schemaVersion": "raft-readiness/v1", "selfId": selfID, "numGroups": len(app.RaftGroups), "verifiedGroups": verified})
	})
}
