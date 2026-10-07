package controllers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/osvaldoandrade/codeq/internal/leaderforward"
)

// respondWriteError writes the right response for a service-level error
// from a WRITE controller (create, claim, submit, nack, etc.). A Raft
// "not leader" error is forwarded to the configured leader in-process
// (platform ADR-0022 C1.5) or answered with 503 leader_unavailable; no
// client ever receives a 307. Every other error keeps the usual 400.
func respondWriteError(c *gin.Context, err error) {
	if maybeForwardLeader(c, err) {
		return
	}
	c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
}

// maybeForwardLeader handles a Raft "not leader" error and returns true
// when it wrote the response. Otherwise it returns false and writes nothing.
func maybeForwardLeader(c *gin.Context, err error) bool {
	return leaderforward.HandleNotLeader(c, err)
}
