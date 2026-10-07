package controllers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/osvaldoandrade/codeq/internal/services"
	"github.com/osvaldoandrade/codeq/pkg/domain"

	"github.com/gin-gonic/gin"
)

const (
	// errTaskNotFound is the missing-task text, shared with foreign tasks so
	// the route is not an existence oracle.
	errTaskNotFound = "task not found"
	// maxLongPollSeconds caps `?waitSeconds=` to keep connection budgets
	// predictable. Producers that want to wait longer should retry.
	maxLongPollSeconds = 60
	// longPollInterval is how often the controller re-checks the result
	// while waiting. Pebble's `Get` is a memtable/L0 lookup so 100 ms is
	// effectively free; tighter intervals add CPU without changing
	// perceived latency.
	longPollInterval = 100 * time.Millisecond
)

type getResultController struct{ svc services.ResultsService }

func NewGetResultController(s services.ResultsService) *getResultController {
	return &getResultController{svc: s}
}

// Handle returns the stored result for a task. With `?waitSeconds=N`
// the request is held open until either the result becomes available
// or the deadline elapses (long-poll). Without the query param the
// behavior is the original fire-and-return.
func (h *getResultController) Handle(c *gin.Context) {
	id := c.Param("id")
	wait := parseWaitSeconds(c.Query("waitSeconds"))

	rec, task, err := h.svc.Get(c.Request.Context(), id)
	if resultHidden(c, task, err) {
		writeTaskNotFound(c)
		return
	}
	if err == nil {
		c.JSON(http.StatusOK, gin.H{"task": task, "result": rec})
		return
	}
	if wait == 0 || err.Error() == errTaskNotFound {
		// No long-poll requested, or the task itself doesn't exist —
		// no amount of waiting will make a result appear.
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	h.longPoll(c, id, wait)
}

func (h *getResultController) longPoll(c *gin.Context, id string, wait int) {
	deadline := time.NewTimer(time.Duration(wait) * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(longPollInterval)
	defer tick.Stop()

	for {
		select {
		case <-c.Request.Context().Done():
			// Client gave up; nothing left to send.
			return
		case <-deadline.C:
			rec, task, err := h.svc.Get(c.Request.Context(), id)
			if resultHidden(c, task, err) {
				writeTaskNotFound(c)
				return
			}
			if err != nil {
				c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"task": task, "result": rec})
			return
		case <-tick.C:
			rec, task, err := h.svc.Get(c.Request.Context(), id)
			if resultHidden(c, task, err) {
				writeTaskNotFound(c)
				return
			}
			if err != nil {
				continue
			}
			c.JSON(http.StatusOK, gin.H{"task": task, "result": rec})
			return
		}
	}
}

// resultHidden reports whether a lookup returned a task the token may not
// read (or a result without its task). A missing task (task nil with an
// error) keeps today's response; a foreign task is answered with the same
// "task not found" body immediately, without long-polling, so neither the
// body nor the latency reveals that it exists.
func resultHidden(c *gin.Context, task *domain.Task, err error) bool {
	if task == nil && err != nil {
		return false
	}
	return !taskVisible(c, task)
}

func writeTaskNotFound(c *gin.Context) {
	c.JSON(http.StatusNotFound, gin.H{"error": errTaskNotFound})
}

func parseWaitSeconds(raw string) int {
	if raw == "" {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0
	}
	if n > maxLongPollSeconds {
		return maxLongPollSeconds
	}
	return n
}
