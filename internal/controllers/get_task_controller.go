package controllers

import (
	"net/http"

	"github.com/osvaldoandrade/codeq/internal/services"

	"github.com/gin-gonic/gin"
)

type getTaskController struct{ svc services.SchedulerService }

func NewGetTaskController(svc services.SchedulerService) *getTaskController {
	return &getTaskController{svc}
}

func (h *getTaskController) Handle(c *gin.Context) {
	taskID := c.Param("id")
	task, err := h.svc.GetTask(c.Request.Context(), taskID)
	// A task outside the token tenant (or binding command) is reported
	// exactly like a missing one, so there is no existence oracle.
	if err != nil || !taskVisible(c, task) {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	c.JSON(http.StatusOK, task)
}
