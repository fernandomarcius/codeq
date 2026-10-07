package controllers

import (
	"github.com/gin-gonic/gin"

	"github.com/osvaldoandrade/codeq/internal/middleware"
	"github.com/osvaldoandrade/codeq/pkg/domain"
)

// taskVisible reports whether the authenticated token may read the task.
// Every token kind is confined to its own tenant; a binding-scoped token is
// additionally confined to its single command.
func taskVisible(c *gin.Context, task *domain.Task) bool {
	if task == nil {
		return false
	}
	tenantID := middleware.GetTenantID(c)
	if tenantID == "" || task.TenantID != tenantID {
		return false
	}
	if scope, ok := middleware.GetBindingScope(c); ok && string(task.Command) != scope.EventType {
		return false
	}
	return true
}
