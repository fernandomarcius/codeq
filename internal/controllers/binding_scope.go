package controllers

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/osvaldoandrade/codeq/internal/authclaims"
	"github.com/osvaldoandrade/codeq/internal/middleware"
	"github.com/osvaldoandrade/codeq/pkg/domain"
)

const (
	reasonEventType = "event_type_not_allowed"
	reasonWebhook   = "webhook_not_allowed"
	reasonNotOwner  = "not_owner"
)

// publishDenial returns the stable error code and metric reason when a
// Publish binding must not enqueue the task, or empty strings when allowed.
// The command must equal the token's single event type and webhooks are
// refused outright (platform ADR-0022 C1.2).
func publishDenial(scope *authclaims.BindingScope, cmd domain.Command, webhook string) (string, string) {
	if string(cmd) != scope.EventType {
		return middleware.CodeEventTypeNotAllow, reasonEventType
	}
	if webhook != "" {
		return middleware.CodeWebhookNotAllowed, reasonWebhook
	}
	return "", ""
}

// bindingOwnsTask applies the strict Subscribe-binding owner rule: the task
// belongs to the token tenant and command and is leased to the token subject.
// Unlike the legacy result path, an empty WorkerID never matches.
func bindingOwnsTask(scope *authclaims.BindingScope, task *domain.Task) bool {
	return task != nil &&
		task.TenantID == scope.TenantID &&
		string(task.Command) == scope.EventType &&
		task.WorkerID != "" &&
		task.WorkerID == scope.Subject
}

type taskLookup func(ctx context.Context, id string) *domain.Task

// denyUnlessBindingOwner refuses a binding-scoped worker request for a task
// it does not own. Missing and foreign tasks give the same 403 not-owner so
// the route is not an existence oracle. Other token kinds pass unchanged.
func denyUnlessBindingOwner(c *gin.Context, taskID string, lookup taskLookup) bool {
	scope, ok := middleware.GetBindingScope(c)
	if !ok {
		return false
	}
	if bindingOwnsTask(scope, lookup(c.Request.Context(), taskID)) {
		return false
	}
	middleware.DenyBinding(c, scope, http.StatusForbidden, middleware.CodeNotOwner, reasonNotOwner, nil)
	return true
}

func schedulerLookup(get func(ctx context.Context, id string) (*domain.Task, error)) taskLookup {
	return func(ctx context.Context, id string) *domain.Task {
		task, err := get(ctx, id)
		if err != nil {
			return nil
		}
		return task
	}
}

func resultsLookup(get func(ctx context.Context, id string) (*domain.ResultRecord, *domain.Task, error)) taskLookup {
	return func(ctx context.Context, id string) *domain.Task {
		_, task, _ := get(ctx, id)
		return task
	}
}
