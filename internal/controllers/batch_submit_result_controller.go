package controllers

import (
	"errors"
	"net/http"

	"github.com/osvaldoandrade/codeq/internal/middleware"
	"github.com/osvaldoandrade/codeq/internal/services"
	"github.com/osvaldoandrade/codeq/pkg/domain"

	"github.com/gin-gonic/gin"
)

const maxBatchResultSize = 100

type batchSubmitResultController struct{ svc services.ResultsService }

func NewBatchSubmitResultController(s services.ResultsService) *batchSubmitResultController {
	return &batchSubmitResultController{svc: s}
}

type batchResultItem struct {
	TaskID string `json:"taskId" binding:"required"`
	domain.SubmitResultRequest
}

type batchSubmitReq struct {
	Results []batchResultItem `json:"results" binding:"required,min=1"`
}

type batchSubmitResult struct {
	TaskID string               `json:"taskId"`
	Result *domain.ResultRecord `json:"result,omitempty"`
	Error  string               `json:"error,omitempty"`
}

func (h *batchSubmitResultController) Handle(c *gin.Context) {
	var req batchSubmitReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if len(req.Results) > maxBatchResultSize {
		c.JSON(http.StatusBadRequest, gin.H{"error": "batch size exceeds maximum of 100"})
		return
	}

	claims, ok := middleware.GetWorkerClaims(c)
	if !ok || claims == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing worker claims"})
		return
	}

	// Convert to service batch items
	items := make([]domain.BatchSubmitItem, len(req.Results))
	for i, item := range req.Results {
		item.SubmitResultRequest.WorkerID = claims.Subject
		items[i] = domain.BatchSubmitItem{
			TaskID:              item.TaskID,
			SubmitResultRequest: item.SubmitResultRequest,
		}
	}

	// Use batch submit for optimized RTT reduction
	responses, err := h.submitAuthorized(c, items)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "batch submit failed"})
		return
	}

	// Convert to response format
	batchResults := make([]batchSubmitResult, len(responses))
	for i, resp := range responses {
		if resp.Error != "" {
			batchResults[i] = batchSubmitResult{TaskID: resp.TaskID, Error: resp.Error}
		} else {
			batchResults[i] = batchSubmitResult{TaskID: resp.TaskID, Result: resp.Result}
		}
	}

	c.JSON(http.StatusOK, gin.H{"results": batchResults})
}

// submitAuthorized submits the batch. For a Subscribe-binding token every
// item is first checked with the strict binding owner rule; refused items get
// the per-item error not-owner and are never submitted, preserving the
// route's per-item result semantics. Other token kinds are unchanged.
func (h *batchSubmitResultController) submitAuthorized(c *gin.Context, items []domain.BatchSubmitItem) ([]domain.BatchSubmitResponse, error) {
	scope, ok := middleware.GetBindingScope(c)
	if !ok {
		return h.svc.BatchSubmit(c.Request.Context(), items)
	}
	lookup := resultsLookup(h.svc.Get)
	responses := make([]domain.BatchSubmitResponse, len(items))
	allowed := make([]domain.BatchSubmitItem, 0, len(items))
	positions := make([]int, 0, len(items))
	for i, item := range items {
		if !bindingOwnsTask(scope, lookup(c.Request.Context(), item.TaskID)) {
			middleware.RecordBindingDenial(reasonNotOwner, c.FullPath())
			responses[i] = domain.BatchSubmitResponse{TaskID: item.TaskID, Error: middleware.CodeNotOwner}
			continue
		}
		allowed = append(allowed, item)
		positions = append(positions, i)
	}
	if len(allowed) == 0 {
		return responses, nil
	}
	submitted, err := h.svc.BatchSubmit(c.Request.Context(), allowed)
	if err != nil {
		return nil, err
	}
	if len(submitted) != len(allowed) {
		return nil, errors.New("batch submit returned a mismatched result count")
	}
	for j, response := range submitted {
		responses[positions[j]] = response
	}
	return responses, nil
}
