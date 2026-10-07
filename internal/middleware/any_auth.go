package middleware

import (
	"net/http"

	"github.com/osvaldoandrade/codeq/internal/authclaims"
	"github.com/osvaldoandrade/codeq/pkg/auth"
	"github.com/osvaldoandrade/codeq/pkg/config"

	"github.com/gin-gonic/gin"
)

// AnyAuthMiddleware creates middleware that accepts either worker or producer tokens
func AnyAuthMiddleware(workerValidator, producerValidator auth.Validator, cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		if claims, ok := validateWith(workerValidator, header); ok {
			if claims.HasScope(topicManageScope) {
				c.AbortWithStatus(http.StatusForbidden)
				return
			}
			// A binding candidate accepted by the worker validator is judged
			// here so an invalid one is refused instead of retried as producer.
			if len(claims.EventTypes) > 0 || authclaims.RequiresBindingScope(claims) {
				anyAuthWorker(c, claims)
				return
			}
		}

		if claims, ok := validateWith(producerValidator, header); ok {
			if claims.HasScope(topicManageScope) {
				c.AbortWithStatus(http.StatusForbidden)
				return
			}
			anyAuthProducer(c, cfg, claims)
			return
		}

		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
	}
}

func validateWith(validator auth.Validator, header string) (*auth.Claims, bool) {
	if validator == nil {
		return nil, false
	}
	claims, err := validateBearer(validator, header)
	return claims, err == nil && claims != nil
}

func anyAuthWorker(c *gin.Context, claims *auth.Claims) {
	if !authorizeBindingScope(c, claims) {
		return
	}
	tenantID, err := extractTenantID(claims)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{tenantClaimsErrorKey: tenantClaimsErrorMessage})
		return
	}
	c.Set("workerClaims", claims)
	c.Set("tenantID", tenantID)
	c.Set("authType", "worker")
	c.Next()
}

func anyAuthProducer(c *gin.Context, cfg *config.Config, claims *auth.Claims) {
	if !authorizeBindingScope(c, claims) {
		return
	}
	tenantID, err := extractTenantID(claims)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{tenantClaimsErrorKey: tenantClaimsErrorMessage})
		return
	}
	setProducerContext(c, cfg, claims, tenantID)
	c.Set("authType", "producer")
	c.Next()
}
