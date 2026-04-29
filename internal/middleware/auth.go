package middleware

import (
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/wereliang/aiagw/pkg/errcode"
)

const apiKeyContextKey = "api_key"

// Auth returns a Gin middleware that validates Bearer API keys against a
// configured set of valid keys stored in Redis.
func Auth(validAPIKeys map[string]bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			abortWithError(c, errcode.ErrAuthentication("missing Authorization header"))
			return
		}

		if !strings.HasPrefix(authHeader, "Bearer ") {
			abortWithError(c, errcode.ErrAuthentication("Authorization header must use Bearer scheme"))
			return
		}

		apiKey := strings.TrimPrefix(authHeader, "Bearer ")
		if apiKey == "" {
			abortWithError(c, errcode.ErrAuthentication("API key is empty"))
			return
		}

		if len(validAPIKeys) > 0 && !validAPIKeys[apiKey] {
			abortWithError(c, errcode.ErrAuthentication("invalid API key"))
			return
		}

		c.Set(apiKeyContextKey, apiKey)
		c.Next()
	}
}

func GetAPIKey(c *gin.Context) string {
	v, _ := c.Get(apiKeyContextKey)
	s, _ := v.(string)
	return s
}

func abortWithError(c *gin.Context, apiErr *errcode.APIError) {
	c.AbortWithStatusJSON(apiErr.HTTPStatus, errcode.ErrorResponse{Error: apiErr})
}
