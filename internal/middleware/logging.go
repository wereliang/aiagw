package middleware

import (
	"bytes"
	"io"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const maxBodyLogSize = 2048

func Logging(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		var bodyStr string
		if c.Request.Method == "POST" || c.Request.Method == "PUT" || c.Request.Method == "PATCH" {
			if c.Request.Body != nil {
				bodyBytes, _ := io.ReadAll(c.Request.Body)
				c.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
				if len(bodyBytes) > maxBodyLogSize {
					bodyStr = string(bodyBytes[:maxBodyLogSize]) + "...(truncated)"
				} else {
					bodyStr = string(bodyBytes)
				}
			}
		}

		c.Next()

		fields := []zap.Field{
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("latency", time.Since(start)),
			zap.String("client_ip", c.ClientIP()),
		}

		if query != "" {
			fields = append(fields, zap.String("query", query))
		}
		if bodyStr != "" {
			fields = append(fields, zap.String("body", bodyStr))
		}

		logger.Info("request", fields...)
	}
}
