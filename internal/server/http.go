package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/wereliang/aiagw/internal/handler"
	"github.com/wereliang/aiagw/internal/middleware"
)

type HTTPServer struct {
	engine *gin.Engine
	server *http.Server
	port   int
}

func NewHTTPServer(
	port int,
	logger *zap.Logger,
	validAPIKeys map[string]bool,
	chatHandler *handler.ChatHandler,
	modelsHandler *handler.ModelsHandler,
	sessionHandler *handler.SessionHandler,
) *HTTPServer {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(middleware.Logging(logger))

	engine.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	v1 := engine.Group("/v1")
	v1.Use(middleware.Auth(validAPIKeys))
	{
		v1.POST("/chat/completions", chatHandler.Handle)
		v1.GET("/models", modelsHandler.Handle)
		v1.DELETE("/sessions/:session_id", sessionHandler.Handle)
	}

	return &HTTPServer{engine: engine, port: port}
}

func (s *HTTPServer) Run() error {
	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: s.engine,
	}
	return s.server.ListenAndServe()
}

func (s *HTTPServer) Shutdown(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

func (s *HTTPServer) Engine() *gin.Engine {
	return s.engine
}
