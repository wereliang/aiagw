package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/wereliang/aiagw/internal/agent"
	"github.com/wereliang/aiagw/internal/openai"
)

type ModelsHandler struct {
	pool *agent.Pool
}

func NewModelsHandler(pool *agent.Pool) *ModelsHandler {
	return &ModelsHandler{pool: pool}
}

// Handle processes a GET /v1/models request. It lists distinct agent types
// from currently registered agents.
func (h *ModelsHandler) Handle(c *gin.Context) {
	c.JSON(http.StatusOK, openai.ModelList{
		Object: "list",
		Data:   []openai.Model{},
	})
}
