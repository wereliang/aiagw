package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/wereliang/aiagw/internal/session"
	"github.com/wereliang/aiagw/pkg/errcode"
)

type SessionHandler struct {
	sessionMgr *session.Manager
}

func NewSessionHandler(sessionMgr *session.Manager) *SessionHandler {
	return &SessionHandler{sessionMgr: sessionMgr}
}

func (h *SessionHandler) Handle(c *gin.Context) {
	sessionID := c.Param("session_id")

	if _, err := h.sessionMgr.Get(c.Request.Context(), sessionID); err != nil {
		respondError(c, errcode.ErrNotFound("session not found"))
		return
	}

	if err := h.sessionMgr.Delete(c.Request.Context(), sessionID); err != nil {
		respondError(c, errcode.ErrServiceUnavailable("failed to delete session: "+err.Error()))
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"deleted": true,
		"id":      sessionID,
	})
}
