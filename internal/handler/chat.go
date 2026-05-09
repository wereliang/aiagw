package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	pb "github.com/wereliang/aiagw/api/proto"
	"github.com/wereliang/aiagw/internal/agent"
	"github.com/wereliang/aiagw/internal/openai"
	"github.com/wereliang/aiagw/internal/router"
	"github.com/wereliang/aiagw/internal/session"
	"github.com/wereliang/aiagw/pkg/errcode"
)

const responseTimeout = 300 * time.Second

type ChatHandler struct {
	sessionMgr      *session.Manager
	grpcServer      *agent.GRPCServer
	router          *router.Router
	pool            *agent.Pool
	forwarder       *agent.Forwarder
	gatewayInstance string
	logger          *zap.Logger
}

func NewChatHandler(
	sessionMgr *session.Manager,
	grpcServer *agent.GRPCServer,
	rtr *router.Router,
	pool *agent.Pool,
	forwarder *agent.Forwarder,
	gatewayInstance string,
	logger *zap.Logger,
) *ChatHandler {
	return &ChatHandler{
		sessionMgr:      sessionMgr,
		grpcServer:      grpcServer,
		router:          rtr,
		pool:            pool,
		forwarder:       forwarder,
		gatewayInstance: gatewayInstance,
		logger:          logger,
	}
}

func (h *ChatHandler) Handle(c *gin.Context) {
	var req openai.ChatCompletionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, errcode.ErrInvalidRequest("invalid request body: "+err.Error()))
		return
	}

	if len(req.Messages) == 0 {
		respondError(c, errcode.ErrInvalidRequest("messages must not be empty"))
		return
	}

	agentID, rawSessionID, agentType, isExisting, err := h.resolveAgent(c, req.Model)
	if err != nil {
		return
	}

	requestID := uuid.New().String()
	agentReq := openai.ToAgentRequest(requestID, rawSessionID, &req)

	h.logger.Info("chat request",
		zap.String("request_id", requestID),
		zap.String("model", req.Model),
		zap.String("agent_id", agentID),
		zap.String("agent_type", agentType),
		zap.Bool("stream", req.Stream),
		zap.Bool("existing_session", isExisting),
		zap.String("request_session_id", rawSessionID),
		zap.Int("messages_count", len(req.Messages)),
	)

	if isExisting {
		c.Header("X-Session-Id", session.ComposeSessionID(agentType, rawSessionID))
	}

	if req.Stream {
		h.handleStream(c, agentID, requestID, agentReq, agentType, isExisting)
	} else {
		h.handleNonStream(c, agentID, requestID, agentReq, agentType, isExisting)
	}
}

func (h *ChatHandler) resolveAgent(c *gin.Context, model string) (agentID, rawSessionID, agentType string, isExisting bool, err error) {
	ctx := c.Request.Context()

	if compositeID := c.GetHeader("X-Session-Id"); compositeID != "" {
		sess, sessErr := h.sessionMgr.Get(ctx, compositeID)
		if sessErr == nil {
			if sess.AgentType == model {
				_ = h.sessionMgr.Touch(ctx, compositeID)
				_, rawID, _ := session.ParseSessionID(compositeID)
				return sess.AgentID, rawID, sess.AgentType, true, nil
			}
			h.logger.Info("session agent_type mismatch, re-routing",
				zap.String("session_agent_type", sess.AgentType),
				zap.String("request_model", model),
			)
		}
	}

	selected, routeErr := h.router.Route(ctx, model)
	if routeErr != nil {
		if errors.Is(routeErr, router.ErrNoAgentAvailable) {
			respondError(c, errcode.ErrServiceUnavailable("no agent available to handle the request"))
			return "", "", "", false, routeErr
		}
		respondError(c, errcode.ErrServiceUnavailable("routing failed: "+routeErr.Error()))
		return "", "", "", false, routeErr
	}

	_ = h.pool.IncrActiveSessions(ctx, selected.AgentType, selected.ID)

	return selected.ID, "", selected.AgentType, false, nil
}

func (h *ChatHandler) ensureSession(c *gin.Context, agentID, agentType, agentSessionID string) {
	ctx := c.Request.Context()

	rawID := agentSessionID
	if rawID == "" {
		rawID = uuid.New().String()
	}

	sess, err := h.sessionMgr.CreateWithID(ctx, rawID, agentID, agentType, h.gatewayInstance)
	if err != nil {
		return
	}

	c.Header("X-Session-Id", sess.ID)
}

func (h *ChatHandler) handleNonStream(c *gin.Context, agentID, requestID string, req *pb.AgentRequest, agentType string, isExisting bool) {
	if h.grpcServer.HasLocalAgent(agentID) {
		h.logger.Debug("handling request locally",
			zap.String("request_id", requestID),
			zap.String("agent_id", agentID),
		)
		h.handleNonStreamLocal(c, agentID, requestID, req, agentType, isExisting)
		return
	}
	h.logger.Info("forwarding request to remote gateway",
		zap.String("request_id", requestID),
		zap.String("agent_id", agentID),
		zap.String("agent_type", agentType),
	)
	h.handleNonStreamRemote(c, agentID, req, agentType, isExisting)
}

func (h *ChatHandler) handleNonStreamLocal(c *gin.Context, agentID, requestID string, req *pb.AgentRequest, agentType string, isExisting bool) {
	respCh := make(chan *pb.AgentResponse, 16)

	h.grpcServer.RegisterResponseHandler(requestID, func(resp *pb.AgentResponse) {
		respCh <- resp
	})
	defer func() {
		h.logger.Info("unregistering response handler",
			zap.String("request_id", requestID),
			zap.String("agent_id", agentID),
		)
		h.grpcServer.UnregisterResponseHandler(requestID)
	}()

	if err := h.grpcServer.SendRequest(agentID, req); err != nil {
		respondError(c, errcode.ErrServiceUnavailable("failed to send request to agent: "+err.Error()))
		return
	}

	finalResp := h.collectNonStreamResponse(c, agentID, agentType, respCh, isExisting)
	if finalResp != nil {
		result := openai.FromAgentResponse(finalResp, agentType)
		c.JSON(http.StatusOK, result)
	}
}

func (h *ChatHandler) collectNonStreamResponse(c *gin.Context, agentID, agentType string, respCh <-chan *pb.AgentResponse, isExisting bool) *pb.AgentResponse {
	var contentBuf strings.Builder
	sessionHandled := false
	requestSessionID := c.GetHeader("X-Session-Id")

	for {
		select {
		case resp, ok := <-respCh:
			if !ok {
				respondError(c, errcode.ErrServiceUnavailable("agent closed stream without responding"))
				return nil
			}

			respSessionID := resp.GetSessionId()
			if !sessionHandled && respSessionID != "" {
				compositeRespSessionID := session.ComposeSessionID(agentType, respSessionID)
				if requestSessionID != compositeRespSessionID {
					h.logger.Info("session updated",
						zap.String("agent_id", agentID),
						zap.String("request_session_id", requestSessionID),
						zap.String("response_session_id", compositeRespSessionID),
					)
					h.ensureSession(c, agentID, agentType, respSessionID)
				}
				sessionHandled = true
			}

			if chunk := resp.GetChunk(); chunk != nil {
				contentBuf.WriteString(chunk.GetContent())
			}

			if resp.GetDone() {
				if msg := resp.GetMessage(); msg != nil {
					contentBuf.Write(msg.GetContent())
				}
				finalContent, _ := json.Marshal(contentBuf.String())
				return &pb.AgentResponse{
					RequestId: resp.GetRequestId(),
					SessionId: resp.GetSessionId(),
					Content: &pb.AgentResponse_Message{
						Message: &pb.ChatMessage{
							Role:    "assistant",
							Content: finalContent,
						},
					},
					Done: true,
				}
			}

		case <-time.After(responseTimeout):
			h.logger.Warn("non-stream response timeout",
				zap.String("agent_id", agentID),
			)
			respondError(c, errcode.ErrTimeout("agent did not respond within timeout"))
			return nil

		case <-c.Request.Context().Done():
			h.logger.Warn("non-stream client disconnected",
				zap.String("agent_id", agentID),
			)
			respondError(c, errcode.ErrTimeout("request cancelled"))
			return nil
		}
	}
}

func (h *ChatHandler) handleNonStreamRemote(c *gin.Context, agentID string, req *pb.AgentRequest, agentType string, isExisting bool) {
	ctx := c.Request.Context()

	respCh, err := h.dialRemoteAgent(ctx, agentType, agentID, req)
	if err != nil {
		respondError(c, errcode.ErrServiceUnavailable("failed to forward request: "+err.Error()))
		return
	}

	finalResp := h.collectNonStreamResponse(c, agentID, agentType, respCh, isExisting)
	if finalResp != nil {
		result := openai.FromAgentResponse(finalResp, agentType)
		c.JSON(http.StatusOK, result)
	}
}

func (h *ChatHandler) handleStream(c *gin.Context, agentID, requestID string, req *pb.AgentRequest, agentType string, isExisting bool) {
	if h.grpcServer.HasLocalAgent(agentID) {
		h.logger.Debug("handling stream locally",
			zap.String("request_id", requestID),
			zap.String("agent_id", agentID),
		)
		h.handleStreamLocal(c, agentID, requestID, req, agentType, isExisting)
		return
	}
	h.logger.Info("forwarding stream to remote gateway",
		zap.String("request_id", requestID),
		zap.String("agent_id", agentID),
		zap.String("agent_type", agentType),
	)
	h.handleStreamRemote(c, agentID, req, agentType, isExisting)
}

func (h *ChatHandler) handleStreamLocal(c *gin.Context, agentID, requestID string, req *pb.AgentRequest, agentType string, isExisting bool) {
	respCh := make(chan *pb.AgentResponse, 16)

	h.grpcServer.RegisterResponseHandler(requestID, func(resp *pb.AgentResponse) {
		respCh <- resp
	})
	defer func() {
		h.logger.Info("unregistering stream handler",
			zap.String("request_id", requestID),
			zap.String("agent_id", agentID),
		)
		h.grpcServer.UnregisterResponseHandler(requestID)
	}()

	if err := h.grpcServer.SendRequest(agentID, req); err != nil {
		respondError(c, errcode.ErrServiceUnavailable("failed to send request to agent: "+err.Error()))
		return
	}

	h.writeStreamResponses(c, agentID, agentType, respCh, isExisting)
}

func (h *ChatHandler) handleStreamRemote(c *gin.Context, agentID string, req *pb.AgentRequest, agentType string, isExisting bool) {
	ctx := c.Request.Context()

	respCh, err := h.dialRemoteAgent(ctx, agentType, agentID, req)
	if err != nil {
		respondError(c, errcode.ErrServiceUnavailable("failed to forward request: "+err.Error()))
		return
	}

	h.writeStreamResponses(c, agentID, agentType, respCh, isExisting)
}

func (h *ChatHandler) writeStreamResponses(c *gin.Context, agentID, agentType string, respCh <-chan *pb.AgentResponse, isExisting bool) {
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")

	flusher, _ := c.Writer.(http.Flusher)
	sessionHandled := false
	requestSessionID := c.GetHeader("X-Session-Id")

	for {
		select {
		case resp, ok := <-respCh:
			if !ok {
				fmt.Fprint(c.Writer, "data: [DONE]\n\n")
				if flusher != nil {
					flusher.Flush()
				}
				return
			}

			respSessionID := resp.GetSessionId()
			if !sessionHandled && respSessionID != "" {
				compositeRespSessionID := session.ComposeSessionID(agentType, respSessionID)
				c.Header("X-Session-Id", compositeRespSessionID)
				if requestSessionID != compositeRespSessionID {
					h.logger.Info("session updated",
						zap.String("agent_id", agentID),
						zap.String("request_session_id", requestSessionID),
						zap.String("response_session_id", compositeRespSessionID),
					)
					h.ensureSession(c, agentID, agentType, respSessionID)
				}
				sessionHandled = true
			}

			if resp.GetDone() {
				chunk := openai.FromAgentResponseChunk(resp, agentType)
				data, _ := json.Marshal(chunk)
				fmt.Fprintf(c.Writer, "data: %s\n\n", data)
				fmt.Fprint(c.Writer, "data: [DONE]\n\n")
				if flusher != nil {
					flusher.Flush()
				}
				return
			}

			if chk := resp.GetChunk(); chk != nil {
				deltaContent := chk.GetContent()
				deltaReasoning := chk.GetReasoningContent()
				if deltaContent != "" || deltaReasoning != "" {
					chunk := openai.MakeDeltaChunk(resp.GetRequestId(), agentType, deltaContent, deltaReasoning)
					data, _ := json.Marshal(chunk)
					fmt.Fprintf(c.Writer, "data: %s\n\n", data)
					if flusher != nil {
						flusher.Flush()
					}
				}
			}

		case <-time.After(responseTimeout):
			h.logger.Warn("stream response timeout",
				zap.String("agent_id", agentID),
			)
			respondError(c, errcode.ErrTimeout("agent stream timed out"))
			return

		case <-c.Request.Context().Done():
			h.logger.Warn("stream client disconnected",
				zap.String("agent_id", agentID),
			)
			return
		}
	}
}

func (h *ChatHandler) dialRemoteAgent(ctx context.Context, agentType, agentID string, req *pb.AgentRequest) (<-chan *pb.AgentResponse, error) {
	agentInfo, err := h.pool.Get(ctx, agentType, agentID)
	if err != nil {
		return nil, fmt.Errorf("lookup agent %s: %w", agentID, err)
	}

	addr, err := h.forwarder.LookupInstance(ctx, agentInfo.GatewayInstance)
	if err != nil {
		return nil, fmt.Errorf("lookup gateway %s: %w", agentInfo.GatewayInstance, err)
	}

	h.logger.Info("dialing remote gateway",
		zap.String("request_id", req.GetRequestId()),
		zap.String("agent_id", agentID),
		zap.String("remote_gateway", agentInfo.GatewayInstance),
		zap.String("remote_addr", addr),
	)

	return h.forwarder.ForwardToRemote(ctx, addr, agentID, req)
}

func respondError(c *gin.Context, apiErr *errcode.APIError) {
	c.AbortWithStatusJSON(apiErr.HTTPStatus, errcode.ErrorResponse{Error: apiErr})
}
