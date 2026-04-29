package agent

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	pb "github.com/wereliang/aiagw/api/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const requestChannelSize = 64

type ResponseHandler func(resp *pb.AgentResponse)

type agentConn struct {
	stream   pb.AgentGateway_ConnectServer
	info     *AgentInfo
	requests chan *pb.AgentRequest
	done     chan struct{}
}

type GRPCServer struct {
	pb.UnimplementedAgentGatewayServer
	pool             *Pool
	logger           *zap.Logger
	mu               sync.RWMutex
	localAgents      map[string]*agentConn
	responseHandlers map[string]ResponseHandler
}

func NewGRPCServer(pool *Pool, opts ...GRPCServerOption) *GRPCServer {
	s := &GRPCServer{
		pool:             pool,
		logger:           zap.NewNop(),
		localAgents:      make(map[string]*agentConn),
		responseHandlers: make(map[string]ResponseHandler),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

type GRPCServerOption func(*GRPCServer)

func WithLogger(logger *zap.Logger) GRPCServerOption {
	return func(s *GRPCServer) {
		s.logger = logger
	}
}

func (s *GRPCServer) Connect(stream pb.AgentGateway_ConnectServer) error {
	firstMsg, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Internal, "failed to receive first message: %v", err)
	}

	reg := firstMsg.GetRegister()
	if reg == nil {
		return status.Error(codes.InvalidArgument, "first message must be AgentRegister")
	}

	info := &AgentInfo{
		ID:              reg.GetAgentId(),
		AgentType:       reg.GetAgentType(),
		GatewayInstance: s.pool.gatewayInstance,
		Status:          StatusOnline,
	}

	if err := s.pool.Register(stream.Context(), info); err != nil {
		s.logger.Warn("failed to register agent",
			zap.Any("agent_info", info),
			zap.Error(err),
		)
		return status.Errorf(codes.Internal, "failed to register agent: %v", err)
	}

	conn := &agentConn{
		stream:   stream,
		info:     info,
		requests: make(chan *pb.AgentRequest, requestChannelSize),
		done:     make(chan struct{}),
	}

	s.mu.Lock()
	s.localAgents[info.ID] = conn
	s.mu.Unlock()

	s.logger.Info("agent connected",
		zap.String("agent_id", info.ID),
		zap.String("agent_type", info.AgentType),
	)

	go s.sendLoop(conn)

	recvErr := s.recvLoop(conn)

	close(conn.done)

	s.mu.Lock()
	delete(s.localAgents, info.ID)
	s.mu.Unlock()

	unregCtx, unregCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer unregCancel()
	if unregErr := s.pool.Unregister(unregCtx, info.ID, info.AgentType); unregErr != nil {
		s.logger.Error("failed to unregister agent from pool",
			zap.String("agent_id", info.ID),
			zap.Error(unregErr),
		)
	}

	s.logger.Info("agent disconnected",
		zap.String("agent_id", info.ID),
	)

	return recvErr
}

func (s *GRPCServer) sendLoop(conn *agentConn) {
	for {
		select {
		case <-conn.done:
			return
		case req, ok := <-conn.requests:
			if !ok {
				return
			}
			msg := &pb.GatewayMessage{
				Payload: &pb.GatewayMessage_Request{
					Request: req,
				},
			}
			if err := conn.stream.Send(msg); err != nil {
				s.logger.Error("failed to send request to agent",
					zap.String("agent_id", conn.info.ID),
					zap.String("request_id", req.GetRequestId()),
					zap.Error(err),
				)
				return
			}
		}
	}
}

func (s *GRPCServer) recvLoop(conn *agentConn) error {
	for {
		msg, err := conn.stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "recv error from agent %s: %v", conn.info.ID, err)
		}

		switch payload := msg.GetPayload().(type) {
		case *pb.AgentMessage_Heartbeat:
			hbErr := s.pool.UpdateHeartbeat(conn.stream.Context(), conn.info.AgentType, conn.info.ID)
			if hbErr == ErrAgentNotFound {
				s.logger.Warn("agent expired in Redis, forcing disconnect for reconnection",
					zap.String("agent_id", conn.info.ID),
					zap.String("agent_type", conn.info.AgentType),
				)
				return status.Errorf(codes.Aborted, "agent expired, please reconnect")
			} else if hbErr != nil {
				s.logger.Error("failed to update heartbeat",
					zap.String("agent_id", conn.info.ID),
					zap.Error(hbErr),
				)
			}

		case *pb.AgentMessage_Response:
			resp := payload.Response
			requestID := resp.GetRequestId()

			s.mu.RLock()
			handler, ok := s.responseHandlers[requestID]
			s.mu.RUnlock()

			if ok {
				handler(resp)
			} else {
				s.logger.Warn("no handler for response",
					zap.String("request_id", requestID),
					zap.String("agent_id", conn.info.ID),
				)
			}

		case *pb.AgentMessage_Register:
			s.logger.Warn("unexpected register message after handshake",
				zap.String("agent_id", conn.info.ID),
			)

		default:
			s.logger.Warn("unknown message payload from agent",
				zap.String("agent_id", conn.info.ID),
			)
		}
	}
}

func (s *GRPCServer) HasLocalAgent(agentID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.localAgents[agentID]
	return ok
}

func (s *GRPCServer) SendRequest(agentID string, req *pb.AgentRequest) error {
	s.mu.RLock()
	conn, ok := s.localAgents[agentID]
	s.mu.RUnlock()

	if !ok {
		return ErrAgentNotFound
	}

	select {
	case conn.requests <- req:
		return nil
	default:
		return fmt.Errorf("request channel full for agent %s", agentID)
	}
}

func (s *GRPCServer) RegisterResponseHandler(requestID string, handler ResponseHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responseHandlers[requestID] = handler
}

func (s *GRPCServer) UnregisterResponseHandler(requestID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.responseHandlers, requestID)
}
