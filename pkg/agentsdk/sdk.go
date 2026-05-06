package agentsdk

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	pb "github.com/wereliang/aiagw/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type Request struct {
	RequestID string
	SessionID string
	Model     string
	Messages  []Message
}

type Message struct {
	Role    string
	Content string
}

type Handler func(ctx context.Context, req *Request) Response

type StreamHandler func(ctx context.Context, req *Request, stream ResponseStream)

type ResponseStream interface {
	SendChunk(content string) error
	SendThinkingChunk(reasoning string) error
	SendMessage(role, content string) error
	SetSessionID(id string)
}

type Response struct {
	SessionID string
	Role      string
	Content   string
}

type Agent struct {
	id        string
	agentType string
	addr      string
	handler   Handler
	streamH   StreamHandler
	heartbeat time.Duration
	metadata  map[string]string

	// Reconnection settings
	reconnect        bool
	reconnectDelay   time.Duration
	maxReconnectWait time.Duration
	onConnect        func()
	onDisconnect     func(error)

	conn   *grpc.ClientConn
	stream pb.AgentGateway_ConnectClient
	mu     sync.Mutex
	done   chan struct{}
}

type Option func(*Agent)

func WithHeartbeat(d time.Duration) Option {
	return func(a *Agent) { a.heartbeat = d }
}

func WithMetadata(md map[string]string) Option {
	return func(a *Agent) { a.metadata = md }
}

func WithStreamHandler(h StreamHandler) Option {
	return func(a *Agent) { a.streamH = h }
}

func WithReconnect(enabled bool) Option {
	return func(a *Agent) { a.reconnect = enabled }
}

func WithReconnectDelay(d time.Duration) Option {
	return func(a *Agent) { a.reconnectDelay = d }
}

func WithMaxReconnectWait(d time.Duration) Option {
	return func(a *Agent) { a.maxReconnectWait = d }
}

func WithOnConnect(f func()) Option {
	return func(a *Agent) { a.onConnect = f }
}

func WithOnDisconnect(f func(error)) Option {
	return func(a *Agent) { a.onDisconnect = f }
}

func New(addr, agentID, agentType string, handler Handler, opts ...Option) *Agent {
	a := &Agent{
		id:               agentID,
		agentType:        agentType,
		addr:             addr,
		handler:          handler,
		heartbeat:        30 * time.Second,
		reconnect:        true,
		reconnectDelay:   3 * time.Second,
		maxReconnectWait: 60 * time.Second,
		done:             make(chan struct{}),
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func (a *Agent) Run(ctx context.Context) error {
	currentDelay := a.reconnectDelay

	for {
		select {
		case <-a.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := a.connect(ctx)
		if err != nil {
			if !a.reconnect {
				return err
			}
			log.Printf("connection failed: %v, retrying in %v", err, currentDelay)
			if a.onDisconnect != nil {
				a.onDisconnect(err)
			}

			select {
			case <-a.done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(currentDelay):
				currentDelay = min(currentDelay*2, a.maxReconnectWait)
				continue
			}
		}

		currentDelay = a.reconnectDelay

		if a.onConnect != nil {
			a.onConnect()
		}

		err = a.runSession(ctx)

		a.cleanup()

		if err == nil || !a.reconnect {
			return err
		}

		log.Printf("disconnected: %v, reconnecting in %v", err, currentDelay)
		if a.onDisconnect != nil {
			a.onDisconnect(err)
		}

		select {
		case <-a.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(currentDelay):
			currentDelay = min(currentDelay*2, a.maxReconnectWait)
		}
	}
}

func (a *Agent) connect(ctx context.Context) error {
	conn, err := grpc.NewClient(a.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{PermitWithoutStream: true, Time: 60 * time.Second}),
	)
	if err != nil {
		return fmt.Errorf("dial gateway: %w", err)
	}

	client := pb.NewAgentGatewayClient(conn)
	stream, err := client.Connect(ctx)
	if err != nil {
		conn.Close()
		return fmt.Errorf("connect: %w", err)
	}

	reg := &pb.AgentMessage{
		Payload: &pb.AgentMessage_Register{
			Register: &pb.AgentRegister{
				AgentId:   a.id,
				AgentType: a.agentType,
				Metadata:  a.metadata,
			},
		},
	}
	if err := stream.Send(reg); err != nil {
		conn.Close()
		return fmt.Errorf("send register: %w", err)
	}

	a.mu.Lock()
	a.conn = conn
	a.stream = stream
	a.mu.Unlock()

	return nil
}

func (a *Agent) runSession(ctx context.Context) error {
	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
	defer heartbeatCancel()

	go a.heartbeatLoop(heartbeatCtx)

	return a.recvLoop(ctx)
}

func (a *Agent) cleanup() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.stream != nil {
		a.stream.CloseSend()
		a.stream = nil
	}
	if a.conn != nil {
		a.conn.Close()
		a.conn = nil
	}
}

func (a *Agent) Close() {
	select {
	case <-a.done:
		return
	default:
		close(a.done)
	}
	a.cleanup()
}

func (a *Agent) recvLoop(ctx context.Context) error {
	for {
		msg, err := a.stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			select {
			case <-a.done:
				return nil
			default:
				return fmt.Errorf("recv: %w", err)
			}
		}

		req := msg.GetRequest()
		if req == nil {
			continue
		}

		go a.handleRequest(ctx, req)
	}
}

func (a *Agent) handleRequest(ctx context.Context, req *pb.AgentRequest) {
	sdkReq := toSDKRequest(req)

	if a.streamH != nil {
		rs := &responseStream{
			agent:     a,
			requestID: req.RequestId,
			sessionID: req.SessionId,
		}
		a.streamH(ctx, sdkReq, rs)
		rs.finish()
		return
	}

	resp := a.handler(ctx, sdkReq)

	sessionID := resp.SessionID
	if sessionID == "" {
		sessionID = req.SessionId
	}

	a.send(&pb.AgentResponse{
		RequestId: req.RequestId,
		SessionId: sessionID,
		Content: &pb.AgentResponse_Message{
			Message: &pb.ChatMessage{
				Role:    resp.Role,
				Content: resp.Content,
			},
		},
		Done: true,
	})
}

func (a *Agent) send(resp *pb.AgentResponse) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stream.Send(&pb.AgentMessage{
		Payload: &pb.AgentMessage_Response{Response: resp},
	})
}

func (a *Agent) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(a.heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-a.done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.mu.Lock()
			a.stream.Send(&pb.AgentMessage{
				Payload: &pb.AgentMessage_Heartbeat{
					Heartbeat: &pb.Heartbeat{
						Timestamp: time.Now().Unix(),
					},
				},
			})
			a.mu.Unlock()
		}
	}
}

type responseStream struct {
	agent     *Agent
	requestID string
	sessionID string
	sent      bool
}

func (rs *responseStream) SetSessionID(id string) {
	rs.sessionID = id
}

func (rs *responseStream) SendChunk(content string) error {
	rs.sent = true

	rs.agent.send(&pb.AgentResponse{
		RequestId: rs.requestID,
		SessionId: rs.sessionID,
		Content: &pb.AgentResponse_Chunk{
			Chunk: &pb.StreamChunk{Content: content},
		},
		Done: false,
	})
	return nil
}

func (rs *responseStream) SendThinkingChunk(reasoning string) error {
	rs.sent = true

	rs.agent.send(&pb.AgentResponse{
		RequestId: rs.requestID,
		SessionId: rs.sessionID,
		Content: &pb.AgentResponse_Chunk{
			Chunk: &pb.StreamChunk{ReasoningContent: reasoning},
		},
		Done: false,
	})
	return nil
}

func (rs *responseStream) SendMessage(role, content string) error {
	rs.sent = true

	rs.agent.send(&pb.AgentResponse{
		RequestId: rs.requestID,
		SessionId: rs.sessionID,
		Content: &pb.AgentResponse_Message{
			Message: &pb.ChatMessage{Role: role, Content: content},
		},
		Done: true,
	})
	return nil
}

func (rs *responseStream) finish() {
	if !rs.sent {
		rs.agent.send(&pb.AgentResponse{
			RequestId: rs.requestID,
			SessionId: rs.sessionID,
			Content: &pb.AgentResponse_Message{
				Message: &pb.ChatMessage{Role: "assistant", Content: ""},
			},
			Done: true,
		})
	}
}

func toSDKRequest(req *pb.AgentRequest) *Request {
	msgs := make([]Message, len(req.Messages))
	for i, m := range req.Messages {
		msgs[i] = Message{Role: m.Role, Content: m.Content}
	}
	return &Request{
		RequestID: req.RequestId,
		SessionID: req.SessionId,
		Model:     req.Model,
		Messages:  msgs,
	}
}
