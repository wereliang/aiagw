package agent

import (
	pb "github.com/wereliang/aiagw/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// InternalServer implements the GatewayInternal gRPC service. It allows peer
// gateway instances to forward chat requests to agents connected to this
// instance. The calling gateway passes the target agent ID via the
// "x-agent-id" gRPC metadata header.
type InternalServer struct {
	pb.UnimplementedGatewayInternalServer
	grpcServer *GRPCServer
}

// NewInternalServer creates an InternalServer backed by the given GRPCServer
// which holds the local agent connections.
func NewInternalServer(grpcServer *GRPCServer) *InternalServer {
	return &InternalServer{grpcServer: grpcServer}
}

// ForwardRequest receives an AgentRequest from a peer gateway, dispatches it
// to the locally connected agent, and streams AgentResponse messages back
// until the agent signals Done=true.
func (s *InternalServer) ForwardRequest(req *pb.AgentRequest, stream grpc.ServerStreamingServer[pb.AgentResponse]) error {
	// Extract agent ID from gRPC metadata.
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return status.Error(codes.InvalidArgument, "missing metadata")
	}

	agentIDs := md.Get("x-agent-id")
	if len(agentIDs) == 0 {
		return status.Error(codes.InvalidArgument, "missing x-agent-id metadata")
	}
	agentID := agentIDs[0]

	if !s.grpcServer.HasLocalAgent(agentID) {
		return status.Errorf(codes.NotFound, "agent %s not connected to this instance", agentID)
	}

	// Register a response handler that feeds responses into a channel.
	respCh := make(chan *pb.AgentResponse, 16)
	s.grpcServer.RegisterResponseHandler(req.GetRequestId(), func(resp *pb.AgentResponse) {
		respCh <- resp
	})
	defer s.grpcServer.UnregisterResponseHandler(req.GetRequestId())

	// Forward the request to the locally connected agent.
	if err := s.grpcServer.SendRequest(agentID, req); err != nil {
		return status.Errorf(codes.Internal, "failed to send request to agent: %v", err)
	}

	// Stream responses back to the calling gateway until Done=true or the
	// context is cancelled.
	for {
		select {
		case resp := <-respCh:
			if err := stream.Send(resp); err != nil {
				return err
			}
			if resp.GetDone() {
				return nil
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}
