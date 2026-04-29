package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pb "github.com/wereliang/aiagw/api/proto"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// ErrGatewayNotFound is returned when a gateway instance cannot be found by ID.
var ErrGatewayNotFound = errors.New("gateway instance not found")

// Forwarder manages the registration and lookup of gateway instances in Redis.
// Each gateway node registers itself with a TTL so that stale entries expire
// automatically. Other gateway nodes can look up a peer's gRPC address to
// forward requests for agents connected to that peer.
type Forwarder struct {
	client     *redis.Client
	instanceID string
	addr       string
	ttl        time.Duration

	connMu sync.Mutex
	conns  map[string]*grpc.ClientConn
}

// NewForwarder creates a Forwarder that registers and looks up gateway
// instances in Redis. instanceID uniquely identifies this gateway node, addr
// is the gRPC listen address advertised to peers, and ttl controls how long
// the registration survives without a keepalive refresh.
func NewForwarder(client *redis.Client, instanceID, addr string, ttl time.Duration) *Forwarder {
	return &Forwarder{
		client:     client,
		instanceID: instanceID,
		addr:       addr,
		ttl:        ttl,
		conns:      make(map[string]*grpc.ClientConn),
	}
}

// gatewayKey returns the Redis key for a gateway instance hash.
func gatewayKey(instanceID string) string {
	return "gateway:" + instanceID
}

// RegisterInstance stores this gateway's address and metadata in Redis with the
// configured TTL. Call RefreshKeepalive periodically to prevent expiration.
func (f *Forwarder) RegisterInstance(ctx context.Context) error {
	key := gatewayKey(f.instanceID)
	now := time.Now().Unix()

	pipe := f.client.Pipeline()
	pipe.HSet(ctx, key, map[string]any{
		"addr":           f.addr,
		"started_at":     now,
		"last_keepalive": now,
	})
	pipe.Expire(ctx, key, f.ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// RefreshKeepalive updates the last_keepalive timestamp and resets the TTL so
// the gateway registration does not expire.
func (f *Forwarder) RefreshKeepalive(ctx context.Context) error {
	key := gatewayKey(f.instanceID)

	pipe := f.client.Pipeline()
	pipe.HSet(ctx, key, "last_keepalive", time.Now().Unix())
	pipe.Expire(ctx, key, f.ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// LookupInstance returns the gRPC address of the gateway identified by
// instanceID. Returns ErrGatewayNotFound when no such instance exists or the
// registration has expired.
func (f *Forwarder) LookupInstance(ctx context.Context, instanceID string) (string, error) {
	addr, err := f.client.HGet(ctx, gatewayKey(instanceID), "addr").Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", ErrGatewayNotFound
		}
		return "", err
	}
	return addr, nil
}

// UnregisterInstance removes this gateway's registration from Redis.
func (f *Forwarder) UnregisterInstance(ctx context.Context) error {
	return f.client.Del(ctx, gatewayKey(f.instanceID)).Err()
}

// ForwardToRemote dials a remote gateway at gatewayAddr and streams the
// response from ForwardRequest back through a channel. The agentID is sent as
// gRPC metadata so the remote gateway knows which agent should handle the
// request.
func (f *Forwarder) ForwardToRemote(ctx context.Context, gatewayAddr, agentID string, req *pb.AgentRequest) (<-chan *pb.AgentResponse, error) {
	conn, err := f.getOrDial(gatewayAddr)
	if err != nil {
		return nil, fmt.Errorf("dial remote gateway %s: %w", gatewayAddr, err)
	}

	client := pb.NewGatewayInternalClient(conn)

	md := metadata.Pairs("x-agent-id", agentID)
	ctx = metadata.NewOutgoingContext(ctx, md)

	stream, err := client.ForwardRequest(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("forward request: %w", err)
	}

	respCh := make(chan *pb.AgentResponse, 16)
	go func() {
		defer close(respCh)
		for {
			resp, err := stream.Recv()
			if err != nil {
				return
			}
			respCh <- resp
			if resp.Done {
				return
			}
		}
	}()

	return respCh, nil
}

// getOrDial returns a cached gRPC connection to addr or creates a new one.
func (f *Forwarder) getOrDial(addr string) (*grpc.ClientConn, error) {
	f.connMu.Lock()
	defer f.connMu.Unlock()

	if conn, ok := f.conns[addr]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	f.conns[addr] = conn
	return conn, nil
}

// Close tears down all cached gRPC connections.
func (f *Forwarder) Close() {
	f.connMu.Lock()
	defer f.connMu.Unlock()
	for _, conn := range f.conns {
		conn.Close()
	}
	f.conns = nil
}
