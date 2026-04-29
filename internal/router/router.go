package router

import (
	"context"
	"errors"

	"github.com/wereliang/aiagw/internal/agent"
)

var ErrNoAgentAvailable = errors.New("no available agent")

type Router struct {
	pool *agent.Pool
}

func New(pool *agent.Pool) *Router {
	return &Router{pool: pool}
}

// Route finds the best available agent for the given agent type using
// least-connections load balancing.
func (r *Router) Route(ctx context.Context, agentType string) (*agent.AgentInfo, error) {
	agents, err := r.pool.ListByType(ctx, agentType)
	if err != nil {
		return nil, err
	}

	var best *agent.AgentInfo
	for _, a := range agents {
		if a.Status != agent.StatusOnline {
			continue
		}
		if best == nil || a.ActiveSessions < best.ActiveSessions {
			best = a
		}
	}

	if best == nil {
		return nil, ErrNoAgentAvailable
	}

	return best, nil
}
