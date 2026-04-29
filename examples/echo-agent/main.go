package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/wereliang/aiagw/pkg/agentsdk"
)

func main() {
	addr := "localhost:9090"
	if v := os.Getenv("GATEWAY_ADDR"); v != "" {
		addr = v
	}

	agent := agentsdk.New(
		addr,
		"echo-agent-1",
		"echo",
		func(ctx context.Context, req *agentsdk.Request) agentsdk.Response {
			lastMsg := req.Messages[len(req.Messages)-1]
			return agentsdk.Response{
				Role:    "assistant",
				Content: "echo: " + lastMsg.Content,
			}
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		fmt.Println("\nshutting down...")
		agent.Close()
		cancel()
	}()

	log.Printf("echo agent connecting to gateway at %s", addr)
	if err := agent.Run(ctx); err != nil {
		log.Fatalf("agent exited: %v", err)
	}
}
