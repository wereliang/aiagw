package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/wereliang/aiagw/pkg/agentsdk"
)

func main() {
	addr := "localhost:19090"
	if v := os.Getenv("GATEWAY_ADDR"); v != "" {
		addr = v
	}

	agent := agentsdk.New(
		addr,
		"stream-agent-1",
		"stream-echo",
		nil,
		agentsdk.WithStreamHandler(
			func(ctx context.Context, req *agentsdk.Request, stream agentsdk.ResponseStream) {
				fmt.Printf("request: %v\n", req)
				lastMsg := req.Messages[len(req.Messages)-1]
				words := strings.Fields(lastMsg.Content)

				for _, word := range words {
					stream.SendChunk(word + " ")
				}

				fmt.Printf("response: %v\n", strings.Join(words, " "))
				stream.SendMessage("assistant", strings.Join(words, " "))
			},
		),
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

	log.Printf("stream agent connecting to gateway at %s", addr)
	if err := agent.Run(ctx); err != nil {
		log.Fatalf("agent exited: %v", err)
	}
}
