.PHONY: all build clean test lint proto run help \
	docker-gateway docker-agent docker-all docker-push \
	kind-load kind-deploy kind-clean

# Build variables
BINARY_NAME=gateway
BUILD_DIR=.
CMD_DIR=./cmd/gateway
PROTO_DIR=./api/proto

# Docker variables
DOCKER_REGISTRY ?=
IMAGE_TAG ?= latest
GATEWAY_IMAGE = $(if $(DOCKER_REGISTRY),$(DOCKER_REGISTRY)/)aiagw/gateway:$(IMAGE_TAG)
AGENT_IMAGE = $(if $(DOCKER_REGISTRY),$(DOCKER_REGISTRY)/)aiagw/claude-proxy-agent:$(IMAGE_TAG)
AGENT_PY_IMAGE = $(if $(DOCKER_REGISTRY),$(DOCKER_REGISTRY)/)aiagw/claude-proxy-agent-python:$(IMAGE_TAG)

# Kind variables
KIND_CLUSTER ?= mycluster

# Go build flags
LDFLAGS=-ldflags "-s -w"

all: build

## build: Build the gateway binary
build:
	go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) $(CMD_DIR)

## build-debug: Build with debug symbols
build-debug:
	go build -o $(BUILD_DIR)/$(BINARY_NAME) $(CMD_DIR)

## clean: Remove build artifacts
clean:
	rm -f $(BUILD_DIR)/$(BINARY_NAME)
	go clean

## test: Run all tests
test:
	go test -v -race ./...

## test-cover: Run tests with coverage
test-cover:
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

## lint: Run linters
lint:
	go vet ./...
	@which staticcheck > /dev/null 2>&1 && staticcheck ./... || echo "staticcheck not installed"

## proto: Generate protobuf code
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/*.proto

## run: Build and run the gateway
run: build
	./$(BINARY_NAME) -config configs/gateway.yaml

## deps: Download dependencies
deps:
	go mod download
	go mod tidy

## docker-gateway: Build gateway Docker image
docker-gateway:
	docker build -t $(GATEWAY_IMAGE) -f deploy/docker/Dockerfile.gateway .

## docker-agent: Build claude-proxy-agent Docker image (Go)
docker-agent:
	docker build -t $(AGENT_IMAGE) -f deploy/docker/Dockerfile.claude-proxy-agent .

## docker-agent-python: Build claude-proxy-agent Docker image (Python)
docker-agent-python:
	docker build -t $(AGENT_PY_IMAGE) -f deploy/docker/Dockerfile.claude-proxy-agent-python .

## docker-all: Build all Docker images
docker-all: docker-gateway docker-agent

## docker-push: Push all Docker images to registry
docker-push: docker-all
	docker push $(GATEWAY_IMAGE)
	docker push $(AGENT_IMAGE)

## kind-load: Build images and load into kind cluster
kind-load: docker-all
	kind load docker-image $(GATEWAY_IMAGE) --name $(KIND_CLUSTER)
	kind load docker-image $(AGENT_IMAGE) --name $(KIND_CLUSTER)

## kind-deploy: Load images and apply k8s manifests to kind
kind-deploy: kind-load
	kubectl apply -f deploy/k8s/namespace.yaml
	kubectl apply -f deploy/k8s/redis.yaml
	kubectl apply -f deploy/k8s/secrets.yaml
	kubectl apply -f deploy/k8s/gateway.yaml
	kubectl apply -f deploy/k8s/claude-proxy-agent.yaml

## kind-clean: Delete all aiagw resources from kind
kind-clean:
	kubectl delete namespace aiagw --ignore-not-found

## help: Show this help
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^## ' Makefile | sed 's/## /  /'
