.PHONY: all build clean test lint proto run help

# Build variables
BINARY_NAME=gateway
BUILD_DIR=.
CMD_DIR=./cmd/gateway
PROTO_DIR=./api/proto

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

## help: Show this help
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^## ' Makefile | sed 's/## /  /'
