#!/bin/bash
set -e

PROTO_DIR="../../api/proto"
OUT_DIR="."

python -m grpc_tools.protoc \
    -I "$PROTO_DIR" \
    --python_out="$OUT_DIR" \
    --grpc_python_out="$OUT_DIR" \
    "$PROTO_DIR/agent.proto"

echo "Generated agent_pb2.py and agent_pb2_grpc.py"
