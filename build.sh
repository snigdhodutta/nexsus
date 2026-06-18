#!/bin/bash

# Build script for Nexsus WebSocket server
set -e

echo "Building Nexsus WebSocket server..."

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Build the server
go build -o nexsus-server ./cmd/server

echo "Build successful! Binary created at: $SCRIPT_DIR/nexsus-server"
echo ""
echo "Usage:"
echo "  ./nexsus-server [options]"
echo ""
echo "Options:"
echo "  -addr string        HTTP server address (default \":8080\")"
echo "  -max-conn int       Maximum number of connections (default 10000)"
echo "  -ping-interval duration  Ping interval for keep-alive (default 30s)"
echo "  -write-timeout duration Write timeout for WebSocket connections (default 5s)"
echo "  -read-timeout duration  Read timeout for WebSocket connections (default 30s)"
echo "  -buffer-size int    Buffer size for messages (default 4096)"
echo ""
echo "Example:"
echo "  ./nexsus-server -addr :9000 -max-conn 50000"
