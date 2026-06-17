# Nexsus - High-Performance WebSocket Library

[![Go Version](https://img.shields.io/badge/go-1.19+-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A high-performance, efficient, and scalable Go WebSocket library that can be used as a message broker replacement for scenarios requiring real-time communication.

## Features

- **High Performance**: Built on top of the optimized `gorilla/websocket` library with minimal overhead
- **Low Latency**: Efficient message routing and connection management
- **Scalable Architecture**: Pluggable backplane system for horizontal scaling
- **Flexible**: Supports custom backends (NATS, Redis, Kafka, etc.)
- **Connection Pooling**: Efficient connection management with configurable limits
- **Topic-Based Pub/Sub**: Built-in publish/subscribe messaging pattern
- **Graceful Shutdown**: Proper cleanup and connection draining
- **Keep-Alive**: Automatic ping/pong for connection health monitoring
- **Backpressure Handling**: Non-blocking writes with configurable buffer sizes

## Installation

```bash
go get github.com/nexsus-ws/nexsus
```

## Quick Start

### Starting a Server

```go
package main

import (
    "context"
    "log"
    "net/http"
    "time"

    "github.com/nexsus-ws/nexsus"
    "github.com/nexsus-ws/nexsus/server"
)

func main() {
    // Create server configuration
    config := &nexsus.ServerConfig{
        MaxConnections: 10000,
        WriteTimeout:   5 * time.Second,
        ReadTimeout:    30 * time.Second,
        PingInterval:   30 * time.Second,
        BufferSize:     4096,
    }

    // Create Nexsus server
    ns := nexsus.NewServer(config)

    // Start the server
    if err := ns.Start(context.Background()); err != nil {
        log.Fatal(err)
    }

    // Create WebSocket handler
    handler := server.NewHandler(ns, server.DefaultWSConfig())

    // Set up HTTP route
    http.HandleFunc("/ws", handler.ServeHTTP)

    // Start HTTP server
    log.Println("Server starting on :8080")
    log.Fatal(http.ListenAndServe(":8080", nil))
}
```

### Client Example (JavaScript)

```javascript
const ws = new WebSocket('ws://localhost:8080/ws');

ws.onopen = () => {
    console.log('Connected!');
    
    // Subscribe to topics
    ws.send(JSON.stringify({
        topic: '__subscribe__',
        payload: JSON.stringify(['chat', 'notifications'])
    }));
};

ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    console.log('Received:', msg);
};

// Send a message
ws.send(JSON.stringify({
    topic: 'chat',
    payload: JSON.stringify({ text: 'Hello!' })
}));
```

## Architecture

### Core Components

1. **Server**: Main WebSocket server managing connections and message routing
2. **Connection**: Abstract interface for WebSocket connections
3. **Backplane**: Pluggable backend for distributed messaging
4. **ConnectionPool**: High-performance connection pooling

### Backplane System

The backplane system allows Nexsus to scale horizontally by distributing messages across multiple server instances:

- **Memory Backplane** (default): For single-instance deployments
- **Custom Backplanes**: Implement the `Backplane` interface for your own backend

Example custom backplane:

```go
type MyBackplane struct {
    // Your backend connection
}

func (m *MyBackplane) Connect(ctx context.Context) error {
    // Connect to your backend
    return nil
}

func (m *MyBackplane) Publish(ctx context.Context, msg *nexsus.Message) error {
    // Publish to your backend
    return nil
}

// ... implement other methods
```

## Configuration

### Server Configuration

```go
type ServerConfig struct {
    Backplane         Backplane       // Custom backplane (optional)
    MaxConnections    int             // Maximum concurrent connections
    WriteTimeout      time.Duration   // Write operation timeout
    ReadTimeout       time.Duration   // Read operation timeout
    PingInterval      time.Duration   // Keep-alive ping interval
    BufferSize        int             // Message buffer size
    EnableCompression bool            // Enable per-message compression
}
```

### WebSocket Configuration

```go
type WSConfig struct {
    CheckOrigin       func(r *http.Request) bool  // CORS validation
    Subprotocols      []string                     // Supported subprotocols
    EnableCompression bool                         // Enable compression
    ReadBufferSize    int                          // Read buffer size
    WriteBufferSize   int                          // Write buffer size
}
```

## API Reference

### Server Methods

- `NewServer(config *ServerConfig) *Server` - Create a new server instance
- `Start(ctx context.Context) error` - Start the server
- `Stop(ctx context.Context) error` - Gracefully stop the server
- `Subscribe(conn Connection, topics ...string) error` - Subscribe to topics
- `Unsubscribe(conn Connection, topics ...string) error` - Unsubscribe from topics
- `Publish(ctx context.Context, msg *Message) error` - Publish a message
- `Broadcast(ctx context.Context, msg *Message) error` - Broadcast to all connections
- `ConnectionCount() int` - Get active connection count

### Message Format

```json
{
    "id": "unique-id",
    "topic": "channel-name",
    "payload": {},
    "timestamp": "2024-01-01T00:00:00Z",
    "metadata": {}
}
```

## Special Topics

- `__subscribe__` - Subscribe to topics (send as topic with payload array)
- `__unsubscribe__` - Unsubscribe from topics
- `__ping__` - Server keep-alive ping

## Performance Tips

1. **Disable Compression**: Unless necessary, keep `EnableCompression: false` for better performance
2. **Optimize Buffer Sizes**: Adjust `BufferSize` based on your message patterns
3. **Use Connection Pooling**: The built-in pool is optimized for high concurrency
4. **Monitor Connections**: Use the `/stats` endpoint to monitor connection counts
5. **Tune Timeouts**: Adjust read/write timeouts based on your network conditions

## Building from Source

```bash
# Clone the repository
git clone https://github.com/nexsus-ws/nexsus.git
cd nexsus

# Build using the build script
./build.sh

# Or build manually
go build -o nexsus-server ./cmd/server
```

## Running the Demo Server

```bash
./nexsus-server -addr :8080 -max-conn 10000
```

### Command Line Options

- `-addr`: HTTP server address (default ":8080")
- `-max-conn`: Maximum number of connections (default 10000)
- `-ping-interval`: Ping interval for keep-alive (default 30s)
- `-write-timeout`: Write timeout (default 5s)
- `-read-timeout`: Read timeout (default 30s)
- `-buffer-size`: Buffer size for messages (default 4096)

## Testing

Connect to the WebSocket endpoint:

```bash
# Using websocat
websocat ws://localhost:8080/ws

# Using wscat
wscat -c ws://localhost:8080/ws
```

Subscribe to a topic:

```json
{"topic": "__subscribe__", "payload": ["my-topic"]}
```

Send a message:

```json
{"topic": "my-topic", "payload": {"message": "Hello!"}}
```

## License

MIT License - see [LICENSE](LICENSE) for details

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues and questions, please open an issue on GitHub.
