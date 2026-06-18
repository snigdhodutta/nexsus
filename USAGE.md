# Nexsus Usage Guide

This guide provides detailed instructions on how to use the Nexsus WebSocket library in your projects.

## Table of Contents

1. [Basic Setup](#basic-setup)
2. [Server Configuration](#server-configuration)
3. [Client Integration](#client-integration)
4. [Publish/Subscribe Pattern](#publishsubscribe-pattern)
5. [Custom Backplane](#custom-backplane)
6. [Advanced Features](#advanced-features)
7. [Performance Optimization](#performance-optimization)
8. [Troubleshooting](#troubleshooting)

## Basic Setup

### Installing the Library

```bash
go get github.com/nexsus-ws/nexsus
```

### Minimal Server Example

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
    // Create server with default configuration
    config := nexsus.DefaultServerConfig()
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

## Server Configuration

### Custom Configuration

```go
config := &nexsus.ServerConfig{
    MaxConnections:    50000,           // Support up to 50k concurrent connections
    WriteTimeout:      3 * time.Second, // Timeout for write operations
    ReadTimeout:       60 * time.Second, // Timeout for read operations
    PingInterval:      15 * time.Second, // Send ping every 15 seconds
    BufferSize:        8192,            // Larger buffer for high throughput
    EnableCompression: false,           // Disable for better performance
}

ns := nexsus.NewServer(config)
```

### Configuration Options Explained

| Option | Default | Description |
|--------|---------|-------------|
| `MaxConnections` | 10000 | Maximum concurrent WebSocket connections |
| `WriteTimeout` | 5s | Timeout for writing messages to clients |
| `ReadTimeout` | 30s | Timeout for reading messages from clients |
| `PingInterval` | 30s | Interval for keep-alive ping messages |
| `BufferSize` | 4096 | Size of the message buffer per connection |
| `EnableCompression` | false | Enable per-message compression (reduces throughput) |

### WebSocket Handler Configuration

```go
wsConfig := &server.WSConfig{
    CheckOrigin: func(r *http.Request) bool {
        // Allow all origins (configure for production)
        return true
    },
    Subprotocols:      []string{"nexsus-v1", "graphql-ws"},
    EnableCompression: false,
    ReadBufferSize:    4096,
    WriteBufferSize:   4096,
}

handler := server.NewHandler(ns, wsConfig)
```

## Client Integration

### JavaScript Client

```javascript
class NexsusClient {
    constructor(url) {
        this.url = url;
        this.ws = null;
        this.reconnectInterval = 1000;
        this.maxReconnectInterval = 30000;
    }

    connect() {
        this.ws = new WebSocket(this.url);

        this.ws.onopen = () => {
            console.log('Connected to Nexsus');
            this.reconnectInterval = 1000; // Reset reconnect interval
        };

        this.ws.onmessage = (event) => {
            const msg = JSON.parse(event.data);
            this.handleMessage(msg);
        };

        this.ws.onclose = () => {
            console.log('Disconnected, reconnecting...');
            setTimeout(() => this.connect(), this.reconnectInterval);
            this.reconnectInterval = Math.min(
                this.reconnectInterval * 2,
                this.maxReconnectInterval
            );
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };
    }

    handleMessage(msg) {
        if (msg.topic === '__ping__') {
            // Handle ping (optional - library handles pong automatically)
            return;
        }
        console.log('Received message:', msg);
    }

    subscribe(topics) {
        this.ws.send(JSON.stringify({
            topic: '__subscribe__',
            payload: JSON.stringify(topics)
        }));
    }

    unsubscribe(topics) {
        this.ws.send(JSON.stringify({
            topic: '__unsubscribe__',
            payload: JSON.stringify(topics)
        }));
    }

    publish(topic, payload) {
        this.ws.send(JSON.stringify({
            topic: topic,
            payload: JSON.stringify(payload)
        }));
    }
}

// Usage
const client = new NexsusClient('ws://localhost:8080/ws');
client.connect();

// Subscribe to topics
client.subscribe(['chat', 'notifications']);

// Publish a message
client.publish('chat', { text: 'Hello, World!' });
```

### Go Client

```go
package main

import (
    "encoding/json"
    "log"
    "time"

    "github.com/gorilla/websocket"
)

type Message struct {
    ID        string          `json:"id,omitempty"`
    Topic     string          `json:"topic"`
    Payload   json.RawMessage `json:"payload"`
    Timestamp time.Time       `json:"timestamp"`
}

func main() {
    conn, _, err := websocket.DefaultDialer.Dial("ws://localhost:8080/ws", nil)
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    // Subscribe to topics
    subscribeMsg := map[string]interface{}{
        "topic":   "__subscribe__",
        "payload": []string{"chat", "notifications"},
    }
    conn.WriteJSON(subscribeMsg)

    // Read messages
    go func() {
        for {
            var msg Message
            if err := conn.ReadJSON(&msg); err != nil {
                log.Println("Read error:", err)
                return
            }
            log.Printf("Received: topic=%s, payload=%s", msg.Topic, msg.Payload)
        }
    }()

    // Publish a message
    publishMsg := map[string]interface{}{
        "topic":   "chat",
        "payload": map[string]string{"text": "Hello!"},
    }
    conn.WriteJSON(publishMsg)

    // Keep alive
    select {}
}
```

### Python Client

```python
import asyncio
import websockets
import json

async def nexsus_client():
    uri = "ws://localhost:8080/ws"
    async with websockets.connect(uri) as websocket:
        # Subscribe to topics
        await websocket.send(json.dumps({
            "topic": "__subscribe__",
            "payload": json.dumps(["chat", "notifications"])
        }))

        # Listen for messages
        async for message in websocket:
            msg = json.loads(message)
            print(f"Received: {msg}")

        # Publish a message
        await websocket.send(json.dumps({
            "topic": "chat",
            "payload": json.dumps({"text": "Hello!"})
        }))

asyncio.run(nexsus_client())
```

## Publish/Subscribe Pattern

### Subscribing to Topics

Clients subscribe to topics by sending a special message:

```json
{
    "topic": "__subscribe__",
    "payload": ["topic1", "topic2", "topic3"]
}
```

### Unsubscribing from Topics

```json
{
    "topic": "__unsubscribe__",
    "payload": ["topic1"]
}
```

### Publishing Messages

Any message sent to a topic (other than special topics) is published to all subscribers:

```json
{
    "topic": "chat",
    "payload": {
        "user": "alice",
        "text": "Hello everyone!"
    }
}
```

### Server-Side Publishing

You can also publish messages from the server:

```go
msg := &nexsus.Message{
    Topic:   "notifications",
    Payload: json.RawMessage(`{"type": "alert", "message": "System update"}`),
}

if err := ns.Publish(context.Background(), msg); err != nil {
    log.Printf("Publish error: %v", err)
}
```

### Broadcasting to All Connections

```go
msg := &nexsus.Message{
    Topic:   "__broadcast__",
    Payload: json.RawMessage(`{"announcement": "Maintenance at midnight"}`),
}

if err := ns.Broadcast(context.Background(), msg); err != nil {
    log.Printf("Broadcast error: %v", err)
}
```

## Custom Backplane

For horizontal scaling, implement a custom backplane:

```go
type RedisBackplane struct {
    client *redis.Client
    msgChan chan *nexsus.Message
    subs    map[string]bool
    mu      sync.RWMutex
}

func NewRedisBackplane(addr string) *RedisBackplane {
    return &RedisBackplane{
        client:  redis.NewClient(&redis.Options{Addr: addr}),
        msgChan: make(chan *nexsus.Message, 10000),
        subs:    make(map[string]bool),
    }
}

func (r *RedisBackplane) Connect(ctx context.Context) error {
    pubsub := r.client.Subscribe(ctx, "nexsus:*")
    go func() {
        ch := pubsub.Channel()
        for msg := range ch {
            var m nexsus.Message
            json.Unmarshal([]byte(msg.Payload), &m)
            r.msgChan <- &m
        }
    }()
    return nil
}

func (r *RedisBackplane) Disconnect(ctx context.Context) error {
    close(r.msgChan)
    return r.client.Close()
}

func (r *RedisBackplane) Publish(ctx context.Context, msg *nexsus.Message) error {
    data, _ := json.Marshal(msg)
    return r.client.Publish(ctx, "nexsus:"+msg.Topic, data).Err()
}

func (r *RedisBackplane) Subscribe(topics ...string) error {
    r.mu.Lock()
    defer r.mu.Unlock()
    for _, topic := range topics {
        r.subs[topic] = true
    }
    return nil
}

func (r *RedisBackplane) Unsubscribe(topics ...string) error {
    r.mu.Lock()
    defer r.mu.Unlock()
    for _, topic := range topics {
        delete(r.subs, topic)
    }
    return nil
}

func (r *RedisBackplane) Messages() <-chan *nexsus.Message {
    return r.msgChan
}

func (r *RedisBackplane) IsDistributed() bool {
    return true
}
```

Using the custom backplane:

```go
config := nexsus.DefaultServerConfig()
config.Backplane = NewRedisBackplane("localhost:6379")

ns := nexsus.NewServer(config)
```

## Advanced Features

### Connection Management

```go
// Get connection count
count := ns.ConnectionCount()
log.Printf("Active connections: %d", count)

// Get specific connection
conn, err := ns.GetConnection("connection-id")
if err != nil {
    log.Printf("Connection not found: %v", err)
}

// Remove connection
if err := ns.RemoveConnection("connection-id"); err != nil {
    log.Printf("Error removing connection: %v", err)
}
```

### Graceful Shutdown

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := ns.Stop(ctx); err != nil {
    log.Printf("Shutdown error: %v", err)
}
```

### Health Checks

The demo server includes health check endpoints:

```bash
# Check server health
curl http://localhost:8080/health

# Get statistics
curl http://localhost:8080/stats
```

## Performance Optimization

### Tuning for High Throughput

```go
config := &nexsus.ServerConfig{
    MaxConnections:    100000,          // Scale up connections
    WriteTimeout:      2 * time.Second, // Shorter timeout
    ReadTimeout:       120 * time.Second, // Longer read timeout for idle connections
    PingInterval:      60 * time.Second, // Less frequent pings
    BufferSize:        16384,           // Larger buffers
    EnableCompression: false,           // Always disable for max throughput
}
```

### Memory Optimization

```go
config := &nexsus.ServerConfig{
    MaxConnections: 10000,
    BufferSize:     1024,  // Smaller buffers for memory-constrained environments
}
```

### Low Latency Configuration

```go
config := &nexsus.ServerConfig{
    WriteTimeout: 1 * time.Second,  // Fast fail on slow writes
    BufferSize:   512,              // Small buffers for quick processing
}
```

### Load Testing

Use tools like `wrk` or `bombardier` to test performance:

```bash
# Install bombardier
go install github.com/codesenberg/bombardier@latest

# Run load test (requires WebSocket support)
bombardier -c 1000 -d 30s ws://localhost:8080/ws
```

## Troubleshooting

### Connection Issues

**Problem**: Clients cannot connect

**Solutions**:
1. Check firewall rules allow WebSocket traffic
2. Verify CORS settings in `WSConfig.CheckOrigin`
3. Ensure the server is listening on the correct address

### Message Delivery Issues

**Problem**: Messages not being delivered

**Solutions**:
1. Verify clients are subscribed to the correct topics
2. Check buffer sizes - increase if messages are being dropped
3. Monitor connection count against `MaxConnections` limit

### High Memory Usage

**Problem**: Server using too much memory

**Solutions**:
1. Reduce `BufferSize` configuration
2. Lower `MaxConnections` limit
3. Implement connection cleanup for idle clients

### Performance Issues

**Problem**: High latency or low throughput

**Solutions**:
1. Disable compression (`EnableCompression: false`)
2. Tune buffer sizes based on message patterns
3. Consider implementing a distributed backplane for horizontal scaling
4. Profile the application to identify bottlenecks

```bash
# Enable Go profiling
go tool pprof http://localhost:6060/debug/pprof/profile
```

## Best Practices

1. **Always handle errors**: Check return values from all Nexsus operations
2. **Implement reconnection logic**: Clients should automatically reconnect on disconnect
3. **Use meaningful topic names**: Organize topics hierarchically (e.g., `chat.room.general`)
4. **Monitor connection counts**: Use the stats endpoint to track active connections
5. **Set appropriate timeouts**: Balance between responsiveness and network conditions
6. **Test under load**: Perform load testing before production deployment
7. **Use structured logging**: Log connection events and errors for debugging
8. **Implement rate limiting**: Protect against message floods from clients

## Examples

See the `cmd/server` directory for a complete example server implementation.

## Support

For additional help, refer to:
- [README.md](README.md) - General overview and quick start
- GitHub Issues - Report bugs and request features
- Go Documentation - `godoc github.com/nexsus-ws/nexsus`
