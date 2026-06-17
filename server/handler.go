// Package server provides HTTP server integration for Nexsus WebSocket connections.
package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nexsus-ws/nexsus"
)

// WSConfig holds WebSocket-specific configuration.
type WSConfig struct {
	// CheckOrigin function for CORS validation
	CheckOrigin func(r *http.Request) bool

	// Subprotocols supported by the server
	Subprotocols []string

	// EnableCompression enables per-message compression
	EnableCompression bool

	// ReadBufferSize for WebSocket connections
	ReadBufferSize int

	// WriteBufferSize for WebSocket connections
	WriteBufferSize int
}

// DefaultWSConfig returns optimized WebSocket defaults.
func DefaultWSConfig() *WSConfig {
	return &WSConfig{
		CheckOrigin:       func(r *http.Request) bool { return true },
		Subprotocols:      []string{"nexsus-v1"},
		EnableCompression: false,
		ReadBufferSize:    4096,
		WriteBufferSize:   4096,
	}
}

// WSConnection wraps a WebSocket connection with Nexsus Connection interface.
type WSConnection struct {
	id      string
	ws      *websocket.Conn
	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	writeCh chan *nexsus.Message
	closed  bool
}

// NewWSConnection creates a new WebSocket connection wrapper.
func NewWSConnection(id string, ws *websocket.Conn, bufferSize int) *WSConnection {
	ctx, cancel := context.WithCancel(context.Background())
	conn := &WSConnection{
		id:      id,
		ws:      ws,
		ctx:     ctx,
		cancel:  cancel,
		writeCh: make(chan *nexsus.Message, bufferSize),
	}

	go conn.writePump()
	return conn
}

// ID returns the connection ID.
func (c *WSConnection) ID() string {
	return c.id
}

// Send sends a message through the WebSocket.
func (c *WSConnection) Send(msg *nexsus.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nexsus.ErrServerClosed
	}

	select {
	case c.writeCh <- msg:
		return nil
	default:
		// Channel full, message dropped
		return nil
	}
}

// Close closes the WebSocket connection.
func (c *WSConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	c.cancel()
	close(c.writeCh)
	return c.ws.Close()
}

// Context returns the connection context.
func (c *WSConnection) Context() context.Context {
	return c.ctx
}

// writePump continuously writes messages to the WebSocket.
func (c *WSConnection) writePump() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case msg, ok := <-c.writeCh:
			if !ok {
				return
			}
			if err := c.writeMessage(msg); err != nil {
				return
			}
		}
	}
}

func (c *WSConnection) writeMessage(msg *nexsus.Message) error {
	c.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return c.ws.WriteJSON(msg)
}

// readPump continuously reads messages from the WebSocket.
func (c *WSConnection) readPump(handler func(*nexsus.Message)) {
	defer c.Close()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			var msg nexsus.Message
			if err := c.ws.ReadJSON(&msg); err != nil {
				return
			}
			handler(&msg)
		}
	}
}

// Handler creates an HTTP handler function for WebSocket upgrades.
type Handler struct {
	server   *nexsus.Server
	config   *WSConfig
	upgrader *websocket.Upgrader
}

// NewHandler creates a new WebSocket HTTP handler.
func NewHandler(server *nexsus.Server, config *WSConfig) *Handler {
	if config == nil {
		config = DefaultWSConfig()
	}

	upgrader := &websocket.Upgrader{
		CheckOrigin:       config.CheckOrigin,
		Subprotocols:      config.Subprotocols,
		EnableCompression: config.EnableCompression,
		ReadBufferSize:    config.ReadBufferSize,
		WriteBufferSize:   config.WriteBufferSize,
	}

	return &Handler{
		server:   server,
		config:   config,
		upgrader: upgrader,
	}
}

// ServeHTTP handles WebSocket upgrade requests.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, "Failed to upgrade connection", http.StatusBadRequest)
		return
	}

	id := generateConnectionID()
	wsConn := NewWSConnection(id, conn, 256)

	if err := h.server.AddConnection(wsConn); err != nil {
		conn.Close()
		http.Error(w, "Failed to add connection", http.StatusInternalServerError)
		return
	}

	// Handle incoming messages
	wsConn.readPump(func(msg *nexsus.Message) {
		h.handleMessage(wsConn, msg)
	})

	// Cleanup on disconnect
	h.server.RemoveConnection(id)
}

func (h *Handler) handleMessage(conn *WSConnection, msg *nexsus.Message) {
	switch msg.Topic {
	case "__subscribe__":
		var topics []string
		if err := json.Unmarshal(msg.Payload, &topics); err == nil {
			h.server.Subscribe(conn, topics...)
		}
	case "__unsubscribe__":
		var topics []string
		if err := json.Unmarshal(msg.Payload, &topics); err == nil {
			h.server.Unsubscribe(conn, topics...)
		}
	default:
		// Publish message to topic
		h.server.Publish(conn.Context(), msg)
	}
}

func generateConnectionID() string {
	// Simple ID generation - in production use UUID
	return time.Now().Format("20060102150405.000000")
}
