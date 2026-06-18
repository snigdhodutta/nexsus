// Package nexsus provides a high-performance, scalable WebSocket library
// that can be used as a message broker replacement.
package nexsus

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/nexsus-ws/nexsus/backplane"
	"github.com/nexsus-ws/nexsus/backplane/memory"
	"github.com/nexsus-ws/nexsus/internal/pool"
)

var (
	ErrConnectionNotFound = errors.New("connection not found")
	ErrServerClosed       = errors.New("server closed")
	ErrInvalidMessage     = errors.New("invalid message")
)

// Message represents a message sent through the WebSocket
type Message = backplane.Message

// Connection represents a WebSocket connection
type Connection interface {
	ID() string
	Send(msg *Message) error
	Close() error
	Context() context.Context
}

// Backplane is the interface for distributed messaging backends
type Backplane = backplane.Backplane

// ServerConfig holds configuration for the Nexsus server
type ServerConfig struct {
	Backplane         Backplane
	MaxConnections    int
	WriteTimeout      time.Duration
	ReadTimeout       time.Duration
	PingInterval      time.Duration
	BufferSize        int
	EnableCompression bool
}

// DefaultServerConfig returns a ServerConfig with optimized defaults
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		MaxConnections:    10000,
		WriteTimeout:      5 * time.Second,
		ReadTimeout:       30 * time.Second,
		PingInterval:      30 * time.Second,
		BufferSize:        4096,
		EnableCompression: false,
	}
}

// Server is the main Nexsus WebSocket server
type Server struct {
	config      *ServerConfig
	backplane   Backplane
	connPool    *pool.ConnectionPool
	subscribers map[string]map[string]Connection
	mu          sync.RWMutex
	closed      chan struct{}
	wg          sync.WaitGroup
}

// NewServer creates a new Nexsus server instance
func NewServer(config *ServerConfig) *Server {
	if config == nil {
		config = DefaultServerConfig()
	}

	bp := config.Backplane
	if bp == nil {
		bp = memory.NewMemoryBackplane()
	}

	return &Server{
		config:      config,
		backplane:   bp,
		connPool:    pool.NewConnectionPool(config.MaxConnections),
		subscribers: make(map[string]map[string]Connection),
		closed:      make(chan struct{}),
	}
}

// Start initializes the server and starts background workers
func (s *Server) Start(ctx context.Context) error {
	if err := s.backplane.Connect(ctx); err != nil {
		return err
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runBackgroundWorkers(ctx)
	}()

	return nil
}

// Stop gracefully shuts down the server
func (s *Server) Stop(ctx context.Context) error {
	close(s.closed)

	if err := s.backplane.Disconnect(ctx); err != nil {
		return err
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Subscribe registers a connection to receive messages on a topic
func (s *Server) Subscribe(conn Connection, topics ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, topic := range topics {
		if s.subscribers[topic] == nil {
			s.subscribers[topic] = make(map[string]Connection)
		}
		s.subscribers[topic][conn.ID()] = conn
	}

	return s.backplane.Subscribe(topics...)
}

// Unsubscribe removes a connection from topic subscriptions
func (s *Server) Unsubscribe(conn Connection, topics ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, topic := range topics {
		if subs, ok := s.subscribers[topic]; ok {
			delete(subs, conn.ID())
			if len(subs) == 0 {
				delete(s.subscribers, topic)
			}
		}
	}

	return s.backplane.Unsubscribe(topics...)
}

// Publish sends a message to all subscribers of a topic
func (s *Server) Publish(ctx context.Context, msg *Message) error {
	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now().UTC()
	}

	if err := s.backplane.Publish(ctx, msg); err != nil {
		return err
	}

	return s.deliverLocal(msg)
}

// Broadcast sends a message to all connected clients
func (s *Server) Broadcast(ctx context.Context, msg *Message) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, subs := range s.subscribers {
		for _, conn := range subs {
			if err := conn.Send(msg); err != nil {
				continue
			}
		}
	}

	return nil
}

// GetConnection retrieves a connection by ID
func (s *Server) GetConnection(id string) (Connection, error) {
	return s.connPool.Get(id)
}

// ConnectionCount returns the number of active connections
func (s *Server) ConnectionCount() int {
	return s.connPool.Count()
}

// AddConnection registers a new connection with the server
func (s *Server) AddConnection(conn Connection) error {
	return s.connPool.Add(conn)
}

// RemoveConnection removes a connection from the server
func (s *Server) RemoveConnection(id string) error {
	conn, err := s.connPool.Remove(id)
	if err != nil {
		return err
	}

	s.mu.Lock()
	for topic, subs := range s.subscribers {
		delete(subs, id)
		if len(subs) == 0 {
			delete(s.subscribers, topic)
		}
	}
	s.mu.Unlock()

	if err := conn.Close(); err != nil {
		return err
	}

	return nil
}

func (s *Server) runBackgroundWorkers(ctx context.Context) {
	ticker := time.NewTicker(s.config.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.closed:
			return
		case <-ticker.C:
			s.sendKeepAlive(ctx)
		case msg := <-s.backplane.Messages():
			s.handleBackplaneMessage(msg)
		}
	}
}

func (s *Server) sendKeepAlive(ctx context.Context) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pingMsg := &Message{
		Topic:     "__ping__",
		Payload:   json.RawMessage(`{"type":"ping"}`),
		Timestamp: time.Now().UTC(),
	}

	for _, subs := range s.subscribers {
		for _, conn := range subs {
			_ = conn.Send(pingMsg)
		}
	}
}

func (s *Server) handleBackplaneMessage(msg *Message) {
	s.deliverLocal(msg)
}

func (s *Server) deliverLocal(msg *Message) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	subs, ok := s.subscribers[msg.Topic]
	if !ok {
		return nil
	}

	for _, conn := range subs {
		if err := conn.Send(msg); err != nil {
			continue
		}
	}

	return nil
}
