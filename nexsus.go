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
	"github.com/nexsus-ws/nexsus/internal/pool"
)

var (
	ErrConnectionNotFound = errors.New("connection not found")
	ErrServerClosed       = errors.New("server closed")
	ErrInvalidMessage     = errors.New("invalid message")
	ErrBufferFull         = errors.New("message buffer full")
)

// Message represents a message sent through the WebSocket
type Message = backplane.Message

// Connection represents a WebSocket connection
type Connection interface {
	ID() string
	Send(msg *Message) error
	SendBinary(data []byte) error
	Close() error
	Context() context.Context
	SetMetadata(key string, value interface{})
	GetMetadata(key string) interface{}
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
	MaxMessageSize    int64
	AllowedOrigins    []string
}

// DefaultServerConfig returns a ServerConfig with optimized defaults
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		MaxConnections:    100000,
		WriteTimeout:      2 * time.Second,
		ReadTimeout:      60 * time.Second,
		PingInterval:      45 * time.Second,
		BufferSize:        8192,
		EnableCompression: false,
		MaxMessageSize:    1024 * 1024, // 1MB default
		AllowedOrigins:    []string{"*"},
	}
}

// Server is the main Nexsus WebSocket server
type Server struct {
	config      *ServerConfig
	backplane   Backplane
	connPool    *pool.ConnectionPool
	subscribers map[string]map[string]Connection
	conns       map[string]Connection
	mu          sync.RWMutex
	closed      chan struct{}
	wg          sync.WaitGroup
	msgChan     chan *Message
}

// NewServer creates a new Nexsus server instance
func NewServer(config *ServerConfig) *Server {
	if config == nil {
		config = DefaultServerConfig()
	}

	bp := config.Backplane
	if bp == nil {
		bp = backplane.GetDefaultBackplane()
	}

	poolSize := config.MaxConnections
	if poolSize < 1000 {
		poolSize = 1000
	}

	return &Server{
		config:      config,
		backplane:   bp,
		connPool:    pool.NewConnectionPool(poolSize),
		subscribers: make(map[string]map[string]Connection, 256),
		conns:       make(map[string]Connection, poolSize),
		closed:      make(chan struct{}),
		msgChan:     make(chan *Message, config.BufferSize),
	}
}

// Start initializes the server and starts background workers
func (s *Server) Start(ctx context.Context) error {
	if err := s.backplane.Connect(ctx); err != nil {
		return err
	}

	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		s.runBackgroundWorkers(ctx)
	}()

	go func() {
		defer s.wg.Done()
		s.processMessageQueue(ctx)
	}()

	return nil
}

// Stop gracefully shuts down the server
func (s *Server) Stop(ctx context.Context) error {
	close(s.closed)

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		if err := s.backplane.Disconnect(ctx); err != nil {
			return err
		}
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

	select {
	case s.msgChan <- msg:
	default:
		// Queue full, try direct publish
		if err := s.backplane.Publish(ctx, msg); err != nil {
			return err
		}
		return s.deliverLocal(msg)
	}

	return nil
}

// Broadcast sends a message to all connected clients
func (s *Server) Broadcast(ctx context.Context, msg *Message) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, conn := range s.conns {
		select {
		case s.msgChan <- msg:
		default:
			_ = conn.Send(msg)
		}
	}

	return nil
}

// GetConnection retrieves a connection by ID
func (s *Server) GetConnection(id string) (Connection, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	conn, ok := s.conns[id]
	if !ok {
		return nil, ErrConnectionNotFound
	}
	return conn, nil
}

// ConnectionCount returns the number of active connections
func (s *Server) ConnectionCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.conns)
}

// AddConnection registers a new connection with the server
func (s *Server) AddConnection(conn Connection) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.conns) >= s.config.MaxConnections {
		return errors.New("max connections reached")
	}

	s.conns[conn.ID()] = conn
	return nil
}

// RemoveConnection removes a connection from the server
func (s *Server) RemoveConnection(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	conn, ok := s.conns[id]
	if !ok {
		return ErrConnectionNotFound
	}

	delete(s.conns, id)

	for topic, subs := range s.subscribers {
		delete(subs, id)
		if len(subs) == 0 {
			delete(s.subscribers, topic)
		}
	}

	return conn.Close()
}

// GetAllTopics returns all active topics
func (s *Server) GetAllTopics() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topics := make([]string, 0, len(s.subscribers))
	for topic := range s.subscribers {
		topics = append(topics, topic)
	}
	return topics
}

// GetTopicSubscriberCount returns the number of subscribers for a topic
func (s *Server) GetTopicSubscriberCount(topic string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if subs, ok := s.subscribers[topic]; ok {
		return len(subs)
	}
	return 0
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

func (s *Server) processMessageQueue(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.closed:
			return
		case msg := <-s.msgChan:
			if err := s.backplane.Publish(ctx, msg); err != nil {
				continue
			}
			_ = s.deliverLocal(msg)
		}
	}
}

func (s *Server) sendKeepAlive(ctx context.Context) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pingMsg := &Message{
		ID:        "ping",
		Topic:     "__ping__",
		Payload:   json.RawMessage(`{"type":"ping","ts":` + string(time.Now().UnixNano()) + `}`),
		Timestamp: time.Now().UTC(),
	}

	for _, conn := range s.conns {
		_ = conn.Send(pingMsg)
	}
}

func (s *Server) handleBackplaneMessage(msg *Message) {
	_ = s.deliverLocal(msg)
}

func (s *Server) deliverLocal(msg *Message) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	subs, ok := s.subscribers[msg.Topic]
	if !ok {
		return nil
	}

	for _, conn := range subs {
		_ = conn.Send(msg)
	}

	return nil
}
