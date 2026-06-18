// Package pool provides high-performance connection pooling.
package pool

import (
	"context"
	"errors"
	"sync"

	"github.com/nexsus-ws/nexsus/backplane"
)

var (
	ErrPoolFull         = errors.New("connection pool is full")
	ErrConnectionExists = errors.New("connection already exists")
	ErrConnectionNotFound = errors.New("connection not found")
	ErrServerClosed     = errors.New("server closed")
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

// ConnectionPool manages WebSocket connections with high performance.
type ConnectionPool struct {
	mu          sync.RWMutex
	connections map[string]Connection
	maxSize     int
	count       int
}

// NewConnectionPool creates a new connection pool with the specified max size.
func NewConnectionPool(maxSize int) *ConnectionPool {
	return &ConnectionPool{
		connections: make(map[string]Connection, maxSize),
		maxSize:     maxSize,
	}
}

// Add adds a connection to the pool.
func (p *ConnectionPool) Add(conn Connection) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.maxSize > 0 && p.count >= p.maxSize {
		return ErrPoolFull
	}

	if _, exists := p.connections[conn.ID()]; exists {
		return ErrConnectionExists
	}

	p.connections[conn.ID()] = conn
	p.count++
	return nil
}

// Get retrieves a connection by ID.
func (p *ConnectionPool) Get(id string) (Connection, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	conn, ok := p.connections[id]
	if !ok {
		return nil, ErrConnectionNotFound
	}
	return conn, nil
}

// Remove removes a connection from the pool and returns it.
func (p *ConnectionPool) Remove(id string) (Connection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	conn, ok := p.connections[id]
	if !ok {
		return nil, ErrConnectionNotFound
	}

	delete(p.connections, id)
	p.count--
	return conn, nil
}

// Count returns the number of connections in the pool.
func (p *ConnectionPool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.count
}

// Range iterates over all connections in the pool.
func (p *ConnectionPool) Range(fn func(id string, conn Connection) bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for id, conn := range p.connections {
		if !fn(id, conn) {
			break
		}
	}
}

// Clear removes all connections from the pool.
func (p *ConnectionPool) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.connections = make(map[string]Connection, p.maxSize)
	p.count = 0
}
