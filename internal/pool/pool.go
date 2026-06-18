// Package pool provides a high-performance connection pool for WebSocket connections.
package pool

import (
	"errors"
	"sync"
)

// Connection represents a generic connection interface.
type Connection interface {
	ID() string
	Close() error
}

// ConnectionPool manages WebSocket connections with efficient memory usage.
type ConnectionPool struct {
	conns  map[string]Connection
	mu     sync.RWMutex
	maxCap int
}

// NewConnectionPool creates a new connection pool with the specified capacity.
func NewConnectionPool(maxCapacity int) *ConnectionPool {
	if maxCapacity < 100 {
		maxCapacity = 100
	}
	return &ConnectionPool{
		conns:  make(map[string]Connection, maxCapacity/10),
		maxCap: maxCapacity,
	}
}

// Add adds a connection to the pool.
func (p *ConnectionPool) Add(conn Connection) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.conns) >= p.maxCap {
		return errors.New("connection pool at capacity")
	}

	p.conns[conn.ID()] = conn
	return nil
}

// Get retrieves a connection by ID.
func (p *ConnectionPool) Get(id string) (Connection, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	conn, ok := p.conns[id]
	if !ok {
		return nil, errors.New("connection not found")
	}
	return conn, nil
}

// Remove removes a connection from the pool and returns it.
func (p *ConnectionPool) Remove(id string) (Connection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	conn, ok := p.conns[id]
	if !ok {
		return nil, errors.New("connection not found")
	}

	delete(p.conns, id)
	return conn, nil
}

// Count returns the number of connections in the pool.
func (p *ConnectionPool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.conns)
}

// Range iterates over all connections in the pool.
func (p *ConnectionPool) Range(fn func(id string, conn Connection) bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for id, conn := range p.conns {
		if !fn(id, conn) {
			return
		}
	}
}

// Clear removes all connections from the pool.
func (p *ConnectionPool) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns = make(map[string]Connection, p.maxCap/10)
}
