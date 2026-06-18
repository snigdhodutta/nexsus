// Package memory provides an in-memory backplane implementation for single-instance deployments.
package memory

import (
	"context"
	"errors"
	"sync"

	"github.com/nexsus-ws/nexsus/backplane"
)

// MemoryBackplane is an in-memory implementation of the Backplane interface.
// It's suitable for single-instance deployments and testing.
type MemoryBackplane struct {
	mu       sync.RWMutex
	msgChan  chan *backplane.Message
	topics   map[string]bool
	closed   bool
	subWg    sync.WaitGroup
}

// NewMemoryBackplane creates a new in-memory backplane instance.
func NewMemoryBackplane() backplane.Backplane {
	bp := &MemoryBackplane{
		msgChan: make(chan *backplane.Message, 50000),
		topics:  make(map[string]bool),
	}
	backplane.RegisterDefaultBackplane(bp)
	return bp
}

// Connect initializes the memory backplane (no-op for in-memory).
func (m *MemoryBackplane) Connect(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = false
	return nil
}

// Disconnect closes the memory backplane.
func (m *MemoryBackplane) Disconnect(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.closed {
		m.closed = true
		close(m.msgChan)
	}

	m.subWg.Wait()
	return nil
}

// Publish sends a message to the local message channel.
func (m *MemoryBackplane) Publish(ctx context.Context, msg *backplane.Message) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return errors.New("server closed")
	}

	select {
	case m.msgChan <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Channel full, drop message or handle backpressure
		return nil
	}
}

// Subscribe registers interest in topics (no-op for in-memory as all messages are broadcast).
func (m *MemoryBackplane) Subscribe(topics ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, topic := range topics {
		m.topics[topic] = true
	}

	return nil
}

// Unsubscribe removes interest in topics.
func (m *MemoryBackplane) Unsubscribe(topics ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, topic := range topics {
		delete(m.topics, topic)
	}

	return nil
}

// Messages returns the channel for receiving messages.
func (m *MemoryBackplane) Messages() <-chan *backplane.Message {
	return m.msgChan
}

// IsDistributed returns false as this is a single-instance backplane.
func (m *MemoryBackplane) IsDistributed() bool {
	return false
}

// Name returns the name of the backplane implementation.
func (m *MemoryBackplane) Name() string {
	return "memory"
}
