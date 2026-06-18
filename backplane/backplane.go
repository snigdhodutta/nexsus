// Package backplane defines the interface for distributed messaging backends.
package backplane

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// Message represents a message sent through the WebSocket
type Message struct {
	ID        string                 `json:"id,omitempty"`
	Topic     string                 `json:"topic"`
	Payload   json.RawMessage        `json:"payload"`
	Timestamp time.Time              `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Type      MessageType            `json:"type,omitempty"`
}

// MessageType defines the type of message
type MessageType string

const (
	MessageTypeJSON    MessageType = "json"
	MessageTypeBinary  MessageType = "binary"
	MessageTypePing    MessageType = "ping"
	MessageTypePong    MessageType = "pong"
	MessageTypeClose   MessageType = "close"
)

// Backplane is the interface that all backplane implementations must satisfy.
// It enables horizontal scaling by distributing messages across multiple server instances.
type Backplane interface {
	// Connect establishes connection to the backplane service
	Connect(ctx context.Context) error

	// Disconnect gracefully closes the backplane connection
	Disconnect(ctx context.Context) error

	// Publish sends a message through the backplane to all subscribed instances
	Publish(ctx context.Context, msg *Message) error

	// Subscribe registers interest in specific topics
	Subscribe(topics ...string) error

	// Unsubscribe removes interest in specific topics
	Unsubscribe(topics ...string) error

	// Messages returns a channel for receiving messages from the backplane
	Messages() <-chan *Message

	// IsDistributed returns true if this backplane supports multi-instance communication
	IsDistributed() bool

	// Name returns the name of the backplane implementation
	Name() string
}

var (
	defaultBackplane Backplane
	backplaneMu      sync.RWMutex
)

// RegisterDefaultBackplane sets the default backplane for single-instance deployments
func RegisterDefaultBackplane(bp Backplane) {
	backplaneMu.Lock()
	defer backplaneMu.Unlock()
	defaultBackplane = bp
}

// GetDefaultBackplane returns the currently registered default backplane
func GetDefaultBackplane() Backplane {
	backplaneMu.RLock()
	defer backplaneMu.RUnlock()
	if defaultBackplane == nil {
		return nil
	}
	return defaultBackplane
}
