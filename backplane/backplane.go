// Package backplane defines the interface for distributed messaging backends.
package backplane

import (
	"context"
	"encoding/json"
	"time"
)

// Message represents a message sent through the WebSocket
type Message struct {
	ID        string                 `json:"id,omitempty"`
	Topic     string                 `json:"topic"`
	Payload   json.RawMessage        `json:"payload"`
	Timestamp time.Time              `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

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
}
