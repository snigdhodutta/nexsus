// Package server provides HTTP server integration for Nexsus WebSocket connections.
package server

import (
"context"
"crypto/tls"
"encoding/json"
"fmt"
"net"
"net/http"
"strings"
"sync"
"time"

"github.com/google/uuid"
"github.com/nexsus-ws/nexsus"
"nhooyr.io/websocket"
"nhooyr.io/websocket/wsjson"
)

// WSConfig holds WebSocket-specific configuration.
type WSConfig struct {
CheckOrigin       func(r *http.Request) bool
Subprotocols      []string
EnableCompression bool
ReadBufferSize    int
WriteBufferSize   int
MaxMessageSize    int64
TLSConfig         *tls.Config
}

// DefaultWSConfig returns optimized WebSocket defaults.
func DefaultWSConfig() *WSConfig {
return &WSConfig{
CheckOrigin:       func(r *http.Request) bool { return true },
Subprotocols:      []string{"nexsus-v1", "graphql-ws"},
EnableCompression: false,
ReadBufferSize:    32768,
WriteBufferSize:   32768,
MaxMessageSize:    1024 * 1024,
}
}

// WSConnection wraps a WebSocket connection with Nexsus Connection interface.
type WSConnection struct {
id         string
ws         *websocket.Conn
mu         sync.Mutex
ctx        context.Context
cancel     context.CancelFunc
writeCh    chan *nexsus.Message
closed     bool
metadata   sync.Map
connCtx    context.Context
connCancel context.CancelFunc
}

// NewWSConnection creates a new WebSocket connection wrapper.
func NewWSConnection(id string, ws *websocket.Conn, bufferSize int) *WSConnection {
ctx, cancel := context.WithCancel(context.Background())
connCtx, connCancel := context.WithCancel(ctx)
conn := &WSConnection{
id:         id,
ws:         ws,
ctx:        ctx,
cancel:     cancel,
connCtx:    connCtx,
connCancel: connCancel,
writeCh:    make(chan *nexsus.Message, bufferSize),
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
return nexsus.ErrBufferFull
}
}

// SendBinary sends binary data through the WebSocket.
func (c *WSConnection) SendBinary(data []byte) error {
c.mu.Lock()
defer c.mu.Unlock()
if c.closed {
return nexsus.ErrServerClosed
}
ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
defer cancel()
err := c.ws.Write(ctx, websocket.MessageBinary, data)
if err != nil {
c.Close()
}
return err
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
c.connCancel()
for len(c.writeCh) > 0 {
select {
case <-c.writeCh:
default:
}
}
close(c.writeCh)
return c.ws.Close(websocket.StatusNormalClosure, "closed")
}

// Context returns the connection context.
func (c *WSConnection) Context() context.Context {
return c.connCtx
}

// SetMetadata sets metadata on the connection.
func (c *WSConnection) SetMetadata(key string, value interface{}) {
c.metadata.Store(key, value)
}

// GetMetadata gets metadata from the connection.
func (c *WSConnection) GetMetadata(key string) interface{} {
v, _ := c.metadata.Load(key)
return v
}

// writePump continuously writes messages to the WebSocket.
func (c *WSConnection) writePump() {
defer func() { recover() }()
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
ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
defer cancel()
err := wsjson.Write(ctx, c.ws, msg)
if err != nil {
c.Close()
}
return err
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
err := wsjson.Read(c.ctx, c.ws, &msg)
if err != nil {
return
}
handler(&msg)
}
}
}

// Handler creates an HTTP handler function for WebSocket upgrades.
type Handler struct {
server  *nexsus.Server
config  *WSConfig
origins map[string]bool
}

// NewHandler creates a new WebSocket HTTP handler.
func NewHandler(server *nexsus.Server, config *WSConfig) *Handler {
if config == nil {
config = DefaultWSConfig()
}
origins := make(map[string]bool)
for _, o := range []string{"*"} {
if o == "*" {
origins["*"] = true
break
}
origins[o] = true
}
return &Handler{
server:  server,
config:  config,
origins: origins,
}
}

// ServeHTTP handles WebSocket upgrade requests.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
if !h.validateOrigin(r) {
http.Error(w, "origin not allowed", http.StatusForbidden)
return
}
acceptOpts := &websocket.AcceptOptions{
Subprotocols:    h.config.Subprotocols,
OriginPatterns:  []string{"*"},
CompressionMode: websocket.CompressionDisabled,
}
if h.config.EnableCompression {
acceptOpts.CompressionMode = websocket.CompressionContextTakeover
}
conn, err := websocket.Accept(w, r, acceptOpts)
if err != nil {
http.Error(w, "Failed to upgrade connection", http.StatusBadRequest)
return
}
conn.SetReadLimit(h.config.MaxMessageSize)
id := generateConnectionID()
wsConn := NewWSConnection(id, conn, 256)
if err := h.server.AddConnection(wsConn); err != nil {
conn.Close(websocket.StatusPolicyViolation, "max connections reached")
http.Error(w, "Failed to add connection", http.StatusInternalServerError)
return
}
go wsConn.readPump(func(msg *nexsus.Message) {
h.handleMessage(wsConn, msg)
})
go func() {
<-wsConn.Context().Done()
h.server.RemoveConnection(id)
}()
}

func (h *Handler) validateOrigin(r *http.Request) bool {
if h.origins["*"] {
return true
}
origin := r.Header.Get("Origin")
if origin == "" {
return true
}
return h.origins[origin] || h.origins[strings.TrimPrefix(origin, "https://")] || h.origins[strings.TrimPrefix(origin, "http://")]
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
case "__ping__":
pongMsg := &nexsus.Message{
ID:        msg.ID,
Topic:     "__pong__",
Payload:   msg.Payload,
Timestamp: time.Now().UTC(),
Type:      "pong",
}
conn.Send(pongMsg)
default:
h.server.Publish(conn.Context(), msg)
}
}

func generateConnectionID() string {
return uuid.New().String()
}

// TLSConfig creates a TLS configuration for secure WebSocket connections.
func TLSConfig(certFile, keyFile string) (*tls.Config, error) {
cert, err := tls.LoadX509KeyPair(certFile, keyFile)
if err != nil {
return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
}
return &tls.Config{
Certificates: []tls.Certificate{cert},
MinVersion:   tls.VersionTLS12,
CipherSuites: []uint16{
tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
},
}, nil
}

// StartSecureServer starts an HTTPS server with WebSocket support.
func StartSecureServer(addr string, handler http.Handler, certFile, keyFile string) error {
server := &http.Server{
Addr:      addr,
Handler:   handler,
TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13},
}
return server.ListenAndServeTLS(certFile, keyFile)
}

// StartServer starts an HTTP server with optional HTTP/2 support.
func StartServer(addr string, handler http.Handler) error {
listener, err := net.Listen("tcp", addr)
if err != nil {
return err
}
defer listener.Close()
server := &http.Server{
Addr:         addr,
Handler:      handler,
ReadTimeout:  15 * time.Second,
WriteTimeout: 15 * time.Second,
IdleTimeout:  60 * time.Second,
}
return server.Serve(listener)
}
