// Package main provides a demo server using the nexsus WebSocket library.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nexsus-ws/nexsus"
	"github.com/nexsus-ws/nexsus/server"
)

var (
	addr         = flag.String("addr", ":8080", "HTTP server address")
	maxConn      = flag.Int("max-conn", 10000, "Maximum number of connections")
	pingInterval = flag.Duration("ping-interval", 30*time.Second, "Ping interval for keep-alive")
	writeTimeout = flag.Duration("write-timeout", 5*time.Second, "Write timeout for WebSocket connections")
	readTimeout  = flag.Duration("read-timeout", 30*time.Second, "Read timeout for WebSocket connections")
	bufferSize   = flag.Int("buffer-size", 4096, "Buffer size for messages")
)

func main() {
	flag.Parse()

	// Create server configuration
	config := &nexsus.ServerConfig{
		MaxConnections:    *maxConn,
		WriteTimeout:      *writeTimeout,
		ReadTimeout:       *readTimeout,
		PingInterval:      *pingInterval,
		BufferSize:        *bufferSize,
		EnableCompression: false,
	}

	// Create Nexsus server
	ns := nexsus.NewServer(config)

	// Start the server
	ctx := context.Background()
	if err := ns.Start(ctx); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// Create WebSocket handler
	wsConfig := server.DefaultWSConfig()
	handler := server.NewHandler(ns, wsConfig)

	// Set up HTTP routes
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handler.ServeHTTP)
	mux.HandleFunc("/health", healthHandler(ns))
	mux.HandleFunc("/stats", statsHandler(ns))

	// Create HTTP server
	httpServer := &http.Server{
		Addr:         *addr,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Channel to listen for shutdown signals
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// Start HTTP server in a goroutine
	go func() {
		log.Printf("Nexsus WebSocket server starting on %s", *addr)
		log.Printf("WebSocket endpoint: ws://localhost%s/ws", *addr)
		log.Printf("Health check: http://localhost%s/health", *addr)
		log.Printf("Stats endpoint: http://localhost%s/stats", *addr)
		log.Printf("Max connections: %d", *maxConn)
		log.Printf("Press Ctrl+C to stop")

		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	<-shutdown
	log.Println("Shutting down server...")

	// Graceful shutdown with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Stop Nexsus server
	if err := ns.Stop(shutdownCtx); err != nil {
		log.Printf("Error stopping Nexsus server: %v", err)
	}

	// Shutdown HTTP server
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error shutting down HTTP server: %v", err)
	}

	log.Println("Server stopped gracefully")
}

func healthHandler(ns *nexsus.Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"healthy","connections":%d}`, ns.ConnectionCount())
	}
}

func statsHandler(ns *nexsus.Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"connections":%d,"uptime":"%s"}`, 
			ns.ConnectionCount(), 
			time.Since(time.Now()).String())
	}
}
