package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

// SSEEvent is a Server-Sent Event.
type SSEEvent struct {
	Event string      `json:"event"`
	Data  interface{} `json:"data"`
}

// SSEBus broadcasts events to all connected SSE clients.
type SSEBus struct {
	mu      sync.RWMutex
	clients map[chan SSEEvent]struct{}
}

// NewSSEBus creates a new SSEBus.
func NewSSEBus() *SSEBus {
	return &SSEBus{clients: make(map[chan SSEEvent]struct{})}
}

// Subscribe adds a new client channel and returns it. Returns false (and
// does not register) if MaxSSEClients has been reached, so the caller can
// return 503 (H-16: previously there was no cap, so an unauthenticated
// client could open unlimited subscribers, each pinning a goroutine + FD).
func (b *SSEBus) Subscribe() (chan SSEEvent, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.clients) >= MaxSSEClients {
		return nil, false
	}
	ch := make(chan SSEEvent, SSEClientChannelBuffer) // M-32: was hardcoded 64
	b.clients[ch] = struct{}{}
	return ch, true
}

// Unsubscribe removes and closes a client channel.
func (b *SSEBus) Unsubscribe(ch chan SSEEvent) {
	b.mu.Lock()
	delete(b.clients, ch)
	b.mu.Unlock()
	close(ch)
}

// Publish sends an event to all subscribers (non-blocking; slow clients are dropped).
func (b *SSEBus) Publish(event SSEEvent) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.clients {
		select {
		case ch <- event:
		default:
			// Drop event for slow client
		}
	}
}

// handleSSEEvents streams events to the client.
func (s *Server) handleSSEEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// CT-04: track the SSE handler on sseWg so Server.Shutdown can
	// drain SSE goroutines in addition to benchmark goroutines.
	s.sseWg.Add(1)
	defer s.sseWg.Done()

	ch, ok := s.sseBus.Subscribe()
	if !ok {
		// H-16: subscriber cap reached.
		w.Header().Set("Retry-After", "1")
		errResponse(w, http.StatusServiceUnavailable, "SSE subscriber limit reached")
		return
	}
	defer s.sseBus.Unsubscribe(ch)

	// Tie the per-connection select to the server's sseCtx so
	// Shutdown drains this handler without waiting for the
	// per-request r.Context() to fire (which only happens after
	// httpServer.Shutdown's deadline).
	ctx, cancel := mergeContexts(r.Context(), s.sseCtx)
	defer cancel()

	reqID := reqIDFromCtx(r)
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				return
			}
			data, err := json.Marshal(evt.Data)
			if err != nil {
				// L-63: was silently `continue`.
				s.logger.Warn("sse marshal failed", "err", err, "req_id", reqID, "event", evt.Event)
				continue
			}
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evt.Event, data); err != nil {
				s.logger.Warn("sse write failed", "err", err, "req_id", reqID)
				return
			}
			flusher.Flush()
		}
	}
}

// mergeContexts returns a context that is cancelled when EITHER ctx or
// fallback is cancelled. Used to make SSE handlers exit on either the
// per-request context (client disconnect) or the server-wide shutdown
// context (CT-04).
func mergeContexts(ctx, fallback context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	out, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		select {
		case <-ctx.Done():
		case <-fallback.Done():
		}
	}()
	return out, cancel
}
