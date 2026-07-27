// Package apiwire groups the HTTP middleware shared across all routes:
// request-ID injection, structured access logging, body-size limiting,
// CORS, admin-token gating, and per-route latency metrics.
package apiwire

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"regexp"
	"sync"
	"time"

	"log/slog"

	"github.com/sanskarpan/TransactionManager/internal/metrics"
)

// requestIDKey is the context key carrying the per-request correlation ID.
type requestIDKey struct{}

type requestIDContextKey = requestIDKey

// RequestIDFromContext returns the request ID stored by the RequestID
// middleware, or "" if none.
func RequestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDContextKey{}).(string); ok {
		return v
	}
	return ""
}

// RequestID generates a fresh 16-byte hex request ID for every inbound
// request and stores it in the request context so handlers and log lines
// can correlate. The X-Request-ID response header echoes the value back
// to the caller for client-side correlation.
//
// M-34: a client-supplied X-Request-ID is accepted only if it matches
// ^[A-Za-z0-9_-]{1,64}$; otherwise a fresh server-side ID is generated.
// This prevents log forgery / correlation pollution from arbitrary header
// values.
var clientReqIDRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// RequestID injects a per-request correlation ID into the context and
// response header, accepting a client-supplied X-Request-ID when valid.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var id string
		if h := r.Header.Get("X-Request-ID"); h != "" && clientReqIDRe.MatchString(h) {
			id = h
		} else {
			buf := make([]byte, 16)
			_, _ = rand.Read(buf)
			id = hex.EncodeToString(buf)
		}
		w.Header().Set("X-Request-ID", id)
		ctx := context.WithValue(r.Context(), requestIDContextKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// AccessLog records one structured log line per request, including the
// request ID, method, path, status, duration, and remote address.
// Status-class log level: 5xx → ERROR, 4xx → WARN, 3xx → DEBUG, 2xx → INFO.
// (CT-08: previously the docstring said "4xx → INFO" while the code was
// 4xx → WARN — the code is correct, the doc was stale.)
func AccessLog(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rw, r)
			dur := time.Since(start)

			var level slog.Level
			switch {
			case rw.status >= 500:
				level = slog.LevelError
			case rw.status >= 400:
				level = slog.LevelWarn
			case rw.status >= 300:
				level = slog.LevelDebug
			default:
				level = slog.LevelInfo
			}

			logger.LogAttrs(r.Context(), level, "http",
				slog.String("req_id", RequestIDFromContext(r.Context())),
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rw.status),
				slog.Int64("dur_ms", dur.Milliseconds()),
				slog.String("remote", r.RemoteAddr),
			)
		})
	}
}

// statusWriter captures the response status code for logging. Because it
// embeds the http.ResponseWriter interface (not the concrete conn type),
// additional methods like Flush, Hijack, CloseNotify must be forwarded
// explicitly — they are not part of the embedded interface and would
// otherwise be lost (CT-19: SSE handlers returned HTTP 500 "streaming
// unsupported" because the type assertion w.(http.Flusher) failed).
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Flush forwards to the underlying writer if it supports streaming. The
// type assertion handles the common case where the wrapped writer is the
// real *net/http conn; httptest.ResponseRecorder does not implement
// http.Flusher, in which case we silently no-op (the test framework
// captures output regardless).
func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack forwards to the underlying writer (used by WebSocket upgrades).
func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := w.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, errors.New("hijacker not supported")
}

// CloseNotify (http.CloseNotifier) was intentionally NOT forwarded:
// the interface has been deprecated since Go 1.11 in favour of
// r.Context().Done(). Forwarding it would only mask broken
// long-poll clients and re-introduce the SA1019 warning.

// MaxBody wraps the request body with http.MaxBytesReader so a single
// client cannot stream unbounded JSON into a handler. Enforced on every
// POST route (C-06). The 413 Payload Too Large response is emitted by
// the stdlib when the cap is exceeded.
func MaxBody(maxBytes int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// CORS reflects the request's Origin header back as Access-Control-Allow-
// Origin only when it is in the allowlist (H-21: previously the allowlist was
// comma-joined and written verbatim, which browsers reject for multi-origin
// values; with the dev "*" default any site could CSRF gated endpoints).
// Empty allowOrigins preserves the permissive "*" default for local dev only;
// production must pass a real allowlist via CORS_ALLOW_ORIGINS. Vary: Origin
// is always set so caches do not pin a single-origin ACAO.
func CORS(allowOrigins []string) func(http.Handler) http.Handler {
	allowAll := len(allowOrigins) == 0
	allowSet := make(map[string]struct{}, len(allowOrigins))
	for _, o := range allowOrigins {
		allowSet[o] = struct{}{}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			switch {
			case allowAll:
				w.Header().Set("Access-Control-Allow-Origin", "*")
			case origin != "":
				if _, ok := allowSet[origin]; ok {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Add("Vary", "Origin")
				}
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Request-ID, X-Admin-Token")
			w.Header().Set("Access-Control-Expose-Headers", "X-Request-ID")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// AdminToken gates destructive endpoints behind a shared-secret header.
// If AdminToken is empty (ADMIN_TOKEN not set), the middleware returns
// HTTP 403 Forbidden so the endpoints are unavailable rather than
// unprotected. When set, requests missing X-Admin-Token or carrying the
// wrong value get 401.
//
// H-21: comparison uses crypto/subtle.ConstantTimeCompare so a remote
// caller cannot recover the token byte-by-byte via timing. A short token
// is still rejected by main.go (MinAdminTokenLen) at startup.
//
// This is a coarse authorization layer, not a substitute for real authn;
// see docs/adr/0007-admin-token.md for the threat model.
func AdminToken(token string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if token == "" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				w.Write([]byte(`{"error":"admin token not configured"}`))
				return
			}
			got := r.Header.Get("X-Admin-Token")
			if got == "" || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// HTTPMetrics records per-request latency into the supplied histogram and
// bumps a 1xx/2xx/3xx/4xx/5xx status-class counter (M-33/H-16: previously
// the status code was discarded, so operators could not detect 5xx spikes
// or 4xx abuse waves via /api/metrics). Always registered regardless of
// whether the latency histogram is wired, so status counters are never
// silently absent.
func HTTPMetrics(m *metrics.Metrics, hist *metrics.Histogram) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rw, r)
			if hist != nil {
				hist.Record(float64(time.Since(start).Milliseconds()))
			}
			if m != nil {
				m.RecordHTTPStatus(rw.status)
			}
		})
	}
}

// RateLimit applies a per-remote-addr token bucket. rps==0 disables it
// (local-dev default); production should pass a non-zero value. Burst is
// the bucket capacity. Clients that exhaust their tokens receive 429.
// H-16: without this, a single client can open unlimited SSE connections,
// begin unbounded transactions, or kick off back-to-back benchmarks.
// bucketTTL is the idle duration after which a per-IP rate-limit bucket is
// eligible for eviction. bucketSweepEvery controls how often the sweep runs
// (every N requests, measured across all clients sharing this middleware
// instance). Together they bound map growth without requiring a background
// goroutine.
const (
	bucketTTL         = 5 * time.Minute
	bucketSweepEvery  = 1000
)

func RateLimit(rps float64, burst int) func(http.Handler) http.Handler {
	if rps <= 0 || burst <= 0 {
		// Disabled — return a no-op middleware so the chain composition is
		// stable.
		return func(next http.Handler) http.Handler { return next }
	}
	var mu sync.Mutex
	buckets := make(map[string]*rateBucket)
	var reqCount int
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			b, ok := buckets[r.RemoteAddr]
			if !ok {
				b = newRateBucket(float64(burst), rps)
				buckets[r.RemoteAddr] = b
			}
			ok = b.take()
			reqCount++
			if reqCount%bucketSweepEvery == 0 {
				cutoff := time.Now().Add(-bucketTTL)
				for addr, bkt := range buckets {
					if bkt.last.Before(cutoff) {
						delete(buckets, addr)
					}
				}
			}
			mu.Unlock()
			if !ok {
				w.Header().Set("Retry-After", "1")
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// rateBucket is a minimal token bucket. Not thread-safe on its own; callers
// (RateLimit) guard it under the shared mu.
type rateBucket struct {
	tokens   float64
	last     time.Time
	refillR  float64 // tokens per second
	capacity float64
}

func newRateBucket(capacity, refillR float64) *rateBucket {
	return &rateBucket{tokens: capacity, last: time.Now(), refillR: refillR, capacity: capacity}
}

func (b *rateBucket) take() bool {
	now := time.Now()
	elapsed := now.Sub(b.last).Seconds()
	b.last = now
	b.tokens += elapsed * b.refillR
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
