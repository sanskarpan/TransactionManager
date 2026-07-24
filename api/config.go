// Package api implements the HTTP API surface for the transaction manager.
//
// This file holds cross-handler configuration constants. Centralizing them
// (rather than scattering magic numbers through the handlers) makes the
// operational knobs grep-able and lets the runbook reference them by name.
package api

import "time"

// HTTP / lifecycle timeouts. Wired into the http.Server in cmd/server/main.go.
const (
	DefaultReadTimeout     = 15 * time.Second
	DefaultWriteTimeout    = 30 * time.Second
	DefaultIdleTimeout     = 60 * time.Second
	DefaultShutdownTimeout = 10 * time.Second
)

// DefaultLockTimeout is the per-transaction lock-acquisition timeout used
// when the caller omits lockTimeoutMs from /api/txn/begin.
const DefaultLockTimeout = 5 * time.Second

// MaxLockTimeout is the upper bound on a caller-supplied lock timeout. A
// larger value would let a single client pin a transaction (and its
// locks) indefinitely, which is a self-DoS vector under abuse.
const MaxLockTimeout = 5 * time.Minute

// MaxRequestBodyBytes is the cap on any single request body. Enforced by
// the maxBody middleware on every POST route (C-06).
const MaxRequestBodyBytes = 1 << 20 // 1 MiB

// DeadlockDetectorTickInterval is the period at which the background
// detector runs DFS over the wait-for graph. Wired in cmd/server/main.go.
const DeadlockDetectorTickInterval = 50 * time.Millisecond

// VacuumInterval is the default interval between MVCC vacuum passes.
const VacuumInterval = 30 * time.Second

// DeadlockHistoryMaxRecords bounds the in-memory deadlock history ring
// buffer so it does not grow without limit.
const DeadlockHistoryMaxRecords = 100

// SSEClientChannelBuffer is the per-SSE-subscriber channel buffer. Slow
// clients that cannot drain within this many events have their events
// dropped (Publish is non-blocking).
const SSEClientChannelBuffer = 64

// BenchmarkJobMaxRetained bounds the in-memory map of finished benchmark
// jobs awaiting result polling. Older finished jobs are evicted FIFO.
const BenchmarkJobMaxRetained = 32

// WFGStreamTickInterval is the SSE push cadence for the wait-for graph
// live stream.
const WFGStreamTickInterval = 500 * time.Millisecond

// ─── Capacity / abuse-protection bounds (H-16, H-24) ────────────────────────
//
// These caps close the trivial self-DoS vectors: unauthenticated clients
// (the default in local dev) could otherwise open unlimited SSE
// connections, begin unbounded transactions, or kick off back-to-back
// 5-minute benchmarks. Production deployments should set ADMIN_TOKEN and
// CORS_ALLOW_ORIGINS and sit behind a reverse proxy that enforces
// connection limits, but the server must still defend itself.

const (
	// MaxActiveTxns caps the number of concurrently-active transactions.
	// Exceeding it returns 503 so a single client cannot OOM the process
	// by spinning up unbounded Begin calls.
	MaxActiveTxns = 10_000

	// MaxSSEClients caps the number of concurrent SSE subscribers. Each
	// subscriber holds a goroutine, a buffered channel, and a file
	// descriptor; Publish is O(N) over subscribers, so an unbounded flood
	// also slows every publisher.
	MaxSSEClients = 256

	// BenchmarkMaxConcurrent caps the number of in-flight (non-Done)
	// benchmark jobs. Without this, an attacker could submit thousands of
	// 64-goroutine jobs that all remain in-flight simultaneously.
	BenchmarkMaxConcurrent = 4

	// RateLimitRPS is the per-remote-addr token-bucket refill rate
	// (requests/second) for the public API. Zero disables rate limiting
	// (local-dev default); production should override via env.
	RateLimitRPS = 0

	// RateLimitBurst is the token-bucket capacity for per-remote-addr
	// rate limiting.
	RateLimitBurst = 100

	// MaxConnsPerHost caps the number of simultaneous TCP connections per
	// remote host, complementing the SSE/DoS caps at the transport layer.
	MaxConnsPerHost = 256

	// MinAdminTokenLen is the minimum acceptable length (bytes) for
	// ADMIN_TOKEN. Shorter tokens are rejected at startup so a weak
	// shared secret cannot protect destructive endpoints.
	MinAdminTokenLen = 16
)
