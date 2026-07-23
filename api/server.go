// Package api provides the HTTP API server for the transaction manager.
package api

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"log/slog"

	"github.com/sanskarpan/TransactionManager/api/apiwire"
	"github.com/sanskarpan/TransactionManager/internal/deadlock"
	"github.com/sanskarpan/TransactionManager/internal/metrics"
	"github.com/sanskarpan/TransactionManager/internal/mvcc"
	"github.com/sanskarpan/TransactionManager/internal/scenario"
	"github.com/sanskarpan/TransactionManager/internal/txn"
)

// ServerConfig carries the operational knobs that main.go injects into the
// HTTP server. All fields have safe defaults so a zero-value ServerConfig
// produces a working server suitable for local development.
type ServerConfig struct {
	// Logger is the slog.Logger used for structured access logging. If
	// nil, slog.Default() is used.
	Logger *slog.Logger
	// CORSAllowOrigins is the comma-separated list of origins permitted by
	// the CORS middleware. Empty (default) preserves the permissive "*" for
	// local development; production deployments should pass the frontend
	// origin(s).
	CORSAllowOrigins []string
	// AdminToken, if non-empty, requires destructive endpoints
	// (/api/reset, /api/mvcc/vacuum, /api/benchmark/run) to carry the
	// matching X-Admin-Token header. Empty in local dev.
	AdminToken string
	// HTTPLatencyHist, if non-nil, records per-request latency in ms.
	HTTPLatencyHist *metrics.Histogram
}

// Server is the HTTP API server.
type Server struct {
	txnMgr    *txn.Manager
	deadlockD *deadlock.Detector
	wfg       *deadlock.WaitForGraph
	dlHistory *deadlock.History
	vacuum    *mvcc.Vacuum
	m         *metrics.Metrics
	sseBus    *SSEBus
	scenarios *scenario.Registry
	router    chi.Router
	logger    *slog.Logger
	cfg       ServerConfig

	// ready is set once the catalog has been seeded and the server is
	// accepting traffic. /readyz returns 503 until this is true (H-05).
	ready atomic.Bool

	// benchmarkJobs is per-Server (H-01: previously a package-level map
	// shared across all Server instances and never pruned). Job IDs are
	// unguessable 16-byte hex (H-23) so we do not need a monotonic
	// sequence counter.
	benchmarkJobs   map[string]*benchmarkJob
	benchmarkJobsMu sync.Mutex

	// jobCtx is cancelled by Shutdown so in-flight benchmark goroutines
	// exit promptly (H-15). jobWg tracks them so Shutdown can drain.
	jobCtx    context.Context
	jobCancel context.CancelFunc
	jobWg     sync.WaitGroup

	// sseCtx/sseCancel/sseWg mirror jobCtx/jobCancel/jobWg for SSE handler
	// goroutines (CT-04). Previously SSE handlers exited only on
	// r.Context().Done() firing after httpServer.Shutdown's 10s
	// timeout; they are now explicitly tracked so Shutdown can drain
	// them as part of the orderly teardown.
	sseCtx    context.Context
	sseCancel context.CancelFunc
	sseWg     sync.WaitGroup
}

// NewServer creates a new HTTP API server.
func NewServer(
	mgr *txn.Manager,
	dd *deadlock.Detector,
	wfg *deadlock.WaitForGraph,
	dlHistory *deadlock.History,
	vacuum *mvcc.Vacuum,
	m *metrics.Metrics,
) *Server {
	return NewServerWithConfig(mgr, dd, wfg, dlHistory, vacuum, m, ServerConfig{})
}

// NewServerWithConfig is the constructor used by main.go to inject
// logger, CORS, admin token, and metrics knobs.
func NewServerWithConfig(
	mgr *txn.Manager,
	dd *deadlock.Detector,
	wfg *deadlock.WaitForGraph,
	dlHistory *deadlock.History,
	vacuum *mvcc.Vacuum,
	m *metrics.Metrics,
	cfg ServerConfig,
) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	jobCtx, jobCancel := context.WithCancel(context.Background())
	sseCtx, sseCancel := context.WithCancel(context.Background())
	s := &Server{
		txnMgr:        mgr,
		deadlockD:     dd,
		wfg:           wfg,
		dlHistory:     dlHistory,
		vacuum:        vacuum,
		m:             m,
		sseBus:        NewSSEBus(),
		scenarios:     scenario.NewRegistry(),
		logger:        cfg.Logger,
		cfg:           cfg,
		benchmarkJobs: make(map[string]*benchmarkJob),
		jobCtx:        jobCtx,
		jobCancel:     jobCancel,
		sseCtx:        sseCtx,
		sseCancel:     sseCancel,
	}
	s.setupRouter()
	// CT-18: surface scenario-internal deadlocks to /api/deadlocks. The
	// deadlock_cycle scenario uses a private Manager (so it does not
	// share the main server's Detector); instead, when a
	// deadlock-class failure is observed, the scenario invokes this
	// reporter which appends a DeadlockRecord to the main history.
	if s.dlHistory != nil {
		scenario.SetDeadlockReporter(func(victim uint64, cycle []uint64) {
			s.dlHistory.Add(&deadlock.Record{
				ID:           fmt.Sprintf("dl-scenario-%d", time.Now().UnixNano()),
				DetectedAt:   time.Now(),
				Cycle:        cycle,
				Victim:       victim,
				VictimReason: "scenario-detected",
			})
		})
	}
	// CT-03: background eviction of finished benchmark jobs. Previously
	// eviction ran only inside handleBenchmarkRun after adding a new job,
	// so long-running processes that stopped accepting new benchmarks
	// accumulated Done jobs in the map forever. Cap to
	// BenchmarkJobMaxRetained; a finished job is removed once the cap
	// is exceeded, regardless of whether new jobs are being submitted.
	go s.evictFinishedBenchmarkJobsLoop()
	return s
}

// Shutdown cancels in-flight benchmark goroutines and waits for them to
// drain. main.go calls this after httpServer.Shutdown so benchmark workers
// do not continue mutating a Manager that is being torn down (H-15/H-20).
// It also performs a final eviction of finished benchmark jobs and signals
// SSE handlers to exit (CT-04).
func (s *Server) Shutdown(ctx context.Context) error {
	if s.jobCancel != nil {
		s.jobCancel()
	}
	if s.sseCancel != nil {
		s.sseCancel()
	}
	done := make(chan struct{})
	go func() {
		s.jobWg.Wait()
		s.sseWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	// Final eviction so the benchmarkJobs map does not retain
	// Done jobs after the last /api/benchmark/run.
	s.benchmarkJobsMu.Lock()
	s.evictFinishedBenchmarkJobs()
	s.benchmarkJobsMu.Unlock()
	return nil
}

// evictFinishedBenchmarkJobsLoop runs eviction periodically so that a
// process which stops accepting new benchmark submissions still drops
// Done jobs (CT-03).
func (s *Server) evictFinishedBenchmarkJobsLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.jobCtx.Done():
			return
		case <-ticker.C:
			s.benchmarkJobsMu.Lock()
			s.evictFinishedBenchmarkJobs()
			s.benchmarkJobsMu.Unlock()
		}
	}
}

func (s *Server) setupRouter() {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(apiwire.RequestID)
	r.Use(apiwire.AccessLog(s.logger))
	r.Use(apiwire.MaxBody(MaxRequestBodyBytes))
	r.Use(apiwire.CORS(s.cfg.CORSAllowOrigins))
	// M-33/H-16: always register HTTPMetrics (status-class counters are
	// independent of the latency histogram); the handler tolerates a nil
	// histogram. Previously the middleware was only registered when a
	// histogram was wired, so a misconfigured deploy lost even latency data.
	r.Use(apiwire.HTTPMetrics(s.m, s.cfg.HTTPLatencyHist))
	// H-16: per-remote-addr rate limit. No-op when RateLimitRPS==0 (local
	// dev default); production should set a non-zero value via env.
	r.Use(apiwire.RateLimit(RateLimitRPS, RateLimitBurst))

	// ─── Health ──────────────────────────────────────────────────────────
	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)

	// Transaction lifecycle
	r.Post("/api/txn/begin", s.handleBegin)
	r.Post("/api/txn/{id}/commit", s.handleCommit)
	r.Post("/api/txn/{id}/abort", s.handleAbort)
	r.Get("/api/txn/{id}/status", s.handleStatus)
	r.Get("/api/txn/active", s.handleActive)

	// Row operations
	r.Post("/api/txn/{id}/read", s.handleRead)
	r.Post("/api/txn/{id}/write", s.handleWrite)
	r.Post("/api/txn/{id}/scan", s.handleScan)
	r.Post("/api/txn/{id}/insert", s.handleInsert)
	r.Post("/api/txn/{id}/delete", s.handleDelete)

	// Savepoints
	r.Post("/api/txn/{id}/savepoint", s.handleSavepoint)
	r.Post("/api/txn/{id}/rollback-to", s.handleRollbackTo)
	r.Delete("/api/txn/{id}/savepoint/{name}", s.handleReleaseSavepoint)

	// Locks / deadlocks
	r.Get("/api/locks", s.handleLocks)
	r.Get("/api/locks/{table}/{key}", s.handleLockByResource)
	r.Get("/api/deadlocks", s.handleDeadlocks)
	r.Get("/api/wfg", s.handleWFG)

	// MVCC
	r.Get("/api/mvcc/chain/{table}/{key}", s.handleVersionChain)
	r.Get("/api/mvcc/stats", s.handleMVCCStats)
	r.With(apiwire.AdminToken(s.cfg.AdminToken)).Post("/api/mvcc/vacuum", s.handleVacuum)

	// Metrics
	r.Get("/api/metrics", s.handleMetrics)

	// Admin (gated)
	r.With(apiwire.AdminToken(s.cfg.AdminToken)).Post("/api/reset", s.handleReset)

	// Scenarios
	r.Get("/api/scenarios", s.handleListScenarios)
	r.Post("/api/scenarios/{name}/run", s.handleRunScenario)

	// Benchmark (gated)
	r.With(apiwire.AdminToken(s.cfg.AdminToken)).Post("/api/benchmark/run", s.handleBenchmarkRun)
	r.Get("/api/benchmark/results/{jobId}", s.handleBenchmarkResults)

	// SSE
	r.Get("/sse/events", s.handleSSEEvents)
	r.Get("/sse/wfg", s.handleSSEWFG)

	s.router = r
}

// MarkReady flips the readiness flag so /readyz returns 200. Called by
// main.go once the catalog is seeded and the HTTP server is listening.
func (s *Server) MarkReady() { s.ready.Store(true) }

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

// Handler returns the underlying http.Handler.
func (s *Server) Handler() http.Handler { return s.router }

// handleHealthz is a liveness probe: process is up. Does NOT check
// downstream dependencies; use /readyz for that.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// handleReadyz is a readiness probe: returns 200 only once MarkReady has
// been called AND the txn manager's catalog is non-empty. Orchestrators
// should hold traffic until this returns 200.
func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() || len(s.txnMgr.Catalog.List()) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"not ready"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}

// Logger exposes the server's structured logger for handlers that need
// to emit domain events (deadlock abort, SSI abort, etc.).
func (s *Server) Logger() *slog.Logger { return s.logger }

// SetReadyForTest flips readiness for tests that construct a Server
// without going through main.go.
func (s *Server) SetReadyForTest(ready bool) { s.ready.Store(ready) }

// MetricsForTest exposes the metrics object for regression tests that need
// to assert on counter values (e.g. H-19 no-double-count). Production code
// should not call this.
func (s *Server) MetricsForTest() *metrics.Metrics { return s.m }

// ensure unused-import guards don't grow stale
var _ = context.Background
