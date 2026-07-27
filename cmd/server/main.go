// Package main is the entry point for the Transaction Manager HTTP server.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/sanskarpan/TransactionManager/api"
	"github.com/sanskarpan/TransactionManager/api/apiwire"
	"github.com/sanskarpan/TransactionManager/internal/deadlock"
	"github.com/sanskarpan/TransactionManager/internal/lock"
	"github.com/sanskarpan/TransactionManager/internal/metrics"
	"github.com/sanskarpan/TransactionManager/internal/mvcc"
	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/txn"
	"github.com/sanskarpan/TransactionManager/internal/types"
)

// buildVersion is overridden at link time (-ldflags "-X main.buildVersion=...").
// CI tags merges to main; see .github/workflows/ci.yml.
var buildVersion = "dev"

func main() {
	logLevel := slog.LevelInfo
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		switch strings.ToLower(v) {
		case "debug":
			logLevel = slog.LevelDebug
		case "warn":
			logLevel = slog.LevelWarn
		case "error":
			logLevel = slog.LevelError
		}
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})).
		With(slog.String("service", "txn-mgr"), slog.String("version", buildVersion))
	slog.SetDefault(logger)

	addr := ":8080"
	if v := os.Getenv("LISTEN_ADDR"); v != "" {
		addr = v
	}
	corsOrigins := splitCSV(os.Getenv("CORS_ALLOW_ORIGINS"))
	adminToken := os.Getenv("ADMIN_TOKEN")

	// M-36/H-21: reject a short admin token at startup so a weak shared
	// secret cannot protect destructive endpoints. A token shorter than
	// MinAdminTokenLen is a configuration error, not a silent degradation.
	if adminToken != "" && len(adminToken) < api.MinAdminTokenLen {
		logger.Error("ADMIN_TOKEN is too short; refusing to start",
			"min_len", api.MinAdminTokenLen, "got", len(adminToken))
		os.Exit(1)
	}
	if adminToken == "" {
		logger.Warn("ADMIN_TOKEN unset — destructive admin endpoints return 403; set ADMIN_TOKEN to enable them")
	}

	// --- Storage ---
	catalog := storage.NewCatalog()
	storage.SeedCatalog(catalog)

	// --- Transaction Manager ---
	mgr := txn.NewManager(catalog)

	// Seed MVCC store from all catalog tables using txn ID 0 as seed txn
	const seedTxnID mvcc.TxnID = 0
	for _, name := range catalog.List() {
		tbl, ok := catalog.Lookup(name)
		if !ok {
			continue
		}
		rows := tbl.Scan(nil)
		mvcc.SeedMVCCStore(mgr.MVCCStore, name, rows, seedTxnID)
	}
	// Mark seed transaction as committed so versions are visible
	mgr.MarkCommitted(txn.ID(seedTxnID))

	// --- Wait-for Graph & Deadlock Detector ---
	//
	// H-09: the deadlock detector's abort callback removes the victim's
	// WFG node BEFORE invoking mgr.Abort, so the next 50ms tick cannot
	// find the same cycle again and emit a duplicate DeadlockRecord. We
	// also log the abort with the victim + cycle so on-call can triage.
	wfg := deadlock.NewWaitForGraph()
	dlHistory := deadlock.NewHistory()
	dd := deadlock.NewDetector(wfg, dlHistory, func(victimID uint64, cycle []uint64) {
		logger.Info("deadlock detected, aborting victim",
			"victim", victimID, "cycle", cycle)
		wfg.RemoveNode(victimID)
		// L-65/H-13: surface a failed victim-abort rather than discard it.
		if err := mgr.Abort(txn.ID(victimID), types.NewDeadlockError()); err != nil {
			logger.Error("deadlock-victim abort failed", "victim", victimID, "err", err)
		}
	})

	// Wire WFG adapter into manager
	mgr.WFG.AddEdgeFn = wfg.AddEdge
	mgr.WFG.RemoveEdgesFn = wfg.RemoveEdges
	mgr.WFG.RemoveNodeFn = wfg.RemoveNode

	dd.Start()
	defer dd.Stop()

	// --- Vacuum ---
	vac := mvcc.NewVacuum(mgr.MVCCStore, txn.VacuumChecker{M: mgr}, api.VacuumInterval)

	vac.Run()
	defer vac.Stop()

	// --- Metrics ---
	m := &metrics.Metrics{}

	// H-19: metrics callbacks are the single source of truth for
	// txnBegins/txnCommits/txnAborts. Handlers must not also bump them.
	// H-13: log deadlock-victim abort failures rather than discard them.
	// CT-10: classify the abort reason here so SSIAborts reflects the
	// benchmark/scenario path (previously SSIAborts was only bumped by
	// handleCommit's error branch and so was missing for all system-
	// initiated aborts).
	mgr.OnBegin = func(_ *txn.Transaction) { m.TxnBegins.Add(1) }
	mgr.OnCommit = func(_ *txn.Transaction) { m.TxnCommits.Add(1) }
	mgr.OnAbort = func(_ *txn.Transaction, reason error) {
		m.TxnAborts.Add(1)
		var txnErr *types.TxnError
		if errors.As(reason, &txnErr) {
			switch txnErr.Code {
			case types.ErrDeadlock:
				m.Deadlocks.Add(1)
			case types.ErrSerializationFailure:
				m.SSIAborts.Add(1)
			case types.ErrWriteConflict:
				m.WriteConflicts.Add(1)
			case types.ErrLockTimeout:
				m.LockTimeouts.Add(1)
			}
		}
	}
	// CT-17: a lock-timeout is not an abort (the txn stays active), so
	// OnAbort does not fire. The manager emits OnLockTimeout for the
	// LockTimeouts metric.
	mgr.OnLockTimeout = func(_ *txn.Transaction, _ lock.ResourceID) { m.LockTimeouts.Add(1) }

	// --- HTTP API ---
	httpLatency := metrics.NewHistogram([]int64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000})
	srv := api.NewServerWithConfig(mgr, dd, wfg, dlHistory, vac, m, api.ServerConfig{
		Logger:           logger,
		CORSAllowOrigins: corsOrigins,
		AdminToken:       adminToken,
		HTTPLatencyHist:  httpLatency,
	})

	httpServer := &http.Server{
		Addr:        addr,
		Handler:     srv,
		ReadTimeout: api.DefaultReadTimeout,
		// H-17: WriteTimeout is a hard deadline on the entire response-write
		// phase, which would forcibly close every SSE stream at ~30s. SSE
		// endpoints (/sse/events, /sse/wfg) hold the connection open
		// indefinitely; non-streaming handlers enforce their own per-request
		// timeouts via context. Set 0 so long-lived streams survive; the
		// ReadTimeout + ReadHeaderTimeout still bound header reads.
		WriteTimeout:      0,
		ReadHeaderTimeout: api.DefaultReadTimeout,
		IdleTimeout:       api.DefaultIdleTimeout,
		MaxHeaderBytes:    1 << 20,
		// Per-host connection limiting is not exposed on http.Server; it must
		// be enforced at the reverse proxy or via a net.Listener wrapper.
		// See docs/RUNBOOK.md.
	}

	// Bind the listener up-front so /readyz only flips after the socket is
	// actually accepting (H-18: previously MarkReady was called immediately
	// after launching the ListenAndServe goroutine, with no synchronization
	// that the listener actually bound the port — orchestrators routed
	// traffic to a dead instance on bind failure).
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("failed to bind listener", "addr", addr, "err", err)
		// H-20: do not call os.Exit from a goroutine; run defers by returning
		// from main after tearing down background workers.
		return
	}

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("transaction manager HTTP API listening", "addr", addr)
		tlsCert := os.Getenv("TLS_CERT_FILE")
		tlsKey := os.Getenv("TLS_KEY_FILE")
		if err := validateTLSConfig(tlsCert, tlsKey); err != nil {
			logger.Error("TLS configuration invalid", "err", err,
				"cert_set", tlsCert != "", "key_set", tlsKey != "")
			serveErr <- err
			return
		}
		if tlsCert != "" && tlsKey != "" {
			// H-24: TLS so an externally-reachable deploy does not serve
			// admin token + txn data in cleartext.
			serveErr <- httpServer.ServeTLS(ln, tlsCert, tlsKey)
		} else {
			serveErr <- httpServer.Serve(ln)
		}
	}()

	// Once the listener is bound, mark the server ready so /readyz returns 200.
	srv.MarkReady()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server error", "err", err)
		}
	case <-quit:
		logger.Info("shutting down")
	}

	ctx, cancel := context.WithTimeout(context.Background(), api.DefaultShutdownTimeout)
	defer cancel()
	// CT-22: cancel the server's sseCtx BEFORE httpServer.Shutdown so
	// SSE handlers exit immediately and the long-lived /sse/* connections
	// close. Without this, httpServer.Shutdown waits the full
	// DefaultShutdownTimeout (10 s) for those connections to drain.
	// The Server.Shutdown call below drains the in-flight job goroutines
	// in parallel with the HTTP shutdown.
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		if err := httpServer.Shutdown(ctx); err != nil {
			logger.Error("http shutdown error", "err", err)
		}
	}()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("benchmark/sse drain error", "err", err)
	}
	<-shutdownDone
	logger.Info("server stopped")
}

// splitCSV parses a comma-separated env var into a slice of trimmed,
// non-empty strings.
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// apiwire reference to keep the import flagged as used even if the only
// caller is in test; the package ships reusable middleware.
var _ = apiwire.RequestID

// validateTLSConfig is CT-01's fail-closed guard: both-or-neither for
// TLS_CERT_FILE and TLS_KEY_FILE. If exactly one is set, the server would
// otherwise silently fall back to plaintext HTTP and serve the admin
// token + txn data in cleartext. Exported for unit testing.
func validateTLSConfig(certFile, keyFile string) error {
	if (certFile == "") != (keyFile == "") {
		return errors.New("incomplete TLS configuration: both TLS_CERT_FILE and TLS_KEY_FILE must be set, or neither")
	}
	return nil
}
