package api

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"sync"
	"time"

	chi "github.com/go-chi/chi/v5"
	"github.com/sanskarpan/TransactionManager/benchmark"
)

// handleListScenarios returns all registered scenarios.
func (s *Server) handleListScenarios(w http.ResponseWriter, _ *http.Request) {
	type scenarioInfo struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		AnomalyType string `json:"anomalyType"`
	}
	all := s.scenarios.All()
	result := make([]scenarioInfo, 0, len(all))
	for _, sc := range all {
		result = append(result, scenarioInfo{
			Name:        sc.ScenarioName(),
			Description: sc.Describe(),
			AnomalyType: string(sc.AnomalyKind()),
		})
	}
	okJSON(w, result)
}

// ScenarioRunRequest parameters for running a scenario.
type ScenarioRunRequest struct {
	Protocol  string `json:"protocol"`  // "2pl" or "mvcc"
	Isolation string `json:"isolation"` // "read_uncommitted", "read_committed", "repeatable_read", "serializable"
}

// handleRunScenario executes a named scenario and returns the result.
//
// C-08: unknown protocol/isolation are rejected with 400 rather than
// silently falling back to defaults (which previously produced a scenario
// result for parameters the caller never asked for).
func (s *Server) handleRunScenario(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	sc, ok := s.scenarios.Get(name)
	if !ok {
		errResponse(w, http.StatusNotFound, "scenario not found")
		return
	}

	var req ScenarioRunRequest
	if status, msg := decodeJSON(r, &req, true); status != 0 {
		errResponse(w, status, msg)
		return
	}

	protocol, err := protocolFromString(req.Protocol)
	if err != nil {
		errResponse(w, http.StatusBadRequest, "unknown protocol")
		return
	}
	isolation, err := isolationFromString(req.Isolation)
	if err != nil {
		errResponse(w, http.StatusBadRequest, "unknown isolation level")
		return
	}

	result := sc.Run(s.txnMgr, isolation, protocol)

	type stepDTO struct {
		TxnLabel  string      `json:"txnLabel"`
		Op        string      `json:"op"`
		Table     string      `json:"table"`
		Key       string      `json:"key"`
		Result    interface{} `json:"result"`
		Error     string      `json:"error,omitempty"`
		Timestamp string      `json:"timestamp"`
		Note      string      `json:"note,omitempty"`
	}
	steps := make([]stepDTO, 0, len(result.Steps))
	for _, st := range result.Steps {
		// H-22: route step errors through the public-response sanitizer so
		// internal package paths / txn IDs / lock-state strings are not
		// leaked to the caller. AGENTS.md mandates errWrite for sanitized
		// output; the step field previously copied st.Error.Error() verbatim.
		errStr := ""
		if st.Error != nil {
			_, errStr = errToPublicResponse(st.Error)
		}
		steps = append(steps, stepDTO{
			TxnLabel:  st.TxnLabel,
			Op:        st.Op,
			Table:     st.Table,
			Key:       st.Key,
			Result:    st.Result,
			Error:     errStr,
			Timestamp: st.Timestamp.Format(time.RFC3339Nano),
			Note:      st.Note,
		})
	}
	okJSON(w, map[string]interface{}{
		"name":            result.Name,
		"isolation":       req.Isolation,
		"protocol":        req.Protocol,
		"anomalyOccurred": result.AnomalyOccurred,
		"resolved":        result.Resolved,
		"anomalyType":     string(result.AnomalyType),
		"explanation":     result.Explanation,
		"steps":           steps,
	})
}

// ─── Benchmark ────────────────────────────────────────────────────────────────

// benchmarkJob holds an async benchmark result.
type benchmarkJob struct {
	ID        string
	StartedAt time.Time
	Done      bool
	Result    benchmark.Result
	mu        sync.Mutex
}

// BenchmarkRunRequest parameters for running a benchmark.
type BenchmarkRunRequest struct {
	Protocol    string `json:"protocol"`
	Isolation   string `json:"isolation"`
	Concurrency int    `json:"concurrency"`
	DurationSec int    `json:"durationSec"`
	NumAccounts int    `json:"numAccounts"`
	MaxRetries  int    `json:"maxRetries"`
}

// handleBenchmarkRun starts an async benchmark and returns a job ID.
//
// H-01: benchmarkJobs is per-Server (not package-level), job IDs are
// monotonic (so two jobs started in the same second no longer collide),
// and the map is evicted FIFO once it exceeds BenchmarkJobMaxRetained
// finished jobs (so long-running processes don't leak memory).
//
// C-08: protocol/isolation are validated; unknown values return 400
// rather than silently running with defaults.
func (s *Server) handleBenchmarkRun(w http.ResponseWriter, r *http.Request) {
	var req BenchmarkRunRequest
	if status, msg := decodeJSON(r, &req, true); status != 0 {
		errResponse(w, status, msg)
		return
	}
	if _, err := protocolFromString(req.Protocol); err != nil {
		errResponse(w, http.StatusBadRequest, "unknown protocol")
		return
	}
	if _, err := isolationFromString(req.Isolation); err != nil {
		errResponse(w, http.StatusBadRequest, "unknown isolation level")
		return
	}
	// Validate / clamp concurrency / duration / accounts / retries so a
	// misconfigured or hostile caller cannot run a benchmark that pins the
	// process for an hour or allocates millions of rows.
	// M-37: negatives are rejected with 400 (previously silently defaulted);
	// zero/omitted still selects a sane default; implausibly large values are
	// clamped (and the effective value is echoed in the response).
	if req.Concurrency < 0 {
		errResponse(w, http.StatusBadRequest, "concurrency must be >= 0")
		return
	}
	if req.Concurrency == 0 {
		req.Concurrency = 4
	}
	if req.Concurrency > 64 {
		req.Concurrency = 64
	}
	if req.DurationSec < 0 {
		errResponse(w, http.StatusBadRequest, "durationSec must be >= 0")
		return
	}
	if req.DurationSec == 0 {
		req.DurationSec = 10
	}
	if req.DurationSec > 300 {
		req.DurationSec = 300
	}
	if req.NumAccounts < 0 {
		errResponse(w, http.StatusBadRequest, "numAccounts must be >= 0")
		return
	}
	if req.NumAccounts == 0 {
		req.NumAccounts = 100
	}
	if req.NumAccounts > 100_000 {
		req.NumAccounts = 100_000
	}
	if req.MaxRetries < 0 {
		errResponse(w, http.StatusBadRequest, "maxRetries must be >= 0")
		return
	}
	if req.MaxRetries == 0 {
		req.MaxRetries = 3
	}
	if req.MaxRetries > 10 {
		req.MaxRetries = 10
	}

	cfg := benchmark.DefaultConfig()
	cfg.Protocol, _ = protocolFromString(req.Protocol)
	cfg.Isolation, _ = isolationFromString(req.Isolation)
	cfg.Concurrency = req.Concurrency
	cfg.Duration = time.Duration(req.DurationSec) * time.Second
	cfg.NumAccounts = req.NumAccounts
	cfg.MaxRetries = req.MaxRetries

	// H-15: cap concurrent in-flight (non-Done) jobs so an attacker cannot
	// pin the process with thousands of simultaneous 64-goroutine jobs.
	s.benchmarkJobsMu.Lock()
	inflight := 0
	for _, j := range s.benchmarkJobs {
		if !j.Done {
			inflight++
		}
	}
	if inflight >= BenchmarkMaxConcurrent {
		s.benchmarkJobsMu.Unlock()
		w.Header().Set("Retry-After", "5")
		errResponse(w, http.StatusTooManyRequests, "too many concurrent benchmark jobs; retry later")
		return
	}
	// H-23: issue unguessable 16-byte hex job IDs so /api/benchmark/results/{jobId}
	// cannot be enumerated by an unauthenticated caller (the results endpoint
	// is intentionally ungated so the frontend can poll without the admin
	// token in dev).
	jobID := newUnguessableID()
	job := &benchmarkJob{ID: jobID, StartedAt: time.Now()}
	s.benchmarkJobs[jobID] = job
	s.evictFinishedBenchmarkJobs()

	// CT-21: increment jobWg INSIDE the locked section. Otherwise a
	// concurrent Shutdown could observe jobWg == 0 (since Add has not
	// run yet), call jobCancel + jobWg.Wait, return before this
	// goroutine starts, and the goroutine would then run against a
	// torn-down server (the only mitigation is that jobCtx cancel
	// makes RunCtx return quickly).
	s.jobWg.Add(1)
	s.benchmarkJobsMu.Unlock()

	// H-15: honour jobCtx so Shutdown cancels and drains in-flight
	// benchmarks cleanly.
	go func() {
		defer s.jobWg.Done()
		runner := benchmark.NewRunner(cfg, nil).WithContext(s.jobCtx)
		result := runner.RunCtx(s.jobCtx)
		job.mu.Lock()
		job.Result = result
		job.Done = true
		job.mu.Unlock()
	}()

	okJSON(w, map[string]interface{}{
		"jobId":     jobID,
		"status":    "running",
		"effective": map[string]interface{}{"concurrency": req.Concurrency, "durationSec": req.DurationSec, "numAccounts": req.NumAccounts, "maxRetries": req.MaxRetries},
	})
}

// newUnguessableID returns a 16-byte hex string suitable for unguessable,
// non-enumerable job IDs (H-23).
func newUnguessableID() string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	return "job-" + hex.EncodeToString(buf)
}

// evictFinishedBenchmarkJobs drops oldest finished jobs above the retention
// cap. Caller must hold s.benchmarkJobsMu.
//
// CT-20: previously this loop read each job's `Done` without holding
// the job's per-job mutex, racing the writer in handleBenchmarkRun
// that sets Done=true under job.mu. We now snapshot under job.mu and
// collect lightweight (id, startedAt) entries for the eviction
// comparison, then delete the real job by ID.
func (s *Server) evictFinishedBenchmarkJobs() {
	type snap struct {
		id        string
		startedAt time.Time
	}
	finished := make([]snap, 0, len(s.benchmarkJobs))
	for _, j := range s.benchmarkJobs {
		j.mu.Lock()
		done, startedAt := j.Done, j.StartedAt
		j.mu.Unlock()
		if done {
			finished = append(finished, snap{id: j.ID, startedAt: startedAt})
		}
	}
	for len(finished) > BenchmarkJobMaxRetained {
		oldest := finished[0]
		for _, j := range finished[1:] {
			if j.startedAt.Before(oldest.startedAt) {
				oldest = j
			}
		}
		delete(s.benchmarkJobs, oldest.id)
		// Remove from slice for the next iteration.
		for i, j := range finished {
			if j == oldest {
				finished = append(finished[:i], finished[i+1:]...)
				break
			}
		}
	}
}

// handleBenchmarkResults returns the result of a benchmark job.
func (s *Server) handleBenchmarkResults(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "jobId")
	s.benchmarkJobsMu.Lock()
	job, ok := s.benchmarkJobs[jobID]
	s.benchmarkJobsMu.Unlock()
	if !ok {
		errResponse(w, http.StatusNotFound, "job not found")
		return
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	if !job.Done {
		okJSON(w, map[string]interface{}{"jobId": jobID, "status": "running"})
		return
	}
	res := job.Result
	var p50, p95, p99 float64
	if res.LatencyHist != nil {
		p50 = res.LatencyHist.P50()
		p95 = res.LatencyHist.P95()
		p99 = res.LatencyHist.P99()
	}
	okJSON(w, map[string]interface{}{
		"jobId":        jobID,
		"status":       "done",
		"committed":    res.Committed,
		"aborted":      res.Aborted,
		"conflicts":    res.Conflicts,
		"deadlocks":    res.Deadlocks,
		"totalTxns":    res.TotalTxns,
		"tps":          res.TPS,
		"balanceSumOK": res.BalanceSumOK,
		"durationMs":   res.Duration.Milliseconds(),
		"latencyP50Ms": p50,
		"latencyP95Ms": p95,
		"latencyP99Ms": p99,
	})
}
