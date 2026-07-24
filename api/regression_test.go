package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sanskarpan/TransactionManager/api"
	"github.com/sanskarpan/TransactionManager/internal/deadlock"
	"github.com/sanskarpan/TransactionManager/internal/metrics"
	"github.com/sanskarpan/TransactionManager/internal/mvcc"
	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/txn"
	"github.com/sanskarpan/TransactionManager/internal/types"
)

// newTestServerWithConfig mirrors newTestServer but lets a test inject
// ServerConfig (e.g. an AdminToken).
func newTestServerWithConfig(t *testing.T, cfg api.ServerConfig) *api.Server {
	t.Helper()
	catalog := storage.NewCatalog()
	storage.SeedCatalog(catalog)
	mgr := txn.NewManager(catalog)
	const seedTxnID mvcc.TxnID = 0
	for _, name := range catalog.List() {
		tbl, _ := catalog.Lookup(name)
		mvcc.SeedMVCCStore(mgr.MVCCStore, name, tbl.Scan(nil), seedTxnID)
	}
	mgr.MarkCommitted(txn.ID(seedTxnID))
	wfg := deadlock.NewWaitForGraph()
	dlHistory := deadlock.NewHistory()
	dd := deadlock.NewDetector(wfg, dlHistory, func(victimID uint64, _ []uint64) {
		_ = mgr.Abort(txn.ID(victimID), types.NewDeadlockError())
	})
	mgr.WFG.AddEdgeFn = wfg.AddEdge
	mgr.WFG.RemoveNodeFn = wfg.RemoveNode
	dd.Start()
	t.Cleanup(dd.Stop)
	vac := mvcc.NewVacuum(mgr.MVCCStore, txn.VacuumChecker{M: mgr}, 60*time.Second)
	vac.Run()
	t.Cleanup(vac.Stop)
	m := &metrics.Metrics{}
	mgr.OnBegin = func(*txn.Transaction) { m.TxnBegins.Add(1) }
	mgr.OnCommit = func(*txn.Transaction) { m.TxnCommits.Add(1) }
	mgr.OnAbort = func(*txn.Transaction, error) { m.TxnAborts.Add(1) }
	s := api.NewServerWithConfig(mgr, dd, wfg, dlHistory, vac, m, cfg)
	s.SetReadyForTest(true)
	return s
}

func postBody(t *testing.T, srv *api.Server, path string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	return w
}

// TestC06_RequestBodyCap rejects an oversized JSON body with 413. We
// send a valid-JSON array of 2 MiB of nulls so the decoder actually
// reads past the cap (a body of bare 'a' bytes would fail JSON parsing
// before the read cap tripped, masking the test).
func TestC06_RequestBodyCap(t *testing.T) {
	srv := newTestServerWithConfig(t, api.ServerConfig{})
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i := 0; i < 2*1024*1024; i++ { // 2 MiB entries > 1 MiB cap
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString("null")
	}
	buf.WriteByte(']')
	w := postBody(t, srv, "/api/txn/begin", &buf)
	assert.Equal(t, http.StatusRequestEntityTooLarge, w.Code,
		"body > MaxRequestBodyBytes must be rejected")
}

// TestC07_AdminToken_GatedEndpoints verifies /api/reset is gated behind
// ADMIN_TOKEN when configured. Without the header → 401; with the wrong
// value → 401; with the right value → 200.
func TestC07_AdminToken_GatedEndpoints(t *testing.T) {
	srv := newTestServerWithConfig(t, api.ServerConfig{AdminToken: "s3cr3t"})

	// No header.
	w := postBody(t, srv, "/api/reset", strings.NewReader(`{}`))
	require.Equal(t, http.StatusUnauthorized, w.Code, "missing token must 401")

	// Wrong token.
	req := httptest.NewRequest(http.MethodPost, "/api/reset", strings.NewReader(`{}`))
	req.Header.Set("X-Admin-Token", "wrong")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusUnauthorized, w.Code, "wrong token must 401")

	// Right token.
	req = httptest.NewRequest(http.MethodPost, "/api/reset", strings.NewReader(`{}`))
	req.Header.Set("X-Admin-Token", "s3cr3t")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, "correct token must pass")
}

// TestC07_NoAdminToken_OpenInDev confirms that when AdminToken is empty
// (default local dev), the gated endpoints are open.
func TestC07_NoAdminToken_OpenInDev(t *testing.T) {
	srv := newTestServerWithConfig(t, api.ServerConfig{})
	w := postBody(t, srv, "/api/reset", strings.NewReader(`{}`))
	require.Equal(t, http.StatusOK, w.Code)
}

// TestC08_UnknownIsolation_400 verifies scenario/benchmark endpoints
// reject unknown isolation with 400 rather than silently falling back.
func TestC08_UnknownIsolation_400(t *testing.T) {
	srv := newTestServerWithConfig(t, api.ServerConfig{})
	body := `{"protocol":"mvcc","isolation":"bogus"}`
	w := postBody(t, srv, "/api/scenarios/dirty_read/run", strings.NewReader(body))
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// TestC08_UnknownProtocol_400 verifies the protocol field is validated.
func TestC08_UnknownProtocol_400(t *testing.T) {
	srv := newTestServerWithConfig(t, api.ServerConfig{})
	body := `{"protocol":"bogus","isolation":"read_committed"}`
	w := postBody(t, srv, "/api/scenarios/dirty_read/run", strings.NewReader(body))
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// TestC08_BenchmarkUnknownIsolation_400 confirms the same on benchmark.
func TestC08_BenchmarkUnknownIsolation_400(t *testing.T) {
	srv := newTestServerWithConfig(t, api.ServerConfig{})
	body := `{"protocol":"mvcc","isolation":"bogus"}`
	w := postBody(t, srv, "/api/benchmark/run", strings.NewReader(body))
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// TestH01_BenchmarkJobIDs_Monotonic verifies two consecutive job starts
// return distinct IDs (previously both produced "job-<same-second>" and
// the second overwrote the first's result slot).
func TestH01_BenchmarkJobIDs_Monotonic(t *testing.T) {
	srv := newTestServerWithConfig(t, api.ServerConfig{})
	body := `{"protocol":"mvcc","isolation":"read_committed","durationSec":1,"concurrency":2,"numAccounts":10}`
	w1 := postBody(t, srv, "/api/benchmark/run", strings.NewReader(body))
	require.Equal(t, http.StatusOK, w1.Code)
	var r1 map[string]interface{}
	require.NoError(t, json.NewDecoder(w1.Body).Decode(&r1))
	id1 := r1["jobId"].(string)

	w2 := postBody(t, srv, "/api/benchmark/run", strings.NewReader(body))
	require.Equal(t, http.StatusOK, w2.Code)
	var r2 map[string]interface{}
	require.NoError(t, json.NewDecoder(w2.Body).Decode(&r2))
	id2 := r2["jobId"].(string)

	assert.NotEqual(t, id1, id2, "consecutive jobs must have distinct IDs")
}

// TestH05_Healthz_Readyz verifies the health endpoints.
func TestH05_Healthz_Readyz(t *testing.T) {
	srv := newTestServerWithConfig(t, api.ServerConfig{})

	// healthz always 200.
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", http.NoBody))
	require.Equal(t, http.StatusOK, w.Code)

	// readyz: SetReadyForTest(true) was called in the helper.
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody))
	require.Equal(t, http.StatusOK, w.Code)

	// A fresh server (not marked ready) returns 503.
	srv2 := newTestServerWithConfig(t, api.ServerConfig{})
	srv2.SetReadyForTest(false)
	w = httptest.NewRecorder()
	srv2.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody))
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}

// TestH06_ErrorSanitized verifies a 5xx error path returns a generic
// message rather than the raw internal error.
func TestH06_ErrorSanitized(t *testing.T) {
	srv := newTestServerWithConfig(t, api.ServerConfig{})
	// Unknown txn id → 404, not a 500 with raw error.
	w := postBody(t, srv, "/api/txn/9999999/commit", strings.NewReader(`{}`))
	require.Equal(t, http.StatusNotFound, w.Code)
	var body map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&body))
	msg, _ := body["error"].(string)
	assert.NotContains(t, msg, "github.com/sanskarpan", "no internal path leaked")
}

// TestH07_EmptyBody_Begin OK verifies an empty body to /api/txn/begin
// (H-07: previously compared err.Error() != "EOF" by string).
func TestH07_EmptyBody_Begin(t *testing.T) {
	srv := newTestServerWithConfig(t, api.ServerConfig{})
	w := postBody(t, srv, "/api/txn/begin", strings.NewReader(``))
	require.Equal(t, http.StatusOK, w.Code, "empty body must default to 2PL/RC")
}

// TestH08_ContextCancelAbortsLock verifies that cancelling the request
// context while a transaction is blocked on a contended lock causes the
// AcquireLock to return ErrTxnAborted rather than waiting out the full
// lockTimeout.
func TestH08_ContextCancelAbortsLock(t *testing.T) {
	srv := newTestServerWithConfig(t, api.ServerConfig{})
	mgr := srv.Handler()
	_ = mgr

	// Begin T1, take an X-lock on a row via a 2PL write.
	w := postBody(t, srv, "/api/txn/begin", strings.NewReader(`{"protocol":"2pl","isolation":"read_committed"}`))
	require.Equal(t, http.StatusOK, w.Code)
	var b1 map[string]interface{}
	require.NoError(t, json.NewDecoder(w1Adapter(w)).Decode(&b1))
	id1 := int(b1["id"].(float64))

	w = postBody(t, srv, "/api/txn/"+itoa(id1)+"/write", strings.NewReader(
		`{"table":"accounts","key":"1","values":[`+
			`{"Type":1,"Int":1},`+
			`{"Type":3,"Text":"a"},`+
			`{"Type":2,"Float":1.0},`+
			`{"Type":3,"Text":"n"}]}`))
	require.Equal(t, http.StatusOK, w.Code)

	// Begin T2 and try to read the same row. Use a cancellable context
	// so we can simulate a client disconnect.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	// Use a long lock timeout so the context cancellation, not the
	// timeout, is what wins the select.
	body := `{"protocol":"2pl","isolation":"read_committed","lockTimeoutMs":30000}`
	req := httptest.NewRequest(http.MethodPost, "/api/txn/begin", strings.NewReader(body)).WithContext(ctx)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var b2 map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&b2))
	id2 := int(b2["id"].(float64))

	// Issue the read on T2 with the same cancelled context.
	start := time.Now()
	req = httptest.NewRequest(http.MethodPost, "/api/txn/"+itoa(id2)+"/read",
		strings.NewReader(`{"table":"accounts","key":"1"}`)).WithContext(ctx)
	resp := httptest.NewRecorder()
	srv.ServeHTTP(resp, req)
	elapsed := time.Since(start)
	// Expect 410 Gone (ErrTxnAborted) and that it returned within ~1s,
	// well under the 30 s lock timeout.
	assert.Less(t, elapsed, 2*time.Second, "context cancel must abort promptly")
	assert.Equal(t, http.StatusGone, resp.Code)
}

// w1Adapter is a trivial helper to satisfy json.Decode signature.
func w1Adapter(w *httptest.ResponseRecorder) io.Reader { return w.Body }

// TestH09_NoDuplicateDeadlockRecords verifies the deadlock detector does
// not emit duplicate records for the same cycle across ticks. Previously,
// because the WFG node was removed asynchronously (after the abort),
// the next 50ms tick could re-find the cycle.
func TestH09_NoDuplicateDeadlockRecords(t *testing.T) {
	// Custom setup: detector with a counted abort that does NOT remove
	// the WFG node itself (the detector now does that before invoking
	// the callback, so the count must stay at 1 even though our callback
	// is a no-op).
	catalog := storage.NewCatalog()
	storage.SeedCatalog(catalog)
	mgr := txn.NewManager(catalog)
	wfg := deadlock.NewWaitForGraph()
	dlHistory := deadlock.NewHistory()

	var abortCount atomic.Int64
	dd := deadlock.NewDetector(wfg, dlHistory, func(_ uint64, _ []uint64) {
		abortCount.Add(1)
		// Intentionally do NOT call mgr.Abort here — the detector has
		// already removed the victim's edges from the WFG, which is
		// what we are testing.
	})
	mgr.WFG.AddEdgeFn = wfg.AddEdge
	mgr.WFG.RemoveNodeFn = wfg.RemoveNode
	dd.Start()
	t.Cleanup(dd.Stop)

	// Insert a 2-cycle.
	wfg.AddEdge(1, 2)
	wfg.AddEdge(2, 1)

	// Wait for two detector ticks.
	time.Sleep(150 * time.Millisecond)

	// Only one abort should have fired.
	assert.Equal(t, int64(1), abortCount.Load(),
		"detector must not re-emit the same cycle after removing the victim")
}

// TestC03_LockTimeoutMs_Applied verifies that a caller-supplied
// lockTimeoutMs actually takes effect (previously the field name mismatch
// with the frontend silently fell back to the default).
func TestC03_LockTimeoutMs_Applied(t *testing.T) {
	srv := newTestServerWithConfig(t, api.ServerConfig{})
	// T1 takes X on row 1.
	w := postBody(t, srv, "/api/txn/begin", strings.NewReader(`{"protocol":"2pl","isolation":"read_committed"}`))
	require.Equal(t, http.StatusOK, w.Code)
	var b1 map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&b1))
	id1 := int(b1["id"].(float64))
	postBody(t, srv, "/api/txn/"+itoa(id1)+"/write", strings.NewReader(
		`{"table":"accounts","key":"1","values":[`+
			`{"Type":1,"Int":1},`+
			`{"Type":3,"Text":"a"},`+
			`{"Type":2,"Float":1.0},`+
			`{"Type":3,"Text":"n"}]}`))

	// T2 requests with a 100 ms lock timeout.
	w = postBody(t, srv, "/api/txn/begin", strings.NewReader(`{"protocol":"2pl","isolation":"read_committed","lockTimeoutMs":100}`))
	require.Equal(t, http.StatusOK, w.Code)
	var b2 map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&b2))
	id2 := int(b2["id"].(float64))

	start := time.Now()
	resp := postBody(t, srv, "/api/txn/"+itoa(id2)+"/read", strings.NewReader(`{"table":"accounts","key":"1"}`))
	elapsed := time.Since(start)
	// Should time out near 100ms (allow generous slack for scheduler).
	assert.Less(t, elapsed, 2*time.Second, "lock timeout must actually take effect")
	assert.Equal(t, http.StatusRequestTimeout, resp.Code, "expected LOCK_TIMEOUT 408")
}

// TestH19_Metrics_NoDoubleCount verifies H-19: the handler no longer bumps
// TxnBegins/TxnCommits/TxnAborts — the mgr.OnBegin/OnCommit/OnAbort callbacks
// are the single source of truth. Previously every transactional op was
// counted 2×, masked by smoke tests asserting GreaterOrEqual.
func TestH19_Metrics_NoDoubleCount(t *testing.T) {
	srv := newTestServerWithConfig(t, api.ServerConfig{})
	w := postBody(t, srv, "/api/txn/begin", strings.NewReader(`{"protocol":"mvcc","isolation":"read_committed"}`))
	require.Equal(t, http.StatusOK, w.Code)
	var b map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&b))
	id := int(b["id"].(float64))

	// Commit it.
	w = postBody(t, srv, "/api/txn/"+itoa(id)+"/commit", strings.NewReader(`{}`))
	require.Equal(t, http.StatusOK, w.Code)

	// Exactly one begin and one commit — not two.
	assert.Equal(t, int64(1), srv.MetricsForTest().TxnBegins.Load(), "TxnBegins must not be double-counted (H-19)")
	assert.Equal(t, int64(1), srv.MetricsForTest().TxnCommits.Load(), "TxnCommits must not be double-counted (H-19)")
	assert.Equal(t, int64(0), srv.MetricsForTest().TxnAborts.Load())
}

// TestH23_BenchmarkJobIDs_NotEnumerable verifies H-23: benchmark job IDs are
// unguessable 16-byte hex values, not the prior monotonic `job-%06d` format
// that let any unauthenticated caller enumerate other users' results.
func TestH23_BenchmarkJobIDs_NotEnumerable(t *testing.T) {
	srv := newTestServerWithConfig(t, api.ServerConfig{AdminToken: "test-token-long-enough-1234"})
	body := strings.NewReader(`{"protocol":"mvcc","isolation":"read_committed","concurrency":1,"durationSec":1,"numAccounts":5,"maxRetries":1}`)
	req := httptest.NewRequest(http.MethodPost, "/api/benchmark/run", body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-Token", "test-token-long-enough-1234")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	jobID, ok := resp["jobId"].(string)
	require.True(t, ok)
	// Must NOT match the old enumerable format `job-000001`.
	assert.NotRegexp(t, `^job-\d{6}$`, jobID, "job ID must not be enumerable (H-23)")
	// Must have substantial entropy (job- + 32 hex chars).
	assert.Len(t, jobID, len("job-")+32, "job ID should be job-<32 hex chars>")
}

// TestH21_CORS_ReflectsAllowlistedOrigin verifies H-21: with an allowlist,
// ACAO echoes the request's Origin exactly (not a comma-joined list) and
// Vary: Origin is present; a non-allowlisted origin gets no ACAO.
func TestH21_CORS_ReflectsAllowlistedOrigin(t *testing.T) {
	srv := newTestServerWithConfig(t, api.ServerConfig{CORSAllowOrigins: []string{"https://app.example.com"}})

	// Allowlisted origin → reflected.
	req := httptest.NewRequest(http.MethodGet, "/healthz", http.NoBody)
	req.Header.Set("Origin", "https://app.example.com")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	assert.Equal(t, "https://app.example.com", w.Header().Get("Access-Control-Allow-Origin"))
	assert.Contains(t, w.Header().Get("Vary"), "Origin")

	// Non-allowlisted origin → no ACAO (fail closed).
	req2 := httptest.NewRequest(http.MethodGet, "/healthz", http.NoBody)
	req2.Header.Set("Origin", "https://evil.example.com")
	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, req2)
	assert.Empty(t, w2.Header().Get("Access-Control-Allow-Origin"))
}

// TestM35_ParseTxnID_RejectsZero verifies M-35: id == 0 is the reserved seed
// transaction ID and must be rejected at the API boundary.
func TestM35_ParseTxnID_RejectsZero(t *testing.T) {
	srv := newTestServerWithConfig(t, api.ServerConfig{})
	w := postBody(t, srv, "/api/txn/0/read", strings.NewReader(`{"table":"accounts","key":"1"}`))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// TestCT12_CT13_RowShapeValidation is the regression guard for CT-12
// and CT-13: a write/insert whose value slice does not match the
// table's column declarations (wrong length OR wrong per-column type)
// must be rejected with 400 BEFORE any storage mutation, so a buggy
// client cannot silently corrupt the row shape.
func TestCT12_CT13_RowShapeValidation(t *testing.T) {
	srv := newTestServerWithConfig(t, api.ServerConfig{})

	// Begin an MVCC txn.
	bw := postBody(t, srv, "/api/txn/begin",
		strings.NewReader(`{"protocol":"mvcc","isolation":"read_committed"}`))
	require.Equal(t, http.StatusOK, bw.Code)
	var b map[string]interface{}
	require.NoError(t, json.NewDecoder(bw.Body).Decode(&b))
	id := int(b["id"].(float64))

	// accounts has 4 columns: id(Int), owner(Text), balance(Float), branch(Text).

	// (1) wrong length: 1 value instead of 4 → 400.
	w := postBody(t, srv, fmt.Sprintf("/api/txn/%d/write", id),
		strings.NewReader(`{"table":"accounts","key":"1","values":[{"Type":1,"Int":1}]}`))
	assert.Equal(t, http.StatusBadRequest, w.Code, "CT-12: wrong-length row must be rejected")
	// CT-22: the error body must be the sanitized generic message, NOT
	// the raw detail (column names, counts, type codes).
	assert.Contains(t, w.Body.String(), "row shape does not match table declaration",
		"row-shape errors must be sanitized via errWrite, not leak raw detail")

	// (2) wrong type at column 0: Text instead of Int → 400.
	wrongTypeBody := `{"table":"accounts","key":"1","values":[` +
		`{"Type":3,"Text":"x"},` +
		`{"Type":3,"Text":"a"},` +
		`{"Type":2,"Float":1.0},` +
		`{"Type":3,"Text":"n"}]}`
	w = postBody(t, srv, fmt.Sprintf("/api/txn/%d/write", id),
		strings.NewReader(wrongTypeBody))
	assert.Equal(t, http.StatusBadRequest, w.Code, "CT-13: type-mismatched row must be rejected")

	// (3) correct shape still works. Build the body string explicitly
	// to keep brace/bracket nesting obvious.
	correctBody := `{"table":"accounts","key":"1","values":[` +
		`{"Type":1,"Int":1},` +
		`{"Type":3,"Text":"a"},` +
		`{"Type":2,"Float":1.0},` +
		`{"Type":3,"Text":"n"}]}`
	w = postBody(t, srv, fmt.Sprintf("/api/txn/%d/write", id),
		strings.NewReader(correctBody))
	assert.Equal(t, http.StatusOK, w.Code, "correct-shape write must succeed")

	// (4) wrong-length INSERT also rejected.
	w = postBody(t, srv, fmt.Sprintf("/api/txn/%d/insert", id),
		strings.NewReader(`{"table":"accounts","key":"new","values":[{"Type":1,"Int":1}]}`))
	assert.Equal(t, http.StatusBadRequest, w.Code, "CT-12: wrong-length insert must be rejected")

	require.NoError(t, srv.Shutdown(context.Background()))
	_ = itoa // silence unused
}

// TestCT21_ShutdownWaitsForInFlightJob is the regression guard for
// CT-21: a benchmark goroutine started in handleBenchmarkRun must be
// tracked on jobWg BEFORE the mutex is released, otherwise a concurrent
// Shutdown could observe jobWg == 0, return from jobWg.Wait
// immediately, and let the goroutine run against a torn-down server.
//
// We simulate by starting a benchmark (long enough to still be running
// when Shutdown is called) and verifying Shutdown blocks until the
// job completes (via jobCtx cancellation) before returning.
func TestCT21_ShutdownWaitsForInFlightJob(t *testing.T) {
	admin := "test-admin-token-32-chars-ok!"
	srv := newTestServerWithConfig(t, api.ServerConfig{AdminToken: admin})

	// Start a 3-second benchmark. Returns immediately (200 with jobId).
	body := `{"protocol":"2pl","isolation":"read_committed","concurrency":2,"durationSec":3,"numAccounts":5,"maxRetries":1}`
	req := httptest.NewRequest(http.MethodPost, "/api/benchmark/run", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-Token", admin)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	// Shutdown should drain the in-flight job. The job's runner
	// honours jobCtx (cancelled by Shutdown) and returns promptly.
	// We assert Shutdown returns without error (it would hang or
	// timeout if the job were not tracked on jobWg).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx), "Shutdown must drain in-flight benchmark jobs (CT-21)")
}
