package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/deadlock"
	"github.com/sanskarpan/TransactionManager/internal/metrics"
	"github.com/sanskarpan/TransactionManager/internal/mvcc"
	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/txn"
	"github.com/sanskarpan/TransactionManager/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCoverage_NewServer(t *testing.T) {
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
	vac := mvcc.NewVacuum(mgr.MVCCStore, txn.VacuumChecker{M: mgr}, 60_000_000_000)
	vac.Run()
	t.Cleanup(vac.Stop)
	m := &metrics.Metrics{}
	s := NewServer(mgr, dd, wfg, dlHistory, vac, m)
	require.NotNil(t, s)
	s.SetReadyForTest(true)

	_ = s.Logger()
}

func TestCoverage_MarkReady(t *testing.T) {
	srv := newAPIServer(t)
	srv.SetReadyForTest(false)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)

	srv.MarkReady()

	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestCoverage_SubscribeUnsubscribe(t *testing.T) {
	bus := NewSSEBus()
	require.NotNil(t, bus)

	ch, ok := bus.Subscribe()
	require.True(t, ok)
	require.NotNil(t, ch)

	bus.Unsubscribe(ch)

	_, open := <-ch
	assert.False(t, open)
}

func TestCoverage_SubscribeMaxClients(t *testing.T) {
	bus := NewSSEBus()
	channels := make([]chan SSEEvent, 0, MaxSSEClients)
	for i := 0; i < MaxSSEClients; i++ {
		ch, ok := bus.Subscribe()
		require.True(t, ok)
		channels = append(channels, ch)
	}

	_, ok := bus.Subscribe()
	assert.False(t, ok)

	for _, ch := range channels {
		bus.Unsubscribe(ch)
	}
}

func TestCoverage_handleLockByResource(t *testing.T) {
	srv := newAPIServer(t)

	// No lock held yet — returns empty queue
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/locks/accounts/nonexistent", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var empty map[string]interface{}
	decodeJSONBody(t, w, &empty)
	assert.NotEmpty(t, empty["resource"])
	assert.Empty(t, empty["granted"])
	assert.Empty(t, empty["waiting"])

	// Begin a 2PL txn and acquire a read lock
	w = post(srv, "/api/txn/begin", `{"protocol":"2pl","isolation":"read_committed"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var b map[string]interface{}
	decodeJSONBody(t, w, &b)
	id := int(b["id"].(float64))

	w = post(srv, "/api/txn/"+itoa(id)+"/read", `{"table":"accounts","key":"1"}`)
	require.Equal(t, http.StatusOK, w.Code)

	// Lock queue should now have an entry
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/locks/accounts/1", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var lockInfo map[string]interface{}
	decodeJSONBody(t, w, &lockInfo)
	assert.Equal(t, "row:accounts:1", lockInfo["resource"])
	granted, _ := lockInfo["granted"].([]interface{})
	assert.GreaterOrEqual(t, len(granted), 1)
}

func TestCoverage_handleDeadlocks(t *testing.T) {
	srv := newAPIServer(t)

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/deadlocks", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var result []interface{}
	decodeJSONBody(t, w, &result)
	assert.Empty(t, result)
}

func TestCoverage_handleWFG(t *testing.T) {
	srv := newAPIServer(t)

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/wfg", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var result map[string]interface{}
	decodeJSONBody(t, w, &result)
	assert.Empty(t, result["nodes"])
	assert.Empty(t, result["edges"])
}

func TestCoverage_handleMVCCStats(t *testing.T) {
	srv := newAPIServer(t)

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/mvcc/stats", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var result map[string]interface{}
	decodeJSONBody(t, w, &result)
	assert.Contains(t, result, "totalChains")
	assert.Contains(t, result, "totalVersions")
}

func TestCoverage_handleVacuum(t *testing.T) {
	srv := newAPIServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/mvcc/vacuum", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var result map[string]interface{}
	decodeJSONBody(t, w, &result)
	assert.Equal(t, true, result["ok"])
}

func TestCoverage_handleDelete(t *testing.T) {
	srv := newAPIServer(t)

	// Unknown txn → 404
	w := post(srv, "/api/txn/999999/delete", `{"table":"accounts","key":"1"}`)
	require.Equal(t, http.StatusNotFound, w.Code)

	// Begin txn
	w = post(srv, "/api/txn/begin", `{"protocol":"mvcc","isolation":"read_committed"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var b map[string]interface{}
	decodeJSONBody(t, w, &b)
	id := int(b["id"].(float64))

	// Malformed body → 400
	w = post(srv, "/api/txn/"+itoa(id)+"/delete", `not-json`)
	require.Equal(t, http.StatusBadRequest, w.Code)

	// Successfully delete
	w = post(srv, "/api/txn/"+itoa(id)+"/delete", `{"table":"accounts","key":"1"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var result map[string]interface{}
	decodeJSONBody(t, w, &result)
	assert.Equal(t, true, result["ok"])

	// Unknown table → errWrite (non-nil error)
	w = post(srv, "/api/txn/"+itoa(id)+"/delete", `{"table":"nonexistent","key":"1"}`)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestCoverage_handleScan(t *testing.T) {
	srv := newAPIServer(t)

	// Unknown txn → 404
	w := post(srv, "/api/txn/999999/scan", `{"table":"accounts"}`)
	require.Equal(t, http.StatusNotFound, w.Code)

	// Begin txn
	w = post(srv, "/api/txn/begin", `{"protocol":"mvcc","isolation":"read_committed"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var b map[string]interface{}
	decodeJSONBody(t, w, &b)
	id := int(b["id"].(float64))

	// Malformed body → 400
	w = post(srv, "/api/txn/"+itoa(id)+"/scan", `not-json`)
	require.Equal(t, http.StatusBadRequest, w.Code)

	// Successfully scan
	w = post(srv, "/api/txn/"+itoa(id)+"/scan", `{"table":"accounts"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var result map[string]interface{}
	decodeJSONBody(t, w, &result)
	assert.Contains(t, result, "rows")
	assert.Contains(t, result, "count")
	assert.Greater(t, int(result["count"].(float64)), 0)
}

func TestCoverage_handleBenchmarkResults(t *testing.T) {
	srv := newAPIServer(t)

	// Unknown job → 404
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/benchmark/results/nonexistent", nil))
	require.Equal(t, http.StatusNotFound, w.Code)

	// Running job → status "running"
	srv.benchmarkJobsMu.Lock()
	srv.benchmarkJobs["job-running"] = &benchmarkJob{ID: "job-running", Done: false}
	srv.benchmarkJobsMu.Unlock()

	w = httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/benchmark/results/job-running", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var res map[string]interface{}
	decodeJSONBody(t, w, &res)
	assert.Equal(t, "running", res["status"])

	// Completed job → status "done"
	srv.benchmarkJobsMu.Lock()
	srv.benchmarkJobs["job-done"] = &benchmarkJob{ID: "job-done", Done: true}
	srv.benchmarkJobsMu.Unlock()

	w = httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/benchmark/results/job-done", nil))
	require.Equal(t, http.StatusOK, w.Code)
	decodeJSONBody(t, w, &res)
	assert.Equal(t, "done", res["status"])
}

func TestCoverage_handleReleaseSavepoint(t *testing.T) {
	srv := newAPIServer(t)

	// Begin txn
	w := post(srv, "/api/txn/begin", `{"protocol":"2pl","isolation":"read_committed"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var b map[string]interface{}
	decodeJSONBody(t, w, &b)
	id := int(b["id"].(float64))

	// Create a savepoint
	w = post(srv, "/api/txn/"+itoa(id)+"/savepoint", `{"name":"sp1"}`)
	require.Equal(t, http.StatusOK, w.Code)

	// Release it (DELETE /api/txn/{id}/savepoint/{name}")
	req := httptest.NewRequest(http.MethodDelete, "/api/txn/"+itoa(id)+"/savepoint/sp1", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var result map[string]interface{}
	decodeJSONBody(t, w, &result)
	assert.Equal(t, "sp1", result["name"])
	assert.Equal(t, true, result["released"])

	// Error: unknown txn → 404
	req = httptest.NewRequest(http.MethodDelete, "/api/txn/999999/savepoint/sp1", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)

	// Error: non-existent savepoint → errWrite error
	req = httptest.NewRequest(http.MethodDelete, "/api/txn/"+itoa(id)+"/savepoint/nonexistent", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestCoverage_handleSSEEvents(t *testing.T) {
	srv := newAPIServer(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/sse/events", nil).WithContext(ctx)
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		srv.ServeHTTP(w, req)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)

	srv.sseBus.Publish(SSEEvent{Event: "txn_begin", Data: map[string]interface{}{"id": uint64(1)}})

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("SSE events handler did not exit on context cancellation")
	}

	assert.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))
	assert.Equal(t, "no-cache", w.Header().Get("Cache-Control"))
	assert.Contains(t, w.Body.String(), "event: txn_begin")
}

func TestCoverage_handleSSEWFG(t *testing.T) {
	srv := newAPIServer(t)

	srv.wfg.AddEdge(1, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/sse/wfg", nil).WithContext(ctx)
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		srv.ServeHTTP(w, req)
		close(done)
	}()

	time.Sleep(600 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("SSE WFG handler did not exit on context cancellation")
	}

	assert.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))
	assert.Equal(t, "no-cache", w.Header().Get("Cache-Control"))
	assert.Contains(t, w.Body.String(), "data: ")
}

func TestCoverage_handleInsert(t *testing.T) {
	srv := newAPIServer(t)

	// Unknown txn → 404
	w := post(srv, "/api/txn/999999/insert", `{"table":"accounts","key":"k","values":[]}`)
	require.Equal(t, http.StatusNotFound, w.Code)

	// Begin txn
	w = post(srv, "/api/txn/begin", `{"protocol":"mvcc","isolation":"read_committed"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var b map[string]interface{}
	decodeJSONBody(t, w, &b)
	id := int(b["id"].(float64))

	// Malformed body → 400
	w = post(srv, "/api/txn/"+itoa(id)+"/insert", `not-json`)
	require.Equal(t, http.StatusBadRequest, w.Code)

	// Unknown table → 404
	w = post(srv, "/api/txn/"+itoa(id)+"/insert", `{"table":"nonexistent","key":"k","values":[]}`)
	require.Equal(t, http.StatusNotFound, w.Code)

	// Insert a new row
	vals := `[{"Type":1,"Int":999},{"Type":3,"Text":"test"},{"Type":2,"Float":0.0},{"Type":3,"Text":"test"}]`
	w = post(srv, "/api/txn/"+itoa(id)+"/insert", `{"table":"accounts","key":"insertedkey","values":`+vals+`}`)
	require.Equal(t, http.StatusOK, w.Code)
	var result map[string]interface{}
	decodeJSONBody(t, w, &result)
	assert.Equal(t, true, result["ok"])

	vals2 := `[{"Type":1,"Int":1000},{"Type":3,"Text":"test2"},{"Type":2,"Float":1.0},{"Type":3,"Text":"south"}]`

	// Successful insert with committed txn (no conflict since txn1 is not in txn2's snapshot)
	w = post(srv, "/api/txn/"+itoa(id)+"/insert", `{"table":"accounts","key":"insertedkey2","values":`+vals2+`}`)
	require.Equal(t, http.StatusOK, w.Code)

	// Begin second txn that overlaps with txn1 (don't commit txn1)
	// txn2's snapshot will include txn1's active ID, so insert of same key → write-write conflict (409)
	w = post(srv, "/api/txn/begin", `{"protocol":"mvcc","isolation":"read_committed"}`)
	require.Equal(t, http.StatusOK, w.Code)
	decodeJSONBody(t, w, &b)
	id2 := int(b["id"].(float64))
	w = post(srv, "/api/txn/"+itoa(id2)+"/insert", `{"table":"accounts","key":"insertedkey","values":`+vals+`}`)
	require.Equal(t, http.StatusConflict, w.Code)

	// Cleanup
	w = post(srv, "/api/txn/"+itoa(id)+"/abort", `{}`)
	require.Equal(t, http.StatusOK, w.Code)
	w = post(srv, "/api/txn/"+itoa(id2)+"/abort", `{}`)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestCoverage_errToPublicResponse(t *testing.T) {
	// ErrLockTimeout → 408
	status, msg := errToPublicResponse(types.NewLockTimeoutError())
	assert.Equal(t, http.StatusRequestTimeout, status)
	assert.Contains(t, msg, "timed out")

	// ErrTxnNotFound → 404
	status, msg = errToPublicResponse(types.NewTxnNotFoundError(42))
	assert.Equal(t, http.StatusNotFound, status)
	assert.Contains(t, msg, "not found")

	// io.EOF → 400
	status, msg = errToPublicResponse(io.EOF)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, msg, "required")

	// http.ErrHandlerTimeout → 503
	status, msg = errToPublicResponse(http.ErrHandlerTimeout)
	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Contains(t, msg, "timed out")

	// raw error → 500
	status, msg = errToPublicResponse(errors.New("unexpected"))
	assert.Equal(t, http.StatusInternalServerError, status)
	assert.Contains(t, msg, "internal server error")
}

func TestCoverage_handleDeadlocksNilHistory(t *testing.T) {
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
	dd := deadlock.NewDetector(wfg, nil, func(victimID uint64, _ []uint64) {
		_ = mgr.Abort(txn.ID(victimID), types.NewDeadlockError())
	})
	mgr.WFG.AddEdgeFn = wfg.AddEdge
	mgr.WFG.RemoveNodeFn = wfg.RemoveNode
	dd.Start()
	t.Cleanup(dd.Stop)
	vac := mvcc.NewVacuum(mgr.MVCCStore, txn.VacuumChecker{M: mgr}, 60_000_000_000)
	vac.Run()
	t.Cleanup(vac.Stop)
	m := &metrics.Metrics{}
	s := NewServerWithConfig(mgr, dd, wfg, nil, vac, m, ServerConfig{})
	s.SetReadyForTest(true)

	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/deadlocks", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var result []interface{}
	decodeJSONBody(t, w, &result)
	assert.Empty(t, result)
}

func TestCoverage_handleStatus(t *testing.T) {
	srv := newAPIServer(t)

	// Begin txn
	w := post(srv, "/api/txn/begin", `{"protocol":"mvcc","isolation":"read_committed"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var b map[string]interface{}
	decodeJSONBody(t, w, &b)
	id := int(b["id"].(float64))

	// Status of active txn
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/txn/"+itoa(id)+"/status", nil))
	require.Equal(t, http.StatusOK, w.Code)
	var statusResp map[string]interface{}
	decodeJSONBody(t, w, &statusResp)
	assert.Equal(t, float64(id), statusResp["id"])
	assert.Equal(t, "active", statusResp["status"])

	// Commit
	w = post(srv, "/api/txn/"+itoa(id)+"/commit", `{}`)
	require.Equal(t, http.StatusOK, w.Code)

	// Status of committed txn
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/txn/"+itoa(id)+"/status", nil))
	require.Equal(t, http.StatusOK, w.Code)
	decodeJSONBody(t, w, &statusResp)
	assert.Equal(t, "committed", statusResp["status"])
}

func TestCoverage_handleSavepointErrors(t *testing.T) {
	srv := newAPIServer(t)

	w := post(srv, "/api/txn/begin", `{"protocol":"mvcc","isolation":"read_committed"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var b map[string]interface{}
	decodeJSONBody(t, w, &b)
	id := int(b["id"].(float64))

	// Missing name → 400
	w = post(srv, "/api/txn/"+itoa(id)+"/savepoint", `{}`)
	require.Equal(t, http.StatusBadRequest, w.Code)

	// Nonexistent txn → 404
	w = post(srv, "/api/txn/99999/savepoint", `{"name":"sp1"}`)
	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestCoverage_handleRollbackToErrors(t *testing.T) {
	srv := newAPIServer(t)

	w := post(srv, "/api/txn/begin", `{"protocol":"mvcc","isolation":"read_committed"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var b map[string]interface{}
	decodeJSONBody(t, w, &b)
	id := int(b["id"].(float64))

	// Missing name → 400
	w = post(srv, "/api/txn/"+itoa(id)+"/rollback-to", `{}`)
	require.Equal(t, http.StatusBadRequest, w.Code)

	// Nonexistent savepoint → 404
	w = post(srv, "/api/txn/"+itoa(id)+"/rollback-to", `{"name":"nonexistent"}`)
	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestCoverage_evictFinishedBenchmarkJobs(t *testing.T) {
	srv := newAPIServer(t)

	// Add more than BenchmarkJobMaxRetained finished jobs
	baseTime := time.Now()
	srv.benchmarkJobsMu.Lock()
	for i := 0; i < BenchmarkJobMaxRetained+5; i++ {
		id := fmt.Sprintf("job-%d", i)
		srv.benchmarkJobs[id] = &benchmarkJob{
			ID:        id,
			StartedAt: baseTime.Add(time.Duration(i) * time.Second),
			Done:      true,
		}
	}
	srv.evictFinishedBenchmarkJobs()
	srv.benchmarkJobsMu.Unlock()

	assert.LessOrEqual(t, len(srv.benchmarkJobs), BenchmarkJobMaxRetained)
}
