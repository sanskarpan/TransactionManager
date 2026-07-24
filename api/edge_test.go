package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sanskarpan/TransactionManager/internal/deadlock"
	"github.com/sanskarpan/TransactionManager/internal/metrics"
	"github.com/sanskarpan/TransactionManager/internal/mvcc"
	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/txn"
	"github.com/sanskarpan/TransactionManager/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newAPIServer mirrors the api_test.newTestServer helper but lives in
// package api so fuzz targets and edge-case tests can use unexported
// helpers without crossing the test-package boundary.
func newAPIServer(t *testing.T) *Server {
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
	vac := mvcc.NewVacuum(mgr.MVCCStore, txn.VacuumChecker{M: mgr}, 60_000_000_000) // 60s
	vac.Run()
	t.Cleanup(vac.Stop)
	m := &metrics.Metrics{}
	s := NewServerWithConfig(mgr, dd, wfg, dlHistory, vac, m, ServerConfig{})
	s.SetReadyForTest(true)
	return s
}

func post(srv *Server, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	return w
}

func decodeJSONBody(t *testing.T, w *httptest.ResponseRecorder, v interface{}) {
	t.Helper()
	require.NoError(t, json.NewDecoder(w.Body).Decode(v))
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf []byte
	if n < 0 {
		buf = append(buf, '-')
		n = -n
	}
	var digits [20]byte
	i := len(digits)
	for n > 0 {
		i--
		digits[i] = byte('0' + n%10)
		n /= 10
	}
	buf = append(buf, digits[i:]...)
	return string(buf)
}

// TestEdge_MalformedJSON verifies handlers reject malformed JSON with
// 400 rather than panicking (L-02).
func TestEdge_MalformedJSON(t *testing.T) {
	srv := newAPIServer(t)
	w := post(srv, "/api/txn/begin", `{not json`)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// TestEdge_NonNumericTxnID verifies a non-numeric {id} URL param yields
// 400, not a 500 (L-02).
func TestEdge_NonNumericTxnID(t *testing.T) {
	srv := newAPIServer(t)
	w := post(srv, "/api/txn/not-a-number/commit", `{}`)
	require.Equal(t, http.StatusBadRequest, w.Code)
}

// TestEdge_UnknownTable verifies a write to a non-existent table returns
// a sanitized 404, not a 500 with an internal path.
func TestEdge_UnknownTable(t *testing.T) {
	srv := newAPIServer(t)
	w := post(srv, "/api/txn/begin", `{"protocol":"2pl","isolation":"read_committed"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var b map[string]interface{}
	decodeJSONBody(t, w, &b)
	id := int(b["id"].(float64))
	w = post(srv, "/api/txn/"+itoa(id)+"/write",
		`{"table":"does_not_exist","key":"x","values":[]}`)
	require.Equal(t, http.StatusNotFound, w.Code, "unknown table → 404")
}

// TestEdge_ReadUnknownKey returns 200 with found=false.
func TestEdge_ReadUnknownKey(t *testing.T) {
	srv := newAPIServer(t)
	w := post(srv, "/api/txn/begin", `{"protocol":"mvcc","isolation":"read_committed"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var b map[string]interface{}
	decodeJSONBody(t, w, &b)
	id := int(b["id"].(float64))
	w = post(srv, "/api/txn/"+itoa(id)+"/read",
		`{"table":"accounts","key":"nonexistent-key"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var r map[string]interface{}
	decodeJSONBody(t, w, &r)
	assert.Equal(t, false, r["found"])
}

// TestEdge_GETOnPOSTRoute verifies method mismatch is handled (chi
// returns 405).
func TestEdge_GETOnPOSTRoute(t *testing.T) {
	srv := newAPIServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/txn/begin", http.NoBody)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

// FuzzParseTxnID covers the URL-param parser for arbitrary inputs. The
// handler must never panic: any of 200/404/400 is acceptable, only 500
// is a bug.
func FuzzParseTxnID(f *testing.F) {
	f.Add("0")
	f.Add("1")
	f.Add("18446744073709551615")
	f.Add("-1")
	f.Add("abc")
	f.Add("")
	f.Add("0x10")
	f.Add("1e3")
	f.Fuzz(func(t *testing.T, raw string) {
		srv := newAPIServer(t)
		req := httptest.NewRequest(http.MethodGet, "/api/txn/"+raw+"/status", http.NoBody)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		assert.NotEqual(t, http.StatusInternalServerError, w.Code)
	})
}

// FuzzIsolationFromString covers the isolation-level parser for
// arbitrary strings. The handler must not panic and must return a 200
// (known) or 400 (unknown) status.
func FuzzIsolationFromString(f *testing.F) {
	f.Add("read_committed")
	f.Add("rc")
	f.Add("")
	f.Add("BOGUS")
	f.Add("read_uncommitted")
	f.Add("serializable")
	f.Fuzz(func(t *testing.T, iso string) {
		// Avoid quote-injection breaking the JSON envelope: replace
		// any double-quote with a single quote before splicing into
		// the body string.
		escaped := strings.ReplaceAll(iso, `"`, `'`)
		srv := newAPIServer(t)
		body := `{"protocol":"mvcc","isolation":"` + escaped + `"}`
		req := httptest.NewRequest(http.MethodPost, "/api/txn/begin", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, req)
		assert.NotEqual(t, http.StatusInternalServerError, w.Code)
	})
}

// TestEdge_MetricsNoCollector verifies /api/metrics returns an empty
// object when no metrics collector is configured (the s.m == nil branch).
func TestEdge_MetricsNoCollector(t *testing.T) {
	catalog := storage.NewCatalog()
	mgr := txn.NewManager(catalog)
	wfg := deadlock.NewWaitForGraph()
	dlHistory := deadlock.NewHistory()
	dd := deadlock.NewDetector(wfg, dlHistory, func(victimID uint64, _ []uint64) {
		_ = mgr.Abort(txn.ID(victimID), types.NewDeadlockError())
	})
	mgr.WFG.AddEdgeFn = wfg.AddEdge
	mgr.WFG.RemoveNodeFn = wfg.RemoveNode
	dd.Start()
	t.Cleanup(dd.Stop)
	srv := NewServerWithConfig(mgr, dd, wfg, dlHistory, nil, nil, ServerConfig{})
	srv.SetReadyForTest(true)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/metrics", http.NoBody)
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var m map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&m))
	assert.Empty(t, m)
}

// TestEdge_DeleteRow verifies the /api/txn/{id}/delete handler (handleDelete).
func TestEdge_DeleteRow(t *testing.T) {
	srv := newAPIServer(t)
	w := post(srv, "/api/txn/begin", `{"protocol":"mvcc","isolation":"read_committed"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var b map[string]interface{}
	decodeJSONBody(t, w, &b)
	id := int(b["id"].(float64))
	w = post(srv, "/api/txn/"+itoa(id)+"/delete",
		`{"table":"accounts","key":"1"}`)
	require.Equal(t, http.StatusOK, w.Code)
}

// TestEdge_DeleteMissingRow verifies deleting a nonexistent row returns 404
// because the row is not found.
func TestEdge_DeleteMissingRow(t *testing.T) {
	srv := newAPIServer(t)
	w := post(srv, "/api/txn/begin", `{"protocol":"mvcc","isolation":"read_committed"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var b map[string]interface{}
	decodeJSONBody(t, w, &b)
	id := int(b["id"].(float64))
	w = post(srv, "/api/txn/"+itoa(id)+"/delete",
		`{"table":"accounts","key":"nonexistent"}`)
	require.Equal(t, http.StatusNotFound, w.Code)
}

// TestEdge_AbortMissingTxn verifies aborting a non-existent transaction
// is idempotent (returns 200, not an error).
func TestEdge_AbortMissingTxn(t *testing.T) {
	srv := newAPIServer(t)
	w := post(srv, "/api/txn/99999/abort", `{}`)
	require.Equal(t, http.StatusOK, w.Code)
}

// TestEdge_AbortCommittedTxn verifies aborting an already-committed
// transaction returns 409 (the error path in handleAbort).
func TestEdge_AbortCommittedTxn(t *testing.T) {
	srv := newAPIServer(t)
	w := post(srv, "/api/txn/begin", `{"protocol":"mvcc","isolation":"read_committed"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var b map[string]interface{}
	decodeJSONBody(t, w, &b)
	id := int(b["id"].(float64))
	w = post(srv, "/api/txn/"+itoa(id)+"/commit", `{}`)
	require.Equal(t, http.StatusOK, w.Code)
	w = post(srv, "/api/txn/"+itoa(id)+"/abort", `{}`)
	require.Equal(t, http.StatusConflict, w.Code)
}

// TestEdge_ScanWithFilter verifies the /api/txn/{id}/scan handler
// (handleScan) returns rows for the given table.
func TestEdge_ScanWithFilter(t *testing.T) {
	srv := newAPIServer(t)
	w := post(srv, "/api/txn/begin", `{"protocol":"mvcc","isolation":"read_committed"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var b map[string]interface{}
	decodeJSONBody(t, w, &b)
	id := int(b["id"].(float64))
	w = post(srv, "/api/txn/"+itoa(id)+"/scan", `{"table":"accounts"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var r map[string]interface{}
	decodeJSONBody(t, w, &r)
	assert.NotNil(t, r["rows"])
}

// TestEdge_ReleaseSavepoint verifies releasing a savepoint succeeds.
// The savepoint is created via POST /api/txn/{id}/savepoint and released
// via DELETE /api/txn/{id}/savepoint/{name}.
func TestEdge_ReleaseSavepoint(t *testing.T) {
	srv := newAPIServer(t)
	w := post(srv, "/api/txn/begin", `{"protocol":"mvcc","isolation":"read_committed"}`)
	require.Equal(t, http.StatusOK, w.Code)
	var b map[string]interface{}
	decodeJSONBody(t, w, &b)
	id := int(b["id"].(float64))
	w = post(srv, "/api/txn/"+itoa(id)+"/savepoint", `{"name":"sp1"}`)
	require.Equal(t, http.StatusOK, w.Code)
	req := httptest.NewRequest(http.MethodDelete, "/api/txn/"+itoa(id)+"/savepoint/sp1", http.NoBody)
	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, req)
	require.Equal(t, http.StatusOK, w2.Code)
}

// TestEdge_LockByResourceNotFound verifies lock-by-resource returns 200
// with an empty result when no lock queue exists for the resource.
func TestEdge_LockByResourceNotFound(t *testing.T) {
	srv := newAPIServer(t)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/locks/nonexistent_table/nonexistent_key", http.NoBody)
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var r map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&r))
	assert.Empty(t, r["request"])
}

// TestEdge_Vacuum verifies the vacuum endpoint succeeds.
func TestEdge_Vacuum(t *testing.T) {
	srv := newAPIServer(t)
	w := post(srv, "/api/mvcc/vacuum", `{}`)
	require.Equal(t, http.StatusOK, w.Code)
	var r map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&r))
	assert.Equal(t, true, r["ok"])
}

// TestEdge_MVCCStats verifies the MVCC stats endpoint returns stats.
func TestEdge_MVCCStats(t *testing.T) {
	srv := newAPIServer(t)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/mvcc/stats", http.NoBody)
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var r map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&r))
	assert.NotNil(t, r["totalChains"])
}

// TestEdge_MetricsWithCollector verifies /api/metrics returns a snapshot
// when a metrics collector is configured.
func TestEdge_MetricsWithCollector(t *testing.T) {
	srv := newAPIServer(t)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/metrics", http.NoBody)
	srv.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var m map[string]interface{}
	require.NoError(t, json.NewDecoder(w.Body).Decode(&m))
	assert.NotEmpty(t, m)
}
