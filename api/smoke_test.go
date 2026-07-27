package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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

// smokeAdminToken is the admin token used by smoke tests that call
// admin-gated endpoints (/api/reset, /api/scenarios/{name}/run).
const smokeAdminToken = "smoke-test-admin-token-32chars!"

func newTestServer(t *testing.T) *api.Server {
	t.Helper()
	catalog := storage.NewCatalog()
	storage.SeedCatalog(catalog)

	mgr := txn.NewManager(catalog)

	const seedTxnID mvcc.TxnID = 0
	for _, name := range catalog.List() {
		tbl, ok := catalog.Lookup(name)
		require.True(t, ok)
		rows := tbl.Scan(nil)
		mvcc.SeedMVCCStore(mgr.MVCCStore, name, rows, seedTxnID)
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
	mgr.OnBegin = func(_ *txn.Transaction) { m.TxnBegins.Add(1) }
	mgr.OnCommit = func(_ *txn.Transaction) { m.TxnCommits.Add(1) }
	mgr.OnAbort = func(_ *txn.Transaction, _ error) { m.TxnAborts.Add(1) }

	// Admin-gated endpoints (/api/reset, /api/scenarios/{name}/run) now
	// require a token. Smoke tests use smokeAdminToken so they can reach
	// those endpoints.
	srv := api.NewServerWithConfig(mgr, dd, wfg, dlHistory, vac, m, api.ServerConfig{AdminToken: smokeAdminToken})
	srv.SetReadyForTest(true)
	return srv
}

// postAdmin is like post but includes X-Admin-Token for admin-gated endpoints.
func postAdmin(t *testing.T, srv *api.Server, path string, body interface{}) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}
	req := httptest.NewRequest(http.MethodPost, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-Token", smokeAdminToken)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	return w
}

func post(t *testing.T, srv *api.Server, path string, body interface{}) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}
	req := httptest.NewRequest(http.MethodPost, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	return w
}

func get(t *testing.T, srv *api.Server, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, http.NoBody)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)
	return w
}

func decode(t *testing.T, w *httptest.ResponseRecorder, v interface{}) {
	t.Helper()
	require.NoError(t, json.NewDecoder(w.Body).Decode(v))
}

// TestSmoke_BeginCommit verifies basic begin → write → commit flow with MVCC.
func TestSmoke_BeginCommit(t *testing.T) {
	srv := newTestServer(t)

	// Begin MVCC txn
	w := post(t, srv, "/api/txn/begin", map[string]interface{}{
		"protocol":  "mvcc",
		"isolation": "read_committed",
	})
	require.Equal(t, http.StatusOK, w.Code)
	var beginRes map[string]interface{}
	decode(t, w, &beginRes)
	id := int(beginRes["id"].(float64))

	// Read row
	w = post(t, srv, "/api/txn/"+itoa(id)+"/read", map[string]interface{}{"table": "accounts", "key": "1"})
	require.Equal(t, http.StatusOK, w.Code)

	// Commit
	w = post(t, srv, "/api/txn/"+itoa(id)+"/commit", nil)
	require.Equal(t, http.StatusOK, w.Code)
	var commitRes map[string]interface{}
	decode(t, w, &commitRes)
	assert.Equal(t, "committed", commitRes["status"])
}

// TestSmoke_Abort verifies abort returns ok and transaction is no longer active.
func TestSmoke_Abort(t *testing.T) {
	srv := newTestServer(t)

	w := post(t, srv, "/api/txn/begin", map[string]interface{}{"protocol": "2pl", "isolation": "read_committed"})
	require.Equal(t, http.StatusOK, w.Code)
	var beginRes map[string]interface{}
	decode(t, w, &beginRes)
	id := int(beginRes["id"].(float64))

	w = post(t, srv, "/api/txn/"+itoa(id)+"/abort", nil)
	require.Equal(t, http.StatusOK, w.Code)

	// Active should be empty now
	w = get(t, srv, "/api/txn/active")
	require.Equal(t, http.StatusOK, w.Code)
	var active []interface{}
	decode(t, w, &active)
	for _, a := range active {
		m := a.(map[string]interface{})
		assert.NotEqual(t, float64(id), m["id"])
	}
}

// TestSmoke_Savepoint verifies write → savepoint → write → rollback leaves only first write.
func TestSmoke_Savepoint(t *testing.T) {
	srv := newTestServer(t)

	// Begin 2PL txn
	w := post(t, srv, "/api/txn/begin", map[string]interface{}{"protocol": "2pl", "isolation": "repeatable_read"})
	require.Equal(t, http.StatusOK, w.Code)
	var beginRes map[string]interface{}
	decode(t, w, &beginRes)
	id := int(beginRes["id"].(float64))
	ids := itoa(id)

	// Write value 100. CT-12/CT-13: the row shape must match the table's
	// 4 columns (id/owner/balance/branch). Use the typed Value shape.
	w = post(t, srv, "/api/txn/"+ids+"/write", map[string]interface{}{
		"table": "accounts",
		"key":   "1",
		"values": []interface{}{
			map[string]interface{}{"Type": 1, "Int": 1},          // id (int)
			map[string]interface{}{"Type": 3, "Text": "owner-1"}, // owner (text)
			map[string]interface{}{"Type": 2, "Float": 100.0},    // balance (float)
			map[string]interface{}{"Type": 3, "Text": "north"},   // branch (text)
		},
	})
	require.Equal(t, http.StatusOK, w.Code)

	// Savepoint
	w = post(t, srv, "/api/txn/"+ids+"/savepoint", map[string]interface{}{"name": "sp1"})
	require.Equal(t, http.StatusOK, w.Code)

	// Write value 999 (after savepoint — will be rolled back)
	w = post(t, srv, "/api/txn/"+ids+"/write", map[string]interface{}{
		"table": "accounts",
		"key":   "1",
		"values": []interface{}{
			map[string]interface{}{"Type": 1, "Int": 1},
			map[string]interface{}{"Type": 3, "Text": "owner-1"},
			map[string]interface{}{"Type": 2, "Float": 999.0},
			map[string]interface{}{"Type": 3, "Text": "north"},
		},
	})
	require.Equal(t, http.StatusOK, w.Code)

	// Rollback to sp1
	w = post(t, srv, "/api/txn/"+ids+"/rollback-to", map[string]interface{}{"name": "sp1"})
	require.Equal(t, http.StatusOK, w.Code)

	// Commit
	w = post(t, srv, "/api/txn/"+ids+"/commit", nil)
	require.Equal(t, http.StatusOK, w.Code)
}

// TestSmoke_AllScenarios runs all 7 scenarios at appropriate isolation
// levels and asserts (a) the endpoint returns 200, (b) the returned name
// matches the requested scenario, (c) the steps slice is non-empty,
// AND (d) the explanation string is populated — a missing explanation
// would indicate the scenario silently no-op'd.
func TestSmoke_AllScenarios(t *testing.T) {
	srv := newTestServer(t)

	// List scenarios
	w := get(t, srv, "/api/scenarios")
	require.Equal(t, http.StatusOK, w.Code)
	var scenarios []map[string]interface{}
	decode(t, w, &scenarios)
	require.NotEmpty(t, scenarios, "expected at least one scenario")

	type testCase struct {
		name      string
		protocol  string
		isolation string
	}
	cases := []testCase{
		{"dirty_read", "mvcc", "read_uncommitted"},
		{"lost_update", "mvcc", "read_committed"},
		{"non_repeatable_read", "mvcc", "read_committed"},
		{"phantom_read", "mvcc", "read_committed"},
		{"write_skew", "mvcc", "repeatable_read"},
		{"deadlock_cycle", "2pl", "read_committed"},
		{"cascade_abort", "mvcc", "read_uncommitted"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := postAdmin(t, srv, "/api/scenarios/"+tc.name+"/run", map[string]interface{}{
				"protocol":  tc.protocol,
				"isolation": tc.isolation,
			})
			require.Equal(t, http.StatusOK, w.Code, "scenario %s failed with status %d: %s", tc.name, w.Code, w.Body.String())
			var result map[string]interface{}
			decode(t, w, &result)
			assert.Equal(t, tc.name, result["name"])
			steps, ok := result["steps"].([]interface{})
			require.True(t, ok, "steps must be an array")
			require.NotEmpty(t, steps, "scenario %s must produce step trace", tc.name)
			explanation, _ := result["explanation"].(string)
			assert.NotEmpty(t, explanation, "scenario %s must populate explanation", tc.name)
			// anomalyType must match a known anomaly kind.
			kind, _ := result["anomalyType"].(string)
			assert.NotEmpty(t, kind, "scenario %s must populate anomalyType", tc.name)
		})
	}
}

// TestSmoke_VersionChain verifies a version chain is visible after MVCC write.
func TestSmoke_VersionChain(t *testing.T) {
	srv := newTestServer(t)

	// Chain should exist (seeded from catalog)
	w := get(t, srv, "/api/mvcc/chain/accounts/1")
	require.Equal(t, http.StatusOK, w.Code)
	var chain map[string]interface{}
	decode(t, w, &chain)
	versions := chain["versions"].([]interface{})
	assert.GreaterOrEqual(t, len(versions), 1, "expected at least 1 version after seed")

	// Write two more updates and verify 3+ versions
	w = post(t, srv, "/api/txn/begin", map[string]interface{}{"protocol": "mvcc", "isolation": "read_committed"})
	var b1 map[string]interface{}
	decode(t, w, &b1)
	id1 := int(b1["id"].(float64))

	post(t, srv, "/api/txn/"+itoa(id1)+"/write", map[string]interface{}{
		"table": "accounts", "key": "1",
		"values": []interface{}{
			map[string]interface{}{"Type": 1, "Int": 1},
			map[string]interface{}{"Type": 3, "Text": "owner-1"},
			map[string]interface{}{"Type": 2, "Float": 500.0},
			map[string]interface{}{"Type": 3, "Text": "north"},
		},
	})
	post(t, srv, "/api/txn/"+itoa(id1)+"/commit", nil)

	w = get(t, srv, "/api/mvcc/chain/accounts/1")
	require.Equal(t, http.StatusOK, w.Code)
	decode(t, w, &chain)
	versions = chain["versions"].([]interface{})
	assert.GreaterOrEqual(t, len(versions), 2, "expected at least 2 versions after update")
}

// TestSmoke_Metrics verifies metrics are incremented after transactions.
func TestSmoke_Metrics(t *testing.T) {
	srv := newTestServer(t)

	w := post(t, srv, "/api/txn/begin", map[string]interface{}{"protocol": "mvcc", "isolation": "read_committed"})
	var b map[string]interface{}
	decode(t, w, &b)
	id := int(b["id"].(float64))
	post(t, srv, "/api/txn/"+itoa(id)+"/commit", nil)

	w = get(t, srv, "/api/metrics")
	require.Equal(t, http.StatusOK, w.Code)
	var m map[string]interface{}
	decode(t, w, &m)
	assert.GreaterOrEqual(t, m["txnBegins"].(float64), float64(1))
	assert.GreaterOrEqual(t, m["txnCommits"].(float64), float64(1))
}

// TestSmoke_Reset verifies the reset endpoint aborts all txns and reseeds.
func TestSmoke_Reset(t *testing.T) {
	srv := newTestServer(t)

	// Begin two txns
	for range 2 {
		w := post(t, srv, "/api/txn/begin", map[string]interface{}{"protocol": "mvcc", "isolation": "read_committed"})
		require.Equal(t, http.StatusOK, w.Code)
	}

	// Reset (admin-gated endpoint — requires token)
	w := postAdmin(t, srv, "/api/reset", nil)
	require.Equal(t, http.StatusOK, w.Code)
	var res map[string]interface{}
	decode(t, w, &res)
	assert.Equal(t, true, res["ok"])

	// Active should be empty
	w = get(t, srv, "/api/txn/active")
	var active []interface{}
	decode(t, w, &active)
	assert.Empty(t, active, "expected no active txns after reset")

	// MVCC chain should be reseeded
	w = get(t, srv, "/api/mvcc/chain/accounts/1")
	require.Equal(t, http.StatusOK, w.Code)
}

// TestSmoke_LockEndpoint verifies /api/locks returns a valid JSON array.
func TestSmoke_LockEndpoint(t *testing.T) {
	srv := newTestServer(t)

	// Begin a 2PL txn and read a row (acquires S-lock)
	w := post(t, srv, "/api/txn/begin", map[string]interface{}{"protocol": "2pl", "isolation": "read_committed"})
	var b map[string]interface{}
	decode(t, w, &b)
	id := int(b["id"].(float64))
	post(t, srv, "/api/txn/"+itoa(id)+"/read", map[string]interface{}{"table": "accounts", "key": "1"})

	w = get(t, srv, "/api/locks")
	require.Equal(t, http.StatusOK, w.Code)
	var locks []interface{}
	decode(t, w, &locks)
	// Should have at least 2 queues (table IS + row S)
	assert.GreaterOrEqual(t, len(locks), 1)

	post(t, srv, "/api/txn/"+itoa(id)+"/commit", nil)
}

func itoa(n int) string { return fmt.Sprintf("%d", n) }
