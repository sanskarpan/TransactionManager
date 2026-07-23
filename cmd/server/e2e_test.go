// Package main is the entry point for the Transaction Manager HTTP server.
// The e2e test boots the server as a subprocess and exercises the HTTP API
// at the integration level.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const e2ePort = 18520

var e2eBase = fmt.Sprintf("http://127.0.0.1:%d", e2ePort)

func buildServer(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "server"+exeSuffix())
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = filepath.Join(repoRoot(t), "cmd", "server")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build server: %v\n%s", err, out)
	}
	return bin
}

func repoRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "env", "GOMOD").CombinedOutput()
	if err != nil {
		t.Fatalf("go env GOMOD: %v", err)
	}
	return filepath.Dir(strings.TrimSpace(string(out)))
}

func exeSuffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

func startServer(t *testing.T) *exec.Cmd {
	t.Helper()
	bin := buildServer(t)
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"LISTEN_ADDR=127.0.0.1:"+strconv.Itoa(e2ePort),
		"LOG_LEVEL=error",
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
		}
	})
	return cmd
}

func waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(e2eBase + "/readyz")
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			return
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("server did not become ready within 10s")
}

func e2ePost(t *testing.T, path, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, e2eBase+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	return resp
}

func e2eGet(t *testing.T, path string) *http.Response {
	t.Helper()
	resp, err := http.Get(e2eBase + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	return resp
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	_ = resp.Body.Close()
	return string(b)
}

func decodeMap(t *testing.T, data string) map[string]interface{} {
	t.Helper()
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(data), &m); err != nil {
		t.Fatalf("decode JSON: %v\nbody: %s", err, data)
	}
	return m
}

func beginTxn(t *testing.T, protocol, isolation string) int {
	t.Helper()
	body := fmt.Sprintf(`{"protocol":"%s","isolation":"%s"}`, protocol, isolation)
	resp := e2ePost(t, "/api/txn/begin", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("begin: status %d, body: %s", resp.StatusCode, readBody(t, resp))
	}
	m := decodeMap(t, readBody(t, resp))
	return int(m["id"].(float64))
}

// TestE2E_ServerLifecycle verifies the server starts, becomes ready, and
// can be gracefully shut down.
func TestE2E_ServerLifecycle(t *testing.T) {
	startServer(t)
	waitReady(t)

	resp := e2eGet(t, "/healthz")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz: status %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	resp = e2eGet(t, "/readyz")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz: status %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

// TestE2E_TxnLifecycle verifies begin → write → read → commit flow.
func TestE2E_TxnLifecycle(t *testing.T) {
	startServer(t)
	waitReady(t)

	// Begin
	id := beginTxn(t, "mvcc", "read_committed")

	// Write
	resp := e2ePost(t, fmt.Sprintf("/api/txn/%d/write", id),
		`{"table":"accounts","key":"1","values":[{"Type":1,"Int":1},{"Type":3,"Text":"owner-1"},{"Type":2,"Float":10000},{"Type":3,"Text":"north"}]}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write: status %d, body: %s", resp.StatusCode, readBody(t, resp))
	}
	_ = resp.Body.Close()

	// Read
	resp = e2ePost(t, fmt.Sprintf("/api/txn/%d/read", id),
		`{"table":"accounts","key":"1"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("read: status %d, body: %s", resp.StatusCode, readBody(t, resp))
	}
	m := decodeMap(t, readBody(t, resp))
	if m["found"] != true {
		t.Fatal("read did not find row")
	}

	// Commit
	resp = e2ePost(t, fmt.Sprintf("/api/txn/%d/commit", id), `{}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("commit: status %d, body: %s", resp.StatusCode, readBody(t, resp))
	}
	_ = resp.Body.Close()
}

// TestE2E_MVCCScan verifies scan returns rows from the seeded data.
func TestE2E_MVCCScan(t *testing.T) {
	startServer(t)
	waitReady(t)

	id := beginTxn(t, "mvcc", "read_committed")
	resp := e2ePost(t, fmt.Sprintf("/api/txn/%d/scan", id), `{"table":"accounts"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("scan: status %d, body: %s", resp.StatusCode, readBody(t, resp))
	}
	m := decodeMap(t, readBody(t, resp))
	rows, ok := m["rows"].([]interface{})
	if !ok || len(rows) == 0 {
		t.Fatal("scan returned no rows")
	}
}

// TestE2E_AbortAndStatus verifies abort transitions txn state correctly.
func TestE2E_AbortAndStatus(t *testing.T) {
	startServer(t)
	waitReady(t)

	id := beginTxn(t, "mvcc", "read_committed")

	// Abort
	resp := e2ePost(t, fmt.Sprintf("/api/txn/%d/abort", id), `{}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("abort: status %d, body: %s", resp.StatusCode, readBody(t, resp))
	}
	_ = resp.Body.Close()

	// Status should show aborted
	resp = e2eGet(t, fmt.Sprintf("/api/txn/%d/status", id))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status after abort: status %d, body: %s", resp.StatusCode, readBody(t, resp))
	}
	m := decodeMap(t, readBody(t, resp))
	if m["status"] != "aborted" {
		t.Fatalf("expected aborted, got %v", m["status"])
	}

	// Double abort should succeed idempotently
	resp = e2ePost(t, fmt.Sprintf("/api/txn/%d/abort", id), `{}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("double abort: status %d, body: %s", resp.StatusCode, readBody(t, resp))
	}
	_ = resp.Body.Close()
}

// TestE2E_Metrics verifies the metrics endpoint returns data.
func TestE2E_Metrics(t *testing.T) {
	startServer(t)
	waitReady(t)

	resp := e2eGet(t, "/api/metrics")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics: status %d, body: %s", resp.StatusCode, readBody(t, resp))
	}
	m := decodeMap(t, readBody(t, resp))
	if _, ok := m["txnBegins"]; !ok {
		t.Fatal("metrics missing txnBegins")
	}
}

// TestE2E_ListScenarios verifies the scenarios endpoint.
func TestE2E_ListScenarios(t *testing.T) {
	startServer(t)
	waitReady(t)

	resp := e2eGet(t, "/api/scenarios")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("scenarios: status %d, body: %s", resp.StatusCode, readBody(t, resp))
	}
	_ = resp.Body.Close()
}

// TestE2E_InsertAndDelete verifies insert+delete flow.
func TestE2E_InsertAndDelete(t *testing.T) {
	startServer(t)
	waitReady(t)

	id := beginTxn(t, "mvcc", "read_committed")

	// Insert
	resp := e2ePost(t, fmt.Sprintf("/api/txn/%d/insert", id),
		`{"table":"accounts","key":"999","values":[{"Type":1,"Int":999},{"Type":3,"Text":"e2e-test"},{"Type":2,"Float":5000},{"Type":3,"Text":"central"}]}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("insert: status %d, body: %s", resp.StatusCode, readBody(t, resp))
	}
	_ = resp.Body.Close()

	// Read it back
	resp = e2ePost(t, fmt.Sprintf("/api/txn/%d/read", id),
		`{"table":"accounts","key":"999"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("read after insert: status %d, body: %s", resp.StatusCode, readBody(t, resp))
	}
	m := decodeMap(t, readBody(t, resp))
	if m["found"] != true {
		t.Fatal("inserted row not found")
	}

	// Delete
	resp = e2ePost(t, fmt.Sprintf("/api/txn/%d/delete", id),
		`{"table":"accounts","key":"999"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete: status %d, body: %s", resp.StatusCode, readBody(t, resp))
	}
	_ = resp.Body.Close()

	// Commit
	resp = e2ePost(t, fmt.Sprintf("/api/txn/%d/commit", id), `{}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("commit: status %d, body: %s", resp.StatusCode, readBody(t, resp))
	}
	_ = resp.Body.Close()
}

// TestE2E_Savepoint verifies savepoint, rollback, and release.
func TestE2E_Savepoint(t *testing.T) {
	startServer(t)
	waitReady(t)

	id := beginTxn(t, "mvcc", "repeatable_read")

	// Create a savepoint
	resp := e2ePost(t, fmt.Sprintf("/api/txn/%d/savepoint", id), `{"name":"sp1"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("savepoint: status %d, body: %s", resp.StatusCode, readBody(t, resp))
	}
	_ = resp.Body.Close()

	// Rollback to it
	resp = e2ePost(t, fmt.Sprintf("/api/txn/%d/rollback-to", id), `{"name":"sp1"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rollback: status %d, body: %s", resp.StatusCode, readBody(t, resp))
	}
	_ = resp.Body.Close()

	// Release it
	req, err := http.NewRequest(http.MethodDelete,
		fmt.Sprintf("%s/api/txn/%d/savepoint/sp1", e2eBase, id), nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("release savepoint: status %d, body: %s", resp2.StatusCode, readBody(t, resp2))
	}
	_ = resp2.Body.Close()

	// Commit
	resp = e2ePost(t, fmt.Sprintf("/api/txn/%d/commit", id), `{}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("commit: status %d, body: %s", resp.StatusCode, readBody(t, resp))
	}
	_ = resp.Body.Close()
}
