# Production Readiness Audit — Defect List (Phase 1)

Audit performed against the full tree: `cmd/`, `api/`, `internal/` (txn, lock, mvcc,
deadlock, isolation, storage, scenario, metrics, types), `benchmark/`, `web/`, and
project metadata (`go.mod`, `Makefile`, `README.md`).

Findings are ordered by severity. Each fix is accompanied by a regression test in
the corresponding phase. Locations use `file:line` referencing the pre-fix tree.

---

## Critical

### C-01 — `txn.ReadSet` / `WriteSet` mutated under inconsistent locking (data race)
- **Category:** Correctness / Reliability
- **Location:** `internal/txn/manager.go:543-544` (MVCCRead), `:625-627` (MVCCWrite), `:687` (TwoPLWrite)
- **Root cause:** `MVCCRead` writes `txn.ReadSet[...]` and `txn.SIReadKeys[...]` under `txn.mu`.
  `MVCCWrite` writes `txn.WriteSet[...]` *without* taking `txn.mu`. `TwoPLWrite` likewise
  writes `txn.WriteSet[...]` and `txn.UndoLog.Append(...)` without `txn.mu`. A concurrent
  reader (e.g. `handleStatus` walking the txn, or `ActiveTxns`) racing with a writer
  triggers a `go test -race` failure on the map.
- **Blast radius:** Any concurrent API request that touches a txn while another goroutine
  mutates its `WriteSet`/`ReadSet` → data race / crash under `-race` in production.
- **Fix:** Centralize `txn` internal-state mutation behind accessor methods (`addRead`,
  `addWrite`) that always hold `txn.mu`. Replace bare map writes with the accessors.

### C-02 — `MVCCWrite` (UndoUpdate) orphans versions between `head` and `visible`
- **Category:** Correctness
- **Location:** `internal/txn/manager.go:601-602` (UndoUpdate branch)
- **Root cause:** Code walks `chain.UnsafeHead()` → `Prev` to find the first visible
  version, then does `chain.SetHead(newVer)` with `newVer.Prev = visible`. Any version
  between the original head and `visible` (e.g. an aborted transaction's un-pruned
  version) is unlinked from the chain and can never be reached again. `RemoveByXMin`
  walks from `head`, so the orphaned version is also never cleaned up.
- **Blast radius:** Memory leak; in pathological cases (aborted txn whose version sat
  at head), a subsequent update loses visibility of stale-but-referenced versions.
- **Fix:** Splice `newVer` into the chain *between visible and visible.Prev* rather
  than replacing head. i.e. `newVer.Prev = visible.Prev; visible.Prev = newVer; head = newVer`
  only when `visible == head`; otherwise keep head and insert correctly.

### C-03 — Frontend / backend lock-timeout contract mismatch (unit + field name)
- **Category:** Correctness / Reliability
- **Location:** `api/handlers_txn.go:18,46` vs `web/src/api.ts:8`
- **Root cause:** `BeginRequest.LockTimeout` is documented as **milliseconds**
  (`lockTimeoutMs`) and the handler does `time.Duration(req.LockTimeout) * time.Millisecond`.
  The frontend sends `lockTimeoutSec: timeout ?? 30` (i.e. **30 seconds**). The backend
  interprets `30` as 30 **milliseconds**, so any contended lock acquisition times out
  almost instantly. The field names also differ (`lockTimeoutMs` vs `lockTimeoutSec`),
  so the JSON key never matches and the backend falls back to the 5 s default *only*
  because the field is never populated — but if the field names ever align, the unit
  bug produces 30 ms timeouts.
- **Blast radius:** Every frontend-driven contended transaction spurious-fails with
  `LOCK_TIMEOUT` once the field names are reconciled; currently the timeout is silently
  ignored (defaulted) which masks the bug.
- **Fix:** Standardize on `lockTimeoutMs` (int, ms) on both sides; frontend converts
  seconds → ms before sending. Add a round-trip test.

### C-04 — `Vacuum.Stop()` is not idempotent (double-close panic)
- **Category:** Reliability
- **Location:** `internal/mvcc/vacuum.go:52-54`
- **Root cause:** `Stop()` does `close(v.stop)` with no `sync.Once` guard. Two calls
  (e.g. `defer vac.Stop()` + explicit shutdown path) panic on close-of-closed-channel.
- **Blast radius:** Process crash during shutdown if `Stop` is invoked twice (common
  in graceful-shutdown + test-cleanup paths).
- **Fix:** Guard `Stop` with `sync.Once`.

### C-05 — `Reset()` races in-flight operations (no quiesce barrier)
- **Category:** Correctness / Reliability
- **Location:** `internal/txn/manager.go:334-365`
- **Root cause:** `Reset()` collects active IDs under `RLock`, releases the lock, calls
  `Abort` (re-acquires `m.mu`) for each, then `MVCCStore.Clear()` and reseeds. A
  concurrently-running `MVCCRead`/`MVCCWrite` on another goroutine can be mid-flight
  on a chain that `Clear` deletes out from under it, producing a nil-deref or stale
  read.
- **Blast radius:** Hitting `POST /api/reset` during traffic → nil-pointer panics in
  handlers, lost writes.
- **Fix:** Take `m.mu.Lock()` for the whole reset (Abort is inlined to not re-acquire),
  OR introduce a generation counter that readers check.

### C-06 — `http.Server` has no body size limits; handlers decode unbounded bodies
- **Category:** Security
- **Location:** `cmd/server/main.go:79-85`, all `json.NewDecoder(r.Body).Decode` sites
- **Root cause:** No `MaxHeaderBytes`, no `http.MaxBytesReader` wrapping of `r.Body`.
  A client can stream a multi-GB JSON body; `json.Decode` will allocate until OOM.
- **Blast radius:** Memory-exhaustion DoS from a single connection.
- **Fix:** Wrap `r.Body` with `http.MaxBytesReader(w, r.Body, maxBodyBytes)` in a
  middleware; set `MaxHeaderBytes` on the server.

### C-07 — No authentication, open CORS `*`, `/api/reset` unguarded
- **Category:** Security
- **Location:** `api/server.go:118-128` (CORS), `api/handlers_infra.go:148-151` (reset)
- **Root cause:** Every endpoint is unauthenticated. CORS is `*`. The destructive
  `POST /api/reset` (aborts all txns, wipes MVCC, reseeds) has no guard. Combined with
  open CORS, any web origin can wipe the database.
- **Blast radius:** Complete data loss from any malicious or accidentally-triggered
  cross-origin request.
- **Fix:** Add an opt-in admin-token middleware for destructive endpoints (`/reset`,
  `/mvcc/vacuum`, `/benchmark/run`); restrict CORS to a configurable allowlist;
  document the threat model.

### C-08 — `scenario` / `benchmark` request validation fails open
- **Category:** Security / Correctness
- **Location:** `api/handlers_scenario.go:53-54`, `:145-146`
- **Root cause:** `protocol, _ := protocolFromString(req.Protocol)` and
  `isolation, _ := isolationFromString(req.Isolation)` silently discard parse errors;
  unknown values silently fall back to `2PL` / `ReadCommitted`. The handler then runs
  a scenario with parameters the caller did not request and reports success.
- **Blast radius:** Misleading benchmark/scenario results; can mask regressions.
- **Fix:** Return `400 Bad Request` on unknown protocol/isolation. Use the existing
  `ErrInvalidIsolation` code.

---

## High

### H-01 — `benchmarkJobs` global map grows unbounded; job IDs collide
- **Category:** Reliability
- **Location:** `api/handlers_scenario.go:107-111, 152-157`
- **Root cause:** `benchmarkJobs` is a package-level `map[string]*benchmarkJob` that is
  appended to on every `POST /api/benchmark/run` and never pruned. `benchmarkJobSeq`
  is incremented but the value is never used in the job ID (`jobID` is time-formatted),
  so two jobs started in the same second collide and the second overwrites the first.
- **Blast radius:** Memory leak over time; lost results under burst load.
- **Fix:** Move `benchmarkJobs` onto `Server`; use a monotonic counter in the job ID;
  cap the map and evict oldest finished jobs.

### H-02 — `committed` / `aborted` maps in `TxnManager` grow forever
- **Category:** Reliability
- **Location:** `internal/txn/manager.go:20-21`
- **Root cause:** `m.committed` and `m.aborted` are never pruned. Every txn ID ever
  created stays reachable forever. `IsCommitted`/`IsAborted` are O(1) but the map
  grows with total historical txns.
- **Blast radius:** Memory leak in long-running deployments.
- **Fix:** Introduce a `minRetainedID` watermark = `OldestActiveTxnID()`; periodically
  drop committed/aborted entries below the watermark whose versions have also been
  vacuumed. Document the trade-off.

### H-03 — `LockTable.queues` and `MVCCStore.chains` never garbage-collected
- **Category:** Reliability
- **Location:** `internal/lock/table.go:6-21`, `internal/mvcc/store.go:11-39`
- **Root cause:** Empty queues/chains (after lock release / vacuum prune) remain in
  the `sync.Map` forever. Over a workload touching many distinct keys, the maps grow
  without bound.
- **Blast radius:** Memory leak proportional to cardinality of touched keys.
- **Fix:** `LoadAndDelete` empty queues in `ReleaseAllLocks`; `LoadAndDelete` empty
  chains in `Vacuum.pruneVersions`.

### H-04 — No HTTP request logging, no request IDs, no structured logging
- **Category:** Observability
- **Location:** `cmd/server/main.go` (uses `log.Printf`), `api/server.go` (no logging middleware)
- **Root cause:** Bare `log.Printf` in main; no per-request log line, no correlation
  ID, no level field, no redaction. Impossible to triage a production incident.
- **Blast radius:** No way to diagnose 5xx spikes or correlate user reports to logs.
- **Fix:** Adopt `log/slog` (stdlib); add a `RequestID` + `logging` middleware on the
  chi router; replace `log.Printf` in main.

### H-05 — No `/healthz` (liveness) or `/readyz` (readiness) endpoints
- **Category:** Observability
- **Location:** `api/server.go:52-106` (route table)
- **Root cause:** No health endpoints exist. Orchestration systems cannot distinguish
  "process alive but not ready" from "process alive and serving".
- **Blast radius:** Rolling deploys and self-healing don't work.
- **Fix:** Add `/healthz` (process up) and `/readyz` (catalog seeded + HTTP server
  accepting connections).

### H-06 — Raw `err.Error()` returned to API clients
- **Category:** Security / Observability
- **Location:** `api/handlers_txn.go:53,89,108`, `api/handlers_ops.go:47,92,128,164,210`, `api/handlers_scenario.go:49`
- **Root cause:** `errResponse(w, httpStatus(err), err.Error())` echoes the raw error
  string to the caller. Internal paths, txn IDs, and implementation details leak.
- **Blast radius:** Information disclosure to API consumers.
- **Fix:** Map `*types.TxnError` to a stable public message; log the raw error server-
  side with request ID.

### H-07 — `io.EOF` compared by string
- **Category:** Reliability
- **Location:** `api/handlers_txn.go:31`
- **Root cause:** `err.Error() != "EOF"` instead of `errors.Is(err, io.EOF)`. If the
  stdlib ever changes the error message or wraps it, the check breaks silently.
- **Blast radius:** Empty-body POST /api/txn/begin starts returning 400.
- **Fix:** `if err != nil && !errors.Is(err, io.EOF)`.

### H-08 — HTTP handler ignores request context (client disconnects don't cancel)
- **Category:** Reliability
- **Location:** All `handle*` functions, `internal/lock/grant.go:38-98`
- **Root cause:** `AcquireLock` blocks on `<-req.GrantedCh` / `<-abortCh` / `<-timer.C`
  with no `r.Context().Done()` branch. A client that disconnects during a contended
  lock acquisition still holds the txn (and its locks) for the full `lockTimeout`.
- **Blast radius:** Abandoned client connections keep transactions and locks pinned.
- **Fix:** Plumb `r.Context()` into `AcquireLock` as an additional cancellation source.

### H-09 — Deadlock detector `Abort(victim)` not coordinated with WFG removal
- **Category:** Reliability
- **Location:** `internal/deadlock/detector.go:42-53`, `cmd/server/main.go:51-54`
- **Root cause:** `FindCycle` returns a victim; the detector then calls `abort(victim)`
  which (in main) calls `mgr.Abort` → `ReleaseAllLocks` → `wfg.RemoveEdges`. Between
  `FindCycle` and the WFG node actually being removed, the next 50 ms tick can find the
  same cycle again, add a duplicate `DeadlockRecord`, and re-abort (idempotent, but
  noisy). Also, if `abort` panics or hangs, the cycle stays in the graph forever.
- **Blast radius:** Duplicate deadlock records; potential permanent cycle if abort
  fails.
- **Fix:** `FindCycle` should atomically remove the victim's edges before returning;
  detector should track "in-progress abort" set to skip re-detection.

### H-10 — `select { default: close(AbortCh) }` pattern is fragile
- **Category:** Reliability
- **Location:** `internal/txn/manager.go:206-210`
- **Root cause:** The non-blocking `select` + `close` pattern is safe only because
  `Abort` holds `m.mu`. Any future caller that aborts without `m.mu` (or a second
  concurrent abort from a lock-acquire timeout path) double-closes `AbortCh` → panic.
- **Blast radius:** Process crash if a second abort path is added.
- **Fix:** Use `sync.Once` to guard the close.

### H-11 — Frontend `commit`/`abort` type signatures don't match backend response
- **Category:** Correctness
- **Location:** `web/src/api.ts:9-10` vs `api/handlers_txn.go:97,116`
- **Root cause:** Frontend types both as `{ ok: boolean }`; backend returns
  `{ id: number, status: "committed" | "aborted" }`. TypeScript callers reading `ok`
  get `undefined`.
- **Blast radius:** UI shows wrong success/failure state.
- **Fix:** Align frontend types to backend response shape.

### H-12 — No HTTP metrics middleware (latency, status codes, rate)
- **Category:** Observability
- **Location:** `api/server.go:52-106`
- **Root cause:** `metrics.Metrics` only tracks txn-level counters. There is no
  per-route latency histogram, no error-rate, no inflight gauge.
- **Blast radius:** Cannot detect slow endpoints or 5xx spikes.
- **Fix:** Add a `metrics.HTTPMiddleware` recording latency + status for every route.

---

## Medium

### M-01 — Dead code: `globalCounter`, `NextTxnID`, `ResetCounter`
- **Category:** Code quality
- **Location:** `internal/txn/id.go:7-15`
- **Root cause:** Package-level `globalCounter` is never read by `TxnManager` (which
  uses its own `m.nextID`). `ResetCounter` has no callers. Global mutable state is an
  anti-pattern that breaks test isolation.
- **Fix:** Delete the file.

### M-02 — Dead code: `SavepointStack` type
- **Category:** Code quality
- **Location:** `internal/txn/savepoint.go:10-44`
- **Root cause:** `TxnManager` uses `[]*Savepoint` directly (`transaction.go:56`).
  `SavepointStack` is never instantiated.
- **Fix:** Delete the type, keep `Savepoint`.

### M-03 — Dead code: `LockRequest.CancelCh`, `LockU`, `ResourcePredicate`, `UpgradeLock`
- **Category:** Code quality
- **Location:** `internal/lock/request.go:32`, `internal/lock/mode.go:6`, `internal/lock/resource.go:11`, `internal/lock/upgrade.go` (whole file)
- **Root cause:** `CancelCh` is never sent-to/closed. `LockU` is in the compatibility
  matrix but no caller requests it. `ResourcePredicate` is never constructed.
  `UpgradeLock` is never invoked (manager only ever acquires fresh, never upgrades).
- **Fix:** Remove; document that upgrade is currently unused.

### M-04 — Dead code: `TxnPrepared` status, `Priority`, `RestartCount`, `WaitTime`
- **Category:** Code quality
- **Location:** `internal/txn/transaction.go:18,67`
- **Root cause:** `TxnPrepared` is never set. `Priority`/`RestartCount`/`WaitTime`
  are initialised and never updated.
- **Fix:** Remove or wire up (preferred: remove until 2PC is real).

### M-05 — Dead code: `WaitDie` / `WoundWait` not wired in
- **Category:** Code quality / Correctness (doc)
- **Location:** `internal/deadlock/prevention.go` (whole file), `README.md:39-42`
- **Root cause:** The README advertises three prevention policies (Detect, Wait-Die,
  Wound-Wait) but `LockAcquirer.AcquireLock` always blocks; only the background
  `DeadlockDetector` resolves cycles. Wait-Die / Wound-Wait are dead code with tests.
- **Fix:** Either wire them into `AcquireLock` (parametrised by policy) or remove
  them and correct the README.

### M-06 — Dead code: `WindowedStats`, `SeedFromCatalog`, several `Err*` codes
- **Category:** Code quality
- **Location:** `internal/metrics/counters.go:127-191`, `internal/mvcc/store.go:71-79`, `internal/types/errors.go:22-26,77-79,85-87`
- **Root cause:** `WindowedStats` is never used. `SeedFromCatalog` is an empty stub.
  `ErrInvalidIsolation`/`ErrMaxRetriesExceeded`/`ErrWounded`/`ErrDuplicateKey` and
  their constructors are never returned.
- **Fix:** Remove; keep `ErrDuplicateKey` if a unique-index layer is added later.

### M-07 — Dead directory: `internal/operations/`
- **Category:** Code quality
- **Location:** `internal/operations/` (empty)
- **Fix:** Remove the directory.

### M-08 — `isTypesErr` reimplements `errors.As` incorrectly
- **Category:** Code quality / Correctness
- **Location:** `api/handlers_ops.go:225-230`
- **Root cause:** Does a non-unwrapping type assertion; wrapped `*TxnError`s are missed.
- **Fix:** Replace call sites with `errors.As(err, &txnErr)`.

### M-09 — Magic numbers throughout
- **Category:** Code quality
- **Location:** `internal/txn/manager.go:83` (5s), `api/handlers_txn.go:48` (5s), `internal/deadlock/detector.go:37` (50ms), `cmd/server/main.go:64,82-84,101` (30s/15s/30s/60s/10s), `internal/deadlock/history.go:23` (100), `api/sse.go:29` (64), `api/handlers_infra.go:112` (500ms)
- **Fix:** Hoist into named `const` blocks in each package.

### M-10 — `"\xff\xff\xff\xff"` gap-lock infinity sentinel
- **Category:** Correctness
- **Location:** `internal/txn/gap_lock.go:66,107`
- **Root cause:** A real key containing 0xff bytes collides with the sentinel.
- **Fix:** Use an explicit `bool` "upper-bound open" flag on `ResourceForGap`, or
  document the key-charset constraint in the README.

### M-11 — Empty-directory `internal/operations/` and stray `var _ = ...` suppressions
- **Category:** Code quality
- **Location:** `internal/mvcc/store.go:102`, `internal/mvcc/chain_test.go:87`, `internal/lock/queue_test.go:133`
- **Root cause:** `var _ = types.Value{}` etc. suppress "unused import" lint instead
  of removing the import.
- **Fix:** Remove unused imports.

### M-12 — `go.mod` placeholder module path + `// indirect` on direct deps
- **Category:** Code quality
- **Location:** `go.mod:1,5-10`
- **Root cause:** Module is `github.com/yourname/txn-manager`; all direct deps
  (`chi`, `testify`, `yaml`) are marked `// indirect`.
- **Fix:** Rename to `github.com/sanskarpan/TransactionManager`; run `go mod tidy`.

---

## Low

### L-01 — Tests with false confidence
- **Location:** `internal/scenario/scenario_test.go:38-47` (lost_update "test" only logs),
  `:77-85` (deadlock only logs), `api/smoke_test.go:180-218` (asserts name + non-nil steps only),
  `internal/txn/gap_lock_test.go:60-63` (passes whether insert succeeds or times out),
  `internal/txn/ssi_test.go:108-138` (acknowledges flakiness in comment)
- **Fix:** Replace with content-level assertions; remove `t.Logf`-only "tests".

### L-02 — No error-path tests in API smoke suite
- **Location:** `api/smoke_test.go`
- **Fix:** Add cases for: malformed JSON, non-numeric txn ID, unknown table, GET on POST
  route, oversized body.

### L-03 — No fuzz targets for input parsing
- **Location:** `api/helpers.go` (`isolationFromString`, `protocolFromString`, `parseTxnID`),
  `internal/types/value.go` (`Compare`)
- **Fix:** Add `func FuzzIsolationFromString` / `FuzzValueCompare` etc.

### L-04 — `TestLockQueue_RaceCondition` holds `q.mu` for 1ms under lock
- **Location:** `internal/lock/queue_test.go:106-131`
- **Fix:** Restructure to release the lock during sleep.

### L-05 — No benchmark regression thresholds
- **Location:** `benchmark/tpcb_test.go`
- **Fix:** Gate on `result.TPS > X` for a documented floor.

### L-06 — README claims "All 12 test packages pass with the race detector" — unverifiable
- **Location:** `README.md:132`
- **Fix:** Remove the claim or wire `go test ./... -race` into CI and link the run.

### L-07 — `web/node_modules` and personal home-directory files appear under git
- **Location:** repo root is `/Users/sanskar` (home dir), no project-level `.git`
- **Fix:** `git init` at project root; add `.gitignore` excluding `web/node_modules`,
  `bin/`, build artifacts; never track `node_modules`.

### L-08 — No LICENSE, CONTRIBUTING, ADRs, runbook
- **Fix:** Add `LICENSE` (MIT), `CONTRIBUTING.md`, `docs/adr/`, `docs/RUNBOOK.md`.

### L-09 — `vite.config.ts` hardcodes `localhost:8080`
- **Location:** `web/vite.config.ts:16-21`
- **Fix:** Read `VITE_API_BASE` env with `localhost:8080` default.

### L-10 — `fmt.Fprintf` for SSE ignores write errors
- **Location:** `api/sse.go:84`, `api/handlers_infra.go:141`
- **Fix:** Check `n, err` and abort the connection on write failure.

---

## Dependency audit

`go.mod` requires:
- `github.com/go-chi/chi/v5 v5.2.5` — current, no known CVEs.
- `github.com/stretchr/testify v1.11.1` — current.
- `gopkg.in/yaml.v3 v3.0.1` — current; not used by application code (only by testify
  indirectly for assertion helpers). Candidate for removal via `go mod tidy`.
- `github.com/davecgh/go-spew`, `github.com/pmezard/go-difflib` — testify transitive.

No SAST tool is currently run; CI (Phase 7) will add `govulncheck` and `gosec`.

---

## Summary counts

| Severity | Count |
|---|---|
| Critical | 8 |
| High | 12 |
| Medium | 12 |
| Low | 10 |
| **Total** | **42** |

Phases 2–6 address Critical + High first, then Medium. Low-severity items are
bundled into Phase 6 (code quality) and Phase 5 (tests).
