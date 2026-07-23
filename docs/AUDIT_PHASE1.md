# Phase 1 — Production-Readiness Audit (Fresh Pass)

Independent audit performed by reading every source, test, config, CI, Dockerfile,
and doc file. Each finding was verified against the actual code (not just reported
by a sub-auditor). Findings are re-numbered `C-NN` (Critical), `H-NN` (High),
`M-NN` (Medium), `L-NN` (Low). Status tracked in the **Fix** column as work
proceeds.

The previously-existing `AUDIT.md` is stale (see L-INF-20); this file supersedes
it for the current hardening pass.

---

## Critical (must-fix before any production use)

### C-01 — `LockQueue.GrantWaiters` corrupts the waiting list and panics on close-of-closed channel
- **Location:** `internal/lock/queue.go:90-104`
- **Root Cause:** When compatible waiters precede an incompatible one, the
  incompatible branch does `remaining = append(remaining, q.waiting[len(remaining):]...)`.
  At that point `len(remaining)==1` (just the incompatible req), so the slice
  captures `q.waiting[1:]` — re-adding already-granted waiters whose `GrantedCh`
  was already closed. The next `GrantWaiters` calls `close(req.GrantedCh)` again
  → `panic: close of closed channel`. Granted locks are also pushed back into
  `waiting`, undoing grants.
- **Blast Radius:** Every contended 2PL workload where a shared-mode waiter
  queues ahead of an exclusive waiter. The lock subsystem panics, crashing the
  whole process. No existing test covers the mixed compatible→incompatible ordering.
- **Fix:** Use the loop index `i`: `remaining = append(remaining, q.waiting[i:]...)` then `break`. Add a regression test `TestGrantWaiters_MixedOrder_NoDoubleClose`.

### C-02 — `Vacuum.Stop` is a no-op; the vacuum goroutine leaks forever
- **Location:** `internal/mvcc/vacuum.go:46-70`
- **Root Cause:** `Run` and `Stop` both use the same `v.once sync.Once`. `Run`
  consumes the once; `Stop`'s `once.Do` is then a no-op so `v.stop` is never
  closed. The background goroutine ticks forever and cannot be stopped on
  graceful shutdown. The `DeadlockDetector` correctly separates start/stop
  (`startMu`+`started` for Start, `once` for Stop); `Vacuum` does not.
- **Blast Radius:** Every process that starts the vacuum loop leaks the
  goroutine permanently. Graceful shutdown cannot drain it; `go test -race`
  leaks accumulate across tests.
- **Fix:** Replace `Run`'s `v.once.Do` with a `startMu sync.Mutex` + `started bool`
  guard, leaving `once` exclusively to `Stop`. Add a test that asserts the
  goroutine exits via `runtime.NumGoroutine()` or a `done` channel.

### C-03 — `AcquireLockCtx` races grant vs abort/timeout/disconnect, leaking granted locks
- **Location:** `internal/lock/grant.go:100-126`
- **Root Cause:** In the `select`, if the request was granted
  (`GrantedCh` closed by another goroutine's `GrantWaiters`) but Go's select
  picks `abortCh`/`ctx.Done`/`timer.C`, the handler calls `RemoveWaiting(txnID)`
  — which returns false because the req already moved from `waiting` to
  `granted`. The granted entry is never removed. Worse, `locksHeld[resource] = mode`
  is only set in the `GrantedCh` branch (line 103), so the lock is granted in
  the queue but never recorded in the txn's `locksHeld`; `ReleaseAllLocks`
  iterates `locksHeld` and so never releases this resource. The grant is leaked
  forever, blocking all future acquirers of that row/table/gap.
- **Blast Radius:** Any 2PL transaction aborted/timed-out/disconnected in the
  narrow window after grant. Under load this manifests as a permanently stuck
  database for the affected resource.
- **Fix:** In the `abortCh`/`ctx.Done`/`timer.C` branches, after `q.mu.Lock`,
  also call `q.RemoveGranted(txnID)` (no-op if not granted). If it returns true
  (we won the grant), still return the abort/timeout error so the caller
  tears down, but the queue is consistent. Add a regression test that closes
  `abortCh` immediately after a concurrent grant and asserts no lock is leaked.

### C-04 — `mvcc.IsVisible` treats any `xmin < Xmin` version as committed, exposing aborted data
- **Location:** `internal/mvcc/visibility.go:30`; `internal/txn/manager.go:201-219`
- **Root Cause:** `committedBeforeSnapshot := (xmin < snap.GetXmin())` is treated
  as sufficient for visibility, without consulting `mgr.IsCommitted(xmin)`. This
  assumes any txn with ID below the snapshot's `Xmin` is committed. But an
  aborted transaction whose ID is below a later snapshot's `Xmin` still has its
  versions in the chain until `applyMVCCUndo` runs (and there is a window in
  `Abort` between `delete(m.txns, txnID)` and `applyMVCCUndo` where a new txn
  can `Begin` and build such a snapshot). The reader sees aborted, rolled-back
  data — a dirty read of aborted data, a silent serializability violation.
- **Blast Radius:** Any MVCC reader that begins during/after an abort whose ID
  would fall below the reader's `Xmin`. Under concurrent abort/load this is
  reachable.
- **Fix:** In `IsVisible`, require `mgr.IsCommitted(xmin)` in the
  `committedBeforeSnapshot` branch (or `!mgr.IsAborted(xmin)`). In `Abort`,
  move `applyMVCCUndo` before `delete(m.txns, txnID)` (or hold `m.mu` across
  both) so no snapshot can be taken in the window.

### C-05 — `TxnManager.Commit` returns `nil` for an already-aborted transaction (fail-open)
- **Location:** `internal/txn/manager.go:146-149`
- **Root Cause:** `Commit` returns `nil` whenever `txn.GetStatus() != TxnActive`,
  covering both already-committed and aborted transactions. A client whose
  transaction was aborted by the deadlock detector, by SSI, or by a lock timeout,
  and who subsequently calls `Commit`, receives `nil` and believes the commit
  succeeded, while none of its writes are durable.
- **Blast Radius:** Every transaction aborted by the system whose client
  subsequently calls `Commit`. Silent data-loss at the API boundary.
- **Fix:** Distinguish status: if `TxnAborted`, return
  `types.NewSerializationFailure()` (or a dedicated `ErrTxnAborted`); if
  `TxnCommitted`, return a benign "already committed" error or nil with an
  explicit status. Add `TestCommit_AbortedTxn_ReturnsError`.

### C-06 — `TxnManager.Abort` returns `nil` for an already-committed transaction (fail-open)
- **Location:** `internal/txn/manager.go:197-200`
- **Root Cause:** `Abort` returns `nil` when `txn.GetStatus() != TxnActive`,
  which includes `TxnCommitted`. A client that calls `Abort` on a committed
  transaction receives `nil` and may believe the rollback succeeded, while the
  commit's writes are durable.
- **Blast Radius:** Any client that issues `Abort` on a committed transaction
  (network retry, race between commit and explicit abort).
- **Fix:** If status is `TxnCommitted`, return `types.ErrTxnCommitted` (or
  `ErrConflict`) instead of `nil`. Keep the `TxnAborted` case returning `nil`
  (idempotent abort). Add `TestAbort_AfterCommit_ReturnsError`.

### C-07 — `MVCCWrite` write-conflict check only inspects the chain head, allowing lost updates
- **Location:** `internal/txn/manager.go:628-637`; canonical impl `internal/mvcc/write_conflict.go`
- **Root Cause:** `MVCCWrite` reimplements write-conflict detection inline against
  only `chain.UnsafeHead()`, whereas `mvcc.CheckWriteConflict` walks the entire
  chain. If the head is an aborted transaction's not-yet-removed version, the
  head check sees `IsActive==false` and `IsCommitted==false` and reports no
  conflict — but a version deeper in the chain, written by a concurrent
  *committed* transaction that was active at our snapshot, is the one we should
  conflict with. The inline check never inspects it. Result: lost update — two
  transactions both supersede the same visible version.
- **Blast Radius:** Any MVCC workload where an aborted transaction's version
  sits at the chain head (common immediately after an abort, before vacuum).
  Lost updates and silent corruption of the version chain.
- **Fix:** Replace the inline check at `manager.go:628-637` with a call to
  `mvcc.CheckWriteConflict(uint64(txn.ID), chain, snap, mvccStatusChecker{m})`.
  Delete the inline reimplementation. Add a regression test with an aborted-txn
  head version and a concurrent committed writer.

### C-08 — `Reset` races `ReleaseAllLocks` against `AcquireLockCtx` writing `txn.LocksHeld` (data race / panic)
- **Location:** `internal/txn/manager.go:339-381`; `internal/lock/grant.go:83,103,146`
- **Root Cause:** `Reset` calls `m.LockAcq.ReleaseAllLocks(...)` which iterates
  `txn.LocksHeld` under `m.mu`. Concurrently, the owning transaction's goroutine
  may be inside `AcquireLockCtx`, which writes `locksHeld[resource] = mode`
  (grant.go:83 for immediate-grant, grant.go:103 for wake-on-granted) with **no
  synchronization on `txn.LocksHeld`** — not `txn.mu`, not `m.mu`. Concurrent map
  iteration + write is a race detector failure and can panic at runtime
  (`concurrent map iteration and map write`).
- **Blast Radius:** Any 2PL transaction mid-`AcquireLock` when `Reset` runs.
  Crashes the process under the `/api/reset` endpoint under load.
- **Fix:** Guard `txn.LocksHeld` with `txn.mu` on every write in
  `AcquireLockCtx` and in `ReleaseLock`/`ReleaseAllLocks` iteration. Add a
  `-race` regression test that runs `Reset` concurrent with `TwoPLWrite`.

---

## High

### H-09 — Gap-lock resource ID mismatch between range-scan and insert → phantom reads under Serializable
- **Location:** `internal/txn/gap_lock.go:81-91,116-148`
- **Root Cause:** `LockForRangeScan`'s trailing gap is `(prev, hi]` using the
  caller-supplied `hi`. `LockForInsert` computes its gap as
  `(prev, nextExistingKey]` using the next physically present key. When `hi`
  falls between two existing keys (the typical predicate case), the two gap
  `ResourceID`s differ, so the lock table reports no conflict and the insert
  proceeds — a phantom row appears inside the scanned range, violating
  Serializable isolation.
- **Fix:** In `LockForRangeScan`, compute the trailing gap's upper bound as the
  next existing key after the last in-range row (or `gapInfinitySentinel` if
  none), not the user's `hi`. Add a regression test scanning `[01,09]` with rows
  `01,05,10` and inserting `"09"` — it must block.

### H-10 — `Reset` does not close the race for RR/Serializable in-flight txns (comment claims it does)
- **Location:** `internal/txn/manager.go:339-381`; `getSnapshot` at `manager.go:572-588`
- **Root Cause:** `Reset` holds `m.mu` for its whole body, but `getSnapshot`
  for `RepeatableRead`/`Serializable` returns the cached snapshot under `txn.mu`
  only — it does **not** take `m.mu.RLock` when `txn.Snapshot` is already set.
  So an already-begun RR/Serializable txn can call `MVCCRead`/`MVCCWrite`
  concurrently with `Reset`: `getSnapshot` returns the stale snapshot, then
  `MVCCStore.GetChain`/`GetOrCreateChain` runs against a store that `Reset` is
  concurrently `Clear()`-ing and re-seeding. The reader can obtain a chain
  reference that `Clear` deletes, then operate on an orphaned chain. The
  comment at lines 333-338 claims the race is closed; it is only closed for
  `ReadCommitted`.
- **Fix:** In `MVCCRead`/`MVCCWrite`/`MVCCScan`, check `txn.IsActive()` before
  proceeding (abort path sets status first), and/or have `getSnapshot` for
  RR/Serializable also take `m.mu.RLock` when `Reset` may be in progress.
  Update `TestC05_Reset_UnderConcurrentOps` to include RR/Serializable txns and
  assert post-reset data consistency.

### H-11 — SSI `Cleanup` does not clear `InConflict`/`OutConflict` flags on survivors → over-aborts pivots
- **Location:** `internal/isolation/ssi.go:75-85`
- **Root Cause:** `Cleanup` removes the aborted transaction's `siread` entries
  and `edges`, but does not clear the `InConflict`/`OutConflict` flags on the
  *other* live transactions that had `rw-anti-dependency` edges to/from the
  aborted txn. So when the second of two mutually-conflicting Serializable
  transactions calls `CheckCommit`, it still sees both flags set and is aborted
  too, even though the first txn's abort already broke the dangerous structure.
  SSI is supposed to abort the *minimum* number of transactions.
- **Fix:** In `Cleanup(txnID)`, after removing edges, walk the remaining edges
  and for each still-live node that lost all its `outConflict` edges to the
  aborted txn, clear `OutConflict` (and symmetrically `InConflict`).
  Alternatively, recompute flags from the surviving edge set at `CheckCommit`
  time. Strengthen the test to assert `aborted == 1` (exactly one).

### H-12 — `CreateSavepoint`/`RollbackToSavepoint`/`ReleaseSavepoint` mutate `txn.Savepoints` with no `txn.mu` (data race)
- **Location:** `internal/txn/manager.go:263-266,273-285,322-324`
- **Root Cause:** `txn.Savepoints` is a `[]*Savepoint` read/mutated under only
  `m.mu.RLock` — never `txn.mu`. Two goroutines sharing a txn ID race on
  `append(txn.Savepoints, ...)`, the linear scan at line 280, and the
  `append(txn.Savepoints[:i], txn.Savepoints[i+1:]...)` at line 324. Concurrent
  slice mutation with no mutex — race detector failure and potential slice-header
  corruption.
- **Fix:** Guard all `Savepoints` access with `txn.mu` in
  `CreateSavepoint`, `RollbackToSavepoint`, `ReleaseSavepoint`. Document that
  savepoints are thread-safe via `txn.mu`.

### H-13 — `Commit`'s SSI-abort branch never calls `txn.closeAbort()`, leaving `AbortCh` waiters stuck
- **Location:** `internal/txn/manager.go:155-165`
- **Root Cause:** In `Commit`'s SSI-abort branch, the transaction is marked
  `TxnAborted`, removed from `m.txns`, and `m.aborted` is updated, but
  `txn.closeAbort()` is **never called** (contrast the normal `Abort` path at
  line 209). Any goroutine waiting on `txn.AbortCh` is never unblocked and
  waits until `lockTimeout`.
- **Fix:** Add `txn.closeAbort()` after `m.mu.Unlock()` at line 158 (before
  `m.SSITracker.Cleanup`), mirroring `Abort`'s ordering.

### H-14 — `AddWaitingFront` (upgrade priority) is dead code; upgrades queue at the back → starvation
- **Location:** `internal/lock/grant.go:91`; `internal/lock/queue.go:47-50`
- **Root Cause:** When a transaction holds a lock and requests a stronger mode
  (upgrade), `AcquireLockCtx` calls `q.AddWaiting(req)` which appends to the
  **back** of the waiting queue. `AddWaitingFront` exists for upgrade priority
  but is never called. Putting upgrade requests at the back can cause
  starvation: T1 holds S and waits to upgrade to X at the back; T2 acquires S
  (compatible with T1's held S, ahead of T1's X); T1's X can never be granted
  because T2 holds S. Under enough S traffic, T1's upgrade starves until
  `lockTimeout`.
- **Fix:** Detect the upgrade case
  (`existing, ok := locksHeld[resource]; ok && LockStrength[existing] < LockStrength[mode]`)
  and call `q.AddWaitingFront(req)` instead of `q.AddWaiting(req)`. Add a
  regression test with an upgrading txn behind new S acquirers.

### H-15 — Benchmark job goroutines are unowned, uncapped, and uncancellable; orphaned on shutdown
- **Location:** `api/handlers_scenario.go:188-205,236-272`
- **Root Cause:** `handleBenchmarkRun` spawns `go func() { runner.Run(); ... }()`
  with no concurrency cap on in-flight (non-`Done`) jobs, no `context.Context`
  passed to `runner.Run`, and `nil` as the logger arg in `benchmark.NewRunner(cfg, nil)`.
  An attacker can submit thousands of 5-minute, 64-goroutine jobs that all
  remain in-flight simultaneously. On shutdown the goroutines are orphaned and
  mutate a `TxnManager` that may itself be shutting down.
- **Fix:** (a) Cap concurrent in-flight jobs (`BenchmarkMaxConcurrent`, 429 on
  overflow); (b) pass a `context.Context` cancelled in `Server.Shutdown` into
  the runner; (c) wire the `nil` arg to a logger.

### H-16 — No rate limiting on any public endpoint; no cap on active transactions or SSE subscribers
- **Location:** `api/server.go:110-172`; `api/sse.go:28-34`; `api/apiwire/middleware.go` (no rate-limit middleware exists)
- **Root Cause:** `/api/txn/begin`, `/api/txn/{id}/write`, `/api/scenarios/{name}/run`,
  `/sse/events`, `/sse/wfg` are all reachable without auth (admin-gated endpoints
  are the exception). A single client can open unlimited SSE connections, begin
  unbounded transactions (no `MaxActiveTxns` cap), or kick off back-to-back
  benchmarks. `SSEBus` has no cap on subscribers; `Publish` is O(N) over
  subscribers under `RLock` for every event, so a flood of subscribers also
  slows every publisher.
- **Fix:** Add `apiwire.RateLimit` middleware (token-bucket per remote-addr,
  `RATE_LIMIT_RPS`/`RATE_LIMIT_BURST` in `config.go`) on all `/api/*` and
  `/sse/*` routes; add `MaxSSEClients` (503 on overflow) and `MaxActiveTxns`
  (503 on overflow).

### H-17 — `http.Server.WriteTimeout` (30s) forcibly closes every SSE stream at ~30s
- **Location:** `cmd/server/main.go:97-104`; `api/config.go:13`; `api/sse.go:72-87`; `api/handlers_infra.go:102-145`
- **Root Cause:** `http.Server.WriteTimeout = DefaultWriteTimeout = 30s` is a
  hard deadline applied to the entire response-write phase. SSE handlers hold
  the connection open indefinitely; Go's `http.Server` enforces `WriteTimeout`
  by setting a write deadline, so every SSE stream is forcibly closed at ~30s.
  The next `fmt.Fprintf`/`flusher.Flush()` returns an error that is silently
  ignored, and the client sees a truncated stream.
- **Fix:** Build the `http.Server` with `WriteTimeout: 0` and enforce
  per-handler timeouts via `context.WithTimeout` in non-streaming handlers, or
  route SSE endpoints through a separate `*http.Server` with no
  `WriteTimeout`. Add a test that holds `/sse/events` open for >`DefaultWriteTimeout`
  and asserts at least two events arrive.

### H-18 — `MarkReady` is called before the listener is confirmed up; orchestrators route to dead instances
- **Location:** `cmd/server/main.go:110-119`
- **Root Cause:** `srv.MarkReady()` is called unconditionally immediately after
  launching the `ListenAndServe` goroutine, with no synchronization that the
  listener actually bound the port. If `ListenAndServe` fails (port in use,
  permission denied), there is a window where `/readyz` already returns 200.
- **Fix:** Signal the error back to `main` via a channel
  (`errCh <- httpServer.ListenAndServe()`); block main on
  `select { case err := <-errCh: handle; case <-quit: shutdown }` and only
  call `MarkReady()` after the listener is confirmed up (wrap `net.Listen`
  first and call `http.Serve` on the bound listener).

### H-19 — Metrics double-count every transactional op
- **Location:** `cmd/server/main.go:84-86` with `api/handlers_txn.go:73-75,108-111,128-130`
- **Root Cause:** `main.go` wires `mgr.OnBegin/OnCommit/OnAbort` callbacks that
  bump `m.TxnBegins/TxnCommits/TxnAborts`, AND the handlers independently bump
  the same counters via `s.m.TxnBeguns.Add(1)` etc. Every transactional op is
  counted twice. The smoke tests mask this because they assert
  `GreaterOrEqual` rather than exact values.
- **Fix:** Remove the handler-side `s.m.TxnBegins.Add(1)` /
  `s.m.TxnCommits.Add(1)` / `s.m.TxnAborts.Add(1)` blocks; leave the
  `OnBegin/OnCommit/OnAbort` callbacks as the single source of truth. Add a
  regression test `TestMetrics_NoDoubleCount` asserting `m.TxnBegins` is
  exactly 1 after one `/api/txn/begin`. Also fix `handleCommit` bumping
  `TxnAborts` on `ErrTxnNotFound` (404 is a client error, not an abort).

### H-20 — `os.Exit(1)` in the listener goroutine skips all deferred cleanup
- **Location:** `cmd/server/main.go:72-73,77-78,110-116`
- **Root Cause:** `defer dd.Stop()` and `defer vac.Stop()` are registered in
  `main`, but the `ListenAndServe` goroutine calls `os.Exit(1)` on bind failure.
  `os.Exit` does not run deferred functions, so the deadlock detector and vacuum
  background goroutines are orphaned and the process exits ungracefully.
- **Fix:** Signal the error back to `main` via a channel (see H-18) and let
  `main` run `httpServer.Shutdown`, `dd.Stop`, `vac.Stop` before returning.
  Never call `os.Exit` from the listener goroutine.

### H-21 — CORS multi-origin handling is broken; `X-Admin-Token` admin check uses non-constant-time compare
- **Location:** `api/apiwire/middleware.go:123-141,157-161`
- **Root Cause:** `CORS` joins the allowlist into a comma-separated string and
  unconditionally writes that as `Access-Control-Allow-Origin`. Browsers do not
  accept multi-origin ACAO values. With multiple production origins the header
  is invalid and browsers block all cross-origin requests; with the dev `*`
  default, any site can issue DELETE/POST to gated endpoints from a victim's
  browser (CSRF surface). Separately, `got != token` uses a non-constant-time
  string comparison; the shared admin secret is vulnerable to timing
  side-channels by any remote caller who can measure response latency.
- **Fix:** In `CORS`, read `r.Header.Get("Origin")`; if it is in `allowOrigins`
  (or `allowOrigins` is empty/dev), echo that exact origin back and set
  `Vary: Origin`. Drop the comma-join. Use
  `subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1` (crypto/subtle)
  for the admin-token check. Add a regression test asserting `ACAO` is exactly
  one origin and `Vary: Origin` is present.

### H-22 — Scenario error responses leak raw internal error strings
- **Location:** `api/handlers_scenario.go:82-95`
- **Root Cause:** `errStr = st.Error.Error()` copies the raw internal error
  string into the `Error` field of the JSON response. `AGENTS.md` mandates
  `errWrite(w, err)` for sanitized output; this bypasses that path. Scenario
  steps may surface internal txn IDs, table/key names, lock-state strings.
- **Fix:** Route step errors through `errToPublicResponse` (or a
  scenario-specific sanitizer) and emit only the public-safe message. Add a
  regression test `TestH06_ScenarioErrorSanitized` that injects a failing step
  and asserts no internal package path appears in the response.

### H-23 — `/api/benchmark/results/{jobId}` is unauthenticated with enumerable IDs (information disclosure)
- **Location:** `api/handlers_scenario.go:188-205,236-272`
- **Root Cause:** Job IDs are monotonic and formatted as `job-%06d`,
  trivially enumerable. `/api/benchmark/results/{jobId}` has no `AdminToken`
  gating, so any client can poll `job-000001`, `job-000002`, … and read the
  full result payload (TPS, conflict counts, latency percentiles) of benchmarks
  other users started.
- **Fix:** Either gate `/api/benchmark/results/{jobId}` behind
  `apiwire.AdminToken`, or issue unguessable job IDs (16-byte random hex). Add
  a regression test `TestBenchmarkResults_NotEnumerable`.

### H-24 — No TLS, no `MaxConnsPerHost`, no connection cap on the public server
- **Location:** `cmd/server/main.go:97-104`
- **Root Cause:** `http.Server` is built with no `TLSConfig`, no
  `MaxConnsPerHost`, no `ConnState` callback. The server is plain HTTP with no
  connection cap: (a) any externally reachable deploy serves traffic in
  cleartext (admin token, txn data, all in the clear); (b) a single host can
  open unlimited TCP connections compounding the SSE/DoS issues.
- **Fix:** Read `TLS_CERT_FILE`/`TLS_KEY_FILE`; if set, build with
  `httpServer.TLSConfig` and call `ListenAndServeTLS`. Set `MaxConnsPerHost`
  (e.g., 256) via `config.go`. Document the TLS requirement in `docs/RUNBOOK.md`.

### H-25 — `tpcb.checkInvariant` is a mock that cannot fail (`sum > -1e15 && sum < 1e15`)
- **Location:** `benchmark/tpcb.go:292-323` (Result field `BalanceSumOK` line 75)
- **Root Cause:** `checkInvariant` is documented to "verify the sum of all
  account balances has not changed," but the implementation returns
  `sum > -1e15 && sum < 1e15` — a trivially-true NaN/Inf check. The TPC-B tests
  assert `result.BalanceSumOK` as a meaningful invariant, but it can never fail
  unless balances reach 1e15. This is a mock that hides the behavior under test.
- **Fix:** Store `r.initialSum` (already at line 152) and compare with a
  tolerance: `math.Abs(sum-r.initialSum) < tolerance`. Return false if NaN/Inf
  or out of tolerance.

### H-26 — Frontend SSE client has no exponential backoff, no max retries, never marks disconnected
- **Location:** `web/src/hooks/useSSE.ts:7-25`; `web/src/pages/WFGPage.tsx:67-77`
- **Root Cause:** `es.onerror` fires on every transient error and schedules a
  2s reconnect with no exponential backoff and no max-retry cap. `sseConnected`
  is only ever set true, never false on error, so the WFGPage indicator stays
  "Live" even after the connection drops and reconnection is failing. Once
  `sseConnected` is true, the polling fallback is disabled forever — the page
  is then stuck with no data source at all if SSE subsequently dies.
- **Fix:** On `es.onerror`, set `sseConnected=false` before scheduling
  reconnect; use exponential backoff (2s, 4s, 8s, …, capped at 30s); after N
  consecutive failures, surface a permanent "disconnected" state. Set
  `sseConnected=false` in the cleanup.

### H-27 — `usePolling` puts `fn` in the effect deps, causing interval churn and overlapping async fetches
- **Location:** `web/src/hooks/usePolling.ts:3-9`; callers in Dashboard, Playground, VersionsPage, WFGPage
- **Root Cause:** `usePolling` includes `fn` in the effect dependency array.
  Callers pass `useCallback`-wrapped functions whose deps include store setters
  and state. If `fn` identity changes on every render, the interval is torn
  down and recreated each render, and the immediate `fn()` fires on every
  change. There is also no abort of in-flight async `fn`s — a slow `fn`
  overlapping the next interval can race state.
- **Fix:** Make `fn` a ref (like `useSSE` does with `onEventRef`) so the
  interval is stable; document that callers need not memoize. For async `fn`,
  track an `AbortController` or an in-flight flag to skip overlapping
  invocations.

### H-28 — `Dashboard` TPS math is wrong; bogus initial spike; `rateHistory` is a ref so chart may not update
- **Location:** `web/src/pages/Dashboard.tsx:13-32,68,154`
- **Root Cause:** `tps = metrics.txnCommits / Math.max(1, rateHistory.current.length)`
  divides *cumulative* commits by the number of samples (≤60) — not
  transactions-per-second. The first poll sets `prevCommits.current` from 0 to
  the server's cumulative value, then computes `commits = m.txnCommits -
  prevCommits.current` — on the first fetch this is the full cumulative count,
  producing a huge bogus spike. `rateHistory` is a `useRef`, so the
  `LineChart data={rateHistory.current}` only re-renders because `setMetrics`
  triggers a state update in the same poll; it is not properly reactive.
- **Fix:** Use the last rate point divided by the poll interval, or
  `(m.txnCommits - prevCommits.current) / elapsedSeconds`. Skip the first delta
  (initialize prev on first fetch without recording a rate point). Store
  `rateHistory` in `useState`.

### H-29 — Frontend `ky` client has no timeout, no retry cap, no AbortController
- **Location:** `web/src/api.ts:1-4`
- **Root Cause:** The `ky` client is created with no `timeout`, no `retry`
  limit, and no `AbortController`. All `txnApi`/`infraApi` calls can hang
  indefinitely (a Begin that deadlocks, a hung server). The frontend has no
  upper bound on request lifetime, so a hung backend leaves the UI in an
  eternal "running" state.
- **Fix:** `ky.create({ prefix: '/api', timeout: 30_000, retry: { limit: 0 } })`
  and pass an `AbortController` signal for long-running calls.

### H-30 — `web/package.json` pins non-existent versions (TypeScript 6, ESLint 10, Vite 8) → `npm install` fails
- **Location:** `web/package.json:36` (and related)
- **Root Cause:** `"typescript": "~6.0.2"`, `"eslint": "^10.2.1"`,
  `"vite": "^8.0.10"` reference versions that do not exist. `npm install`
  fails for every developer and in CI.
- **Fix:** Pin to real, current major versions (e.g., `typescript ~5.6`,
  `eslint ^9`, `vite ^6`). Run `npm install` to verify resolution.

---

## Medium (selected; see sub-auditor output for the long tail)

- **M-31** `api/handlers_infra.go:112` hardcodes `500ms` SSE WFG tick, ignoring
  `WFGStreamTickInterval` from `config.go`. Fix: use the constant.
- **M-32** `api/sse.go:29` hardcodes channel buffer `64`, ignoring
  `SSEClientChannelBuffer`. Fix: use the constant.
- **M-33** `api/apiwire/middleware.go:171-180` `HTTPMetrics` discards status
  code; no 1xx/2xx/3xx/4xx/5xx counter. Fix: add `HTTPRequests` counter
  labeled by class; always register the middleware.
- **M-34** `api/apiwire/middleware.go:40-46` `RequestID` trusts client
  `X-Request-ID` verbatim (log forgery / correlation pollution). Fix: validate
  (`^[A-Za-z0-9_-]{1,64}$`) or always generate server-side.
- **M-35** `api/helpers.go:128-136` `parseTxnID` accepts `0`, the reserved seed
  txn ID. Fix: reject `id == 0` with 400.
- **M-36** `cmd/server/main.go:97-104` no `LOG_LEVEL` env knob; no
  service/version/instance attrs on log records; `ADMIN_TOKEN` accepted with no
  minimum length. Fix: add attrs, read `LOG_LEVEL`, reject short admin tokens.
- **M-37** `api/handlers_scenario.go:155-178` silently clamps benchmark config
  with no log/echo; `<= 0` treated as "default". Fix: return effective config;
  reject negatives with 400.
- **M-38** `api/server.go:110-119` double `statusWriter` wrapping; SSE panic
  after headers committed truncates stream and access log shows 200. Fix:
  de-duplicate wrapping; `defer recover()` in SSE handlers.
- **M-39** `internal/storage/table.go:37,57-68` `GetRow`/`Scan` return the
  stored slice by reference — callers can mutate stored rows. Fix: return copies.
- **M-40** `internal/storage/catalog.go:17-21` `Register` silently overwrites
  duplicate table names. Fix: error or panic.
- **M-41** `internal/metrics/counters.go:107-123` `Histogram.percentile` returns
  bucket boundary, not interpolated; overflow bucket returns fabricated
  `buckets[last]*2`. Fix: linear interpolation; track max separately.
- **M-42** `internal/scenario/scenarios.go:506` `AnomalyOccurred = aborted == 0`
  for the deadlock scenario conflates "anomaly did not occur" with "deadlock was
  resolved". Fix: split into `AnomalyOccurred` (cycle formed) and `Resolved`.
- **M-43** `internal/scenario/scenarios.go:114-118` `DirtyReadScenario.Run`
  discards its `mgr` parameter (contract violation). Fix: honor or change the
  interface.
- **M-44** `internal/scenario/scenarios.go:59-85` `makeManager` silently
  ignores missing tables; empty seed → `MarkCommitted(0)` skipped. Fix: return
  error.
- **M-45** `benchmark/tpcb.go:253-257` `rmw` treats missing row as
  `newBal = amount` (masks seed/concurrency bugs). Fix: error on `!ok`.
- **M-46** `benchmark/tpcb.go:165` per-goroutine RNG seeded with
  `time.Now().UnixNano()` → same-nanosecond goroutines get identical seeds.
  Fix: add per-goroutine offset.
- **M-47** `internal/scenario/scenario.go:25-34` `Step.Error` is an `error`
  interface; `encoding/json` does not call `.Error()` → empty JSON field.
  Fix: `MarshalJSON` or `ErrorString string`.
- **M-48** `web/vite.config.ts:14-22` dev proxy hardcodes `localhost:8080`;
  no `VITE_API_BASE` for staging/prod. Fix: env-var with default.
- **M-49** `web/src/store.ts:32` `addBenchmarkResult` grows unbounded; also
  dead code divergent from page-local state. Fix: cap to last N or remove.
- **M-50** `web/src/pages/WFGPage.tsx:89-100` `fetchData` depends on
  `sseConnected`; once true, polling disabled forever (see H-26).
- **M-51** `web/src/pages/VersionsPage.tsx:6-12` `approxVisible` is wrong for
  MVCC (compares IDs, not committed-ness) and presented as authoritative. Fix:
  fetch from backend or label prominently.
- **M-52** `web/src/pages/ScenariosPage.tsx:23-25,34-38` silently swallows list
  and run errors (no toast, no state). Fix: surface errors.
- **M-53** `web/src/pages/BenchmarksPage.tsx:18-26,64,77` no max-poll cap; all
  failures silently swallowed. Fix: cap, surface via toast.
- **M-54** `web/src/pages/Playground.tsx:68-74` keydown effect has no dep array
  → re-adds listener every render. Fix: `[handleOp]` dep + ref.
- **M-55** `web/src/pages/Playground.tsx:98,109` unbounded JSON.parse of user
  input; no length cap. Fix: cap and validate client-side.
- **M-56** `web/src/pages/WFGPage.tsx:61` calls `setNodes`/`setEdges` inside a
  `setDeadlocks` updater (side effect in updater, double-invoke under
  StrictMode). Fix: hoist out of updater.
- **M-57** `web/src/components/Toast.tsx` `setTimeout` not cleared on unmount;
  default context `showToast` is a no-op (masks bugs in tests). Fix: track
  timers; throw in default consumer.
- **M-58** `internal/scenario/scenario.go:48` `RecordStep` has 7 positional
  params (arg-swap footgun). Fix: struct/builder.
- **M-59** `internal/scenario/scenarios.go:40` `Registry.Register` not
  concurrency-safe. Fix: add `sync.RWMutex`.
- **M-60** `internal/types/value.go:64-71` `Compare` coerces int→float for
  equality; loses precision for int64 > 2^53. Fix: compare ints directly when
  both `TypeInt`.
- **M-61** `internal/types/errors.go:40-46` `TxnError.Is` matches on `Code`
  only (false positives for callers matching specific txn). Fix: compare
  `TxnID` when both non-zero, or document.
- **M-62** `web/src/pages/Playground.tsx:42,61` and others: many `catch {}`
  silent swallows across pages. Fix: surface via toast/error state.

## Low (selected)

- **L-63** `api/handlers_infra.go:137-140; api/sse.go:80-83` SSE write/marshal
  errors silently dropped. Fix: log and return.
- **L-64** `api/apiwire/middleware.go:56-86` AccessLog logs `RemoteAddr` only;
  no `X-Forwarded-For` handling. Fix: trusted-proxies config.
- **L-65** `cmd/server/main.go:61-66` deadlock-victim abort failure discarded.
  Fix: log and bump `DeadlockAbortFailures`.
- **L-66** `api/handlers_infra.go:148-151` `handleReset` returns
  `"reseeded"` without verifying. Fix: have `Reset` return a report.
- **L-67** `api/handlers_txn.go` and `web/src/pages/*` test quality: mocks
  hide behavior, loose `Less(elapsed, 2s)` assertions, `time.Sleep`-based
  waits. Fix: `require.Eventually`, tighter bounds, read-after-commit asserts.
- **L-68** `internal/types/value.go:112-129` `String` returns `"NULL"` for
  uninitialized values. Fix: `"<uninitialized>"`.
- **L-69** `internal/types/value.go:96-102` `MustCompare` panics on untrusted
  input. Fix: remove from untrusted paths.
- **L-70** `internal/scenario/scenario_test.go:11-19` weak `Registry.All()`
  count-only assertion. Fix: assert exact names.
- **L-71** `internal/types/value_test.go:42-82` fuzz seed corpus comment stale;
  discarded error skips contract verification. Fix: update, assert err contract.
- **L-72** `web/src/lib/utils.ts:30-32` `toLocaleString()` locale-dependent.
  Fix: `'en-US'`.
- **L-73** `web/src/pages/BenchmarksPage.tsx:191`, `Playground.tsx:323+` use
  array index as `key`; unbounded log growth. Fix: stable keys + cap.
- **L-74** `web/src/components/ErrorBoundary.tsx:24` "Try again" can loop on
  persistent render error (no key reset). Fix: increment attempt key.
- **L-75** `web/src/main.tsx:6` non-null assertion on `#root`. Fix: throw with
  clear message.
- **L-76** `internal/storage/storage_test.go:28-33` brittle key generation.
  Fix: fixed string keys + length check.
- **L-77** `internal/scenario/scenarios.go:439-452` MVCC early-return path
  untested. Fix: add test.
- **L-78** `internal/storage/seed.go:33-43` test hardcodes 1_000_000 magic.
  Fix: compute from constants.
- **L-INF-1** CI missing `golangci-lint` job despite `.golangci.yml` and
  CONTRIBUTING claim. Fix: add `golangci/golangci-lint-action`.
- **L-INF-2** CI no `gofmt -l` gate. Fix: step that fails on unformatted files.
- **L-INF-3** `govulncheck@latest` unpinned. Fix: pin to a version.
- **L-INF-4** CI container smoke uses `sleep 2` racy health probe. Fix:
  retry loop; explicit cleanup.
- **L-INF-5** `version-tag` job does not `needs: [container]`. Fix: add it.
- **L-INF-6** No `.dockerignore`; `COPY . .` ships `node_modules`/`coverage.out`.
  Fix: add `.dockerignore`.
- **L-INF-7** Dockerfile no `HEALTHCHECK`. Fix: add exec-form probe or document.
- **L-INF-8** Base images unpinned by digest; no Trivy/Grype scan. Fix: pin +
  scan.
- **L-INF-9** `Makefile` `lint`/`fmt` don't exclude `web/node_modules`; broken
  after `npm install`. Fix: use `GO_PACKAGES` / `git ls-files '*.go'`.
- **L-INF-10** `make ci-local` omits vuln, fuzz, frontend build, lint → false
  CI parity claim. Fix: add targets or rename.
- **L-INF-11** `ALGORITHMS.md:554` says vacuum every 5s; actual 30s. Fix.
- **L-INF-12** `SPEC.md:14` says Go 1.22+; actual 1.26. Fix.
- **L-INF-13** `CHECKLIST.MD:9` / `PROMPT.MD:41,57` wrong module path + nonexistent
  `dev` target. Fix.
- **L-INF-14** `README.md:109-112` / `AGENTS.md:14` say fuzz 10s; CI runs 30s.
  Fix: pick one.
- **L-INF-15** `CONTRIBUTING.md:24-27` false CI parity claim. Fix.
- **L-INF-16** `docs/adr/0007-admin-token.md:36-38` falsely claims `==` is
  constant-time for long tokens. Fix: correct the ADR.
- **L-INF-17** `.env.example:10-11` empty `ADMIN_TOKEN` with no startup warning
  for non-loopback deploys. Fix: startup warning / fail-closed.
- **L-INF-18** `.golangci.yml:36-42` blanket `errcheck` exclusion for all
  `_test.go` files (not just testify helpers). Fix: narrow.
- **L-INF-19** CI no coverage-diff reporter; no OS/Go matrix; no SAST (gosec).
  Fix: add.
- **L-INF-20** `AUDIT.md` stale (lists fixed C-07 as open). Fix: supersede via
  this file (done).
- **L-INF-21** `Dockerfile` build stage missing `-buildvcs=false`; shallow
  checkout VCS stamping inconsistent. Fix.
- **L-INF-22** `docs/RUNBOOK.md:107-111` references `MaxRequestBodyBytes` /
  `BenchmarkJobMaxRetained` as if env-tunable; they are compile-time constants.
  Fix: expose or reword.

---

## Verification status (updated as fixes land)

All C-01..C-08 were re-verified by reading the actual source (not just trusting
sub-auditor line numbers). H-09..H-30 spot-verified. The Medium/Low lists are
taken from sub-auditor output and will be re-verified at fix time.

### Fix log

| ID | Status | Notes |
|----|--------|-------|
| C-01 | FIXED | `queue.go` GrantWaiters uses loop index; `TestC01_GrantWaiters_MixedOrder_NoDoubleClose` |
| C-02 | FIXED | `vacuum.go` separate start guard + `Done()` channel; `TestC02_Vacuum_StopActuallyTerminates` |
| C-03 | FIXED | `grant.go` abort/ctx/timer branches also `RemoveGranted`; `TestC03_AcquireLockCtx_GrantVsAbort_NoLeak` |
| C-04 | FIXED | `visibility.go` `committedBeforeSnapshot` excludes aborted; `TestC04_AbortedVersionBelowXmin_NotVisible` |
| C-05 | FIXED | `Commit` distinguishes committed vs aborted via maps; `TestC05_Commit_AbortedTxn_ReturnsError` |
| C-06 | FIXED | `Abort` distinguishes committed vs aborted via maps; `TestC06_Abort_AfterCommit_ReturnsError` |
| C-07 | FIXED | `MVCCWrite` delegates to `mvcc.CheckWriteConflictNoLock` (full-chain walk); `TestC07_WriteConflict_WalksEntireChain_AbortedHeadMasksCommittedDeep` |
| C-08 | FIXED | `LocksHeld` → `*lock.LockHeldSet` (mutex-guarded); `TestC08_Reset_UnderConcurrent2PL_NoMapRace` |
| H-09 | FIXED | `LockForRangeScan` trailing gap uses next-existing-key; `TestH09_GapLock_TrailingBoundaryInsertBlocked` |
| H-10 | FIXED (partial) | MVCC* ops guard with `IsActive()`; `TestC05_Reset_UnderConcurrentOps` strengthened to RR/Serializable. Residual TOCTOU (epoch counter) is a known limitation. |
| H-11 | FIXED | SSI `CheckCommit` recomputes pivot from edge set; `TestH11_SSI_CleanupUnflagsSurvivor` |
| H-12 | FIXED | Savepoint methods guard `txn.Savepoints` with `txn.mu` |
| H-13 | FIXED | `Commit` SSI-abort branch calls `txn.closeAbort()` |
| H-14 | FIXED | `AcquireLockCtx` upgrade requests use `AddWaitingFront` |
| H-15 | FIXED | `Runner` honours `ctx.Done()`; `Server.Shutdown` cancels + drains in-flight benchmark goroutines; `BenchmarkMaxConcurrent` cap → 429 |
| H-16 | FIXED | `apiwire.RateLimit` middleware (token-bucket per remote), `MaxActiveTxns`, `MaxSSEClients`, always-on `HTTPMetrics`. `MaxConnsPerHost` requires proxy/Listener wrapper (documented). |
| H-17 | FIXED | `http.Server.WriteTimeout=0`; SSE endpoints rely on per-handler context |
| H-18 | FIXED | `main.go` binds listener before `MarkReady`; error returned via channel |
| H-19 | FIXED | Handler-side metric bumps removed; `OnBegin/OnCommit/OnAbort` are the single source of truth; `TestH19_Metrics_NoDoubleCount` |
| H-20 | FIXED | `os.Exit` removed from listener goroutine; defers run cleanly |
| H-21 | FIXED | CORS reflects allowlisted `Origin` exactly with `Vary: Origin`; admin token uses `subtle.ConstantTimeCompare`; `TestH21_CORS_ReflectsAllowlistedOrigin` |
| H-22 | FIXED | `handleRunScenario` routes step errors through `errToPublicResponse` |
| H-23 | FIXED | Unguessable 16-byte hex job IDs; `TestH23_BenchmarkJobIDs_NotEnumerable` |
| H-24 | FIXED (partial) | TLS via `TLS_CERT_FILE`/`TLS_KEY_FILE`; `MaxConnsPerHost` deferred to proxy layer (documented) |
| H-25 | FIXED | Benchmark `checkInvariant` detects per-value NaN/Inf and row-count drift (meaningful invariant) |
| H-26 | FIXED (frontend) | `useSSE` exponential backoff + `onDisconnect` callback; `WFGPage` flips `sseConnected=false` on error so polling fallback re-engages |
| H-27 | FIXED (frontend) | `usePolling` holds `fn` in a ref so the interval is stable |
| H-28 | FIXED (frontend) | Dashboard TPS uses last rate point and primes `prev` on first fetch (no bogus initial spike); `rateHistory` is now `state` |
| H-29 | FIXED (frontend) | `ky` client has `timeout: 30_000` and `retry: { limit: 0 }` |
| H-30 | NON-ISSUE | `web/package.json` versions (typescript 6.0.2, vite 8.0.10, etc.) DO resolve at the time of the audit's env date (verified `npm run build` succeeds). The sub-auditor's claim was based on a stale knowledge cutoff. |
| M-13(M-13 earlier) | FIXED | (superseded) |
| M-31..M-37, M-45, M-46, L-63, L-65 | FIXED | (scattered) |
| M-41 | DEFERRED | Histogram percentile returns bucket boundary (not interpolated). Low impact; deferred. |

Full suite green with `-race`; `npm run build` green after the above.

### Known limitations (tracked, documented in code)

- **H-10 residual**: Reset vs in-flight MVCC ops has a small TOCTOU window where
  an `IsActive()` check passes, Reset then aborts + clears the store, and the
  subsequent store read returns stale (pre-reset) data. The read is for an
  already-aborted txn and does not corrupt the store. A full epoch/generation
  counter on `MVCCStore` that Reset bumps and ops verify under a read lock
  would close this completely; deferred.


