# Phase 2 — Production Audit: Second Pass (Post-Hardening)

This file supersedes `AUDIT_PHASE1.md` for the second-pass audit and
end-to-end testing pass. It documents new defects found after the
initial hardening, the two deep sub-agent reviews, and 30+ real curl
exercises against a live server.

## End-to-end verification (live server, port 18080)

| Check | Result |
|---|---|
| `go build` | clean |
| `go vet` (excluding web/node_modules) | clean |
| `gofmt -l` | clean |
| `go test ./... -race -count=1` | all green |
| `govulncheck` | no vulnerabilities found |
| Coverage | 70.4% (above 65% baseline) |
| `golangci-lint` (v2.12.2) | 299 issues, real ones fixed; backlog is doc-comments + test-helper errcheck |
| Frontend `npx tsc --noEmit` | clean |
| Frontend `npm run build` | clean |

## Defects found in this pass

### Critical

- **CT-19** `api/apiwire/middleware.go:104` — `statusWriter` embedded only
  the `http.ResponseWriter` interface, so additional methods (`Flush`,
  `Hijack`, `CloseNotify`) on the concrete conn were lost. SSE handlers
  returned HTTP 500 "streaming unsupported" on every real request. **This
  is a production-breaking bug that has been live since the
  `statusWriter` wrapper was introduced.** Fix: forward `Flush`, `Hijack`,
  `CloseNotify` explicitly. Verified live: `/sse/events` now streams
  6 events across 2 begin/abort/commit pairs. Regression guard:
  `TestCT19_StatusWriter_ForwardsFlush` (unit + interface-conformance).

- **CT-12 / CT-13** `api/handlers_ops.go:handleWrite/handleInsert` — the
  write/insert API accepted any-length value slice and any-type values,
  silently storing a 1-element row in a 4-column table (or a Text value
  in an Int column). Real e2e: a curl with `"values": [{"type":"float",...}]`
  returned `"ok": true` and the row was now structurally 1-column instead
  of 4. Real data corruption. Fix: `validateRowShape(tbl, values)` checks
  `len(values) == len(tbl.Columns)` and `values[i].Type == tbl.Columns[i].Type`
  before any storage mutation; returns 400 on mismatch. Regression guard:
  `TestCT12_CT13_RowShapeValidation`.

### High

- **CT-01** `cmd/server/main.go` — TLS activation required both
  `TLS_CERT_FILE != ""` AND `TLS_KEY_FILE != ""`. If only one was set
  the server silently fell back to plaintext HTTP, leaking admin token
  + txn data in cleartext. **Real e2e: setting only `TLS_CERT_FILE`
  produced a log line `TLS configuration invalid` and the server
  exited**. Fix: extracted `validateTLSConfig(certFile, keyFile string) error`
  with the "both-or-neither" rule. Regression guard: `TestCT01_ValidateTLSConfig`
  (table-driven). Also fixed TLS handshake verified with self-signed cert.

- **CT-02** `internal/mvcc/vacuum.go` — `PruneHistory()` was defined on
  `TxnManager` but never called. The committed/aborted history maps
  grew for the lifetime of the process (~28 GB/day at 10k
  commits/sec). Real production impact. Fix: vacuum tick now calls
  `PruneHistory()`; `PruneHistory` also caps each map to
  `MaxHistoryRetained` (65 536) as a hard memory bound. Regression
  guard: `TestCT02_Vacuum_InvokesPruneHistory` (counter), `TestCT02_PruneHistory_BoundsMaps`.

- **CT-03** `api/handlers_scenario.go:evictFinishedBenchmarkJobs` — eviction
  only ran on new `/api/benchmark/run` submissions. A process that
  stopped accepting new benchmarks accumulated `Done=true` jobs
  forever. Fix: a background goroutine `evictFinishedBenchmarkJobsLoop`
  ticks every 30s; `Server.Shutdown` also performs a final eviction.

- **CT-11** `internal/scenario/scenarios.go:506` — the deadlock
  scenario's `AnomalyOccurred = aborted == 0` was inverted: a deadlock
  cycle that was successfully resolved (a victim aborted) reported
  `anomalyOccurred=false` (because `aborted > 0`). Real e2e: the
  `/api/scenarios/deadlock_cycle/run` response read
  `anomalyOccurred: false` with `explanation: "Deadlock detected and
  resolved..."`. Fix: split into `AnomalyOccurred` (true when a
  cycle forms) and `Resolved` (true when a victim was chosen). Regression
  guard: `TestDeadlockCycle_AnomalyAndResolved`.

### Medium

- **CT-08** `api/apiwire/middleware.go:AccessLog` — the docstring said
  "4xx → INFO" while the code was "4xx → WARN". The `level := slog.LevelInfo`
  initial assignment was then overwritten in every branch of the switch
  (ineffassign). Fix: docstring corrected; `var level slog.Level`
  declared inside the switch and assigned per branch (no default; all
  paths assign).

- **CT-10** `cmd/server/main.go:OnAbort` — `SSIAborts` was only bumped
  inside `handleCommit` (the explicit API path). System-initiated
  SSI aborts (benchmark/scenario/SSI pivot) never reached the metric.
  Fix: classify in the `OnAbort` callback (single source of truth)
  using the error code.

- **CT-17 (new)** `cmd/server/main.go:OnAbort` — `LockTimeouts` is
  classified in the callback, but lock-timeout is NOT a txn abort
  (the txn stays active, the operation just returns an error). So
  `LockTimeouts` never increments. **Documented limitation**; the metric
  was misleading. Fix deferred: add an `OnLockTimeout` callback to
  TxnManager that the lock-acquirer can invoke.

- **CT-18 (new)** Deadlock scenarios do not update the main server's
  `/api/deadlocks` endpoint because the scenario uses its own
  `makeManager` and does not wire up the main `DeadlockDetector`.
  Real e2e: `/api/deadlocks` is always empty after running a
  scenario, even when the scenario successfully detected a deadlock.
  Documented limitation: the scenario is a teaching witness of
  protocol behavior, not a live deadlock trace.

- **CT-04** `api/server.go:Server.Shutdown` — drains the benchmark
  job goroutines via `jobCtx` + `jobWg`, but does NOT track SSE
  handler goroutines. They exit only when `r.Context().Done()` fires
  after `httpServer.Shutdown`'s 10s timeout. Documented limitation:
  SSE handlers are bounded by `httpServer.Shutdown` but not
  individually tracked.

- **CT-05** `internal/deadlock/detector.go` — `DeadlockDetector` has
  no `Done()` channel (vacuum does). Inconsistent. Deferred.

### Low

- **CT-20** Real e2e: `MaxSSEClients` cap test inconclusive (the 0.5s
  `--max-time` aborted all 260 concurrent requests before reading
  responses). The cap is in code at `api/sse.go:34-37`; verification
  is left for a longer-running test.

- **CT-21 (new)** Real e2e: deadlock scenario always reports
  `anomalyOccurred: false` (superseded by CT-11 fix).

## End-to-end checks performed

- `go build`, `go vet`, `gofmt`, `go mod tidy`, `govulncheck` (no CVEs)
- 50 healthz hits in a tight loop: all 200 (rate limit off in dev)
- TLS handshake with self-signed cert
- TLS incomplete config: server logs error and exits (CT-01)
- Admin token: short token refuses to start; correct token grants 200
  on `/api/reset`; missing token returns 401
- Body size cap: 11.5 MB JSON → 413 Payload Too Large
- `parseTxnID` rejects 0: 400 (M-35)
- 2PL lock timeout: 408 with sanitized message, `lockTimeouts` metric
  increments (CT-17)
- Savepoint flow: write → savepoint → write → rollback-to → read back
  showed the second write was correctly rolled back (key 4 reverted)
- Benchmark cap: 5th concurrent benchmark request returned 429 with
  "too many concurrent benchmark jobs; retry later" (H-15)
- Real benchmark: TPS 12.47, 25 committed, 2 aborted, balanceSumOK=true
- SSI pivot fix (H-11) live: write-skew scenario now correctly aborts
  exactly one pivot; survivor commits
- Deadlock scenario after CT-11: `anomalyOccurred: true, resolved: true`
- /api/deadlocks after running the deadlock scenario now contains the
  scenario-detected record (CT-18)
- Graceful shutdown with SSE stream open + running benchmark: 0-second
  exit, SSE handler exits on sseCtx (CT-22). Before CT-22, shutdown
  waited the full 10s `DefaultShutdownTimeout` because the SSE handler
  ignored sseCtx and only exited on per-request r.Context() firing
  after the HTTP shutdown deadline.
- SSE WriteTimeout=0 verified: 60s stream, 119 ticks (H-17)
- 200 chains + 201 versions visible via `/api/mvcc/stats` after a
  benchmark seeded its 4 tables × 50 rows
- SSE `/sse/events` and `/sse/wfg` both stream correctly (CT-19)

## Third-pass sub-agent audit (CT-23..CT-26, FE-01..FE-05)

- **CT-23** `api/handlers_ops.go:validateRowShape` — was using
  `ErrInvalidIsolation` for row-shape errors. Added
  `ErrInvalidRowShape` (`INVALID_ROW_SHAPE`) and `NewRowShapeError`;
  mapped to "row shape does not match table declaration" in
  `errToPublicResponse`.
- **CT-24** `internal/scenario/scenarios.go:DeadlockCycle` —
  `Resolved = aborted > 0` was too lax (matched when both txns timed
  out without a detector intervening). Now `Resolved = aborted == 1`
  (exactly one victim aborted, the other committed). The scenario
  test was updated to assert `Resolved: false` (this scenario has no
  detector wired; both txns hit the 3s lock timeout).
- **CT-25** `internal/scenario/scenarios.go:DeadlockCycle` —
  `reportDeadlock` cycle was `[t1, t2]` (open). Now `[t1, t2, t1]`
  to match `FindCycle`'s closed-cycle format.
- **CT-26** `.golangci.yml` — narrowed the scenarios.go errcheck
  exclusion with a text filter. Note: golangci-lint v2 does not
  honour v1 path/text filter syntax; we accept the 18 intentional
  errcheck warnings and document the limitation. See comments in
  the file.
- **FE-01** `web/src/pages/WFGPage.tsx:fetchData` — polling fallback
  could overwrite fresher SSE data if SSE reconnected mid-fetch.
  Added a re-check after the await.
- **FE-02** `web/src/store.ts:addBenchmarkResult` — grew unbounded.
  Capped to `MaxClientBenchmarkResults = 50`.
- **FE-03** `web/src/store.ts` — dead `sseConnected`/`setSseConnected`
  fields (WFGPage maintains its own useState). Removed.
- **FE-04** `web/src/pages/Playground.tsx:50` — setTimeout without
  cleanup. Documented as a cosmetic leak (the `logRef.current?.scrollTo`
  no-ops after unmount).
- **FE-05** `web/src/pages/VersionsPage.tsx:46` — setTimeout without
  cleanup. Documented as a cosmetic leak (React 18 suppresses the
  setState warning).

## Known limitations (tracked, documented)

- **CT-04** SSE handler drain on shutdown now happens via sseCtx cancel
  (CT-22 verified live: 0s shutdown with an open SSE stream).
- **CT-05** `DeadlockDetector.Done()` added for parity with
  `Vacuum.Done()`; regression test `TestCT05_DeadlockDetector_Done`.
- **CT-17** `LockTimeouts` metric now increments via the new
  `OnLockTimeout` callback (regression tests: `TestCT17_LockTimeout_FiresCallback`,
  `TestCT17_LockTimeout_DoesNotFireOnAbort`; live verification: real
  lock-timeout produced `lockTimeouts: 1` in `/api/metrics`).
- **CT-18** Deadlock scenario now reports to the main server's
  `/api/deadlocks` via `SetDeadlockReporter` (live verified: the
  record appears with `victimReason: "scenario-detected"`).
- **CT-22** Shutdown ordering fixed: sseCtx cancel before
  `httpServer.Shutdown`, in parallel goroutines, joined via
  `shutdownDone`. Verified live: 0-second exit with open SSE stream
  + running benchmark.
- **H-10** Reset vs in-flight MVCC ops residual TOCTOU.
- **CT-26** golangci-lint v2 scenarios.go exclusion (18 warnings).
