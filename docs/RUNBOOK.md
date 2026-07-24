# Runbook — Transaction Manager

Operational guide for deploying, monitoring, and triaging the
Transaction Manager in production. The service is in-memory; restart
loses state.

## Deploy

1. Build the container image: `docker build -t txn-manager:<tag> .`
2. Run with production hardening:

   ```bash
   docker run --rm -p 8080:8080 \
     -e ADMIN_TOKEN=$(openssl rand -hex 32) \
     -e CORS_ALLOW_ORIGINS=https://app.example.com \
     -e LOG_LEVEL=INFO \
     txn-manager:<tag>
   ```

3. Wait for `/readyz` to return 200 before routing traffic.
4. Smoke-test: `curl -fsS :8080/healthz && curl -fsS :8080/readyz`.

The distroless runtime image has no shell. Use `docker cp` or a
sidecar for ad-hoc inspection.

## Roll back a bad deploy

1. Stop the new pod: `docker stop <container>` (or roll the deployment
   back to the previous image tag in your orchestrator).
2. The previous image tag is the last tag pushed by CI's
   `version-tag` job — `v<YYYYMMDD>-<short-sha>`.
3. Restart the previous image. State is lost on restart (in-memory
   store); clients must re-issue in-flight transactions.

## Top 5 production failure modes

### 1. Memory growth — `committed`/`aborted` maps and MVCC chains

**Symptom:** RSS climbs monotonically under sustained load.

**Diagnosis:**
- `GET /api/mvcc/stats` — `totalChains` and `totalVersions` should
  not grow unbounded over hours.
- `GET /api/metrics` — `vacuumRuns` should increment every 30 s
  (vacuum interval). `versionsPruned` being 0 is NOT necessarily a
  problem on a read-mostly workload — it only goes up when committed
  deletes are below the oldest-active horizon (CT-23).

**Mitigation:**
- Confirm the vacuum background loop is running (the loop is silent
  — it does not log per tick; check `vacuumRuns` increments in
  `/api/metrics` instead of grepping logs).
- `POST /api/mvcc/vacuum` (with `X-Admin-Token`) to trigger a
  synchronous vacuum and observe `pruned` incrementing immediately.
- If still growing, restart the pod (state is lost).

### 2. Sustained 5xx — SSI abort storm

**Symptom:** `txnAborts` climbs; clients see `409 serialization
failure`.

**Diagnosis:**
- `GET /api/metrics` — `ssiAborts` increments faster than
  `txnCommits`.
- Logs: `serialization failure` lines spike.

**Mitigation:**
- The workload is hitting SSI's dangerous structure too often. Move
  some transactions from `serializable` to `repeatable_read` if the
  client tolerates the weaker guarantee.
- Restart to clear SIREAD state if a runaway transaction has
  poisoned the tracker.

### 3. Lock-acquisition timeouts

**Symptom:** Clients see `408 LOCK_TIMEOUT`; `lockTimeouts` metric
climbs.

**Diagnosis:**
- `GET /api/locks` — long `waiting` queues on a few resources
  indicate a hot row.
- `GET /api/wfg` — a long wait-for chain without a cycle indicates
  contention, not deadlock.

**Mitigation:**
- Clients should retry on `408` with backoff.
- Long-held locks indicate a client bug (transaction never commits);
  `POST /api/reset` (admin) clears all state.

### 4. Deadlock victim storm

**Symptom:** `deadlocks` metric climbs; throughput is low.

**Diagnosis:**
- `GET /api/deadlocks` — recent records show the cycle structure.
  Repeated 2-txns cycles on the same resources indicate a client
  access-order bug.

**Mitigation:**
- Clients should acquire locks in a consistent global order to break
  cycles.
- The detector picks the youngest txn as victim (see ADR 0003);
  longer-running txns survive.

### 5. Process OOM / restart loop

**Symptom:** Container OOM-killed; restarts in a loop.

**Diagnosis:**
- Check `MaxRequestBodyBytes` (1 MiB) is enforced — a single
  oversized body should not OOM the process.
- A runaway benchmark (`POST /api/benchmark/run` with high
  `numAccounts`) can allocate millions of rows. CT-24: there are
  TWO caps — `BenchmarkMaxConcurrent = 4` (the real DoS bound on
  in-flight jobs) and `BenchmarkJobMaxRetained = 32` (a cap on
  *finished* jobs awaiting result polling). The in-flight cap is
  what protects CPU and memory; finished-job retention only
  affects poll-result lookup. Verify the in-flight cap is being
  honoured by `GET /api/benchmark/results/<jobId>` during the storm.

**Mitigation:**
- Restart with a memory limit; the distroless image is ~10 MB
  resident at idle. Sustained RSS > 1 GB indicates a leak — file
  an issue with the metrics from `/api/metrics` and the logs.

## On-call escalation

1. Page the on-call if `/healthz` returns non-200 for > 60 s (process
   hung) or `/readyz` flaps for > 2 min (catalog corruption).
2. Attach the request IDs from the failing client reports — they
   appear in the `X-Request-ID` response header and the `req_id` slog
   field on every log line.
3. The CI run for the deployed tag is the source of truth for what
   code is running: `https://github.com/sanskarpan/TransactionManager/actions?query=branch:main`.

## Logging

Logs are JSON-formatted slog lines on stdout. Every HTTP request
produces one line with fields: `req_id`, `method`, `path`, `status`,
`dur_ms`, `remote`. Level mapping:

- 2xx/3xx → INFO
- 4xx → WARN
- 5xx → ERROR

Domain events (deadlock abort, SSI abort) produce additional lines
with `victim` / `cycle` fields. Capture stdout to your log aggregator.
