# Transaction Manager

An in-memory transaction manager implementing two concurrency-control protocols (strict 2PL with intention locks + MVCC with SSI) and exposing them via an HTTP API with a React+TypeScript exploration UI. Designed as a teaching artifact that is nonetheless hardened to production-grade standards: race-free under `-race`, structured-logging, health probes, request-size caps, admin-token gating, and CI.

## Quick start

```bash
# 1. Backend (Go 1.26+)
go run ./cmd/server

# 2. Frontend (separate terminal)
cd web && npm install && npm run dev
```

Open <http://localhost:5173> — the Vite dev server proxies `/api` and `/sse` to `localhost:8080`.

## Architecture

```
cmd/server/           HTTP server entry point, slog wiring, graceful shutdown
api/                  HTTP handlers (chi router), config, public-error sanitizer
api/apiwire/          Reusable middleware: RequestID, AccessLog, MaxBody, CORS, AdminToken
internal/
  lock/               Lock modes, lock table, wait queues, FIFO fairness
  deadlock/           Wait-for graph, DFS cycle detection, victim selection
                      (Wait-Die / Wound-Wait policies defined but not yet wired)
  mvcc/               Version chains, visibility predicate, vacuum (with empty-chain reaping)
  isolation/          SSI tracker (SIREAD locks + rw-anti-dependency graph)
  txn/                TxnManager — orchestrates 2PL and MVCC, savepoints, undo
  storage/            Row storage, catalog, seed data (accounts/products/inventory)
  scenario/           7 anomaly scenarios with step-by-step execution trace
  metrics/            Atomic counters + latency histogram (HTTP middleware in apiwire)
  types/              Value, TxnError, error codes
benchmark/            TPC-B workload, balance invariant verification
web/                  React + TypeScript + Tailwind + Vite frontend
docs/                 Architecture Decision Records (0001-0007) + RUNBOOK.md
AUDIT.md              Phase-1 production-readiness audit (42 prioritized defects)
```

### System map

```
            ┌──────────────────────┐
            │  React UI (web/)     │
            │  Vite + ky + zustand │
            └──────────┬───────────┘
                       │ HTTP + SSE
                       ▼
┌──────────────────────────────────────────────────────────────┐
│  api.Server  (chi router + middleware)                       │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ apiwire: RequestID → AccessLog → MaxBody → CORS → Auth  │ │
│  └─────────────────────────────────────────────────────────┘ │
│  Handlers: txn lifecycle, row ops, savepoints, scenarios,   │
│             benchmark, locks/deadlocks/mvcc, metrics, health│
└──────────────────────┬───────────────────────────────────────┘
                       ▼
┌──────────────────────────────────────────────────────────────┐
│  txn.TxnManager                                              │
│  - txns / committed / aborted maps (mu-protected)           │
│  - LockAcquirer (2PL) → LockTable (sync.Map of LockQueues)  │
│  - MVCCStore (sync.Map of VersionChains)                    │
│  - SSITracker (SIREAD + rw-anti-dependency edges)           │
│  - Undo log + savepoints                                     │
└──────┬───────────────────────────────┬──────────────────────┘
       ▼                               ▼
┌──────────────────────┐    ┌────────────────────────────┐
│ DeadlockDetector      │    │ Vacuum (background loop)   │
│ 50ms DFS over WFG     │    │ 30s prune + empty-chain   │
│ victim = youngest     │    │ reap                      │
└──────────────────────┘    └────────────────────────────┘
```

## Prerequisites

- **Go** 1.26+ (matches `go.mod`)
- **Node.js** 22+ for the frontend
- (Optional) `golangci-lint`, `govulncheck` for local lint/vuln scans

## Configuration reference

All configuration is via environment variables (no config files, no flags).

| Variable | Type | Default | Required | Description |
|---|---|---|---|---|
| `LISTEN_ADDR` | string | `:8080` | no | Bind address for the HTTP server |
| `ADMIN_TOKEN` | string | *(empty)* | production only | If set, destructive endpoints (`/api/reset`, `/api/mvcc/vacuum`, `/api/benchmark/run`) require `X-Admin-Token: <token>`. Compared with `crypto/subtle.ConstantTimeCompare`. Must be ≥ `MinAdminTokenLen` (16) bytes; the server refuses to start with a shorter token. Empty = open (dev only). |
| `CORS_ALLOW_ORIGINS` | CSV | `*` | no | Comma-separated allowed origins. Empty = permissive (`*`). The server reflects the request's `Origin` header back as `Access-Control-Allow-Origin` only when it is in the allowlist, and sets `Vary: Origin`. Production must set the frontend origin(s). |
| `LOG_LEVEL` | string | `INFO` | no | slog level (`DEBUG`/`INFO`/`WARN`/`ERROR`) |
| `TLS_CERT_FILE` | path | *(empty)* | production | If set together with `TLS_KEY_FILE`, serves HTTPS via `httpServer.ServeTLS` instead of plaintext HTTP. |
| `TLS_KEY_FILE` | path | *(empty)* | production | TLS private key (see `TLS_CERT_FILE`). |

There is no DB, cache, or queue — the entire system is in-memory. Restart loses state.

## Running tests

```bash
# Unit tests (no race — fast feedback)
go test ./... -count=1 -timeout 300s

# Full suite with race detector (CI parity)
go test ./... -race -count=1 -timeout 300s

# Coverage report
go test ./... -coverprofile=coverage.out -coverpkg=./...
go tool cover -func=coverage.out

# Benchmarks
go test ./internal/... -bench=. -benchmem -benchtime=5s

# Fuzz smoke (30s per target — matches CI)
go test ./internal/types -fuzz=FuzzValue_Compare -fuzztime=30s -run='^$'
go test ./api -fuzz=FuzzParseTxnID -fuzztime=30s -run='^$'
go test ./api -fuzz=FuzzIsolationFromString -fuzztime=30s -run='^$'
```

Or via Makefile:

```bash
make test        # unit
make race        # race detector
make coverage    # coverage report
make bench       # benchmarks
make ci-local    # CI parity (tidy + fmt + vet + race + coverage)
```

## Build and run in Docker

```bash
# Build the production image (multi-stage, distroless runtime)
docker build -t txn-manager:latest .

# Run with production hardening
docker run --rm -p 8080:8080 \
  -e ADMIN_TOKEN=$(openssl rand -hex 32) \
  -e CORS_ALLOW_ORIGINS=https://app.example.com \
  txn-manager:latest

# Health-check
curl -fsS http://localhost:8080/healthz
curl -fsS http://localhost:8080/readyz
```

## API reference

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/healthz` | — | Liveness probe |
| GET | `/readyz` | — | Readiness probe |
| POST | `/api/txn/begin` | — | Begin a transaction |
| POST | `/api/txn/{id}/commit` | — | Commit |
| POST | `/api/txn/{id}/abort` | — | Abort |
| GET  | `/api/txn/{id}/status` | — | Status |
| GET  | `/api/txn/active` | — | Active txns |
| POST | `/api/txn/{id}/read` | — | Read a row |
| POST | `/api/txn/{id}/write` | — | Update a row |
| POST | `/api/txn/{id}/scan` | — | Scan a table |
| POST | `/api/txn/{id}/insert` | — | Insert |
| POST | `/api/txn/{id}/delete` | — | Delete |
| POST | `/api/txn/{id}/savepoint` | — | Create savepoint |
| POST | `/api/txn/{id}/rollback-to` | — | Rollback to savepoint |
| DELETE | `/api/txn/{id}/savepoint/{name}` | — | Release savepoint |
| GET  | `/api/locks` | — | All lock queues |
| GET  | `/api/locks/{table}/{key}` | — | Queue for a row |
| GET  | `/api/deadlocks` | — | Deadlock history |
| GET  | `/api/wfg` | — | Wait-for graph |
| GET  | `/api/mvcc/chain/{table}/{key}` | — | Version chain |
| GET  | `/api/mvcc/stats` | — | Vacuum stats |
| POST | `/api/mvcc/vacuum` | **admin** | Trigger vacuum |
| GET  | `/api/metrics` | — | Metrics snapshot |
| POST | `/api/reset` | **admin** | Abort all + reseed |
| GET  | `/api/scenarios` | — | List scenarios |
| POST | `/api/scenarios/{name}/run` | — | Run a scenario |
| POST | `/api/benchmark/run` | **admin** | Start TPC-B |
| GET  | `/api/benchmark/results/{jobId}` | — | Poll benchmark result |
| GET  | `/sse/events` | — | SSE event stream |
| GET  | `/sse/wfg` | — | SSE wait-for-graph stream |

Every response carries an `X-Request-ID` header (or echoes the caller's). 4xx/5xx bodies are `{"error": "<sanitized message>"}` — internal details are logged server-side with the request ID, never leaked to the client.

### BeginRequest

```json
{ "protocol": "mvcc", "isolation": "read_committed", "lockTimeoutMs": 5000 }
```

- `protocol`: `"2pl"` (default) or `"mvcc"`
- `isolation`: `"read_uncommitted"` / `"read_committed"` (default) / `"repeatable_read"` / `"serializable"`
- `lockTimeoutMs`: integer milliseconds, `0` = server default (5s); max 5min

## Anomaly scenarios

| Scenario | Occurs at | Prevented at |
|---|---|---|
| Dirty Read | Read Uncommitted | Read Committed+ |
| Lost Update | Read Committed (MVCC) | Repeatable Read+ |
| Non-Repeatable Read | Read Committed | Repeatable Read+ |
| Phantom Read | Read Committed | Repeatable Read+ (MVCC snapshot) |
| Write Skew | Repeatable Read | Serializable (SSI) |
| Deadlock Cycle | Any (2PL) | N/A — victim is chosen |
| Cascade Abort | Read Uncommitted | Read Committed+ |

## Production deployment checklist

This service is single-tenant, in-memory, and stateless across restarts. Before
exposing it to anything other than a developer laptop, do all of the following:

1. **Set a strong admin token** — at least 16 random bytes (`openssl rand -hex 32`).
   The server refuses to start on a non-loopback bind with a missing or short
   token (see `docs/RUNBOOK.md`).
2. **Set `CORS_ALLOW_ORIGINS`** to the frontend origin(s). Never run with `*`
   outside dev.
3. **Terminate TLS at a reverse proxy** (or set `TLS_CERT_FILE` /
   `TLS_KEY_FILE` to serve HTTPS directly). Plain HTTP leaks the admin token.
4. **Place behind a reverse proxy that enforces per-host connection limits.**
   The Go server has no `http.Server.MaxConnsPerHost` (the stdlib field does
   not exist); the proxy is the right layer.
5. **Enforce per-host rate limits** in the proxy (the server's token-bucket
   middleware is best-effort and per-`RemoteAddr`, which is the proxy address
   when proxied — treat it as a backstop, not a primary control).
6. **Wire external health probes** to `/healthz` (liveness) and `/readyz`
   (readiness — returns 503 until the catalog is seeded and `MarkReady` fires).
7. **Tail structured logs** (slog JSON to stdout). Every line carries
   `req_id`, `service`, `version`; level is `ERROR` for 5xx, `WARN` for 4xx.
8. **Scrape `/api/metrics`** — atomic counters + status-class counters.
9. **On-call:** read `docs/RUNBOOK.md` (rollback, top-5 failure modes, escalation).
10. **Audit:** `docs/AUDIT_PHASE1.md` lists every defect and the regression
    guard for each Critical/High fix.

## Architecture Decision Records

See `docs/adr/0001-0007` for the design rationale behind the two protocols,
the gap-locking scheme, the deadlock policy, MVCC visibility, SSI, and the
admin-token model.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Branch from `main`, name branches `feat/...`, `fix/...`, or `docs/...`; squash-merge PRs with Conventional-Commits messages. Every bug fix MUST include a regression test named `Test<DefectID>_<Behaviour>`.

## License

MIT — see [LICENSE](LICENSE).
