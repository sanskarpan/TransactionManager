# API Reference

All endpoints are served on `:8080` (configurable via `LISTEN_ADDR`). Every response includes an `X-Request-ID` header. Error bodies are `{"error": "<message>"}` — internal details are never leaked to the client.

## Health

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/healthz` | — | Liveness probe — always 200 when the process is up |
| GET | `/readyz` | — | Readiness probe — 503 until the catalog is seeded |

## Transaction lifecycle

### Begin

`POST /api/txn/begin`

```json
{
  "protocol":      "mvcc",
  "isolation":     "serializable",
  "lockTimeoutMs": 5000
}
```

| Field | Values | Default |
|---|---|---|
| `protocol` | `"2pl"`, `"mvcc"` | `"2pl"` |
| `isolation` | `"read_uncommitted"`, `"read_committed"`, `"repeatable_read"`, `"serializable"` | `"read_committed"` |
| `lockTimeoutMs` | integer ms, `0` = server default (5 s), max 5 min | `0` |

**Response 200:**
```json
{"txnId": 1, "protocol": "mvcc", "isolation": "serializable"}
```

**Response 503:** too many active transactions (when `MaxActive` cap is reached).

### Commit / Abort / Status

| Method | Path | Description |
|---|---|---|
| POST | `/api/txn/{id}/commit` | Commit — may return `ErrSSIConflict` for Serializable MVCC |
| POST | `/api/txn/{id}/abort` | Abort and roll back |
| GET | `/api/txn/{id}/status` | `{"status": "active"/"committed"/"aborted"}` |
| GET | `/api/txn/active` | List of all active transaction IDs |

## Row operations

All row operations require the transaction to be active. The `table` field must be one of the seeded tables (`accounts`, `products`, `inventory`).

### Read

`POST /api/txn/{id}/read`

```json
{"table": "accounts", "key": "1"}
```

Returns the row visible to the transaction's snapshot, or 404 if not found.

### Write (update)

`POST /api/txn/{id}/write`

```json
{"table": "accounts", "key": "1", "values": [{"type": "int", "v": 250}]}
```

### Insert

`POST /api/txn/{id}/insert`

```json
{"table": "accounts", "key": "999", "values": [{"type": "int", "v": 100}]}
```

### Delete

`POST /api/txn/{id}/delete`

```json
{"table": "accounts", "key": "999"}
```

### Scan

`POST /api/txn/{id}/scan`

```json
{"table": "accounts"}
```

Returns all rows in the table visible to the transaction's snapshot.

## Savepoints

| Method | Path | Body | Description |
|---|---|---|---|
| POST | `/api/txn/{id}/savepoint` | `{"name":"sp1"}` | Create savepoint |
| POST | `/api/txn/{id}/rollback-to` | `{"name":"sp1"}` | Roll back to savepoint |
| DELETE | `/api/txn/{id}/savepoint/{name}` | — | Release (forget) savepoint |

## Locks and deadlocks

| Method | Path | Description |
|---|---|---|
| GET | `/api/locks` | All active lock queues with granted and waiting sets |
| GET | `/api/locks/{table}/{key}` | Lock queue for a specific row |
| GET | `/api/deadlocks` | Recent deadlock history (cycle, victim, timestamp in RFC3339) |
| GET | `/api/wfg` | Current wait-for graph as `{nodes: [], edges: [{from, to}]}` |

## MVCC

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/api/mvcc/chain/{table}/{key}` | — | Full version chain for a row |
| GET | `/api/mvcc/stats` | — | Vacuum statistics (pruned versions, runs, lastRunAt) |
| POST | `/api/mvcc/vacuum` | **admin** | Trigger a manual vacuum pass |

## Scenarios

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/api/scenarios` | — | List all available scenarios |
| POST | `/api/scenarios/{name}/run` | **admin** | Run a scenario, returns step-by-step trace |

See [Anomaly Scenarios](scenarios.md) for the list of scenario names.

## Benchmark

| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/api/benchmark/run` | **admin** | Start a TPC-B benchmark job, returns `{"jobId": "..."}` |
| GET | `/api/benchmark/results/{jobId}` | — | Poll for result (pending/running/done) |

## Metrics

`GET /api/metrics` — returns a JSON snapshot of all atomic counters and the HTTP latency histogram.

## Admin endpoints

Destructive endpoints (`/api/reset`, `/api/mvcc/vacuum`, `/api/benchmark/run`, `/api/scenarios/{name}/run`) require the `X-Admin-Token` header when `ADMIN_TOKEN` is configured. Without the env var, all admin endpoints return **403** (unavailable, not unprotected).

```bash
curl -X POST http://localhost:8080/api/reset \
  -H 'X-Admin-Token: <your-token>'
```

`POST /api/reset` aborts all active transactions and reseeds the catalog to the 100-account initial state.

## SSE streams

| Path | Tick | Payload |
|---|---|---|
| `GET /sse/events` | on change | Domain events (txn begun/committed/aborted, deadlock) |
| `GET /sse/wfg` | 500 ms | Wait-for graph snapshot `{nodes, edges}` |

Both streams use the standard `text/event-stream` content type. Clients should handle reconnects.
