# Quick Start

## Prerequisites

| Tool | Minimum version |
|---|---|
| Go | 1.22+ |
| Node.js | 22+ |
| Git | any recent |

Optional: `golangci-lint`, `govulncheck` for local quality checks.

## 1. Clone and run the backend

```bash
git clone https://github.com/sanskarpan/TransactionManager.git
cd TransactionManager

go run ./cmd/server
```

The server binds to `:8080` by default. You'll see structured JSON logs on stdout.

Verify it's up:

```bash
curl -s http://localhost:8080/healthz   # → {"status":"ok"}
curl -s http://localhost:8080/readyz    # → {"status":"ready"}
```

## 2. Run the frontend

In a second terminal:

```bash
cd web
npm install
npm run dev
```

Open **<http://localhost:5173>** — the Vite dev server proxies `/api` and `/sse` to `localhost:8080`.

## 3. Start a transaction

```bash
# Begin a Serializable MVCC transaction
curl -s -X POST http://localhost:8080/api/txn/begin \
  -H 'Content-Type: application/json' \
  -d '{"protocol":"mvcc","isolation":"serializable"}' | jq .
# → {"txnId": 1, "protocol": "mvcc", "isolation": "serializable", ...}

# Read a row (accounts table, key "1")
curl -s -X POST http://localhost:8080/api/txn/1/read \
  -H 'Content-Type: application/json' \
  -d '{"table":"accounts","key":"1"}' | jq .

# Commit
curl -s -X POST http://localhost:8080/api/txn/1/commit | jq .
```

## 4. Run a scenario

```bash
# List available scenarios
curl -s http://localhost:8080/api/scenarios | jq '.[].name'

# Run the "write_skew" scenario (requires admin token — see below)
curl -s -X POST http://localhost:8080/api/scenarios/write_skew/run \
  -H 'X-Admin-Token: dev' | jq .
```

## Running tests

```bash
# Unit tests (fast)
go test ./...

# Race-detector pass (CI parity)
go test -race ./...

# Benchmarks
go test ./benchmark/... -bench=. -benchmem
```

## Docker

```bash
docker build -t txn-manager:latest .

docker run --rm -p 8080:8080 \
  -e ADMIN_TOKEN=$(openssl rand -hex 32) \
  txn-manager:latest
```

## Configuration

All configuration is via environment variables — no config files.

| Variable | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `:8080` | TCP bind address |
| `ADMIN_TOKEN` | *(empty)* | Secret for destructive endpoints. Empty = open (dev only). |
| `CORS_ALLOW_ORIGINS` | `*` | Comma-separated allowed origins. Set the frontend origin in production. |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARN` / `ERROR` |
| `TLS_CERT_FILE` | *(empty)* | Path to TLS certificate. Enables HTTPS when set with `TLS_KEY_FILE`. |
| `TLS_KEY_FILE` | *(empty)* | Path to TLS private key. |

!!! warning "Production"
    Set `ADMIN_TOKEN` (≥ 16 bytes), `CORS_ALLOW_ORIGINS`, and terminate TLS
    before exposing the service. See the [Runbook](RUNBOOK.md) for the full checklist.
