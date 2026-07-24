# AGENTS.md — instructions for AI agents working on this repo

This file gives opencode (and any other agent) the verifiable commands
needed to keep the codebase healthy. Run the appropriate one before
declaring a task done.

## Commands

- **Build:** `go build ./...`
- **Vet:** `go vet ./...`
- **Unit tests (race):** `go test ./... -race -count=1 -timeout 300s`
- **Benchmarks:** `go test ./internal/... -bench=. -benchmem -benchtime=5s`
- **Coverage:** `go test ./... -coverprofile=coverage.out -coverpkg=./... && go tool cover -func=coverage.out`
- **Fuzz smoke:** `go test ./internal/types -fuzz=FuzzValue_Compare -fuzztime=10s -run='^$'`
- **Module hygiene:** `go mod tidy && git diff --exit-code go.mod go.sum`
- **Lint (if golangci-lint is installed):** `golangci-lint run`
- **Vulnerability scan (if govulncheck is installed):** `govulncheck ./...`
- **Run server:** `go run ./cmd/server`
- **Frontend dev:** `cd web && npm install && npm run dev`

## Conventions

- Go module path: `github.com/sanskarpan/TransactionManager`
- Public error messages returned to API clients must be sanitized via
  `errWrite(w, err)`; never pass raw `err.Error()` to `errResponse`.
- New public constants (timeouts, intervals, caps) go in
  `api/config.go`; do not scatter magic numbers through handlers.
- Every fix MUST be accompanied by a regression test named
  `Test<DefectID>_<Behaviour>` (see `internal/txn/regression_test.go`,
  `internal/mvcc/vacuum_test.go`, `api/regression_test.go`).
- ADRs live in `docs/adr/` and are numbered; the runbook is
  `docs/RUNBOOK.md`.

## Threat model summary

- The HTTP API is open by default in local dev (no ADMIN_TOKEN).
- Destructive endpoints (`/api/reset`, `/api/mvcc/vacuum`,
  `/api/benchmark/run`) are gated behind `X-Admin-Token` when
  `ADMIN_TOKEN` is set.
- CORS is `*` in dev; production must pass `CORS_ALLOW_ORIGINS`.
- See `docs/adr/0007-admin-token.md` for the full model.
