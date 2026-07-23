# ADR 0007 — Admin token for destructive endpoints

**Status:** accepted
**Date:** 2026-04-28

## Context

The HTTP API exposes three destructive endpoints:

- `POST /api/reset` — aborts every active transaction, wipes the MVCC
  store, and reseeds the catalog. Total data loss.
- `POST /api/mvcc/vacuum` — triggers a synchronous vacuum pass that
  prunes visible-dead versions.
- `POST /api/benchmark/run` — spawns an asynchronous TPC-B workload
  that pins N goroutines for up to 5 minutes and allocates millions
  of rows; a hostile or misconfigured caller can self-DoS.

In local development the API is intentionally open — no
authentication, CORS `*`, no rate limiting — so the React frontend and
curl "just work". In production, leaving these endpoints open to any
web origin (CORS `*` + no auth) allows a single cross-origin POST to
wipe the database or pin the process.

## Decision

Add an opt-in shared-secret middleware (`apiwire.AdminToken`) that
gates the three destructive endpoints behind `X-Admin-Token`. The
token is read from the `ADMIN_TOKEN` environment variable in
`cmd/server/main.go`.

- **`ADMIN_TOKEN` empty (default)** — middleware is a no-op. Local dev
  works unchanged. The threat model explicitly accepts that any
  caller can wipe the database.
- **`ADMIN_TOKEN` set** — destructive endpoints require the header.
  Missing or wrong → `401 Unauthorized`. The header value is compared
  with `crypto/subtle.ConstantTimeCompare` (api/apiwire/middleware.go)
  to avoid a timing side-channel by which an attacker who can measure
  response latency across many requests could recover `ADMIN_TOKEN`
  byte-by-byte. Note: Go's `==` on strings is **never** constant-time
  regardless of token length, so `ConstantTimeCompare` is mandatory for
  any shared-secret comparison, not optional for short tokens.

CORS is independently configurable via `CORS_ALLOW_ORIGINS` (CSV).
Production MUST set this to the frontend origin(s); `*` is dev-only.

## Consequences

- A single shared secret is coarse authorization, not authentication.
  Any party holding the token (the frontend, an operator, a leaked
  token) can call any destructive endpoint. Sufficient for a
  single-tenant teaching service; insufficient for multi-tenant.
- The token travels in a header on every destructive call. Use HTTPS
  in production (out of scope for this codebase; the server is
  HTTP-only by design).
- Rate limiting is NOT implemented; an authenticated caller can still
  hammer `/api/benchmark/run`. The `BenchmarkJobMaxRetained` cap
  bounds memory, not CPU.
- Frontend deployments that use the admin token must proxy through a
  same-origin backend that injects the header; exposing the token in
  browser JS defeats the gate.

## Alternatives considered

- **Per-user authentication (OAuth/JWT)** — rejected; out of scope
  for a single-tenant in-memory teaching service. The
  `apiwire.AdminToken` middleware is the seam where real authn would
  be inserted.
- **Always-on token requirement** — rejected; breaks the
  zero-config local dev experience.
- **IP allowlisting** — rejected; brittle behind load balancers and
  no better than a token for the threat model.
