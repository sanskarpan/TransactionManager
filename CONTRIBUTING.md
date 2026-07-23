# Contributing to Transaction Manager

Thanks for taking the time to contribute. This document is short on
purpose; the conventions below keep the codebase reviewable and the CI
green.

## Branch & commit conventions

- Branch from `main`. Name branches `feat/...`, `fix/...`,
  `docs/...`, `chore/...`, or `refactor/...`.
- Use Conventional-Commits messages: `feat(api): add X`,
  `fix(txn): handle Y`, `docs: ...`. The CI version-tag job derives
  the next tag from the merge commit, so the message matters.
- Squash-merge PRs. One logical change per PR.

## Before you push

Run:

```bash
make ci-local
```

This runs `go mod tidy` check, `gofmt` check, `go vet`, the full test
suite with `-race`, and coverage. CI runs the same suite plus
`govulncheck`, `golangci-lint`, fuzz smoke, and a container build; if
it's not green locally it won't be green in CI.

## Test discipline

- Every bug fix MUST include a regression test named
  `Test<DefectID>_<Behaviour>`. Defect IDs are documented in
  [`AUDIT.md`](AUDIT.md). See `internal/txn/regression_test.go`,
  `internal/mvcc/vacuum_test.go`, and `api/regression_test.go` for
  examples.
- Tests must be hermetic: no shared mutable state, no reliance on
  wall-clock ordering, no external network. Use `t.Cleanup` for
  teardown.
- Replace `t.Logf`-only "tests" with content-level assertions. A test
  that only logs is not a test.
- New input-parsing code must include a fuzz target. See
  `FuzzParseTxnID`, `FuzzIsolationFromString`, `FuzzValue_Compare`.

## Code style

- `gofmt` is non-negotiable. Run `make fmt` before pushing.
- No `// indirect` markers on direct dependencies in `go.mod`. Run
  `go mod tidy`.
- No magic numbers: timeouts, intervals, and caps go in
  `api/config.go` (cross-handler) or a `const` block at the top of
  the defining file (package-local).
- Error messages returned to API clients must be sanitized via
  `errWrite(w, err)`; never pass raw `err.Error()` to `errResponse`.
- New exported functions/types/packages have a doc comment explaining
  *why* they exist, not just *what* they do.

## Architecture Decision Records

Significant design decisions are recorded in [`docs/adr/`](docs/adr/).
If your PR introduces or changes a cross-cutting decision, add an ADR
superseding the prior one (don't edit history; write
`0008-foo-supersedes-0003.md`).

## Threat model

The API is open by default in local dev (no `ADMIN_TOKEN`). If you add
a destructive endpoint, gate it behind `apiwire.AdminToken` in
`api/server.go` and document it in `docs/adr/0007-admin-token.md`.
