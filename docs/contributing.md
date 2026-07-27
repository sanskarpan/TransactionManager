# Contributing

## Setup

```bash
git clone https://github.com/sanskarpan/TransactionManager.git
cd TransactionManager

# Backend
go mod download
go build ./...

# Frontend
cd web && npm install
```

## Branch naming

| Type | Pattern | Example |
|---|---|---|
| Feature | `feat/...` | `feat/wait-die-policy` |
| Bug fix | `fix/...` | `fix/phantom-under-rr` |
| Docs | `docs/...` | `docs/ssi-deep-dive` |
| Refactor | `refactor/...` | `refactor/store-two-level` |

Branches are squash-merged; the PR title becomes the merge commit message.

## Commit messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>: <short summary>

[optional body]

Co-Authored-By: ...
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `perf`.

## Testing requirements

Every bug fix **must** include a regression test named `Test<ID>_<Behaviour>`:

```go
// Regression guard for CT-19: ReapEmpty race with MVCCWrite.
func TestCT19_ReapEmptyRaceWithMVCCWrite(t *testing.T) { ... }
```

All tests must pass the race detector:

```bash
go test -race ./...
```

New concurrent paths must have a table-driven test that exercises them under `-race`.

## Code style

```bash
gofmt -w .             # formatting (enforced by CI)
go vet ./...           # static analysis
golangci-lint run      # linter suite (config in .golangci.yml)
```

Key linter rules:
- No `//nolint` without a comment explaining why.
- Exported symbols must have doc comments.
- Error returns must be checked (errcheck).

## Adding a new anomaly scenario

1. Implement `scenario.Scenario` in `internal/scenario/`:

    ```go
    type MyScenario struct{}

    func (s *MyScenario) Name() string        { return "my_scenario" }
    func (s *MyScenario) Description() string { return "Demonstrates ..." }
    func (s *MyScenario) Run(ctx context.Context, mgr *txn.Manager) (*Result, error) {
        // interleave transactions here
    }
    ```

2. Register it in `internal/scenario/registry.go`:

    ```go
    r.Register(&MyScenario{})
    ```

3. Add a step-by-step explanation to [Anomaly Scenarios](scenarios.md).

4. Write an integration test in `api/` that calls `POST /api/scenarios/my_scenario/run`
   and asserts the expected trace.

## Adding a new isolation level or protocol variant

1. Add the new `IsolationLevel` constant to `internal/txn/types.go`.
2. Wire dispatch in `txn.Manager` methods (`MVCCRead`, `MVCCWrite`, `Commit`, etc.).
3. Add an ADR in `docs/adr/` explaining the design decision.
4. Update the [Isolation Level Reference](deep-dives/isolation-levels.md).

## CI pipeline

The CI workflow (`.github/workflows/ci.yml`) runs on every push and PR:

| Step | Command |
|---|---|
| Format check | `gofmt -l .` |
| Vet | `go vet ./...` |
| Lint | `golangci-lint run` |
| Race tests | `go test -race ./...` |
| Coverage | `go test -coverprofile=coverage.out ./...` |
| Fuzz smoke | 30 s per fuzz target |
| Vuln scan | `govulncheck ./...` |

The docs CI (`.github/workflows/docs.yml`) builds the MkDocs site on every change to
`docs/` or `mkdocs.yml` and deploys to GitHub Pages on `main`.

## Good first issues

- Wire **Wait-Die** or **Wound-Wait** as a selectable deadlock policy (the skeleton
  is in `internal/deadlock/`; it needs a `policy` parameter on `Manager.Begin`).
- Add a **`/api/txn/active/count`** endpoint returning just the count (no lock needed,
  uses an atomic).
- Add a **metrics endpoint** that emits Prometheus text format alongside the existing
  JSON snapshot.
- Write a **Jepsen-style linearizability** test harness for the TPC-B invariant
  under concurrent abort injection.
- Add **WebSocket** support alongside SSE for the WFG stream (SSE is unidirectional;
  WebSocket would allow the client to request graph subsets).
