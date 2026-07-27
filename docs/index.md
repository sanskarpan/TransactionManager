# Transaction Manager

A **from-scratch, production-hardened transaction engine** in Go — implementing
two full concurrency-control protocols, four isolation levels, a deadlock detector,
MVCC vacuum, and a real HTTP API, all backed by an interactive React UI.

Built as a teaching artifact that is nonetheless correct under `-race`, structured-logged,
health-probed, rate-limited, and continuously verified by CI.

---

## Why this exists

Most concurrency-control tutorials stop at "acquire a lock, release a lock."
They do not show:

- why **dirty reads** still happen under Read Committed with 2PL
- how an MVCC snapshot makes **phantom reads** disappear without locking
- what a **write-skew** cycle looks like, step by step, and exactly which commit aborts
- how a **deadlock** looks in the wait-for graph while it forms

This project makes all of those anomalies **observable and reproducible**. Two full
protocols run side-by-side against the same row storage and the same scenario harness,
so trade-offs are tangible rather than theoretical. Every scenario produces a
step-by-step trace you can read line by line.

---

## What it implements

=== "Protocols"

    | Protocol | Isolation levels |
    |---|---|
    | **Strict 2PL** with IS/IX/SIX intention locks | RU · RC · RR · Serializable |
    | **MVCC** with per-row version chains | RU · RC · RR · Serializable (SSI) |

=== "Anomalies reproduced"

    | Anomaly | Occurs at | Prevented at |
    |---|---|---|
    | Dirty Read | Read Uncommitted | Read Committed+ |
    | Non-Repeatable Read | Read Committed | Repeatable Read+ |
    | Phantom Read | Read Committed | Repeatable Read+ |
    | Lost Update | RC (MVCC) | Repeatable Read+ |
    | Write Skew | Repeatable Read | Serializable (SSI) |
    | Deadlock Cycle | any (2PL) | detected + resolved |
    | Cascade Abort | Read Uncommitted | Read Committed+ |

=== "Infrastructure"

    | Feature | Detail |
    |---|---|
    | HTTP API | chi router, 28 endpoints, SSE streams |
    | Observability | structured slog, per-request IDs, `/api/metrics` |
    | Security | admin-token gating, CORS allowlist, `crypto/subtle` comparison |
    | Reliability | graceful shutdown, readiness probe, in-flight drain |
    | Testing | `-race` clean, fuzz targets, regression guards |
    | Benchmarks | TPC-B workload, balance-invariant verifier |

---

## System map

```
            ┌──────────────────────┐
            │  React UI (web/)     │
            │  Vite · ky · zustand │
            └──────────┬───────────┘
                       │ HTTP + SSE
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  api.Server  (chi router)                                        │
│  RequestID → AccessLog → MaxBody → CORS → RateLimit → AdminToken │
│  28 endpoints: txn lifecycle, row ops, savepoints,              │
│  scenarios, benchmark, locks/deadlocks/mvcc, metrics, health    │
└──────────────────────┬───────────────────────────────────────────┘
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  txn.Manager (central orchestrator)                              │
│  ├─ Protocol2PL → LockTable (sync.Map of LockQueues)            │
│  ├─ ProtocolMVCC → MVCCStore (2-level sync.Map of VersionChains) │
│  ├─ SSITracker (SIREAD locks + rw-anti-dependency graph)         │
│  └─ UndoLog + Savepoints per transaction                        │
└──────┬───────────────────────────────┬────────────────────────────┘
       ▼                               ▼
┌─────────────────────┐    ┌──────────────────────────┐
│ DeadlockDetector    │    │ Vacuum (background)       │
│ 50 ms DFS over WFG  │    │ 30 s prune + reap        │
│ youngest victim     │    │ + history-map pruning     │
└─────────────────────┘    └──────────────────────────┘
```

---

## Start here

<div class="grid cards" markdown>

-   **[Quick Start](quickstart.md)**

    Run the server and send your first transaction in under 60 seconds.

-   **[Anomaly Scenarios](scenarios.md)**

    Step-by-step reproductions of all 7 classic concurrency anomalies.

-   **[API Reference](api-reference.md)**

    All 28 HTTP endpoints with request / response shapes.

-   **[Architecture Overview](architecture/overview.md)**

    Package layout, data flow, and concurrency model.

</div>

---

## Deep dives

| Article | What you'll learn |
|---|---|
| [MVCC vs 2PL](deep-dives/mvcc-vs-2pl.md) | When readers block writers (and when they don't), and why |
| [Snapshot visibility](deep-dives/visibility-algorithm.md) | The predicate that decides which version a transaction sees |
| [Isolation level reference](deep-dives/isolation-levels.md) | Formal definitions, SQL standard, what each level allows |

---

## Design decisions

Seven Architecture Decision Records capture the "why" behind non-obvious choices.

| ADR | Decision |
|---|---|
| [0001](adr/0001-two-protocols.md) | Two protocols (2PL + MVCC) implemented side-by-side |
| [0002](adr/0002-strict-2pl-intention-locks.md) | Strict 2PL with IS/IX/SIX intention locks |
| [0003](adr/0003-deadlock-resolution.md) | Deadlock detection (cycle-based) over prevention policies |
| [0004](adr/0004-mvcc-visibility.md) | MVCC visibility predicate and snapshot design |
| [0005](adr/0005-ssi-serializable.md) | SSI for Serializable MVCC (not 2PL predicate locking) |
| [0006](adr/0006-gap-locking.md) | Gap locking for phantom prevention in 2PL |
| [0007](adr/0007-admin-token.md) | Admin token model and threat boundary |

---

> This site is generated with [MkDocs Material](https://squidfunk.github.io/mkdocs-material/)
> from the `docs/` directory. Every page is a real file in the repository.
