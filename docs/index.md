# Transaction Manager

An in-memory **transaction manager** that implements two concurrency-control protocols side-by-side — **strict two-phase locking (2PL)** and **multi-version concurrency control (MVCC)** — and exposes them through a full HTTP API and an interactive React UI.

Built as a teaching artifact hardened to production standards: race-free under `-race`, structured logging, health probes, admin-token gating, and CI.

---

## What it demonstrates

| Concept | Where |
|---|---|
| Strict 2PL with IS/IX/SIX intention locks | `internal/lock/` |
| FIFO wait queues + lock upgrade | `internal/lock/` |
| Background deadlock detector (DFS over WFG) | `internal/deadlock/` |
| MVCC version chains + vacuum | `internal/mvcc/` |
| Four isolation levels (RU → RC → RR → Serializable) | `internal/txn/` |
| Serializable Snapshot Isolation (SSI) | `internal/isolation/` |
| Savepoints + undo log | `internal/txn/` |
| 7 concurrency anomaly scenarios | `internal/scenario/` |
| TPC-B benchmark with balance invariant | `benchmark/` |
| Full REST + SSE API (chi router) | `api/` |
| React + TypeScript exploration UI | `web/` |

---

## Why two protocols?

A single protocol makes half the curriculum invisible. 2PL cannot demonstrate write-skew under Repeatable Read; MVCC cannot demonstrate lock-based deadlock cycles or gap locking. Running both against the same row store and scenario framework makes the trade-offs tangible. See [ADR-0001](adr/0001-two-protocols.md) for the full rationale.

---

## System map

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
│  │ RequestID → AccessLog → MaxBody → CORS → AdminToken     │ │
│  └─────────────────────────────────────────────────────────┘ │
│  Handlers: txn lifecycle, row ops, savepoints, scenarios,   │
│             benchmark, locks/deadlocks/mvcc, metrics, health │
└──────────────────────┬───────────────────────────────────────┘
                       ▼
┌──────────────────────────────────────────────────────────────┐
│  txn.TxnManager                                              │
│  - txns / committed / aborted maps (mu-protected)           │
│  - LockAcquirer (2PL) → LockTable                           │
│  - MVCCStore (two-level sync.Map of VersionChains)          │
│  - SSITracker (SIREAD + rw-anti-dependency edges)           │
│  - Undo log + savepoints                                     │
└──────┬───────────────────────────────┬──────────────────────┘
       ▼                               ▼
┌──────────────────────┐    ┌────────────────────────────┐
│ DeadlockDetector      │    │ Vacuum (background loop)   │
│ 50 ms DFS over WFG    │    │ 30 s prune + empty-chain   │
│ victim = youngest     │    │ reap                       │
└──────────────────────┘    └────────────────────────────┘
```

---

## Quick links

- [Quick Start](quickstart.md) — run the server in 60 seconds
- [Architecture Overview](architecture/overview.md) — packages and data flow
- [API Reference](api-reference.md) — all HTTP endpoints
- [Anomaly Scenarios](scenarios.md) — the 7 reproducible anomalies
- [Operations Runbook](RUNBOOK.md) — deploy, monitor, triage
