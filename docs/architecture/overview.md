# System Overview

## Package layout

```
TransactionManager/
├── cmd/server/         Entry point: parse env, wire dependencies, run HTTP server
├── api/                HTTP handlers (chi router), SSE bus, benchmark jobs
│   └── apiwire/        Middleware: RequestID, AccessLog, MaxBody, CORS, AdminToken, RateLimit
├── internal/
│   ├── txn/            TxnManager — central orchestrator for all transaction operations
│   ├── lock/           Lock table, lock modes (IS/S/IX/SIX/X), FIFO wait queues
│   ├── deadlock/       Wait-for graph (WFG), DFS cycle detector, victim selection
│   ├── mvcc/           Version chains, visibility predicate, vacuum, store
│   ├── isolation/      SSI tracker: SIREAD locks + rw-anti-dependency graph
│   ├── storage/        Row type, catalog (tables), seed data
│   ├── scenario/       7 anomaly scenarios with step-by-step trace
│   ├── metrics/        Atomic counters + latency histogram
│   └── types/          Value type, TxnError, error codes
├── benchmark/          TPC-B workload + balance-invariant verifier
├── web/                React + TypeScript + Tailwind + Vite frontend
└── docs/               ADRs, runbook, audit reports
```

## Data flow: transaction lifecycle

```
Client (curl / React UI)
    │
    │  POST /api/txn/begin  {protocol, isolation, lockTimeoutMs}
    ▼
api.handleBegin
    │  calls txn.Manager.Begin(protocol, isolation)
    ▼
txn.Manager.Begin
    │  allocates monotonic txn ID (atomic uint64)
    │  creates Transaction{ID, Protocol, Isolation, Snapshot, UndoLog, ...}
    │  registers in m.txns map (under m.mu write lock)
    └─ returns txnID to client

    ─── during transaction ───────────────────────────────────────
    │
    │  POST /api/txn/{id}/read  {table, key}
    ▼
    Protocol2PL → lock.LockTable.Acquire(S, row)
                → storage.Table.GetRow(key)
    ProtocolMVCC → mvcc.Store.GetChain(table, key)
                 → chain.FindVisible(snapshot predicate)

    │  POST /api/txn/{id}/write  {table, key, values}
    ▼
    Protocol2PL → lock.LockTable.Acquire(X, row)
                → storage.Table.PutRow(key, values)
                → txn.UndoLog.Append(op, before-image)
    ProtocolMVCC → check write conflict (CheckWriteConflict)
                 → chain.Prepend(new Version{XMin=txnID})
                 → txn.UndoLog.Append(for savepoint rollback)
    ─────────────────────────────────────────────────────────────

    │  POST /api/txn/{id}/commit
    ▼
txn.Manager.Commit
    │  (MVCC+Serializable) SSITracker.CheckCommit(txn) — detect write skew
    │  marks txn committed in m.committed map
    │  (2PL) releases all locks via LockTable
    │  (MVCC+Serializable) SSITracker.Cleanup(txnID)
    └─ returns nil or TxnError
```

## Concurrency model

The `TxnManager` uses two levels of locking:

- **`m.mu` (sync.RWMutex)** — guards the `txns`, `committed`, and `aborted` maps.
  Most read-path operations take `RLock`; Begin and state transitions take `Lock`.
- **Per-transaction `txn.mu` (sync.RWMutex)** — guards fields on a single
  transaction (UndoLog, Savepoints, read/write sets). Allows concurrent
  operations on different transactions without serializing on `m.mu`.
- **Per-chain locks** — each `VersionChain` has its own `sync.RWMutex`.
  Readers call `FindVisible` under `RLock`; writers call `Prepend` under `Lock`.
- **`LockTable`** — uses `sync.Map` for the table of queues and per-queue
  mutexes for the grant/wait lists inside each queue.

## Background goroutines

| Goroutine | Period | Purpose |
|---|---|---|
| `deadlock.Detector` | 50 ms | DFS over WFG, abort youngest victim |
| `mvcc.Vacuum` | 30 s | Prune dead versions, reap empty chains, bound history maps |
| `api.evictFinishedBenchmarkJobsLoop` | 30 s | Remove Done benchmark jobs from the in-memory map |

All background goroutines respect a cancellable context (`jobCtx` / `sseCtx`)
and are drained by `Server.Shutdown` before the process exits.
