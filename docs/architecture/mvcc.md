# MVCC — Multi-Version Concurrency Control

## Core idea

Instead of overwriting rows, MVCC **appends new versions**. Each version is stamped with the transaction ID that created it (`XMin`) and, when deleted or replaced, the ID of the deleting transaction (`XMax`). A reader applies a **visibility predicate** to the version chain to find the version visible to its snapshot — without acquiring any locks.

This means readers and writers never block each other, which is the key advantage over 2PL for read-heavy workloads.

## Version chain structure

```
chain for (accounts, "42"):

  head → [XMin=7, XMax=11, balance=200]   (written by T7, overwritten by T11)
       → [XMin=11, XMax=0, balance=250]   (written by T11, current)
```

Versions are stored newest-first. `XMax=0` means the version is not yet deleted or replaced. The chain lives in a two-level `sync.Map`: `table → rowKey → *VersionChain`.

## Snapshots

At `BEGIN` (Repeatable Read / Serializable) or at each statement (Read Committed), the transaction captures a **snapshot**:

```go
type Snapshot struct {
    Xmin   ID        // txn's own ID
    Xmax   ID        // next ID to be allocated at snapshot time
    Active []ID      // IDs of transactions active (uncommitted) at snapshot time
}
```

A version `v` is **visible** to a snapshot if:

1. `v.XMin` is committed **and** `v.XMin < Xmax` **and** `v.XMin ∉ Active` — the row was written before the snapshot and the writer committed before we started.
2. `v.XMin == Xmin` — we wrote this version ourselves.
3. `v.XMax == 0` — the version is not deleted.
4. OR `v.XMax` is not yet committed at snapshot time (even if `XMax` has committed since then, from our snapshot's perspective it hadn't).

This logic lives in `internal/mvcc/visibility.go:IsVisible`.

## Isolation levels under MVCC

| Level | Snapshot taken | What it prevents |
|---|---|---|
| Read Uncommitted | per statement | nothing |
| Read Committed | per statement | dirty reads |
| Repeatable Read | at BEGIN | dirty reads, non-repeatable reads, phantoms |
| Serializable | at BEGIN + SSI check at commit | all anomalies including write skew |

!!! note
    Read Uncommitted with MVCC is unusual — in a real MVCC database (PostgreSQL)
    it behaves like Read Committed. Here it is implemented as truly reading
    uncommitted versions, for teaching purposes.

## Write conflicts

When two MVCC transactions try to write the same row, a **write-write conflict check** (`CheckWriteConflict`) decides who proceeds:

- If the row's current `XMin` belongs to a **committed** transaction that was **in our snapshot's active set** (i.e., it committed after we started), we have a conflict → `ErrWriteConflict`.
- If the row's `XMin` belongs to an **active** transaction → conflict (we must not overwrite an uncommitted write).
- Otherwise → no conflict, proceed.

The check uses a single `TxnStatus(id)` call under one read lock to atomically read both active and committed state, avoiding a TOCTOU race.

## Vacuum

The vacuum runs every 30 seconds and performs two passes:

1. **Prune dead versions** — for each version chain, delete versions whose `XMax` is below the oldest active transaction ID (no active transaction will ever see them again).
2. **Reap empty chains** — remove chains with zero versions from the store. The reap holds the chain's write lock across the tombstone and `sync.Map.Delete` to prevent a concurrent `MVCCWrite` from appending to an already-deleted chain.

The `committed` and `aborted` history maps are also pruned: entries older than the oldest active transaction are dropped (horizon-based), and the committed map is capped at 65,536 entries (cap-based, O(n log n) via sort) to bound memory.

## Undo log

Even though MVCC doesn't rewrite rows in-place, the transaction still maintains an **undo log** for two purposes:

1. **Savepoint rollback** — `ROLLBACK TO SAVEPOINT` must undo MVCC writes (remove the version this txn created from the chain) back to the savepoint marker.
2. **Abort** — `applyMVCCUndo` walks the undo log to call `RemoveByXMin` and `ClearXMax` on every chain this transaction touched.
