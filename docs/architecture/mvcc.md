# MVCC — Multi-Version Concurrency Control

> **See also:** [ADR-0004 — MVCC visibility](../adr/0004-mvcc-visibility.md) · [SSI](ssi.md) · [MVCC vs 2PL](../deep-dives/mvcc-vs-2pl.md) · [Snapshot visibility algorithm](../deep-dives/visibility-algorithm.md)

## Core principle

Instead of overwriting a row, MVCC **appends a new version** stamped with the writing
transaction's ID (`XMin`). The old version is not deleted — it stays in the chain so
concurrent readers can still see it. When a transaction deletes or replaces a row,
it stamps the old version's `XMax` with its own ID.

**Result:** readers and writers never block each other. A reader traverses the version
chain and picks the version that was _committed and visible_ at its snapshot time —
without acquiring any lock.

---

## Version chain structure

Each `(table, key)` pair has one `VersionChain` holding a linked list of versions,
newest first:

```
VersionChain  accounts / "42"

  head ──► Version { XMin=11, XMax=0,  Data=[balance: 250] }   ← current
           Version { XMin=7,  XMax=11, Data=[balance: 200] }   ← superseded by T11
           Version { XMin=3,  XMax=7,  Data=[balance: 100] }   ← superseded by T7
```

`XMin` = the transaction that created this version.  
`XMax` = the transaction that deleted/replaced it (`0` means still live).

All chains live in a two-level `sync.Map`: `table → rowKey → *VersionChain`. This
makes `ForEachChainInTable` (used by scans) O(rows in table) rather than O(total rows
across all tables).

---

## Snapshots

At `BEGIN` (Repeatable Read / Serializable) or at each statement (Read Committed),
the manager captures a snapshot:

```go
type Snapshot struct {
    Xmin   uint64    // this transaction's own ID
    Xmax   uint64    // next ID to allocate at snapshot time
    Active []uint64  // IDs currently active (uncommitted) at snapshot time
}
```

**Interpretation:**

| ID range | Meaning |
|---|---|
| `id < Xmin` | Began before us — committed or aborted before we started |
| `Xmin ≤ id < Xmax` and `id ∉ Active` | Began after us? No — committed before our snapshot |
| `Xmin ≤ id < Xmax` and `id ∈ Active` | Was active when we snapshotted — treat as uncommitted |
| `id ≥ Xmax` | Began strictly after our snapshot — invisible |

---

## Visibility predicate

A version `v` is visible to transaction `T` with snapshot `snap` if **all** of:

```
1. v.XMin is committed AND
2. v.XMin < snap.Xmax AND
3. v.XMin ∉ snap.Active        (committed before our snapshot, not mid-flight)
   OR v.XMin == snap.Xmin      (we wrote this version ourselves)

AND

4. v.XMax == 0                 (not deleted)
   OR v.XMax is not committed at our snapshot time
      (i.e., the deleter started after us, or hasn't committed yet)
```

This logic is in `internal/mvcc/visibility.go:IsVisible`. The full derivation is in
[Snapshot visibility algorithm](../deep-dives/visibility-algorithm.md).

---

## Isolation levels under MVCC

| Level | Snapshot taken | Anomalies prevented |
|---|---|---|
| **Read Uncommitted** | — (reads latest version) | none — dirty reads visible |
| **Read Committed** | per statement | dirty reads |
| **Repeatable Read** | at `BEGIN` | dirty reads, non-repeatable reads, phantoms |
| **Serializable** | at `BEGIN` + SSI commit check | all, including write skew |

!!! note "MVCC + RU"
    Read Uncommitted in a real MVCC system (e.g. PostgreSQL) behaves like
    Read Committed — you cannot actually read uncommitted data from a version chain
    because uncommitted versions are never the "current" version a Postgres reader
    would pick. Here, RU is implemented to read the _latest_ version regardless of
    commit status, for pedagogical clarity.

---

## Write conflict detection

When T2 tries to write a key that T1 has already modified:

```
CheckWriteConflict(txn=T2, chain, snap=T2.snapshot):

  for each version v in chain:
      active, committed = TxnStatus(v.XMin)  // single atomic read

      if active:
          // T1 is still running — T2 cannot overwrite an uncommitted write
          return ErrWriteConflict

      if v.XMin ∈ snap.Active AND committed:
          // T1 committed AFTER T2's snapshot was taken
          // T2's snapshot cannot see T1's write, but T1 is now durably committed
          // Allowing T2 to proceed = lost update
          return ErrWriteConflict

  return nil   // no conflict
```

`TxnStatus` reads `active` and `committed` under a single `RLock` — a single atomic
call that prevents the TOCTOU race of calling `IsActive` and `IsCommitted` separately
(a transaction could commit between the two calls).

---

## Undo log

Even in MVCC, transactions maintain an undo log for two purposes:

**1. Savepoint rollback** — `ROLLBACK TO SAVEPOINT sp1` must undo MVCC changes
back to the savepoint marker. For each entry since the savepoint:

```
UndoInsert  → chain.RemoveByXMin(txnID)      // remove the version we created
UndoDelete  → chain.ClearXMax(txnID)         // restore XMax=0 on the version we marked deleted
UndoUpdate  → RemoveByXMin + ClearXMax       // both
```

**2. Abort** — `applyMVCCUndo` walks the full undo log and undoes all chain mutations.

The undo log is separate from the MVCC chain — it records `(table, key, op, before-image)`
tuples that describe _what was done_, not what the data was.

---

## Vacuum

The vacuum runs every **30 seconds** and performs three passes:

### Pass 1 — Horizon-based pruning

```
horizon = OldestActiveTxnID()    // 0 if no active transactions

for each version v in every chain:
    if v.XMax != 0
       AND IsCommitted(v.XMax)
       AND v.XMax < horizon:
        // No active transaction can see this version; safe to drop
        remove v from chain
```

### Pass 2 — Empty-chain reaping

After pruning, chains with zero remaining versions are removed from the store.
The reap holds the chain's write lock across the tombstone and `sync.Map.Delete` to
prevent a concurrent `MVCCWrite` from appending to a chain that is being deleted
(CT-19 / CT-26 regression guards in `internal/txn/regression_test.go`).

### Pass 3 — History map bounding

The `committed` and `aborted` maps in `TxnManager` are pruned:

- **Horizon-based**: entries older than `OldestActiveTxnID` are dropped.
- **Cap-based** (committed map only): if the map exceeds 65,536 entries, the oldest
  are sorted and deleted — O(n log n), not the O(n²) inner-loop it started as.
- The `aborted` map is **never** cap-pruned: removing an abort record for an ID above
  the horizon would cause `IsAborted` to return `false` for a genuinely aborted
  transaction, potentially causing a dirty read.

---

## Properties

| Property | Value |
|---|---|
| Reader–writer blocking | **None** — readers take no locks |
| Writer–writer blocking | Write-conflict check; ErrWriteConflict on collision |
| Deadlocks | Not possible in pure MVCC |
| Phantom prevention | Free at Repeatable Read+ (snapshot excludes later inserts) |
| Write skew prevention | Requires SSI (Serializable only) |
| Space overhead | O(versions per row × live rows) — vacuum reclaims |
| Snapshot cost | O(active transactions) per `BEGIN` |
