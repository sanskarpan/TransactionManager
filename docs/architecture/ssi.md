# Serializable Snapshot Isolation (SSI)

## The problem SSI solves

Snapshot isolation (Repeatable Read in MVCC) prevents dirty reads, non-repeatable reads, and phantoms — but it does **not** prevent **write skew**.

Write skew occurs when two transactions each read a set of rows, make a decision based on those reads, and then write disjoint rows — neither write conflicts with the other, yet the combined result is impossible under any serial execution.

**Classic example — two doctors on call:**

```
T1 reads: doctors_on_call = {Alice, Bob}  → decides Alice can go off call
T2 reads: doctors_on_call = {Alice, Bob}  → decides Bob can go off call

T1 writes: Alice.on_call = false
T2 writes: Bob.on_call = false

Result: 0 doctors on call — impossible if the constraint is "≥ 1 must be on call"
```

Neither write conflicts with the other's read, so snapshot isolation allows this.

## How SSI detects it

SSI extends snapshot isolation with lightweight conflict tracking. The key insight (from Cahill et al., 2008): every non-serializable execution under snapshot isolation exhibits a **dangerous structure** — a cycle of two **rw-anti-dependency** edges.

An **rw-anti-dependency** edge T1 → T2 means:
- T1 read a version of a row
- T2 wrote a **later** version of that row (committed after T1's snapshot)

SSI tracks these edges and aborts a transaction when a cycle of exactly two such edges is detected at commit time.

## Implementation

The `SSITracker` in `internal/isolation/ssi.go` maintains:

- **SIREAD locks** — a set of keys each transaction has read, associated with the transaction.
- **rw-anti-dependency graph** — directed edges between transactions.

```
RecordRead(txn, key):
    add key to txn's SIREAD set

RecordWrite(writer, key):
    for each reader that has a SIREAD lock on key:
        add edge reader → writer
        if writer already has an edge writer → X and X → reader:
            cycle detected → abort writer (or reader at commit time)

CheckCommit(txn):
    if txn has an incoming rw-edge AND an outgoing rw-edge:
        abort txn with ErrSSIConflict
```

## When SSI fires

```
T1: BEGIN mvcc serializable
T2: BEGIN mvcc serializable

T1: READ  doctors where on_call=true  → SIREAD on {Alice, Bob}
T2: READ  doctors where on_call=true  → SIREAD on {Alice, Bob}

T1: WRITE Alice.on_call = false
    → adds edge T2 → T1  (T2 had SIREAD on Alice, T1 wrote it)

T2: WRITE Bob.on_call = false
    → adds edge T1 → T2  (T1 had SIREAD on Bob, T2 wrote it)

T1: COMMIT
    CheckCommit: T1 has outgoing edge T1→T2 AND incoming edge T2→T1 → cycle
    T1 aborted with ErrSSIConflict

T2: COMMIT
    CheckCommit: T2 has only the outgoing edge T2→T1 (T1 is gone)
    T2 commits successfully
```

## SSI vs 2PL Serializable

| Property | SSI | 2PL Serializable |
|---|---|---|
| Readers block writers | No | Yes (S lock held) |
| Writers block readers | No | Yes (X lock held) |
| Deadlocks possible | No | Yes |
| False aborts possible | Yes (rare) | No |
| Typical throughput | Higher | Lower |

SSI aborts more transactions than strictly necessary in some edge cases (the cycle heuristic is conservative), but eliminates deadlocks entirely for MVCC workloads.

## Reference

Cahill, M. J., Röhm, U., & Fekete, A. D. (2008). *Serializable isolation for snapshot databases*. SIGMOD.
