# Serializable Snapshot Isolation (SSI)

> **See also:** [ADR-0005 — SSI Serializable](../adr/0005-ssi-serializable.md) · [Write Skew scenario](../scenarios.md#write-skew) · [Isolation level reference](../deep-dives/isolation-levels.md)

## The anomaly SSI prevents: write skew

Snapshot isolation (MVCC Repeatable Read) prevents dirty reads, non-repeatable reads,
and phantoms. But it does not prevent **write skew**.

**Write skew** occurs when two transactions:
1. Each read an overlapping set of rows.
2. Each write _disjoint_ rows based on those reads.
3. The combined result violates a constraint that neither write alone would violate.

No write-write conflict exists (writes are to different keys), so snapshot isolation
allows both to commit — but a serial execution of the two transactions would have
prevented at least one from seeing the "bad" shared state.

### Classic example — doctors on call

```
Invariant: at least one doctor must be on call at all times.

T1: READ  on_call                   → {Alice: true, Bob: true}
T2: READ  on_call                   → {Alice: true, Bob: true}

T1: WRITE Alice.on_call = false     (Bob is still on call — OK locally)
T2: WRITE Bob.on_call   = false     (Alice is still on call — OK locally)

T1: COMMIT   ← no write-write conflict
T2: COMMIT   ← no write-write conflict

Result: 0 doctors on call — invariant violated
```

---

## Theoretical basis

Cahill, Röhm, and Fekete (2008) proved: every non-serializable execution under snapshot
isolation exhibits a **dangerous structure** — a cycle containing at least two consecutive
**rw-anti-dependency** edges.

An **rw-anti-dependency** edge `T1 → T2` means:
- T1 read a version of a row (or a predicate range).
- T2 wrote a _later_ version of that row/range after T1's snapshot.

In the doctors example:
- T1 reads Alice → T2 writes Alice → edge **T2 → T1** (T1 is the "earlier" reader, T2 is the later writer)
- T2 reads Bob  → T1 writes Bob  → edge **T1 → T2**

The two edges form a cycle: `T1 → T2 → T1`. SSI detects this at commit time and aborts
one of the transactions.

---

## Implementation: SSITracker

`internal/isolation/ssi.go` maintains two data structures per tracker:

```
siReads:  map[txnID] → set of keys read       // SIREAD "locks"
edges:    directed graph (txnID → txnID)       // rw-anti-dependency edges
```

### RecordRead

```
RecordRead(reader T, key k):
    siReads[T].add(k)

    // Check if any committed writer has already written k after T's snapshot.
    // If so, add an edge from that writer to T.
    for each committed txn W that wrote k:
        if W committed after T's snapshot:
            edges.add(W → T)
            if edges has cycle through T: abort T
```

### RecordWrite

```
RecordWrite(writer W, key k):
    // Find all readers with a SIREAD lock on k.
    for each txn R with k in siReads[R]:
        if R's snapshot predates W:
            edges.add(R → W)
            if edges has cycle through W: mark W as conflicted
```

### CheckCommit

```
CheckCommit(txn T):
    if T has at least one incoming rw-edge AND at least one outgoing rw-edge:
        // T sits in the middle of a cycle: X → T → Y
        abort T with ErrSSIConflict
```

---

## Annotated example — write skew detected

```
T1: BEGIN mvcc serializable
T2: BEGIN mvcc serializable
    (both take snapshots: Active=[T1,T2], Xmax=next)

T1: SCAN doctors                 → siReads[T1] = {Alice, Bob}
T2: SCAN doctors                 → siReads[T2] = {Alice, Bob}

T1: WRITE Alice.on_call = false
    RecordWrite(T1, "Alice"):
        T2 ∈ siReads["Alice"] → add edge T2 → T1

T2: WRITE Bob.on_call = false
    RecordWrite(T2, "Bob"):
        T1 ∈ siReads["Bob"]  → add edge T1 → T2

T1: COMMIT
    CheckCommit(T1):
        incoming edges: {T2 → T1} ✓
        outgoing edges: {T1 → T2} ✓
        cycle detected → ABORT T1 (ErrSSIConflict)

T2: COMMIT
    CheckCommit(T2):
        incoming edges: {T1 → T2} — but T1 is aborted, so no live cycle
        → COMMIT ✓

Final state: Alice = on_call, Bob = off_call (invariant satisfied)
Client retries T1; T1 now sees Bob = off_call → does not take Alice off call.
```

---

## SSI vs 2PL Serializable

| Property | SSI (MVCC Serializable) | 2PL Serializable |
|---|---|---|
| Readers block writers | **No** | Yes (S lock held) |
| Writers block readers | **No** | Yes (X lock held) |
| Deadlocks | **Not possible** | Possible |
| False aborts (safe but unnecessary) | Rare | None |
| Throughput under read-heavy load | Higher | Lower |
| Implementation complexity | Higher | Lower |

SSI is the approach used by PostgreSQL (since 9.1), CockroachDB, and YugabyteDB for
their Serializable isolation tier.

---

## Reference

Cahill, M. J., Röhm, U., & Fekete, A. D. (2008).
*Serializable isolation for snapshot databases.*
SIGMOD 2008. [PDF](https://courses.cs.washington.edu/courses/cse444/08au/544M/READING-LIST/fekete-sigmod2008.pdf)
