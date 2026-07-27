# MVCC vs 2PL — When Readers Block Writers (and When They Don't)

The central trade-off in concurrency control is **throughput vs simplicity**.
Two-phase locking is easy to reason about but readers block writers and vice versa.
MVCC decouples them — but introduces its own costs.

---

## The fundamental difference

| Question | 2PL answer | MVCC answer |
|---|---|---|
| Can a reader block a writer? | **Yes** (S lock conflicts with X) | **No** (reader sees an old version) |
| Can a writer block a reader? | **Yes** (X lock conflicts with S) | **No** (reader already has its snapshot) |
| Can a writer block a writer? | **Yes** (X lock conflicts with X) | **Yes** (write-conflict check) |
| Can deadlocks form? | **Yes** | **No** (no waiting on locks) |
| Do readers see "stale" data? | No — always latest committed | **Yes** — snapshot from `BEGIN` |

---

## How readers are handled

### 2PL

A reader acquires an `S` lock. If a writer holds `X`, the reader **blocks** — it sits
in the wait queue until the writer commits or aborts. This is direct reader–writer
contention.

```
T_writer: WRITE "balance"=500   → holds X(row:balance)
T_reader: READ  "balance"       → waits for X to release
```

### MVCC

A reader takes a snapshot at `BEGIN` (Repeatable Read+) or at the statement (RC).
When it reads "balance", it traverses the version chain and picks the version that
was committed before its snapshot — regardless of what the current writer is doing.
The writer's new version has `XMin = writer's ID`, which is not yet committed from
the reader's perspective.

```
T_writer: WRITE "balance"=500   → prepends Version{XMin=7, XMax=0, data=500}
T_reader: READ  "balance"       →          sees Version{XMin=3, XMax=0, data=100}
                                           (XMin=3 is committed before snap)
```

No blocking. The reader does not even know the writer exists.

---

## How writers are handled

### 2PL

Writers serialise through `X` locks. If T1 holds `X(row:A)` and T2 wants it, T2
blocks. If both also hold each other's resources, a deadlock forms.

### MVCC

Writers check for _write-write conflicts_ before prepending a new version. The check
is lock-free (no queue, no waiting): if the check fails, the writer gets
`ErrWriteConflict` immediately and must retry. There is no "wait until the other
writer commits" — writers either proceed or fail fast.

```
T1: WRITE "balance"=500   → Version{XMin=T1, XMax=0}
T2: WRITE "balance"=600   → CheckWriteConflict: T1 is active, conflict!
                            → ErrWriteConflict (fail fast, retry)
```

This means **MVCC write contention produces retries, not deadlocks**. Under high
write contention on the _same_ key, MVCC can actually have lower throughput than 2PL
because of repeated retries — 2PL at least queues writers and drains them orderly.

---

## Snapshot staleness

MVCC readers at Repeatable Read see data as of their `BEGIN` time, which can be
arbitrarily old. In a long-running analytics query, the data it reads could be
minutes or hours stale.

2PL readers always see the latest committed data (current view). There is no staleness.

This is why long OLAP queries on MVCC systems do not interfere with OLTP writers —
but it also means the query is reading historical data.

---

## Phantom reads

| Protocol | Prevents phantoms at RR? |
|---|---|
| 2PL | **No** — requires gap locks (and only at Serializable in most systems) |
| MVCC | **Yes** — the snapshot excludes rows inserted after `BEGIN` |

MVCC snapshot isolation gives phantom prevention "for free" at Repeatable Read without
any gap locks. 2PL needs explicit range/gap locking to achieve the same.

---

## Write skew

Both protocols are vulnerable to write skew at Repeatable Read. Neither can prevent
it without additional machinery:

- **2PL**: requires predicate locking (lock the entire predicate result set). Coarse
  and expensive — most systems approximate with range/gap locks.
- **MVCC**: requires SSI (rw-anti-dependency tracking). SSI has lower overhead than
  predicate locks but can produce false aborts (safe but unnecessary).

This project implements SSI for MVCC Serializable and gap locking for 2PL Serializable.

---

## When to use which

| Scenario | Prefer |
|---|---|
| Read-heavy OLTP | **MVCC** — reads never block |
| Write-heavy on same rows | **2PL** — orderly queue beats retry storm |
| Long-running analytics with concurrent writes | **MVCC** — reader isolation via snapshot |
| Teaching deadlock detection | **2PL** — MVCC cannot produce lock-based deadlocks |
| Teaching write skew / SSI | **MVCC Serializable** — SSI is more interesting than predicate locks |
| Maximum throughput under low contention | **MVCC** — no lock overhead on reads |

---

## Summary

```
                   2PL                         MVCC
                   ───                         ────
Reader–writer    Blocking (S vs X)          Non-blocking (snapshot)
Writer–writer    Blocking (X vs X)          Fail-fast (conflict check)
Deadlocks        Possible                   Not possible
Staleness        None (current view)        Yes (snapshot age)
Phantom free     Only with gap locks        Free at RR+
Write-skew free  Only with predicate locks  Only with SSI
```
