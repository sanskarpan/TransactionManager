# Isolation Level Reference

This page gives formal definitions, the SQL standard classifications, and a
protocol-by-protocol breakdown of what each level actually prevents in this system.

---

## The SQL standard levels

The ANSI SQL-92 standard defines four isolation levels based on which of three
phenomena they permit:

| Level | Dirty Read | Non-Repeatable Read | Phantom Read |
|---|---|---|---|
| Read Uncommitted | Possible | Possible | Possible |
| Read Committed | Not possible | Possible | Possible |
| Repeatable Read | Not possible | Not possible | Possible |
| Serializable | Not possible | Not possible | Not possible |

These definitions are **weaker than you might expect**: they describe the SQL-92
committee's view of 1992-era pessimistic locking systems. MVCC-based systems (and
this one) can prevent additional anomalies (like phantoms) at levels lower than
Serializable, simply because snapshots are "free" — but the standard does not
require it.

---

## Beyond SQL-92: the extended anomaly taxonomy

Berenson et al. (1995) and Fekete et al. (2005) extended the taxonomy to cover
anomalies the standard misses:

| Anomaly | SQL-92 name? | Prevented by |
|---|---|---|
| Dirty Read | Yes | Read Committed+ |
| Dirty Write | Implied | Any 2PL (strict) |
| Non-Repeatable Read | Yes | Repeatable Read+ |
| Phantom Read | Yes | Serializable (SQL-92) |
| Lost Update | No | Repeatable Read+ |
| Read Skew | No | Repeatable Read+ |
| Write Skew | No | Serializable only |
| Phantom Write (insert gap) | No | Serializable (gap locks) |

---

## Level-by-level breakdown in this system

### Read Uncommitted

The most permissive level. Transactions can read data written by uncommitted
transactions.

**What can happen:**
- Dirty reads — T2 reads T1's write; T1 aborts; T2 has read phantom data.
- Cascade aborts — T2 acts on T1's write; T1 aborts; T2 must also abort.
- All other anomalies.

**2PL implementation:** S locks are not acquired on reads. The reader sees whatever is
currently in the row, regardless of the writer's commit state.

**MVCC implementation:** The visibility predicate skips the `IsCommitted(XMin)` check.
Reads return the newest version unconditionally.

**API:** `{"isolation": "read_uncommitted"}`

---

### Read Committed

The default in most real-world databases (PostgreSQL, Oracle, SQL Server default).
Prevents dirty reads by ensuring reads only see committed data.

**What can happen:**
- Non-repeatable reads — T1 reads "balance=100"; T2 commits balance=200; T1 re-reads
  and sees 200.
- Phantom reads — T1 scans for rows matching a condition; T2 inserts a matching row
  and commits; T1 re-scans and sees a new row.
- Lost updates (MVCC) — T1 and T2 both read balance=100, both increment, one's
  increment is silently lost.

**2PL implementation:** S locks are acquired for the duration of each statement and
released after the statement completes. X locks are held until commit.

**MVCC implementation:** A new snapshot is taken before each statement. The snapshot
excludes all uncommitted writers (`Active` set and `Xmax` updated per statement).

**API:** `{"isolation": "read_committed"}`

---

### Repeatable Read

Prevents non-repeatable reads — re-reading the same row always returns the same result
within a transaction.

**What can happen (SQL-92):**
- Phantom reads (reads of _ranges_ can see new rows from concurrent inserts).

**What MVCC additionally prevents (beyond SQL-92):**
- Phantoms — because the snapshot is frozen at `BEGIN`, a concurrent insert is
  invisible (it has `XMin > snap.Xmax` or is in `Active`).

**What 2PL additionally prevents:**
- Lost updates — the X-lock upgrade on the written row blocks concurrent writers.

**What still happens:**
- Write skew — two transactions read overlapping sets and write disjoint rows.

**2PL implementation:** S locks are held for the entire transaction duration. X locks
on write. No gap locks yet (gap locks only at Serializable).

**MVCC implementation:** Snapshot frozen at `BEGIN`. Write-conflict check on writes.

**API:** `{"isolation": "repeatable_read"}`

---

### Serializable

The strongest level. Every committed execution is equivalent to some serial ordering
of the transactions — as if they ran one at a time.

**Nothing bad can happen.** All anomalies are prevented.

**2PL implementation:** All of Repeatable Read, plus gap locks on range scans to
prevent phantom writes. The gap locking scheme uses lexicographically-ordered key
ranges. See [ADR-0006](../adr/0006-gap-locking.md).

**MVCC implementation:** All of Repeatable Read, plus SSI commit-time check.
The `SSITracker` detects dangerous rw-anti-dependency cycles. See
[SSI](../architecture/ssi.md).

**API:** `{"isolation": "serializable"}`

---

## Comparison across protocols

| Anomaly | 2PL RC | 2PL RR | 2PL SER | MVCC RC | MVCC RR | MVCC SER |
|---|---|---|---|---|---|---|
| Dirty Read | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Non-Repeatable Read | ✗ | ✅ | ✅ | ✗ | ✅ | ✅ |
| Phantom Read | ✗ | ✗ | ✅ | ✗ | **✅** | ✅ |
| Lost Update | ✗ | ✅ | ✅ | ✗ | ✅ | ✅ |
| Write Skew | ✗ | ✗ | ✅ | ✗ | ✗ | ✅ |
| Deadlock | — | — | — | N/A | N/A | N/A |

!!! note "MVCC RR phantom prevention"
    MVCC Repeatable Read prevents phantoms "accidentally" — the snapshot frozen at
    BEGIN excludes any rows inserted by concurrent transactions. The SQL-92 standard
    does not require this at RR; MVCC simply provides it for free.

---

## Choosing an isolation level

```
Need to prevent write skew?          → Serializable (MVCC) or Serializable (2PL)
Need phantom-free scans cheaply?     → MVCC Repeatable Read
Mostly reads, high throughput?       → MVCC Read Committed (or RR for consistency)
Want to observe deadlocks?           → 2PL (any level)
Want to observe write skew + SSI?    → MVCC Serializable
Teaching dirty reads?                → Read Uncommitted (either protocol)
```

---

## References

- Berenson, H. et al. (1995). *A critique of ANSI SQL isolation levels*. SIGMOD 1995.
- Fekete, A. et al. (2005). *Making snapshot isolation serializable*. ACM TODS 30(2).
- Cahill, M. J. et al. (2008). *Serializable isolation for snapshot databases*. SIGMOD 2008.
