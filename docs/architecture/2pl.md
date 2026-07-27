# Two-Phase Locking (2PL)

> **See also:** [ADR-0002 — Strict 2PL & intention locks](../adr/0002-strict-2pl-intention-locks.md) · [Deadlock Detection](deadlock.md) · [MVCC vs 2PL](../deep-dives/mvcc-vs-2pl.md)

## What it is

Two-phase locking is a concurrency-control protocol that guarantees serializability
through one rule: **a transaction may not acquire new locks after it has released any lock**.

This splits every transaction's life into two strict phases:

```
Phase 1 — Growing    │ Phase 2 — Shrinking
─────────────────────┼──────────────────────
Acquire locks freely │ Release locks (no new acquires)
Read / write rows    │ (In strict 2PL: phase 2 begins only at commit/abort)
```

This implementation uses **strict 2PL**: all locks are held until commit or abort.
Strict 2PL prevents cascading aborts — no other transaction can observe a dirty write
because the X lock is held until the writer commits or rolls back.
See [ADR-0002](../adr/0002-strict-2pl-intention-locks.md) for the full rationale.

---

## Lock modes and compatibility

Five modes form the standard **intention lock hierarchy**:

| Mode | Full name | Meaning |
|---|---|---|
| **IS** | Intent Shared | Plans to acquire S on a child resource |
| **IX** | Intent Exclusive | Plans to acquire X on a child resource |
| **S** | Shared | Read lock on this resource |
| **SIX** | Shared + Intent Exclusive | Holds S on this resource; plans X on children |
| **X** | Exclusive | Write lock; incompatible with everything |

**Compatibility matrix** (`✓` = compatible, `✗` = conflict):

| | IS | IX | S | SIX | X |
|---|---|---|---|---|---|
| **IS** | ✓ | ✓ | ✓ | ✓ | ✗ |
| **IX** | ✓ | ✓ | ✗ | ✗ | ✗ |
| **S** | ✓ | ✗ | ✓ | ✗ | ✗ |
| **SIX** | ✓ | ✗ | ✗ | ✗ | ✗ |
| **X** | ✗ | ✗ | ✗ | ✗ | ✗ |

**How intention locks help:** A row write requires `IX(table) + X(row)`. A row read
requires `IS(table) + S(row)`. Two readers on the same table hold `IS + IS` on the table
(compatible) without serializing — only the row-level locks need to be checked against
each other. Without intention locks, every row write would require an `X` on the entire
table, killing parallelism.

---

## Lock acquisition algorithm

```
LockTable.Acquire(txnID, resource, mode):

  queue = getOrCreateQueue(resource)   // sync.Map lookup

  queue.mu.Lock()

  if compatible(mode, all_granted_modes):
      add entry{txnID, mode} to granted set
      add resource to txn.heldLocks
      queue.mu.Unlock()
      return nil                       // fast path: no contention

  // Conflict — enqueue and block
  ch = make(chan struct{}, 1)
  add entry{txnID, mode, ch} to waiting list  // FIFO position preserved
  wfg.AddEdge(txnID, each_conflicting_holder)
  queue.mu.Unlock()

  select {
  case <-ch:            // woken by a releasing transaction
      check for abort signal (deadlock victim)
      return nil
  case <-lockTimeoutCh:
      dequeue self; return ErrLockTimeout
  case <-txnCtx.Done():
      dequeue self; return ErrTxnAborted
  }
```

The queue is **FIFO within a mode class** — a waiting X lock cannot be bypassed by a
later S lock, preventing writer starvation.

---

## Lock upgrade

A transaction that holds `S` on a resource and then needs `X` calls `Upgrade`:

```
Upgrade(txnID, resource):
    Release(S on resource)
    Acquire(X on resource)   // may block if another S-holder is present
```

Upgrade is a deadlock-risk point: if T1 and T2 both hold S and both try to upgrade,
each waits for the other's S to release — a cycle. The deadlock detector resolves this
by aborting the younger transaction.

---

## Held-set tracking

Each `Transaction` keeps a `heldLocks []ResourceID` list.

At commit/abort, `LockTable.ReleaseAll(txnID)`:

1. Iterates `heldLocks` — O(locks held), not O(total queues).
2. For each queue: removes txn from the granted set, wakes the first compatible waiter.
3. Updates the wait-for graph (`RemoveNode(txnID)`).

Without this set, releasing all locks would require a full scan of every queue in the
lock table — prohibitively expensive at scale.

---

## Gap locking for phantom prevention

At `Serializable` isolation, a scan must prevent _phantoms_ — rows inserted by a
concurrent transaction in the scan's key range that would appear on a re-scan.

2PL prevents this with **gap locks**: range locks on the key space between existing rows.

```
Scan accounts WHERE balance > 1000:

  T1 holds:
    IS(table:accounts)               // intention lock on the table
    S(row:accounts:3)                // locks existing match
    S(row:accounts:7)                // locks existing match
    S(gap:accounts:[3..7])           // gap lock: no inserts in this range
    S(gap:accounts:[7..∞])           // gap lock: no inserts above 7
```

A concurrent insert into the gap blocks on the gap lock. See [ADR-0006](../adr/0006-gap-locking.md).

---

## What 2PL prevents at each isolation level

| Isolation level | Dirty read | Non-rep read | Phantom | Lost update | Write skew |
|---|---|---|---|---|---|
| Read Uncommitted | ✗ possible | ✗ | ✗ | ✗ | ✗ |
| Read Committed | ✅ | ✗ | ✗ | ✗ | ✗ |
| Repeatable Read | ✅ | ✅ | ✗ (without gap locks) | ✅ | ✗ |
| Serializable | ✅ | ✅ | ✅ | ✅ | ✅ |

---

## Annotated example — lost update under 2PL

```
T1: BEGIN 2PL REPEATABLE_READ
T2: BEGIN 2PL REPEATABLE_READ

T1: READ accounts "1"
    → IS(table:accounts) granted
    → S(row:accounts:1) granted   balance = 100

T2: READ accounts "1"
    → IS(table:accounts) granted  (compatible with T1's IS)
    → S(row:accounts:1) granted   (compatible with T1's S)
                                  balance = 100

T1: WRITE accounts "1" val=110
    → IX(table:accounts) upgrade: IS→IX granted (compatible with T2's IS)
    → X(row:accounts:1): BLOCKED  T2 holds S → wait-for edge T1→T2

T2: WRITE accounts "1" val=150
    → IX(table:accounts): blocked? No — T1 holds IX (compatible)
    → X(row:accounts:1): BLOCKED  T1 holds S → wait-for edge T2→T1

    Deadlock detector fires (cycle: T1→T2→T1)
    Victim = T2 (highest ID = youngest)
    T2 aborted → releases S(row:accounts:1)

T1: unblocked → acquires X(row:accounts:1) → writes 110 → COMMIT
    Final balance: 110  (T2's +50 lost — client must retry T2)
```
