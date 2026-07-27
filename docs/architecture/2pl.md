# Two-Phase Locking (2PL)

## What it is

Two-phase locking is a concurrency-control protocol that guarantees serializability by enforcing a simple rule: **a transaction may not acquire new locks after it has released any lock**.

This implementation uses **strict 2PL** — all locks are held until commit or abort, which prevents cascading aborts (dirty reads of uncommitted writes that later roll back). See [ADR-0002](../adr/0002-strict-2pl-intention-locks.md) for the design rationale.

## Lock modes

Five modes form a compatibility matrix:

| Mode | Meaning | Compatible with |
|---|---|---|
| **IS** | Intent Shared — intends to acquire S on a child | IS, IX, S |
| **IX** | Intent Exclusive — intends to acquire X on a child | IS, IX |
| **S** | Shared read | IS, S |
| **SIX** | Shared + Intent Exclusive | IS |
| **X** | Exclusive write | *(nothing)* |

Intention modes (IS/IX) are used on **table** resources; S/X on **row** resources. This lets a row write (IX on table + X on row) coexist with reads of other rows (IS on table + S on their rows) without serializing on a single table-level lock.

## Lock acquisition

```
LockTable.Acquire(txnID, resource, mode)
    │
    ├─ load or create LockQueue for resource (sync.Map)
    │
    ├─ check compatibility with currently-granted locks
    │   ├─ compatible → add to granted set, return immediately
    │   └─ incompatible → enqueue in waiting list
    │                    block on channel (with timeout)
    │
    └─ on grant: move from waiting to granted
```

The queue is **FIFO within a mode class**: a waiting S lock cannot jump ahead of a waiting X lock, preventing writer starvation.

## Lock upgrade

A transaction that holds S and needs X calls `Upgrade(txnID, resource)`:

```
Upgrade = Release(S) → Acquire(X)
```

If another S-holder is present, the upgrade waits until all other S-holders release. This is a potential deadlock point (two transactions each trying to upgrade simultaneously), which the deadlock detector resolves.

## Held-set tracking

Each transaction maintains a `heldLocks []ResourceID` set. At commit/abort, `LockTable.ReleaseAll(txnID)` iterates this set and wakes blocked waiters on each queue. Without the held set, we'd have to scan every queue in the lock table — O(total queues) instead of O(locks held by this txn).

## Deadlock

With 2PL, **deadlocks are possible** whenever two transactions wait for each other's locks in a cycle. A 50 ms background goroutine ([Deadlock Detection](deadlock.md)) runs DFS over the wait-for graph and aborts the youngest transaction in the cycle.

## What 2PL prevents

| Anomaly | Prevented? |
|---|---|
| Dirty read | ✅ (strict 2PL: X held until commit) |
| Non-repeatable read | ✅ (S held until commit) |
| Phantom read | ✅ with gap locks (see [ADR-0006](../adr/0006-gap-locking.md)) |
| Lost update | ✅ (X before write, upgrade from S) |
| Write skew | ✅ at Serializable (SIX on predicate scans) |
| Deadlock | ❌ possible — detected and resolved |

## Example: lost update under 2PL

```
T1: BEGIN 2PL REPEATABLE_READ
T2: BEGIN 2PL REPEATABLE_READ

T1: READ accounts key="1"  → acquires S(row:accounts:1), reads balance=100
T2: READ accounts key="1"  → acquires S(row:accounts:1), reads balance=100

T1: WRITE accounts key="1" val=150 → tries X(row:accounts:1)
    T2 holds S → T1 blocks, enters wait-for graph

T2: WRITE accounts key="1" val=200 → tries X(row:accounts:1)
    T1 holds S → T2 blocks

    Deadlock detector fires:
    cycle = [T1 → T2 → T1]
    victim = T2 (youngest)
    T2 aborted with ErrDeadlock

T1: unblocked → acquires X → writes 150 → commits
```
