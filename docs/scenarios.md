# Anomaly Scenarios

The server ships 7 built-in scenarios that reproduce classic concurrency anomalies. Each scenario runs two or more transactions through a predetermined interleaving and returns a step-by-step trace of what happened.

Run any scenario via the API:

```bash
curl -X POST http://localhost:8080/api/scenarios/write_skew/run \
  -H 'X-Admin-Token: <token>' | jq .
```

Or use the React UI's **Scenarios** tab.

---

## Summary table

| Scenario name | Anomaly demonstrated | Occurs at | Prevented at |
|---|---|---|---|
| `dirty_read` | Dirty Read | Read Uncommitted | Read Committed+ |
| `lost_update` | Lost Update | Read Committed (MVCC) | Repeatable Read+ |
| `non_repeatable_read` | Non-Repeatable Read | Read Committed | Repeatable Read+ |
| `phantom_read` | Phantom Read | Read Committed | Repeatable Read+ (snapshot) |
| `write_skew` | Write Skew | Repeatable Read | Serializable (SSI) |
| `deadlock_cycle` | Deadlock | Any (2PL) | N/A — victim is chosen |
| `cascade_abort` | Cascade Abort | Read Uncommitted | Read Committed+ |

---

## Dirty Read

**Protocol:** 2PL or MVCC  
**Isolation level where it occurs:** Read Uncommitted

Transaction T2 reads a row written by T1 **before T1 commits**. If T1 later aborts, T2 has read data that never existed.

```
T1: BEGIN  isolation=read_uncommitted
T2: BEGIN  isolation=read_uncommitted

T1: WRITE accounts key="1" val=500   # balance was 100, now "500" (uncommitted)
T2: READ  accounts key="1"           # sees 500 — a dirty read
T1: ABORT                             # rolls back to 100
T2: READ  accounts key="1"           # now sees 100 — reality changed under T2
```

**Prevented by:** Read Committed (S locks released after statement, or MVCC snapshot excludes uncommitted versions).

---

## Lost Update

**Protocol:** MVCC  
**Isolation level where it occurs:** Read Committed

Two transactions read the same balance, each increment it, and each write back. The second write silently overwrites the first — one update is lost.

```
T1: BEGIN  mvcc isolation=read_committed
T2: BEGIN  mvcc isolation=read_committed

T1: READ accounts key="1"   → balance=100
T2: READ accounts key="1"   → balance=100

T1: WRITE accounts key="1" val=110  (100 + 10)
T2: WRITE accounts key="1" val=150  (100 + 50)  ← overwrites T1's write

T1: COMMIT  → balance=110 durably
T2: COMMIT  → balance=150  (T1's +10 is lost)
```

**Prevented by:** Repeatable Read (MVCC write-conflict check detects that T2's base version was modified by a committed T1 that was active at T2's snapshot time → `ErrWriteConflict`).

---

## Non-Repeatable Read

**Protocol:** MVCC  
**Isolation level where it occurs:** Read Committed

A transaction reads the same row twice and gets different values because another transaction committed a write between the two reads.

```
T1: BEGIN  mvcc isolation=read_committed
T2: BEGIN  mvcc isolation=read_committed

T1: READ accounts key="1"   → 100
T2: WRITE accounts key="1" val=200
T2: COMMIT

T1: READ accounts key="1"   → 200   ← different value!
```

**Prevented by:** Repeatable Read (snapshot frozen at BEGIN; T1 always sees the version from when it started).

---

## Phantom Read

**Protocol:** MVCC  
**Isolation level where it occurs:** Read Committed

A transaction scans a table twice; a second transaction inserts a row between the scans, producing a "phantom" row on the second scan.

```
T1: BEGIN  mvcc isolation=read_committed
T2: BEGIN  mvcc isolation=read_committed

T1: SCAN accounts   → [key=1: 100, key=2: 200]
T2: INSERT accounts key=3 val=300
T2: COMMIT

T1: SCAN accounts   → [key=1: 100, key=2: 200, key=3: 300]  ← phantom!
```

**Prevented by:** Repeatable Read with MVCC (snapshot excludes T2's insert since T2 committed after T1's snapshot was taken).

---

## Write Skew

**Protocol:** MVCC  
**Isolation level where it occurs:** Repeatable Read

Two transactions each read a shared condition, make disjoint writes, and together violate a constraint that neither write alone would violate.

```
T1: BEGIN  mvcc isolation=repeatable_read
T2: BEGIN  mvcc isolation=repeatable_read

# Constraint: at least one doctor must be on call
T1: READ doctors   → Alice=on_call, Bob=on_call
T2: READ doctors   → Alice=on_call, Bob=on_call

T1: WRITE Alice.on_call = false   (Bob is still on call — constraint OK locally)
T2: WRITE Bob.on_call  = false    (Alice is still on call — constraint OK locally)

T1: COMMIT  → OK
T2: COMMIT  → OK   ← but now no one is on call!
```

**Prevented by:** Serializable (SSI detects the rw-anti-dependency cycle T1→T2→T1 and aborts one transaction at commit time).

---

## Deadlock Cycle

**Protocol:** 2PL  
**Isolation level:** any

Two transactions each hold a lock the other needs, forming a cycle in the wait-for graph. The deadlock detector (50 ms DFS) aborts the youngest transaction.

```
T1: BEGIN 2PL
T2: BEGIN 2PL

T1: WRITE accounts key="1"   → acquires X(row:accounts:1)
T2: WRITE accounts key="2"   → acquires X(row:accounts:2)

T1: WRITE accounts key="2"   → blocked, waits for T2 (edge T1→T2)
T2: WRITE accounts key="1"   → blocked, waits for T1 (edge T2→T1)

Deadlock detector: cycle [T1, T2] → abort T2 (youngest)

T1: unblocked → acquires X(row:accounts:2) → commits
T2: receives ErrDeadlock on next call
```

---

## Cascade Abort

**Protocol:** 2PL or MVCC  
**Isolation level where it occurs:** Read Uncommitted

T2 reads T1's uncommitted write. T1 then aborts. T2's data is now garbage — it has read a value that never committed, and may propagate the error further.

With **strict 2PL** (the implementation used here), cascade aborts are prevented at Read Committed and above: X locks are held until commit, so no other transaction can read uncommitted writes. The scenario demonstrates what happens at Read Uncommitted when the protection is removed.
