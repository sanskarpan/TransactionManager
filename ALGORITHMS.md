# Algorithms Reference

Formal treatment of every algorithm in this project: what it does, why it works, what can go wrong, and how to verify it. Read this when you need to understand *why* the implementation is structured the way it is.

---

## 1. Two-Phase Locking (2PL)

### The Core Theorem

**Theorem (2PL Correctness):** Any schedule produced by strict 2PL is conflict-serializable.

A schedule is conflict-serializable if there exists a serial schedule that produces the same result for all conflicting operation pairs. Two operations conflict if they access the same data item and at least one is a write.

### Why Strict (Rigorous) 2PL

There are four variants:

| Variant | Growing Phase | Shrinking Phase | Properties |
|---|---|---|---|
| Basic 2PL | Acquire freely | Release freely | Serializable, but cascading aborts possible |
| Strict 2PL | Acquire freely | Release X locks only at end | Prevents cascading aborts |
| Rigorous 2PL | Acquire freely | Release ALL locks at end | Strictest, no cascading aborts, simpler recovery |
| Conservative 2PL | Acquire ALL locks upfront | Release freely | No deadlocks, low concurrency |

We implement **Rigorous 2PL**: hold all locks (S and X) until commit or abort. This means:
- No cascading aborts (dirty data never read by committed transactions)
- Simpler: release is a single atomic operation at commit time
- Cost: lower concurrency than basic 2PL (hold S locks longer)

### Intention Locks (IS, IX, SIX)

Intention locks allow efficient table-level conflict detection without scanning all row locks.

**Protocol:** Before acquiring a lock on a row, acquire the corresponding intention lock on the table:
- Read row → acquire IS on table, then S on row
- Write row → acquire IX on table, then X on row
- Read all rows for write (table scan + update all) → acquire SIX on table

This means "if T1 holds IX on table T and T2 wants to scan T with S lock, T2 must acquire S on T — but S and IX are incompatible → T2 waits for T1's writes to complete before scanning."

Without intention locks: to check if a table-level S lock conflicts with any row-level X lock, we'd scan every row lock in the table — O(n) per lock acquisition. Intention locks make this O(1).

### Update Lock (U)

The U lock solves a classic deadlock scenario:

```
Without U lock:
T1: S(row) → wants X(row) [upgrade]
T2: S(row) → wants X(row) [upgrade]
Both hold S, both want X, neither can upgrade → deadlock

With U lock:
T1: U(row)  → upgrade to X when ready
T2: tries U(row) → U+U incompatible → T2 waits
T1: upgrades U→X, writes, releases → T2 then gets U
```

U is compatible with S (multiple readers can read while one is "planning to write") but U is not compatible with U (at most one planner). This prevents the symmetric deadlock.

### Lock Upgrade (S→X, U→X)

Lock upgrade must be inserted at the **front** of the wait queue (converting request gets priority) to prevent starvation:

```
Wait queue before upgrade: [X(T3), S(T4)]
T1 holds S, wants X:
  → Without priority: T1 goes to back → must wait for T3 and T4 first → slow
  → With priority: T1 converting request goes to front → granted first when other S holders release
```

**Deadlock during upgrade:** If T1 (S) and T2 (S) both want to upgrade to X simultaneously:
- T1's conversion request is at front of queue
- T2's conversion request is also at front (two converting requests)
- Neither can be granted while the other holds S
- **Deadlock between two upgraders**

Detection: if two Converting requests are at the front and both are blocked by each other, abort one (e.g., T2 = younger). The wait-for graph will show T1 ↔ T2 cycle and the deadlock detector handles it.

---

## 2. MVCC Snapshot Construction

### The Snapshot Triple (Xmin, Xmax, Active)

A snapshot represents the committed state of the database at a specific instant. The triple `(Xmin, Xmax, Active)` is captured atomically (under txnManager lock):

```
Xmin  = min(ID of all currently active transactions)
Xmax  = nextTxnID (the ID that would be assigned to the NEXT transaction)
Active = {all transaction IDs that are currently active}
```

**Why this works:** At snapshot time:
- All transactions with ID < Xmin are committed (they finished before the oldest active txn started)
- All transactions with ID ≥ Xmax haven't started yet (they don't exist)
- Transactions in Active are running and their writes are not yet committed

**The visibility predicate** (for version V, transaction T with snapshot S):
```
xmin_visible = (V.XMin < S.Xmin)                    -- committed before all active txns
             ∨ (V.XMin ∈ [S.Xmin, S.Xmax)           -- committed during window
                ∧ committed(V.XMin)
                ∧ V.XMin ∉ S.Active)

self_created = (V.XMin == T.ID)                      -- T wrote this version

xmax_invisible = (V.XMax == 0)                       -- not deleted
               ∨ (V.XMax == T.ID)                   -- deleted by us, we see it as gone
               ∨ (V.XMax ≥ S.Xmax)                  -- deleter not yet started
               ∨ (V.XMax ∈ S.Active)                -- deleter active = deletion uncommitted
               ∨ aborted(V.XMax)                     -- deleter aborted = deletion rolled back

visible = (self_created ∨ xmin_visible) ∧ xmax_invisible
```

**Read Committed vs Repeatable Read:**
- READ COMMITTED: refresh snapshot at every statement (re-compute Xmin, Xmax, Active before each read/scan)
- REPEATABLE READ: take snapshot once at BEGIN, use it for every operation throughout the transaction

### Version Chain Layout

```
Chain for row "accounts:1":

HEAD → [V3: XMin=42, XMax=0, balance=950]
         |
       [V2: XMin=30, XMax=42, balance=1000]
         |
       [V1: XMin=10, XMax=30, balance=900]
```

- V3 is the current live version (XMax=0)
- V2 was created by T30, deleted (superseded) by T42
- V1 was created by T10, deleted by T30

A transaction with snapshot Xmin=25, Xmax=45, Active=[30]:
- V3: XMin=42 is in [25,45) and Active? No (42 not in Active=[30]). Committed? Need to check. If T42 committed → V3 xmin_visible. XMax=0 → xmax_invisible. V3 is visible.
- V2: XMin=30 is in Active=[30] → xmin NOT visible. V2 not visible.
- V1: XMin=10 < 25 → xmin_visible. XMax=30 ∈ Active=[30] → xmax_invisible (deleter still running). V1 IS visible!

So this transaction sees V1 (balance=900), not V3 (balance=950). Correct: T30's update was running when we took our snapshot and isn't committed from our perspective.

### Write-Write Conflict (First-Writer-Wins)

When T2 tries to update row R that T1 has already updated (while T1 is still active):

```
Version chain: [V2: XMin=T1.ID, XMax=0] → [V1: XMin=prev, XMax=T1.ID]

T2's read: finds V1 visible (T1 is in Active when T2 took snapshot)
T2's write: tries to set V1.XMax = T2.ID (to delete V1 and create V3)
  → But V2 exists with XMin=T1.ID (active) → WriteConflict!

Alternative detection:
T2's write: tries to update, finds that the "visible" version is V1 (XMax=T1.ID, but T1 active)
  → The XMax itself signals that T1 is updating this row
  → First-writer-wins: T2 must abort (or wait if we implement wait-on-conflict)
```

**Implementation choice:** In PostgreSQL, the second writer blocks until the first commits or aborts. If the first commits, the second gets a serialization failure (at RR) or proceeds to re-read and try again (at RC). We implement the simpler "abort immediately on write-write conflict" which is correct but less live (lower throughput under write contention).

---

## 3. Deadlock Detection: DFS on Wait-For Graph

### Why DFS (not Floyd's algorithm)

Floyd's cycle detection (tortoise and hare) works on linked lists but not directed graphs. For the wait-for graph we use DFS with three-color marking, which:
- Detects any cycle in O(V + E)
- Identifies the specific nodes in the cycle (for victim selection and reporting)
- Works incrementally (each call to FindCycle scans the entire current graph)

### Why Periodic (50ms) Instead of On-Demand

On-demand detection: check for cycles every time an edge is added. Cost: O(V+E) per lock acquisition. With 1000 concurrent transactions and 10 locks each, that's 10,000 cycle checks per second, each O(1000+10000) = O(11000) → 110M operations/second just for deadlock detection. Too expensive.

Periodic detection: run DFS every 50ms regardless of lock activity. Cost: at most 20 × O(V+E) per second. With our workload, V ≈ 10-100 and E ≈ V, so O(V+E) ≈ O(100-1000) operations → 20,000-200,000 operations/second. Negligible.

The 50ms latency before deadlock is detected is acceptable. Transactions are already blocked waiting for locks — 50ms additional wait doesn't significantly change user experience.

### Victim Selection Strategies

We use **youngest transaction** (highest TxnID) as victim because:
- Youngest has done the least work (fewest operations since BEGIN)
- Deterministic: same cycle always produces same victim (no oscillation)
- Easy to implement: just take max(cycle IDs)

Alternative strategies:
- **Fewest locks held:** minimize wasted work in terms of locks to release
- **Smallest undo log:** minimize I/O on abort (in disk-based systems)
- **User-specified priority:** explicit transaction priority for important transactions

---

## 4. Deadlock Prevention: Wait-Die vs Wound-Wait

Both strategies assign priority by transaction age (older = higher priority = lower TxnID). Neither produces deadlocks.

### Wait-Die

"Older transactions wait; younger transactions die (abort themselves)."

```
T_req requests lock held by T_hold:
  if T_req.ID < T_hold.ID:  // T_req is older → lower ID → higher priority
    WAIT (older can wait for younger)
  else:
    DIE (younger aborts itself rather than wait for older)
```

**No deadlock proof:** A deadlock requires a cycle T1 → T2 → ... → T1 in the wait-for graph. Under Wait-Die, T_i waits for T_j only if T_i.ID < T_j.ID (i.e., T_i is older). This means edges in the wait-for graph always go from lower-ID to higher-ID. A directed graph where all edges go in the same direction (lower to higher) can never have a cycle. QED.

**Livelock concern:** A young transaction T_young may be repeatedly aborted and restarted (always dying when it conflicts with older transactions). Solution: keep T_young's original timestamp across restarts so it gets progressively older and eventually waits instead of dying.

### Wound-Wait

"Older transactions preempt (wound) younger; younger transactions wait for older."

```
T_req requests lock held by T_hold:
  if T_req.ID < T_hold.ID:  // T_req is older
    WOUND T_hold (abort T_hold, the younger one)
  else:
    WAIT (younger waits for older)
```

**No deadlock proof:** T_i waits for T_j only if T_i.ID > T_j.ID (T_i is younger, waiting for older). Edges go from higher-ID to lower-ID. Again, all edges go in the same direction → no cycles.

**Comparison with Wait-Die:**
- Wait-Die is **non-preemptive**: young transactions abort themselves when they would cause a wait that threatens deadlock. Older transactions are never disturbed.
- Wound-Wait is **preemptive**: older transactions abort younger holders. Better for long-running important transactions (they always proceed). Worse for young transactions (they can be wounded even if they would finish before the older one).
- Wait-Die generates more aborts (every young-conflicts-old is an abort). Wound-Wait generates fewer aborts (only when an old transaction is blocked by a young one).

---

## 5. Serializable Snapshot Isolation (SSI)

### The Core Insight (Cahill 2008)

Snapshot Isolation (REPEATABLE READ in MVCC) allows non-serializable executions due to **write skew**: two transactions each read overlapping data and write disjoint parts, each individually valid, but together violating an invariant.

SSI adds conflict detection on top of SI at near-zero overhead. The key observation:

**Every non-serializable SI execution contains a "dangerous structure":** a pivot transaction T_pivot such that:
- There exists T_in with T_in →rw T_pivot (T_pivot read something T_in wrote, or will write)
- There exists T_out with T_pivot →rw T_out (T_out read something T_pivot wrote)

This is called "two consecutive rw-anti-dependency edges meeting at T_pivot."

### rw-Anti-Dependency

T1 →rw T2 (read-write anti-dependency from T1 to T2) if:
- T1 read version V of object X
- T2 later wrote a new version V' of X that supersedes V
- In a serial order, T1 must come before T2 (because T1 read the old value)
- But in a serial order, T2 must come before T1 (because T1 should see T2's write)
- This contradiction → T1 and T2 cannot be serialized in either order → dangerous

### SIREAD Locks

SIREAD (Serializable Isolation READ) locks track what each transaction has read. Unlike regular S locks:
- SIREAD locks don't block writers (no conflict with X locks)
- SIREAD locks are used only for conflict detection
- SIREAD locks are held until the transaction commits (not released during shrinking phase)

When T_writer writes key K:
1. Look up siread[K] to find all transactions that have a SIREAD on K
2. For each T_reader in siread[K]: record rw-anti-dependency T_reader →rw T_writer

### Pivot Detection at Commit

At commit time for transaction T:
- If T.InConflict (some T_in has T_in →rw T) AND T.OutConflict (T has T →rw T_out) → T is a pivot → abort T

This is the simplified version. The full Cahill algorithm also checks committed transactions:
- If T_in is committed AND T has T_in →rw T AND T has T →rw T_out (for any T_out, committed or active) → unsafe

The simplified version (InConflict && OutConflict booleans) catches most cases. The full version handles edge cases where T_in commits before T.

### Write Skew Trace

Doctor on-call: doctors A and B are both on-call. Invariant: at least one must be on-call.

```
T1 reads A (SIREAD[A] = {T1})
T1 reads B (SIREAD[B] = {T1})
T2 reads A (SIREAD[A] = {T1, T2})
T2 reads B (SIREAD[B] = {T1, T2})

T1 writes A:
  → NotifyWrite(T1, "A")
  → siread[A] = {T1, T2}; for T2: T2.InConflict=true, T1.OutConflict=true
  → Edge: T2 →rw T1

T2 writes B:
  → NotifyWrite(T2, "B")
  → siread[B] = {T1, T2}; for T1: T1.InConflict=true, T2.OutConflict=true
  → Edge: T1 →rw T2

T1 commits:
  → CheckCommit(T1): T1.InConflict=true, T1.OutConflict=true → T1 IS A PIVOT → ABORT T1!

OR (if T1 commits first, before NotifyWrite for T2's write):
  → T1 not yet InConflict at its commit time → T1 commits
  → T2 commits:
  → CheckCommit(T2): T2.InConflict=true, T2.OutConflict=true → ABORT T2
```

Either T1 or T2 is aborted. The result: only one doctor goes off-call. Invariant maintained.

### SSI False Abort Rate

SSI is conservative — it aborts some transactions that would actually be serializable. The false abort rate is proportional to the conflict rate, which is low for typical OLTP workloads (< 5% abort rate at high concurrency for TPC-C-like workloads). The benefit is 100% correct serializability without the overhead of locking reads.

---

## 6. Gap Locking for Phantom Prevention

### The Phantom Problem

```
T1: SELECT * FROM orders WHERE amount > 1000  → {row5: 1500, row8: 2000}
T2: INSERT INTO orders (id=99, amount=5000)
T2: COMMIT
T1: SELECT * FROM orders WHERE amount > 1000  → {row5: 1500, row8: 2000, row99: 5000}
                                                  ↑ PHANTOM! row99 wasn't there before
```

This doesn't violate row-level S locks (T1 has S on row5 and row8; T2 inserts a new row not covered by any lock). Phantom prevention requires locking the "space where new rows could appear."

### Next-Key Locking (InnoDB-style)

For a range scan [lo, hi], we lock:
1. All existing rows in [lo, hi] (with S locks)
2. The gaps between those rows (with S gap locks)
3. The gap after the last row in the range (up to +∞ or the next key)

The gap lock on (last_key, +∞) prevents insertions after the scan range.
The gap lock on (-∞, first_key) prevents insertions before the scan range.

**Gap Lock Representation:**
```
Gap(T, lo, hi): lock the interval (lo, hi] in table T

Inserting key K into table T:
  prevKey = largest existing key < K
  Lock(Gap(T, prevKey, K)) with X mode
  → If T1 holds S gap lock on (prevKey, K], T2's X gap lock blocks → phantom prevented
```

**Compatibility:** Gap locks from different transactions on overlapping ranges conflict:
- S gap + S gap: compatible (multiple scanners can have overlapping gaps)
- S gap + X gap (insert): incompatible (scanner blocks inserter)
- X gap + X gap: incompatible (two inserters into same gap block each other)

### Why MVCC Alone Doesn't Prevent Phantoms (at RR level)

Under MVCC REPEATABLE READ:
- T1 scans: sees only rows visible in its snapshot → {row5, row8}
- T2 inserts row99 and commits
- T1 scans again: its snapshot is the same (taken at BEGIN) → still sees {row5, row8}

**No phantom!** MVCC RR prevents phantoms through snapshot isolation. This is why PostgreSQL RR is stronger than SQL standard RR (SQL standard RR allows phantoms; PostgreSQL doesn't).

However, under MVCC, write skew can still occur (as shown in the doctor scenario). SSI is needed to prevent write skew, not gap locks.

**Gap locks are primarily useful in the 2PL path** where reads use locks (not snapshots) and phantoms would otherwise occur.

---

## 7. Undo Log and Savepoints

### Undo-Based Recovery

The undo log records before-images of every modified row, enabling rollback without a separate redo log (we don't need crash recovery in our in-memory system):

```
T1: UPDATE accounts SET balance=900 WHERE id=1  (was 1000)
  → UndoLog.Append({Op: UndoUpdate, Table: accounts, Key: 1, Before: [1, "Alice", 1000.0]})

T1: INSERT INTO products (id=99, name="Widget")
  → UndoLog.Append({Op: UndoInsert, Table: products, Key: 99, Before: nil})

T1: ROLLBACK:
  → Apply undo entries in reverse:
    UndoInsert → DELETE FROM products WHERE id=99
    UndoUpdate → UPDATE accounts SET balance=1000 WHERE id=1
```

### Savepoints

A savepoint is a marker in the undo log. Rolling back to a savepoint applies undo entries from the current position back to the savepoint marker:

```
T1 BEGIN
T1 UPDATE x=1  → UndoLog: [{UPDATE x, before=0}]
T1 SAVEPOINT sp1  → SavepointStack: [{name:"sp1", undoPos:1}]
T1 UPDATE y=1  → UndoLog: [{UPDATE x, before=0}, {UPDATE y, before=0}]
T1 UPDATE z=1  → UndoLog: [{UPDATE x, before=0}, {UPDATE y, before=0}, {UPDATE z, before=0}]
T1 ROLLBACK TO sp1:
  → Apply undo[3]: restore z=0
  → Apply undo[2]: restore y=0
  → Stop at savepoint position 1
  → Truncate UndoLog to position 1
  → Stack: [{name:"sp1", undoPos:1}] (savepoint still exists — can rollback again)

T1 COMMIT:
  → x=1 is committed (only the pre-savepoint write)
```

**Lock behavior at savepoints:** In Rigorous 2PL, locks are released only at commit/abort. Savepoint rollback does NOT release locks acquired after the savepoint. This prevents other transactions from reading/writing the (now-rolled-back) data while T1 is still running.

---

## 8. MVCC Vacuum (Garbage Collection)

### The Horizon

The vacuum horizon is the TxnID of the oldest active transaction. Any version with XMax < horizon and XMax committed is invisible to all current and future transactions (because all future snapshots will have Xmin ≥ horizon, so xmax < horizon means xmax < Xmin = committed and not in Active for any future snapshot).

```
horizon = min(all active TxnIDs)

Safe to prune version V if:
  V.XMax != 0          -- version has been deleted
  ∧ V.XMax < horizon   -- deleted by a transaction older than all active ones
  ∧ committed(V.XMax)  -- and the deletion is committed
```

### The Chain Ordering After Pruning

After pruning, the tail of the version chain is removed. The remaining chain must still be a valid version history:

```
Before vacuum (horizon = 50):
  [V4: XMin=60, XMax=0] → [V3: XMin=40, XMax=60] → [V2: XMin=20, XMax=40] → [V1: XMin=5, XMax=20]

V1: XMax=20 < 50 and committed → PRUNE
V2: XMax=40 < 50 and committed → PRUNE
V3: XMax=60 ≥ 50 → KEEP (might be visible to transactions with Xmax > 60)

After vacuum:
  [V4: XMin=60, XMax=0] → [V3: XMin=40, XMax=60]
```

The chain is now shorter. V3 is the oldest version that might still be visible.

### Aborted Versions

Aborted transaction versions (XMin = aborted TxnID) should also be pruned during vacuum. They're never visible to anyone (the creator aborted), so they waste memory. In practice, abort undo-log processing removes these versions immediately (on abort, we remove the version from the chain). Vacuum handles any stragglers.

---

## 9. Transaction ID Monotonicity

TxnIDs must be monotonically increasing and globally unique. We use `atomic.Uint64`:

```go
var nextID atomic.Uint64

func NextTxnID() TxnID {
    return TxnID(nextID.Add(1))
}
```

Starting from 1 (0 = invalid/null TxnID). This gives 2^64 - 1 ≈ 18 quintillion unique IDs. At 1 million transactions/second, this wraps in 584,542 years.

**Snapshot consistency:** Because TxnIDs are assigned atomically before the transaction is registered in the active set, there's no window where a transaction exists but has no ID. The snapshot `(Xmin, Xmax, Active)` is captured under the txnManager lock, ensuring a consistent view of which IDs are active.

---

## 10. TPC-B Benchmark Design

### Why TPC-B

TPC-B (Transaction Processing Performance Council - Benchmark B) is the classic OLTP benchmark for database systems. Its debit/credit transaction is simple but exposes all the concurrency issues we care about:

- **Write-write conflicts:** Two transactions debiting the same account simultaneously
- **Read-write conflicts:** One transaction reading an account while another updates it
- **Deadlocks:** T1 holds A waits B; T2 holds B waits A
- **Lost updates:** Under insufficient isolation, two concurrent increments produce one net increment

### Why the Balance Invariant

```
Initial state: SUM(balance) = 1,000,000
Each transaction: debit - X from account A, credit + X to account B
Net effect on total: -X + X = 0

Therefore: SUM(balance) must remain 1,000,000 after any set of committed transactions
```

If the invariant is violated, exactly one type of bug occurred:
- `SUM < 1,000,000`: a debit committed without a corresponding credit (lost credit)
- `SUM > 1,000,000`: a credit applied twice or debit lost (lost debit or double credit)

Both indicate a lost update or dirty read — the signature of insufficient isolation.

### Abort Rate Expectations

| Isolation Level | Expected Abort Rate (8 concurrency, 100 accounts) |
|---|---|
| READ COMMITTED | < 1% (write-write conflicts only) |
| REPEATABLE READ | 5-15% (first-writer-wins causes more aborts) |
| SERIALIZABLE | 5-20% (SSI adds additional aborts for dangerous structures) |

Higher abort rates = more retries = lower throughput, but stronger correctness guarantees.

### Verifying No Lost Updates

In addition to the SUM invariant, verify individual account balances are consistent:
```
For a transfer of $100 from account A to account B:
  final_A = initial_A - (# times A was debited × 100) + (# times A was credited × 100)
```

This is harder to verify without logging all transfers, so the SUM invariant is the primary check.

---

## 11. Performance Considerations

### Lock Table: sync.Map vs map + sync.Mutex

We use `sync.Map` for the lock table (resource → queue) because:
- Lock acquisitions are read-heavy (most lookups find an existing queue, rarely create new ones)
- `sync.Map` optimizes for the case where entries are written once and read many times
- No need to hold a global mutex during per-queue operations

The per-queue `sync.Mutex` handles concurrent access to a single queue's granted/waiting lists.

### MVCC Store: sync.Map for Chains

Each version chain has its own `sync.RWMutex`. The chain store uses `sync.Map` (chainKey → *VersionChain). This means:
- Looking up a chain: lock-free O(1) (sync.Map read path)
- Creating a new chain: atomic store via sync.Map
- Reading a chain's versions: per-chain RLock
- Writing to a chain (new version): per-chain Lock

Throughput: with N tables × M rows, each having independent chain locks, concurrent operations on different rows proceed without interference.

### Snapshot Take Under Manager Lock

The snapshot must be taken atomically. We hold the txnManager global lock during snapshot construction to ensure:
1. No new transactions start between reading Xmin and reading Active
2. No active transactions commit between reading Active and reading Xmax

This is a brief critical section (just reading a few maps), not holding the lock during any I/O.

### Vacuum Frequency

Every 5 seconds is appropriate for our workload. More frequent vacuum:
- Faster memory reclamation
- More CPU overhead (scanning all chains)

Less frequent:
- Less CPU overhead
- More memory used by dead versions

For a production system, trigger vacuum based on dead version count, not just time.