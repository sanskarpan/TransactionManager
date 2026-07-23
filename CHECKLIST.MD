# Implementation Checklist

Strict phase order. Every phase must pass `go test ./... -race` before proceeding to the next.

---

## Phase 0 — Scaffolding

- [ ] `go mod init github.com/yourname/txn-manager`
- [ ] `go get github.com/go-chi/chi/v5 github.com/stretchr/testify`
- [ ] Create full directory tree from SPEC.md
- [ ] `cmd/server/main.go`: stub that prints "transaction manager starting"
- [ ] Create `Makefile`: `build`, `test`, `race`, `bench`, `run`, `dev`, `lint`

**Frontend:**
- [ ] `cd web && npm create vite@latest . -- --template react-ts`
- [ ] Install: `tailwindcss @tailwindcss/vite framer-motion @xyflow/react @dagrejs/dagre @dagrejs/graphlib recharts zustand ky`
- [ ] `npx shadcn@latest init`
- [ ] Add shadcn: `button badge card tabs select separator scroll-area tooltip sheet dialog alert`
- [ ] `vite.config.ts`: proxy `/api` and `/sse` → `http://localhost:8080`
- [ ] Verify `npm run dev` runs clean

---

## Phase 1 — Value Types and Errors

- [ ] Define `ValueType` enum: Null, Int, Float, Text, Bool
- [ ] Implement `Value` struct with union fields + `IsNull bool`
- [ ] Implement `Value.Compare(other Value) (int, error)`: -1/0/1, type coercion Int↔Float
- [ ] Implement `Value.String() string` for display
- [ ] Implement `Value.Equal(other Value) bool`
- [ ] Define `ErrorCode` string type with all error constants from SPEC.md
- [ ] Define `TxnError` struct implementing `error` interface
- [ ] Implement typed constructors: `ErrDeadlock(...)`, `ErrWriteConflict(victim TxnID)`, etc.
- [ ] Write value tests: comparison across types, null handling, coercion

---

## Phase 2 — Storage Layer (2PL mode)

- [ ] Define `RowKey string` type
- [ ] Implement `Row` struct: `Key RowKey`, `Values []Value`
- [ ] Implement `Column` struct: name, type, notNull, pk
- [ ] Implement `Table` struct: name, schema, `rows map[RowKey][]Value`, `sync.RWMutex`
- [ ] Implement `Table.GetRow(key RowKey) ([]Value, bool)` (under read lock)
- [ ] Implement `Table.PutRow(key RowKey, values []Value)` (under write lock)
- [ ] Implement `Table.DeleteRow(key RowKey) bool`
- [ ] Implement `Table.Scan(filter FilterFunc) []Row` (under read lock)
- [ ] Implement `Table.Keys() []RowKey` sorted ascending
- [ ] Implement `Catalog` struct: `tables map[string]*Table`
- [ ] Implement `Catalog.Register(table *Table)`
- [ ] Implement `Catalog.Lookup(name string) (*Table, bool)`
- [ ] Implement `Catalog.List() []string`
- [ ] Implement seed function: create `accounts` (100 rows), `products` (50), `inventory` (50)
- [ ] Verify seed: `SUM(accounts.balance) == 1,000,000` in test
- [ ] Write storage tests: CRUD, scan with filter, catalog operations

---

## Phase 3 — Transaction ID and Snapshot

- [ ] Implement `TxnID uint64` with global atomic counter (`atomic.Uint64`)
- [ ] Implement `NextTxnID() TxnID` (monotone, thread-safe)
- [ ] Define `TxnStatus` enum: Active, Committed, Aborted
- [ ] Define `ConcurrencyProtocol` enum: Protocol2PL, ProtocolMVCC
- [ ] Define `IsolationLevel` enum: ReadUncommitted, ReadCommitted, RepeatableRead, Serializable
- [ ] Implement `Snapshot` struct: `Xmin, Xmax TxnID`, `Active []TxnID`
- [ ] Implement `Snapshot.Contains(id TxnID) bool` (checks if id is in Active set)
- [ ] Implement `Snapshot.IsCommitted(id TxnID) bool` (id < Xmin and not in Active)
- [ ] Implement `Transaction` struct with all fields from SPEC.md
- [ ] Implement `Transaction.IsActive/IsCommitted/IsAborted() bool`
- [ ] Implement `Transaction.LockTimeout` field, default 5 seconds
- [ ] Write snapshot tests: visibility rules for different xmin/xmax/active combos

---

## Phase 4 — Undo Log and Savepoints

- [ ] Implement `UndoEntry` struct: op type, table, key, before-image, savepoint ID
- [ ] Implement `UndoLog` struct: `[]UndoEntry` with mutex
- [ ] Implement `UndoLog.Append(op, table, key, before)` 
- [ ] Implement `UndoLog.Len() int`
- [ ] Implement `UndoLog.EntriesFrom(position int) []UndoEntry` (for savepoint rollback)
- [ ] Implement `UndoLog.TruncateTo(position int)` (after savepoint rollback)
- [ ] Implement `Savepoint` struct: `Name string`, `UndoPosition int`, `LocksHeld snapshot`
- [ ] Implement `SavepointStack.Push(name string, position int)`
- [ ] Implement `SavepointStack.Find(name string) (*Savepoint, bool)`
- [ ] Implement `SavepointStack.Pop(name string) error`
- [ ] Write undo log tests: append, rollback to position, truncate

---

## Phase 5 — Lock Modes and Compatibility

- [ ] Implement `LockMode` int enum (NL=0 through U=6)
- [ ] Implement `LockCompatible [7][7]bool` matrix from SPEC.md exactly
- [ ] Write compatibility matrix unit test: verify every cell matches the spec table
- [ ] Implement `LockStrength [7]int` for upgrade decisions
- [ ] Implement `DominantMode(a, b LockMode) LockMode`
- [ ] Implement `ResourceType` enum: Table, Row, Gap, Predicate
- [ ] Implement `ResourceID` struct with all fields
- [ ] Implement `ResourceID.String() string` (for map keys and display)
- [ ] Implement `ResourceForTable(table string) ResourceID`
- [ ] Implement `ResourceForRow(table, key string) ResourceID`
- [ ] Implement `ResourceForGap(table, lo, hi string) ResourceID`
- [ ] Write lock mode tests: all compatibility pairs, dominant mode

---

## Phase 6 — Lock Table and Queue

- [ ] Implement `LockStatus` enum: Granted, Waiting, Converting
- [ ] Implement `LockRequest` struct: TxnID, Mode, Status, grantedCh, cancelCh, timestamps
- [ ] Implement `LockQueue` struct: resource, `granted []*LockRequest`, `waiting []*LockRequest`, mutex
- [ ] Implement `LockQueue.CanGrant(mode LockMode, exceptTxn TxnID) bool`:
  - [ ] Returns true if `mode` is compatible with every granted lock (except our own)
- [ ] Implement `LockQueue.AddGranted(req *LockRequest)` 
- [ ] Implement `LockQueue.AddWaiting(req *LockRequest)` (appends to wait queue)
- [ ] Implement `LockQueue.RemoveGranted(txnID TxnID) bool`
- [ ] Implement `LockQueue.RemoveWaiting(txnID TxnID) bool`
- [ ] Implement `LockQueue.GrantWaiters()`: iterate waiting, grant compatible requests in order, signal their `grantedCh`
- [ ] Implement `LockQueue.ConflictingTxns(mode LockMode, requester TxnID) []TxnID`: who blocks us?
- [ ] Implement `LockTable` struct: `queues sync.Map` (ResourceID.String() → *LockQueue)
- [ ] Implement `LockTable.GetOrCreate(resource ResourceID) *LockQueue`
- [ ] Implement `LockTable.Get(resource ResourceID) (*LockQueue, bool)`
- [ ] Implement `LockTable.Snapshot() []LockQueueSnapshot` (for API)
- [ ] Write lock queue tests:
  - [ ] S+S compatible (both granted), S+X incompatible (X waits), X+X incompatible
  - [ ] After S holder releases, waiting X gets granted
  - [ ] Multiple S holders block one X; X granted only after all S released

---

## Phase 7 — Deadlock Detection

- [ ] Implement `WaitForGraph` struct: `edges map[TxnID]map[TxnID]struct{}`, `sync.RWMutex`
- [ ] Implement `WaitForGraph.AddEdge(from, to TxnID)`
- [ ] Implement `WaitForGraph.RemoveNode(txnID TxnID)` (remove all edges from and to this node)
- [ ] Implement `WaitForGraph.Nodes() []TxnID`
- [ ] Implement `WaitForGraph.Edges() [][2]TxnID`
- [ ] Implement `WaitForGraph.FindCycle() (victim TxnID, cycle []TxnID, found bool)`:
  - [ ] DFS with white/gray/black coloring
  - [ ] Cycle path extraction from parent map
  - [ ] Victim selection: highest TxnID (youngest transaction)
- [ ] Implement `DeadlockRecord` struct: ID, detected time, cycle []TxnID, victim, victimReason, edges
- [ ] Implement `DeadlockHistory`: ring buffer of last 100 records
- [ ] Implement `DeadlockDetector` struct wrapping WaitForGraph + history
- [ ] Implement `DeadlockDetector.Start()`: background goroutine, runs `FindCycle` every 50ms
- [ ] When cycle found: call `txnManager.Abort(victim, ErrDeadlock)`, add to history
- [ ] Implement `DeadlockDetector.Stop()`
- [ ] Write deadlock detection tests:
  - [ ] 2-node cycle detected: T1→T2→T1
  - [ ] 3-node cycle detected: T1→T2→T3→T1
  - [ ] No false positive: T1→T2, T3→T4 (two separate waits, no cycle)
  - [ ] Cycle removed after victim abort
  - [ ] Concurrent AddEdge + FindCycle with `-race`

---

## Phase 8 — Deadlock Prevention

- [ ] Define `PreventionPolicy` enum: PolicyDetect, PolicyWaitDie, PolicyWoundWait
- [ ] Implement `WaitDie(requester, holder *Transaction) DeadlockAction`:
  - [ ] requester.ID < holder.ID → ActionWait (older waits for younger)
  - [ ] else → ActionDie (younger aborts itself)
- [ ] Implement `WoundWait(requester, holder *Transaction) DeadlockAction`:
  - [ ] requester.ID < holder.ID → ActionWound (older preempts younger)
  - [ ] else → ActionWait (younger waits for older)
- [ ] `ActionWound`: call `txnManager.Abort(holder.ID, ErrWounded)`, signal holder's `abortCh`
- [ ] Write prevention tests:
  - [ ] WaitDie: txn 5 requests lock held by txn 10 → wait (5 < 10)
  - [ ] WaitDie: txn 10 requests lock held by txn 5 → die (10 > 5)
  - [ ] WoundWait: txn 5 requests lock held by txn 10 → wound (5 < 10, older wounds younger)
  - [ ] WoundWait: txn 10 requests lock held by txn 5 → wait (10 > 5)
  - [ ] WoundWait: wounded txn (10) gets aborted and its lock released, waking txn 5

---

## Phase 9 — 2PL Protocol (Lock Acquisition Engine)

- [ ] Implement `AcquireLock(txn *Transaction, resource ResourceID, mode LockMode) error`:
  - [ ] Check if txn already holds sufficient or stronger lock → return nil
  - [ ] Check if txn holds weaker lock → upgrade path
  - [ ] Get/create queue for resource
  - [ ] Check compatibility with current grants (excluding own)
  - [ ] If compatible: grant immediately, update `txn.LocksHeld[resource]`
  - [ ] If not compatible: apply prevention policy (if not Detect)
  - [ ] If waiting: add to WaitForGraph, add to wait queue, block on `grantedCh` with timeout
  - [ ] On grant: remove from WaitForGraph, update txn state
  - [ ] On timeout: remove wait request, return ErrLockTimeout
  - [ ] On abort signal (`txn.abortCh`): remove wait request, return ErrTxnAborted
- [ ] Implement `UpgradeLock(txn *Transaction, resource ResourceID, newMode LockMode) error`:
  - [ ] Verify txn holds a lock on resource (else error)
  - [ ] If newMode == held mode: no-op
  - [ ] If held mode subsumes new mode: no-op
  - [ ] Remove old grant, add Converting request to FRONT of wait queue
  - [ ] Wait for grant (priority upgrade)
- [ ] Implement `ReleaseLock(txn *Transaction, resource ResourceID)` (for 2PC future use)
- [ ] Implement `ReleaseAllLocks(txn *Transaction)`:
  - [ ] For each resource in txn.LocksHeld: removeGranted, call GrantWaiters
  - [ ] Remove txn from WaitForGraph
  - [ ] Clear txn.LocksHeld
- [ ] Acquire table-level IS/IX before row-level S/X (intention locking protocol):
  - [ ] Read on row → IS on table, S on row
  - [ ] Write on row → IX on table, X on row
  - [ ] Full table scan for write → SIX on table
- [ ] Write 2PL acquisition tests:
  - [ ] Concurrent reads all granted simultaneously
  - [ ] Write blocks concurrent reads
  - [ ] Lock released → waiter woken
  - [ ] Lock timeout returns error
  - [ ] `-race` with 50 goroutines acquiring locks on same resource

---

## Phase 10 — MVCC Version Chains

- [ ] Implement `Version` struct: XMin, XMax, Data, Cmin, CreatedAt, Prev
- [ ] Implement `VersionChain` struct: `head *Version`, `key RowKey`, `sync.RWMutex`
- [ ] Implement `VersionChain.Prepend(v *Version)` (insert at head, thread-safe)
- [ ] Implement `VersionChain.Head() *Version`
- [ ] Implement `VersionChain.All() []*Version` (newest to oldest)
- [ ] Implement `VersionChain.SetXMax(xmin TxnID, xmax TxnID) bool` (mark version as deleted)
- [ ] Implement `VersionChain.Prune(pred func(*Version) bool)` (remove versions matching pred)
- [ ] Implement `MVCCStore` struct: `chains sync.Map` (string → *VersionChain)
- [ ] Implement `MVCCStore.GetChain(table, key string) (*VersionChain, bool)`
- [ ] Implement `MVCCStore.GetOrCreateChain(table, key string) *VersionChain`
- [ ] Implement `MVCCStore.ForEachChain(fn func(*VersionChain))`
- [ ] Implement `MVCCStore.AllVersions() map[string][]*Version` (for API/visualization)
- [ ] Write version chain tests: prepend, xmax update, prune, concurrent access

---

## Phase 11 — MVCC Visibility and Reads

- [ ] Implement `IsVisible(v *Version, txn *Transaction, snap *Snapshot, mgr *TxnManager) bool`:
  - [ ] `xminVisible`: xmin < snap.Xmin (committed before snapshot) OR (committed AND not in Active AND < Xmax)
  - [ ] `selfCreated`: xmin == txn.ID
  - [ ] `xmaxInvisible`: xmax == 0 OR xmax >= snap.Xmax OR xmax in Active OR mgr.IsAborted(xmax)
  - [ ] return (selfCreated OR xminVisible) AND (xmaxInvisible OR xmax == txn.ID)
- [ ] Implement `VersionChain.FindVisible(txn *Transaction, snap *Snapshot, mgr *TxnManager) *Version`
- [ ] Implement `MVCCRead(txn, table, key) ([]Value, bool, error)`:
  - [ ] For ReadCommitted: use per-statement snapshot (refresh `txn.CurrentStatementSnapshot`)
  - [ ] Find visible version in chain
  - [ ] Record key in txn.ReadSet (for SSI)
  - [ ] If SSI: call `ssiTracker.RecordRead(txn, key)`
  - [ ] Return data or miss
- [ ] Implement `MVCCScan(txn, table, filterFn) ([]Row, error)`:
  - [ ] Iterate all chains for table
  - [ ] For each chain: find visible version, apply filter
  - [ ] Return all matching rows
  - [ ] If SSI: record SIREAD on each visited key + predicate
- [ ] Write visibility tests:
  - [ ] Version created by committed xmin IS visible with correct snapshot
  - [ ] Version created by active xmin NOT visible to other transaction
  - [ ] Version with xmax set NOT visible (deleted)
  - [ ] ReadCommitted gets fresh snapshot per call
  - [ ] RepeatableRead sees same snapshot throughout txn
  - [ ] Self-written version IS visible to own transaction

---

## Phase 12 — MVCC Writes and Write-Write Conflict

- [ ] Implement `CheckWriteConflict(txn *Transaction, chain *VersionChain, mgr *TxnManager) error`:
  - [ ] Find the current visible version for this txn
  - [ ] If visible version's XMin != txn.ID AND that txn is still active → ErrWriteConflict
  - [ ] If visible version's XMin committed but txn started after it and another active txn also wrote it → ErrWriteConflict
- [ ] Implement `MVCCInsert(txn, table, key, values) error`:
  - [ ] GetOrCreate chain
  - [ ] Check for existing visible version (duplicate key check)
  - [ ] Check write-write conflict
  - [ ] Create new Version with XMin=txn.ID
  - [ ] Prepend to chain
  - [ ] SSI: ssiTracker.RecordWrite(txn, key)
  - [ ] UndoLog.Append(UndoInsert, ...)
  - [ ] Add key to txn.WriteSet
- [ ] Implement `MVCCUpdate(txn, table, key, newValues) error`:
  - [ ] Find visible version
  - [ ] Check write-write conflict
  - [ ] UndoLog.Append(UndoUpdate, ..., oldValues)
  - [ ] Set XMax on old version = txn.ID
  - [ ] Create new version, prepend
  - [ ] SSI: RecordWrite
- [ ] Implement `MVCCDelete(txn, table, key) error`:
  - [ ] Find visible version
  - [ ] Check write-write conflict
  - [ ] UndoLog.Append(UndoDelete, ..., oldValues)
  - [ ] Set XMax on visible version = txn.ID
  - [ ] SSI: RecordWrite
- [ ] Write write-conflict tests:
  - [ ] T1 and T2 both update same row → second gets ErrWriteConflict
  - [ ] T1 updates, commits, T2 updates same row → T2 succeeds (new version)
  - [ ] Undo log captures before-images correctly

---

## Phase 13 — SSI: Serializable Snapshot Isolation

- [ ] Implement `SSITracker` struct: `siread sync.Map` (key → `map[TxnID]*Transaction`), `edges sync.Map`
- [ ] Implement `SSITracker.RecordRead(reader *Transaction, key string)`:
  - [ ] Add reader to siread[key]
  - [ ] Add key to reader.SIReadKeys
- [ ] Implement `SSITracker.RecordWrite(writer *Transaction, key string)`:
  - [ ] For each txn in siread[key] that != writer and is still Active:
    - [ ] Set reader.InConflict = true (someone wrote what I read)
    - [ ] Set writer.OutConflict = true (I wrote what someone read)
    - [ ] Record edge: reader.ID →rw writer.ID
- [ ] Implement `SSITracker.CheckCommit(txn *Transaction) error`:
  - [ ] If txn.InConflict AND txn.OutConflict → return ErrSerializationFailure
  - [ ] Additionally check: for each outgoing edge T_self →rw T_other, if T_other also has InConflict → unsafe pivot
- [ ] Implement `SSITracker.Cleanup(txn *Transaction)`:
  - [ ] Remove txn from all siread maps
  - [ ] Remove all edges involving txn.ID
- [ ] Write SSI tests (critical):
  - [ ] Write skew scenario: T1 reads x,y; T2 reads x,y; T1 writes x; T2 writes y; T2 commits → T1 gets serialization failure
  - [ ] Non-conflicting concurrent txns: T1 reads and writes disjoint keys from T2 → both commit
  - [ ] Single writer is never a pivot (inConflict=false → no abort)
  - [ ] Committed txn cleanup removes it from siread maps

---

## Phase 14 — Transaction Manager (Orchestrator)

- [ ] Implement `TxnManager` struct:
  - [ ] `mu sync.RWMutex`
  - [ ] `txns map[TxnID]*Transaction`
  - [ ] `committed map[TxnID]struct{}` (set of committed IDs)
  - [ ] `aborted map[TxnID]struct{}`
  - [ ] `nextID atomic.Uint64`
  - [ ] `lockTable *LockTable`
  - [ ] `mvccStore *MVCCStore`
  - [ ] `catalog *Catalog` (2PL storage)
  - [ ] `ssiTracker *SSITracker`
  - [ ] `deadlockDetector *DeadlockDetector`
  - [ ] `vacuum *Vacuum`
  - [ ] `eventBus *SSEBus`
  - [ ] `metrics *Metrics`
- [ ] Implement `TxnManager.Begin(protocol, isolation, timeout) (*Transaction, error)`:
  - [ ] Assign TxnID, create Transaction
  - [ ] If MVCC: take snapshot (or defer to first statement for RC)
  - [ ] Register in txns map
  - [ ] Emit SSE event: txn_begin
- [ ] Implement `TxnManager.Commit(txnID TxnID) error`:
  - [ ] Verify txn is Active
  - [ ] If SSI: call ssiTracker.CheckCommit → abort if failure
  - [ ] If 2PL: call ReleaseAllLocks
  - [ ] If MVCC: mark all versions written by this txn as committed (XMax entries with our ID become visible)
  - [ ] Move from txns map to committed set
  - [ ] Set txn.Status = Committed
  - [ ] SSI cleanup
  - [ ] Emit SSE: txn_commit
  - [ ] Update metrics
- [ ] Implement `TxnManager.Abort(txnID TxnID, reason error) error`:
  - [ ] Verify txn is Active (idempotent: if already aborted, return nil)
  - [ ] If 2PL: ReleaseAllLocks (wakes waiters)
  - [ ] If MVCC: set XMax=0 on all versions written by this txn (they're now invisible)... actually: mark all our XMin versions as aborted by setting aborted set; mark any XMax=txn.ID versions as "not deleted" (restore XMax to 0)
  - [ ] Apply undo log in reverse for 2PL storage
  - [ ] Set txn.Status = Aborted
  - [ ] Signal txn.abortCh (wake any goroutines waiting on this txn)
  - [ ] SSI cleanup
  - [ ] Emit SSE: txn_abort
- [ ] Implement `TxnManager.CreateSavepoint(txnID TxnID, name string) error`
- [ ] Implement `TxnManager.RollbackToSavepoint(txnID TxnID, name string) error`:
  - [ ] Apply undo log entries from current position back to savepoint position
  - [ ] For 2PL: release locks acquired after savepoint (complex; use lock snapshot)
  - [ ] Truncate undo log to savepoint position
  - [ ] Pop savepoints newer than this one
- [ ] Implement `TxnManager.ReleaseSavepoint(txnID TxnID, name string) error`
- [ ] Implement helpers: `IsCommitted(id)`, `IsAborted(id)`, `IsActive(id)`, `OldestActiveTxnID()`
- [ ] Implement `TxnManager.TakeSnapshot() *Snapshot`
- [ ] Write transaction manager integration tests:
  - [ ] Begin → read → write → commit: data visible to subsequent transaction
  - [ ] Begin → write → abort: data NOT visible after abort
  - [ ] Savepoint: write → savepoint → write more → rollback-to-savepoint → commit: only pre-savepoint writes visible
  - [ ] Concurrent commit and abort: no data races

---

## Phase 15 — 2PL Read/Write Operations

- [ ] Implement `TwoPLRead(txn *Transaction, table, key string) ([]Value, bool, error)`:
  - [ ] If ReadUncommitted: read directly from table (no lock)
  - [ ] Else: AcquireLock(txn, ResourceForRow(table, key), LockS)
  - [ ] Read from catalog storage
  - [ ] Return value
- [ ] Implement `TwoPLWrite(txn *Transaction, table, key string, values []Value, op WriteOp) error`:
  - [ ] AcquireLock(txn, ResourceForTable(table), LockIX) — intent on table
  - [ ] AcquireLock(txn, ResourceForRow(table, key), LockX)
  - [ ] UndoLog.Append with before-image
  - [ ] Apply write to catalog storage
- [ ] Implement `TwoPLScan(txn *Transaction, table string, filter FilterFunc) ([]Row, error)`:
  - [ ] For RepeatableRead: AcquireLock(table, LockS)
  - [ ] For Serializable (2PL): gap lock entire table
  - [ ] Read all matching rows under locks
- [ ] Implement `TwoPLInsert` with gap lock for phantom prevention
- [ ] Apply undo log on abort: reverse all entries using catalog storage operations
- [ ] Write 2PL integration tests: full read-write-commit under each isolation level

---

## Phase 16 — MVCC Vacuum

- [ ] Implement `Vacuum` struct: store, txnMgr, interval, stop chan
- [ ] Implement `Vacuum.Run()`: background goroutine, ticks every 5 seconds
- [ ] Implement `Vacuum.pruneVersions(horizon TxnID)`:
  - [ ] ForEachChain: remove versions where XMax != 0, XMax <= horizon, txn committed
  - [ ] Update metrics.VersionsPruned
- [ ] Implement `Vacuum.Stats() VacuumStats`: total chains, total versions, last run, pruned count
- [ ] Implement `TxnManager.OldestActiveTxnID() TxnID`
- [ ] Write vacuum tests:
  - [ ] After 3 transactions update same row, vacuum removes old versions
  - [ ] Running transaction's readable version is NOT pruned
  - [ ] Aborted version IS pruned (set XMax before pruning)

---

## Phase 17 — Gap Locking for 2PL Phantom Prevention

- [ ] Implement `LockForRangeScan(txn *Transaction, table string, lo, hi string) error`:
  - [ ] AcquireLock(table-level, IS)
  - [ ] Get sorted key list from table
  - [ ] For each key in [lo, hi]: AcquireLock(ResourceForRow, S)
  - [ ] For each gap in range: AcquireLock(ResourceForGap, S)
- [ ] Implement `LockForInsert(txn *Transaction, table string, key string) error`:
  - [ ] Find prev and next key in table
  - [ ] AcquireLock(ResourceForGap(table, prev, key), X) — locks the gap
  - [ ] AcquireLock(ResourceForRow(table, key), X)
- [ ] Write gap lock tests:
  - [ ] T1 scans [1,10] with gap locks; T2 tries to insert key 5 → T2 blocks
  - [ ] T1 scans [1,10]; T2 inserts key 11 (outside range) → T2 succeeds immediately
  - [ ] After T1 commits (gap lock released), T2 continues and inserts

---

## Phase 18 — Anomaly Scenarios

For each scenario, implement a `Scenario` struct with `Name`, `Description`, `AnomalyType`, `Steps []ScenarioStep`.

- [ ] Define `Scenario` interface: `Name()`, `Describe()`, `Run(mgr *TxnManager, isolation IsolationLevel, protocol ConcurrencyProtocol) *ScenarioResult`
- [ ] Implement `ScenarioResult` struct: steps log, anomaly occurred bool, anomaly type, explanation string
- [ ] Implement `DirtyReadScenario` — T1 writes dirty, T2 reads, T1 aborts
- [ ] Implement `LostUpdateScenario` — T1 and T2 both read-modify-write same row
- [ ] Implement `NonRepeatableReadScenario` — T1 reads twice, T2 commits between
- [ ] Implement `PhantomReadScenario` — T1 scans range, T2 inserts, T1 scans again
- [ ] Implement `WriteSkewScenario` — doctor on-call problem
- [ ] Implement `DeadlockCycleScenario` — T1 locks A waits B; T2 locks B waits A
- [ ] Implement `CascadeAbortScenario` — T1 dirty-writes, T2 reads dirty, T1 aborts
- [ ] Implement `ScenarioRegistry`: map[name]*Scenario
- [ ] Write scenario tests: each scenario at different isolation levels verifies expected outcome
  - [ ] DirtyRead occurs at ReadUncommitted (MVCC), prevented at ReadCommitted
  - [ ] LostUpdate occurs at ReadCommitted (MVCC), prevented at RepeatableRead
  - [ ] WriteSkew occurs at RepeatableRead, prevented at Serializable (SSI aborts one txn)
  - [ ] DeadlockCycle: victim is chosen, other txn succeeds

---

## Phase 19 — Metrics

- [ ] Implement `Metrics` struct with all atomic counters from SPEC.md
- [ ] Implement `Histogram` struct: configurable buckets, atomic bucket counters
- [ ] Implement `Histogram.Record(valueMs float64)`
- [ ] Implement `Histogram.P50/P95/P99() float64` (approximate from bucket boundaries)
- [ ] Implement `WindowedStats` ring buffer (60 × 1s buckets, same as cache project)
- [ ] Wire metrics increments into: TxnManager.Begin/Commit/Abort, AcquireLock, deadlock detector
- [ ] Implement `Metrics.Snapshot() MetricsSnapshot` for API serialization
- [ ] Write metrics tests: increment + concurrent reads with `-race`

---

## Phase 20 — TPC-B Benchmark

- [ ] Implement `TPCBWorkload` struct: accounts table ref, debit/credit transfer logic
- [ ] Implement `TPCBTransaction(mgr, concurrencyProtocol, isolation)`:
  - [ ] Pick two random account IDs
  - [ ] Read both balances (within transaction)
  - [ ] Write debit (balance - 100) on account A
  - [ ] Write credit (balance + 100) on account B
  - [ ] Commit; on conflict/deadlock: retry up to 3 times with exponential backoff
- [ ] Implement `BenchmarkRunner.Run(config BenchmarkConfig) *BenchmarkResult`:
  - [ ] Spawn `config.Concurrency` goroutines
  - [ ] Each runs TPCBTransaction in a loop for `config.Duration`
  - [ ] Collect commit/abort/deadlock counts + latency samples
  - [ ] After run: verify SUM(accounts.balance) == 1,000,000
- [ ] Implement `BenchmarkResult` with all fields from SPEC.md
- [ ] Write benchmark correctness test:
  - [ ] Run 1000 transactions at RepeatableRead with concurrency=4
  - [ ] Verify balance invariant holds
  - [ ] Verify no lost updates (commit count + abort count = total attempts)

---

## Phase 21 — HTTP API

- [ ] Set up chi router with CORS, logging, recovery middleware
- [ ] Implement SSE broadcaster: `SSEBus` with per-connection channel, broadcast method
- [ ] Implement all handlers from SPEC.md:
  - [ ] `POST /api/txn/begin`
  - [ ] `POST /api/txn/{id}/commit`
  - [ ] `POST /api/txn/{id}/abort`
  - [ ] `GET /api/txn/{id}/status`
  - [ ] `GET /api/txn/active`
  - [ ] `POST /api/txn/{id}/read`
  - [ ] `POST /api/txn/{id}/write`
  - [ ] `POST /api/txn/{id}/scan`
  - [ ] `POST /api/txn/{id}/insert`
  - [ ] `POST /api/txn/{id}/delete`
  - [ ] `POST /api/txn/{id}/savepoint`
  - [ ] `POST /api/txn/{id}/rollback-to`
  - [ ] `DELETE /api/txn/{id}/savepoint/{name}`
  - [ ] `GET /api/locks`
  - [ ] `GET /api/locks/{table}/{key}`
  - [ ] `GET /api/deadlocks`
  - [ ] `GET /api/wfg`
  - [ ] `GET /api/mvcc/chain/{table}/{key}`
  - [ ] `GET /api/mvcc/stats`
  - [ ] `POST /api/mvcc/vacuum`
  - [ ] `GET /api/scenarios`
  - [ ] `POST /api/scenarios/{name}/run`
  - [ ] `POST /api/benchmark/run` (async, returns job ID)
  - [ ] `GET /api/benchmark/results/{jobId}`
  - [ ] `GET /api/metrics`
  - [ ] `GET /sse/events`
  - [ ] `GET /sse/wfg`
- [ ] Write API httptest tests: all endpoints, happy path + all error codes

---

## Phase 22 — Frontend Infrastructure

- [ ] React Router v6: Dashboard `/`, Playground `/playground`, WFG `/wfg`, Versions `/versions`, Scenarios `/scenarios`, Benchmarks `/benchmarks`
- [ ] TypeScript types: mirror all backend DTOs exactly with strict typing
- [ ] API client with typed functions for every endpoint
- [ ] `useSSE(url, onEvent)` hook: EventSource with reconnect on error (2s delay)
- [ ] Zustand store: activeTxns, wfgState, scenarios, benchmarkResults, mvccChains
- [ ] Nav component with active states
- [ ] Error boundary + global error toast

---

## Phase 23 — Dashboard Page

- [ ] Four metric cards: active txns, txns/sec, abort rate, deadlock count (SSE-live)
- [ ] Transaction table: ID, protocol, isolation, status, locks held count, waiting-for, duration (live)
- [ ] Status badges: color per isolation level (RU=gray, RC=blue, RR=amber, SER=purple)
- [ ] Rolling 60s line chart: commits/s vs aborts/s (Recharts `LineChart`, SSE-fed)
- [ ] Abort rate breakdown bar chart by abort reason (deadlock / write-conflict / SSI / timeout)
- [ ] Quick action buttons: "Start Demo Deadlock", "Reset Database"

---

## Phase 24 — Playground Page

- [ ] Three transaction tabs (T1, T2, T3) with independent state
- [ ] Each tab: protocol selector, isolation selector, BEGIN button
- [ ] Operation builder: table, key, op type, values (JSON editor)
- [ ] EXECUTE button (Ctrl+Enter)
- [ ] COMMIT / ABORT / SAVEPOINT / ROLLBACK-TO buttons
- [ ] Results log: scrollable, timestamped, color-coded (hit=green, miss=gray, error=red)
- [ ] Lock status panel: current locks held + any lock being waited for (live-updating)
- [ ] Error modals: deadlock / write-conflict / serialization failure with full details
- [ ] Quick test panel: preset operations ("Transfer $100 from acct 1 to acct 2")

---

## Phase 25 — Wait-For Graph Page

- [ ] React Flow canvas with custom `TxnNode` and `ConflictEdge` components
- [ ] `TxnNode`: circle colored by isolation level, shows txn ID + duration + wait status
- [ ] `ConflictEdge`: dashed red when part of detected deadlock cycle, labeled with resource
- [ ] dagre auto-layout, re-layout on node add/remove
- [ ] SSE subscription to `/sse/wfg` for live updates
- [ ] Deadlock history sidebar: last 20 deadlocks with cycle diagram
- [ ] "Simulate Deadlock" button: triggers `DeadlockCycleScenario`
- [ ] Empty state: "No active transactions — start some in the Playground"

---

## Phase 26 — Version Chains Page

- [ ] Table/key selector form
- [ ] Version chain horizontal timeline: newest → oldest, each version is a colored bubble
- [ ] Bubble: XMin badge (green=committed, blue=active, red=aborted), XMax badge if set, values preview
- [ ] Visibility matrix table: rows = active transactions, columns = versions, cell = ✓/✗ with reason
- [ ] "All Chains" tab: paginated table of all rows per table with version count
- [ ] Vacuum panel: oldest horizon, total versions, pruned count, last vacuum time, VACUUM NOW button
- [ ] Live update: poll every 2s when transactions are active

---

## Phase 27 — Scenarios Page

- [ ] Scenario grid: cards for all 7 scenarios with anomaly type badge
- [ ] Scenario player (Sheet component):
  - [ ] Timeline header: T1, T2, T3 columns
  - [ ] Step list: each step highlighted as current
  - [ ] NEXT STEP / AUTOPLAY (1 step/second) buttons
  - [ ] Current database state panel (balance values, version counts)
  - [ ] Result panel: "Anomaly Detected / Prevented", explanation text
  - [ ] Isolation level selector: re-run same scenario at different level

---

## Phase 28 — Benchmarks Page

- [ ] Config form: protocol, isolation, concurrency (1/2/4/8/16), duration (10/30/60s)
- [ ] RUN button with live progress: committed count, abort count, elapsed time
- [ ] Results section:
  - [ ] Throughput bar chart by concurrency level
  - [ ] Latency chart: P50/P95/P99 vs concurrency (Recharts LineChart)
  - [ ] Abort rate bar by reason type
  - [ ] Balance invariant check: large ✓ or ✗ with actual sum
- [ ] Comparison mode: run 2PL vs MVCC simultaneously, overlay charts
- [ ] Result table: one row per run with all metrics, sortable

---

## Phase 29 — Integration and Polish

- [ ] Smoke test: start server, run all 7 scenarios, verify expected outcomes
- [ ] Smoke test: TPC-B at concurrency=8, RR, MVCC — balance invariant holds
- [ ] Smoke test: deadlock between T1 and T2 in playground — victim aborted, other commits
- [ ] Smoke test: version chain viewer shows 3 versions after 2 updates to same row
- [ ] `go test ./... -race -count=1` — zero failures
- [ ] `npx tsc --noEmit` — zero errors
- [ ] Load test: 100 concurrent transactions for 30s — no goroutine leaks, no panics
- [ ] README.md: setup in 3 commands, architecture overview, key concepts explained

---

## Testing Requirements Summary

| Component | Must verify |
|---|---|
| Lock Compatibility Matrix | All 49 cells |
| MVCC Visibility | All 4 isolation level variants |
| SSI | Write-skew detected, non-conflicting allowed |
| Deadlock Detection | 2-cycle, 3-cycle, no false positive |
| Deadlock Prevention | WaitDie + WoundWait victim selection |
| Savepoints | Rollback restores exact pre-savepoint state |
| Undo Log | Abort restores original data via before-images |
| Vacuum | Doesn't prune visible versions |
| TPC-B | Balance invariant: SUM = 1,000,000 after concurrent run |
| Scenarios | Each anomaly occurs OR is prevented at correct isolation level |