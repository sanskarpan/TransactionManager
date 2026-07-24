# Transaction Manager — Technical Specification

## Overview

A production-quality in-memory transaction manager built from scratch in Go implementing two concurrency control paradigms — Two-Phase Locking (2PL) and Multi-Version Concurrency Control (MVCC) — with all four SQL isolation levels, Serializable Snapshot Isolation (SSI), deadlock detection and prevention, predicate/gap locking for phantom prevention, savepoints, undo logging, MVCC vacuum, and a React+TypeScript frontend with live wait-for graph visualization, version-chain inspector, anomaly scenario runner, and TPC-B benchmarking.

---

## Tech Stack

### Backend
| Concern | Choice |
|---|---|
| Language | Go 1.22+ |
| HTTP Router | `chi` v5 |
| Live Events | Server-Sent Events (stdlib) |
| Concurrency | `sync.RWMutex`, `sync/atomic`, `context` |
| Testing | `testing` + `testify` + `go test -race` |
| No external DB | 100% in-memory, hand-rolled |

### Frontend
| Concern | Choice |
|---|---|
| Language | TypeScript 5+ strict |
| Framework | React 18 + Vite 5 |
| Styling | Tailwind CSS v3 + shadcn/ui |
| Graph viz | React Flow (`@xyflow/react`) + `@dagrejs/dagre` |
| Charts | Recharts |
| Animation | Framer Motion |
| State | Zustand |
| Live data | native `EventSource` (SSE) |
| HTTP | `ky` |

---

## Project Structure

```
txn-manager/
├── cmd/server/main.go
├── internal/
│   ├── types/
│   │   ├── value.go          # SQL value types (INT, TEXT, FLOAT, BOOL, NULL)
│   │   └── errors.go         # Typed error hierarchy
│   ├── storage/
│   │   ├── catalog.go        # Table registry
│   │   ├── table.go          # Table: schema + row store
│   │   ├── row.go            # Row representation + RowKey
│   │   └── seed.go           # Seed data (accounts, products, inventory)
│   ├── txn/
│   │   ├── id.go             # TxnID: atomic counter, monotone
│   │   ├── transaction.go    # Transaction struct + state machine
│   │   ├── snapshot.go       # Snapshot: Xmin, Xmax, Active[]
│   │   ├── manager.go        # TxnManager: Begin/Commit/Abort + protocol dispatch
│   │   ├── undo_log.go       # Per-txn undo log (before-images)
│   │   └── savepoint.go      # Savepoint stack
│   ├── lock/
│   │   ├── mode.go           # LockMode enum + 7×7 compatibility matrix
│   │   ├── resource.go       # ResourceID (Table / Row / Gap)
│   │   ├── request.go        # LockRequest + LockStatus
│   │   ├── queue.go          # Per-resource LockQueue (granted + waiting)
│   │   ├── table.go          # LockTable: global registry of queues
│   │   ├── grant.go          # Grant logic: check compatibility, wake waiters
│   │   └── upgrade.go        # Lock upgrade (S→X, U→X) with deadlock avoidance
│   ├── deadlock/
│   │   ├── graph.go          # WaitForGraph: adjacency map, DFS cycle detection
│   │   ├── detector.go       # Background detector (runs every 50ms)
│   │   ├── prevention.go     # Wait-Die + Wound-Wait strategies
│   │   └── history.go        # DeadlockRecord store (last 100)
│   ├── mvcc/
│   │   ├── version.go        # Version: XMin, XMax, Data, Prev, Cmin
│   │   ├── chain.go          # VersionChain per row-key
│   │   ├── store.go          # MVCCStore: map[RowKey]*VersionChain
│   │   ├── visibility.go     # IsVisible(version, txn, level) per isolation level
│   │   ├── write_conflict.go # First-writer-wins check
│   │   └── vacuum.go         # Background GC: prune invisible old versions
│   ├── isolation/
│   │   ├── level.go          # IsolationLevel enum + per-level Read/Write rules
│   │   ├── ssi.go            # SSI: SIREAD locks + rw-anti-dependency tracking
│   │   ├── gap_lock.go       # Next-key / gap locks for 2PL phantom prevention
│   │   └── anomaly.go        # Anomaly type enum + per-level anomaly matrix
│   ├── operations/
│   │   ├── read.go           # Read operation: dispatch to 2PL or MVCC path
│   │   ├── write.go          # Write operation: dispatch to 2PL or MVCC path
│   │   ├── scan.go           # Range scan: with gap locking or MVCC snapshot
│   │   └── predicate.go      # Predicate lock registration + matching
│   ├── metrics/
│   │   ├── counters.go       # Atomic counters: commits, aborts, deadlocks, conflicts
│   │   ├── histograms.go     # Latency histograms (P50/P95/P99) per operation
│   │   └── window.go         # Sliding 60s time-series for dashboard charts
│   ├── benchmark/
│   │   ├── tpcb.go           # TPC-B: account debit/credit transfers
│   │   ├── runner.go         # Concurrent workload runner
│   │   └── result.go         # Result aggregation + anomaly verification
│   └── scenario/
│       ├── scenario.go       # Scenario interface + registry
│       ├── dirty_read.go
│       ├── lost_update.go
│       ├── non_repeatable.go
│       ├── phantom.go
│       ├── write_skew.go
│       ├── deadlock_cycle.go
│       └── cascade_abort.go
├── api/
│   ├── server.go
│   ├── handler_txn.go        # begin/commit/abort/status
│   ├── handler_ops.go        # read/write/scan/insert/delete
│   ├── handler_lock.go       # lock table state, lock history
│   ├── handler_mvcc.go       # version chain, vacuum stats
│   ├── handler_deadlock.go   # wait-for graph, deadlock history
│   ├── handler_scenario.go   # run predefined scenarios
│   ├── handler_benchmark.go  # TPC-B runs
│   ├── sse.go                # SSE broadcaster
│   ├── middleware.go
│   └── dto.go
├── web/
│   ├── src/
│   │   ├── pages/
│   │   │   ├── Dashboard.tsx
│   │   │   ├── Playground.tsx
│   │   │   ├── WaitForGraph.tsx
│   │   │   ├── VersionChains.tsx
│   │   │   ├── Scenarios.tsx
│   │   │   └── Benchmarks.tsx
│   │   ├── components/
│   │   │   ├── txn/
│   │   │   │   ├── TransactionTable.tsx
│   │   │   │   ├── LockHeldBadge.tsx
│   │   │   │   ├── IsolationBadge.tsx
│   │   │   │   └── TxnTimeline.tsx
│   │   │   ├── graph/
│   │   │   │   ├── WFGCanvas.tsx       # React Flow wait-for graph
│   │   │   │   ├── TxnNode.tsx
│   │   │   │   └── ConflictEdge.tsx
│   │   │   ├── mvcc/
│   │   │   │   ├── VersionChainRow.tsx # Horizontal chain of version bubbles
│   │   │   │   └── VisibilityMatrix.tsx
│   │   │   ├── scenario/
│   │   │   │   ├── ScenarioPlayer.tsx  # Step-through animation
│   │   │   │   └── AnomalyBadge.tsx
│   │   │   └── shared/
│   │   │       ├── LiveCounter.tsx
│   │   │       └── LatencyChart.tsx
│   │   ├── store/
│   │   │   └── txnStore.ts
│   │   ├── hooks/
│   │   │   ├── useSSE.ts
│   │   │   └── usePoll.ts
│   │   ├── api/client.ts
│   │   └── types/index.ts
│   ├── package.json
│   └── vite.config.ts
├── go.mod
└── Makefile
```

---

## Core Data Types

### TxnID and Transaction State Machine

```go
// internal/txn/id.go
type TxnID uint64   // monotonically increasing, 0 = invalid

// internal/txn/transaction.go
type TxnStatus int
const (
    TxnActive    TxnStatus = iota
    TxnCommitted
    TxnAborted
    TxnPrepared  // 2PC future use
)

type ConcurrencyProtocol int
const (
    Protocol2PL  ConcurrencyProtocol = iota  // Two-Phase Locking
    ProtocolMVCC                              // Multi-Version Concurrency Control
)

type Transaction struct {
    ID          TxnID
    Status      TxnStatus
    Protocol    ConcurrencyProtocol
    Isolation   IsolationLevel
    Snapshot    *Snapshot         // MVCC: set at begin or first statement
    LocksHeld   map[ResourceID]LockMode
    ReadSet     map[RowKey]TxnID  // MVCC/SSI: which version was read
    WriteSet    map[RowKey]struct{}
    UndoLog     *UndoLog
    Savepoints  []*Savepoint
    
    // SSI fields
    InConflict  bool              // someone wrote something I read (T_other →rw T_self)
    OutConflict bool              // I wrote something someone reads (T_self →rw T_other)
    SIReadKeys  map[RowKey]struct{} // keys protected by SIREAD locks

    // Metrics
    BeginAt     time.Time
    WaitTime    time.Duration
    RestartCount int
    Priority    int               // for wound-wait victim selection
    
    // Concurrency
    mu          sync.Mutex
    abortCh     chan error         // closed when txn is wounded/aborted
}
```

### Snapshot (MVCC)

```go
// internal/txn/snapshot.go
type Snapshot struct {
    Xmin    TxnID    // all txns with ID < Xmin are committed when snapshot was taken
    Xmax    TxnID    // next TxnID to be assigned; all IDs >= Xmax are future txns
    Active  []TxnID  // txns between Xmin and Xmax still running at snapshot time
    TakenAt time.Time
}
```

### Lock Mode and Compatibility

```go
// internal/lock/mode.go
type LockMode int
const (
    LockNL  LockMode = iota  // 0: No Lock
    LockIS                    // 1: Intention Shared
    LockIX                    // 2: Intention Exclusive
    LockS                     // 3: Shared
    LockSIX                   // 4: Shared + Intention Exclusive
    LockX                     // 5: Exclusive
    LockU                     // 6: Update (read phase; upgrades to X)
)

// LockCompatible[requesting][held] — true means they can coexist
var LockCompatible = [7][7]bool{
    //      NL     IS     IX     S      SIX    X      U
    /* NL*/ {true,  true,  true,  true,  true,  true,  true},
    /* IS*/ {true,  true,  true,  true,  true,  false, true},
    /* IX*/ {true,  true,  true,  false, false, false, false},
    /* S */ {true,  true,  false, true,  false, false, true},
    /*SIX*/ {true,  true,  false, false, false, false, false},
    /* X */ {true,  false, false, false, false, false, false},
    /* U */ {true,  true,  false, true,  false, false, false},
}

// LockStrength for upgrade decisions (higher = stronger)
var LockStrength = [7]int{0, 1, 2, 3, 4, 6, 5}

// DominantMode returns the mode that subsumes both (for lock escalation)
func DominantMode(a, b LockMode) LockMode
```

### Resource ID

```go
// internal/lock/resource.go
type ResourceType int
const (
    ResourceTable ResourceType = iota
    ResourceRow
    ResourceGap       // gap between two keys (for phantom prevention)
    ResourcePredicate // for predicate locking
)

type ResourceID struct {
    Type  ResourceType
    Table string
    Key   string  // row key or empty for table-level
    GapLo string  // gap lower bound (exclusive); empty = -∞
    GapHi string  // gap upper bound (inclusive); empty = +∞
}
```

### Lock Queue

```go
// internal/lock/queue.go
type LockStatus int
const (
    LockStatusGranted   LockStatus = iota
    LockStatusWaiting
    LockStatusConverting // upgrading an existing lock
)

type LockRequest struct {
    TxnID     TxnID
    Mode      LockMode
    Status    LockStatus
    grantedCh chan struct{}    // closed when lock is granted
    cancelCh  chan struct{}    // closed to cancel the wait
    RequestAt time.Time
    GrantedAt time.Time
}

type LockQueue struct {
    mu      sync.Mutex
    res     ResourceID
    granted []*LockRequest
    waiting []*LockRequest
}
```

### MVCC Version

```go
// internal/mvcc/version.go
type Version struct {
    XMin      TxnID       // Transaction that created this version
    XMax      TxnID       // Transaction that deleted/superseded this version (0 = live)
    Data      []Value     // Row data at this version
    Cmin      int         // Command counter within XMin (for same-txn visibility)
    CreatedAt time.Time
    Prev      *Version    // Older version (singly linked, head = newest)
}

// internal/mvcc/chain.go
type VersionChain struct {
    mu   sync.RWMutex
    head *Version // newest version at front
    key  RowKey
}
```

### Undo Log

```go
// internal/txn/undo_log.go
type UndoOpType int
const (
    UndoInsert UndoOpType = iota  // undo = delete this row
    UndoUpdate                     // undo = restore before-image
    UndoDelete                     // undo = re-insert before-image
)

type UndoEntry struct {
    Op          UndoOpType
    Table       string
    Key         RowKey
    BeforeImage []Value  // nil for UndoInsert
    SavepointID int      // which savepoint boundary this entry belongs to
}

type UndoLog struct {
    entries []UndoEntry
}
```

---

## Backend: Concurrency Control Algorithms

### 2PL: Two-Phase Locking

**Rigorous 2PL** (all locks held until commit/abort):
- Growing phase: acquire any lock needed
- Shrinking phase: release ALL locks atomically at commit/abort
- Prevents cascading aborts (no dirty reads possible since X locks held until commit)

**Lock acquisition flow:**
```
AcquireLock(txn, resource, mode):
  queue = lockTable.GetOrCreate(resource)
  queue.mu.Lock()
  
  // Check if txn already holds a sufficient or stronger lock
  if existing := txn.LocksHeld[resource]; existing >= mode:
    queue.mu.Unlock(); return nil
  
  // Check compatibility with all granted locks
  compatible = true
  for each granted request in queue.granted:
    if granted.TxnID != txn.ID && !LockCompatible[mode][granted.Mode]:
      compatible = false; break
  
  if compatible:
    // Grant immediately
    queue.granted = append(queue.granted, &LockRequest{txn.ID, mode, Granted})
    txn.LocksHeld[resource] = mode
    queue.mu.Unlock()
    return nil
  
  // Conflict: check prevention policy before waiting
  if prevention == WaitDie || prevention == WoundWait:
    if should_die(txn, conflictingTxn):
      queue.mu.Unlock(); return ErrTransactionAborted
    if should_wound(txn, conflictingTxn):
      abortTxn(conflictingTxn)
  
  // Add to wait queue
  req = &LockRequest{txn.ID, mode, Waiting, make(chan struct{})}
  queue.waiting = append(queue.waiting, req)
  waitForGraph.AddEdge(txn.ID, conflictingTxn.ID)
  queue.mu.Unlock()
  
  // Block until granted, timed out, or transaction aborted
  select {
    case <-req.grantedCh: return nil
    case <-txn.abortCh:   return ErrTransactionAborted
    case <-time.After(txn.LockTimeout): return ErrLockTimeout
  }
```

**Lock release on commit/abort:**
```
ReleaseLocks(txn):
  for resource, mode in txn.LocksHeld:
    queue = lockTable.Get(resource)
    queue.mu.Lock()
    queue.removeGranted(txn.ID)
    waitForGraph.RemoveEdges(txn.ID)
    queue.tryGrantWaiting()  // wake up next compatible waiter(s)
    queue.mu.Unlock()
  txn.LocksHeld = {}
```

**Lock upgrade (S → X, U → X):**
```
UpgradeLock(txn, resource, newMode):
  queue = lockTable.Get(resource)
  queue.mu.Lock()
  
  // Can only upgrade if no other txn holds an incompatible lock
  otherConflicts = false
  for each granted in queue.granted:
    if granted.TxnID != txn.ID && !LockCompatible[newMode][granted.Mode]:
      otherConflicts = true; break
  
  if !otherConflicts:
    queue.updateGranted(txn.ID, newMode)
    txn.LocksHeld[resource] = newMode
    queue.mu.Unlock(); return nil
  
  // Must wait for upgrade — insert at front of wait queue (priority)
  req = &LockRequest{txn.ID, newMode, Converting, grantedCh}
  queue.waiting = prepend(queue.waiting, req)
  queue.mu.Unlock()
  ... (wait as above)
```

### MVCC: Multi-Version Concurrency Control

**Snapshot construction (at BEGIN or first statement depending on isolation level):**
```
TakeSnapshot(txnManager):
  txnManager.mu.RLock()
  defer txnManager.mu.RUnlock()
  
  xmin = txnManager.minActiveTxnID()  // lowest ID of any running txn
  xmax = txnManager.nextTxnID         // next ID to be assigned
  active = [id for id, txn in txnManager.active if txn.Status == Active]
  
  return &Snapshot{Xmin: xmin, Xmax: xmax, Active: active}
```

**Visibility check (the most critical function):**
```go
func IsVisible(v *Version, txn *Transaction, level IsolationLevel) bool {
    snap := txn.Snapshot
    if level == ReadCommitted {
        snap = txn.CurrentStatementSnapshot // refreshed per statement
    }

    xmin := v.XMin
    xmax := v.XMax

    // Was this version created by our own transaction?
    selfCreated := xmin == txn.ID

    // Is xmin's transaction committed and visible in our snapshot?
    xminVisible := xmin < snap.Xmin || 
                   (txnManager.IsCommitted(xmin) && !containsID(snap.Active, xmin) && xmin < snap.Xmax)

    if !selfCreated && !xminVisible {
        return false  // creator not yet committed (or not in our snapshot)
    }

    // Is the version not deleted, or deleted by an invisible transaction?
    if xmax == 0 {
        return true  // not deleted
    }

    xmaxInvisible := xmax >= snap.Xmax ||
                     containsID(snap.Active, xmax) ||
                     txnManager.IsAborted(xmax)

    return xmaxInvisible || xmax == txn.ID
}
```

**MVCC Write (INSERT):**
```
MVCCInsert(txn, table, key, values):
  chain = mvccStore.GetOrCreate(table, key)
  chain.mu.Lock()
  
  // Check write-write conflict (first-writer-wins)
  for v in chain.versions:
    if IsVisible(v, txn, txn.Isolation):
      if v.XMin in activeTransactions && v.XMin != txn.ID:
        chain.mu.Unlock()
        return ErrWriteConflict  // concurrent writer exists
  
  newVersion = &Version{XMin: txn.ID, Data: values}
  chain.Prepend(newVersion)
  
  // SSI: notify any SIREAD holders
  if txn.Isolation == Serializable:
    ssi.NotifyWrite(txn, table+":"+key)
  
  // Undo log
  txn.UndoLog.Append(UndoInsert, table, key, nil)
  chain.mu.Unlock()
```

**MVCC Write (UPDATE):**
```
MVCCUpdate(txn, table, key, newValues):
  chain = mvccStore.Get(table, key)
  chain.mu.Lock()
  
  visible = chain.FindVisible(txn)
  if visible == nil: return ErrRowNotFound
  
  // Write-write conflict check
  if visible.XMin != txn.ID && IsActive(visible.XMin):
    chain.mu.Unlock(); return ErrWriteConflict
  
  txn.UndoLog.Append(UndoUpdate, table, key, visible.Data)
  
  visible.XMax = txn.ID   // mark old version as deleted by us
  newVersion = &Version{XMin: txn.ID, Data: newValues, Prev: visible}
  chain.Prepend(newVersion)
  
  if txn.Isolation == Serializable:
    ssi.NotifyWrite(txn, table+":"+key)
  chain.mu.Unlock()
```

### SSI: Serializable Snapshot Isolation

Based on "Serializable Isolation for Snapshot Databases" (Cahill, Röhm, Fekete 2008).

**Core concept:** SSI runs on top of Snapshot Isolation (REPEATABLE READ MVCC). It adds detection of "dangerous structures" — cycles in the serialization graph that contain at least two consecutive rw-anti-dependency edges.

**rw-anti-dependency:** T1 →rw T2 if T2 read a version of row R that T1 later overwrote. This means T1 must appear to execute after T2's read but the anti-dependency forces T2 after T1 — creating a potential cycle.

**Implementation:**

```go
// internal/isolation/ssi.go
type SSITracker struct {
    mu       sync.RWMutex
    // SIREAD locks: key → set of txns holding SIREAD on that key
    siread   map[string]map[TxnID]*Transaction
    // Serialization graph edges: T_from →rw T_to
    edges    map[TxnID]map[TxnID]struct{}
}

// Called when T_reader reads key K
func (s *SSITracker) RecordRead(reader *Transaction, key string) {
    s.mu.Lock()
    defer s.mu.Unlock()
    if s.siread[key] == nil {
        s.siread[key] = make(map[TxnID]*Transaction)
    }
    s.siread[key][reader.ID] = reader
    reader.SIReadKeys[key] = struct{}{}
}

// Called when T_writer writes key K
func (s *SSITracker) RecordWrite(writer *Transaction, key string) {
    s.mu.Lock()
    defer s.mu.Unlock()
    // For each T_reader that has a SIREAD on K: T_reader →rw T_writer
    for readerID, reader := range s.siread[key] {
        if readerID == writer.ID { continue }
        if reader.Status != TxnActive { continue }
        // Record: reader reads something writer will overwrite
        s.addEdge(readerID, writer.ID)
        reader.InConflict = true
        writer.OutConflict = true
    }
}

// Called at commit time to check if this txn is a pivot
func (s *SSITracker) CheckCommit(txn *Transaction) error {
    s.mu.RLock()
    defer s.mu.RUnlock()
    // Txn is a dangerous pivot if it has BOTH inConflict AND outConflict
    if txn.InConflict && txn.OutConflict {
        return ErrSerializationFailure
    }
    return nil
}

// Cleanup SIREAD locks and edges when txn commits/aborts
func (s *SSITracker) Cleanup(txn *Transaction)
```

### Deadlock Detection: Wait-For Graph

```go
// internal/deadlock/graph.go
type WaitForGraph struct {
    mu    sync.RWMutex
    edges map[TxnID]map[TxnID]struct{}  // T_i waits for T_j
}

// AddEdge: T_from is waiting for a lock held by T_to
func (g *WaitForGraph) AddEdge(from, to TxnID)

// RemoveEdges: remove all edges from this txn (when it stops waiting)
func (g *WaitForGraph) RemoveEdges(txn TxnID)

// FindCycle: DFS with three-color marking, returns victim TxnID + cycle path
func (g *WaitForGraph) FindCycle() (victim TxnID, cycle []TxnID, found bool) {
    g.mu.RLock(); defer g.mu.RUnlock()
    
    color := make(map[TxnID]int) // 0=white, 1=gray(in stack), 2=black(done)
    parent := make(map[TxnID]TxnID)
    
    var dfs func(v TxnID) bool
    dfs = func(v TxnID) bool {
        color[v] = 1
        for w := range g.edges[v] {
            if color[w] == 1 {
                // Back edge found → cycle from w back to w through v
                cycle = extractCycle(parent, v, w)
                victim = selectVictim(cycle) // youngest txn
                return true
            }
            if color[w] == 0 {
                parent[w] = v
                if dfs(w) { return true }
            }
        }
        color[v] = 2
        return false
    }
    
    for txn := range g.edges {
        if color[txn] == 0 {
            if dfs(txn) { return victim, cycle, true }
        }
    }
    return 0, nil, false
}
```

### Deadlock Prevention Policies

```go
// internal/deadlock/prevention.go
type PreventionPolicy int
const (
    PolicyDetect    PreventionPolicy = iota // background detection
    PolicyWaitDie                           // non-preemptive
    PolicyWoundWait                         // preemptive
)

// WaitDie: older transaction waits; younger dies
func WaitDie(requester, holder *Transaction) DeadlockAction {
    if requester.ID < holder.ID {
        return ActionWait   // requester is older → wait
    }
    return ActionDie        // requester is younger → abort self
}

// WoundWait: older wounds (preempts) younger; younger waits
func WoundWait(requester, holder *Transaction) DeadlockAction {
    if requester.ID < holder.ID {
        return ActionWound  // requester is older → abort holder
    }
    return ActionWait       // requester is younger → wait
}
```

### Gap Locking (Phantom Prevention in 2PL mode)

```go
// internal/isolation/gap_lock.go

// For a range scan [lo, hi], acquire gap locks:
//   gap(-∞, lo) + lock(lo) + gap(lo, next_key) + lock(next_key) + ... + gap(last_key, hi)
// Prevents insertions into the range by concurrent transactions

// GapSpec represents the gap between two adjacent keys
type GapSpec struct {
    Lo    string  // exclusive lower bound; empty = -∞
    Hi    string  // inclusive upper bound; empty = +∞
}

func ResourceForGap(table string, lo, hi string) ResourceID {
    return ResourceID{Type: ResourceGap, Table: table, GapLo: lo, GapHi: hi}
}

// When T inserts key K into table T_tab:
//   Must acquire IX on T_tab (table-level intent)
//   Must acquire X on gap(prev_key, K] to prevent concurrent rangescan conflict
func LockForInsert(txn *Transaction, table string, key string, lockTable *LockTable) error
```

### MVCC Vacuum

```go
// internal/mvcc/vacuum.go
type Vacuum struct {
    store    *MVCCStore
    txnMgr   *TxnManager
    interval time.Duration
    stop     chan struct{}
}

// Background loop: every `interval` seconds
func (v *Vacuum) Run() {
    ticker := time.NewTicker(v.interval)
    for {
        select {
        case <-ticker.C:
            horizon := v.txnMgr.OldestActiveTxnID()
            v.pruneVersions(horizon)
        case <-v.stop:
            return
        }
    }
}

// Prune versions with XMax <= horizon (permanently deleted and invisible to all)
func (v *Vacuum) pruneVersions(horizon TxnID) {
    v.store.ForEachChain(func(chain *VersionChain) {
        chain.mu.Lock()
        defer chain.mu.Unlock()
        // Walk chain, remove versions where XMax != 0 && XMax <= horizon && txn committed
        chain.Prune(func(ver *Version) bool {
            return ver.XMax != 0 && 
                   ver.XMax <= horizon && 
                   v.txnMgr.IsCommitted(ver.XMax)
        })
    })
}
```

---

## Isolation Levels and Anomaly Matrix

| Anomaly | Read Uncommitted | Read Committed | Repeatable Read | Serializable |
|---|---|---|---|---|
| Dirty Read | ✗ POSSIBLE | ✓ Prevented | ✓ Prevented | ✓ Prevented |
| Lost Update | ✗ POSSIBLE | ✗ POSSIBLE | ✓ Prevented* | ✓ Prevented |
| Non-Repeatable Read | ✗ POSSIBLE | ✗ POSSIBLE | ✓ Prevented | ✓ Prevented |
| Phantom Read | ✗ POSSIBLE | ✗ POSSIBLE | ✗ POSSIBLE† | ✓ Prevented |
| Write Skew | ✗ POSSIBLE | ✗ POSSIBLE | ✗ POSSIBLE | ✓ Prevented |
| Read Skew | ✗ POSSIBLE | ✗ POSSIBLE | ✓ Prevented | ✓ Prevented |

*MVCC first-writer-wins prevents lost updates at RR level.
†PostgreSQL RR prevents phantoms via MVCC snapshot; true phantom protection requires SSI or gap locks.

### How Each Level is Implemented

**READ UNCOMMITTED (2PL mode):**
- Read operations acquire NO locks (not even shared)
- Write operations still acquire X locks
- Dirty reads possible: read data from uncommitted transactions

**READ UNCOMMITTED (MVCC mode):**
- Skip visibility check entirely
- Return the newest version regardless of commit status

**READ COMMITTED (MVCC mode):**
- Take a new snapshot at the start of each statement
- Only see versions where xmin is committed at the time of the statement

**REPEATABLE READ (MVCC mode):**
- Take snapshot once at transaction BEGIN
- All reads throughout the transaction see the same consistent snapshot
- Write-write conflicts detected: if concurrent txn wrote same row, abort

**SERIALIZABLE (MVCC + SSI):**
- REPEATABLE READ + SSI checks
- SIREAD locks track reads; writes check for rw-anti-dependencies
- Abort transaction if dangerous structure detected

---

## Predefined Anomaly Scenarios

Each scenario is a scripted sequence of operations across 2-3 transactions with assertions that verify which anomaly occurs or is prevented based on isolation level.

### Scenario 1: Dirty Read
```
T1: BEGIN; UPDATE accounts SET balance=0 WHERE id=1
T2: BEGIN; READ accounts WHERE id=1  → sees balance=0 (DIRTY if RU, missed if RC+)
T1: ABORT
T2: sees stale/incorrect data
```
**Expected behavior:**
- READ UNCOMMITTED: T2 sees balance=0 (dirty read)
- READ COMMITTED+: T2 sees original balance (dirty read prevented)

### Scenario 2: Lost Update
```
T1: BEGIN RC; READ x=100
T2: BEGIN RC; READ x=100
T1: WRITE x = x + 50 = 150; COMMIT
T2: WRITE x = x + 50 = 150; COMMIT  ← T1's update is lost!
Expected: x=200, Actual: x=150
```
**Expected behavior:**
- READ COMMITTED: lost update possible
- REPEATABLE READ (MVCC): T2's write detects write-write conflict → abort
- SERIALIZABLE: same

### Scenario 3: Non-Repeatable Read
```
T1: BEGIN; READ x=100
T2: BEGIN; UPDATE x=200; COMMIT
T1: READ x again → x=200 (different from first read!)
```
**Expected:**
- RC: non-repeatable read occurs
- RR+: T1 sees x=100 both times (snapshot isolation)

### Scenario 4: Phantom Read
```
T1: BEGIN; SELECT * FROM orders WHERE amount > 1000  → {row5, row8}
T2: BEGIN; INSERT INTO orders (id=99, amount=5000); COMMIT
T1: SELECT * FROM orders WHERE amount > 1000  → {row5, row8, row99} PHANTOM!
```
**Expected:**
- RR (MVCC): T1's snapshot prevents phantom (doesn't see T2's insert)
- Serializable: SSI detects write-after-read conflict → abort T2

### Scenario 5: Write Skew
```
Hospital: invariant = at least one doctor on call at all times
T1: BEGIN; READ doctors_on_call = {A, B}; UPDATE doctor_A = off_call; COMMIT
T2: BEGIN; READ doctors_on_call = {A, B}; UPDATE doctor_B = off_call; COMMIT
Both see 2 doctors; both remove one; result: 0 doctors on call!
```
**Expected:**
- RR: write skew occurs (both read overlapping sets, write disjoint keys)
- Serializable (SSI): detects the dangerous structure → aborts one transaction

### Scenario 6: Deadlock Cycle
```
T1: LOCK(account_1); waiting for LOCK(account_2)
T2: LOCK(account_2); waiting for LOCK(account_1)
→ Deadlock! Detector selects victim (T2, younger), aborts T2
T1: proceeds and commits
```

### Scenario 7: Cascading Abort (2PL READ UNCOMMITTED)
```
T1: WRITE x=dirty_value
T2: READ x (reads uncommitted dirty value) 
T1: ABORT
T2: has read data that never existed → must also abort (cascading)
```
**This shows why Read Uncommitted is dangerous.**

---

## Storage Layer

### Schema and Row Model

```go
// internal/types/value.go
type ValueType int
const (TypeNull, TypeInt, TypeFloat, TypeText, TypeBool ValueType = iota, 1, 2, 3, 4)

type Value struct {
    Type    ValueType
    IsNull  bool
    Int     int64
    Float   float64
    Text    string
    Bool    bool
}

// internal/storage/row.go
type RowKey string  // string representation of primary key

type Row struct {
    Key    RowKey
    Values []Value
}

// internal/storage/table.go
type Column struct {
    Name    string
    Type    ValueType
    NotNull bool
    PK      bool
}

type Table struct {
    Name    string
    Columns []Column
    // In 2PL mode: rows stored as map[RowKey][]Value (latest committed only)
    // In MVCC mode: rows are in the MVCCStore (version chains)
    rows    map[RowKey][]Value  // 2PL mode only
    mu      sync.RWMutex
}
```

### Seed Tables

Three tables pre-populated on startup:

**accounts** (100 rows):
```
id INT PK, owner TEXT, balance FLOAT, branch TEXT
Branches: north, south, east, west, central
Invariant: total balance across all accounts = 1,000,000
```

**products** (50 rows):
```
id INT PK, name TEXT, price FLOAT, category TEXT, stock INT
```

**inventory** (50 rows):
```
product_id INT FK, warehouse TEXT, quantity INT
Invariant: sum(inventory.quantity) matches products.stock
```

The `accounts` table is the core TPC-B table. The invariant `SUM(balance) = 1,000,000` can be used to verify no lost updates occur under any isolation level.

---

## API Specification

Base URL: `http://localhost:8080/api`

### Transaction Lifecycle

**`POST /txn/begin`**
```json
// Request
{ "protocol": "mvcc", "isolation": "repeatable_read", "lockTimeout": 5000 }
// Response
{ "txnId": "42", "protocol": "mvcc", "isolation": "repeatable_read", "beginAt": "..." }
```

**`POST /txn/{id}/commit`**
```json
// Response
{ "txnId": "42", "status": "committed", "durationMs": 23, "opsCount": 5 }
// Error (SSI serialization failure)
{ "code": "SERIALIZATION_FAILURE", "message": "SSI detected dangerous structure" }
```

**`POST /txn/{id}/abort`** — always succeeds

**`GET /txn/{id}/status`**
```json
{
  "txnId": "42",
  "status": "active",
  "protocol": "mvcc",
  "isolation": "repeatable_read",
  "locksHeld": [
    { "resource": "row:accounts:1", "mode": "S", "grantedAt": "..." }
  ],
  "waitingFor": null,
  "readSet": ["accounts:1", "accounts:5"],
  "writeSet": ["accounts:1"],
  "undoLogDepth": 2,
  "restartCount": 0,
  "waitTimeMs": 12
}
```

**`GET /txn/active`** — list all active transactions

### Row Operations

**`POST /txn/{id}/read`**
```json
// Request
{ "table": "accounts", "key": "1" }
// Response (hit)
{ "key": "1", "values": {"id": 1, "owner": "Alice", "balance": 1200.00}, "version": {"xmin": 10, "xmax": 0} }
// Response (miss — visibility check failed)
{ "key": "1", "found": false, "reason": "invisible_version" }
```

**`POST /txn/{id}/write`**
```json
// Request
{ "table": "accounts", "key": "1", "values": {"id": 1, "owner": "Alice", "balance": 1150.00}, "op": "update" }
// Response
{ "key": "1", "op": "update", "lockAcquired": "X", "prevVersion": {"xmin": 10} }
// Error (write conflict)
{ "code": "WRITE_CONFLICT", "conflictingTxn": "38" }
```

**`POST /txn/{id}/scan`**
```json
// Request
{ "table": "accounts", "filter": {"column": "balance", "op": ">", "value": 1000} }
// Response
{ "rows": [...], "scanned": 100, "gapLocksAcquired": 3 }
```

**`POST /txn/{id}/insert`** — insert new row (RowKey = primary key value)

**`POST /txn/{id}/delete`** — delete row

### Savepoints

**`POST /txn/{id}/savepoint`** `{ "name": "sp1" }` → saves undo log position

**`POST /txn/{id}/rollback-to`** `{ "name": "sp1" }` → replay undo entries back to sp1

**`DELETE /txn/{id}/savepoint/{name}`** — release savepoint

### Lock Table

**`GET /locks`** — all active lock grants + waiting requests

**`GET /locks/{table}/{key}`** — lock state for one resource

**`GET /deadlocks`** — last 100 deadlock records
```json
{
  "deadlocks": [
    {
      "id": "dl-42",
      "detectedAt": "...",
      "cycle": [15, 18, 15],
      "victim": 18,
      "victimReason": "youngest",
      "cycleEdges": [
        { "from": 15, "to": 18, "resource": "row:accounts:5", "mode": "X" },
        { "from": 18, "to": 15, "resource": "row:accounts:9", "mode": "X" }
      ]
    }
  ]
}
```

### MVCC

**`GET /mvcc/chain/{table}/{key}`** — version chain for a row
```json
{
  "key": "accounts:1",
  "versions": [
    { "xmin": 42, "xmax": 0, "data": {...}, "createdAt": "...", "committed": false },
    { "xmin": 10, "xmax": 42, "data": {...}, "createdAt": "...", "committed": true }
  ],
  "visibilityMatrix": {
    "txn-38": true,
    "txn-39": false
  }
}
```

**`GET /mvcc/stats`** — vacuum stats (total versions, pruned versions, oldest horizon)

**`POST /mvcc/vacuum`** — trigger manual vacuum

### Wait-For Graph

**`GET /wfg`** — current wait-for graph
```json
{
  "nodes": [
    { "txnId": "15", "isolation": "rr", "status": "active", "waitingFor": "18" }
  ],
  "edges": [
    { "from": "15", "to": "18", "resource": "row:accounts:5", "since": "..." }
  ]
}
```

### Scenarios

**`GET /scenarios`** — list all available scenarios with descriptions

**`POST /scenarios/{name}/run`**
```json
// Request
{ "isolation": "read_committed", "protocol": "mvcc" }
// Response
{
  "name": "write_skew",
  "isolation": "read_committed",
  "steps": [
    { "step": 1, "txn": "T1", "op": "BEGIN", "result": "ok", "note": "T1 starts" },
    { "step": 2, "txn": "T1", "op": "READ doctors_on_call", "result": "{A, B}", "note": "" },
    ...
  ],
  "anomalyOccurred": true,
  "anomalyType": "write_skew",
  "explanation": "Both T1 and T2 read the same rows but wrote to different keys..."
}
```

### Benchmark

**`POST /benchmark/run`**
```json
// Request
{ "workload": "tpcb", "concurrency": 8, "durationSec": 30, "protocol": "mvcc", "isolation": "repeatable_read" }
// Response (async, poll for results)
{ "jobId": "bm-1", "status": "running" }
```

**`GET /benchmark/results/{jobId}`**
```json
{
  "throughput": 12340,
  "p50LatencyMs": 0.4,
  "p95LatencyMs": 1.2,
  "p99LatencyMs": 8.9,
  "abortRate": 0.032,
  "deadlockCount": 7,
  "balanceSumInvariant": true,
  "perIsolationComparison": [...]
}
```

### SSE Events

**`GET /sse/events`** — stream of all transaction events:
```
data: {"type":"txn_begin","txnId":"42","isolation":"rr","ts":"..."}
data: {"type":"lock_wait","txnId":"42","resource":"row:accounts:1","mode":"X","blockedBy":"38"}
data: {"type":"lock_granted","txnId":"42","resource":"row:accounts:1","mode":"X"}
data: {"type":"deadlock_detected","victim":"38","cycle":[38,42,38]}
data: {"type":"txn_commit","txnId":"42","durationMs":23}
data: {"type":"vacuum_run","pruned":142,"remaining":891}
```

**`GET /sse/wfg`** — streams wait-for graph changes only (for the graph visualizer)

---

## Frontend: Page Specifications

### Dashboard (`/`)

Four metric cards at top (live via SSE):
- Active transactions (count by isolation level as color segments)
- Transactions/sec (rolling 10s average)
- Abort rate % (aborts / (commits + aborts))
- Deadlocks detected (since server start)

Transaction table below: one row per active transaction.
Columns: ID, Protocol, Isolation, Status, Locks Held, Waiting For, Duration, Restart Count.
Live-updating with SSE event feed.

Rolling 60s line chart: Commits/s and Aborts/s overlaid.

### Playground (`/playground`)

Split-pane interface: up to 3 simultaneous transaction windows (tabs).

Each transaction window:
- Protocol selector: `2PL` / `MVCC`
- Isolation selector: `RU` / `RC` / `RR` / `Serializable`
- BEGIN button
- Operation builder: table selector, key input, op type (READ/WRITE/SCAN/INSERT/DELETE), value editor
- EXECUTE button (keyboard: Ctrl+Enter)
- COMMIT / ABORT / SAVEPOINT buttons
- Results log: scrolling list of operations + results + errors

Live lock status panel: which locks this transaction holds + any it's waiting for.

Write conflict / deadlock alert: toast notification when conflict detected on any transaction.

### Wait-For Graph (`/wfg`)

Full-page React Flow canvas.
- Nodes: transaction circles (colored by isolation level)
- Edges: directed arrows = "T_i waits for T_j" (labeled with resource + mode)
- Deadlock edges: highlighted red
- Node tooltip: txn ID, isolation, how long waiting
- Auto-layout via dagre, re-layout on change
- History panel: last 20 deadlock events with cycle path display
- "Simulate Deadlock" button: auto-starts a deadlock scenario

### Version Chains (`/versions`)

Table selector dropdown. Key input.
Shows version chain for that row as a horizontal timeline:

```
[V1: xmin=10 ✓committed xmax=42] → [V2: xmin=42 ✗active xmax=0 CURRENT]
```

Each version: bubble with xmin/xmax badges, values, timestamp.
Committed = green border, Active = blue border, Aborted = red strikethrough.

Visibility matrix: table showing which active transactions can see which version.

"All chains" view: paginated table of all rows with version count + latest values.

Vacuum controls: oldest horizon display, manual vacuum button, versions pruned counter.

### Scenarios (`/scenarios`)

Grid of scenario cards: name, description, which anomaly it demonstrates, required isolation levels.

Click a scenario → open step-through player:
- Timeline at top showing T1, T2, T3 tracks
- Step-by-step execution with NEXT / AUTO PLAY buttons
- Each step: highlights the current operation on the timeline + shows the result
- Final state panel: what the database looks like, was the anomaly detected?
- "Run at different isolation level" controls: show prevention vs occurrence

### Benchmarks (`/benchmarks`)

Config: protocol, isolation level, concurrency (1/2/4/8/16), duration.

Progress: live counter of committed/aborted txns while running.

Results after completion:
- Bar chart: throughput at different concurrency levels
- Line chart: latency percentiles (P50/P95/P99) vs concurrency
- Bar chart: abort rate by isolation level
- Invariant check panel: "SUM(accounts.balance) = 1,000,000?" ✓/✗

Comparison mode: run 2PL vs MVCC under same workload, overlay results.

---

## Error Catalog

```go
type ErrorCode string
const (
    ErrDeadlock              ErrorCode = "DEADLOCK"
    ErrWriteConflict         ErrorCode = "WRITE_CONFLICT"
    ErrSerializationFailure  ErrorCode = "SERIALIZATION_FAILURE"
    ErrLockTimeout           ErrorCode = "LOCK_TIMEOUT"
    ErrTxnAborted            ErrorCode = "TXN_ABORTED"
    ErrTxnNotFound           ErrorCode = "TXN_NOT_FOUND"
    ErrRowNotFound           ErrorCode = "ROW_NOT_FOUND"
    ErrDuplicateKey          ErrorCode = "DUPLICATE_KEY"
    ErrTableNotFound         ErrorCode = "TABLE_NOT_FOUND"
    ErrInvalidIsolation      ErrorCode = "INVALID_ISOLATION"
    ErrSavepointNotFound     ErrorCode = "SAVEPOINT_NOT_FOUND"
    ErrMaxRetriesExceeded    ErrorCode = "MAX_RETRIES_EXCEEDED"
)

type TxnError struct {
    Code           ErrorCode `json:"code"`
    Message        string    `json:"message"`
    TxnID          TxnID     `json:"txnId,omitempty"`
    ConflictingTxn TxnID     `json:"conflictingTxn,omitempty"`
    Resource       string    `json:"resource,omitempty"`
    RetryAfterMs   int       `json:"retryAfterMs,omitempty"`
}
```

---

## TPC-B Workload

Classic banking debit/credit transfer. Each transaction:
1. BEGIN with configured isolation level
2. SELECT balance FROM accounts WHERE id = $random_account (T1 → read debit account)
3. SELECT balance FROM accounts WHERE id = $other_account (T2 → read credit account)
4. UPDATE accounts SET balance = balance - 100 WHERE id = $debit_account
5. UPDATE accounts SET balance = balance + 100 WHERE id = $credit_account
6. COMMIT (or ABORT on conflict, retry up to 3 times)

**Verification:** After every concurrent run, execute:
```sql
SELECT SUM(balance) FROM accounts
```
If result ≠ 1,000,000 → isolation is insufficient (lost update or write skew occurred).

This is the most important correctness check for the whole system.

---

## Metrics

```go
type Metrics struct {
    // Counters
    TxnBegins    atomic.Int64
    TxnCommits   atomic.Int64
    TxnAborts    atomic.Int64
    Deadlocks    atomic.Int64
    LockTimeouts atomic.Int64
    WriteConflicts atomic.Int64
    SSIAborts    atomic.Int64

    // Per operation
    ReadsTotal   atomic.Int64
    WritesTotal  atomic.Int64
    ScansTotal   atomic.Int64

    // MVCC
    VersionsCreated  atomic.Int64
    VersionsPruned   atomic.Int64
    VacuumRuns       atomic.Int64

    // Latency histograms (lock wait time)
    LockWaitHist  *Histogram  // buckets: 0, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s, ∞
    TxnDurHist    *Histogram  // transaction duration distribution
}
```

---

## Non-Goals (v1)

- Network distribution / distributed transactions
- Disk persistence / WAL recovery
- B-tree indexes (linear scan only)
- SQL parser (operations via API JSON)
- Replication
- Two-phase commit (2PC) across nodes
- Flashback / time-travel queries (though version chains support it conceptually)