# Claude Code Master Prompt — Transaction Manager

Read this entire document. Then read `SPEC.md`, `CHECKLIST.md`, and `ALGORITHMS.md`. Build in the exact phase order defined in CHECKLIST.md. Do not skip phases. Do not write a single line of code before reading all four documents.

---

## What You Are Building

A production-grade in-memory transaction manager implementing two complete concurrency control paradigms with all four SQL isolation levels:

```
2PL Path:
  Begin → AcquireLocks(IS/IX/S/SIX/X/U) → [DeadlockDetect/Prevention] → Commit/Abort → ReleaseLocks

MVCC Path:
  Begin → TakeSnapshot(Xmin,Xmax,Active) → Read(VersionChain.FindVisible) → Write(NewVersion+XMax) → [SSI Check] → Commit/Abort

Both paths share:
  TxnManager → UndoLog → Savepoints → SSEBus → Metrics
```

---

## Non-Negotiable Rules

1. **Phase order is mandatory.** `go test ./... -race` must pass before moving to the next phase.
2. **No external concurrency libraries.** Every lock, wait queue, and deadlock algorithm is hand-written.
3. **Lock compatibility matrix must be exact.** Verify all 49 cells with a unit test before writing any lock acquisition code.
4. **MVCC visibility must be correct.** The `IsVisible` function is the most critical function in the project. Write 12+ tests covering every visibility case before using it in reads.
5. **SSI must correctly detect write skew.** The doctor on-call scenario must abort one transaction at Serializable level and succeed at RepeatableRead. Test this explicitly.
6. **TPC-B invariant must hold.** `SUM(accounts.balance) == 1,000,000` after every concurrent benchmark run. A failure here means a correctness bug — not a performance bug.
7. **All concurrent code passes `-race`.** Run `go test ./... -race -count=1` before every phase transition.
8. **TypeScript strict mode, no `any`.** `"strict": true` in tsconfig.

---

## Phase 0 — Setup

```bash
mkdir txn-manager && cd txn-manager
go mod init github.com/yourname/txn-manager
go get github.com/go-chi/chi/v5 github.com/stretchr/testify/assert github.com/stretchr/testify/require

mkdir -p cmd/server
mkdir -p internal/{types,storage,txn,lock,deadlock,mvcc,isolation,operations,metrics,benchmark,scenario}
mkdir -p api

cd web
npm create vite@latest . -- --template react-ts
npm install tailwindcss @tailwindcss/vite framer-motion @xyflow/react @dagrejs/dagre @dagrejs/graphlib recharts zustand ky
npx shadcn@latest init
npx shadcn@latest add button badge card tabs select separator scroll-area tooltip sheet dialog alert
```

`Makefile`:
```makefile
.PHONY: build test race bench run dev lint

build:
	go build -o bin/server ./cmd/server

test:
	go test ./... -v -count=1

race:
	go test ./... -race -count=1

bench:
	go test ./internal/... -bench=. -benchmem -benchtime=5s

run:
	go run ./cmd/server

dev:
	make run & sleep 1 && (cd web && npm run dev)

lint:
	go vet ./...
	cd web && npx tsc --noEmit
```

---

## Phase 1 — Value Types (Start Here)

Write the test first:
```go
func TestValue_Compare(t *testing.T) {
    // Int comparisons
    assert.Equal(t, -1, MustCompare(IntVal(1), IntVal(2)))
    assert.Equal(t, 0,  MustCompare(IntVal(5), IntVal(5)))
    assert.Equal(t, 1,  MustCompare(IntVal(9), IntVal(3)))
    // Int-Float coercion
    assert.Equal(t, 0,  MustCompare(IntVal(3), FloatVal(3.0)))
    // Null handling
    _, err := IntVal(1).Compare(NullVal())
    assert.ErrorIs(t, err, ErrNullComparison)
}
```

Then implement.

---

## Phase 5 — Lock Compatibility Matrix

**Write the compatibility test BEFORE any lock acquisition code:**

```go
func TestLockCompatibilityMatrix(t *testing.T) {
    // Truth table from SPEC.md — verify all 49 cells
    cases := []struct{ req, held LockMode; want bool }{
        // NL is compatible with everything
        {LockNL, LockNL, true}, {LockNL, LockX, true},
        // IS is compatible with IS, IX, S, SIX, U but NOT X
        {LockIS, LockIS, true}, {LockIS, LockIX, true},
        {LockIS, LockS, true},  {LockIS, LockSIX, true},
        {LockIS, LockX, false}, {LockIS, LockU, true},
        // IX compatible with IS, IX but NOT S, SIX, X, U
        {LockIX, LockIS, true},  {LockIX, LockIX, true},
        {LockIX, LockS, false},  {LockIX, LockSIX, false},
        {LockIX, LockX, false},  {LockIX, LockU, false},
        // S compatible with IS, S, U but NOT IX, SIX, X
        {LockS, LockIS, true},   {LockS, LockIX, false},
        {LockS, LockS, true},    {LockS, LockSIX, false},
        {LockS, LockX, false},   {LockS, LockU, true},
        // SIX compatible with IS, IX but NOT S, SIX, X, U
        {LockSIX, LockIS, true},  {LockSIX, LockIX, false},
        {LockSIX, LockS, false},  {LockSIX, LockSIX, false},
        {LockSIX, LockX, false},  {LockSIX, LockU, false},
        // X compatible with nothing except NL
        {LockX, LockNL, true},  {LockX, LockIS, false},
        {LockX, LockIX, false}, {LockX, LockS, false},
        {LockX, LockSIX, false},{LockX, LockX, false},
        {LockX, LockU, false},
        // U compatible with IS, S but NOT IX, SIX, X, U
        {LockU, LockIS, true},  {LockU, LockIX, false},
        {LockU, LockS, true},   {LockU, LockSIX, false},
        {LockU, LockX, false},  {LockU, LockU, false},
    }
    for _, c := range cases {
        got := LockCompatible[c.req][c.held]
        assert.Equal(t, c.want, got, "LockCompatible[%v][%v]", c.req, c.held)
    }
}
```

If this test fails, do not proceed to Phase 6.

---

## Phase 6 — Lock Queue: The Most Critical Synchronization Point

The lock queue is where correctness and performance intersect. Get this exactly right.

```go
// LockQueue.GrantWaiters is called after a grant is released.
// It must grant all compatible waiting requests (not just the first one).
// Multiple S-lock waiters can all be granted simultaneously.
func (q *LockQueue) GrantWaiters() {
    // Build current granted mode (excluding any just-released lock)
    var currentMode LockMode = LockNL
    for _, g := range q.granted {
        currentMode = DominantMode(currentMode, g.Mode)
    }
    
    // Iterate wait queue in FIFO order
    remaining := q.waiting[:0]
    for _, req := range q.waiting {
        if LockCompatible[req.Mode][currentMode] {
            // Grant this request
            req.Status = LockStatusGranted
            req.GrantedAt = time.Now()
            q.granted = append(q.granted, req)
            currentMode = DominantMode(currentMode, req.Mode)
            close(req.grantedCh)  // wake the waiting goroutine
        } else {
            // Stop at first incompatible request (preserve FIFO for fairness)
            // BUT: continue checking if this mode allows sharing (S+S case)
            // Simple rule: stop after first incompatible to prevent starvation
            remaining = append(remaining, req)
            // Continue scanning for requests compatible with current (not the blocker)
            // This handles: S(granted) + S(waiting1 granted) + X(waiting2 blocked) + S(waiting3 granted)
            // In practice, strict FIFO is safer — don't grant past an incompatible waiter
        }
    }
    q.waiting = remaining
}
```

**Critical: FIFO fairness.** Do NOT grant a request that is behind an incompatible request in the queue. This prevents starvation of X-lock waiters by a stream of S-lock holders. However, you CAN grant multiple simultaneous S requests that are all at the front of the queue before the first X waiter.

**The grantedCh closing pattern:** Use `close(req.grantedCh)` to wake waiters — a closed channel is immediately readable by all blocked goroutines (not just one). Never use `req.grantedCh <- struct{}{}` (send-based) because if the waiter timed out, the send blocks forever and leaks the goroutine.

**Lock acquisition with timeout:**
```go
func (t *LockTable) Acquire(txn *Transaction, resource ResourceID, mode LockMode) error {
    q := t.GetOrCreate(resource)
    q.mu.Lock()

    // Check if we already hold this lock (or stronger)
    for _, g := range q.granted {
        if g.TxnID == txn.ID {
            if LockStrength[g.Mode] >= LockStrength[mode] {
                q.mu.Unlock()
                return nil // already holds sufficient lock
            }
            // Need upgrade — handled separately
        }
    }

    req := &LockRequest{
        TxnID:     txn.ID,
        Mode:      mode,
        grantedCh: make(chan struct{}),
        cancelCh:  make(chan struct{}),
        RequestAt: time.Now(),
    }

    if q.CanGrant(mode, txn.ID) {
        req.Status = LockStatusGranted
        req.GrantedAt = time.Now()
        q.granted = append(q.granted, req)
        txn.mu.Lock()
        txn.LocksHeld[resource] = mode
        txn.mu.Unlock()
        q.mu.Unlock()
        return nil
    }

    // Conflict: apply prevention policy
    conflictors := q.ConflictingTxns(mode, txn.ID)
    if txn.PreventionPolicy != PolicyDetect && len(conflictors) > 0 {
        holder := txnManager.GetTxn(conflictors[0])
        action := txn.PreventionPolicy.Evaluate(txn, holder)
        switch action {
        case ActionDie:
            q.mu.Unlock()
            return &TxnError{Code: ErrTxnAborted, Message: "wait-die: younger transaction aborted"}
        case ActionWound:
            txnManager.Abort(holder.ID, ErrWounded) // preempt holder
            // After wounding, try to grant (holder will release locks)
        }
    }

    // Add to wait queue, register in wait-for graph
    req.Status = LockStatusWaiting
    q.waiting = append(q.waiting, req)
    for _, conflictorID := range conflictors {
        txn.waitForGraph.AddEdge(txn.ID, conflictorID)
    }
    q.mu.Unlock()

    // Block until granted, aborted, or timed out
    timer := time.NewTimer(txn.LockTimeout)
    defer timer.Stop()
    select {
    case <-req.grantedCh:
        txn.waitForGraph.RemoveEdges(txn.ID)
        txn.mu.Lock()
        txn.LocksHeld[resource] = mode
        txn.mu.Unlock()
        return nil
    case <-txn.abortCh:
        q.mu.Lock()
        q.RemoveWaiting(txn.ID)
        q.mu.Unlock()
        txn.waitForGraph.RemoveEdges(txn.ID)
        return &TxnError{Code: ErrTxnAborted}
    case <-timer.C:
        q.mu.Lock()
        q.RemoveWaiting(txn.ID)
        q.mu.Unlock()
        txn.waitForGraph.RemoveEdges(txn.ID)
        return &TxnError{Code: ErrLockTimeout}
    }
}
```

---

## Phase 7 — Deadlock Detection

**DFS cycle detection with path reconstruction:**

```go
func (g *WaitForGraph) FindCycle() (victim TxnID, cycle []TxnID, found bool) {
    g.mu.RLock()
    defer g.mu.RUnlock()

    color := make(map[TxnID]int) // 0=white, 1=gray, 2=black
    parent := make(map[TxnID]TxnID)
    var cycleStart, cycleEnd TxnID

    var dfs func(v TxnID) bool
    dfs = func(v TxnID) bool {
        color[v] = 1 // gray: in current DFS path
        for w := range g.edges[v] {
            if color[w] == 1 { // back edge = cycle
                cycleStart, cycleEnd = w, v
                return true
            }
            if color[w] == 0 {
                parent[w] = v
                if dfs(w) { return true }
            }
        }
        color[v] = 2 // black: done
        return false
    }

    for node := range g.edges {
        if color[node] == 0 {
            if dfs(node) {
                // Reconstruct cycle: cycleStart → ... → cycleEnd → cycleStart
                cycle = []TxnID{cycleStart}
                for cur := cycleEnd; cur != cycleStart; cur = parent[cur] {
                    cycle = append([]TxnID{cur}, cycle...)
                }
                cycle = append(cycle, cycleStart) // close the cycle
                victim = selectVictim(cycle)
                return victim, cycle, true
            }
        }
    }
    return 0, nil, false
}

// Victim = youngest transaction (highest ID) — has done the least work
func selectVictim(cycle []TxnID) TxnID {
    var max TxnID
    for _, id := range cycle {
        if id > max { max = id }
    }
    return max
}
```

**Background detector goroutine:**
```go
func (d *DeadlockDetector) Start() {
    go func() {
        ticker := time.NewTicker(50 * time.Millisecond)
        defer ticker.Stop()
        for {
            select {
            case <-ticker.C:
                victim, cycle, found := d.graph.FindCycle()
                if found {
                    record := &DeadlockRecord{
                        ID:          fmt.Sprintf("dl-%d", time.Now().UnixNano()),
                        DetectedAt:  time.Now(),
                        Cycle:       cycle,
                        Victim:      victim,
                        VictimReason: "youngest",
                    }
                    d.history.Add(record)
                    d.metrics.Deadlocks.Add(1)
                    d.txnMgr.Abort(victim, ErrDeadlock)
                }
            case <-d.stop:
                return
            }
        }
    }()
}
```

**Important:** The detector runs concurrently with lock acquisition. The `WaitForGraph` uses `sync.RWMutex` — FindCycle takes RLock (many concurrent readers OK). AddEdge/RemoveNode take Lock (exclusive). This is correct — the graph can be read for cycle detection while lock acquisition adds edges.

---

## Phase 10-11 — MVCC: The IsVisible Function

This is the most important function in the entire project. Write it with maximum care.

```go
// IsVisible reports whether version v is visible to transaction txn
// using the given snapshot snap.
// mgr is needed to check committed/aborted status of other transactions.
func IsVisible(v *Version, txn *Transaction, snap *Snapshot, mgr *TxnManager) bool {
    xmin := v.XMin
    xmax := v.XMax

    // ── Step 1: Is the creator (xmin) visible? ─────────────────────────────
    // Case A: Created by our own transaction
    selfCreated := (xmin == txn.ID)

    // Case B: Created by a transaction committed before our snapshot
    committedBeforeSnapshot := (xmin < snap.Xmin)

    // Case C: Created by a transaction that committed during our snapshot window
    // (between Xmin and Xmax) but was not active when we took the snapshot
    committedDuringWindow := (xmin >= snap.Xmin &&
        xmin < snap.Xmax &&
        mgr.IsCommitted(xmin) &&
        !snap.Contains(xmin))

    xminVisible := selfCreated || committedBeforeSnapshot || committedDuringWindow
    if !xminVisible {
        return false
    }

    // ── Step 2: Is the deletion (xmax) invisible to us? ────────────────────
    // If xmax == 0: not deleted, visible
    if xmax == 0 {
        return true
    }

    // Deleted by our own transaction: we see it as deleted
    if xmax == txn.ID {
        return false // we deleted it, so it's gone from our perspective
    }

    // xmax is a transaction that was still running when we took our snapshot
    if snap.Contains(xmax) {
        return true // deleter was active → deletion not committed → we still see the row
    }

    // xmax started after our snapshot → we don't see its deletion
    if xmax >= snap.Xmax {
        return true
    }

    // xmax has aborted → deletion rolled back → we still see the row
    if mgr.IsAborted(xmax) {
        return true
    }

    // xmax committed before our snapshot → deletion is visible to us → row gone
    return false
}
```

**Write these tests before using IsVisible in reads:**

```go
func TestMVCC_Visibility(t *testing.T) {
    mgr := NewTxnManager()

    t.Run("committed_before_snapshot_visible", func(t *testing.T) {
        // xmin=5 committed, snapshot.Xmin=10 → 5 < 10 → visible
        snap := &Snapshot{Xmin: 10, Xmax: 20, Active: nil}
        v := &Version{XMin: 5, XMax: 0}
        mgr.setCommitted(5)
        assert.True(t, IsVisible(v, makeTxn(15), snap, mgr))
    })

    t.Run("active_xmin_not_visible", func(t *testing.T) {
        // xmin=12 still active (in Active list) → not visible to others
        snap := &Snapshot{Xmin: 10, Xmax: 20, Active: []TxnID{12}}
        v := &Version{XMin: 12, XMax: 0}
        assert.False(t, IsVisible(v, makeTxn(15), snap, mgr))
    })

    t.Run("own_version_visible", func(t *testing.T) {
        // xmin == txn.ID → self-written → visible
        snap := &Snapshot{Xmin: 10, Xmax: 20, Active: []TxnID{15}}
        v := &Version{XMin: 15, XMax: 0}
        assert.True(t, IsVisible(v, makeTxn(15), snap, mgr))
    })

    t.Run("deleted_by_committed_txn_invisible", func(t *testing.T) {
        // xmin committed, xmax committed before snapshot → row deleted
        snap := &Snapshot{Xmin: 10, Xmax: 20, Active: nil}
        v := &Version{XMin: 5, XMax: 8}
        mgr.setCommitted(5); mgr.setCommitted(8)
        assert.False(t, IsVisible(v, makeTxn(15), snap, mgr))
    })

    t.Run("deleted_by_active_txn_still_visible", func(t *testing.T) {
        // xmax is active → deletion not committed → we still see the row
        snap := &Snapshot{Xmin: 10, Xmax: 20, Active: []TxnID{14}}
        v := &Version{XMin: 5, XMax: 14}
        mgr.setCommitted(5)
        assert.True(t, IsVisible(v, makeTxn(15), snap, mgr))
    })

    t.Run("deleted_by_aborted_txn_still_visible", func(t *testing.T) {
        // xmax aborted → deletion rolled back → row visible
        snap := &Snapshot{Xmin: 10, Xmax: 20, Active: nil}
        v := &Version{XMin: 5, XMax: 13}
        mgr.setCommitted(5); mgr.setAborted(13)
        assert.True(t, IsVisible(v, makeTxn(15), snap, mgr))
    })

    t.Run("read_committed_fresh_snapshot_each_statement", func(t *testing.T) {
        // RC: first read sees snapshot{active=[12]}, second read sees snapshot{active=[]} (12 committed)
        // Should see version created by 12 on second read
        // Test by constructing two different snapshots for the same txn
    })
}
```

---

## Phase 12 — MVCC Writes: The Write-Write Conflict

Write-write conflict (first-writer-wins) is what prevents lost updates at RepeatableRead level:

```go
func CheckWriteConflict(txn *Transaction, chain *VersionChain, mgr *TxnManager) error {
    // Walk version chain to find if any concurrent transaction has written this row
    // "concurrent" = started after our snapshot Xmin and was active when we began
    head := chain.Head()
    for v := head; v != nil; v = v.Prev {
        if v.XMin == txn.ID {
            continue // our own write, no conflict
        }
        // Is this writer concurrent with us?
        if mgr.IsActive(v.XMin) && v.XMin != txn.ID {
            return &TxnError{
                Code:           ErrWriteConflict,
                ConflictingTxn: v.XMin,
                Message:        "concurrent transaction already modified this row",
            }
        }
        // Has a concurrent transaction (active at our start, now committed) written it?
        if txn.Snapshot.Contains(v.XMin) && mgr.IsCommitted(v.XMin) {
            return &TxnError{
                Code:           ErrWriteConflict,
                ConflictingTxn: v.XMin,
                Message:        "concurrent transaction committed a conflicting write",
            }
        }
    }
    return nil
}
```

**What about the XMax conflict?** When T1 updates row R (sets old.XMax = T1.ID), and T2 also wants to update R, T2 will find that the visible version has XMax = T1.ID (which is active). This signals a write-write conflict — T1 already "claimed" this row.

---

## Phase 13 — SSI: The Write Skew Test

This is the key correctness test for Serializable isolation:

```go
func TestSSI_WriteSkew(t *testing.T) {
    // Doctor on-call scenario:
    // Invariant: at least one doctor (A or B) must be on_call=true
    // T1 reads both, sees 2 on call, sets A to off
    // T2 reads both, sees 2 on call, sets B to off
    // Result without SSI: 0 doctors on call (invariant broken)
    // Result with SSI: T2 gets serialization failure

    mgr := NewTxnManager()
    setupDoctors(mgr) // insert doctor_A{on_call=true}, doctor_B{on_call=true}

    t1, _ := mgr.Begin(ProtocolMVCC, Serializable, 0)
    t2, _ := mgr.Begin(ProtocolMVCC, Serializable, 0)

    // T1 reads both doctors
    mgr.MVCCRead(t1, "doctors", "A")
    mgr.MVCCRead(t1, "doctors", "B")

    // T2 reads both doctors  
    mgr.MVCCRead(t2, "doctors", "A")
    mgr.MVCCRead(t2, "doctors", "B")

    // T1 writes doctor A off-call
    err := mgr.MVCCUpdate(t1, "doctors", "A", offCall)
    require.NoError(t, err)

    // T2 writes doctor B off-call
    err = mgr.MVCCUpdate(t2, "doctors", "B", offCall)
    require.NoError(t, err)

    // T1 commits first
    err = mgr.Commit(t1.ID)
    require.NoError(t, err) // T1 has outConflict=true but inConflict=false → not pivot yet

    // T2 commits → T2 has inConflict=true (T1 wrote what T2 read... wait)
    // Actually: T2 reads A and B (SIREADs on A,B)
    //           T1 writes A → T2.inConflict=true (T_other wrote what T_self read)
    //           T2 writes B → T1.outConflict=true (T_self wrote what T_other reads)
    //           Wait, let me re-trace:
    //
    // SSITracker.RecordRead(T2, "A"), RecordRead(T2, "B")
    // SSITracker.RecordWrite(T1, "A"):
    //   → siread["A"] contains T2 → record T2.inConflict=true, T1.outConflict=true
    // SSITracker.RecordRead(T1, "A"), RecordRead(T1, "B")  ← T1 also reads both
    // SSITracker.RecordWrite(T2, "B"):
    //   → siread["B"] contains T1 → record T1.inConflict=true, T2.outConflict=true
    //
    // T1 at commit: inConflict=true, outConflict=true → ABORT T1 (pivot!)
    // OR T2 at commit: inConflict=true, outConflict=true → ABORT T2
    //
    // Typically: first to commit survives; second sees the conflict

    err = mgr.Commit(t2.ID)
    assert.ErrorIs(t, err, ErrSerializationFailure,
        "SSI must abort T2: write skew would violate doctor on-call invariant")

    // Verify invariant: at least one doctor still on call
    onCall := countOnCall(mgr)
    assert.GreaterOrEqual(t, onCall, 1, "invariant: at least one doctor on call")
}
```

**Important SSI detail:** The `RecordRead` calls must happen BEFORE the `RecordWrite` calls for SSI to detect the conflict. In the scenario above, T1 and T2 both read before writing — so siread entries exist when the writes happen. If T1 wrote before T2 read, there would be no rw-anti-dependency from T2 to T1 (T2 would see T1's write and either conflict or read the updated value).

**Simplification:** For correctness with the pivot check, track both directions:
- `InConflict`: someone wrote something I read (T_other →rw T_self)
- `OutConflict`: I wrote something someone else read (T_self →rw T_other)

A transaction is a pivot (and must abort) if it has BOTH InConflict AND OutConflict with respect to transactions that are committed or will commit.

---

## Phase 14 — TxnManager: Abort is the Hardest Operation

Abort must correctly handle both 2PL and MVCC paths and be safe to call concurrently:

```go
func (m *TxnManager) Abort(txnID TxnID, reason error) error {
    m.mu.Lock()
    txn, ok := m.txns[txnID]
    if !ok {
        m.mu.Unlock()
        return nil // already aborted or committed
    }
    if txn.Status != TxnActive {
        m.mu.Unlock()
        return nil // idempotent
    }
    txn.Status = TxnAborted
    delete(m.txns, txnID)
    m.aborted[txnID] = struct{}{}
    m.mu.Unlock()

    // Signal all goroutines blocked waiting for this txn's abort channel
    // (e.g., goroutines blocked in AcquireLock waiting for this txn's lock)
    select {
    case <-txn.abortCh: // already closed
    default:
        close(txn.abortCh)
    }

    // ── 2PL: apply undo log in reverse, then release locks ─────────────────
    if txn.Protocol == Protocol2PL {
        m.applyUndoLog(txn) // restore before-images to catalog storage
        m.lockTable.ReleaseAllLocks(txn)
    }

    // ── MVCC: revert all version writes ────────────────────────────────────
    if txn.Protocol == ProtocolMVCC {
        // For every version this txn created (XMin == txnID): mark chain as if it never existed
        // Implementation: walk mvccStore, find chains where head.XMin == txnID
        // Option A: for each key in txn.WriteSet, get chain, remove our version
        for key := range txn.WriteSet {
            table, rowKey := parseKey(key)
            chain, exists := m.mvccStore.GetChain(table, rowKey)
            if !exists { continue }
            chain.mu.Lock()
            // Remove versions with XMin == txnID
            chain.RemoveByXMin(txnID)
            // Restore any XMax == txnID we set (un-delete those versions)
            chain.ClearXMax(txnID)
            chain.mu.Unlock()
        }
    }

    // ── SSI cleanup ────────────────────────────────────────────────────────
    m.ssiTracker.Cleanup(txn)

    // ── Emit event ─────────────────────────────────────────────────────────
    m.eventBus.Emit(Event{Type: "txn_abort", TxnID: txnID, Reason: reason.Error()})
    m.metrics.TxnAborts.Add(1)

    return nil
}
```

**The MVCC undo during abort is critical:** When T aborts:
1. Versions it created (`XMin == T.ID`): must be made invisible → remove from chain
2. Versions it marked deleted (`XMax == T.ID`): deletion must be undone → set `XMax = 0` on those versions

This is why the `txn.WriteSet` tracks all keys written — we need to find these version chains efficiently on abort.

---

## Phase 18 — TPC-B Benchmark Correctness

The benchmark MUST verify the balance invariant. Make this a first-class concern:

```go
func (b *BenchmarkRunner) VerifyInvariant(mgr *TxnManager) (bool, float64) {
    // Start a read-only transaction at Serializable level
    txn, _ := mgr.Begin(ProtocolMVCC, Serializable, 30*time.Second)
    defer mgr.Abort(txn.ID, nil)

    rows, _ := mgr.MVCCScan(txn, "accounts", func(row Row) bool { return true })

    var sum float64
    for _, row := range rows {
        balance := row.Values[balanceCol].Float
        sum += balance
    }

    _ = mgr.Commit(txn.ID)
    return math.Abs(sum-1_000_000.0) < 0.01, sum
}
```

Run this after every benchmark and every concurrency test. A failing invariant is a correctness bug.

**TPC-B transaction with retry:**
```go
func (b *BenchmarkRunner) RunOneTPCB(mgr *TxnManager, isolation IsolationLevel) error {
    const maxRetries = 3
    for attempt := 0; attempt < maxRetries; attempt++ {
        err := b.tryTPCB(mgr, isolation)
        if err == nil {
            return nil
        }
        var txnErr *TxnError
        if errors.As(err, &txnErr) {
            switch txnErr.Code {
            case ErrWriteConflict, ErrDeadlock, ErrSerializationFailure:
                // Retriable — exponential backoff
                time.Sleep(time.Duration(1<<attempt) * time.Millisecond)
                continue
            }
        }
        return err // non-retriable error
    }
    return &TxnError{Code: ErrMaxRetriesExceeded}
}

func (b *BenchmarkRunner) tryTPCB(mgr *TxnManager, isolation IsolationLevel) error {
    txn, err := mgr.Begin(ProtocolMVCC, isolation, 5*time.Second)
    if err != nil { return err }

    // Pick two random accounts
    debitID := fmt.Sprintf("%d", rand.Intn(100)+1)
    creditID := fmt.Sprintf("%d", rand.Intn(100)+1)
    for creditID == debitID { creditID = fmt.Sprintf("%d", rand.Intn(100)+1) }

    // Read both balances
    debitRow, ok, err := mgr.Read(txn, "accounts", debitID)
    if err != nil { mgr.Abort(txn.ID, err); return err }
    if !ok { mgr.Abort(txn.ID, nil); return nil }

    creditRow, ok, err := mgr.Read(txn, "accounts", creditID)
    if err != nil { mgr.Abort(txn.ID, err); return err }
    if !ok { mgr.Abort(txn.ID, nil); return nil }

    // Compute new balances
    amount := 100.0
    newDebit := debitRow[balanceCol].Float - amount
    newCredit := creditRow[balanceCol].Float + amount

    // Write both
    if err := mgr.Write(txn, "accounts", debitID, withBalance(debitRow, newDebit)); err != nil {
        mgr.Abort(txn.ID, err); return err
    }
    if err := mgr.Write(txn, "accounts", creditID, withBalance(creditRow, newCredit)); err != nil {
        mgr.Abort(txn.ID, err); return err
    }

    return mgr.Commit(txn.ID)
}
```

---

## Phase 25 — Wait-For Graph Frontend (React Flow)

```tsx
// web/src/components/graph/WFGCanvas.tsx
import ReactFlow, { Node, Edge, useNodesState, useEdgesState, Controls, Background } from '@xyflow/react';
import dagre from '@dagrejs/dagre';

const NODE_WIDTH = 160;
const NODE_HEIGHT = 70;

function layoutGraph(nodes: Node[], edges: Edge[]): { nodes: Node[], edges: Edge[] } {
    const g = new dagre.graphlib.Graph();
    g.setDefaultEdgeLabel(() => ({}));
    g.setGraph({ rankdir: 'LR', nodesep: 80, ranksep: 100 });

    nodes.forEach(n => g.setNode(n.id, { width: NODE_WIDTH, height: NODE_HEIGHT }));
    edges.forEach(e => g.setEdge(e.source, e.target));
    dagre.layout(g);

    return {
        nodes: nodes.map(n => {
            const pos = g.node(n.id);
            return { ...n, position: { x: pos.x - NODE_WIDTH/2, y: pos.y - NODE_HEIGHT/2 } };
        }),
        edges,
    };
}

export function WFGCanvas({ wfgState }: { wfgState: WFGState }) {
    const rfNodes: Node[] = wfgState.nodes.map(n => ({
        id: String(n.txnId),
        type: 'txnNode',
        data: n,
        position: { x: 0, y: 0 }, // dagre will set this
    }));

    const rfEdges: Edge[] = wfgState.edges.map(e => ({
        id: `${e.from}-${e.to}`,
        source: String(e.from),
        target: String(e.to),
        type: 'conflictEdge',
        data: e,
        animated: e.isDeadlock,
        style: { stroke: e.isDeadlock ? '#ef4444' : '#94a3b8', strokeWidth: e.isDeadlock ? 3 : 1.5 },
        label: `${e.resource} (${e.mode})`,
    }));

    const { nodes: laid, edges: laidEdges } = layoutGraph(rfNodes, rfEdges);
    const [nodes, , onNodesChange] = useNodesState(laid);
    const [edges, , onEdgesChange] = useEdgesState(laidEdges);

    return (
        <div style={{ height: '100%', width: '100%' }}>
            <ReactFlow
                nodes={nodes}
                edges={edges}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                nodeTypes={{ txnNode: TxnNode }}
                edgeTypes={{ conflictEdge: ConflictEdge }}
                fitView
            >
                <Controls />
                <Background />
            </ReactFlow>
        </div>
    );
}
```

**TxnNode** (custom React Flow node):
```tsx
function TxnNode({ data }: { data: WFGNodeData }) {
    const isolationColor = {
        read_uncommitted: 'bg-gray-100 border-gray-300',
        read_committed:   'bg-blue-50  border-blue-300',
        repeatable_read:  'bg-amber-50 border-amber-300',
        serializable:     'bg-purple-50 border-purple-300',
    }[data.isolation] ?? 'bg-gray-50';

    return (
        <div className={`rounded-lg border-2 p-3 w-40 text-center ${isolationColor} ${data.waitingFor ? 'opacity-75' : ''}`}>
            <div className="font-mono font-bold text-sm">T{data.txnId}</div>
            <div className="text-xs text-gray-500 mt-0.5">{data.isolation.replace('_',' ')}</div>
            {data.waitingFor && (
                <div className="text-xs text-red-500 mt-1 font-medium">
                    waiting {Math.round(data.waitMs)}ms
                </div>
            )}
            <Handle type="source" position={Position.Right} />
            <Handle type="target" position={Position.Left} />
        </div>
    );
}
```

---

## Phase 26 — Version Chain Visualizer

```tsx
// web/src/components/mvcc/VersionChainRow.tsx
import { motion, AnimatePresence } from 'framer-motion';

export function VersionChainRow({ chain }: { chain: VersionChainData }) {
    return (
        <div className="flex items-center gap-2 overflow-x-auto py-3">
            <span className="text-xs text-gray-400 font-mono shrink-0">
                {chain.key}
            </span>
            <div className="text-gray-300">←</div>
            <div className="flex gap-2">
                <AnimatePresence initial={false}>
                    {chain.versions.map((v, i) => (
                        <motion.div
                            key={`${v.xmin}-${v.xmax}`}
                            initial={{ opacity: 0, scale: 0.8 }}
                            animate={{ opacity: 1, scale: 1 }}
                            exit={{ opacity: 0, scale: 0.6 }}
                            className={`
                                flex-shrink-0 rounded-lg border-2 p-2 w-36 text-xs
                                ${v.committed ? 'border-green-400 bg-green-50' :
                                  v.aborted  ? 'border-red-400   bg-red-50   opacity-50' :
                                               'border-blue-400  bg-blue-50'}
                            `}
                        >
                            <div className="font-mono font-bold">
                                xmin=<span className={v.committed ? 'text-green-700' : 'text-blue-700'}>{v.xmin}</span>
                            </div>
                            <div className="font-mono">
                                xmax=<span className={v.xmax === 0 ? 'text-gray-400' : 'text-red-600'}>{v.xmax || '∅'}</span>
                            </div>
                            <div className="mt-1 truncate text-gray-600">
                                {JSON.stringify(v.data).slice(0, 30)}
                            </div>
                            {i === 0 && <div className="mt-1 text-green-600 font-medium">CURRENT</div>}
                        </motion.div>
                    ))}
                </AnimatePresence>
            </div>
        </div>
    );
}
```

---

## Correctness Invariants to Enforce

These must hold at all times. Write assertion helpers:

1. **Lock table invariant:** At any moment, for every resource, at most one X-lock holder exists.
2. **Wait-for acyclicity (without deadlock):** The WFG must be acyclic at all times *except* during the 50ms window before the deadlock detector runs.
3. **MVCC version monotonicity:** For any version chain, `head.XMin >= head.Prev.XMin` (newer versions have higher TxnIDs).
4. **Snapshot correctness:** For any two concurrent transactions T1 and T2, if T1 committed before T2's snapshot was taken, T2 must see T1's writes.
5. **TPC-B invariant:** `SUM(accounts.balance) == 1,000,000` after any set of committed transactions.
6. **No dirty reads at RC+:** A transaction at ReadCommitted must never see uncommitted data.
7. **Repeatable reads:** Two reads of the same key within the same RR transaction return the same value.
8. **SSI aborts write skew:** The doctor on-call scenario must abort one transaction at Serializable level.

---

## Common Pitfalls

**Lock Queue:**
- Closing `grantedCh` vs sending to it: always use `close(ch)`. Multiple goroutines may be woken (e.g., multiple S-lock waiters can all be granted when an X-lock is released).
- Not removing waiter from queue when it times out → goroutine leak + stale entry blocks future grants.
- Not removing edges from WaitForGraph when lock is granted → false deadlock detection.

**MVCC Visibility:**
- Off-by-one with Xmin/Xmax: `Xmin < snap.Xmin` (strict less than) vs `Xmin <= snap.Xmin` (wrong — Xmin itself might be active).
- Forgetting the `snap.Contains(xmax)` check: xmax might be a concurrent transaction (active when we started) → deletion not committed → we see the row.
- Not handling the case where xmin == txn.ID but the version has xmax != txn.ID (we created the row but someone else deleted it concurrently).

**SSI:**
- Calling `RecordWrite` BEFORE `RecordRead` in the test setup defeats the detection (siread map is empty when write happens).
- Not cleaning up siread entries on commit/abort → memory leak + false conflicts for future transactions.
- Using the same `InConflict`/`OutConflict` booleans for both "I am the reader" and "I am the writer" roles — must track both independently.

**Transaction Manager:**
- Not signaling `txn.abortCh` when aborting → goroutines stuck in `AcquireLock` wait forever (goroutine leak).
- Double-abort: check `txn.Status` under lock before aborting, return nil if already aborted.
- MVCC abort not restoring XMax: when T aborts after setting XMax = T.ID on a version, must reset XMax to 0.

**Deadlock Detector:**
- Calling `txnManager.Abort` inside the WFG read lock → deadlock (Abort tries to acquire txn manager lock, which may already hold WFG lock). Always release WFG lock before calling Abort.
- Not stopping the detector goroutine on shutdown → goroutine leak in tests.

**TPC-B:**
- Not retrying on write conflict → all concurrent transactions abort after the first conflict → 0% throughput.
- Ignoring the balance sign: debit decreases balance, credit increases. Getting this backward violates the invariant.
- Reading balance as integer when it should be float → rounding errors accumulate across transfers.

---

## Definition of Done

1. `go test ./... -race -count=1` — zero failures, zero races
2. `npx tsc --noEmit` — zero type errors
3. All 7 scenarios produce correct anomaly detection/prevention at each isolation level
4. TPC-B at concurrency=8, Serializable, MVCC — balance invariant holds, abort rate < 20%
5. Deadlock in playground: create T1→T2, T2→T1 wait cycle → victim aborted within 100ms
6. Version chain viewer shows 3 bubbles after 2 updates to same row
7. WFG shows live animated edges when transactions are waiting for locks
8. SSI correctly aborts write-skew scenario (doctor on-call) and allows non-conflicting concurrent txns
9. `make build` produces a working binary
10. README covers: setup in 3 commands, quick demo of each anomaly, architecture diagram