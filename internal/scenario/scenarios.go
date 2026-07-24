package scenario

import (
	"sync"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/mvcc"
	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/txn"
	"github.com/sanskarpan/TransactionManager/internal/types"
)

// Scenario is the interface every anomaly demonstration implements.
type Scenario interface {
	ScenarioName() string
	Describe() string
	AnomalyKind() AnomalyType
	Run(mgr *txn.Manager, isolation txn.IsolationLevel, protocol txn.ConcurrencyProtocol) *Result
}

// Registry holds all registered scenarios by name.
type Registry struct {
	m map[string]Scenario
}

// NewRegistry builds the default scenario registry.
func NewRegistry() *Registry {
	r := &Registry{m: make(map[string]Scenario)}
	r.Register(&DirtyReadScenario{})
	r.Register(&LostUpdateScenario{})
	r.Register(&NonRepeatableReadScenario{})
	r.Register(&PhantomReadScenario{})
	r.Register(&WriteSkewScenario{})
	r.Register(&DeadlockCycleScenario{})
	r.Register(&CascadeAbortScenario{})
	return r
}

// Register adds a scenario to the registry.
func (r *Registry) Register(s Scenario) { r.m[s.ScenarioName()] = s }

// Get retrieves a scenario by name.
func (r *Registry) Get(name string) (Scenario, bool) {
	s, ok := r.m[name]
	return s, ok
}

// All returns all registered scenarios.
func (r *Registry) All() []Scenario {
	out := make([]Scenario, 0, len(r.m))
	for _, s := range r.m {
		out = append(out, s)
	}
	return out
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func makeManager(tables map[string][]storage.Column, seed map[string]map[string][]types.Value) *txn.Manager {
	catalog := storage.NewCatalog()
	for name, cols := range tables {
		catalog.Register(storage.NewTable(name, cols))
	}
	mgr := txn.NewManager(catalog)
	seeded := false
	for table, rows := range seed {
		rowsSlice := make([]storage.Row, 0, len(rows))
		for k, v := range rows {
			rowsSlice = append(rowsSlice, storage.Row{Key: storage.RowKey(k), Values: v})
		}
		mvcc.SeedMVCCStore(mgr.MVCCStore, table, rowsSlice, 0)
		seeded = true
		// Also seed catalog for 2PL
		tbl, ok := catalog.Lookup(table)
		if ok {
			for k, v := range rows {
				tbl.PutRow(storage.RowKey(k), v)
			}
		}
	}
	if seeded {
		mgr.MarkCommitted(0)
	}
	return mgr
}

func isolationName(i txn.IsolationLevel) string {
	switch i {
	case txn.ReadUncommitted:
		return "ReadUncommitted"
	case txn.ReadCommitted:
		return "ReadCommitted"
	case txn.RepeatableRead:
		return "RepeatableRead"
	case txn.Serializable:
		return "Serializable"
	}
	return "Unknown"
}

// ─── Dirty Read ───────────────────────────────────────────────────────────────

// DirtyReadScenario demonstrates reading uncommitted data.
type DirtyReadScenario struct{}

// ScenarioName returns the unique name of this scenario.
func (s *DirtyReadScenario) ScenarioName() string { return "dirty_read" }

// AnomalyKind returns the anomaly type this scenario demonstrates.
func (s *DirtyReadScenario) AnomalyKind() AnomalyType { return DirtyRead }

// Describe returns a human-readable description of the scenario.
func (s *DirtyReadScenario) Describe() string {
	return "T1 writes a dirty value; T2 reads; T1 aborts. " +
		"At ReadUncommitted, T2 sees the dirty value. " +
		"At ReadCommitted+, T2 sees only committed data."
}

// Run executes the dirty-read anomaly scenario.
func (s *DirtyReadScenario) Run(_ *txn.Manager, isolation txn.IsolationLevel, protocol txn.ConcurrencyProtocol) *Result {
	mgr := makeManager(
		map[string][]storage.Column{"accounts": {{Name: "balance", Type: types.TypeInt}}},
		map[string]map[string][]types.Value{"accounts": {"A": {types.IntVal(100)}}},
	)
	res := &Result{Name: s.ScenarioName(), Isolation: isolation, Protocol: protocol, AnomalyType: DirtyRead}

	t1, _ := mgr.Begin(protocol, isolation, 5*time.Second)
	res.RecordStep("T1", "begin", "", "", nil, nil, "")

	if protocol == txn.ProtocolMVCC {
		_ = mgr.MVCCWrite(t1, "accounts", "A", []types.Value{types.IntVal(999)}, txn.UndoUpdate)
	} else {
		_ = mgr.TwoPLWrite(t1, "accounts", "A", []types.Value{types.IntVal(999)}, txn.UndoUpdate)
	}
	res.RecordStep("T1", "write", "accounts", "A", 999, nil, "dirty write (not committed)")

	t2, _ := mgr.Begin(protocol, isolation, 5*time.Second)
	res.RecordStep("T2", "begin", "", "", nil, nil, "")

	var v2 []types.Value
	var found bool
	if protocol == txn.ProtocolMVCC {
		v2, found, _ = mgr.MVCCRead(t2, "accounts", "A")
	} else {
		v2, found, _ = mgr.TwoPLRead(t2, "accounts", "A")
	}
	var readVal interface{}
	if found && len(v2) > 0 {
		readVal = v2[0].Int
	}
	anomaly := found && len(v2) > 0 && v2[0].Int == 999
	res.RecordStep("T2", "read", "accounts", "A", readVal, nil, "")

	_ = mgr.Abort(t1.ID, nil)
	res.RecordStep("T1", "abort", "", "", nil, nil, "")
	_ = mgr.Commit(t2.ID)

	res.AnomalyOccurred = anomaly
	if anomaly {
		res.Explanation = "T2 read T1's uncommitted value (dirty read). Only occurs at ReadUncommitted."
	} else {
		res.Explanation = "Dirty read prevented: T2 saw only committed data."
	}
	return res
}

// ─── Lost Update ─────────────────────────────────────────────────────────────

// LostUpdateScenario demonstrates concurrent read-modify-write leading to lost updates.
type LostUpdateScenario struct{}

// ScenarioName returns the unique name of this scenario.
func (s *LostUpdateScenario) ScenarioName() string { return "lost_update" }

// AnomalyKind returns the anomaly type this scenario demonstrates.
func (s *LostUpdateScenario) AnomalyKind() AnomalyType { return LostUpdate }

// Describe returns a human-readable description of the scenario.
func (s *LostUpdateScenario) Describe() string {
	return "T1 and T2 both read balance, each writes a new value. " +
		"At ReadCommitted MVCC, T1's update may be overwritten by T2. " +
		"At RepeatableRead+, write-write conflict aborts one."
}

// Run executes the lost-update anomaly scenario.
func (s *LostUpdateScenario) Run(_ *txn.Manager, isolation txn.IsolationLevel, protocol txn.ConcurrencyProtocol) *Result {
	mgr := makeManager(
		map[string][]storage.Column{"accounts": {{Name: "balance", Type: types.TypeInt}}},
		map[string]map[string][]types.Value{"accounts": {"A": {types.IntVal(100)}}},
	)
	res := &Result{Name: s.ScenarioName(), Isolation: isolation, Protocol: protocol, AnomalyType: LostUpdate}

	// Classic lost update: T1 reads, T2 reads, T1 writes+commits, T2 writes+commits.
	// At RC: T2's per-statement snapshot sees T1 committed → write succeeds → lost update.
	// At RR: T2's snapshot taken at Begin still has T1 in active list → write conflict → prevented.
	t1, _ := mgr.Begin(protocol, isolation, 5*time.Second)
	t2, _ := mgr.Begin(protocol, isolation, 5*time.Second)

	var v1, v2 []types.Value
	if protocol == txn.ProtocolMVCC {
		v1, _, _ = mgr.MVCCRead(t1, "accounts", "A")
		v2, _, _ = mgr.MVCCRead(t2, "accounts", "A")
	} else {
		v1, _, _ = mgr.TwoPLRead(t1, "accounts", "A")
		v2, _, _ = mgr.TwoPLRead(t2, "accounts", "A")
	}
	res.RecordStep("T1", "read", "accounts", "A", v1[0].Int, nil, "sees 100")
	res.RecordStep("T2", "read", "accounts", "A", v2[0].Int, nil, "sees 100")

	newVal1 := v1[0].Int + 50 // T1 computes 150

	// T1 writes and commits first
	var w1Err error
	if protocol == txn.ProtocolMVCC {
		w1Err = mgr.MVCCWrite(t1, "accounts", "A", []types.Value{types.IntVal(newVal1)}, txn.UndoUpdate)
	} else {
		w1Err = mgr.TwoPLWrite(t1, "accounts", "A", []types.Value{types.IntVal(newVal1)}, txn.UndoUpdate)
	}
	c1Err := mgr.Commit(t1.ID)
	res.RecordStep("T1", "write+commit", "accounts", "A", newVal1, c1Err, "T1 sets balance to 150 and commits")

	// T2 now writes (T1 already committed)
	newVal2 := v2[0].Int - 30 // T2 still uses its stale read of 100, computes 70
	var w2Err, c2Err error
	if w1Err == nil && c1Err == nil {
		if protocol == txn.ProtocolMVCC {
			w2Err = mgr.MVCCWrite(t2, "accounts", "A", []types.Value{types.IntVal(newVal2)}, txn.UndoUpdate)
		} else {
			w2Err = mgr.TwoPLWrite(t2, "accounts", "A", []types.Value{types.IntVal(newVal2)}, txn.UndoUpdate)
		}
	}
	if w2Err == nil {
		c2Err = mgr.Commit(t2.ID)
	} else {
		_ = mgr.Abort(t2.ID, w2Err)
		c2Err = w2Err
	}
	res.RecordStep("T2", "write+commit", "accounts", "A", newVal2, c2Err, "T2 tries to set balance to 70 (stale read)")

	// Lost update: T1 committed 150, T2 overwrote it with 70 (ignoring T1's change)
	anomaly := c1Err == nil && c2Err == nil
	res.AnomalyOccurred = anomaly
	if anomaly {
		res.Explanation = "Lost update: T1 changed balance to 150 and committed. T2 read 100, computed 70, then overwrote T1's 150. T1's change is lost at " + isolationName(isolation) + "."
	} else {
		res.Explanation = "Write conflict detected: T2 saw T1's committed version in its snapshot active list (RR) → prevented."
	}
	return res
}

// ─── Non-Repeatable Read ─────────────────────────────────────────────────────

// NonRepeatableReadScenario demonstrates a value changing between two reads in the same txn.
type NonRepeatableReadScenario struct{}

// ScenarioName returns the unique name of this scenario.
func (s *NonRepeatableReadScenario) ScenarioName() string { return "non_repeatable_read" }

// AnomalyKind returns the anomaly type this scenario demonstrates.
func (s *NonRepeatableReadScenario) AnomalyKind() AnomalyType { return NonRepeatableRead }

// Describe returns a human-readable description of the scenario.
func (s *NonRepeatableReadScenario) Describe() string {
	return "T1 reads a value twice; T2 updates and commits between reads. " +
		"At ReadCommitted, T1 sees different values. " +
		"At RepeatableRead+, T1 sees the same value both times."
}

// Run executes the non-repeatable-read anomaly scenario.
func (s *NonRepeatableReadScenario) Run(_ *txn.Manager, isolation txn.IsolationLevel, protocol txn.ConcurrencyProtocol) *Result {
	mgr := makeManager(
		map[string][]storage.Column{"items": {{Name: "price", Type: types.TypeInt}}},
		map[string]map[string][]types.Value{"items": {"X": {types.IntVal(10)}}},
	)
	res := &Result{Name: s.ScenarioName(), Isolation: isolation, Protocol: protocol, AnomalyType: NonRepeatableRead}

	t1, _ := mgr.Begin(protocol, isolation, 5*time.Second)
	res.RecordStep("T1", "begin", "", "", nil, nil, "")

	var v1 []types.Value
	if protocol == txn.ProtocolMVCC {
		v1, _, _ = mgr.MVCCRead(t1, "items", "X")
	} else {
		v1, _, _ = mgr.TwoPLRead(t1, "items", "X")
	}
	res.RecordStep("T1", "read", "items", "X", v1[0].Int, nil, "first read=10")

	// T2 updates and commits (RC protocol for T2 so it doesn't block)
	t2, _ := mgr.Begin(protocol, txn.ReadCommitted, 5*time.Second)
	if protocol == txn.ProtocolMVCC {
		_ = mgr.MVCCWrite(t2, "items", "X", []types.Value{types.IntVal(99)}, txn.UndoUpdate)
	} else {
		_ = mgr.TwoPLWrite(t2, "items", "X", []types.Value{types.IntVal(99)}, txn.UndoUpdate)
	}
	_ = mgr.Commit(t2.ID)
	res.RecordStep("T2", "write+commit", "items", "X", 99, nil, "updated price to 99")

	var v2 []types.Value
	if protocol == txn.ProtocolMVCC {
		v2, _, _ = mgr.MVCCRead(t1, "items", "X")
	} else {
		v2, _, _ = mgr.TwoPLRead(t1, "items", "X")
	}
	_ = mgr.Commit(t1.ID)

	anomaly := len(v2) > 0 && v2[0].Int != v1[0].Int
	res.RecordStep("T1", "read", "items", "X", v2[0].Int, nil, "second read")

	res.AnomalyOccurred = anomaly
	if anomaly {
		res.Explanation = "Non-repeatable read: T1 saw 10 then 99. Occurs at ReadCommitted."
	} else {
		res.Explanation = "Repeatable read guaranteed: T1 saw the same value both times."
	}
	return res
}

// ─── Phantom Read ─────────────────────────────────────────────────────────────

// PhantomReadScenario demonstrates new rows appearing between scans.
type PhantomReadScenario struct{}

// ScenarioName returns the unique name of this scenario.
func (s *PhantomReadScenario) ScenarioName() string { return "phantom_read" }

// AnomalyKind returns the anomaly type this scenario demonstrates.
func (s *PhantomReadScenario) AnomalyKind() AnomalyType { return PhantomRead }

// Describe returns a human-readable description of the scenario.
func (s *PhantomReadScenario) Describe() string {
	return "T1 scans a range; T2 inserts a new row and commits; T1 scans again. " +
		"At RepeatableRead MVCC, T1 sees the phantom. " +
		"At Serializable (SSI or gap locks), phantom is prevented."
}

// Run executes the phantom-read anomaly scenario.
func (s *PhantomReadScenario) Run(_ *txn.Manager, isolation txn.IsolationLevel, protocol txn.ConcurrencyProtocol) *Result {
	mgr := makeManager(
		map[string][]storage.Column{"salaries": {{Name: "amount", Type: types.TypeInt}}},
		map[string]map[string][]types.Value{"salaries": {
			"emp1": {types.IntVal(5000)},
			"emp3": {types.IntVal(7000)},
		}},
	)
	res := &Result{Name: s.ScenarioName(), Isolation: isolation, Protocol: protocol, AnomalyType: PhantomRead}

	t1, _ := mgr.Begin(protocol, isolation, 5*time.Second)
	res.RecordStep("T1", "begin", "", "", nil, nil, "")

	var rows1 []storage.Row
	var scanErr error
	if protocol == txn.ProtocolMVCC {
		rows1, scanErr = mgr.MVCCScan(t1, "salaries", nil)
	} else {
		rows1, scanErr = mgr.TwoPLScan(t1, "salaries", nil)
	}
	res.RecordStep("T1", "scan", "salaries", "", len(rows1), scanErr, "first scan")

	// T2 inserts and commits
	t2, _ := mgr.Begin(protocol, txn.ReadCommitted, 5*time.Second)
	if protocol == txn.ProtocolMVCC {
		_ = mgr.MVCCWrite(t2, "salaries", "emp2", []types.Value{types.IntVal(6000)}, txn.UndoInsert)
	} else {
		_ = mgr.TwoPLWrite(t2, "salaries", "emp2", []types.Value{types.IntVal(6000)}, txn.UndoInsert)
	}
	_ = mgr.Commit(t2.ID)
	res.RecordStep("T2", "insert+commit", "salaries", "emp2", 6000, nil, "inserted emp2")

	var rows2 []storage.Row
	if protocol == txn.ProtocolMVCC {
		rows2, _ = mgr.MVCCScan(t1, "salaries", nil)
	} else {
		rows2, _ = mgr.TwoPLScan(t1, "salaries", nil)
	}
	_ = mgr.Commit(t1.ID)
	res.RecordStep("T1", "scan", "salaries", "", len(rows2), nil, "second scan")

	anomaly := len(rows2) > len(rows1)
	res.AnomalyOccurred = anomaly
	if anomaly {
		res.Explanation = "Phantom read: T1 saw emp2 in the second scan (not in first). Occurs at RepeatableRead with MVCC."
	} else {
		res.Explanation = "Phantom read prevented: T1 saw the same set of rows in both scans."
	}
	return res
}

// ─── Write Skew ──────────────────────────────────────────────────────────────

// WriteSkewScenario demonstrates the doctor on-call write skew anomaly.
type WriteSkewScenario struct{}

// ScenarioName returns the unique name of this scenario.
func (s *WriteSkewScenario) ScenarioName() string { return "write_skew" }

// AnomalyKind returns the anomaly type this scenario demonstrates.
func (s *WriteSkewScenario) AnomalyKind() AnomalyType { return WriteSkew }

// Describe returns a human-readable description of the scenario.
func (s *WriteSkewScenario) Describe() string {
	return "Doctor on-call: both Alice and Bob are on-call. " +
		"T1 reads both (on-call), sets Alice off-call. " +
		"T2 reads both (on-call), sets Bob off-call. " +
		"At RepeatableRead, both commit leaving nobody on-call. " +
		"At Serializable (SSI), one is aborted."
}

// Run executes the write-skew anomaly scenario.
func (s *WriteSkewScenario) Run(_ *txn.Manager, isolation txn.IsolationLevel, protocol txn.ConcurrencyProtocol) *Result {
	mgr := makeManager(
		map[string][]storage.Column{"doctors": {{Name: "name", Type: types.TypeText}, {Name: "on_call", Type: types.TypeBool}}},
		map[string]map[string][]types.Value{"doctors": {
			"A": {types.TextVal("Alice"), types.BoolVal(true)},
			"B": {types.TextVal("Bob"), types.BoolVal(true)},
		}},
	)
	res := &Result{Name: s.ScenarioName(), Isolation: isolation, Protocol: protocol, AnomalyType: WriteSkew}

	offCall := []types.Value{types.TextVal("off"), types.BoolVal(false)}

	t1, _ := mgr.Begin(protocol, isolation, 5*time.Second)
	t2, _ := mgr.Begin(protocol, isolation, 5*time.Second)

	if protocol == txn.ProtocolMVCC {
		_, _, _ = mgr.MVCCRead(t1, "doctors", "A")
		_, _, _ = mgr.MVCCRead(t1, "doctors", "B")
		_, _, _ = mgr.MVCCRead(t2, "doctors", "A")
		_, _, _ = mgr.MVCCRead(t2, "doctors", "B")
		_ = mgr.MVCCWrite(t1, "doctors", "A", offCall, txn.UndoUpdate)
		_ = mgr.MVCCWrite(t2, "doctors", "B", offCall, txn.UndoUpdate)
	} else {
		_, _, _ = mgr.TwoPLRead(t1, "doctors", "A")
		_, _, _ = mgr.TwoPLRead(t1, "doctors", "B")
		_, _, _ = mgr.TwoPLRead(t2, "doctors", "A")
		_, _, _ = mgr.TwoPLRead(t2, "doctors", "B")
		_ = mgr.TwoPLWrite(t1, "doctors", "A", offCall, txn.UndoUpdate)
		_ = mgr.TwoPLWrite(t2, "doctors", "B", offCall, txn.UndoUpdate)
	}
	res.RecordStep("T1", "read+write", "doctors", "A", "off_call=false", nil, "T1 sets Alice off-call")
	res.RecordStep("T2", "read+write", "doctors", "B", "off_call=false", nil, "T2 sets Bob off-call")

	c1Err := mgr.Commit(t1.ID)
	c2Err := mgr.Commit(t2.ID)
	res.RecordStep("T1", "commit", "", "", nil, c1Err, "")
	res.RecordStep("T2", "commit", "", "", nil, c2Err, "")

	anomaly := c1Err == nil && c2Err == nil
	res.AnomalyOccurred = anomaly
	if anomaly {
		res.Explanation = "Write skew: both committed, nobody is on-call. Invariant violated at " + isolationName(isolation) + "."
	} else {
		res.Explanation = "SSI detected the rw-anti-dependency cycle and aborted one transaction."
	}
	return res
}

// ─── Deadlock Cycle ──────────────────────────────────────────────────────────

// DeadlockCycleScenario demonstrates T1→B, T2→A forming a cycle under 2PL.
type DeadlockCycleScenario struct{}

// ScenarioName returns the unique name of this scenario.
func (s *DeadlockCycleScenario) ScenarioName() string { return "deadlock_cycle" }

// AnomalyKind returns the anomaly type this scenario demonstrates.
func (s *DeadlockCycleScenario) AnomalyKind() AnomalyType { return DeadlockCycle }

// Describe returns a human-readable description of the scenario.
func (s *DeadlockCycleScenario) Describe() string {
	return "T1 holds lock on A, waits for B. T2 holds lock on B, waits for A. " +
		"Deadlock detector aborts the younger transaction (victim)."
}

// Run executes the deadlock-cycle anomaly scenario.
func (s *DeadlockCycleScenario) Run(_ *txn.Manager, isolation txn.IsolationLevel, protocol txn.ConcurrencyProtocol) *Result {
	mgr := makeManager(
		map[string][]storage.Column{"items": {{Name: "val", Type: types.TypeInt}}},
		map[string]map[string][]types.Value{"items": {
			"A": {types.IntVal(1)},
			"B": {types.IntVal(2)},
		}},
	)
	res := &Result{Name: s.ScenarioName(), Isolation: isolation, Protocol: protocol, AnomalyType: DeadlockCycle}

	if protocol == txn.ProtocolMVCC {
		res.Explanation = "Deadlock scenario requires 2PL protocol. MVCC uses write-conflict detection instead."
		return res
	}

	t1, _ := mgr.Begin(protocol, isolation, 3*time.Second)
	t2, _ := mgr.Begin(protocol, isolation, 3*time.Second)
	res.RecordStep("T1", "begin", "", "", nil, nil, "")
	res.RecordStep("T2", "begin", "", "", nil, nil, "")

	// T1 locks A
	_ = mgr.TwoPLWrite(t1, "items", "A", []types.Value{types.IntVal(10)}, txn.UndoUpdate)
	res.RecordStep("T1", "write", "items", "A", 10, nil, "T1 holds X on A")

	// T2 locks B
	_ = mgr.TwoPLWrite(t2, "items", "B", []types.Value{types.IntVal(20)}, txn.UndoUpdate)
	res.RecordStep("T2", "write", "items", "B", 20, nil, "T2 holds X on B")

	var t1FinalErr, t2FinalErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		err := mgr.TwoPLWrite(t1, "items", "B", []types.Value{types.IntVal(11)}, txn.UndoUpdate)
		if err == nil {
			t1FinalErr = mgr.Commit(t1.ID)
		} else {
			t1FinalErr = err
			_ = mgr.Abort(t1.ID, err)
		}
	}()
	go func() {
		defer wg.Done()
		err := mgr.TwoPLWrite(t2, "items", "A", []types.Value{types.IntVal(21)}, txn.UndoUpdate)
		if err == nil {
			t2FinalErr = mgr.Commit(t2.ID)
		} else {
			t2FinalErr = err
			_ = mgr.Abort(t2.ID, err)
		}
	}()
	wg.Wait()

	aborted := 0
	if t1FinalErr != nil {
		aborted++
		res.RecordStep("T1", "abort", "", "", nil, t1FinalErr, "deadlock victim")
	} else {
		res.RecordStep("T1", "commit", "", "", nil, nil, "survived deadlock")
	}
	if t2FinalErr != nil {
		aborted++
		res.RecordStep("T2", "abort", "", "", nil, t2FinalErr, "deadlock victim")
	} else {
		res.RecordStep("T2", "commit", "", "", nil, nil, "survived deadlock")
	}

	// CT-18: surface the observed deadlock to the main server's
	// deadlock history (when a reporter is wired). The scenario uses a
	// 3s lock timeout to force a deterministic deadlock-class failure;
	// whichever txn timed out first is reported as the "victim".
	if aborted > 0 {
		victim := uint64(t1.ID)
		if t1FinalErr == nil {
			victim = uint64(t2.ID)
		}
		// CT-25: the WFG page renders the cycle as `cycle.join(' → ')`
		// and the real graph.FindCycle returns a CLOSED cycle [a, b, a].
		// Match that format so the scenario's record renders as a proper
		// loop rather than an open chain.
		cycle := []uint64{uint64(t1.ID), uint64(t2.ID), uint64(t1.ID)}
		reportDeadlock(victim, cycle)
	}
	res.AnomalyOccurred = true
	// CT-24: Resolved means a victim was actually chosen (one aborted,
	// the other committed). When both txns hit the 3s lock timeout
	// without being picked by a detector, aborted == 2 and the system
	// did NOT actually resolve the deadlock — it timed out.
	res.Resolved = aborted == 1
	switch aborted {
	case 1:
		res.Explanation = "Deadlock detected and resolved: one transaction was aborted as the victim, the other committed."
	case 2:
		res.Explanation = "Cycle formed but neither detector intervened: both transactions hit the lock timeout (3s) and were aborted."
	default:
		res.Explanation = "Cycle formed and resolved without detection (no victim was chosen)."
	}
	return res
}

// ─── Cascade Abort ────────────────────────────────────────────────────────────

// CascadeAbortScenario demonstrates that MVCC prevents reading uncommitted data that gets aborted.
type CascadeAbortScenario struct{}

// ScenarioName returns the unique name of this scenario.
func (s *CascadeAbortScenario) ScenarioName() string { return "cascade_abort" }

// AnomalyKind returns the anomaly type this scenario demonstrates.
func (s *CascadeAbortScenario) AnomalyKind() AnomalyType { return CascadeAbort }

// Describe returns a human-readable description of the scenario.
func (s *CascadeAbortScenario) Describe() string {
	return "T1 writes dirty; T2 reads T1's data; T1 aborts. " +
		"MVCC prevents T2 from reading T1's uncommitted (later aborted) value."
}

// Run executes the cascade-abort anomaly scenario.
func (s *CascadeAbortScenario) Run(_ *txn.Manager, isolation txn.IsolationLevel, protocol txn.ConcurrencyProtocol) *Result {
	mgr := makeManager(
		map[string][]storage.Column{"data": {{Name: "val", Type: types.TypeInt}}},
		map[string]map[string][]types.Value{"data": {"X": {types.IntVal(0)}}},
	)
	res := &Result{Name: s.ScenarioName(), Isolation: isolation, Protocol: protocol, AnomalyType: CascadeAbort}

	t1, _ := mgr.Begin(protocol, isolation, 5*time.Second)
	if protocol == txn.ProtocolMVCC {
		_ = mgr.MVCCWrite(t1, "data", "X", []types.Value{types.IntVal(999)}, txn.UndoUpdate)
	} else {
		_ = mgr.TwoPLWrite(t1, "data", "X", []types.Value{types.IntVal(999)}, txn.UndoUpdate)
	}
	res.RecordStep("T1", "write", "data", "X", 999, nil, "dirty write")

	t2, _ := mgr.Begin(protocol, isolation, 5*time.Second)
	var v []types.Value
	var found bool
	if protocol == txn.ProtocolMVCC {
		v, found, _ = mgr.MVCCRead(t2, "data", "X")
	} else {
		v, found, _ = mgr.TwoPLRead(t2, "data", "X")
	}

	var readVal interface{}
	if found && len(v) > 0 {
		readVal = v[0].Int
	}
	res.RecordStep("T2", "read", "data", "X", readVal, nil, "")

	_ = mgr.Abort(t1.ID, nil)
	res.RecordStep("T1", "abort", "", "", nil, nil, "")
	_ = mgr.Commit(t2.ID)

	anomaly := found && len(v) > 0 && v[0].Int == 999
	res.AnomalyOccurred = anomaly
	if anomaly {
		res.Explanation = "Cascade abort: T2 read T1's dirty value (999) which was later aborted."
	} else {
		res.Explanation = "MVCC/2PL correctly isolated T2 from T1's uncommitted write."
	}
	return res
}
