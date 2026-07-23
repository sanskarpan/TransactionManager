// Package scenario provides types and helpers for demonstrating and testing
// concurrency anomalies under various isolation levels.
package scenario

import (
	"sync"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/txn"
)

// currentReporter is the active DeadlockReporter. Set via
// SetDeadlockReporter; protected by reporterMu. The main server sets this
// on startup so scenario-internal deadlocks (e.g. the deadlock_cycle
// scenario) appear in /api/deadlocks (CT-18).
var (
	reporterMu      sync.Mutex
	currentReporter DeadlockReporter
)

// SetDeadlockReporter installs a process-wide reporter that the deadlock
// scenario invokes when it observes a deadlock cycle. Pass nil to clear.
func SetDeadlockReporter(r DeadlockReporter) {
	reporterMu.Lock()
	currentReporter = r
	reporterMu.Unlock()
}

func reportDeadlock(victim uint64, cycle []uint64) {
	reporterMu.Lock()
	r := currentReporter
	reporterMu.Unlock()
	if r != nil {
		r(victim, cycle)
	}
}

// AnomalyType identifies a class of concurrency anomaly.
type AnomalyType string

const (
	// DirtyRead is the anomaly where one txn reads uncommitted data from another.
	DirtyRead         AnomalyType = "dirty_read"
	// LostUpdate is the anomaly where a concurrent write overwrites another's update.
	LostUpdate        AnomalyType = "lost_update"
	// NonRepeatableRead is the anomaly where a value changes between two reads.
	NonRepeatableRead AnomalyType = "non_repeatable_read"
	// PhantomRead is the anomaly where new rows appear between two scans.
	PhantomRead       AnomalyType = "phantom_read"
	// WriteSkew is the anomaly where two txns read a shared invariant and write oppositely.
	WriteSkew         AnomalyType = "write_skew"
	// DeadlockCycle is the anomaly where two txns wait for each other's locks.
	DeadlockCycle     AnomalyType = "deadlock_cycle"
	// CascadeAbort is the anomaly where one txn's abort causes another's reads to be invalid.
	CascadeAbort      AnomalyType = "cascade_abort"
)

// Step records a single operation in a scenario execution trace.
type Step struct {
	TxnLabel  string
	Op        string
	Table     string
	Key       string
	Result    interface{}
	Error     error
	Timestamp time.Time
	Note      string
}

// DeadlockReporter is the optional sink that scenarios use to surface a
// observed deadlock to the rest of the system (e.g. the main server's
// /api/deadlocks endpoint, CT-18). When nil, scenarios that detect a
// deadlock simply note it in the result; the scenarios package never
// panics if no reporter is wired.
type DeadlockReporter func(victimTxnID uint64, cycle []uint64)

// Result captures the outcome of running a scenario.
type Result struct {
	Name            string
	Isolation       txn.IsolationLevel
	Protocol        txn.ConcurrencyProtocol
	Steps           []Step
	AnomalyOccurred bool
	AnomalyType     AnomalyType
	// Resolved indicates whether the system (deadlock detector, SSI,
	// snapshot isolation) successfully resolved the anomaly once it
	// formed. Meaningful for the deadlock scenario: a deadlock cycle
	// may form (AnomalyOccurred=true) and either be detected and aborted
	// (Resolved=true) or escape detection (Resolved=false, both txns
	// time out instead of being chosen as a victim).
	Resolved    bool
	Explanation string
}

// RecordStep appends a step to the result's trace.
func (r *Result) RecordStep(label, op, table, key string, result interface{}, err error, note string) {
	r.Steps = append(r.Steps, Step{
		TxnLabel:  label,
		Op:        op,
		Table:     table,
		Key:       key,
		Result:    result,
		Error:     err,
		Timestamp: time.Now(),
		Note:      note,
	})
}
