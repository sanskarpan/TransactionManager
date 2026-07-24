package deadlock

import (
	"fmt"
	"sync"
	"time"
)

// AbortFunc is invoked by the detector when a cycle is found. It receives
// the victim TxnID and the full cycle (in edge order) so the caller can
// log both. H-09: previously the detector only passed the victim; the
// caller could not log the cycle and the detector's re-tick could find
// the same cycle again before the victim's WFG edges were removed.
type AbortFunc func(victimID TxnID, cycle []TxnID)

// Detector runs periodic cycle-detection on the wait-for graph
// and aborts a victim transaction when a cycle is found.
type Detector struct {
	graph   *WaitForGraph
	history *History
	abort   AbortFunc
	stop    chan struct{}
	done    chan struct{} // closed when the background goroutine exits
	once    sync.Once
	startMu sync.Mutex
	started bool
}

// NewDetector constructs a detector that will tick every
// DefaultTickInterval once Start is called.
func NewDetector(graph *WaitForGraph, history *History, abort AbortFunc) *Detector {
	return &Detector{
		graph:   graph,
		history: history,
		abort:   abort,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// DefaultTickInterval is the period at which the background detector runs
// DFS over the wait-for graph. Exposed so the runbook can reference it
// without grepping the source.
const DefaultTickInterval = 50 * time.Millisecond

// Start launches the background detector goroutine. Idempotent.
func (d *Detector) Start() {
	d.startMu.Lock()
	defer d.startMu.Unlock()
	if d.started {
		return
	}
	d.started = true
	go func() {
		defer close(d.done)
		ticker := time.NewTicker(DefaultTickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				victim, cycle, found := d.graph.FindCycle()
				if !found {
					continue
				}
				record := &Record{
					ID:           fmt.Sprintf("dl-%d", time.Now().UnixNano()),
					DetectedAt:   time.Now(),
					Cycle:        cycle,
					Victim:       victim,
					VictimReason: "youngest",
				}
				d.history.Add(record)
				// Remove the victim's node from the graph BEFORE
				// invoking the abort callback, so the next tick cannot
				// re-detect the same cycle and add a duplicate record.
				// The callback is still responsible for the actual txn
				// abort + lock release.
				d.graph.RemoveNode(victim)
				d.abort(victim, cycle)
			case <-d.stop:
				return
			}
		}
	}()
}

// Stop signals the detector to exit. Idempotent (C-04 pattern): safe to
// call multiple times.
func (d *Detector) Stop() {
	d.once.Do(func() { close(d.stop) })
}

// Done returns a channel closed when the background goroutine has
// exited. Tests and graceful-shutdown paths use this to wait for a
// real drain (CT-05: previously the detector had no equivalent of
// Vacuum.Done() and a Stop() race could let the caller return before
// the goroutine exited).
func (d *Detector) Done() <-chan struct{} { return d.done }
