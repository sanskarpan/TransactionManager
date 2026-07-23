package deadlock

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWaitForGraph_TwoNodeCycle(t *testing.T) {
	g := NewWaitForGraph()
	g.AddEdge(1, 2)
	g.AddEdge(2, 1)

	victim, cycle, found := g.FindCycle()
	require.True(t, found)
	assert.Equal(t, TxnID(2), victim) // youngest
	assert.NotEmpty(t, cycle)
}

func TestWaitForGraph_ThreeNodeCycle(t *testing.T) {
	g := NewWaitForGraph()
	g.AddEdge(1, 2)
	g.AddEdge(2, 3)
	g.AddEdge(3, 1)

	_, cycle, found := g.FindCycle()
	require.True(t, found)
	assert.NotEmpty(t, cycle)
}

func TestWaitForGraph_NoFalsePositive(t *testing.T) {
	g := NewWaitForGraph()
	g.AddEdge(1, 2) // T1 waits for T2
	g.AddEdge(3, 4) // T3 waits for T4 (separate)

	_, _, found := g.FindCycle()
	assert.False(t, found)
}

func TestWaitForGraph_CycleRemovedAfterVictimAbort(t *testing.T) {
	g := NewWaitForGraph()
	g.AddEdge(1, 2)
	g.AddEdge(2, 1)

	victim, _, found := g.FindCycle()
	require.True(t, found)

	g.RemoveNode(victim)

	_, _, found = g.FindCycle()
	assert.False(t, found)
}

func TestWaitForGraph_ConcurrentAddEdgeAndFindCycle(_ *testing.T) {
	g := NewWaitForGraph()
	var wg sync.WaitGroup

	// Concurrent adds
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			g.AddEdge(TxnID(i), TxnID((i+1)%100))
		}(i)
	}

	// Concurrent FindCycle
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.FindCycle()
		}()
	}

	wg.Wait()
}

func TestDeadlockDetector_DetectsAndAborts(t *testing.T) {
	g := NewWaitForGraph()
	history := NewHistory()

	var mu sync.Mutex
	aborted := make(map[TxnID]bool)

	detector := NewDetector(g, history, func(id TxnID, _ []TxnID) {
		mu.Lock()
		aborted[id] = true
		mu.Unlock()
		g.RemoveNode(id)
	})
	detector.Start()
	defer detector.Stop()

	g.AddEdge(1, 2)
	g.AddEdge(2, 1)

	// Wait for detection
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := len(aborted) > 0
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	assert.True(t, aborted[TxnID(2)], "youngest transaction (2) should be aborted")
}

func TestWaitForGraph_RemoveEdges(t *testing.T) {
	g := NewWaitForGraph()
	g.AddEdge(1, 2)
	g.AddEdge(1, 3)
	g.AddEdge(2, 3)

	g.RemoveEdges(1)

	_, _, found := g.FindCycle()
	assert.False(t, found, "no cycle after removing edges from the source")
}

func TestWaitForGraph_RemoveEdges_NopForMissing(_ *testing.T) {
	g := NewWaitForGraph()
	g.RemoveEdges(999)
}

func TestDeadlockPrevention_WaitDie(t *testing.T) {
	// txn 5 (older) requests lock held by txn 10 (younger) → wait
	assert.Equal(t, ActionWait, WaitDie(5, 10))
	// txn 10 (younger) requests lock held by txn 5 (older) → die
	assert.Equal(t, ActionDie, WaitDie(10, 5))
}

func TestDeadlockPrevention_WoundWait(t *testing.T) {
	// txn 5 (older) requests lock held by txn 10 (younger) → wound
	assert.Equal(t, ActionWound, WoundWait(5, 10))
	// txn 10 (younger) requests lock held by txn 5 (older) → wait
	assert.Equal(t, ActionWait, WoundWait(10, 5))
}

// TestCT05_DeadlockDetector_Done is the regression guard for CT-05: the
// detector must expose a Done() channel that closes when the background
// goroutine exits, so tests and graceful-shutdown paths can wait for a
// real drain (parity with Vacuum.Done()).
func TestCT05_DeadlockDetector_Done(t *testing.T) {
	dd := NewDetector(NewWaitForGraph(), NewHistory(), func(uint64, []uint64) {})
	dd.Start()
	dd.Stop()
	select {
	case <-dd.Done():
		// good — the goroutine exited
	case <-time.After(2 * time.Second):
		t.Fatal("Detector.Done() did not close after Stop (CT-05)")
	}
}
