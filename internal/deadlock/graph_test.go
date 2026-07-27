package deadlock

import (
	"testing"
)

// hasEdge is a test helper that checks whether an edge from→to exists in g.
func hasEdge(g *WaitForGraph, from, to TxnID) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	tos, ok := g.edges[from]
	if !ok {
		return false
	}
	_, exists := tos[to]
	return exists
}

// TestWaitForGraph_RemoveEdges_OnlyRemovesOutgoing verifies that RemoveEdges
// deletes only the outgoing edges of the specified node, leaving incoming
// edges (from other nodes to that node) and unrelated edges intact.
func TestWaitForGraph_RemoveEdges_OnlyRemovesOutgoing(t *testing.T) {
	const A, B, C TxnID = 1, 2, 3

	g := NewWaitForGraph()
	// Build: A→B, B→C, C→A  (cycle so A has an incoming edge from C)
	g.AddEdge(A, B)
	g.AddEdge(B, C)
	g.AddEdge(C, A)

	// Remove only A's outgoing edges (A's lock was granted / A no longer waits)
	g.RemoveEdges(A)

	// A→B should be gone
	if hasEdge(g, A, B) {
		t.Error("expected edge A→B to be removed by RemoveEdges(A), but it still exists")
	}

	// B→C must still exist (unrelated to A)
	if !hasEdge(g, B, C) {
		t.Error("expected edge B→C to survive RemoveEdges(A), but it was removed")
	}

	// C→A (incoming to A) must still exist — RemoveEdges must NOT prune incoming edges
	if !hasEdge(g, C, A) {
		t.Error("expected incoming edge C→A to survive RemoveEdges(A), but it was removed")
	}
}

// TestWaitForGraph_RemoveEdges_PreservesCycle verifies that after removing a
// node's outgoing edges (because its lock was granted), the deadlock detector
// can still find cycles among the remaining nodes.
//
// Setup: A→B, B→C, C→B  (A was waiting for B; B and C are deadlocked)
// After RemoveEdges(A): the B↔C cycle must still be detected.
func TestWaitForGraph_RemoveEdges_PreservesCycle(t *testing.T) {
	const A, B, C TxnID = 10, 20, 30

	g := NewWaitForGraph()
	g.AddEdge(A, B)
	g.AddEdge(B, C)
	g.AddEdge(C, B) // B and C are deadlocked

	// A's lock was granted; remove its outgoing edge
	g.RemoveEdges(A)

	// The B→C→B cycle must still be detectable
	_, cycle, found := g.FindCycle()
	if !found {
		t.Fatal("expected FindCycle to detect B↔C cycle after RemoveEdges(A), but no cycle found")
	}

	// Verify B and C appear in the cycle
	inCycle := make(map[TxnID]bool)
	for _, id := range cycle {
		inCycle[id] = true
	}
	if !inCycle[B] {
		t.Errorf("expected B (%d) in cycle, got %v", B, cycle)
	}
	if !inCycle[C] {
		t.Errorf("expected C (%d) in cycle, got %v", C, cycle)
	}
}

// TestWaitForGraph_RemoveNode_RemovesBothDirections confirms that RemoveNode
// (the full-teardown path) still removes both outgoing AND incoming edges,
// ensuring we have not accidentally broken RemoveNode while fixing RemoveEdges.
func TestWaitForGraph_RemoveNode_RemovesBothDirections(t *testing.T) {
	const A, B, C TxnID = 100, 200, 300

	g := NewWaitForGraph()
	g.AddEdge(A, B)
	g.AddEdge(B, C)
	g.AddEdge(C, A)

	// Full node removal of A (deadlock victim path)
	g.RemoveNode(A)

	// A→B gone (outgoing)
	if hasEdge(g, A, B) {
		t.Error("expected edge A→B to be removed by RemoveNode(A)")
	}
	// C→A gone (incoming)
	if hasEdge(g, C, A) {
		t.Error("expected incoming edge C→A to be removed by RemoveNode(A)")
	}
	// B→C must still exist
	if !hasEdge(g, B, C) {
		t.Error("expected edge B→C to survive RemoveNode(A), but it was removed")
	}
}
