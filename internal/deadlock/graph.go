package deadlock

import (
	"sync"
)

// TxnID is a transaction identifier used in the wait-for graph.
type TxnID = uint64

// WaitForGraph is a directed graph tracking which transactions are waiting
// for locks held by others, used by DeadlockDetector for cycle detection.
type WaitForGraph struct {
	mu    sync.RWMutex
	edges map[TxnID]map[TxnID]struct{}
}

// NewWaitForGraph creates an empty wait-for graph.
func NewWaitForGraph() *WaitForGraph {
	return &WaitForGraph{edges: make(map[TxnID]map[TxnID]struct{})}
}

// AddEdge records that `from` is waiting for `to` to release a lock.
func (g *WaitForGraph) AddEdge(from, to TxnID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.edges[from] == nil {
		g.edges[from] = make(map[TxnID]struct{})
	}
	g.edges[from][to] = struct{}{}
	// Ensure 'to' exists in map for traversal
	if g.edges[to] == nil {
		g.edges[to] = make(map[TxnID]struct{})
	}
}

// RemoveNode removes txnID from the graph, deleting all edges to and from it.
func (g *WaitForGraph) RemoveNode(txnID TxnID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.edges, txnID)
	for from := range g.edges {
		delete(g.edges[from], txnID)
	}
}

// RemoveEdges drops every edge originating from txnID.
func (g *WaitForGraph) RemoveEdges(txnID TxnID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.edges, txnID)
	for from := range g.edges {
		delete(g.edges[from], txnID)
	}
}

// Nodes returns a snapshot of all node IDs currently in the graph.
func (g *WaitForGraph) Nodes() []TxnID {
	g.mu.RLock()
	defer g.mu.RUnlock()
	var result []TxnID
	for n := range g.edges {
		result = append(result, n)
	}
	return result
}

// Edges returns a snapshot of all edges as (from, to) pairs.
func (g *WaitForGraph) Edges() [][2]TxnID {
	g.mu.RLock()
	defer g.mu.RUnlock()
	var result [][2]TxnID
	for from, tos := range g.edges {
		for to := range tos {
			result = append(result, [2]TxnID{from, to})
		}
	}
	return result
}

// FindCycle runs DFS to detect a cycle. The youngest node in the cycle is
// chosen as the victim.
func (g *WaitForGraph) FindCycle() (victim TxnID, cycle []TxnID, found bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	color := make(map[TxnID]int) // 0=white, 1=gray, 2=black
	parent := make(map[TxnID]TxnID)
	var cycleStart, cycleEnd TxnID

	var dfs func(v TxnID) bool
	dfs = func(v TxnID) bool {
		color[v] = 1
		for w := range g.edges[v] {
			if color[w] == 1 {
				cycleStart, cycleEnd = w, v
				return true
			}
			if color[w] == 0 {
				parent[w] = v
				if dfs(w) {
					return true
				}
			}
		}
		color[v] = 2
		return false
	}

	for node := range g.edges {
		if color[node] == 0 {
			if dfs(node) {
				// Reconstruct cycle
				cycle = []TxnID{cycleStart}
				for cur := cycleEnd; cur != cycleStart; cur = parent[cur] {
					cycle = append([]TxnID{cur}, cycle...)
				}
				cycle = append(cycle, cycleStart) // close it
				victim = selectVictim(cycle)
				return victim, cycle, true
			}
		}
	}
	return 0, nil, false
}

func selectVictim(cycle []TxnID) TxnID {
	var maxID TxnID
	for _, id := range cycle {
		if id > maxID {
			maxID = id
		}
	}
	return maxID
}
