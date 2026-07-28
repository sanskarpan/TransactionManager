package raft

import (
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
)

// ErrNotLeader is returned by Propose when the node is not the leader.
var ErrNotLeader = errors.New("raft: not the leader")

// ProposalResult carries the outcome of a Propose call.
type ProposalResult struct {
	Index uint64
	Err   error
}

// pendingProposal tracks an in-flight proposal from a caller.
type pendingProposal struct {
	index  uint64
	result chan ProposalResult
}

// Node is the Raft state machine. Safe for concurrent use.
type Node struct {
	id      NodeID
	peers   map[NodeID]string // peerID → TCP address
	dataDir string

	mu   sync.Mutex
	role atomicRole

	term     atomic.Uint64 // currentTerm (also persisted)
	votedFor atomic.Value  // NodeID (also persisted)

	log       *RaftLog
	transport *TCPTransport
	fsm       FSM

	// Volatile state
	commitIndex atomic.Uint64
	lastApplied uint64 // protected by mu

	// Leader state (nil when follower/candidate)
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	// Known leader (updated on AppendEntries)
	leaderID atomic.Value // NodeID

	// Pending proposals (leader only): index → channel
	pendingMu sync.Mutex
	pending   map[uint64]*pendingProposal

	// Election
	electionTimer *time.Timer
	votesReceived int

	// Randomness for election timeout
	rng *rand.Rand

	// Control
	stopCh chan struct{}
	done   chan struct{}

	// Signal for apply loop
	applySignal chan struct{}

	// Snapshot state
	snapshotIndex uint64 // lastIncludedIndex of the latest snapshot
	snapshotTerm  uint64 // lastIncludedTerm of the latest snapshot
	snapshotData  []byte // most recently taken snapshot bytes (leader sends to lagging followers)
}

// NewNode creates a new Raft node.
func NewNode(id NodeID, peers map[NodeID]string, dataDir string, transport *TCPTransport, fsm FSM) (*Node, error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("raft mkdir %s: %w", dataDir, err)
	}

	l, err := NewRaftLog(dataDir)
	if err != nil {
		return nil, err
	}

	// Seed local rand with time for randomized election timeout.
	//nolint:gosec
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	n := &Node{
		id:          id,
		peers:       peers,
		dataDir:     dataDir,
		log:         l,
		transport:   transport,
		fsm:         fsm,
		pending:     make(map[uint64]*pendingProposal),
		stopCh:      make(chan struct{}),
		done:        make(chan struct{}),
		applySignal: make(chan struct{}, 1),
		rng:         rng,
	}
	n.role.Store(Follower)
	n.votedFor.Store(NodeID(""))
	n.leaderID.Store(NodeID(""))

	if err := n.loadState(); err != nil {
		return nil, err
	}

	return n, nil
}

// Start wires callbacks, starts transport, and launches background goroutines.
func (n *Node) Start() error {
	n.transport.OnRequestVote = func(_ NodeID, args RequestVoteArgs) (RequestVoteReply, error) {
		return n.handleRequestVote("", args), nil
	}
	n.transport.OnAppendEntries = func(_ NodeID, args AppendEntriesArgs) (AppendEntriesReply, error) {
		return n.handleAppendEntries("", args), nil
	}
	n.transport.OnInstallSnapshot = func(_ NodeID, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
		return n.handleInstallSnapshot(args), nil
	}

	if err := n.transport.Start(); err != nil {
		return err
	}

	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()

	go n.run()
	go n.applyLoop()

	return nil
}

// Stop shuts down the node.
func (n *Node) Stop() {
	close(n.stopCh)
	<-n.done
	_ = n.log.Close()
}

// Propose proposes a command to the cluster. Only succeeds on the leader.
func (n *Node) Propose(cmd Command) ProposalResult {
	if !n.IsLeader() {
		return ProposalResult{Err: ErrNotLeader}
	}

	n.mu.Lock()
	term := n.term.Load()
	lastIdx := n.log.LastIndex()
	entry := Entry{
		Index:   lastIdx + 1,
		Term:    term,
		Command: cmd,
	}
	if err := n.log.Append(entry); err != nil {
		n.mu.Unlock()
		return ProposalResult{Err: err}
	}

	resultCh := make(chan ProposalResult, 1)
	n.pendingMu.Lock()
	n.pending[entry.Index] = &pendingProposal{index: entry.Index, result: resultCh}
	n.pendingMu.Unlock()
	n.mu.Unlock()

	// Trigger replication to all peers.
	n.sendHeartbeats()

	// Also check if single-node cluster can commit immediately.
	if len(n.peers) == 0 {
		n.mu.Lock()
		n.tryAdvanceCommitIndex()
		n.mu.Unlock()
	}

	select {
	case res := <-resultCh:
		return res
	case <-time.After(5 * time.Second):
		n.pendingMu.Lock()
		delete(n.pending, entry.Index)
		n.pendingMu.Unlock()
		return ProposalResult{Index: entry.Index, Err: fmt.Errorf("raft: proposal timeout")}
	case <-n.stopCh:
		return ProposalResult{Err: fmt.Errorf("raft: node stopped")}
	}
}

// IsLeader returns true if this node believes it is the current leader.
func (n *Node) IsLeader() bool {
	return n.role.Load() == Leader
}

// LeaderID returns the current known leader ID.
func (n *Node) LeaderID() NodeID {
	v := n.leaderID.Load()
	if v == nil {
		return ""
	}
	return v.(NodeID)
}

// LeaderAddr returns the network address (host:port) of the current known
// leader. When this node is the leader it returns its own transport address.
// When the leader is a peer it looks up the peer's address in the peers map.
// Returns an empty string if the leader is unknown.
func (n *Node) LeaderAddr() string {
	id := n.LeaderID()
	if id == "" {
		return ""
	}
	if id == n.id {
		// This node is the leader; return its own listen address.
		return n.transport.Addr()
	}
	n.mu.Lock()
	addr := n.peers[id]
	n.mu.Unlock()
	return addr
}

// run is the main event loop.
func (n *Node) run() {
	defer close(n.done)

	heartbeatTicker := time.NewTicker(heartbeatInterval)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-n.stopCh:
			if n.electionTimer != nil {
				n.electionTimer.Stop()
			}
			return

		case <-heartbeatTicker.C:
			if n.IsLeader() {
				n.sendHeartbeats()
			}

		case <-n.electionTimerCh():
			n.mu.Lock()
			if !n.IsLeader() {
				n.becomeCandidate()
			}
			n.mu.Unlock()
		}
	}
}

// electionTimerCh returns the timer channel or a nil channel if timer is nil.
func (n *Node) electionTimerCh() <-chan time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.electionTimer == nil {
		return nil
	}
	return n.electionTimer.C
}

// becomeCandidate transitions to candidate and starts an election.
// Caller must hold n.mu.
func (n *Node) becomeCandidate() {
	n.role.Store(Candidate)
	newTerm := n.term.Load() + 1
	n.term.Store(newTerm)
	n.votedFor.Store(n.id)
	n.votesReceived = 1 // Vote for self.
	_ = n.persistState()
	n.resetElectionTimer()

	lastIdx := n.log.LastIndex()
	lastTerm := n.log.LastTerm()
	args := RequestVoteArgs{
		Term:         newTerm,
		CandidateID:  n.id,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	peersCopy := make(map[NodeID]string, len(n.peers))
	for k, v := range n.peers {
		peersCopy[k] = v
	}
	totalVoters := 1 + len(peersCopy) // self + peers
	majority := totalVoters/2 + 1

	for peer, addr := range peersCopy {
		go func(peer NodeID, addr string) {
			reply, err := n.transport.SendRequestVote(peer, addr, args)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()

			// If we got a higher term, step down.
			if reply.Term > n.term.Load() {
				n.becomeFollower(reply.Term)
				return
			}
			// Only count votes if we're still a candidate for this term.
			if n.role.Load() != Candidate || n.term.Load() != newTerm {
				return
			}
			if reply.VoteGranted {
				n.votesReceived++
				if n.votesReceived >= majority {
					n.becomeLeader()
				}
			}
		}(peer, addr)
	}

	// Single-node cluster: become leader immediately.
	if len(peersCopy) == 0 && n.votesReceived >= majority {
		n.becomeLeader()
	}
}

// becomeLeader transitions to leader.
// Caller must hold n.mu.
func (n *Node) becomeLeader() {
	n.role.Store(Leader)
	n.leaderID.Store(n.id)
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}

	lastIdx := n.log.LastIndex()
	n.nextIndex = make(map[NodeID]uint64)
	n.matchIndex = make(map[NodeID]uint64)
	for peer := range n.peers {
		n.nextIndex[peer] = lastIdx + 1
		n.matchIndex[peer] = 0
	}

	// Append no-op entry to commit previous term's entries.
	noopEntry := Entry{
		Index:   lastIdx + 1,
		Term:    n.term.Load(),
		Command: Command{Type: CmdNoop},
	}
	_ = n.log.Append(noopEntry)

	// Send immediate heartbeat.
	go n.sendHeartbeats()
}

// becomeFollower transitions to follower.
// Caller must hold n.mu.
func (n *Node) becomeFollower(term uint64) {
	n.role.Store(Follower)
	n.term.Store(term)
	n.votedFor.Store(NodeID(""))
	_ = n.persistState()
	n.resetElectionTimer()
}

// sendHeartbeats sends AppendEntries to all peers.
func (n *Node) sendHeartbeats() {
	if !n.IsLeader() {
		return
	}

	n.mu.Lock()
	peersCopy := make(map[NodeID]string, len(n.peers))
	for k, v := range n.peers {
		peersCopy[k] = v
	}
	n.mu.Unlock()

	for peer, addr := range peersCopy {
		go n.replicateToPeer(peer, addr)
	}
}

// replicateToPeer sends AppendEntries to a single peer.
func (n *Node) replicateToPeer(peer NodeID, addr string) {
	n.mu.Lock()
	if !n.IsLeader() {
		n.mu.Unlock()
		return
	}

	nextIdx, ok := n.nextIndex[peer]
	if !ok {
		nextIdx = n.log.LastIndex() + 1
	}

	// If the peer is too far behind (its nextIdx is before our snapshot), send
	// a snapshot instead of AppendEntries — we no longer have the missing entries.
	if nextIdx <= n.snapshotIndex {
		snapIdx := n.snapshotIndex
		snapTerm := n.snapshotTerm
		snapData := n.snapshotData
		term := n.term.Load()
		n.mu.Unlock()
		if snapData != nil {
			args := InstallSnapshotArgs{
				Term:              term,
				LeaderID:          n.id,
				LastIncludedIndex: snapIdx,
				LastIncludedTerm:  snapTerm,
				Data:              snapData,
			}
			reply, err := n.transport.SendInstallSnapshot(peer, addr, args)
			if err != nil {
				return
			}
			n.mu.Lock()
			if reply.Term > n.term.Load() {
				n.becomeFollower(reply.Term)
			} else if n.IsLeader() {
				n.nextIndex[peer] = snapIdx + 1
				n.matchIndex[peer] = snapIdx
			}
			n.mu.Unlock()
		}
		return
	}

	prevLogIndex := nextIdx - 1
	var prevLogTerm uint64
	if prevLogIndex > 0 {
		if e, ok := n.log.Get(prevLogIndex); ok {
			prevLogTerm = e.Term
		} else if prevLogIndex == n.snapshotIndex {
			prevLogTerm = n.snapshotTerm
		}
	}

	entries := n.log.Range(nextIdx, n.log.LastIndex())
	term := n.term.Load()
	commitIndex := n.commitIndex.Load()
	n.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}

	reply, err := n.transport.SendAppendEntries(peer, addr, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.term.Load() {
		n.becomeFollower(reply.Term)
		return
	}

	if !n.IsLeader() || n.term.Load() != term {
		return
	}

	if reply.Success {
		// Update matchIndex and nextIndex.
		newMatchIndex := prevLogIndex + uint64(len(entries))
		if newMatchIndex > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatchIndex
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.tryAdvanceCommitIndex()
	} else {
		// Backtrack nextIndex.
		if reply.ConflictIndex > 0 {
			n.nextIndex[peer] = reply.ConflictIndex
		} else if nextIdx > 1 {
			n.nextIndex[peer] = nextIdx - 1
		}
	}
}

// tryAdvanceCommitIndex finds the highest N where a majority have matchIndex >= N.
// Caller must hold n.mu.
func (n *Node) tryAdvanceCommitIndex() {
	if !n.IsLeader() {
		return
	}

	lastIdx := n.log.LastIndex()
	currentCommit := n.commitIndex.Load()
	term := n.term.Load()

	for N := lastIdx; N > currentCommit; N-- {
		e, ok := n.log.Get(N)
		if !ok || e.Term != term {
			continue
		}

		// Count how many peers have matchIndex >= N (including self).
		count := 1 // self always has it
		for _, matchIdx := range n.matchIndex {
			if matchIdx >= N {
				count++
			}
		}

		total := 1 + len(n.peers)
		majority := total/2 + 1
		if count >= majority {
			n.commitIndex.Store(N)
			// Signal apply loop.
			select {
			case n.applySignal <- struct{}{}:
			default:
			}
			break
		}
	}
}

// applyLoop applies committed entries to the FSM.
func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applySignal:
			n.applyCommitted()
		}
	}
}

// applyCommitted applies all committed but not-yet-applied entries.
func (n *Node) applyCommitted() {
	for {
		n.mu.Lock()
		commitIdx := n.commitIndex.Load()
		lastApplied := n.lastApplied
		if lastApplied >= commitIdx {
			n.mu.Unlock()
			return
		}
		applyIdx := lastApplied + 1
		n.mu.Unlock()

		e, ok := n.log.Get(applyIdx)
		if !ok {
			return
		}

		fsmErr := n.fsm.Apply(e)

		n.mu.Lock()
		n.lastApplied = applyIdx
		shouldSnapshot := n.lastApplied-n.snapshotIndex >= 1000
		n.mu.Unlock()

		// Resolve pending proposal if any, propagating any FSM error so the
		// HTTP handler can surface it rather than returning a false 200 OK.
		n.pendingMu.Lock()
		if pp, ok := n.pending[applyIdx]; ok {
			delete(n.pending, applyIdx)
			pp.result <- ProposalResult{Index: applyIdx, Err: fsmErr}
		}
		n.pendingMu.Unlock()

		// Trigger a snapshot every 1000 applied entries to bound log growth.
		if shouldSnapshot {
			n.triggerSnapshot()
		}
	}
}

// triggerSnapshot takes a point-in-time snapshot of the FSM and compacts the log.
func (n *Node) triggerSnapshot() {
	data, err := n.fsm.Snapshot()
	if err != nil {
		return
	}
	n.mu.Lock()
	idx := n.lastApplied
	var term uint64
	if e, ok := n.log.Get(idx); ok {
		term = e.Term
	}
	n.snapshotIndex = idx
	n.snapshotTerm = term
	n.snapshotData = data
	n.log.TruncateBefore(idx)
	n.mu.Unlock()
}

// handleInstallSnapshot applies a snapshot sent by the leader.
func (n *Node) handleInstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	n.mu.Lock()
	currentTerm := n.term.Load()
	if args.Term < currentTerm {
		n.mu.Unlock()
		return InstallSnapshotReply{Term: currentTerm}
	}
	if args.Term > currentTerm {
		n.term.Store(args.Term)
		n.role.Store(Follower)
		n.votedFor.Store(NodeID(""))
		_ = n.persistState()
	}
	n.resetElectionTimer()
	n.mu.Unlock()

	if err := n.fsm.Restore(args.Data); err != nil {
		return InstallSnapshotReply{Term: n.term.Load()}
	}

	n.mu.Lock()
	n.log.TruncateBefore(args.LastIncludedIndex)
	n.snapshotIndex = args.LastIncludedIndex
	n.snapshotTerm = args.LastIncludedTerm
	if n.commitIndex.Load() < args.LastIncludedIndex {
		n.commitIndex.Store(args.LastIncludedIndex)
	}
	if n.lastApplied < args.LastIncludedIndex {
		n.lastApplied = args.LastIncludedIndex
	}
	n.mu.Unlock()

	return InstallSnapshotReply{Term: n.term.Load()}
}

// handleRequestVote handles an incoming RequestVote RPC.
func (n *Node) handleRequestVote(_ NodeID, args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestVoteReply{Term: n.term.Load()}

	if args.Term < n.term.Load() {
		return reply
	}

	if args.Term > n.term.Load() {
		n.becomeFollower(args.Term)
		reply.Term = n.term.Load()
	}

	votedFor := n.votedFor.Load().(NodeID)
	// Grant vote if we haven't voted yet (or already voted for this candidate)
	// AND candidate's log is at least as up-to-date as ours.
	canVote := votedFor == "" || votedFor == args.CandidateID
	logOK := args.LastLogTerm > n.log.LastTerm() ||
		(args.LastLogTerm == n.log.LastTerm() && args.LastLogIndex >= n.log.LastIndex())

	if canVote && logOK {
		n.votedFor.Store(args.CandidateID)
		_ = n.persistState()
		n.resetElectionTimer()
		reply.VoteGranted = true
	}

	return reply
}

// handleAppendEntries handles an incoming AppendEntries RPC.
func (n *Node) handleAppendEntries(_ NodeID, args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendEntriesReply{Term: n.term.Load()}

	if args.Term < n.term.Load() {
		return reply
	}

	if args.Term > n.term.Load() {
		n.becomeFollower(args.Term)
	}

	// Valid leader: reset election timer.
	n.leaderID.Store(args.LeaderID)
	n.resetElectionTimer()

	if n.role.Load() == Candidate {
		n.role.Store(Follower)
	}

	// Check prev log consistency.
	if args.PrevLogIndex > 0 {
		prevEntry, ok := n.log.Get(args.PrevLogIndex)
		if !ok || prevEntry.Term != args.PrevLogTerm {
			// Conflict: provide fast backtrack hint.
			reply.ConflictIndex = n.log.LastIndex() + 1
			if ok {
				reply.ConflictTerm = prevEntry.Term
				// Find first index of conflict term.
				for idx := args.PrevLogIndex; idx > 0; idx-- {
					e, eok := n.log.Get(idx)
					if !eok || e.Term != prevEntry.Term {
						reply.ConflictIndex = idx + 1
						break
					}
					reply.ConflictIndex = idx
				}
			}
			reply.Term = n.term.Load()
			return reply
		}
	}

	// Append new entries.
	for _, entry := range args.Entries {
		existing, ok := n.log.Get(entry.Index)
		if ok && existing.Term != entry.Term {
			// Conflict: truncate from this index.
			if err := n.log.TruncateAfter(entry.Index - 1); err != nil {
				return reply
			}
		}
		if !ok {
			if err := n.log.Append(entry); err != nil {
				return reply
			}
		}
	}

	// Update commit index.
	if args.LeaderCommit > n.commitIndex.Load() {
		newCommit := args.LeaderCommit
		if lastIdx := n.log.LastIndex(); lastIdx < newCommit {
			newCommit = lastIdx
		}
		n.commitIndex.Store(newCommit)
		// Signal apply loop.
		select {
		case n.applySignal <- struct{}{}:
		default:
		}
	}

	reply.Success = true
	reply.Term = n.term.Load()
	return reply
}

// persistState writes PersistentState to disk.
func (n *Node) persistState() error {
	path := filepath.Join(n.dataDir, "raft-state.gob")
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	votedFor := n.votedFor.Load().(NodeID)
	ps := PersistentState{
		CurrentTerm: n.term.Load(),
		VotedFor:    votedFor,
	}
	if err := gob.NewEncoder(f).Encode(ps); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	_ = f.Close()
	return os.Rename(tmp, path)
}

// loadState reads PersistentState from disk if it exists.
func (n *Node) loadState() error {
	path := filepath.Join(n.dataDir, "raft-state.gob")
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil // Fresh start.
	}
	if err != nil {
		return err
	}
	defer f.Close()

	var ps PersistentState
	if err := gob.NewDecoder(f).Decode(&ps); err != nil {
		return err
	}
	n.term.Store(ps.CurrentTerm)
	n.votedFor.Store(ps.VotedFor)
	return nil
}

// resetElectionTimer resets the election timer with a random timeout.
// Caller must hold n.mu.
func (n *Node) resetElectionTimer() {
	timeout := electionTimeoutMin + time.Duration(n.rng.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(timeout)
	} else {
		if !n.electionTimer.Stop() {
			select {
			case <-n.electionTimer.C:
			default:
			}
		}
		n.electionTimer.Reset(timeout)
	}
}
