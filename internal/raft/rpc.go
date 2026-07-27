package raft

// RequestVote RPC.
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntries RPC (also used as heartbeat when Entries is empty).
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

// AppendEntriesReply is the response to an AppendEntries RPC.
type AppendEntriesReply struct {
	Term    uint64
	Success bool
	// Fast backtrack optimization (§5.3):
	ConflictTerm  uint64
	ConflictIndex uint64
}

// InstallSnapshot RPC — sent by leader to bring a lagging follower up to date.
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          NodeID
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte // gob-encoded FSM snapshot
}

// InstallSnapshotReply is the follower's response to InstallSnapshot.
type InstallSnapshotReply struct {
	Term uint64
}

// MsgType constants for the TCP frame header.
const (
	MsgRequestVote          uint8 = 0x01
	MsgRequestVoteReply     uint8 = 0x02
	MsgAppendEntries        uint8 = 0x03
	MsgAppendEntriesReply   uint8 = 0x04
	MsgInstallSnapshot      uint8 = 0x05
	MsgInstallSnapshotReply uint8 = 0x06
)
