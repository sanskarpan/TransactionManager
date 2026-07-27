package raft

import "sync/atomic"

// Role represents the current Raft role of a node.
type Role uint8

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2
)

// NodeID is the unique identifier of a Raft node.
type NodeID string

// PersistentState is saved to disk before responding to any RPC.
type PersistentState struct {
	CurrentTerm uint64
	VotedFor    NodeID // empty = not voted
}

// VolatileState is the in-memory Raft state (lost on crash, rebuilt from log).
type VolatileState struct {
	CommitIndex uint64
	LastApplied uint64
}

// LeaderState is initialized on election, nil on followers/candidates.
type LeaderState struct {
	NextIndex  map[NodeID]uint64
	MatchIndex map[NodeID]uint64
}

// atomicRole wraps Role with atomic load/store for safe concurrent access.
type atomicRole struct{ v atomic.Uint32 }

func (r *atomicRole) Load() Role      { return Role(r.v.Load()) }
func (r *atomicRole) Store(role Role) { r.v.Store(uint32(role)) }
