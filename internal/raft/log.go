package raft

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// CommandType identifies the operation stored in a Raft log entry.
type CommandType uint8

const (
	CmdNoop   CommandType = 0
	CmdBegin  CommandType = 1
	CmdWrite  CommandType = 2
	CmdCommit CommandType = 3
	CmdAbort  CommandType = 4
)

// Command is the payload of a Raft log entry.
type Command struct {
	Type   CommandType
	TxnID  uint64 // Assigned by leader; followers use this exact ID
	Table  string
	RowKey string
	Op     uint8  // UndoInsert=0, UndoUpdate=1, UndoDelete=2 for CmdWrite
	Before []byte // EncodeValues(beforeImage) for CmdWrite
	After  []byte // EncodeValues(afterImage) for CmdWrite
	Iso    uint8  // isolation level for CmdBegin
	Proto  uint8  // protocol for CmdBegin (0=2PL, 1=MVCC)
}

// Entry is one record in the Raft log.
type Entry struct {
	Index   uint64
	Term    uint64
	Command Command
}

// RaftLog manages the persistent Raft log.
// Entries are 1-indexed (index 0 is a sentinel/dummy).
// Invariant: entries[0].Index = 0, entries[0].Term = 0 (dummy).
type RaftLog struct {
	mu      sync.RWMutex
	entries []Entry
	file    *os.File
	path    string
}

// NewRaftLog opens or creates raft.log in dir.
// On create: writes dummy entry (index=0, term=0).
// On open: loads all entries from file.
func NewRaftLog(dir string) (*RaftLog, error) {
	path := filepath.Join(dir, "raft.log")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("raft log open: %w", err)
	}

	l := &RaftLog{
		path: path,
		file: f,
	}

	// Try to load existing entries.
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("raft log stat: %w", err)
	}

	if fi.Size() == 0 {
		// New file: write dummy entry.
		dummy := Entry{Index: 0, Term: 0}
		l.entries = []Entry{dummy}
		if err := l.writeToDisk(); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("raft log write dummy: %w", err)
		}
	} else {
		// Load all entries from file.
		if err := l.loadFromDisk(); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("raft log load: %w", err)
		}
	}

	return l, nil
}

// loadFromDisk reads all entries from the log file.
func (l *RaftLog) loadFromDisk() error {
	if _, err := l.file.Seek(0, 0); err != nil {
		return err
	}

	var entries []Entry
	for {
		// Read length prefix: 4 bytes LE.
		var lenBuf [4]byte
		_, err := l.file.Read(lenBuf[:])
		if err != nil {
			break // EOF
		}
		bodyLen := binary.LittleEndian.Uint32(lenBuf[:])
		body := make([]byte, bodyLen)
		if _, err := l.file.Read(body); err != nil {
			break
		}
		var e Entry
		// Use a temporary buffer reader for gob.
		buf := &bytesReader{data: body}
		dec := gob.NewDecoder(buf)
		if err := dec.Decode(&e); err != nil {
			break
		}
		entries = append(entries, e)
	}

	if len(entries) == 0 {
		// Ensure dummy entry exists.
		entries = []Entry{{Index: 0, Term: 0}}
	}
	l.entries = entries
	return nil
}

// writeToDisk appends all entries to disk (truncates first and rewrites all).
// Call under mu.
func (l *RaftLog) writeToDisk() error {
	if err := l.file.Truncate(0); err != nil {
		return err
	}
	if _, err := l.file.Seek(0, 0); err != nil {
		return err
	}

	for i := range l.entries {
		data, err := encodeEntry(l.entries[i])
		if err != nil {
			return err
		}
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(data)))
		if _, err := l.file.Write(lenBuf[:]); err != nil {
			return err
		}
		if _, err := l.file.Write(data); err != nil {
			return err
		}
	}
	return l.file.Sync()
}

// encodeEntry encodes an Entry to gob bytes.
func encodeEntry(e Entry) ([]byte, error) {
	var buf gobBuffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(e); err != nil {
		return nil, err
	}
	return buf.data, nil
}

// LastIndex returns the index of the last entry (0 if only dummy).
func (l *RaftLog) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[len(l.entries)-1].Index
}

// LastTerm returns the term of the last entry (0 if only dummy).
func (l *RaftLog) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[len(l.entries)-1].Term
}

// Get returns the entry at the given index.
func (l *RaftLog) Get(index uint64) (Entry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, e := range l.entries {
		if e.Index == index {
			return e, true
		}
	}
	return Entry{}, false
}

// Range returns entries with indices in [from, to] inclusive, clipped to bounds.
func (l *RaftLog) Range(from, to uint64) []Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var result []Entry
	for _, e := range l.entries {
		if e.Index >= from && e.Index <= to {
			result = append(result, e)
		}
	}
	return result
}

// Append appends entries to the log and persists them.
func (l *RaftLog) Append(entries ...Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entries...)
	return l.writeToDisk()
}

// TruncateAfter removes all entries with index > index.
func (l *RaftLog) TruncateAfter(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	kept := l.entries[:0]
	for _, e := range l.entries {
		if e.Index <= index {
			kept = append(kept, e)
		}
	}
	l.entries = kept
	return l.writeToDisk()
}

// TruncateBefore removes all entries with Index <= lastIncludedIndex.
// Called after installing a snapshot to compact the in-memory log.
func (l *RaftLog) TruncateBefore(lastIncludedIndex uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	kept := l.entries[:0]
	for _, e := range l.entries {
		if e.Index > lastIncludedIndex {
			kept = append(kept, e)
		}
	}
	l.entries = kept
}

// Close closes the underlying file.
func (l *RaftLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}

// gobBuffer is a simple io.Writer/io.Reader for gob encoding.
type gobBuffer struct {
	data []byte
}

func (b *gobBuffer) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

// bytesReader is a simple io.Reader over a byte slice.
type bytesReader struct {
	data []byte
	pos  int
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("EOF")
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
