// Package isolation provides Serializable Snapshot Isolation (SSI)
// conflict tracking via SIREAD locks and rw-antidependency detection.
package isolation

import (
	"sync"
)

// TxnID is a transaction identifier used by the SSI tracker.
type TxnID = uint64

// SSIConflictTarget is a transaction that can have its conflict flags set
type SSIConflictTarget interface {
	SetInConflict()
	SetOutConflict()
	GetInConflict() bool
	GetOutConflict() bool
	IsActive() bool
	GetID() TxnID
}

// SSITracker tracks SIREAD locks and rw-anti-dependencies for SSI
type SSITracker struct {
	mu     sync.RWMutex
	siread map[string]map[TxnID]SSIConflictTarget // key → readers
	edges  map[TxnID]map[TxnID]struct{}           // reader →rw writer
}

// NewSSITracker creates an empty SSI tracker ready to record reads and writes.
func NewSSITracker() *SSITracker {
	return &SSITracker{
		siread: make(map[string]map[TxnID]SSIConflictTarget),
		edges:  make(map[TxnID]map[TxnID]struct{}),
	}
}

// RecordRead registers a SIREAD lock on key for reader
func (s *SSITracker) RecordRead(reader SSIConflictTarget, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.siread[key] == nil {
		s.siread[key] = make(map[TxnID]SSIConflictTarget)
	}
	s.siread[key][reader.GetID()] = reader
}

// RecordWrite checks for rw-anti-dependencies when writer writes key
func (s *SSITracker) RecordWrite(writer SSIConflictTarget, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for readerID, reader := range s.siread[key] {
		if readerID == writer.GetID() {
			continue
		}
		if !reader.IsActive() {
			continue
		}
		// T_reader →rw T_writer
		reader.SetInConflict()
		writer.SetOutConflict()
		if s.edges[readerID] == nil {
			s.edges[readerID] = make(map[TxnID]struct{})
		}
		s.edges[readerID][writer.GetID()] = struct{}{}
	}
}

// CheckCommit returns an error if txn is a pivot: it has both an incoming
// rw-anti-dependency (it read something a concurrent writer later wrote, i.e.
// it has an outgoing edge edges[txnID][...]) AND an outgoing rw-anti-
// dependency (a concurrent reader read something it later wrote, i.e. some
// edges[R][txnID] exists).
//
// H-11: previously CheckCommit read cached InConflict/OutConflict flags set
// by RecordWrite and never cleared by Cleanup. So when two mutually-
// conflicting Serializable txns both reached CheckCommit and the first
// aborted (Cleanup called on it), the second still saw both flags set and
// was aborted too — over-aborting pivots instead of aborting the minimum.
// We now recompute pivot status from the live edge set, so an aborted
// pivot's Cleanup (which removes its edges) correctly un-flags survivors.
func (s *SSITracker) CheckCommit(txn SSIConflictTarget) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.isPivotLocked(txn.GetID()) {
		return &SSIError{}
	}
	return nil
}

// isPivotLocked recomputes pivot status from the live edge set. Caller MUST
// hold s.mu (RLock or Lock).
//
// Edge direction: edges[readerID][writerID] means "reader read something the
// writer later wrote" (an rw-anti-dependency edge reader→writer).
//   - hasIn (reader role): edges[txnID] is non-empty (txnID read something a
//     concurrent writer later wrote) → InConflict.
//   - hasOut (writer role): some edges[R][txnID] exists (a concurrent reader
//     read something txnID later wrote) → OutConflict.
func (s *SSITracker) isPivotLocked(txnID TxnID) bool {
	if len(s.edges[txnID]) == 0 {
		return false // no InConflict
	}
	for from := range s.edges {
		if _, ok := s.edges[from][txnID]; ok {
			return true // hasOut
		}
	}
	return false
}

// Cleanup removes the transaction from all SIREAD maps and edges
func (s *SSITracker) Cleanup(txnID TxnID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.siread {
		delete(s.siread[key], txnID)
	}
	delete(s.edges, txnID)
	for from := range s.edges {
		delete(s.edges[from], txnID)
	}
}

// Reset clears all SIREAD and edge state (used on database reset).
func (s *SSITracker) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.siread = make(map[string]map[TxnID]SSIConflictTarget)
	s.edges = make(map[TxnID]map[TxnID]struct{})
}

// SSIError is returned by CheckCommit when a dangerous rw-antidependency
// structure (pivot) is detected.
type SSIError struct{}

func (e *SSIError) Error() string {
	return "serialization failure: SSI detected dangerous structure"
}
