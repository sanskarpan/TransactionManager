package deadlock

import (
	"sync"
	"time"
)

// Record captures a single deadlock detection event for
// observability via the API.
type Record struct {
	ID           string
	DetectedAt   time.Time
	Cycle        []TxnID
	Victim       TxnID
	VictimReason string
}

// History is a bounded, thread-safe list of deadlock records.
type History struct {
	mu      sync.Mutex
	records []*Record
	maxSize int
}

// NewHistory creates a history with a default capacity of 100 records.
func NewHistory() *History {
	return &History{maxSize: 100}
}

// Add appends a record, evicting the oldest entry if over capacity.
func (h *History) Add(r *Record) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	if len(h.records) > h.maxSize {
		h.records = h.records[len(h.records)-h.maxSize:]
	}
}

// All returns a copy of every recorded deadlock.
func (h *History) All() []*Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([]*Record, len(h.records))
	copy(result, h.records)
	return result
}
