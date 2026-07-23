package lock

import "time"

// Status reflects whether a lock request is granted, waiting, or converting.
type Status int

const (
	// StatusGranted means the lock has been granted.
	StatusGranted Status = iota
	// StatusWaiting means the request is queued waiting for the lock.
	StatusWaiting
	// StatusConverting means the request is an upgrade waiting for the lock.
	StatusConverting
)

func (s Status) String() string {
	switch s {
	case StatusGranted:
		return "granted"
	case StatusWaiting:
		return "waiting"
	case StatusConverting:
		return "converting"
	}
	return "unknown"
}

// Request is one transaction's outstanding or granted request for a
// lock. GrantedCh is closed when the request transitions to granted.
type Request struct {
	TxnID     uint64
	Mode      Mode
	Status    Status
	GrantedCh chan struct{}
	RequestAt time.Time
	GrantedAt time.Time
}

// NewRequest creates a lock request with an unbuffered GrantedCh.
// The channel is closed by GrantWaiters when the request transitions to granted.
func NewRequest(txnID uint64, mode Mode) *Request {
	return &Request{
		TxnID:     txnID,
		Mode:      mode,
		GrantedCh: make(chan struct{}),
		RequestAt: time.Now(),
	}
}
