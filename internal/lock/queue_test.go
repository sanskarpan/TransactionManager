package lock

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueue_SSCompatible(t *testing.T) {
	res := ResourceForRow("accounts", "1")
	q := NewQueue(res)

	req1 := NewRequest(1, LockS)
	req2 := NewRequest(2, LockS)

	q.mu.Lock()
	assert.True(t, q.CanGrant(LockS, 0))
	q.AddGranted(req1)
	assert.True(t, q.CanGrant(LockS, 0))
	q.AddGranted(req2)
	q.mu.Unlock()

	// Both S granted
	assert.Len(t, q.granted, 2)
	assert.Empty(t, q.waiting)
}

func TestQueue_SXIncompatible(t *testing.T) {
	res := ResourceForRow("accounts", "1")
	q := NewQueue(res)

	req1 := NewRequest(1, LockS)
	q.mu.Lock()
	q.AddGranted(req1)
	assert.False(t, q.CanGrant(LockX, 0))
	q.mu.Unlock()
}

func TestQueue_GrantWaiters(t *testing.T) {
	res := ResourceForRow("accounts", "1")
	q := NewQueue(res)

	// Txn 1 holds S
	req1 := NewRequest(1, LockS)
	q.mu.Lock()
	q.AddGranted(req1)

	// Txn 2 wants X — must wait
	req2 := NewRequest(2, LockX)
	q.AddWaiting(req2)
	q.mu.Unlock()

	// Release txn 1's S lock
	q.mu.Lock()
	q.RemoveGranted(1)
	q.GrantWaiters()
	q.mu.Unlock()

	// Txn 2's channel should be closed (granted)
	select {
	case <-req2.GrantedCh:
		// good
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected grant channel to be closed")
	}
}

func TestQueue_MultipleS_BlockedByX(t *testing.T) {
	res := ResourceForRow("accounts", "1")
	q := NewQueue(res)

	// Txn 1 holds X
	req1 := NewRequest(1, LockX)
	q.mu.Lock()
	q.AddGranted(req1)

	// Txns 2 and 3 both want S — must wait
	req2 := NewRequest(2, LockS)
	req3 := NewRequest(3, LockS)
	q.AddWaiting(req2)
	q.AddWaiting(req3)
	q.mu.Unlock()

	// Release txn 1's X lock
	q.mu.Lock()
	q.RemoveGranted(1)
	q.GrantWaiters()
	q.mu.Unlock()

	// Both S waiters should be granted
	select {
	case <-req2.GrantedCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("req2 not granted")
	}
	select {
	case <-req3.GrantedCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("req3 not granted")
	}
}

// TestC01_GrantWaiters_MixedOrder_NoDoubleClose reproduces C-01: when
// compatible waiters precede an incompatible one in the wait queue, the prior
// implementation re-appended already-granted waiters to `remaining` (because it
// used len(remaining) instead of the loop index), causing the next GrantWaiters
// to close(req.GrantedCh) on an already-closed channel → panic.
//
// The sequence: txn 1 holds X; txns 2 and 3 (S) and txn 4 (X) queue. Release
// the X: txn 2 and 3 (compatible S) grant, txn 4 (incompatible X) stays. A
// subsequent GrantWaiters (e.g. after a release) must not close 2/3's channels
// a second time.
func TestC01_GrantWaiters_MixedOrder_NoDoubleClose(t *testing.T) {
	res := ResourceForRow("accounts", "1")
	q := NewQueue(res)

	req1 := NewRequest(1, LockX)
	q.mu.Lock()
	q.AddGranted(req1)
	req2 := NewRequest(2, LockS)
	req3 := NewRequest(3, LockS)
	req4 := NewRequest(4, LockX)
	q.AddWaiting(req2)
	q.AddWaiting(req3)
	q.AddWaiting(req4)
	q.mu.Unlock()

	// Release txn 1's X → GrantWaiters grants 2 and 3, keeps 4.
	q.mu.Lock()
	q.RemoveGranted(1)
	q.GrantWaiters()
	q.mu.Unlock()

	// 2 and 3 must be granted; 4 must still be waiting.
	assert.True(t, req2.Status == StatusGranted, "req2 should be granted")
	assert.True(t, req3.Status == StatusGranted, "req3 should be granted")
	assert.True(t, req4.Status == StatusWaiting, "req4 should still be waiting")
	assert.Len(t, q.granted, 2)
	assert.Len(t, q.waiting, 1)

	// Now release 2 and 3 simultaneously and GrantWaiters again. The prior bug
	// would have re-added 2 and 3 to `waiting` (with closed GrantedCh) and the
	// second GrantWaiters would close them again → panic. With the fix, this
	// cleanly grants 4.
	q.mu.Lock()
	q.RemoveGranted(2)
	q.RemoveGranted(3)
	q.GrantWaiters()
	q.mu.Unlock()

	assert.True(t, req4.Status == StatusGranted, "req4 should be granted after 2&3 released")
	assert.Len(t, q.granted, 1)
	assert.Empty(t, q.waiting)

	// And the channels of 2/3 (closed once) must still receive without panic.
	select {
	case <-req2.GrantedCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("req2 GrantedCh should remain closed")
	}
	select {
	case <-req3.GrantedCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("req3 GrantedCh should remain closed")
	}
}

func TestQueue_AddWaitingFront(t *testing.T) {
	res := ResourceForRow("accounts", "1")
	q := NewQueue(res)

	req1 := NewRequest(1, LockX)
	req2 := NewRequest(2, LockS)
	req3 := NewRequest(3, LockS)

	q.mu.Lock()
	q.AddGranted(req1)
	q.AddWaiting(req2)
	q.AddWaitingFront(req3)
	q.mu.Unlock()

	assert.Len(t, q.waiting, 2)
	assert.Equal(t, uint64(3), q.waiting[0].TxnID, "front request should be first")
	assert.Equal(t, uint64(2), q.waiting[1].TxnID, "existing request should be second")
	assert.Equal(t, StatusConverting, q.waiting[0].Status, "front request should have StatusConverting")
}

func TestQueue_RaceCondition(_ *testing.T) {
	res := ResourceForRow("accounts", "1")
	q := NewQueue(res)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			req := NewRequest(id, LockS)
			q.mu.Lock()
			if q.CanGrant(LockS, id) {
				q.AddGranted(req)
				q.mu.Unlock()
				time.Sleep(time.Millisecond)
				q.mu.Lock()
				q.RemoveGranted(id)
				q.GrantWaiters()
				q.mu.Unlock()
			} else {
				q.AddWaiting(req)
				q.mu.Unlock()
			}
		}(uint64(i + 1))
	}
	wg.Wait()
}
