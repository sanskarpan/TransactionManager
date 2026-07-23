package lock

import "sync"

// Table is a global map of resource IDs to lock queues, safe for
// concurrent access via sync.Map.
type Table struct {
	queues sync.Map // ResourceID.String() -> *Queue
}

// NewTable creates an empty lock table.
func NewTable() *Table {
	return &Table{}
}

// GetOrCreate returns the queue for resource, creating one if needed.
func (t *Table) GetOrCreate(resource ResourceID) *Queue {
	key := resource.String()
	if v, ok := t.queues.Load(key); ok {
		return v.(*Queue)
	}
	q := NewQueue(resource)
	actual, _ := t.queues.LoadOrStore(key, q)
	return actual.(*Queue)
}

// Get returns the queue for resource, or false if it does not exist.
func (t *Table) Get(resource ResourceID) (*Queue, bool) {
	v, ok := t.queues.Load(resource.String())
	if !ok {
		return nil, false
	}
	return v.(*Queue), true
}

// Delete removes a queue entry. Callers must only invoke this after
// confirming the queue has no granted or waiting requests (otherwise a
// concurrent acquirer may re-introduce a queue via GetOrCreate, which is
// fine — the queue will simply be re-created in fresh state).
func (t *Table) Delete(resource ResourceID) {
	t.queues.Delete(resource.String())
}

// Snapshot returns a copy of every non-empty lock queue.
func (t *Table) Snapshot() []QueueSnapshot {
	var result []QueueSnapshot
	t.queues.Range(func(_, v interface{}) bool {
		q := v.(*Queue)
		snap := q.Snapshot()
		if len(snap.Granted) > 0 || len(snap.Waiting) > 0 {
			result = append(result, snap)
		}
		return true
	})
	return result
}
