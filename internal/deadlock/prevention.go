// Package deadlock provides deadlock detection (Wait-For Graph cycle
// detection), prevention policies (Wait-Die and Wound-Wait), and a
// history of detected cycles for observability.
package deadlock

// PreventionPolicy selects the deadlock prevention strategy.
type PreventionPolicy int

const (
	// PolicyDetect uses cycle detection only (no prevention).
	PolicyDetect PreventionPolicy = iota
	// PolicyWaitDie uses the Wait-Die prevention policy.
	PolicyWaitDie
	// PolicyWoundWait uses the Wound-Wait prevention policy.
	PolicyWoundWait
)

// Action is the result returned by a prevention-policy function.
type Action int

const (
	// ActionWait instructs the caller to wait for the lock.
	ActionWait Action = iota
	// ActionDie instructs the caller to abort (die).
	ActionDie
	// ActionWound instructs the caller to wound the lock holder.
	ActionWound
)

// WaitDie decides that the older txn (lower ID) waits and the younger dies.
func WaitDie(requesterID, holderID TxnID) Action {
	if requesterID < holderID {
		return ActionWait
	}
	return ActionDie
}

// WoundWait decides that the older txn (lower ID) wounds the younger, and
// the younger waits for the older.
func WoundWait(requesterID, holderID TxnID) Action {
	if requesterID < holderID {
		return ActionWound
	}
	return ActionWait
}
