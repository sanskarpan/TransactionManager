// Package types defines the core Value representation and structured
// TxnError types used throughout the transaction manager.
package types

import (
	"errors"
	"fmt"
)

// ErrNullComparison is wrapped by Value.Compare when either operand is
// SQL NULL. The comparison layer surfaces it as a panic-equivalent so
// callers can use errors.Is to detect the bad comparison.
var ErrNullComparison = errors.New("cannot compare NULL values")

// ErrorCode is the machine-readable identifier of a TxnError, used by
// the HTTP API to map an internal error to a public status/message.
// Values are stable strings (not integers) so they can appear in JSON
// responses without an enum lookup.
type ErrorCode string

const (
	// ErrDeadlock is returned by the deadlock detector when a cycle in
	// the wait-for graph is found. The current txn is aborted; the caller
	// should retry.
	ErrDeadlock ErrorCode = "DEADLOCK"
	// ErrWriteConflict is returned by MVCC write paths when the chain
	// has a committed version that conflicts with the writer's snapshot.
	ErrWriteConflict ErrorCode = "WRITE_CONFLICT"
	// ErrSerializationFailure is returned by SSI commit-time validation
	// when a dangerous structure (rw-antidependency cycle) is detected.
	ErrSerializationFailure ErrorCode = "SERIALIZATION_FAILURE"
	// ErrLockTimeout is returned by Acquirer when a 2PL wait exceeds
	// the transaction's configured lock timeout.
	ErrLockTimeout ErrorCode = "LOCK_TIMEOUT"
	// ErrTxnAborted is returned from operations on a transaction that
	// has already been aborted (either explicitly or via a timeout / SSI).
	ErrTxnAborted ErrorCode = "TXN_ABORTED"
	// ErrTxnCommitted is returned when Commit/Abort was attempted on a
	// transaction that had already committed (C-05/C-06 fail-closed fix).
	ErrTxnCommitted ErrorCode = "TXN_COMMITTED"
	// ErrTxnNotFound is returned when an operation references an unknown
	// transaction ID (typically from a stale client or a typo in the URL).
	ErrTxnNotFound ErrorCode = "TXN_NOT_FOUND"
	// ErrRowNotFound is returned by MVCC update/delete when the row is
	// missing at the writer's snapshot.
	ErrRowNotFound ErrorCode = "ROW_NOT_FOUND"
	// ErrDuplicateKey is returned by the (planned) unique-index layer
	// when an insert conflicts with an existing row on the indexed key.
	ErrDuplicateKey ErrorCode = "DUPLICATE_KEY"
	// ErrTableNotFound is returned when an operation references a table
	// that the catalog does not know about.
	ErrTableNotFound ErrorCode = "TABLE_NOT_FOUND"
	// ErrInvalidIsolation is returned when the API receives an unknown
	// isolation-level string and is mapped to 400 Bad Request.
	ErrInvalidIsolation ErrorCode = "INVALID_ISOLATION"
	// ErrSavepointNotFound is returned when a rollback/release savepoint
	// call references a name that was never created.
	ErrSavepointNotFound ErrorCode = "SAVEPOINT_NOT_FOUND"
	// ErrWounded is returned by the Wound-Wait prevention policy when
	// the transaction was wounded by a higher-priority acquirer.
	ErrWounded ErrorCode = "WOUNDED"
	// ErrInvalidRowShape is returned when a write/insert value slice
	// does not match the target table's column declarations (CT-23).
	ErrInvalidRowShape ErrorCode = "INVALID_ROW_SHAPE"
)

// TxnError is the structured error type carried through the transaction
// manager and translated by api/errToPublicResponse into a safe HTTP
// response. Concrete errors are constructed via the New* helpers below.
type TxnError struct {
	Code           ErrorCode `json:"code"`
	Message        string    `json:"message"`
	TxnID          uint64    `json:"txnId,omitempty"`
	ConflictingTxn uint64    `json:"conflictingTxn,omitempty"`
	Resource       string    `json:"resource,omitempty"`
	RetryAfterMs   int       `json:"retryAfterMs,omitempty"`
}

// Error formats the error as "CODE: message". Implements the error
// interface so callers can wrap or log a TxnError directly.
func (e *TxnError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Is reports whether target is a *TxnError with the same Code. Lets
// errors.Is(err, &TxnError{Code: ErrDeadlock}) work for matching
// error codes without comparing the human-readable message.
func (e *TxnError) Is(target error) bool {
	t, ok := target.(*TxnError)
	if !ok {
		return false
	}
	return e.Code == t.Code
}

// NewDeadlockError returns the standard error returned by the deadlock
// detector when it aborts a victim. The HTTP layer maps it to 409
// Conflict with a "retry" message.
func NewDeadlockError() *TxnError {
	return &TxnError{Code: ErrDeadlock, Message: "deadlock detected"}
}

// NewWriteConflictError returns the error raised by MVCCWrite when a
// concurrent committed write is found in the chain. `conflicting` is
// the XMin of the conflicting version, surfaced for client metrics.
func NewWriteConflictError(conflicting uint64) *TxnError {
	return &TxnError{Code: ErrWriteConflict, Message: "write-write conflict", ConflictingTxn: conflicting}
}

// NewSerializationFailure returns the error raised by SSI commit-time
// validation when a dangerous rw-antidependency structure is detected.
// The transaction has already been aborted by the time this is returned.
func NewSerializationFailure() *TxnError {
	return &TxnError{Code: ErrSerializationFailure, Message: "SSI detected dangerous structure"}
}

// NewLockTimeoutError returns the error raised by Acquirer when a
// 2PL wait exceeds the txn's lockTimeout. The HTTP layer maps it to
// 408 Request Timeout.
func NewLockTimeoutError() *TxnError {
	return &TxnError{Code: ErrLockTimeout, Message: "lock acquisition timed out"}
}

// NewTxnAbortedError returns the error used when an operation targets a
// transaction that has already been aborted (e.g. by a slow request
// after a deadlock detector cleanup).
func NewTxnAbortedError() *TxnError {
	return &TxnError{Code: ErrTxnAborted, Message: "transaction was aborted"}
}

// NewTxnCommittedError signals that a Commit/Abort was attempted on a
// transaction that had already committed (C-05/C-06: previously Commit
// returned nil for an already-aborted txn and Abort returned nil for an
// already-committed txn, failing open at the API boundary).
func NewTxnCommittedError(id uint64) *TxnError {
	return &TxnError{Code: ErrTxnCommitted, Message: "transaction already committed", TxnID: id}
}

// NewTxnNotFoundError returns the error raised when an operation
// references an unknown transaction ID. The id is included so the
// HTTP layer can log it (while the public response still says only
// "transaction not found").
func NewTxnNotFoundError(id uint64) *TxnError {
	return &TxnError{Code: ErrTxnNotFound, Message: "transaction not found", TxnID: id}
}

// NewRowNotFoundError returns the error raised by MVCC update / delete
// when the row is not visible at the writer's snapshot. The HTTP layer
// maps it to 404 Not Found.
func NewRowNotFoundError() *TxnError {
	return &TxnError{Code: ErrRowNotFound, Message: "row not found"}
}

// NewDuplicateKeyError returns the duplicate-key error code. Currently
// unused (no unique-index enforcement yet); retained in the public API
// so a future unique-index layer has a stable error to return.
func NewDuplicateKeyError() *TxnError {
	return &TxnError{Code: ErrDuplicateKey, Message: "duplicate key"}
}

// NewTableNotFoundError returns the error raised when a read/write
// targets a table the catalog does not know. The table name is quoted
// in the message so the operator can identify the typo.
func NewTableNotFoundError(name string) *TxnError {
	return &TxnError{Code: ErrTableNotFound, Message: fmt.Sprintf("table %q not found", name)}
}

// NewRowShapeError returns a 400 for write/insert value slices that do not
// match the target table's column declarations (CT-23). Previously this
// used ErrInvalidIsolation as a workaround, which would misroute to
// "unknown isolation level" if the handler ever migrated to errWrite.
func NewRowShapeError(detail string) *TxnError {
	return &TxnError{Code: ErrInvalidRowShape, Message: detail}
}
