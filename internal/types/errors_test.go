package types

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTxnError_Error(t *testing.T) {
	err := NewDeadlockError()
	assert.Equal(t, "DEADLOCK: deadlock detected", err.Error())

	err2 := NewWriteConflictError(42)
	assert.Equal(t, "WRITE_CONFLICT: write-write conflict", err2.Error())

	err3 := NewSerializationFailure()
	assert.Equal(t, "SERIALIZATION_FAILURE: SSI detected dangerous structure", err3.Error())

	err4 := NewLockTimeoutError()
	assert.Equal(t, "LOCK_TIMEOUT: lock acquisition timed out", err4.Error())

	err5 := NewTxnAbortedError()
	assert.Equal(t, "TXN_ABORTED: transaction was aborted", err5.Error())

	err6 := NewTxnCommittedError(7)
	assert.Equal(t, "TXN_COMMITTED: transaction already committed", err6.Error())
	assert.Equal(t, uint64(7), err6.TxnID)

	err7 := NewTxnNotFoundError(99)
	assert.Equal(t, "TXN_NOT_FOUND: transaction not found", err7.Error())
	assert.Equal(t, uint64(99), err7.TxnID)

	err8 := NewRowNotFoundError()
	assert.Equal(t, "ROW_NOT_FOUND: row not found", err8.Error())

	err9 := NewDuplicateKeyError()
	assert.Equal(t, "DUPLICATE_KEY: duplicate key", err9.Error())

	err10 := NewTableNotFoundError("mytable")
	assert.Equal(t, "TABLE_NOT_FOUND: table \"mytable\" not found", err10.Error())
	assert.Equal(t, ErrTableNotFound, err10.Code)

	err11 := NewRowShapeError("invalid shape")
	assert.Equal(t, "INVALID_ROW_SHAPE: invalid shape", err11.Error())
}

func TestTxnError_Is(t *testing.T) {
	err := NewDeadlockError()

	assert.True(t, errors.Is(err, &TxnError{Code: ErrDeadlock}))
	assert.False(t, errors.Is(err, &TxnError{Code: ErrLockTimeout}))
	assert.False(t, errors.Is(err, errors.New("deadlock")))
	assert.False(t, errors.Is(err, nil))
}

func TestTxnError_Is_NonPointer(t *testing.T) {
	err := NewWriteConflictError(1)

	assert.True(t, errors.Is(err, &TxnError{Code: ErrWriteConflict}))
	assert.False(t, errors.Is(err, &TxnError{Code: ErrDeadlock}))
}

func TestErrorCodeConstants(t *testing.T) {
	assert.Equal(t, ErrorCode("DEADLOCK"), ErrDeadlock)
	assert.Equal(t, ErrorCode("WRITE_CONFLICT"), ErrWriteConflict)
	assert.Equal(t, ErrorCode("SERIALIZATION_FAILURE"), ErrSerializationFailure)
	assert.Equal(t, ErrorCode("LOCK_TIMEOUT"), ErrLockTimeout)
	assert.Equal(t, ErrorCode("TXN_ABORTED"), ErrTxnAborted)
	assert.Equal(t, ErrorCode("TXN_COMMITTED"), ErrTxnCommitted)
	assert.Equal(t, ErrorCode("TXN_NOT_FOUND"), ErrTxnNotFound)
	assert.Equal(t, ErrorCode("ROW_NOT_FOUND"), ErrRowNotFound)
	assert.Equal(t, ErrorCode("DUPLICATE_KEY"), ErrDuplicateKey)
	assert.Equal(t, ErrorCode("TABLE_NOT_FOUND"), ErrTableNotFound)
	assert.Equal(t, ErrorCode("INVALID_ISOLATION"), ErrInvalidIsolation)
	assert.Equal(t, ErrorCode("SAVEPOINT_NOT_FOUND"), ErrSavepointNotFound)
	assert.Equal(t, ErrorCode("WOUNDED"), ErrWounded)
	assert.Equal(t, ErrorCode("INVALID_ROW_SHAPE"), ErrInvalidRowShape)
}

func TestNewTableNotFoundError_QuotedName(t *testing.T) {
	err := NewTableNotFoundError("users")
	require.Contains(t, err.Error(), "\"users\"")
	require.Equal(t, ErrTableNotFound, err.Code)
}
