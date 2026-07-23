package txn

import (
	"testing"

	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestUndoLog_AppendAndLen(t *testing.T) {
	log := NewUndoLog()
	assert.Equal(t, 0, log.Len())
	log.Append(UndoUpdate, "accounts", "1", []types.Value{types.IntVal(100)})
	assert.Equal(t, 1, log.Len())
}

func TestUndoLog_EntriesFrom(t *testing.T) {
	log := NewUndoLog()
	log.Append(UndoUpdate, "t", storage.RowKey("1"), []types.Value{types.IntVal(1)})
	log.Append(UndoUpdate, "t", storage.RowKey("2"), []types.Value{types.IntVal(2)})
	log.Append(UndoUpdate, "t", storage.RowKey("3"), []types.Value{types.IntVal(3)})

	entries := log.EntriesFrom(1)
	assert.Len(t, entries, 2)
	assert.Equal(t, storage.RowKey("2"), entries[0].Key)
}

func TestUndoLog_TruncateTo(t *testing.T) {
	log := NewUndoLog()
	log.Append(UndoUpdate, "t", storage.RowKey("1"), nil)
	log.Append(UndoUpdate, "t", storage.RowKey("2"), nil)
	log.Append(UndoUpdate, "t", storage.RowKey("3"), nil)
	log.TruncateTo(1)
	assert.Equal(t, 1, log.Len())
}
