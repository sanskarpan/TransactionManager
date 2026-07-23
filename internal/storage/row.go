package storage

import "github.com/sanskarpan/TransactionManager/internal/types"

// RowKey is the unique identifier for a row within a table.
type RowKey string

// Row is a single row's key-value pair.
type Row struct {
	Key    RowKey
	Values []types.Value
}
