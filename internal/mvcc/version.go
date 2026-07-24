package mvcc

import (
	"time"

	"github.com/sanskarpan/TransactionManager/internal/types"
)

// TxnID is a uint64 transaction identifier
type TxnID = uint64

// Version represents one version of a row
type Version struct {
	XMin      TxnID
	XMax      TxnID // 0 = live (not deleted)
	Data      []types.Value
	Cmin      int // command counter within XMin
	CreatedAt time.Time
	Prev      *Version // older version (singly linked, head = newest)
}
