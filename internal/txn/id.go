package txn

// ID is the unique identifier of a transaction. It is a distinct named
// type so that accidentally mixing it with a raw uint64 is caught at
// compile time. Zero is reserved for the seed transaction (rows present
// before any user transaction began).
type ID uint64
