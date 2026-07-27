package wal

// LSN is a Write-Ahead Log sequence number. Monotonically increasing.
// LSN 0 (LSNInvalid) means "no WAL record has been written yet."
type LSN = uint64

const LSNInvalid LSN = 0
