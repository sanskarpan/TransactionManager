package txn

// Savepoint records a named point within a transaction's undo log that the
// transaction can be rolled back to. UndoPosition is the length of the undo
// log at the time the savepoint was created.
type Savepoint struct {
	Name         string
	UndoPosition int
}
