# ADR 0004 — MVCC visibility predicate and snapshot semantics

**Status:** accepted
**Date:** 2026-04-27

## Context

MVCC needs a visibility predicate that says, given a version `V` and a
transaction `T` with snapshot `S`, whether `V` is visible to `T`.
The predicate must handle: self-created versions, versions committed
before the snapshot, versions committed during the snapshot window,
versions deleted by aborted or in-flight deleters, and versions
created by transactions that started after the snapshot.

## Decision

Implement the standard PostgreSQL-style visibility rules in
`internal/mvcc/visibility.go:IsVisible`. A version `V` (with creator
`XMin` and deleter `XMax`) is visible to transaction `T` with snapshot
`S` iff:

1. `V.XMin` is visible to `T` (one of: self-created; committed before
   `S.Xmin`; committed during the snapshot window and not in
   `S.Active`), AND
2. `V.XMax` is "invisible" to `T` (one of: zero = not deleted;
   belongs to `T` = self-deletion; `XMax` was active at snapshot
   time; `XMax` started after the snapshot; `XMax` aborted).

Snapshots are taken at `BEGIN` for Repeatable Read and Serializable;
Read Committed takes a fresh snapshot per statement via
`getSnapshot`.

## Consequences

- Dirty reads are impossible above Read Uncommitted: an uncommitted
  writer's `XMin` fails step 1.
- Non-repeatable reads are impossible above Read Committed: the
  per-statement snapshot sees the latest committed state but a
  single statement's two reads see the same snapshot.
- Phantoms are impossible at Repeatable Read: the snapshot's
  `Active` list + `Xmax` bound means a row inserted by a txn that
  started after the snapshot is invisible.
- Write-write conflicts are detected at write time by checking the
  chain head against the snapshot; the previous head must not be
  owned by a concurrent active or committed-in-window transaction.

## Alternatives considered

- **Tuple-level timestamp ordering** — rejected; harder to teach
  the snapshot concept and to demonstrate anomalies.
- **First-committed-wins (no abort)** — rejected; produces lost
  updates at Repeatable Read, defeating the curriculum.
