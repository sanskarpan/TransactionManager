# ADR 0005 — SSI for Serializable isolation

**Status:** accepted
**Date:** 2026-04-27

## Context

True Serializable under MVCC requires detecting the "dangerous
structure": a transaction that is the pivot in a cycle of
rw-anti-dependencies (read-write conflicts that are not write-write).
Plain Snapshot Isolation (Repeatable Read here) permits write skew:
two transactions each read the other's write-target, both commit,
invariant violated.

## Decision

Implement the SSI model from Cahill 2008 ("Serializable Isolation for
PostgreSQL") in `internal/isolation/ssi.go`:

- Every read at Serializable registers a SIREAD lock on the read key.
- Every write checks the SIREAD map for other readers; if found,
  sets `InConflict` on the reader and `OutConflict` on the writer,
  and records a `reader → writer` edge.
- At commit, a transaction with both `InConflict` and `OutConflict`
  is a pivot in a dangerous structure → `ErrSerializationFailure`.

## Consequences

- Write skew at Serializable is prevented (see
  `TestSSI_WriteSkew_DoctorOnCall`).
- False positives are possible: SSI aborts a transaction that is a
  pivot even if no actual conflict materializes. Clients must
  retry on `409 serialization failure`.
- The SIREAD map grows with the cardinality of read keys;
  `SSITracker.Cleanup` removes a transaction's entries at
  commit/abort, bounding growth to active transactions.
- SSI is only enabled at Serializable; lower levels skip SIREAD
  registration entirely.

## Alternatives considered

- **2PL with gap locks for Serializable** — also implemented (ADR
  0006); the two are alternative paths to the same guarantee.
- **Materialized conflict resolution (retry every abort)** — rejected;
  too coarse, no teaching value.
- **Full SERIALIZABLE via global ordering** — rejected; serializes
  all writes, defeats the point of MVCC.
