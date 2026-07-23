# ADR 0001 — Two concurrency-control protocols side-by-side

**Status:** accepted
**Date:** 2026-04-27

## Context

This codebase exists to make concurrency-control anomalies (dirty
reads, lost updates, write skew, phantoms, deadlocks) observable and
debuggable. A single protocol (e.g. only 2PL or only MVCC) would make
half the curriculum invisible: 2PL cannot demonstrate write skew
under Repeatable Read, and MVCC cannot demonstrate lock-based
deadlocks. Teaching both side-by-side, against the same row storage
and the same scenario framework, makes the trade-offs tangible.

## Decision

Implement two protocols as equal peers:

- **Protocol2PL** — strict two-phase locking with intention locks
  (IS/IX/SIX), FIFO wait queues, and a background deadlock detector.
- **ProtocolMVCC** — version chains (newest-first), per-statement
  snapshots for Read Committed, transaction-level snapshots for
  Repeatable Read, and SSI for Serializable.

Each transaction selects its protocol at `BEGIN` and the `TxnManager`
dispatches every operation to the right code path. The two protocols
share the catalog, the undo log, and the metrics/metrics package, but
not the lock table or the MVCC store.

## Consequences

- The codebase is roughly twice the size of a single-protocol
  implementation, but every protocol-specific file is self-contained.
- Cross-protocol scenarios (e.g. a 2PL writer racing an MVCC reader)
  are intentionally NOT supported; the two protocols see independent
  state. This is documented in the README's "Anomaly scenarios" table.
- The HTTP API exposes `protocol` on `/api/txn/begin` so the frontend
  can drive either.

## Alternatives considered

- **Only MVCC** — rejected; cannot demonstrate lock-based deadlock
  cycles or 2PL gap-locking.
- **Only 2PL** — rejected; cannot demonstrate SSI write-skew
  prevention or MVCC's read-snapshot trade-offs.
- **A unified protocol with a "mode" flag** — rejected; would obscure
  rather than illuminate the differences.
