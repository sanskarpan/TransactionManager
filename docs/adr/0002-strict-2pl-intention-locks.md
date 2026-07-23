# ADR 0002 — Strict 2PL with intention locks

**Status:** accepted
**Date:** 2026-04-27

## Context

Two-phase locking (2PL) is the canonical pessimistic concurrency
control. Plain 2PL (S/X on rows only) forces every table-level
operation to lock every row, which is wasteful. Strict 2PL (locks
held until commit) avoids the cascading-abort problem of non-strict
2PL. Intention locks (IS/IX/SIX) allow table-level intent without
row-level enumeration.

## Decision

Implement strict 2PL with the full intention-lock hierarchy
(IS/IX/S/SIX/X; U is reserved in the compatibility matrix but no
caller requests it). The compatibility matrix is the standard SQL
one. Lock upgrades are supported via `LockAcquirer.UpgradeLock`
(defined; not currently invoked by the manager — every acquirer
path requests the final mode directly).

Wait queues are FIFO with a fairness break: `GrantWaiters` stops at
the first incompatible request so an X-lock waiter cannot be starved
by a stream of S-lock requesters.

## Consequences

- Locks are released only at commit/abort (`ReleaseAllLocks`),
  preventing cascading aborts.
- The lock table (`sync.Map` of `*LockQueue`) is reaped on
  `ReleaseAllLocks` when a queue becomes empty (H-03 fix), bounding
  memory growth over workloads touching distinct keys.
- Gap locking (ADR 0006) layers on top for Serializable.

## Alternatives considered

- **Non-strict 2PL** — rejected; cascading aborts are a teaching
  negative and a production hazard.
- **Table-lock-only** — rejected; concurrency too coarse.
- **No upgrade path** — accepted as the current state; the curriculum
  does not require on-the-fly upgrades.
