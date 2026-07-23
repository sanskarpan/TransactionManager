# ADR 0006 — Gap locking under 2PL Serializable

**Status:** accepted
**Date:** 2026-04-27

## Context

Under 2PL Serializable, a range scan must not only lock the rows it
sees but also the *gaps* between them, so a concurrent insert cannot
add a phantom row inside the scanned range. Gap locking requires a
defined key ordering, a way to express "the open interval (lo, hi]",
and a way to detect insertions into a gap.

## Decision

Implement `LockForRangeScan` and `LockForInsert` in
`internal/txn/gap_lock.go`:

- The catalog's row keys are lexicographically ordered
  (`storage.Table.Keys` returns them sorted).
- A range scan `[lo, hi]` acquires S-locks on every row in the range
  AND on every gap `(prev, k]` between consecutive rows in the range,
  plus a trailing gap `(last, hi]` or `(last, ∞)`.
- An insert acquires X on the gap `(prev, next]` that surrounds the
  new key, plus X on the new row.

The "infinity" sentinel is the 4-byte string `\xff\xff\xff\xff`.
This requires row keys to be restricted to printable ASCII
(0x20–0x7E) so that no real key collides with the sentinel. The
seed data and the demo frontend use zero-padded integer keys, which
satisfy this. `isGapSafeKey` rejects keys containing non-printable
bytes with `ErrTxnAborted`.

## Consequences

- Phantom reads under 2PL Serializable are prevented (see
  `TestGapLock_InsertBlockedByRangeScan`).
- Gap locks are acquired in addition to row locks, increasing lock
  table cardinality. The H-03 fix reaps empty queues on release, so
  this does not cause unbounded growth.
- The printable-ASCII key constraint is a real restriction; a
  future version should use an explicit "upper-bound open" flag on
  `ResourceForGap` rather than a sentinel string.

## Alternatives considered

- **Predicate locking** — rejected; requires query parsing and is
  out of scope for a key-value API.
- **Serializable via SSI only** — rejected; 2PL Serializable is a
  distinct teaching artifact.
- **No gap locking (RR-only for 2PL)** — rejected; would make 2PL
  Serializable indistinguishable from Repeatable Read.
