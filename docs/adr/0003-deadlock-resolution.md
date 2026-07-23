# ADR 0003 — Deadlock resolution: background DFS detector

**Status:** accepted
**Date:** 2026-04-27
**Supersedes:** nothing

## Context

Under strict 2PL, transactions acquire exclusive locks on rows before
writing. Two transactions each holding a lock the other needs form a
cycle in the wait-for graph (WFG). Left unresolved, both block until
their lock timeout fires — which can be 30 s of degraded throughput
before the system recovers. We need a resolution strategy that is
simple to reason about, fits the teaching narrative, and is reliable
under load.

The classic menu:

1. **Prevention** — Wait-Die and Wound-Wait reject or abort a
   transaction at acquire time so cycles can never form.
2. **Detection** — let transactions block, run a background DFS over
   the WFG, and abort a victim when a cycle is found.

## Decision

Implement **detection** as the production path: a background
`DeadlockDetector` ticks every 50 ms (see
`api.DeadlockDetectorTickInterval`), runs DFS over the WFG, picks the
youngest transaction in the cycle as the victim, removes the victim's
WFG node **before** invoking the abort callback (H-09 fix — previously
the next tick could re-find the same cycle and emit a duplicate
record), and aborts the victim.

Implement Wait-Die and Wound-Wait as `deadlock.WaitDie` /
`deadlock.WoundWait` functions with tests, but **do not wire them into
`LockAcquirer.AcquireLockCtx`**. They are retained as drop-in policies
for a future change.

## Consequences

- 50 ms worst-case time-to-detection; throughput impact is negligible
  (DFS over a few hundred nodes is sub-millisecond).
- Victim selection = "youngest" is the simplest fairness rule: the
  transaction that has done the least work (lowest ID = started first
  in our model is *oldest*; highest = youngest, done least) is the
  victim. See `selectVictim`.
- The 50 ms tick is a magic number; it lives in
  `api.DeadlockDetectorTickInterval` so the runbook can reference it
  by name.
- `WaitDie`/`WoundWait` are dead code until wired in; we mark them as
  such in `internal/deadlock/prevention.go` rather than deleting
  them, because the curriculum references them.

## Alternatives considered

- **Wait-Die as the default** — rejected; produces aborts even when no
  cycle would have formed, which is noisy for the demo frontend.
- **Edge-chasing (Chandy-Misra-Haas)** — rejected; overkill for a
  single-node in-memory system.
- **Timeout-only** — rejected; 30 s of blocking is too long for the
  interactive UI.
