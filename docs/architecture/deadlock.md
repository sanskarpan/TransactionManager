# Deadlock Detection

> **See also:** [ADR-0003 — Deadlock resolution](../adr/0003-deadlock-resolution.md) · [2PL](2pl.md) · [Deadlock Cycle scenario](../scenarios.md#deadlock-cycle)

## Why deadlocks are inevitable with 2PL

Two-phase locking cannot prevent deadlocks — it can only detect and resolve them.
Any protocol that allows a transaction to hold one resource while waiting for another
can produce a cycle:

```
T1: holds X(row:A), waits for X(row:B)
T2: holds X(row:B), waits for X(row:A)
    → cycle → neither can proceed
```

Cycles of 3, 4, or more transactions are equally possible (and harder to spot without
graph analysis).

---

## The wait-for graph (WFG)

`internal/deadlock` maintains a directed **wait-for graph**:

- **Node**: each transaction currently holding or waiting for a lock.
- **Edge T1 → T2**: T1 is waiting for a lock held by T2.

| Operation | WFG change |
|---|---|
| T1 enters wait queue for lock held by T2 | `AddEdge(T1, T2)` |
| T1 is granted its lock (was waiting) | `RemoveEdges(T1)` — outgoing only |
| T1 commits or aborts | `RemoveNode(T1)` — incoming + outgoing |

**`RemoveEdges` vs `RemoveNode`:** when T1 is aborted by the deadlock detector,
its _waiting_ edges (outgoing) go away, but T2 may still have an edge pointing _at_
T1 from a different path. `RemoveNode` is the full cleanup at commit/abort;
`RemoveEdges` is the mid-abort cleanup used by the lock table when a waiter is
granted or times out.

```
// WFG snapshot (T1 → T2 → T3 → T1 cycle):
edges = {
    1: [2],   // T1 waits for T2
    2: [3],   // T2 waits for T3
    3: [1],   // T3 waits for T1  ← cycle closes here
}
```

---

## Detection algorithm — DFS cycle finder

A background goroutine runs every **50 ms**. It takes a snapshot of the edge map
(under a single read lock) and then runs DFS without holding any lock:

```
DetectCycle():
    snapshot = wfg.edgeSnapshot()        // read-locked copy

    visited = {}
    onStack = {}

    for each node n in snapshot:
        if n ∉ visited:
            path = DFS(n, snapshot, visited, onStack)
            if path != nil:
                return path              // first cycle found

DFS(node, edges, visited, onStack):
    mark node visited
    push node onto onStack

    for each neighbor m of node:
        if m ∉ visited:
            result = DFS(m, edges, visited, onStack)
            if result != nil: return result
        elif m ∈ onStack:
            // m is an ancestor → cycle found
            return extractCycle(onStack, m)

    pop node from onStack
    return nil
```

Running DFS on the _snapshot_ (not the live graph) avoids holding the WFG lock for
the full traversal — the traversal may be stale by a few milliseconds, but a cycle
that existed in the snapshot is a real deadlock, not a phantom.

---

## Victim selection

When a cycle `[T1, T2, ..., Tk]` is found, the **youngest transaction** (highest
numeric ID, because IDs are monotonically increasing) is selected as the victim:

```
victim = max(cycle by TxnID)
```

**Why youngest?**

- Younger transactions have done less work on average — aborting them wastes less.
- IDs are monotonic so `max(IDs)` is O(n) with one pass.
- Simple, deterministic, and easy to reason about.

The victim is aborted with `ErrDeadlock`. Its held locks are released, waking the
blocked transactions in the cycle.

---

## Liveness and resolution

After victim selection:

1. Victim's `Abort()` is called → releases all held locks via `heldLocks`.
2. `RemoveNode(victim)` cleans the WFG (incoming + outgoing edges).
3. Each lock queue wakes its first compatible waiter.
4. The next 50 ms tick verifies no cycle remains.

**Worst case latency:** up to ~100 ms from deadlock formation to resolution
(50 ms max wait for the next tick + time to abort + propagation).

---

## Alternative policies (not active)

The codebase includes skeleton implementations of two prevention policies, but they
are not wired into the lock table. They are provided for exploration and comparison:

| Policy | How it works | Advantage | Disadvantage |
|---|---|---|---|
| **Cycle detection** *(active)* | DFS after the fact; abort youngest | Low overhead when deadlocks are rare | Small resolution latency |
| **Wait-Die** | If requester is older than holder: wait; else die | No cycles possible | High abort rate under load |
| **Wound-Wait** | If requester is older: wound (abort) holder; else wait | Favors older txns | More complex; holder may be far into its work |

**Why cycle detection was chosen:** see [ADR-0003](../adr/0003-deadlock-resolution.md).

---

## Observability

The WFG is streamed in real time over `GET /sse/wfg` (500 ms tick):

```json
{
  "nodes": [1, 2, 3],
  "edges": [
    {"from": 1, "to": 2},
    {"from": 2, "to": 3},
    {"from": 3, "to": 1}
  ]
}
```

The React UI renders this as a live directed graph — the deadlock cycle is visually
obvious the moment it forms. The [Deadlock Cycle scenario](../scenarios.md#deadlock-cycle)
uses this stream to show the cycle appearing and then resolving within 50 ms.

Resolved deadlocks are appended to `GET /api/deadlocks`:

```json
[{
  "id": "dl-1234567890",
  "detectedAt": "2026-07-27T08:38:05Z",
  "cycle": [1, 2],
  "victim": 2,
  "victimReason": "youngest"
}]
```
