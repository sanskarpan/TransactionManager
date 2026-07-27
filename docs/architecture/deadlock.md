# Deadlock Detection

## Why deadlocks happen

With two-phase locking, a deadlock occurs when transactions form a **wait cycle**:

```
T1 holds X(row:A), waits for X(row:B)
T2 holds X(row:B), waits for X(row:A)
→ neither can proceed
```

The number of transactions in the cycle can be arbitrarily large. The system cannot distinguish "waiting for a slow transaction" from "waiting in a deadlock" without actively searching for cycles.

## The wait-for graph (WFG)

The `internal/deadlock` package maintains a directed **wait-for graph** (WFG):

- **Node**: every transaction that holds or waits for at least one lock.
- **Edge** T1 → T2: T1 is waiting for a lock held by T2.

Edges are added by the lock table when a transaction enters the wait queue, and removed when the transaction is granted its lock, aborted, or committed.

```go
// Adding an edge (T1 waits for T2)
wfg.AddEdge(T1, T2)

// Removing all edges from a transaction (it was granted or aborted)
wfg.RemoveNode(txnID)   // removes both incoming and outgoing edges
wfg.RemoveEdges(txnID)  // removes only outgoing edges (mid-abort cleanup)
```

The distinction between `RemoveNode` and `RemoveEdges` matters: when T1 is aborted by the deadlock detector, its **outgoing** edges (locks T1 was waiting for) disappear, but T2 may still have an edge pointing at T1's held locks — `RemoveNode` cleans both directions.

## Detection algorithm

A background goroutine runs DFS every **50 ms**:

```
for each node n in WFG:
    DFS from n, tracking visited set + current path
    if we reach a node already on the current path → cycle found
    return cycle as []TxnID
```

The DFS uses a snapshot of the graph edges (read under a single lock) to avoid holding the lock during the traversal.

## Victim selection

When a cycle is found, the **youngest transaction** (highest ID, since IDs are monotonically increasing) is chosen as the victim and aborted with `ErrDeadlock`. Youngest is a reasonable default because:

- Older transactions have done more work; aborting the youngest minimizes wasted work.
- IDs are monotonic so "youngest" is O(1) to compute.

The victim receives `ErrDeadlock` on its next API call. The client is expected to retry.

## Liveness

After victim selection:
1. The victim is aborted (all its locks released).
2. `RemoveNode(victim)` cleans the WFG.
3. Waiting transactions are woken by the lock queue.
4. The next DFS tick verifies no cycle remains.

The 50 ms tick means deadlocks are resolved within ~50–100 ms of forming.

## SSE stream

The WFG is streamed in real time via `GET /sse/wfg` (500 ms tick). The React UI renders it as a live directed graph, making deadlock cycles visually obvious.

## Alternative policies

The code skeleton includes `Wait-Die` and `Wound-Wait` implementations but they are not wired into the lock table (they are exploration code). The active policy is **cycle detection + youngest-victim**.

| Policy | Description | Deadlock-free? |
|---|---|---|
| Cycle detection (active) | DFS after the fact, abort youngest | No (detect + resolve) |
| Wait-Die | Older txn waits; younger txn dies | Yes (prevention) |
| Wound-Wait | Older txn wounds (aborts) younger | Yes (prevention) |

Prevention policies eliminate the DFS cost at the expense of aborting transactions preemptively, which increases false abort rates under low contention.
