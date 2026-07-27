# Snapshot Visibility Algorithm

This article walks through the visibility predicate implemented in
`internal/mvcc/visibility.go:IsVisible` — the function that decides
which version of a row a given transaction is allowed to see.

---

## Inputs

```go
func IsVisible(v *Version, txnID TxnID, snap SnapshotView, mgr TxnStatusChecker) bool
```

| Parameter | Meaning |
|---|---|
| `v` | The candidate version to evaluate |
| `txnID` | The reading transaction's own ID |
| `snap` | The snapshot taken at BEGIN or statement start |
| `mgr` | Status checker: IsCommitted, IsAborted, IsActive, TxnStatus |

A `Snapshot` contains:

```go
type Snapshot struct {
    Xmin   uint64   // reading txn's own ID
    Xmax   uint64   // next ID to allocate at snapshot time
    Active []uint64 // IDs active (uncommitted) at snapshot time
}
```

---

## The predicate, step by step

### Step 1 — Check who created this version (XMin)

**Case A: XMin is the reader itself**

```
v.XMin == txnID  →  the reader wrote this version; always visible
                    (subject to XMax check below)
```

**Case B: XMin is another transaction**

```
if NOT IsCommitted(v.XMin):  →  version is uncommitted → skip to XMax isn't needed
    return false              (uncommitted write from another txn is never visible)

if v.XMin >= snap.Xmax:      →  writer began after our snapshot → not visible
    return false

if v.XMin ∈ snap.Active:     →  writer was active (uncommitted) when we snapshotted
    return false              even if it later committed, from our view it hadn't
```

If none of the above triggered, the version was created by a transaction that:
- Committed
- Began before our snapshot (`XMin < Xmax`)
- Was not active at our snapshot time

→ **XMin is visible**. Proceed to XMax check.

### Step 2 — Check who deleted this version (XMax)

`XMax == 0` means the version is still live (not deleted or replaced).

```
if v.XMax == 0:
    return true    // version is current
```

If `XMax != 0`, the version has been deleted/replaced by transaction `v.XMax`:

**Case A: XMax is the reader itself**

```
v.XMax == txnID  →  we deleted this version ourselves → not visible to us
    return false
```

**Case B: XMax is another transaction**

```
if NOT IsCommitted(v.XMax):
    return true    // deleter hasn't committed — from our view, the version is still live

if v.XMax >= snap.Xmax:
    return true    // deleter began after our snapshot — from our view, deletion didn't happen

if v.XMax ∈ snap.Active:
    return true    // deleter was active (uncommitted) at our snapshot — delete not visible
```

If none triggered: the deleter committed before our snapshot and we are past the
deletion → **not visible**.

```
return false
```

---

## Complete pseudocode

```
IsVisible(v, txnID, snap, mgr):

    # ── XMin check ──────────────────────────────────────────────────
    if v.XMin == txnID:
        xmin_ok = true                # self-write
    elif NOT mgr.IsCommitted(v.XMin):
        return false                  # uncommitted writer
    elif v.XMin >= snap.Xmax:
        return false                  # writer started after our snapshot
    elif snap.Contains(v.XMin):       # snap.Contains checks snap.Active
        return false                  # writer was mid-flight at our snapshot
    else:
        xmin_ok = true

    # ── XMax check ──────────────────────────────────────────────────
    if v.XMax == 0:
        return true                   # not deleted

    if v.XMax == txnID:
        return false                  # we deleted it ourselves

    if NOT mgr.IsCommitted(v.XMax):
        return true                   # deleter uncommitted → deletion invisible

    if v.XMax >= snap.Xmax:
        return true                   # deleter after our snapshot → invisible

    if snap.Contains(v.XMax):
        return true                   # deleter was mid-flight → invisible

    return false                      # deleted before our snapshot → not visible
```

---

## Why `snap.Active` matters

Consider this timeline:

```
T3 commits at time t=10   (Xmax from T5's perspective = 8, so T3 < 8 → should be visible)

But T5's snapshot was taken at t=8 when T3 was still running:
Active = [T3]

Even though T3 has now committed, T5 cannot see T3's writes —
it would violate repeatable read (the snapshot is frozen at t=8).
```

The `Active` set captures exactly which transactions were mid-flight at snapshot time.
Even if they later commit, from this transaction's perspective they did not commit
"before the snapshot", and their changes remain invisible.

---

## Isolation level variations

At **Read Committed**, a new snapshot is taken before each statement, not just at BEGIN.

```
T1 snapshot at statement 1:   Xmax=5, Active=[T4]
T1 snapshot at statement 2:   Xmax=7, Active=[T6]   ← T4 and T5 now visible
```

This means a Read Committed transaction can see different data on successive reads of
the same row — the definition of a non-repeatable read.

At **Repeatable Read** and above, the snapshot is frozen at BEGIN:

```
T1 snapshot at BEGIN:  Xmax=5, Active=[T4]
All statements use this same snapshot.
```

Successive reads of the same row always return the same version.

---

## Edge case: Read Uncommitted

With MVCC at Read Uncommitted, the visibility predicate skips the `IsCommitted(XMin)`
check — it returns the latest version regardless of the writer's commit status. This
is implemented as a special case: an empty `Active` set and `Xmax = ∞` effectively
makes every version visible.

In a real system (PostgreSQL), MVCC RU behaves like RC — you cannot actually read an
uncommitted version through the chain. Here it is explicit for teaching purposes.

---

## The key invariant

No matter how many versions exist in the chain, the predicate returns at most **one**
visible version for any given `(txnID, snapshot)` pair. This follows from the
monotonicity of transaction IDs and the fact that a chain is ordered newest-first:
the first version where `IsVisible` returns `true` is the answer, and we stop there.
