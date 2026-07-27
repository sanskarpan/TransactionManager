# Benchmarks

## TPC-B workload

The `benchmark/` package implements a TPC-B-inspired workload: random balance
transfers between accounts, with a balance-invariant verifier that asserts
`sum(balances) == initial_total` after every run.

```bash
# Run the TPC-B benchmark
go test ./benchmark/... -bench=. -benchmem -benchtime=10s
```

### What TPC-B measures

Each transaction:
1. Picks two random accounts (source, destination).
2. Reads both balances within a single MVCC snapshot.
3. Writes the new balances (transfer amount randomly chosen).
4. Commits.

After all transactions complete, the verifier sums all balances and asserts equality
with the seed total (100 accounts × $10,000 = $1,000,000).

A TPC-B failure reveals either a **lost update** (some balance written was overwritten
without being read back) or a **phantom** (a row was counted in the initial sum but
not seen by the transfer transaction).

---

## Internal microbenchmarks

Run benchmarks across all packages:

```bash
go test ./internal/... -bench=. -benchmem -benchtime=5s
```

Key targets:

| Benchmark | What it measures |
|---|---|
| `BenchmarkVersionChain_FindVisible` | Version chain traversal cost per row |
| `BenchmarkLockTable_AcquireRelease` | Lock acquire + release round-trip (no contention) |
| `BenchmarkLockTable_Contended` | Lock acquire under concurrent writers |
| `BenchmarkMVCCStore_GetOrCreate` | Sync.Map lookup + chain creation |
| `BenchmarkIsVisible` | Visibility predicate per version |

---

## Via the HTTP API

The benchmark can also be triggered through the API (requires admin token):

```bash
# Start a benchmark job
JOB=$(curl -s -X POST http://localhost:8080/api/benchmark/run \
  -H 'X-Admin-Token: <token>' \
  -H 'Content-Type: application/json' \
  -d '{"txns": 1000, "protocol": "mvcc", "isolation": "read_committed"}' | jq -r .jobId)

# Poll until done
curl -s http://localhost:8080/api/benchmark/results/$JOB | jq .
```

The result includes throughput (txns/sec), error count, duration, and whether the
balance invariant held.

---

## Makefile targets

```bash
make bench           # go test ./... -bench=. -benchmem
make race            # go test -race ./...
make coverage        # coverage report to coverage.out
make ci-local        # tidy + fmt + vet + race + coverage (CI parity)
```

---

## Notes on interpreting results

- **Contention level matters.** The lock table benchmarks measure different things
  at zero, low, and high contention. At zero contention, the cost is just the
  `sync.Map` lookup + mutex. At high contention, the cost is dominated by goroutine
  scheduling.

- **Protocol overhead.** MVCC reads have no lock overhead but pay for the version
  chain traversal. 2PL reads pay for the lock acquire/release. At low contention,
  MVCC is faster; at high write contention on the same key, 2PL's queue may outperform
  MVCC's retry loop.

- **Vacuum effects.** On long-running benchmarks, vacuum runs every 30 seconds and
  prunes dead versions. This causes a brief pause visible as a latency spike in the
  histogram.
