# Incremental.Torture

A sustained-throughput rig for the showcase pipeline — the densest graph in the repo (three
joins, an anti-join, a full outer join, linear and non-linear aggregates, distinct, top-k,
tumbling/hopping/session windows and a recursive transitive closure). It exists to answer a
different question from `Incremental.Benchmarks`: not "what does one operator cost per delta
row" but "what does a real graph cost per record when it is running flat out".

```
dotnet run -c Release --project benchmarks/Incremental.Torture -- --workers 4 --seconds 20 --warmup 8
```

Add `--alloc-types` for a byte-weighted allocation histogram by type (an in-process
`GCAllocationTick` listener — no external tooling), `--rate N` to pace at N orders/second
instead of running unpaced, `--json` for a machine-readable summary line.

## What it does

Master data is seeded once. Order traffic is then a **sliding population**: inserting order
_N_ retracts order _N−LiveOrders_, so operator state settles at a fixed size instead of
growing for as long as the run lasts. That is what makes a long run measurable — throughput
is not confounded by dictionaries that keep getting bigger — and it exercises the
negative-weight paths on every step, which a pure-insert benchmark never touches.

Each order carries an insert, a retraction, and up to two payment rows, so the record rate
through the inputs is roughly 3.8x the order rate.

Worker saturation comes from `/proc/self/task`: the circuit names its threads
`circuit-worker-N`, so utime+stime of exactly those threads over wall time over the worker
count answers "are the workers busy?" without instrumenting the step path.

## Baseline

Measured on a 13th-gen i7-13700H, 4 workers, default dataset (150k live orders, 50k
customers, 2k merchants), unpaced:

| | |
|---|---|
| throughput | 160k records/s |
| CPU per record | 41.2 us (process), 14.6 us (workers only) |
| worker saturation | 58% |
| allocated | 11.3 KB/record — 1.7 GB/s |
| working set | 2.4 GB |

The process burns 6.6 cores to keep 4 workers 58% busy: at 1.7 GB/s of allocation the
server GC's own threads are the majority of the machine's work. Allocation, not the step
path, is the thing to attack.
