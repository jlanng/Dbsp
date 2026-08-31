# Incremental.Torture

A sustained-throughput rig for the showcase pipeline — the densest graph in the repo (three
joins, an anti-join, a full outer join, linear and non-linear aggregates, distinct, top-k,
tumbling/hopping/session windows and a recursive transitive closure). It exists to answer a
different question from `Incremental.Benchmarks`: not "what does one operator cost per delta
row" but "what does a real graph cost per record when it is running flat out".

```sh
dotnet run -c Release --project benchmarks/Incremental.Torture -- --workers 4 --seconds 20 --warmup 8
```

| flag | |
|---|---|
| `--alloc-types` | byte-weighted allocation histogram by type (an in-process `GCAllocationTick` listener — no external tooling) |
| `--rate N` | pace at N orders/second instead of running unpaced |
| `--processes N` | cluster mode: N co-hosted processes stepped in lockstep, which puts the exchange wire format on the measured path |
| `--batch`, `--live-orders`, `--workers`, `--customers`, `--merchants`, `--json` | see `Options.cs` |

## What it does

Master data is seeded once. Order traffic is then a **sliding population**: inserting order
_N_ retracts order _N−LiveOrders_, so operator state settles at a fixed size instead of
growing for as long as the run lasts. That is what makes a long run measurable — throughput
is not confounded by dictionaries that keep getting bigger — and it exercises the
negative-weight paths on every step, which a pure-insert benchmark never touches.

Each order carries an insert, a retraction, and up to two payment rows, so the record rate
through the inputs is roughly 3.8x the order rate.

Worker saturation and the CPU split come from `/proc/self/task`: the circuit names its
threads `circuit-worker-N`, so utime+stime of exactly those threads — against the GC threads
and everything else — answers "where is the machine going?" without instrumenting the step
path.

## Results

13th-gen i7-13700H, 4 workers, default dataset (150k live orders, 50k customers, 2k
merchants), unpaced, with this rig's own GC settings — server and concurrent GC, default heap
count, DATAS on (the "Runtime tuning" section below is measured on top of this baseline):

| | before | after |
|---|---|---|
| throughput | 160k records/s | **290k records/s** |
| CPU per record | 41.2 us | **16.6 us** |
| — of which workers | 14.6 us | 9.4 us |
| — of which GC | 25.9 us | 7.1 us |
| allocated per record | 11.3 KB | **2.5 KB** |
| worker saturation | 58% | 68% |

Cluster mode (2 processes x 2 workers), where the exchange wire format is on the path:

| | before (JSON) | after (dense) |
|---|---|---|
| throughput | 194k records/s | **404k records/s** |
| CPU per record | 24.2 us | **10.7 us** |
| bytes per record on the wire | 353 B | **121 B** |

## Runtime tuning (not code)

Two GC settings are worth more than any single code change here, and neither is something
the library can set for you — they are startup configuration:

```xml
<ServerGarbageCollection>true</ServerGarbageCollection>
<ConcurrentGarbageCollection>true</ConcurrentGarbageCollection>
<GarbageCollectionAdaptationMode>0</GarbageCollectionAdaptationMode>   <!-- DATAS off -->
```
```sh
DOTNET_GCHeapCount=4      # match CircuitOptions.Workers, not the core count
```

`DOTNET_GCHeapCount` is read as **hexadecimal** — twelve heaps is `c`, and `16` gets you 22.
Where the worker count is fixed at build time, prefer the runtimeconfig form, which is
decimal: `<RuntimeHostConfigurationOption Include="System.GC.HeapCount" Value="4" />`.

The examples apply all of this by default — `examples/Directory.Build.props` for the three
properties, a `<CircuitWorkers>` property per example for the heap count. This rig
deliberately stops at server + concurrent GC: the table above is measured with DATAS on and
the default heap count, so that the tuning stays a separately visible number, and `--workers`
makes the right heap count a runtime choice anyway. To measure a tuned run, set the variable
on the command line:

```sh
DOTNET_GCHeapCount=4 dotnet run -c Release --project benchmarks/Incremental.Torture -- --workers 4
```

The server GC defaults to one heap per logical core, so a 4-worker circuit on a 20-thread
machine collects 20 heaps with 20 threads — most of them for memory the workers never touch.
DATAS (on by default for server GC since .NET 9) then keeps trimming the heap, which on a
large, mostly-static live set means near-continuous gen2 work. Setting the heap count to the
worker count and turning DATAS off takes this workload from **16.6 us to 11.6 us of CPU per
record** at the cost of a larger working set (2.2 GB to 2.6 GB).

Step batch size trades the same way in miniature: bigger batches amortize per-step cost
across more records (worker CPU falls) but keep more step-scoped garbage alive across a
collection (GC CPU rises). On this workload `MaxStepItemsPerInput` around 4096 is the
minimum of the two.
