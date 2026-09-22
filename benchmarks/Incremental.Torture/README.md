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
| `--seconds N`, `--warmup N` | measured window and warm-up, in seconds (defaults 20 and 5) |
| `--warmup-steps N` | local runs also require this many warm-up steps (default 3000) before measuring — until 60 s of warm-up have passed, after which the clock wins: unpaced steps are large and slow, and the hot paths are compiled long before 3000 of them |
| `--workers`, `--batch`, `--input-capacity`, `--live-orders`, `--orders-per-vsec`, `--customers`, `--merchants`, `--categories`, `--fee-tiers`, `--regions`, `--rings`, `--seed`, `--json` | see `Options.cs` |

## What it does

Master data is seeded once. Order traffic is then a **sliding population**: inserting order
_N_ retracts order _N−LiveOrders_, so operator state settles at a fixed size instead of
growing for the length of the run. That is what makes a long run measurable — throughput is
not confounded by dictionaries that keep getting bigger — and it exercises the
negative-weight paths on every step, which a pure-insert benchmark never touches.

Each order carries an insert, a retraction, and up to two payment rows, so the record rate
through the inputs is roughly 3.8x the order rate.

The local rig reports measured-window step duration mean/p50/p95/p99 from the circuit's
meter, separate from the step interval (which includes idle time in paced runs). The
bounded histogram has one-microsecond bins, caps at one second, and reports overflows
in JSON. Exchange wait is the change in the cumulative worker-wait gauge over the
measurement window, divided by workers and completed steps; it includes recursion
barriers. The metrics share
approximate window boundaries: a step in flight at a boundary can contribute a record
count on one side and a completed duration on the other.

Worker saturation and the CPU split come from `/proc/self/task`: the circuit names its
threads `circuit-worker-N`, so utime+stime of exactly those threads — against the GC threads
and everything else — answers "where is the machine going?" without instrumenting the step
path. The GC's share is further split by thread name — `.NET Server GC` threads do the
blocking collections, `.NET BGC` threads the concurrent gen2 marking — which separates the
cost of allocation churn from the cost of owning a large live set. An in-process
`GCStart`/`GCEnd` listener (`GcProfiler`) adds per-generation collection counts and the
trigger reason of every gen2, the datum that says what to fix. (Blocking-GC wall time is
not observable from inside the process — a managed listener cannot run while the world is
stopped — so blocking time is reported as the `GC.GetTotalPauseDuration` delta instead.)

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
large, mostly-static live set means near-continuous gen2 work — measured: a background gen2
every second, 21 in a 20 s window. Setting the heap count to the worker count and turning
DATAS off takes this workload from **16.6 us to 11.6 us of CPU per record** at the cost of a
larger working set (2.2 GB to 2.6 GB).

Even then a background gen2 fires every ~1.5 s, and every one of them is
`AllocLarge`-triggered: at ~17 k records per step the output Z-sets' dictionary backing
arrays pass the default 85 KB large-object threshold, and that LOH churn keeps exhausting
the gen2 budget — each collection re-marking a ~2.5 GB live set that barely changed.
Raising the threshold so those arrays are ordinary gen0 garbage removes almost all of it
(13 gen2s in the window becomes 2, BGC-thread CPU 5.9 s becomes 0.9 s) and takes the
workload from **11.6 us to 10.5 us of CPU per record**, GC's share from 2.4 us to 1.6 us:

```sh
DOTNET_GCLOHThreshold=100000     # hexadecimal, like all DOTNET_GC* variables: 1 MB
```

or in runtimeconfig form (decimal), which is what `examples/Directory.Build.props` bakes in:
`<RuntimeHostConfigurationOption Include="System.GC.LOHThreshold" Value="1048576" />`.

The value is sized to the object population, not plucked: a delta-sized dictionary is a
prime-capacity `Entry[]` of 24–64 B entries (reference-record rows keep entries small;
`DateTimeOffset`-bearing window keys are the fat end), so the busiest edges at the default
8192-per-input batch allocate ~420–560 KB arrays. Measured across thresholds: 256 KB still
leaves 5 LOH-triggered gen2s in the window, 512 KB is the floor (2, same as 1 MB). The
needed threshold scales linearly with `--batch`, so 1 MB is the floor plus 2x headroom, and
a much larger batch wants a larger value again.

What remains after all three settings is ~15% of process CPU: a dozen ephemeral gen0/1
collections per window whose cost is dominated by scanning the card table of a large,
heavily-written gen2 (the stores) — churn interacting with live-set size, and the reason
allocation per record stays the number this rig watches.

## Output-buffer reuse (code, not tuning)

The LOH findings above led to removing the churn at its source: every operator now draws
its output Z-set from a double-buffered `OutputBuffer` instead of allocating one per step
(safe because output ports copy and nothing retains a delta past one more execution of its
producer — the argument is in `OutputBuffer.cs`). The delta-sized dictionary arrays were
~60% of every record's allocation, so this cut **2,544 B/record to ~970 B/record**; what
remains is mostly the pipeline's own projection rows plus input batches and store growth.
The tuned configuration now measures 11.2 us of CPU per record with GC at 1.3 us
**without** the LOH threshold — the setting stays worthwhile as belt-and-braces for input
batches and store growth, but the library no longer depends on any host GC configuration
to avoid the LOH-triggered gen2 pathology. The configuration a library cannot ask of its
consumers — completely stock workstation GC — goes from 133.8k records/s (11 gen2s and
25.9 us of GC+worker CPU per record) to 144.2k with 2 gen2s and GC-thread time near zero;
its remaining gap to server GC is ephemeral pauses card-scanning the ~2 GB live set,
which only the server GC settings address.

The last slice belongs to the pipeline, not the library: with the showcase's small row
and accumulator types declared as `readonly record struct` (see `Records.cs` /
`Aggregates.cs` and *Running it fast* in `docs/intro.md`), the fold-per-record aggregate
accumulators and per-delta-row projections stop touching the heap — **~950 B/record
becomes ~650 B**, GC CPU per record 1.5 us to 0.8 us measured back-to-back. What remains
is the publish-boundary copy (~285 B, the fee that makes every internal buffer reusable)
and the wide reference rows (`EnrichedOrder`, `Order`) that are better off as one
reference than as a dozen inline fields.

Step batch size trades the same way in miniature: bigger batches amortize per-step cost
across more records (worker CPU falls) but keep more step-scoped garbage alive across a
collection (GC CPU rises). On this workload `MaxStepItemsPerInput` around 4096 is the
minimum of the two.
