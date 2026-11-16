# Incremental.EnrichmentService

A host for `Incremental.Service` with the admin UI, written the way a real one would be:
a contract assembly, a pipeline plugin in two versions, and the adapters a host writes
for the systems only it knows. The guide that explains the service through this example
is [docs/enrichment-service.md](../../docs/enrichment-service.md); this page is the map
and the commands.

```
Incremental.EnrichmentService.csproj   the host (Microsoft.NET.Sdk.Web)
Program.cs                             the modes
Host/Node.cs                           one service node with its web server: registration, backend, admin
Host/Adapters.cs                       two table sources, a source connector, a sink connector
Host/World.cs                          command line, the feeds the adapters read, table and pipeline specs
Host/ReferenceData.cs                  what the generated world looks like; the feed, directory and file stand-ins
Host/Generator.cs                      trades, quotes and reference-data changes while a person watches
Host/KafkaStack.cs                     the Kafka mode's surroundings: Postgres tables, the Debezium connector, topics
Host/Smoke.cs, Host/Model.cs           --headless: the smoke test and the model it checks the sinks against
Contracts/                             Enrichment.Contracts: row types, table definitions, the trade and quote records
Pipelines/                             Enrichment.Pipelines: the five definitions below, one file each
Pipelines.V2/                          the same sources with V2 defined: version 2 of the same plugin
Pipelines.Tests/                       tests of the definitions with PipelineTestHarness: no node, no HTTP
Plugin.props                           what makes a project a plugin package
```

The build puts version 1 of the plugin in `plugins/enrichment.pipelines/` beside the
host, where the service imports it at first lead, and version 2 in
`plugin-packages/enrichment.pipelines-v2.zip`, to upload.

## The pipelines

Read them in this order; each adds one thing to the one before.

| Definition (file in `Pipelines/`) | Deployed as | Shape |
|---|---|---|
| `trade-enrichment` (`TradeEnrichment.cs`) | `trade-enrichment`, and again as `large-trades` with a higher `minNotional` | **Start here.** Keyed trades with event time → `Lookup` counterparties (on arrival) → `Join` instruments (retroactive; notional computed) → filter → `Join` trader-desks (retroactive) → upsert sink. A trade is open to corrections for `openSeconds`, then final and forgotten by every operator (`EvictWhenFinal`): the state is the open window, not the feed |
| `trader-desks` (`TraderDesks.cs`) | `trader-desks` | `traders.Changes() ⋈ desks → ToTable(trader-desks)`: a derived table, no input, no sink |
| `desk-exposure` (`DeskExposure.cs`) | `desk-exposure` | Trades as append-only executions → `LookupAppendOnly` trader-desks → event time → per desk, one-minute tumbling windows, `Sum` → upsert sink |
| `execution-quality` (`ExecutionQuality.cs`) | `execution-quality` | **Two inputs.** Executions `LookupAppendOnly` instruments (their asset class, with no state), then `LeftJoin` reference quotes on (instrument, minute), evicted by the watermark → per asset class, five-minute windows sliding by a minute, a custom linear aggregate → upsert sink. The quotes are read from files |
| `watchlist-alerts` (`WatchlistAlerts.cs`) | `sanctions-alerts` and `credit-alerts`: the table slot `watchlist` bound to a different table in each | Executions → a computed column that throws on a trade with no quantity → filter → `LookupAppendOnly` the watchlist → **append sink** to files. Deployed with `errorPolicy: DeadLetter` and a dead-letter sink |

Version 2 adds a computed column to `trade-enrichment`'s rows (its state is compatible:
replace with `keep`) and makes `desk-exposure` group by region (it is not: `reset`).

Their tests are in `Pipelines.Tests/`:

```bash
dotnet test --project examples/Incremental.EnrichmentService/Pipelines.Tests
```

## Running it

```bash
# one node, in memory, no docker — http://127.0.0.1:5180/admin/
dotnet run --project examples/Incremental.EnrichmentService

# leader and standby in one process, in memory — ports 5180 and 5181
dotnet run --project examples/Incremental.EnrichmentService -- --nodes 2

# the smoke test: exits 1 if any claim does not hold
dotnet run --project examples/Incremental.EnrichmentService -- --headless
dotnet run --project examples/Incremental.EnrichmentService -- --headless --nodes 2
```

In memory the tables are loaded by the two custom table sources and the built-in file
loader, trades come from the custom source connector, quotes from a file per minute
read by the built-in `files` connector, and the upsert sinks are in-process views at
`http://127.0.0.1:5180/views` and `/views/{name}`. The alerts and the dead letters are
JSON-lines files, one object per step, under the directory printed as `files:` —
`alerts/sanctions-alerts/`, `dead-letters/sanctions-alerts/` and the same for
`credit-alerts`. About one generated trade in 200 has no quantity (`--poison`), which
is what the two alert pipelines drop.

With `--nodes 2`, take over from the standby's *Cluster* page (or
`curl -X POST 'http://127.0.0.1:5181/admin/api/node/takeover?confirm=b' -H 'X-Incremental-Admin: 1'`).
The terminal then shows `Service node a is Fenced; it will not lead again in this
process` and `node supervisor: node a ended with exit code 3 (fenced: another node
leads) … starting it again in 200 ms`: b's page shows every pipeline Running, and
within a few seconds a is the standby again, behind b and ready to take over — two
nodes in one process are supervised in that process (`NodeSupervisor.RunInProcessAsync`;
the guide, *A node that stops leading exits*).

### Two nodes on Kafka

```bash
docker compose --profile connectors up -d     # Kafka :9092, Postgres :5432, Kafka Connect :8083

# terminal 1: node a — prepares the stack, then leads
dotnet run --project examples/Incremental.EnrichmentService -- --kafka --node a
# terminal 2: node b — the standby; start it once a is up
dotnet run --project examples/Incremental.EnrichmentService --no-build -- --kafka --node b
```

1. Open <http://127.0.0.1:5180/admin/> (a, Leader) and <http://127.0.0.1:5181/admin/>
   (b, Standby; its *Cluster* page says whether a takeover now would be warm).
2. Kill a. Terminal 1 runs a supervisor and, under it, the node; the supervisor printed
   the node's process id (`node supervisor: started process 12345`). Kill that one:
   `kill -9 $(pgrep -n -f 'EnrichmentService.* --node a')` (`-n`: the newer of the two).
3. Watch b's page: after about three seconds it is Leader, term 2, with every pipeline
   Running; *Cluster → Last takeover* shows the time by stage. It is `Degraded`
   (`no-ready-standby`) for the few seconds until a is back.
4. Terminal 1: the supervisor says the node ended and starts it again. It is the standby
   now, and nobody restarted it — a node that stops leading ends its process, and
   something must always bring it back (`NodeSupervisor` here; your platform's restart
   policy in production; `--no-supervise` to run the node alone).
5. Ctrl+C in terminal 2: b steps down, because a ready standby answers, and a leads
   within a second.

In this mode instruments, traders and desks are rows in Postgres
(`enrich_instruments`, `enrich_traders`, `enrich_desks`), carried to the tables by a
Debezium connector the example registers (`enrich-refdata`); counterparties
and the watchlists still come from files; trades are read from the topic
`enrich.trades` and quotes from `enrich.quotes`; the upsert sinks are the compacted
topics `enrich.out.trade-enrichment`, `enrich.out.large-trades`,
`enrich.out.desk-exposure` and `enrich.out.execution-quality`; the alerts are files as
before, in a directory both processes share; and the dead letters go to the topics
`enrich.dead.sanctions-alerts` and `enrich.dead.credit-alerts`. Change a row yourself
and watch it arrive:

```bash
docker compose exec postgres psql -U incremental -d incremental \
  -c "update enrich_traders set desk_id = 3 where id = 7"
```

The process that leads also generates trades and changes a row every five seconds.
Everything is named after `--prefix` (default `enrich`) and stays until removed:

```bash
dotnet run --project examples/Incremental.EnrichmentService -- --kafka --clean

# the smoke test on Kafka: node a as a supervised child process, killed with SIGKILL and back by itself; its own prefix, removed afterwards
dotnet run --project examples/Incremental.EnrichmentService -- --headless --kafka
```

Both nodes are on one machine here, so the admin API's loopback rule is satisfied and the
probe ports (5190, 5191) are on 127.0.0.1. Across machines, set an authorization policy
(`ServiceAdminOptions.AuthorizationPolicy` — without one every non-loopback request is
refused), listen on an address the peer can reach, and set `AdvertisedAdminUrl` to an
address a browser can reach.

## The host, in one block

<!-- snippet: examples/Incremental.EnrichmentService/Host/Node.cs#host -->
```csharp
var service = builder.Services.AddIncrementalService(o =>
    {
        o.PluginDirectory = Path.Combine(AppContext.BaseDirectory, "plugins");     // imported at first lead
        o.CacheDirectory = Path.Combine(world.DataDirectory, "cache-" + nodeId);    // one per node
        o.AdvertisedAdminUrl = $"http://127.0.0.1:{port}/admin/";                   // where a standby's UI sends you
        o.ExitOnLeadershipLoss = exitOnLeadershipLoss;
    })
    .AddContracts(typeof(Instrument).Assembly)                                      // row types and table definitions
    .AddObjectStore("refdata", _ => new LocalObjectStore(Path.Combine(world.DataDirectory, "refdata")))
    .AddObjectStore("lake", _ => world.Lake)                                        // quote files in, alerts and dead letters out
    .AddSinkConnector("view", _ => new ViewConnector(world.Views));
if (world.Kafka is null)
{
    // The generated feeds and directories: in memory only. On Kafka the generator
    // writes the trades topic and Postgres, and a slot bound to these would read
    // nothing — so they are not offered there, and a spec that names them is refused.
    service
        .AddTableSource("instrument-feed", _ => new InstrumentFeedSourceFactory(world.Instruments))
        .AddTableSource("directory", _ => new DirectorySourceFactory(world.Traders, world.Desks))
        .AddSourceConnector("trade-feed", _ => new TradeFeedConnector(world.Trades));
}
```

The project file is part of the host too: it imports
`src/Incremental.Service/Node.Gc.props`, the collector's settings for a process that is
a node (server, concurrent, not adaptive, and four heaps here where a production
node would have eight). The guide's *Sizing a node* has what each is for and what a
node's memory comes to; the cluster page of the admin UI shows this process's.

## Flags

| | |
|---|---|
| `--nodes 2` | in memory: a standby beside the leader |
| `--kafka`, `--node a\|b` | the Kafka backend; which node this process is (ports 5180/5190 or 5181/5191) |
| `--headless` | the smoke test |
| `--clean` | with `--kafka`: remove the connector and the offsets Kafka Connect keeps for it, the replication slot, the Postgres tables and every topic under the prefix; the prefix can then be used again from scratch |
| `--prefix name` | Kafka topic prefix and Postgres table prefix (lower-case letters and digits). The Kafka smoke (`--headless --kafka`) uses it for its run if given — and refuses a prefix that already has topics, since it removes everything under its prefix when it ends — or a fresh `smoke…` prefix if not |
| `--port N`, `--probe-port N`, `--peer-probe-port N` | |
| `--rate N` | trades per second (50) |
| `--trade-ids N` | trade ids come round after N, a later trade amending the earlier one (0, the default: never; the pipelines' state is bounded by event time, not by the ids) |
| `--poison N` | one trade in N has no quantity (200); 0 for none |
| `--ui-dir DIR` | serve the admin UI's files from DIR (`src/Incremental.Service.Admin/wwwroot`) instead of the embedded copies |
| `--otlp` | export the meters `Incremental.Service` and `Incremental` over OTLP; the endpoint is `OTEL_EXPORTER_OTLP_ENDPOINT` (the guide's *Operating it: monitoring*) |
| `--no-generate`, `--no-pipelines` | start with no generator; seed no pipelines |
| `--no-supervise` | with `--kafka`: run the node alone; without it the process supervises the node (`NodeSupervisor`) and starts it again when it ends fenced, resigned or killed |
| `--bootstrap`, `--postgres`, `--connect`, `--postgres-host-for-connect` | where the stack is, if not the compose file's |
| `--data dir` | where the reference files, the quote, alert and dead-letter files and the plugin caches go (a temporary directory by default) |
| `--verbose` | information-level logs |
