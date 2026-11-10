using System.Text.Json;
using Acme.Contracts;
using Incremental.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using static Incremental.Service.Tests.SupervisorTests;
using static Incremental.Service.Tests.TestService;

namespace Incremental.Service.Tests;

public class NodeTests
{
    private static bool Same<TKey, TRow>(IReadOnlyDictionary<TKey, TRow> actual, IReadOnlyDictionary<TKey, TRow> expected) where TKey : notnull =>
        actual.Count == expected.Count && expected.All(e => actual.TryGetValue(e.Key, out var row) && Equals(row, e.Value));

    private static Dictionary<int, Position> PositionModel(IEnumerable<Trade> trades) =>
        trades.GroupBy(t => t.InstrumentId).ToDictionary(g => g.Key, g => new Position(g.Key, g.Sum(t => t.Quantity)));

    /// <summary>Four pipelines over two tables: a join, an aggregate, a derived table's
    /// writer and its reader.</summary>
    private static async Task DeployAllAsync(TestService service, List<Instrument> instruments)
    {
        await service.DeclareInstrumentsAsync(instruments);
        await service.Tables.DeclareAsync(new TableSpec { Name = "positions", Definition = typeof(PositionsTable).FullName! }, default);
        await service.DeployAsync(Spec("enrich", "enrich", "trades", "enriched"));
        await service.DeployAsync(Spec("counts", "counts", "trades", "counts"));
        await service.DeployAsync(Spec("positions", "positions", "trades"));
        await service.DeployAsync(Spec("report", "position-report", null, "report"));
    }

    private static async Task SinksEqualModelAsync(MemoryConnector memory, List<Trade> trades, List<Instrument> instruments, string when)
    {
        var enriched = memory.UpsertSink<long, EnrichedTrade>("enriched");
        var counts = memory.UpsertSink<string, long>("counts");
        var report = memory.UpsertSink<int, Position>("report");
        await Eventually(() => Same(enriched.Rows, Enrich.Model(trades, instruments)), $"the enriched sink {when}");
        await Eventually(() => Same(counts.Rows, Counts.Model(trades)), $"the counts sink {when}");
        await Eventually(() => Same(report.Rows, PositionModel(trades)), $"the report sink {when}");
    }

    [Fact]
    public async Task Restarting_the_whole_service_reproduces_its_pipelines_and_resumes_from_their_checkpoints()
    {
        // The cluster stands in for durable storage: it outlives the node, as a broker would.
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        var instruments = Instruments(10);
        var trades = Trades(0, 400);
        var log = memory.Log<Trade>("trades");

        IReadOnlyList<PipelineSpec> specsBefore;
        long catalogBefore;
        await using (var first = await StartAsync(cluster, "a", memory))
        {
            await DeployAllAsync(first, instruments);
            await first.DeployAsync(Spec("idle", "counts", "idle-in", "idle-out") with { Desired = DesiredState.Stopped });
            log.AppendRange(trades);
            await SinksEqualModelAsync(memory, trades, instruments, "before the restart");
            specsBefore = first.Pipelines.Pipelines.Select(p => first.Pipelines.Spec(p.Id)).ToList();
            catalogBefore = first.Node.Catalog.Version;
        }

        // While the service is down the world moves on.
        var later = Trades(400, 250);
        log.AppendRange(later);
        trades.AddRange(later);

        await using var second = await StartAsync(cluster, "a", memory);

        // The same pipelines, from the catalog and nothing else: no seed, no deploy call.
        Assert.True(second.Node.IsLeader);
        Assert.Equal(
            specsBefore.Select(s => JsonSerializer.Serialize(s)).Order(),
            second.Pipelines.Pipelines.Select(p => JsonSerializer.Serialize(second.Pipelines.Spec(p.Id))).Order());
        Assert.Equal(["instruments", "positions"], second.Tables.Tables.Select(t => t.Name));
        foreach (var id in new[] { "enrich", "counts", "positions", "report" })
            await second.Running(id);
        Assert.Equal(PipelineState.Stopped, second.Pipelines.Get("idle").State);

        // Tables came back from their logs with their rows.
        Assert.Equal(10, second.Tables.Get<int, Instrument>("instruments").Count);

        // Sinks equal a model computed over everything, before and after.
        await SinksEqualModelAsync(memory, trades, instruments, "after the restart");

        // And they got there by resuming, not replaying: each source read only what was
        // appended while the service was down, on top of the state its checkpoint held.
        foreach (var id in new[] { "enrich", "counts", "positions" })
            Assert.Equal(later.Count, second.Pipelines.Get(id).Sources.Single().Stats.Records);
        Assert.Equal(649, second.Pipelines.Get("counts").CommittedOffsets["trades#p0"]);

        // The same spec, as a caller would format it rather than as the catalog stores it,
        // is no change: it needs no state choice and replays nothing.
        var again = await second.DeployAsync(Spec("counts", "counts", "trades", "counts"));
        Assert.Contains(again.Plan.Steps, s => s.Contains("nothing that touches it changed"));
        await second.Running("counts");
        await SinksEqualModelAsync(memory, trades, instruments, "after redeploying an unchanged spec");
        Assert.Equal(0, second.Pipelines.Get("counts").Sources.Single().Stats.Records);

        // A table change after the restart still corrects what was joined before it.
        instruments[3] = instruments[3] with { Symbol = "RENAMED" };
        var table = second.Tables.Get<int, Instrument>("instruments");
        await table.Writer.UpsertAsync(instruments[3]);
        await table.Writer.CommitAsync();
        await SinksEqualModelAsync(memory, trades, instruments, "after a table change");
    }

    [Fact]
    public async Task A_standby_follows_then_takes_over_and_runs_the_same_pipelines_to_the_same_sinks()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        var instruments = Instruments(10);
        var trades = Trades(0, 300);
        var log = memory.Log<Trade>("trades");

        await using var a = await StartAsync(cluster, "a", memory);
        await using var b = await StartAsync(cluster, "b", memory);
        Assert.Equal(NodeRole.Leader, a.Node.Status.Leadership.Role);
        Assert.Equal(NodeRole.Standby, b.Node.Status.Leadership.Role);
        Assert.Equal("a", b.Node.Status.Leadership.LeaderNodeId);

        await DeployAllAsync(a, instruments);
        log.AppendRange(trades);
        await SinksEqualModelAsync(memory, trades, instruments, "under the first leader");

        // The standby follows the catalog and the tables; it builds nothing.
        await Eventually(() => b.Node.Status.Standby is { TakeoverWouldBeWarm: true }, "the standby being warm");
        await Eventually(() => b.Pipelines.Pipelines.Count == 4 && b.Tables.Tables.Count == 2, "the standby seeing the catalog");
        Assert.All(b.Pipelines.Pipelines, p => Assert.Equal(PipelineState.Unbuilt, p.State));
        Assert.True(b.Tables.Stats("instruments").Following);
        await Eventually(() => b.Tables.Stats("positions").Rows == a.Tables.Stats("positions").Rows, "the standby's copy of the derived table");
        Assert.Equal(HealthLevel.Healthy, b.Node.Health.Level);
        Assert.Equal(new PeerInfo("b", NodeRole.Standby, 0, true, null), await a.Node.Backend.Leadership.ProbePeerAsync(default));

        // Mutations go to the leader.
        var notLeader = await Assert.ThrowsAsync<ServiceException>(() => b.DeployAsync(Spec("x", "counts", "in", "out")));
        Assert.Equal("not-leader", notLeader.Code);
        Assert.Contains("'a'", notLeader.Message);

        // Takeover: the dry run says what will happen and does nothing.
        var dry = await b.Node.TakeOverAsync(new NodeActionOptions(DryRun: true), default);
        Assert.Contains(dry.Steps, s => s.Contains("node 'a' is fenced"));
        Assert.Equal(NodeRole.Standby, b.Node.Status.Leadership.Role);

        await b.Node.TakeOverAsync(new NodeActionOptions(Caller: "tester"), default);
        Assert.Equal(NodeRole.Leader, b.Node.Status.Leadership.Role);
        Assert.True(b.Node.Status.Leadership.Term > a.Node.Status.Leadership.Term);
        Assert.Equal(NodeRole.Fenced, a.Node.Status.Leadership.Role);

        // The fenced node can write nothing: not the catalog, not a table.
        await Eventually(() => !a.Node.IsLeader, "the old leader knowing it is fenced");
        await Assert.ThrowsAsync<ServiceException>(() => a.DeployAsync(Spec("x", "counts", "in", "out")));
        var oldTable = a.Tables.Get<int, Instrument>("instruments");
        await oldTable.Writer.UpsertAsync(new Instrument(99, "GHOST", "ISIN99", 1));
        await Assert.ThrowsAnyAsync<Exception>(async () => await oldTable.Writer.CommitAsync());
        await Eventually(() => a.Pipelines.Pipelines.All(p => p.State != PipelineState.Running), "the old leader's circuits abandoned");
        Assert.Equal(HealthLevel.Unhealthy, a.Node.Health.Level);

        // The new leader built every pipeline from the state it followed as bytes.
        foreach (var id in new[] { "enrich", "counts", "positions", "report" })
            await b.Running(id);
        var later = Trades(300, 200);
        log.AppendRange(later);
        trades.AddRange(later);
        await SinksEqualModelAsync(memory, trades, instruments, "under the second leader");
        Assert.DoesNotContain(b.Tables.Get<int, Instrument>("instruments").Snapshot().Rows(), r => r.Key == 99);

        // Resumed, not replayed: the new leader's sources read only what came after.
        Assert.Equal(later.Count, b.Pipelines.Get("counts").Sources.Single().Stats.Records);
        Assert.Contains(b.Node.Catalog.Audit(), e => e is { Action: "node.takeover", Caller: "tester" });
        Assert.Equal("b", b.Node.Catalog.Get<LeaderRecord>("node/leader")!.NodeId);
    }

    [Fact]
    public async Task Step_down_hands_over_to_a_ready_standby_and_is_refused_without_one()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        await using var a = await StartAsync(cluster, "a", memory);

        // Alone: stepping down would stop the service, so it is refused unless forced.
        var alone = await Assert.ThrowsAsync<ServiceException>(() => a.Node.StepDownAsync(new NodeActionOptions(), default));
        Assert.Equal("not-ready", alone.Code);
        Assert.Contains("No standby answered", alone.Message);
        Assert.Equal(NodeRole.Leader, a.Node.Status.Leadership.Role);

        await a.DeployAsync(Spec("counts", "counts", "trades", "counts"));
        var trades = Trades(0, 200);
        memory.Log<Trade>("trades").AppendRange(trades);
        var counts = memory.UpsertSink<string, long>("counts");
        await Eventually(() => Same(counts.Rows, Counts.Model(trades)), "the first leader's output");

        await using var b = await StartAsync(cluster, "b", memory);
        await Eventually(() => b.Node.Status.Standby is { TakeoverWouldBeWarm: true } && b.Pipelines.Pipelines.Count == 1, "the standby being warm");

        // A leader cannot take over; a standby cannot step down.
        await Assert.ThrowsAsync<ServiceException>(() => a.Node.TakeOverAsync(new NodeActionOptions(), default));
        await Assert.ThrowsAsync<ServiceException>(() => b.Node.StepDownAsync(new NodeActionOptions(), default));

        var plan = await a.Node.StepDownAsync(new NodeActionOptions(), default);
        Assert.Contains(plan.Steps, s => s.Contains("node 'b' takes over"));
        Assert.Equal(NodeRole.Resigned, a.Node.Status.Leadership.Role);
        Assert.Equal(PipelineState.Stopped, a.Pipelines.Get("counts").State);

        // The standby leads without being asked, and continues from the drained checkpoint.
        await Eventually(() => b.Node.Status.Leadership.Role == NodeRole.Leader, "the standby leading after the resignation");
        await b.Running("counts");
        var later = Trades(200, 100);
        memory.Log<Trade>("trades").AppendRange(later);
        await Eventually(() => Same(counts.Rows, Counts.Model(trades.Concat(later))), "the second leader's output");
        Assert.Equal(later.Count, b.Pipelines.Get("counts").Sources.Single().Stats.Records);
    }

    [Fact]
    public async Task One_table_that_cannot_open_and_one_pipeline_that_cannot_build_do_not_block_a_takeover()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        await using var a = await StartAsync(cluster, "a", memory);
        await a.DeclareInstrumentsAsync(Instruments(5));
        await a.DeployAsync(Spec("good", "counts", "trades", "counts"));
        await a.DeployAsync(Spec("joined", "enrich", "trades", "enriched"));

        // Entries the next leader cannot act on: a table whose definition no contract
        // holds, a pipeline whose plugin version is not stored.
        using (var write = await a.Node.Catalog.BeginAsync(default))
        {
            write.Put("table/broken", new TableSpec { Name = "broken", Definition = "Acme.Contracts.NoSuchTable" });
            write.Put("pipeline/orphan", Spec("orphan", "counts", "in", "out", version: "0000000000000000") with { Generation = 1 });
            await write.CommitAsync(default);
        }

        await using var b = await StartAsync(cluster, "b", memory);
        await Eventually(() => b.Pipelines.Pipelines.Count == 3, "the standby seeing the catalog");
        await b.Node.TakeOverAsync(new NodeActionOptions(Force: true), default);

        Assert.Equal(NodeRole.Leader, b.Node.Status.Leadership.Role);
        await b.Running("good");
        await b.Running("joined");
        await Eventually(() => b.Pipelines.Get("orphan").State is PipelineState.Backoff or PipelineState.Failed, "the orphan pipeline failing by itself");
        Assert.Contains("0000000000000000", b.Pipelines.Get("orphan").Faults[0].Message);
        Assert.DoesNotContain(b.Tables.Tables, t => t.Name == "broken");

        var trades = Trades(0, 50, instruments: 5);
        memory.Log<Trade>("trades").AppendRange(trades);
        var counts = memory.UpsertSink<string, long>("counts");
        await Eventually(() => Same(counts.Rows, Counts.Model(trades)), "the good pipeline's output on the new leader");
    }

    [Fact]
    public async Task A_successor_finishes_a_reset_and_a_removal_its_predecessor_only_recorded()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        var trades = Trades(0, 150);
        memory.Log<Trade>("trades").AppendRange(trades);
        await using (var first = await StartAsync(cluster, "a", memory))
        {
            await first.DeployAsync(Spec("to-reset", "counts", "trades", "reset-out"));
            await first.DeployAsync(Spec("to-remove", "counts", "trades", "remove-out"));
            var counted = memory.UpsertSink<string, long>("reset-out");
            await Eventually(() => Same(counted.Rows, Counts.Model(trades)), "the first leader's output");
            await first.Pipelines.StopAsync("to-reset", new ActionOptions(), default);
            await first.Pipelines.StopAsync("to-remove", new ActionOptions(), default);

            // The leader commits its intent and dies before acting on it.
            using var write = await first.Node.Catalog.BeginAsync(default);
            write.Put("pipeline/to-reset", first.Pipelines.Spec("to-reset") with { Desired = DesiredState.Running, Pending = "reset" });
            write.Put("pipeline/to-remove", first.Pipelines.Spec("to-remove") with { Desired = DesiredState.Removed, Pending = "remove" });
            await write.CommitAsync(default);
        }
        Assert.True(cluster.State.RecordCount > 2);

        await using var second = await StartAsync(cluster, "a", memory);

        // The removal is finished and the pipeline never started.
        Assert.DoesNotContain(second.Pipelines.Pipelines, p => p.Id == "to-remove");
        Assert.Null(second.Node.Catalog.Get("pipeline/to-remove"));
        Assert.Empty(second.Node.Catalog.Get<PipelineSpec>("pipeline/to-reset")!.Pending ?? "");

        // The reset is finished before the pipeline is built: a new incarnation, so its
        // source replays from the start into a sink the connector cleared.
        await second.Running("to-reset");
        Assert.Null(second.Pipelines.Spec("to-reset").Pending);
        var fresh = memory.UpsertSink<string, long>("reset-out");
        await Eventually(() => Same(fresh.Rows, Counts.Model(trades)), "the reset pipeline's replayed output");
        Assert.Equal(150, second.Pipelines.Get("to-reset").Sources.Single().Stats.Records);
    }

    [Fact]
    public async Task The_catalog_directory_restores_what_is_deployed_in_a_new_process()
    {
        var directory = Directory.CreateTempSubdirectory("incremental-service-catalog").FullName;
        var files = Directory.CreateTempSubdirectory("incremental-service-files").FullName;
        var memory = new MemoryConnector();
        var instruments = Instruments(6);
        await File.WriteAllLinesAsync(Path.Combine(files, "instruments.jsonl"), instruments.Select(i => JsonSerializer.Serialize(i, JsonSerializerOptions.Web)));
        var trades = Trades(0, 120, instruments: 6);
        memory.Log<Trade>("trades").AppendRange(trades);
        var output = memory.UpsertSink<long, EnrichedTrade>("enriched");

        Task<TestService> Start() => StartAsync(
            new InMemoryServiceCluster(new InMemoryBackendOptions { CatalogDirectory = directory }), "a", memory,
            configure: b => b.AddObjectStore("refdata", _ => new LocalObjectStore(files)));

        await using (var first = await Start())
        {
            await first.Tables.DeclareAsync(new TableSpec
            {
                Name = "instruments", Definition = typeof(InstrumentsTable).FullName!,
                Loader = new TableLoaderSpec("file", "refdata", Json("""{ "path": "instruments.jsonl", "pollInterval": "00:00:00.05" }""")),
            }, default);
            await Eventually(() => first.Tables.Stats("instruments").Rows == 6, "the table loading");
            await first.DeployAsync(Spec("enrich", "enrich", "trades", "enriched"));
            await Eventually(() => Same(output.Rows, Enrich.Model(trades, instruments)), "the first process's output");
        }
        Assert.True(File.Exists(Path.Combine(directory, "catalog.json")));

        // A new cluster is a new process: its tables and state start empty. What is
        // deployed comes back from the directory, and the data from the sources.
        await using var second = await Start();
        Assert.Equal(["instruments"], second.Tables.Tables.Select(t => t.Name));
        await second.Running("enrich");
        await Eventually(() => second.Tables.Stats("instruments").Rows == 6, "the table loading again");
        var later = Trades(120, 30, instruments: 6);
        memory.Log<Trade>("trades").AppendRange(later);
        await Eventually(() => Same(output.Rows, Enrich.Model(trades.Concat(later), instruments)), "the second process's output");
        Assert.Equal(150, second.Pipelines.Get("enrich").Sources.Single().Stats.Records); // replayed: the state was new
    }

    [Fact]
    public async Task Seeds_are_applied_once_and_the_host_owns_the_nodes_lifetime()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        void Seeds(IncrementalServiceBuilder b) => b
            .AddTable(Acme.Contracts.Tables.Instruments)
            .AddPipeline(Spec("seeded", "counts", "trades", "counts"));

        await using (var first = await StartAsync(cluster, "a", memory, configure: Seeds))
        {
            Assert.Equal(["instruments"], first.Tables.Tables.Select(t => t.Name));
            await first.Running("seeded");
            Assert.Equal(HealthLevel.Healthy, first.Node.Health.Level);
            Assert.True(first.Node.Health is { Live: true, Ready: true });

            // Resolved from the container like any service.
            Assert.Same(first.Node.Pipelines, first.Services.GetRequiredService<IPipelineSupervisor>());
            Assert.Contains(first.Services.GetServices<IHostedService>(), s => ReferenceEquals(s, first.Node));

            // After the first lead the catalog wins: the operator stops the seeded pipeline.
            await first.Pipelines.StopAsync("seeded", new ActionOptions(), default);
        }

        await using var second = await StartAsync(cluster, "a", memory, configure: Seeds);
        Assert.Equal(PipelineState.Stopped, second.Pipelines.Get("seeded").State);
        Assert.Equal(DesiredState.Stopped, second.Pipelines.Spec("seeded").Desired);

        // An empty leader is healthy; events are kept with sequence numbers.
        var events = second.Node.RecentEvents();
        Assert.NotEmpty(events);
        Assert.Equal(events.Select(e => e.Sequence), events.Select(e => e.Sequence).Order());
        Assert.Empty(second.Node.RecentEvents(events[^1].Sequence));
    }

    [Fact]
    public async Task Shutdown_drains_the_circuit_while_its_sink_still_reads()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        var trades = Trades(0, 5000);
        var counts = memory.UpsertSink<string, long>("counts");
        await using var service = await StartAsync(cluster, "a", memory);
        await service.DeployAsync(Spec("counts", "counts", "trades", "counts") with
        {
            Circuit = new PipelineCircuitOptions { MaxStepItemsPerInput = 64 },
        });
        memory.Log<Trade>("trades").AppendRange(trades);
        await Eventually(() => counts.Batches > 0, "the first batch");

        // Stopped mid-flow. The host stops the node; the node drains the circuit while the
        // delivery still reads, and the final commit waits for the sink's acknowledgement.
        await service.Node.StopAsync(default);
        Assert.Equal(PipelineState.Stopped, service.Pipelines.Get("counts").State);
        Assert.False(service.Node.IsLeader);

        // So what the checkpoint says was processed is exactly what the sink holds: not
        // one record delivered and uncommitted, not one committed and undelivered.
        var committed = service.Pipelines.Get("counts").CommittedOffsets["trades#p0"];
        Assert.Equal(committed + 1, counts.Rows.Values.Sum());
        Assert.True(Same(counts.Rows, Counts.Model(trades.Take((int)committed + 1))));
    }
}
