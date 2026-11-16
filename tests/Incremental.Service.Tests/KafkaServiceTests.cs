using System.Security.Cryptography;
using System.Text.Json;
using Acme.Contracts;
using Incremental.Kafka;
using Incremental.Storage;
using Incremental.Tables;
using Microsoft.Extensions.DependencyInjection;
using static Incremental.Service.Tests.SupervisorTests;
using static Incremental.Service.Tests.TestService;

namespace Incremental.Service.Tests;

/// <summary>
/// The service on the Kafka backend, two nodes in one process: a leader and a standby on
/// one topic prefix, each with its own <see cref="KafkaLog"/>, so the catalog, the tables,
/// the state and the plugin blobs really do travel through the broker and the fencing is
/// the broker's. Killing a process is <see cref="KafkaServiceProcessTests"/>' job.
/// </summary>
[Collection(KafkaServiceCollection.Name)]
public class KafkaServiceTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Timeout = KafkaTestEnv.Timeout;

    private static JsonElement KafkaBinding(string topic, string extra = "") =>
        Json($$"""{ "connector": "kafka", "connection": "log", "topic": "{{topic}}"{{extra}} }""");

    private static TableSpec InstrumentsFrom(string topic, string name = "instruments") => new()
    {
        Name = name,
        Definition = typeof(InstrumentsTable).FullName!,
        Loader = new TableLoaderSpec("kafka", "log", Json($$"""{ "topic": "{{topic}}" }""")),
    };

    private static Task WarmStandbyAsync(TestService standby) =>
        Eventually(
            () => standby.Node.Status.Standby is { TakeoverWouldBeWarm: true },
            $"the standby to be warm (it says: {string.Join(", ", standby.Node.Status.Standby?.NotWarmBecause ?? ["not a standby"])})");

    private static async Task<long> CatalogVersionAsync(TestService leader, TestService standby)
    {
        var version = leader.Node.Catalog.Version;
        await Eventually(() => standby.Node.Status.Standby is { } s && s.CatalogVersionApplied >= version, "the standby to apply the catalog");
        return version;
    }

    /// <summary>
    /// K5: on a deployment of processes the memory connector's log is written by nothing,
    /// so a starting spec does not bind an input to it — a pipeline deployed from it ran and
    /// read nothing. The input is bound to a topic left to fill in, and the template says so.
    /// The in-memory backend still offers it (SeamTests).
    /// </summary>
    [Fact]
    public async Task On_kafka_a_starting_spec_binds_no_input_to_an_in_process_memory_log()
    {
        await using var env = KafkaTestEnv.Create();
        await using var a = await env.StartNodeAsync("a", lead: true);
        var template = await a.Node.Discovery.TemplateAsync(BuiltIn, "", "counts", "c", default);

        Assert.Equal("kafka", template.Spec.Inputs["trades"].GetProperty("connector").GetString());
        Assert.False(template.Valid);
        Assert.Contains(template.Notes, n => n.Contains("Slot 'trades'") && n.Contains("'kafka'") && n.Contains("left to fill in"));
        Assert.Contains("memory", template.ConnectorsForSlot["trades"]);        // still a connector a host may write
        Assert.Equal("memory", template.Spec.Sinks["out"].GetProperty("connector").GetString());   // a sink is read by whoever asks

        // Filled in, it passes.
        var topic = await env.CreateTopicAsync("trades");
        await a.Pipelines.ValidateAsync(template.Spec with
        {
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = KafkaBinding(topic) },
        }, default);
    }

    [Fact]
    public async Task A_standby_on_kafka_sees_what_the_leader_deploys_and_carries_it_on_after_a_takeover()
    {
        await using var env = KafkaTestEnv.Create();
        var tradesTopic = await env.CreateTopicAsync("trades", partitions: 2);
        var instrumentsTopic = await env.CreateTopicAsync("instruments", compacted: true);
        var enrichedTopic = await env.CreateTopicAsync("enriched", compacted: true);
        var countsTopic = await env.CreateTopicAsync("counts", compacted: true);

        var instruments = Instruments(10);
        var trades = Trades(0, 600);
        foreach (var instrument in instruments)
            env.ProduceJson(instrumentsTopic, instrument.Id, instrument);
        foreach (var trade in trades.Take(300))
            env.ProduceJson(tradesTopic, trade.Id, trade);
        await env.FlushAsync();

        await using var a = await env.StartNodeAsync("a", lead: true);
        await a.Tables.DeclareAsync(InstrumentsFrom(instrumentsTopic), default);
        await Eventually(() => a.Tables.Stats("instruments").Rows == instruments.Count, "the loader to fill the table");
        await a.DeployAsync(Spec("enrich", "enrich") with
        {
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = KafkaBinding(tradesTopic) },
            Sinks = new Dictionary<string, JsonElement> { ["out"] = KafkaBinding(enrichedTopic) },
        });
        await a.DeployAsync(Spec("counts", "counts") with
        {
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = KafkaBinding(tradesTopic) },
            Sinks = new Dictionary<string, JsonElement> { ["out"] = KafkaBinding(countsTopic, """, "producer": "log" """) },
        });
        await KafkaTestEnv.TopicEqualsAsync(enrichedTopic, Enrich.Model(trades.Take(300), instruments), "the first half enriched");
        await KafkaTestEnv.TopicEqualsAsync(countsTopic, Counts.Model(trades.Take(300)), "the first half counted");

        // The standby: everything it knows came through the broker.
        await using var b = await env.StartNodeAsync("b");
        await WarmStandbyAsync(b);
        Assert.False(b.Node.IsLeader);
        Assert.Equal(["counts", "enrich"], b.Pipelines.Pipelines.Select(p => p.Id));
        await Eventually(() => b.Pipelines.Pipelines.All(p => p.State == PipelineState.Built), "the standby to build both circuits");
        var followed = b.Tables.Stats("instruments");
        Assert.True(followed.Following);
        Assert.Equal(instruments.Count, followed.Rows);
        Assert.Equal(a.Tables.Stats("instruments").Epoch, followed.Epoch);
        Assert.Equal(0, followed.LagVersions);

        // The state lag is a figure the node samples once a second, and the leader's
        // last steps are in the sinks before they are committed: read at once it was
        // 322 records on a runner, the commit of what the two sinks already showed.
        await Eventually(() => b.Node.Status.Standby is { StateLagRecords: 0 }, "the standby to have followed the leader's last commit");
        Assert.Equal("a", b.Node.Status.Leadership.LeaderNodeId);
        Assert.NotNull(b.Node.Status.Leadership.Kafka);
        await Assert.ThrowsAsync<ServiceException>(() => b.Tables.DeclareAsync(InstrumentsFrom(instrumentsTopic, "other"), default));

        // The world moves on while leadership changes hands: more trades, an instrument renamed.
        foreach (var trade in trades.Skip(300))
            env.ProduceJson(tradesTopic, trade.Id, trade);
        instruments[2] = instruments[2] with { Symbol = "RENAMED" };
        env.ProduceJson(instrumentsTopic, instruments[2].Id, instruments[2]);
        await env.FlushAsync();

        var plan = await b.Node.TakeOverAsync(new NodeActionOptions(), default).WaitAsync(Timeout);
        Assert.Contains("node 'a' is fenced", plan.Steps[0]);
        Assert.True(b.Node.IsLeader);
        await Eventually(() => a.Node.Status.Leadership.Role == NodeRole.Fenced, "the old leader to find itself fenced");
        Assert.False(a.Node.IsLeader);
        await b.Running("enrich");
        await b.Running("counts");

        await KafkaTestEnv.TopicEqualsAsync(enrichedTopic, Enrich.Model(trades, instruments), "everything enriched after the takeover");
        await KafkaTestEnv.TopicEqualsAsync(countsTopic, Counts.Model(trades), "everything counted after the takeover");

        // "producer": "log" really is the log's transactional producer: every transaction
        // leaves a marker, an offset with no record. The sink's own producer leaves none.
        static long Markers(string topic)
        {
            var records = KafkaTestEnv.ReadTopic(topic);
            return records.Max(r => r.Offset.Value) + 1 - records.Count;
        }
        Assert.True(Markers(countsTopic) > 0, "the counts sink did not write in transactions");
        Assert.Equal(0, Markers(enrichedTopic));

        // Resumed, not replayed: the new leader read each source from the committed offsets.
        var read = b.Pipelines.Get("counts").Sources.Single().Stats.Records;
        Assert.True(read < trades.Count, $"the new leader read {read} of {trades.Count} trades; it should have resumed");

        var timings = b.Node.Status.LastTakeover!;
        var leadership = b.Node.Status.Leadership.Kafka!.LastTakeover!;
        output.WriteLine(
            $"in-process takeover: fence {leadership.Fence.TotalMilliseconds:F0} ms, catalog {timings.Catalog.TotalMilliseconds:F0} ms, " +
            $"{timings.TableCount} table(s) {timings.Tables.TotalMilliseconds:F0} ms, storage {leadership.Storage.TotalMilliseconds:F0} ms, " +
            $"{timings.PipelineCount} pipeline(s) {timings.Pipelines.TotalMilliseconds:F0} ms, total {leadership.Total.TotalMilliseconds:F0} ms");
        Assert.Equal("manual", leadership.Reason);
        Assert.Equal(1, timings.TableCount);
    }

    [Fact]
    public async Task What_the_leader_changed_just_before_a_takeover_is_what_the_new_leader_runs()
    {
        await using var env = KafkaTestEnv.Create();
        var memory = new MemoryConnector();
        var instruments = Instruments(10);
        var trades = Trades(0, 300);
        memory.Log<Trade>("trades").AppendRange(trades);

        await using var a = await env.StartNodeAsync("a", memory, lead: true);
        await a.DeclareInstrumentsAsync(instruments);
        await a.DeployAsync(Spec("kept", "counts", "trades", "kept-out"));
        await a.DeployAsync(Spec("doomed", "counts", "trades", "doomed-out"));
        await a.DeployAsync(Spec("changed", "enrich", "trades", "changed-out", settings: """{ "minQuantity": 0 }"""));
        await a.Running("changed");
        await Eventually(() => memory.UpsertSink<long, EnrichedTrade>("changed-out").Rows.Count == trades.Count, "the first run of 'changed'");

        await using var b = await env.StartNodeAsync("b", memory);
        await WarmStandbyAsync(b);
        Assert.Equal(["changed", "doomed", "kept"], b.Pipelines.Pipelines.Select(p => p.Id));

        // Three changes, and the takeover without waiting for the standby to have seen any
        // of them: the takeover reads the catalog to its end before it acts on it.
        await a.DeployAsync(Spec("late", "counts", "trades", "late-out"));
        await a.Pipelines.RemoveAsync("doomed", new ActionOptions(), default);
        await a.DeployAsync(Spec("changed", "enrich", "trades", "changed-out", settings: """{ "minQuantity": 4 }"""), StateChoice.Reset);
        var leaderVersion = a.Node.Catalog.Version;
        var changedGeneration = a.Pipelines.Spec("changed").Generation;

        await b.Node.TakeOverAsync(new NodeActionOptions(Force: true), default).WaitAsync(Timeout);

        Assert.True(b.Node.Catalog.Version >= leaderVersion);
        Assert.Equal(["changed", "kept", "late"], b.Pipelines.Pipelines.Select(p => p.Id));
        Assert.Throws<ServiceException>(() => b.Pipelines.Get("doomed"));
        Assert.True(b.Pipelines.Spec("changed").Generation >= changedGeneration);
        Assert.Equal(4, b.Pipelines.Spec("changed").Settings!.Value.GetProperty("minQuantity").GetDecimal());
        await b.Running("kept");
        await b.Running("late");
        await b.Running("changed");

        var more = Trades(300, 200);
        memory.Log<Trade>("trades").AppendRange(more);
        trades.AddRange(more);
        var kept = memory.UpsertSink<string, long>("kept-out");
        var late = memory.UpsertSink<string, long>("late-out");
        await Eventually(() => Counts.Model(trades).All(e => kept.Rows.TryGetValue(e.Key, out var n) && n == e.Value), "the kept pipeline to carry on");
        await Eventually(() => Counts.Model(trades).All(e => late.Rows.TryGetValue(e.Key, out var n) && n == e.Value), "the late pipeline to run");

        // The replaced pipeline runs its new settings on new state: the reset recreated its sink.
        var changed = memory.UpsertSink<long, EnrichedTrade>("changed-out");
        var expected = Enrich.Model(trades, instruments, minQuantity: 4);
        await Eventually(
            () => changed.Rows.Count == expected.Count && expected.All(e => changed.Rows.TryGetValue(e.Key, out var row) && row == e.Value),
            "the changed pipeline to run its new settings");
    }

    /// <summary>
    /// A takeover that fences the leader and is then fenced itself, before its term is
    /// committed — what a replaced leader's stale request to the coordinator did in one
    /// broker test, leaving both nodes ended (StaleEpochRequestTests in the Kafka tests
    /// is that request). The standby has promoted nothing: it must still be the standby
    /// it was, with its tables followed and its circuits built, and lead when asked again.
    /// </summary>
    [Fact]
    public async Task A_standby_fenced_before_its_takeover_commits_is_still_a_warm_standby_and_leads_when_asked_again()
    {
        await using var env = KafkaTestEnv.Create();
        var tradesTopic = await env.CreateTopicAsync("trades");
        var countsTopic = await env.CreateTopicAsync("counts", compacted: true);
        var trades = Trades(0, 400);
        foreach (var trade in trades.Take(200))
            env.ProduceJson(tradesTopic, trade.Id, trade);
        await env.FlushAsync();

        await using var a = await env.StartNodeAsync("a", lead: true);
        await a.DeployAsync(Spec("counts", "counts") with
        {
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = KafkaBinding(tradesTopic) },
            Sinks = new Dictionary<string, JsonElement> { ["out"] = KafkaBinding(countsTopic) },
        });
        await KafkaTestEnv.TopicEqualsAsync(countsTopic, Counts.Model(trades.Take(200)), "the first half counted");
        await using var b = await env.StartNodeAsync("b");
        await WarmStandbyAsync(b);
        await Eventually(() => b.Pipelines.Get("counts").State == PipelineState.Built, "the standby to build the circuit");

        // A third process takes the transactional id while b's takeover holds it and has
        // not committed its term, and is gone again.
        var log = ((KafkaServiceBackend)b.Node.Backend).Log;
        var intruded = 0;
        log.WhileAcquiring = point =>
        {
            if (point != "term-read" || Interlocked.Exchange(ref intruded, 1) != 0)
                return;
            var third = KafkaLog.ConnectAsync(env.Log("c")).GetAwaiter().GetResult();
            third.AcquireWriterAsync().WaitAsync(Timeout).GetAwaiter().GetResult();
            third.DisposeAsync().AsTask().GetAwaiter().GetResult();
        };
        var lost = await Assert.ThrowsAsync<ServiceException>(() => b.Node.TakeOverAsync(new NodeActionOptions(), default)).WaitAsync(Timeout);
        Assert.Equal("takeover-lost", lost.Code);
        Assert.Equal(409, lost.Status);

        // b is what it was: a standby, not fenced, its process not asked to end.
        Assert.Equal(NodeRole.Standby, b.Node.Status.Leadership.Role);
        Assert.False(b.Node.Backend.Leadership.WhenFinished.IsCompleted);
        Assert.Equal(1, b.Node.Backend.Describe().Counters["takeoversAbandoned"]);
        Assert.Equal(1, b.Node.Backend.Describe().Counters["writerResets"]);
        await Eventually(() => a.Node.Status.Leadership.Role == NodeRole.Fenced, "the old leader to find itself fenced");

        // Nobody leads. The rest of the input arrives; b is asked again and carries on
        // from the old leader's last checkpoint with the circuit it had built.
        foreach (var trade in trades.Skip(200))
            env.ProduceJson(tradesTopic, trade.Id, trade);
        await env.FlushAsync();
        await WarmStandbyAsync(b);
        Assert.Equal(PipelineState.Built, b.Pipelines.Get("counts").State);
        await b.Node.TakeOverAsync(new NodeActionOptions(), default).WaitAsync(Timeout);
        Assert.True(b.Node.IsLeader);
        Assert.Equal(3, b.Node.Status.Leadership.Term);
        await b.Running("counts");
        await KafkaTestEnv.TopicEqualsAsync(countsTopic, Counts.Model(trades), "everything counted once the standby led");
        var read = b.Pipelines.Get("counts").Sources.Sum(s => s.Stats.Records);
        Assert.InRange(read, 200, 399);   // it resumed; it did not read the topic again
    }

    [Fact]
    public async Task A_deposed_leaders_catalog_write_is_refused_by_the_broker()
    {
        await using var env = KafkaTestEnv.Create();
        var memory = new MemoryConnector();
        await using var a = await env.StartNodeAsync("a", memory, lead: true);
        await a.DeployAsync(Spec("counts", "counts", "trades", "out"));
        await using var b = await env.StartNodeAsync("b", memory);
        await WarmStandbyAsync(b);

        // A sink as the supervisor hands it to a delivery: leased. While A leads, it writes.
        var external = new CountingSink();
        var leased = a.Node.Backend.Leadership.Leased(external);
        await leased.WriteAsync(default, default).AsTask().WaitAsync(Timeout);
        Assert.Equal(1, external.Writes);

        await b.Node.TakeOverAsync(new NodeActionOptions(), default).WaitAsync(Timeout);

        // Straight at the catalog's writer, under the node's own guards: whether or not the
        // old leader has noticed yet, the transaction cannot commit.
        var refused = await Record.ExceptionAsync(async () =>
        {
            using var write = await a.Node.Catalog.BeginAsync(default);
            write.Put("pipeline/zombie", Spec("zombie", "counts", "trades", "zombie-out"));
            await write.CommitAsync(default).WaitAsync(Timeout);
        });
        Assert.NotNull(refused);
        var chain = new List<Exception>();
        for (var ex = refused; ex is not null; ex = ex.InnerException)
            chain.Add(ex);
        Assert.Contains(chain, ex => ex is TableLogFencedException or KafkaLogFencedException);

        // And through the front door it is refused as "not the leader".
        await Eventually(() => a.Node.Status.Leadership.Role == NodeRole.Fenced, "the old leader to find itself fenced");
        var notLeader = await Assert.ThrowsAsync<ServiceException>(() => a.DeployAsync(Spec("zombie", "counts", "trades", "zombie-out")));
        Assert.Equal("not-leader", notLeader.Code);
        Assert.NotNull(a.Node.Status.Leadership.Kafka!.Fault);

        // Fencing does not reach a sink outside Kafka; the lease does. The deposed leader's
        // delivery is refused before the sink is touched.
        await Assert.ThrowsAsync<KafkaLogFencedException>(() => leased.WriteAsync(default, default).AsTask().WaitAsync(Timeout));
        Assert.Equal(1, external.Writes);
        Assert.False(a.Node.Status.Leadership.Kafka!.HoldsLease);
        Assert.True(b.Node.Status.Leadership.Kafka!.HoldsLease);

        // The new leader's catalog never saw it, and still takes writes.
        await b.DeployAsync(Spec("after", "counts", "trades", "after-out"));
        Assert.Null(b.Node.Catalog.Get("pipeline/zombie"));
        Assert.Equal(["after", "counts"], b.Pipelines.Pipelines.Select(p => p.Id));
    }

    private sealed class CountingSink : IUpsertSink<string, long>
    {
        public int Writes;

        public ValueTask WriteAsync(UpsertBatch<string, long> batch, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Writes);
            return default;
        }
    }

    [Fact]
    public async Task The_history_cap_trims_a_kafka_table_log_whatever_a_stopped_reader_holds()
    {
        await using var env = KafkaTestEnv.Create();
        var memory = new MemoryConnector();
        memory.Log<Trade>("trades").AppendRange(Trades(0, 50));
        await using var a = await env.StartNodeAsync("a", memory, lead: true, retainedHistoryVersions: 5);
        var table = await a.DeclareInstrumentsAsync(Instruments(10));
        await a.DeployAsync(Spec("reader", "enrich", "trades", "out"));
        await Eventually(() => memory.UpsertSink<long, EnrichedTrade>("out").Rows.Count == 50, "the reader's output");
        await a.Pipelines.StopAsync("reader", new ActionOptions(), default);
        var held = a.Tables.Stats("instruments").Readers.Single().CommittedVersion;

        // Within the cap the stopped reader holds the floor: nothing it needs is dropped.
        for (var i = 0; i < 3; i++)
            await table.Writer.ReplaceAllAsync(Instruments(10).Select(x => x with { Symbol = $"R{i}-{x.Id}" }));
        await a.Node.TrimAsync();
        var within = a.Tables.Stats("instruments");
        Assert.True(within.OldestRetainedVersion <= held + 1, $"history was trimmed to {within.OldestRetainedVersion} past a reader at {held}");
        Assert.Equal("reader", within.FloorHeldBy);
        Assert.Equal(5 - 3, within.Readers.Single().VersionsUntilCutLoose);

        // Past it the cap wins, and the table says how far the reader is beyond it.
        for (var i = 3; i < 12; i++)
            await table.Writer.ReplaceAllAsync(Instruments(10).Select(x => x with { Symbol = $"R{i}-{x.Id}" }));
        await a.Node.TrimAsync();
        var past = a.Tables.Stats("instruments");
        Assert.Equal(past.Version - 5 + 1, past.OldestRetainedVersion);
        Assert.Null(past.FloorHeldBy);
        Assert.Equal(0, past.Readers.Single().VersionsUntilCutLoose);
    }

    [Fact]
    public async Task Plugin_files_reach_a_standby_and_a_cold_node_through_the_blob_topic()
    {
        await using var env = KafkaTestEnv.Create();
        var memory = new MemoryConnector();
        var instruments = Instruments(10);
        var trades = Trades(0, 200);
        var log = memory.Log<Trade>("trades");
        log.AppendRange(trades);
        var sink = memory.UpsertSink<long, EnrichedTrade>("enriched");
        Dictionary<long, EnrichedTrade> Model() => Enrich.Model(trades, instruments);
        bool SinkIsModel() => sink.Rows.Count == trades.Count && Model().All(e => sink.Rows.TryGetValue(e.Key, out var row) && row == e.Value);

        string version;
        await using (var a = await env.StartNodeAsync("a", memory, lead: true))
        {
            // Only the leader is ever given the plugin's directory.
            var manifest = await a.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines"), default);
            version = manifest.Version;
            await a.DeclareInstrumentsAsync(instruments);
            await a.DeployAsync(new PipelineSpec
            {
                Id = "plugged",
                Plugin = "acme.pipelines",
                Definition = "trade-enrichment",
                Inputs = new Dictionary<string, JsonElement> { ["trades"] = MemoryBinding("trades") },
                Sinks = new Dictionary<string, JsonElement> { ["enriched"] = MemoryBinding("enriched") },
            });
            await Eventually(SinkIsModel, "the plugin's pipeline on the first leader");

            await using (var b = await env.StartNodeAsync("b", memory))
            {
                await WarmStandbyAsync(b);
                Assert.Equal(0, b.Node.Status.Standby!.PluginsPending);

                // Fetched and hash-checked; and loaded, because the standby builds the
                // pipeline's circuit on the state it follows.
                var cache = b.Services.GetRequiredService<Microsoft.Extensions.Options.IOptions<ServiceOptions>>().Value.CacheDirectory!;
                var fetched = Path.Combine(cache, "plugins", "acme.pipelines", version);
                foreach (var file in manifest.Files)
                {
                    var path = Path.Combine(fetched, file.Path);
                    Assert.True(File.Exists(path), $"{file.Path} was not fetched to the standby's cache");
                    Assert.Equal(file.Sha256, Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(path))).ToLowerInvariant());
                }
                await Eventually(() => b.Pipelines.Get("plugged").State == PipelineState.Built, "the standby to build the circuit");
                Assert.Equal("loaded", b.Plugins.Plugins.Single(p => p.Name == "acme.pipelines").LoadState);

                await b.Node.TakeOverAsync(new NodeActionOptions(), default).WaitAsync(Timeout);
                await b.Running("plugged");
                Assert.Equal("loaded", b.Plugins.Plugins.Single(p => p.Name == "acme.pipelines").LoadState);
                var more = Trades(200, 100);
                log.AppendRange(more);
                trades.AddRange(more);
                await Eventually(SinkIsModel, "the plugin's pipeline on the second leader");
            }
        }

        // Both gone. A cold node, with an empty cache and no plugin directory, finds nobody
        // leading, takes over by itself and rebuilds the service from the broker alone.
        var last = Trades(300, 100);
        log.AppendRange(last);
        trades.AddRange(last);
        await using var c = await env.StartNodeAsync("c", memory, automaticTakeover: true);
        await Eventually(() => c.Node.Status.Leadership.Role == NodeRole.Leader, "the cold node to lead");
        Assert.Equal(version, c.Pipelines.Spec("plugged").Version);
        await c.Running("plugged");
        await Eventually(SinkIsModel, "the plugin's pipeline on the cold node");
        Assert.True(c.Pipelines.Get("plugged").Sources.Single().Stats.Records <= last.Count + 50,
            "the cold node replayed its source instead of resuming from the committed offsets");
    }

    [Fact]
    public async Task Step_down_hands_over_at_once_to_a_standby_whose_probe_says_ready()
    {
        await using var env = KafkaTestEnv.Create();
        var memory = new MemoryConnector();
        var ports = KafkaTestEnv.FreePorts(2);
        var trades = Trades(0, 200);
        var log = memory.Log<Trade>("trades");
        log.AppendRange(trades);

        await using var a = await env.StartNodeAsync("a", memory, lead: true, automaticTakeover: true, probePort: ports[0], peerPort: ports[1]);
        await a.DeployAsync(Spec("counts", "counts", "trades", "out"));
        var sink = memory.UpsertSink<string, long>("out");
        await Eventually(() => Counts.Model(trades).All(e => sink.Rows.TryGetValue(e.Key, out var n) && n == e.Value), "the leader's output");

        // Nobody answers on the peer's port yet: that is a degraded leader, and no hand-over.
        await Eventually(() => a.Node.Health.Reasons.Contains("no-ready-standby"), "the leader to miss its standby");
        Assert.Equal(HealthLevel.Degraded, a.Node.Health.Level);
        var refused = await Assert.ThrowsAsync<ServiceException>(() => a.Node.StepDownAsync(new NodeActionOptions(), default));
        Assert.Equal("not-ready", refused.Code);

        await using var b = await env.StartNodeAsync("b", memory, automaticTakeover: true, probePort: ports[1], peerPort: ports[0]);
        await WarmStandbyAsync(b);
        await Eventually(() => a.Node.Status.Peer is { NodeId: "b", Role: NodeRole.Standby, Ready: true }, "the leader to hear a ready standby");
        await Eventually(() => a.Node.Health.Level == HealthLevel.Healthy, "the leader to be healthy");
        await Eventually(() => b.Node.Status.Peer is { NodeId: "a", Role: NodeRole.Leader }, "the standby to hear the leader");

        var clock = System.Diagnostics.Stopwatch.StartNew();
        var plan = await a.Node.StepDownAsync(new NodeActionOptions(), default).WaitAsync(Timeout);
        Assert.Contains("node 'b' takes over", plan.Steps[1]);
        await Eventually(() => b.Node.Status.Leadership.Role == NodeRole.Leader, "the standby to lead after the resignation");
        var handover = clock.Elapsed;
        Assert.Equal(NodeRole.Resigned, a.Node.Status.Leadership.Role);

        // It read the resignation; it did not wait out a silence.
        var takeover = b.Node.Status.Leadership.Kafka!.LastTakeover!;
        Assert.Equal("resigned", takeover.Reason);
        output.WriteLine($"step-down to leading: {handover.TotalMilliseconds:F0} ms (takeover itself {takeover.Total.TotalMilliseconds:F0} ms)");

        var more = Trades(200, 100);
        log.AppendRange(more);
        trades.AddRange(more);
        await b.Running("counts");
        await Eventually(() => Counts.Model(trades).All(e => sink.Rows.TryGetValue(e.Key, out var n) && n == e.Value), "the new leader's output");

        // A clean hand-over leaves nothing to replay.
        Assert.Equal(more.Count, b.Pipelines.Get("counts").Sources.Single().Stats.Records);
    }

    [Fact]
    public async Task The_blob_topic_stores_and_returns_files_whole_across_nodes()
    {
        await using var env = KafkaTestEnv.Create();
        using var first = new KafkaBlobStore(env.Log("a"), "a", Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);
        using var second = new KafkaBlobStore(env.Log("b"), "b", Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance);

        static string Sha(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
        var small = "a small file"u8.ToArray();
        var large = new byte[KafkaBlobStore.ChunkBytes * 3 + 12_345];
        new Random(7).NextBytes(large);
        var exact = new byte[KafkaBlobStore.ChunkBytes];
        new Random(8).NextBytes(exact);
        var empty = Array.Empty<byte>();

        Assert.False(await first.ExistsAsync(Sha(large), default));
        Assert.False(await first.CopyToAsync(Sha(large), new MemoryStream(), default));
        foreach (var bytes in new[] { small, large, exact, empty })
            await first.PutAsync(Sha(bytes), new MemoryStream(bytes), default);
        await first.PutAsync(Sha(large), new MemoryStream(large), default); // idempotent: nothing is written twice
        Assert.Equal(2 + 5 + 2 + 1, KafkaTestEnv.ReadTopic(first.Topic).Count); // chunks and an end record each

        foreach (var bytes in new[] { small, large, exact, empty })
        {
            Assert.True(await second.ExistsAsync(Sha(bytes), default));
            var copy = new MemoryStream();
            Assert.True(await second.CopyToAsync(Sha(bytes), copy, default));
            Assert.Equal(bytes, copy.ToArray());
        }

        // A blob with no end record is a partial upload: absent.
        var partial = "partial"u8.ToArray();
        env.Producer.Produce(first.Topic, new Confluent.Kafka.Message<byte[], byte[]>
        {
            Key = System.Text.Encoding.UTF8.GetBytes(Sha(partial) + "/0"), Value = partial,
        });
        await env.FlushAsync();
        Assert.False(await second.ExistsAsync(Sha(partial), default));

        await second.DeleteAsync(Sha(large), default);
        Assert.False(await first.ExistsAsync(Sha(large), default));
        Assert.False(await first.CopyToAsync(Sha(large), new MemoryStream(), default));
        Assert.True(await first.ExistsAsync(Sha(small), default));
    }

    [Fact]
    public async Task A_dry_run_says_when_an_input_s_topic_or_a_loader_s_topic_does_not_exist_and_creates_neither()
    {
        await using var env = KafkaTestEnv.Create();
        var trades = await env.CreateTopicAsync("trades");
        var instruments = await env.CreateTopicAsync("instruments", compacted: true);
        var missing = env.Prefix + ".no-such-topic";
        await using var a = await env.StartNodeAsync("a", lead: true);

        PipelineSpec Reading(string topic) => Spec("p", "counts") with
        {
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = KafkaBinding(topic) },
            Sinks = new Dictionary<string, JsonElement> { ["out"] = MemoryBinding("out") },
        };
        var dry = await a.Pipelines.DeployAsync(Reading(missing), new DeployOptions(DryRun: true), default);
        var warning = Assert.Single(dry.Plan.Warnings);
        Assert.Contains($"input 'trades': topic {missing} does not exist on connection log", warning);
        Assert.Empty((await a.Pipelines.DeployAsync(Reading(trades), new DeployOptions(DryRun: true), default)).Plan.Warnings);

        var table = await a.Tables.PlanDeclareAsync(InstrumentsFrom(missing), default);
        Assert.Contains($"topic {missing} does not exist on connection log", Assert.Single(table.Warnings));
        Assert.Empty((await a.Tables.PlanDeclareAsync(InstrumentsFrom(instruments), default)).Warnings);

        // Asking did not make it: a broker that creates topics on a metadata request
        // would turn the warning into a topic nobody meant.
        Assert.Single((await a.Pipelines.DeployAsync(Reading(missing), new DeployOptions(DryRun: true), default)).Plan.Warnings);
    }

    [Fact]
    public async Task A_kafka_binding_may_name_a_connection_and_a_topic_and_nothing_of_the_cluster()
    {
        await using var env = KafkaTestEnv.Create();
        var allowed = await env.CreateTopicAsync("tenants.trades");
        await using var a = await env.StartNodeAsync("a", lead: true, configure: builder => builder.AddKafkaConnection("tenants", _ =>
        {
            var connection = new KafkaConnection { BootstrapServers = KafkaTestEnv.Bootstrap };
            connection.AllowedTopicPrefixes.Add(env.Prefix + ".tenants.");
            return connection;
        }));

        async Task<ServiceException> RefusedAsync(string input, string? sink = null)
        {
            var spec = Spec("p", "counts") with
            {
                Inputs = new Dictionary<string, JsonElement> { ["trades"] = Json(input) },
                Sinks = new Dictionary<string, JsonElement> { ["out"] = sink is null ? MemoryBinding("out") : Json(sink) },
            };
            return await Assert.ThrowsAsync<ServiceException>(() => a.Pipelines.ValidateAsync(spec, default));
        }

        var good = $$"""{ "connector": "kafka", "connection": "tenants", "topic": "{{allowed}}" }""";
        await a.Pipelines.ValidateAsync(Spec("p", "counts") with
        {
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Json(good) },
            Sinks = new Dictionary<string, JsonElement> { ["out"] = MemoryBinding("out") },
        }, default);

        Assert.Contains("belong to the named connection", string.Join(" ", (await RefusedAsync(
            $$"""{ "connector": "kafka", "connection": "tenants", "topic": "{{allowed}}", "bootstrapServers": "evil:9092" }""")).Details));
        Assert.Contains("outside what this connection allows", string.Join(" ", (await RefusedAsync(
            $$"""{ "connector": "kafka", "connection": "tenants", "topic": "{{env.Prefix}}.svc.blobs" }""")).Details));
        Assert.Contains("no Kafka connection named 'nowhere'", string.Join(" ", (await RefusedAsync(
            $$"""{ "connector": "kafka", "connection": "nowhere", "topic": "{{allowed}}" }""")).Details));
        Assert.Contains("needs a keyed input", string.Join(" ", (await RefusedAsync(
            $$"""{ "connector": "kafka", "connection": "tenants", "topic": "{{allowed}}", "format": "debezium" }""")).Details));
        Assert.Contains("only that one has the transactional producer", string.Join(" ", (await RefusedAsync(
            good, $$"""{ "connector": "kafka", "connection": "tenants", "topic": "{{allowed}}", "producer": "log" }""")).Details));

        // The service's own topics are nobody's to bind, on any connection.
        foreach (var own in new[] { ".state", ".leader", ".table.svc.catalog.rows", ".svc.blobs" })
            Assert.Contains("belongs to the service's own log", string.Join(" ", (await RefusedAsync(
                $$"""{ "connector": "kafka", "connection": "log", "topic": "{{env.Prefix}}{{own}}" }""")).Details));
        Assert.Contains("belongs to the service's own log", string.Join(" ", (await RefusedAsync(
            good, $$"""{ "connector": "kafka", "connection": "log", "topic": "{{env.Prefix}}.state" }""")).Details));

        // The same rules for a table's loader.
        var loader = await Assert.ThrowsAsync<ServiceException>(() => a.Tables.DeclareAsync(new TableSpec
        {
            Name = "instruments",
            Definition = typeof(InstrumentsTable).FullName!,
            Loader = new TableLoaderSpec("kafka", "tenants", Json($$"""{ "topic": "{{env.Prefix}}.elsewhere" }""")),
        }, default));
        Assert.Contains("outside what this connection allows", loader.Message);
        Assert.Empty(a.Tables.Tables);
    }

    [Fact]
    public async Task Dead_letters_go_to_a_topic_with_their_source_positions_and_keep_arriving_after_a_takeover()
    {
        await using var env = KafkaTestEnv.Create();
        var tradesTopic = await env.CreateTopicAsync("trades");
        var countsTopic = await env.CreateTopicAsync("counts", compacted: true);
        var deadTopic = await env.CreateTopicAsync("dead");

        // One trade in eight has a negative quantity, which the definition throws on; and
        // two records are not trades at all, which the source rejects.
        var trades = Trades(0, 240);
        for (var i = 7; i < trades.Count; i += 8)
            trades[i] = trades[i] with { Quantity = -1 };
        void Produce(IEnumerable<Trade> some)
        {
            foreach (var trade in some)
                env.ProduceJson(tradesTopic, trade.Id, trade);
        }
        Produce(trades.Take(60));
        env.Producer.Produce(tradesTopic, new Confluent.Kafka.Message<byte[], byte[]> { Key = KafkaTestEnv.Json(-1), Value = "not json"u8.ToArray() });
        Produce(trades.Skip(60).Take(60));
        await env.FlushAsync();

        var spec = Spec("fragile", "fragile") with
        {
            Circuit = new PipelineCircuitOptions { ErrorPolicy = RecordErrorPolicy.DeadLetter },
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = KafkaBinding(tradesTopic) },
            Sinks = new Dictionary<string, JsonElement> { ["out"] = KafkaBinding(countsTopic) },
            DeadLetters = KafkaBinding(deadTopic),
        };
        await using var a = await env.StartNodeAsync("a", lead: true);
        var deployed = await a.DeployAsync(spec);

        static List<DeadLetterEnvelope> Read(string topic) =>
            KafkaTestEnv.ReadTopic(topic).Select(r => JsonSerializer.Deserialize<DeadLetterEnvelope>(r.Message.Value, JsonSerializerOptions.Web)!).ToList();
        static long? TradeId(DeadLetterEnvelope e) =>
            e.Source is null ? JsonDocument.Parse(e.Record!).RootElement.GetProperty("id").GetInt64() : null;
        var poisonFirst = trades.Take(120).Where(t => t.Quantity < 0).Select(t => (long?)t.Id).ToList();
        await Eventually(() => Read(deadTopic).Count >= poisonFirst.Count + 1, "the first half's dead letters on the topic");
        await KafkaTestEnv.TopicEqualsAsync(countsTopic, Counts.Model(trades.Take(120).Where(t => t.Quantity >= 0)), "the first half counted");

        var first = Read(deadTopic);
        Assert.Equal(poisonFirst, first.Where(e => e.Source is null).Select(TradeId).Distinct().Order());
        var rejected = Assert.Single(first, e => e.Source is not null);
        Assert.Equal($"{tradesTopic}[0]@60", rejected.Source);
        Assert.Equal("input[trades]", rejected.Operator);
        Assert.Contains("not json", rejected.Record);
        Assert.All(first, e =>
        {
            Assert.Equal("fragile", e.Pipeline);
            Assert.Equal(deployed.Spec.Generation, e.Generation);
            Assert.InRange(e.Step, 1, long.MaxValue);
        });
        Assert.Equal(first.Count, a.Pipelines.Get("fragile").DeadLetterSink!.Delivered);
        Assert.Equal("kafka", a.Pipelines.Get("fragile").DeadLetterSink!.Connector);

        // Leadership moves with more poison on its way.
        await using var b = await env.StartNodeAsync("b");
        await WarmStandbyAsync(b);
        Produce(trades.Skip(120));
        env.Producer.Produce(tradesTopic, new Confluent.Kafka.Message<byte[], byte[]> { Key = KafkaTestEnv.Json(-2), Value = "{ nor this"u8.ToArray() });
        await env.FlushAsync();
        await b.Node.TakeOverAsync(new NodeActionOptions(), default).WaitAsync(Timeout);
        await b.Running("fragile");

        await KafkaTestEnv.TopicEqualsAsync(countsTopic, Counts.Model(trades.Where(t => t.Quantity >= 0)), "everything counted after the takeover");
        var poison = trades.Where(t => t.Quantity < 0).Select(t => (long?)t.Id).ToList();
        await Eventually(() => Read(deadTopic).Count(e => e.Source is not null) >= 2, "the second rejected record on the topic");
        var all = Read(deadTopic);
        // At least once: every dropped record is there; one dropped by a step the old
        // leader had not committed may be there twice.
        Assert.Equal(poison, all.Where(e => e.Source is null).Select(TradeId).Distinct().Order());
        Assert.Equal([$"{tradesTopic}[0]@241", $"{tradesTopic}[0]@60"], all.Where(e => e.Source is not null).Select(e => e.Source).Distinct().Order());

        // How many twice is not a matter of chance: a step's dead letters are with their
        // sink before the step's checkpoint commits, and no step runs while that waits, so
        // what the takeover found published and not committed is one step's — the last one
        // the old leader ran. Every record that is there twice was dropped by that one
        // step; how many records a step holds is the batch's affair (all of the second
        // half when it arrives at once, which is what a slower commit on three brokers
        // makes likely — the bound this test used to have, eight, was one broker's luck).
        var copies = all.GroupBy(e => e.Source ?? "trade " + TradeId(e)).ToList();
        Assert.All(copies, copy => Assert.InRange(copy.Count(), 1, 2));
        var twice = copies.Where(copy => copy.Count() == 2).ToList();
        var steps = twice.Select(copy => copy.First().Step).Distinct().ToList();
        Assert.True(steps.Count <= 1,
            $"{twice.Count} dropped records were delivered twice, by {steps.Count} of the old leader's steps ({string.Join(", ", steps)}); only its last, uncommitted step may be");
        Assert.All(twice, copy => Assert.True(
            copy.First().Source is not null || TradeId(copy.First()) >= 120, $"{copy.Key} was delivered twice and is from before the takeover began"));
    }

    [Fact]
    public async Task A_takeover_in_the_middle_of_open_windows_reopens_none_and_still_closes_them_all()
    {
        await using var env = KafkaTestEnv.Create();
        var ticksTopic = await env.CreateTopicAsync("ticks");
        var liveTopic = await env.CreateTopicAsync("live", compacted: true);
        var finalTopic = await env.CreateTopicAsync("final", compacted: true);
        var aloneTicksTopic = await env.CreateTopicAsync("alone-ticks");
        var aloneLiveTopic = await env.CreateTopicAsync("alone-live", compacted: true);
        var aloneFinalTopic = await env.CreateTopicAsync("alone-final", compacted: true);

        PipelineSpec Windowed(string id, string ticks, string live, string final) => Spec(id, "windowed") with
        {
            Inputs = new Dictionary<string, JsonElement> { ["ticks"] = KafkaBinding(ticks) },
            Sinks = new Dictionary<string, JsonElement> { ["live"] = KafkaBinding(live), ["final"] = KafkaBinding(final) },
        };
        static ChangelogStorageProvider State(TestService node, string id) => node.Node.Context.Storage.Pipeline(id);
        static long Writes(string topic, string key) =>
            KafkaTestEnv.ReadTopic(topic).Count(r => JsonSerializer.Deserialize<string>(r.Message.Key, JsonSerializerOptions.Web) == key);

        // Five traders, a tick every 100 ms of event time for 28 s, in order: nothing is
        // late. Ten-second windows, so each trader has two closed and its third open.
        var id = 0L;
        var ticks = new List<Tick>();
        for (var ms = 0; ms < 28_000; ms += 100)
            ticks.Add(new Tick(++id, $"t{ms / 100 % 5}", ms, 1 + ms % 7));
        // In two parts, because a close is announced by the step after the one that moved
        // the watermark past it: once the first twenty seconds are through, any step that
        // reads the rest announces every trader's first window.
        foreach (var tick in ticks.Where(t => t.AtMs < 20_000))
            env.ProduceJson(ticksTopic, tick.Id, tick);
        await env.FlushAsync();

        await using var a = await env.StartNodeAsync("a", lead: true);
        await a.DeployAsync(Windowed("windowed", ticksTopic, liveTopic, finalTopic));
        await KafkaTestEnv.TopicEqualsAsync(liveTopic, WindowedTotals.Model(ticks.Where(t => t.AtMs < 20_000)), "the first twenty seconds' windows");
        foreach (var tick in ticks.Where(t => t.AtMs >= 20_000))
            env.ProduceJson(ticksTopic, tick.Id, tick);
        await env.FlushAsync();
        await KafkaTestEnv.TopicEqualsAsync(liveTopic, WindowedTotals.Model(ticks), "the live windows before the takeover");
        await Eventually(
            () => Enumerable.Range(0, 5).All(t => KafkaTestEnv.ReadTable<string, WindowTotal>(finalTopic).ContainsKey($"t{t}@0")),
            "the first window of every trader to be final");

        // The checkpoint carries the clock: the watermark, and what the one source has seen.
        var watermark = Tick.Epoch.AddMilliseconds(27_900 - WindowedTotals.LatenessMs).UtcTicks;
        await Eventually(
            () => State(a, "windowed").CommittedOffsets.GetValueOrDefault("ticks#p0", -1) == ticks.Count - 1,
            "the leader to commit everything it read");
        Assert.Equal(watermark, State(a, "windowed").CommittedOffsets["$watermark"]);
        Assert.Equal(Tick.Epoch.AddMilliseconds(27_900).UtcTicks, State(a, "windowed").CommittedOffsets["$watermark#s0"]);

        await using var b = await env.StartNodeAsync("b");
        await WarmStandbyAsync(b);
        await Eventually(() => b.Pipelines.Get("windowed").State == PipelineState.Built, "the standby to build the circuit");
        await Eventually(() => State(b, "windowed").CommittedOffsets.GetValueOrDefault("$watermark") == watermark, "the standby to follow the clock");
        var open = State(a, "windowed").CountEntries();
        var closedWindow = "t0@0";
        var writesBefore = Writes(liveTopic, closedWindow);
        Assert.Equal(new WindowTotal("t0", 0, ticks.Where(t => t.Trader == "t0" && t.AtMs < 10_000).Sum(t => t.Quantity)),
            KafkaTestEnv.ReadTable<string, WindowTotal>(finalTopic)[closedWindow]);

        // Leadership moves with five windows open.
        await b.Node.TakeOverAsync(new NodeActionOptions(), default).WaitAsync(Timeout);
        await b.Running("windowed");

        // The first record the new leader reads is twenty seconds behind the watermark, in
        // a window that closed and was delivered as final. Then the clock moves far ahead
        // on another trader: the five open windows are never touched again.
        var late = new Tick(++id, "t0", 5_000, 1000);
        var ahead = new Tick(++id, "z", 1_000_000, 3);
        env.ProduceJson(ticksTopic, late.Id, late);
        env.ProduceJson(ticksTopic, ahead.Id, ahead);
        await env.FlushAsync();
        await KafkaTestEnv.TopicEqualsAsync(liveTopic, WindowedTotals.Model(ticks.Append(ahead)), "the live windows after the takeover, the late record dropped");

        // Closes are announced by the step after the one that moved the watermark.
        var nudge = new Tick(++id, "z", 1_000_500, 4);
        env.ProduceJson(ticksTopic, nudge.Id, nudge);
        await env.FlushAsync();
        var all = WindowedTotals.Model(ticks.Append(ahead).Append(nudge));
        await KafkaTestEnv.TopicEqualsAsync(liveTopic, all, "every window live");
        await KafkaTestEnv.TopicEqualsAsync(
            finalTopic, all.Where(w => w.Value.Trader != "z").ToDictionary(), "every window final but the one still open");

        // No reopened window: nothing was written for the closed one after the takeover,
        // and its final is the one delivered before it.
        Assert.Equal(writesBefore, Writes(liveTopic, closedWindow));

        // And nothing left behind for the windows that were open at the takeover: the
        // state is that of a pipeline which only ever saw the last two records.
        env.ProduceJson(aloneTicksTopic, ahead.Id, ahead);
        env.ProduceJson(aloneTicksTopic, nudge.Id, nudge);
        await env.FlushAsync();
        await b.DeployAsync(Windowed("alone", aloneTicksTopic, aloneLiveTopic, aloneFinalTopic));
        await KafkaTestEnv.TopicEqualsAsync(aloneLiveTopic, WindowedTotals.Model([ahead, nudge]), "the reference pipeline's one window");
        var alone = 0L;
        var left = 0L;
        await Eventually(
            () =>
            {
                try
                {
                    alone = State(b, "alone").CountEntries();
                    left = State(b, "windowed").CountEntries();
                }
                catch (InvalidOperationException)
                {
                    return false; // a store changed under the count: a step was running
                }
                return alone > 0 && left == alone;
            },
            $"the state to shrink to one open window (it held {open} entries at the takeover)");
        Assert.True(left < open, $"{left} entries now, {open} with five windows open");
        output.WriteLine($"state entries: {open} with five windows open at the takeover, {left} once they closed (a pipeline that saw only the last window: {alone})");
    }

    [Fact]
    public async Task A_takeover_of_the_nodes_own_that_fails_on_kafka_is_told_apart_and_exits_5()
    {
        // The Kafka leadership tells a takeover that failed by the node's own doing from
        // one that another node ended, and the backend must hand that on: without it the
        // node exits 3, which a supervisor answers at once and for ever.
        await using var env = KafkaTestEnv.Create();
        await using var backend = new KafkaServiceBackend(env.Backend("a", automaticTakeover: true));
        await backend.ConnectAsync(default);
        var callbacks = new LeadershipCallbacks(
            _ => Task.CompletedTask, (_, _) => throw new InvalidOperationException("no pipelines today"), _ => Task.CompletedTask,
            () => throw new NotSupportedException());
        await backend.Leadership.StartAsync(callbacks, default);
        await backend.Leadership.WhenFinished.WaitAsync(Timeout);
        var status = backend.Leadership.Status;
        Assert.Equal(NodeRole.Fenced, status.Role);
        Assert.Contains("no pipelines today", status.Kafka!.Fault);
        Assert.True(status.TakeoverFailed);
        Assert.Equal(5, ServiceNode.ExitCodeFor(status));
    }
}
