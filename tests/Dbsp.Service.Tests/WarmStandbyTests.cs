using Acme.Contracts;
using static Dbsp.Service.Tests.SupervisorTests;
using static Dbsp.Service.Tests.TestService;

namespace Dbsp.Service.Tests;

/// <summary>
/// A standby builds each pipeline's circuit on the state it follows, so that a takeover
/// starts circuits instead of building them: what a takeover costs must not grow with
/// what the pipelines hold. And the stores a standby holds must be the leader's, through a
/// change of the code that owns them.
/// </summary>
public class WarmStandbyTests
{
    private static TimeSpan Work(PipelineStartTimings timings) => timings.Build + timings.Start;

    private static bool Same<TKey, TRow>(IReadOnlyDictionary<TKey, TRow> actual, IReadOnlyDictionary<TKey, TRow> expected) where TKey : notnull =>
        actual.Count == expected.Count && expected.All(e => actual.TryGetValue(e.Key, out var row) && Equals(row, e.Value));

    [Fact]
    public async Task A_takeover_starts_what_the_standby_built_and_does_not_pay_for_the_state_again()
    {
        const int small = 300, large = 90_000;
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        await using var a = await StartAsync(cluster, "a", memory);
        await a.DeployAsync(Spec("small", "latest-trades", "small-in", "small-out"));
        await a.DeployAsync(Spec("large", "latest-trades", "large-in", "large-out"));
        var smallIn = memory.KeyedLog<long, Trade>("small-in");
        var largeIn = memory.KeyedLog<long, Trade>("large-in");
        foreach (var trade in Trades(0, small))
            smallIn.Upsert(trade);
        foreach (var trade in Trades(0, large))
            largeIn.Upsert(trade);
        var smallOut = memory.UpsertSink<long, Trade>("small-out");
        var largeOut = memory.UpsertSink<long, Trade>("large-out");
        await Eventually(() => smallOut.Rows.Count == small && largeOut.Rows.Count == large, "the leader to hold both states", TimeSpan.FromSeconds(120));
        await Eventually(
            () => a.Pipelines.Get("large").Sources.Single().Stats.Lag == 0 && a.Pipelines.Get("large").CommittedOffsets.Values.Sum() >= large,
            "the large state to be committed", TimeSpan.FromSeconds(60));

        // The standby arrives with everything already in the log: its build of each
        // pipeline reads the whole state into the circuit's stores, and says how long
        // that took. That is the cost a takeover used to pay.
        await using var b = await StartAsync(cluster, "b", memory);
        await Eventually(
            () => b.Node.Status.Standby is { TakeoverWouldBeWarm: true, PipelinesToBuild: 0 }
                  && b.Pipelines.Pipelines.Count == 2 && b.Pipelines.Pipelines.All(p => p.State == PipelineState.Built),
            "the standby to have built both pipelines", TimeSpan.FromSeconds(60));
        var readSmall = b.Pipelines.Get("small").LastStart!;
        var readLarge = b.Pipelines.Get("large").LastStart!;
        Assert.False(readLarge.Warm);

        // The leader keeps writing; the standby's stores follow.
        foreach (var trade in Trades(large, 500))
            largeIn.Upsert(trade);
        await Eventually(() => largeOut.Rows.Count == large + 500, "the leader to carry on");
        await Eventually(() => b.Node.Status.Standby is { StateLagRecords: 0, TakeoverWouldBeWarm: true }, "the standby to follow");

        await b.Node.TakeOverAsync(new NodeActionOptions(), default).WaitAsync(TestService.Timeout);
        await b.Running("small");
        await b.Running("large");
        var startSmall = b.Pipelines.Get("small").LastStart!;
        var startLarge = b.Pipelines.Get("large").LastStart!;
        Assert.True(startSmall.Warm, "the small pipeline was built again at the takeover");
        Assert.True(startLarge.Warm, "the large pipeline was built again at the takeover");

        lock (typeof(WarmStandbyTests))
            File.AppendAllText(Path.Combine(AppContext.BaseDirectory, "measurements.log"),
                $"{DateTime.UtcNow:O} warm standby, in memory: reading {small} entries {readSmall.Build.TotalMilliseconds:F1} ms, {large} entries " +
                $"{readLarge.Build.TotalMilliseconds:F1} ms; takeover build+start {Work(startSmall).TotalMilliseconds:F1} ms and {Work(startLarge).TotalMilliseconds:F1} ms{Environment.NewLine}");

        // Ratios, not times: 300 times the state adds to the takeover less than a tenth
        // of what reading that state costs. (A takeover that built the circuit from
        // followed bytes would add all of it.)
        Assert.True(Work(startLarge) < Work(startSmall) + readLarge.Build / 10,
            $"the takeover spent {Work(startLarge).TotalMilliseconds:F1} ms building and starting the pipeline with {large} entries and " +
            $"{Work(startSmall).TotalMilliseconds:F1} ms on the one with {small}; reading the large state takes {readLarge.Build.TotalMilliseconds:F1} ms, " +
            "and a takeover must not pay for the state");

        // And it is the same state: an update of an old key and new keys land on it.
        foreach (var trade in (Trade[])[new Trade(5, 1, 99, "changed"), .. Trades(large + 500, 100)])
            largeIn.Upsert(trade);
        await Eventually(() => largeOut.Rows.Count == large + 600 && largeOut.Rows[5].Trader == "changed", "the new leader to carry on from the state");
        Assert.True(b.Pipelines.Get("large").Sources.Single().Stats.Records <= 700, "the new leader read the input again");
    }

    [Fact]
    public async Task A_standby_that_does_not_build_takes_over_from_bytes()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        await using var a = await StartAsync(cluster, "a", memory);
        await a.DeployAsync(Spec("p", "counts", "in", "out"));
        var trades = Trades(0, 200);
        memory.Log<Trade>("in").AppendRange(trades);
        var sink = memory.UpsertSink<string, long>("out");
        await Eventually(() => Same(sink.Rows, Counts.Model(trades)), "the leader's counts");

        await using var b = await StartAsync(cluster, "b", memory, options: o => o.WarmStandbyPipelines = false);
        await Eventually(() => b.Node.Status.Standby is { TakeoverWouldBeWarm: true } && b.Pipelines.Pipelines.Count == 1, "the standby to be warm");
        Assert.Equal(PipelineState.Unbuilt, b.Pipelines.Get("p").State);
        Assert.Null(b.Pipelines.Get("p").LastStart);

        await b.Node.TakeOverAsync(new NodeActionOptions(), default).WaitAsync(TestService.Timeout);
        await b.Running("p");
        Assert.False(b.Pipelines.Get("p").LastStart!.Warm);
        var more = Trades(200, 50);
        memory.Log<Trade>("in").AppendRange(more);
        await Eventually(() => Same(sink.Rows, Counts.Model(trades.Concat(more))), "the counts continuing");
    }

    [Fact]
    public async Task A_pipeline_that_has_never_checkpointed_is_built_by_the_standby_all_the_same()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        await using var a = await StartAsync(cluster, "a", memory);

        // Nothing to read, so nothing to commit: all the standby can go by is the
        // leader's word that the pipeline's state is under this spec's layout.
        await a.DeployAsync(Spec("idle", "counts", "idle-in", "idle-out"));
        await a.Running("idle");
        Assert.Equal(0, a.Pipelines.Get("idle").CommittedStep ?? 0);
        await using var b = await StartAsync(cluster, "b", memory);
        await Eventually(() => b.Pipelines.Pipelines.Any(p => p.Id == "idle" && p.State == PipelineState.Built), "the standby to build the idle pipeline");

        await b.Node.TakeOverAsync(new NodeActionOptions(), default).WaitAsync(TestService.Timeout);
        await b.Running("idle");
        Assert.True(b.Pipelines.Get("idle").LastStart!.Warm);
        var trades = Trades(0, 30);
        memory.Log<Trade>("idle-in").AppendRange(trades);
        var sink = memory.UpsertSink<string, long>("idle-out");
        await Eventually(() => Same(sink.Rows, Counts.Model(trades)), "the pipeline counting on the new leader");
    }

    [Fact]
    public async Task A_table_opened_again_on_the_standby_has_the_circuits_that_read_it_built_again()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        await using var a = await StartAsync(cluster, "a", memory);
        var instruments = Instruments(5);
        var table = await a.DeclareInstrumentsAsync(instruments);
        await a.DeployAsync(Spec("e", "enrich", "in", "out"));
        var trades = Trades(0, 40, instruments: 5);
        memory.Log<Trade>("in").AppendRange(trades);
        var sink = memory.UpsertSink<long, EnrichedTrade>("out");
        await Eventually(() => Same(sink.Rows, Enrich.Model(trades, instruments)), "the leader's output");

        await using var b = await StartAsync(cluster, "b", memory);
        await Eventually(() => b.Pipelines.Pipelines.Any(p => p.Id == "e" && p.State == PipelineState.Built), "the standby to build the circuit");
        var built = b.Pipelines.Get("e").LastStart!.At;

        // The standby's copy of the table is opened from its log again — a new object. A
        // circuit built on the old one would read rows nobody updates any more.
        await b.Tables.ReopenAsync("instruments", default);
        await Eventually(
            () => b.Pipelines.Get("e") is { State: PipelineState.Built, LastStart: { } again } && again.At > built, "the standby to build the circuit again");
        instruments[2] = instruments[2] with { Symbol = "RENAMED", Multiplier = 7 };
        await table.Writer.UpsertAsync(instruments[2]);
        await table.Writer.CommitAsync();
        await Eventually(() => Same(sink.Rows, Enrich.Model(trades, instruments)), "the leader applying the change");
        await Eventually(() => b.Node.Status.Standby is { TakeoverWouldBeWarm: true, StateLagRecords: 0 }, "the standby to follow");

        await b.Node.TakeOverAsync(new NodeActionOptions(), default).WaitAsync(TestService.Timeout);
        await b.Running("e");
        Assert.True(b.Pipelines.Get("e").LastStart!.Warm);
        var more = Trades(40, 20, instruments: 5);
        memory.Log<Trade>("in").AppendRange(more);
        instruments[3] = instruments[3] with { Symbol = "AGAIN", Multiplier = 3 };
        var promoted = b.Tables.Get<int, Instrument>("instruments");
        await promoted.Writer.UpsertAsync(instruments[3]);
        await promoted.Writer.CommitAsync();
        await Eventually(() => Same(sink.Rows, Enrich.Model(trades.Concat(more), instruments)), "the new leader reading the table it follows");
    }

    private const string Plugin = "acme.pipelines";

    private static PipelineSpec TraderCounts(string version) =>
        Spec("tc", "trader-counts", "in", "out", sinkSlot: "counts", plugin: Plugin, version: version);

    private static Dictionary<string, string> CountLines(string version, IEnumerable<Trade> trades) =>
        trades.Select(t => t.Trader + "#" + t.Id).Distinct().GroupBy(x => x[..x.IndexOf('#')])
            .ToDictionary(g => g.Key, g => $"{version}:{g.Key}={g.Count()}");

    [Fact]
    public async Task A_replace_that_keeps_state_reaches_a_standby_holding_the_old_versions_stores()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        await using var a = await StartAsync(cluster, "a", memory);
        var v1 = await a.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines"), default);
        var v2 = await a.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines.V2"), default);
        var trades = Trades(0, 150);
        memory.Log<Trade>("in").AppendRange(trades);
        await a.DeployAsync(TraderCounts(v1.Version));
        var sink = memory.UpsertSink<string, string>("out");
        await Eventually(() => Same(sink.Rows, CountLines("v1", trades)), "version 1 counting");

        // The standby builds version 1's circuit: its stores are of version 1's types.
        await using var b = await StartAsync(cluster, "b", memory);
        await Eventually(() => b.Pipelines.Pipelines.Any(p => p.Id == "tc" && p.State == PipelineState.Built), "the standby to build version 1");
        Assert.Equal(v1.Version, b.Pipelines.Get("tc").Version);
        Assert.Equal("loaded", b.Plugins.Plugins.Single(p => p.Version == v1.Version).LoadState);

        // The leader replaces it with version 2 on the kept state and carries on. The
        // standby gives version 1's stores up at the first record of the new layout —
        // not when it happens to read the catalog — and builds version 2's on the bytes.
        await a.DeployAsync(TraderCounts(v2.Version), StateChoice.Keep);
        await a.Running("tc");
        var more = Trades(150, 60);
        memory.Log<Trade>("in").AppendRange(more);
        await Eventually(() => Same(sink.Rows, CountLines("v2", trades.Concat(more))), "version 2 counting on version 1's state");
        await Eventually(
            () => b.Pipelines.Get("tc") is { State: PipelineState.Built } built && built.Version == v2.Version
                  && b.Node.Status.Standby is { TakeoverWouldBeWarm: true, StateLagRecords: 0 },
            "the standby to build version 2");

        // The takeover starts that circuit, and the counts continue: nothing was lost
        // between two versions' types on the way through the standby. (The leader has
        // committed what it counted: the new leader's forty records below are counted
        // from that checkpoint.)
        await a.Settled();
        await b.Node.TakeOverAsync(new NodeActionOptions(), default).WaitAsync(TestService.Timeout);
        await b.Running("tc");
        Assert.True(b.Pipelines.Get("tc").LastStart!.Warm);
        var last = Trades(210, 40);
        memory.Log<Trade>("in").AppendRange(last);
        await Eventually(() => Same(sink.Rows, CountLines("v2", trades.Concat(more).Concat(last))), "the new leader counting on");
        Assert.Equal(40, b.Pipelines.Get("tc").Sources.Single().Stats.Records);
    }

    [Fact]
    public async Task A_table_opened_again_while_the_standby_builds_on_it_has_the_circuit_built_again()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        await using var a = await StartAsync(cluster, "a", memory);
        var instruments = Instruments(5);
        var table = await a.DeclareInstrumentsAsync(instruments);
        await a.DeployAsync(Spec("e", "enrich", "in", "out"));
        var trades = Trades(0, 40, instruments: 5);
        memory.Log<Trade>("in").AppendRange(trades);
        var sink = memory.UpsertSink<long, EnrichedTrade>("out");
        await Eventually(() => Same(sink.Rows, Enrich.Model(trades, instruments)), "the leader's output");

        // The standby's first build of the circuit has resolved the table when the table
        // is opened from its log again — and that is over before the build is kept. The
        // circuit holds the old instance, which follows nothing any more.
        var standby = new TaskCompletionSource<TestService>(TaskCreationOptions.RunContinuationsAsynchronously);
        var builds = 0;
        await using var b = await StartAsync(cluster, "b", memory, options: o => o.AfterStandbyBuild = async id =>
        {
            if (id != "e" || Interlocked.Increment(ref builds) != 1)
                return;
            await (await standby.Task).Tables.ReopenAsync("instruments", default);
        });
        standby.SetResult(b);
        await Eventually(() => Volatile.Read(ref builds) >= 2 && b.Pipelines.Get("e").State == PipelineState.Built, "the standby to build the circuit again");

        instruments[2] = instruments[2] with { Symbol = "RENAMED", Multiplier = 7 };
        await table.Writer.UpsertAsync(instruments[2]);
        await table.Writer.CommitAsync();
        await Eventually(() => Same(sink.Rows, Enrich.Model(trades, instruments)), "the leader applying the change");
        await Eventually(() => b.Node.Status.Standby is { TakeoverWouldBeWarm: true, StateLagRecords: 0 }, "the standby to follow");

        await b.Node.TakeOverAsync(new NodeActionOptions(), default).WaitAsync(TestService.Timeout);
        await b.Running("e");
        var more = Trades(40, 20, instruments: 5);
        memory.Log<Trade>("in").AppendRange(more);
        instruments[3] = instruments[3] with { Symbol = "AGAIN", Multiplier = 3 };
        var promoted = b.Tables.Get<int, Instrument>("instruments");
        await promoted.Writer.UpsertAsync(instruments[3]);
        await promoted.Writer.CommitAsync();
        await Eventually(() => Same(sink.Rows, Enrich.Model(trades.Concat(more), instruments)), "the new leader reading the table it writes, not one left behind");
    }

    [Fact]
    public async Task A_spec_replaced_while_the_standby_fetches_the_plugin_does_not_reach_that_build()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        await using var a = await StartAsync(cluster, "a", memory);
        var v1 = await a.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines"), default);
        var v2 = await a.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines.V2"), default);
        var trades = Trades(0, 150);
        memory.Log<Trade>("in").AppendRange(trades);
        var first = (await a.DeployAsync(TraderCounts(v1.Version))).Spec;
        var sink = memory.UpsertSink<string, string>("out");
        await Eventually(() => Same(sink.Rows, CountLines("v1", trades)), "version 1 counting");
        var layout1 = PipelineSupervisor.LayoutOf(a.Pipelines.Spec("tc"));

        // The standby has read generation 1's spec and holds version 1's plugin when the
        // leader replaces the pipeline with version 2 and the standby's catalog says so.
        var standby = new TaskCompletionSource<TestService>(TaskCreationOptions.RunContinuationsAsynchronously);
        var binds = 0;
        var builds = 0;
        string? boundAs = null;
        await using var b = await StartAsync(cluster, "b", memory, options: o =>
        {
            o.BeforeStandbyBind = async id =>
            {
                if (id != "tc" || Interlocked.Increment(ref binds) != 1)
                    return;
                var node = await standby.Task;
                await a.DeployAsync(TraderCounts(v2.Version), StateChoice.Keep);
                await Eventually(() => node.Pipelines.Spec("tc").Version == v2.Version, "the standby reading the new spec");
            };
            o.AfterStandbyBuild = async id =>
            {
                if (id != "tc")
                    return;
                // The layout first and the count after it: the test waits on the count
                // and then reads the layout. (The other way round it read null once in
                // some hundred runs on two cores — the hook's thread was taken off the
                // processor between the two lines.) Builds of one pipeline come one at
                // a time, under its queue.
                var layout = (await standby.Task).Node.Context.Storage.Pipeline("tc").Layout;
                if (Volatile.Read(ref builds) == 0)
                    Volatile.Write(ref boundAs, layout);
                Interlocked.Increment(ref builds);
            };
        });
        standby.SetResult(b);

        // What was built under version 1's lease is generation 1's circuit on stores
        // tagged with generation 1's layout — not version 2's spec through version 1's types.
        await Eventually(() => Volatile.Read(ref builds) >= 1, "the standby's first build");
        Assert.Equal(1, first.Generation);
        Assert.Equal(layout1, Volatile.Read(ref boundAs));

        await a.Running("tc");
        var more = Trades(150, 60);
        memory.Log<Trade>("in").AppendRange(more);
        await Eventually(() => Same(sink.Rows, CountLines("v2", trades.Concat(more))), "version 2 counting on version 1's state");
        await Eventually(
            () => b.Pipelines.Get("tc") is { State: PipelineState.Built } built && built.Version == v2.Version
                  && b.Node.Status.Standby is { TakeoverWouldBeWarm: true, StateLagRecords: 0 },
            "the standby to build version 2");
        await b.Node.TakeOverAsync(new NodeActionOptions(), default).WaitAsync(TestService.Timeout);
        await b.Running("tc");
        var last = Trades(210, 40);
        memory.Log<Trade>("in").AppendRange(last);
        await Eventually(() => Same(sink.Rows, CountLines("v2", trades.Concat(more).Concat(last))), "the new leader counting on");
    }

    [Fact]
    public void A_layout_names_everything_that_decides_a_pipelines_stores_and_nothing_else()
    {
        var spec = Spec("p", "enrich", "in", "out", settings: """{ "minQuantity": 2 }""", version: "v1") with
        {
            Tables = new Dictionary<string, string> { ["instruments"] = "instruments-eu" },
        };
        var layout = PipelineSupervisor.LayoutOf(spec);
        Assert.Equal(12, layout.Length);
        Assert.Equal(layout, PipelineSupervisor.LayoutOf(spec with { }));

        // Whose types, which circuit, which stores: each of these is another layout.
        Assert.NotEqual(layout, PipelineSupervisor.LayoutOf(spec with { Version = "v2" }));
        Assert.NotEqual(layout, PipelineSupervisor.LayoutOf(spec with { Plugin = "other" }));
        Assert.NotEqual(layout, PipelineSupervisor.LayoutOf(spec with { Definition = "counts" }));
        Assert.NotEqual(layout, PipelineSupervisor.LayoutOf(spec with { Settings = Json("""{ "minQuantity": 3 }""") }));
        Assert.NotEqual(layout, PipelineSupervisor.LayoutOf(spec with { Circuit = spec.Circuit with { Workers = 2 } }));
        Assert.NotEqual(layout, PipelineSupervisor.LayoutOf(spec with { Circuit = spec.Circuit with { ErrorPolicy = RecordErrorPolicy.DeadLetter } }));
        Assert.NotEqual(layout, PipelineSupervisor.LayoutOf(spec with { Tables = new Dictionary<string, string> { ["instruments"] = "instruments-us" } }));

        // The same settings written another way, and what touches no store, are the same layout.
        Assert.Equal(layout, PipelineSupervisor.LayoutOf(spec with { Settings = Json("""{   "minQuantity":2 }""") }));
        Assert.Equal(layout, PipelineSupervisor.LayoutOf(spec with
        {
            Generation = 9,
            Desired = DesiredState.Stopped,
            Sinks = new Dictionary<string, System.Text.Json.JsonElement> { ["out"] = MemoryBinding("elsewhere") },
            Inputs = new Dictionary<string, System.Text.Json.JsonElement> { ["trades"] = MemoryBinding("another") },
            Circuit = spec.Circuit with { CheckpointInterval = 5 },
        }));
    }

    [Fact]
    public async Task A_standby_is_not_warm_while_a_circuit_it_can_build_is_not_built()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        await using var a = await StartAsync(cluster, "a", memory);
        await a.DeployAsync(Spec("p", "standby-shy", "in", "out"));
        memory.Log<Trade>("in").AppendRange(Trades(0, 10));
        await a.Running("p");

        // The definition's Build does not return on the standby until the gate opens.
        StandbyShy.Gate.Reset();
        StandbyShy.Wait = true;
        try
        {
            await using var b = await StartAsync(cluster, "b", memory);
            await Eventually(
                () => b.Node.Status.Standby is { TakeoverWouldBeWarm: false, PipelinesToBuild: 1 } standby && standby.NotWarmBecause.Contains("pipeline-building"),
                "the standby to say it is still building a pipeline");
            Assert.Equal(PipelineState.Unbuilt, b.Pipelines.Get("p").State);

            // /health/ready says ready (the catalog is open); ?warm=true, the rollout's gate,
            // does not until the takeover would be warm. The leader is ready either way.
            Assert.True(Admin.DbspAdminExtensions.Ready(b.Node, warm: false).Ready);
            Assert.False(Admin.DbspAdminExtensions.Ready(b.Node, warm: true).Ready);
            Assert.True(Admin.DbspAdminExtensions.Ready(a.Node, warm: true).Ready);
            StandbyShy.Gate.Set();
            await Eventually(
                () => b.Node.Status.Standby is { TakeoverWouldBeWarm: true, PipelinesToBuild: 0 } && b.Pipelines.Get("p").State == PipelineState.Built,
                "the standby to be warm once the circuit is built");
            await Eventually(() => Admin.DbspAdminExtensions.Ready(b.Node, warm: true).Ready, "the rollout's gate to open");
        }
        finally
        {
            StandbyShy.Wait = false;
            StandbyShy.Gate.Set();
        }
    }

    [Fact]
    public async Task A_pipeline_removed_on_the_leader_is_let_go_by_the_standby_with_the_plugin_it_held()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        await using var a = await StartAsync(cluster, "a", memory);
        var v1 = await a.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines"), default);
        memory.Log<Trade>("in").AppendRange(Trades(0, 20));
        await a.DeployAsync(TraderCounts(v1.Version));
        await a.Running("tc");

        await using var b = await StartAsync(cluster, "b", memory);
        await Eventually(() => b.Pipelines.Pipelines.Any(p => p.Id == "tc" && p.State == PipelineState.Built), "the standby to build the pipeline");
        Assert.Equal(1, b.Plugins.Plugins.Single(p => p.Version == v1.Version).Users);
        Assert.Equal("loaded", b.Plugins.Plugins.Single(p => p.Version == v1.Version).LoadState);

        // Removed: the standby drops the circuit it built and the plugin version it
        // loaded for it, or that version's code could never unload there.
        await a.Pipelines.RemoveAsync("tc", new ActionOptions(), default).WaitAsync(TestService.Timeout);
        await Eventually(() => b.Pipelines.Pipelines.Count == 0, "the standby to see the removal");
        await Eventually(() => b.Plugins.Plugins.Single(p => p.Version == v1.Version).Users == 0, "the catalog to name no user of the plugin version");

        // "Users" is what the catalog says. Whether this node has let go is whether the
        // version is still loaded here: it is unloaded when its last lease is given back,
        // and the standby's built circuit held one.
        await Eventually(
            () => b.Plugins.Plugins.Single(p => p.Version == v1.Version).LoadState == "stored", "the standby to give its lease back, which unloads the version");
    }

    [Fact]
    public async Task A_pipeline_the_standby_cannot_build_is_shown_and_built_at_the_takeover()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        await using var a = await StartAsync(cluster, "a", memory);
        await a.DeployAsync(Spec("p", "standby-shy", "in", "out"));
        var trades = Trades(0, 80);
        memory.Log<Trade>("in").AppendRange(trades);
        var sink = memory.UpsertSink<string, long>("out");
        await Eventually(() => Same(sink.Rows, Counts.Model(trades)), "the leader's counts");

        StandbyShy.Refuse = true;
        try
        {
            await using var b = await StartAsync(cluster, "b", memory);
            await Eventually(
                () => b.Pipelines.Pipelines.Any(p => p.Id == "p") && b.Pipelines.Get("p").History?.LastOrDefault()?.Why?.Contains("not built on this standby") == true,
                "the standby to report that it could not build the pipeline");
            Assert.Equal(PipelineState.Unbuilt, b.Pipelines.Get("p").State);

            // One pipeline's failure to build ahead is not the standby's: it is still warm.
            await Eventually(() => b.Node.Status.Standby is { TakeoverWouldBeWarm: true, PipelinesToBuild: 0 }, "the standby to be warm all the same");
            Assert.Equal(HealthLevel.Healthy, b.Node.Health.Level);

            StandbyShy.Refuse = false;
            await b.Node.TakeOverAsync(new NodeActionOptions(), default).WaitAsync(TestService.Timeout);
            await b.Running("p");
            Assert.False(b.Pipelines.Get("p").LastStart!.Warm);
            var more = Trades(80, 20);
            memory.Log<Trade>("in").AppendRange(more);
            await Eventually(() => Same(sink.Rows, Counts.Model(trades.Concat(more))), "the counts continuing on the followed state");
        }
        finally
        {
            StandbyShy.Refuse = false;
        }
    }
}

/// <summary><c>counts</c>, with a definition that can be told to throw — a pipeline a
/// standby cannot build — or to wait at a gate. The two tests that use the switches run
/// in this class, one after the other.</summary>
[Pipeline("standby-shy")]
public sealed class StandbyShy : IPipelineDefinition
{
    public static volatile bool Refuse;
    public static volatile bool Wait;
    public static readonly ManualResetEventSlim Gate = new(initialState: true);

    public void Build(PipelineBuilder p)
    {
        if (Refuse)
            throw new InvalidOperationException("this definition will not build here");
        if (Wait)
            Gate.Wait(TimeSpan.FromSeconds(30));
        p.UpsertSink("out", p.Input<Trade>("trades").GroupBy(t => t.Trader).Count().Named("count"));
    }
}
