using System.Diagnostics.Metrics;
using Acme.Contracts;
using Instrument = Acme.Contracts.Instrument;
using Incremental.Kafka;
using static Incremental.Service.Tests.SupervisorTests;
using static Incremental.Service.Tests.TestService;

namespace Incremental.Service.Tests;

/// <summary>The service's own meter, <c>Incremental.Service</c>, as a collector sees it.</summary>
public class MeterTests
{
    /// <summary>Everything the meter reports for the nodes of one test. The meter is one
    /// per process and other tests run beside this one, so measurements are kept by their
    /// <c>node</c> tag.</summary>
    private sealed class Collected : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly HashSet<string> _nodes;
        private readonly List<(string Instrument, double Value, Dictionary<string, string> Tags)> _events = [];
        private readonly List<(string Instrument, double Value, Dictionary<string, string> Tags)> _observed = [];
        private bool _observing;

        public Collected(params string[] nodes)
        {
            _nodes = [.. nodes];
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == ServiceMeter.Name)
                    listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<long>((i, v, tags, _) => Add(i, v, tags));
            _listener.SetMeasurementEventCallback<double>((i, v, tags, _) => Add(i, v, tags));
            _listener.Start();
        }

        private void Add(System.Diagnostics.Metrics.Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var copy = new Dictionary<string, string>();
            foreach (var tag in tags)
                copy[tag.Key] = tag.Value?.ToString() ?? "";
            if (!copy.TryGetValue("node", out var node) || !_nodes.Contains(node))
                return;
            lock (_events)
                (_observing ? _observed : _events).Add((instrument.Name, value, copy));
        }

        /// <summary>Counters and histograms recorded since the listener started.</summary>
        public List<(string Instrument, double Value, Dictionary<string, string> Tags)> Events
        {
            get
            {
                lock (_events)
                    return [.. _events];
            }
        }

        /// <summary>One collection of every observable instrument.</summary>
        public List<(string Instrument, double Value, Dictionary<string, string> Tags)> Observe()
        {
            lock (_events)
            {
                _observed.Clear();
                _observing = true;
            }
            _listener.RecordObservableInstruments();
            lock (_events)
            {
                _observing = false;
                return [.. _observed];
            }
        }

        public void Dispose() => _listener.Dispose();
    }

    private static double? Value(
        List<(string Instrument, double Value, Dictionary<string, string> Tags)> measurements, string instrument, params (string Key, string Value)[] tags)
    {
        var found = measurements.Where(m => m.Instrument == instrument && tags.All(t => m.Tags.GetValueOrDefault(t.Key) == t.Value)).ToList();
        return found.Count switch
        {
            0 => null,
            1 => found[0].Value,
            _ => throw new Xunit.Sdk.XunitException($"{instrument} with those tags was reported {found.Count} times"),
        };
    }

    private static async Task<List<(string Instrument, double Value, Dictionary<string, string> Tags)>> SnapshotAsync(TestService service, Collected collected)
    {
        service.Node.Metrics.Sample(DateTimeOffset.UtcNow);
        await service.Node.RefreshTableGaugesAsync();
        service.Node.RefreshNodeGauges();
        return collected.Observe();
    }

    private static string NewNode(string prefix) => prefix + Guid.NewGuid().ToString("N")[..8];

    [Fact]
    public async Task Leader_reports_its_pipelines_tables_role_and_takeover_with_bounded_tags()
    {
        var node = NewNode("ma");
        using var collected = new Collected(node);
        await using var service = await StartAsync(nodeId: node);
        var instruments = Enumerable.Range(1, 10).Select(i => new Instrument(i, "S" + i, "I" + i, 1)).ToList();
        await service.DeclareInstrumentsAsync(instruments);

        // One pipeline that runs, one that fails for good, one that drops records.
        service.Memory.Log<Trade>("ok-in").AppendRange(Trades(0, 100));
        await service.DeployAsync(Spec("ok", "counts", "ok-in", "ok-out"));
        service.Memory.Log<Trade>("bad-in").Append(new Trade(1, 1, -1, "t"));
        await service.DeployAsync(Spec("bad", "fragile", "bad-in", "bad-out"));
        service.Memory.Log<Trade>("lenient-in").AppendRange([new Trade(1, 1, -1, "t"), new Trade(2, 1, 5, "t"), new Trade(3, 1, -2, "t")]);
        await service.DeployAsync(Spec("lenient", "fragile", "lenient-in", "lenient-out") with
        {
            Circuit = new PipelineCircuitOptions { ErrorPolicy = RecordErrorPolicy.DeadLetter },
        });
        var okOut = service.Memory.UpsertSink<string, long>("ok-out");
        await Eventually(() => okOut.Rows.Values.Sum() == 100, "the running pipeline's output");
        await Eventually(() => service.Pipelines.Get("bad").State == PipelineState.Failed, "the faulting pipeline failing");
        await Eventually(() => service.Pipelines.Get("lenient").DeadLetters == 2, "the lenient pipeline's dead letters");

        var seen = await SnapshotAsync(service, collected);
        (string, string) n = ("node", node);

        // The node.
        Assert.Equal(1, Value(seen, "incremental.service.node.role", n, ("role", "Leader")));
        Assert.Equal(0, Value(seen, "incremental.service.node.role", n, ("role", "Standby")));
        Assert.Equal(Enum.GetValues<NodeRole>().Length, seen.Count(m => m.Instrument == "incremental.service.node.role"));
        Assert.Equal(service.Node.Status.Leadership.Term, Value(seen, "incremental.service.node.term", n));
        Assert.Equal(service.Node.Status.CatalogVersion, Value(seen, "incremental.service.catalog.version", n));
        Assert.Equal((double)HealthLevel.Degraded, Value(seen, "incremental.service.node.health", n));     // a pipeline has failed
        Assert.Null(Value(seen, "incremental.service.node.standby_ready", n));                               // a lone node expects no peer
        Assert.Null(Value(seen, "incremental.service.node.state_lag", n));

        // Pipelines: every state is reported, so a series does not vanish when it is 0.
        Assert.Equal(2, Value(seen, "incremental.service.pipelines", n, ("state", "Running")));
        Assert.Equal(1, Value(seen, "incremental.service.pipelines", n, ("state", "Failed")));
        Assert.Equal(0, Value(seen, "incremental.service.pipelines", n, ("state", "Backoff")));
        Assert.Equal(Enum.GetValues<PipelineState>().Length, seen.Count(m => m.Instrument == "incremental.service.pipelines"));
        Assert.Equal(1, Value(seen, "incremental.service.pipeline.up", n, ("pipeline", "ok")));
        Assert.Equal(0, Value(seen, "incremental.service.pipeline.up", n, ("pipeline", "bad")));
        Assert.Equal(0, Value(seen, "incremental.service.pipeline.source_lag", n, ("pipeline", "ok")));
        Assert.InRange(Value(seen, "incremental.service.pipeline.checkpoint_age", n, ("pipeline", "ok"))!.Value, 0, 30);
        Assert.Null(Value(seen, "incremental.service.pipeline.checkpoint_age", n, ("pipeline", "bad")));     // nothing runs
        Assert.Equal(0, Value(seen, "incremental.service.pipeline.sink_ack_wait", n, ("pipeline", "ok")));
        Assert.Equal(2, Value(seen, "incremental.service.pipeline.dead_letters", n, ("pipeline", "lenient")));
        Assert.Equal(0, Value(seen, "incremental.service.pipeline.dead_letters", n, ("pipeline", "ok")));

        // Tables.
        Assert.Equal(1, Value(seen, "incremental.service.tables", n, ("state", "Open")));
        Assert.Equal(0, Value(seen, "incremental.service.tables", n, ("state", "Faulted")));
        Assert.Equal(10, Value(seen, "incremental.service.table.rows", n, ("table", "instruments")));
        Assert.Equal(service.Tables.Stats("instruments").Version, Value(seen, "incremental.service.table.version", n, ("table", "instruments")));
        Assert.Null(Value(seen, "incremental.service.table.lag", n, ("table", "instruments")));               // a leader follows nothing

        // What it holds: a table by part, a pipeline's state, and the process around them.
        var held = service.Tables.Stats("instruments").Memory!;
        Assert.Equal(held.RowBytes, Value(seen, "incremental.service.table.memory", n, ("table", "instruments"), ("part", "rows")));
        Assert.Equal(held.IndexBytes, Value(seen, "incremental.service.table.memory", n, ("table", "instruments"), ("part", "indexes")));
        Assert.Equal(held.HistoryBytes, Value(seen, "incremental.service.table.memory", n, ("table", "instruments"), ("part", "history")));
        Assert.True(held.RowBytes > 10 * 100, $"ten rows estimated at {held.RowBytes} bytes");
        Assert.Equal(3, seen.Count(m => m.Instrument == "incremental.service.table.memory"));
        Assert.Equal(5, Value(seen, "incremental.service.pipeline.state_entries", n, ("pipeline", "ok")));  // one count per trader
        Assert.InRange(Value(seen, "incremental.service.pipeline.state_memory", n, ("pipeline", "ok"))!.Value, 5 * 30, 5 * 2000);
        Assert.Equal(0, Value(seen, "incremental.service.pipeline.state_entries", n, ("pipeline", "bad")));
        var working = Value(seen, "incremental.service.process.working_set", n)!.Value;
        var live = Value(seen, "incremental.service.process.gc.live", n)!.Value;
        var committed = Value(seen, "incremental.service.process.gc.committed", n)!.Value;
        Assert.True(working > live && committed >= live && live > 0, $"working set {working}, committed {committed}, live {live}");
        if (OperatingSystem.IsLinux())
            Assert.InRange(Value(seen, "incremental.service.process.other", n)!.Value, 0, working);

        // What happened: the takeover and its stages, the faults and the restarts between them.
        var events = collected.Events;
        Assert.Equal(1, Value(events, "incremental.service.takeovers", n));
        foreach (var stage in new[] { "catalog", "tables", "pipelines", "start_leading" })
            Assert.InRange(Value(events, "incremental.service.takeover.duration", n, ("stage", stage))!.Value, 0, 30);
        var policy = FastRestart;
        Assert.Equal(policy.MaxRestarts + 1, events.Count(e => e.Instrument == "incremental.service.pipeline.faults" && e.Tags["pipeline"] == "bad"));
        Assert.Equal(policy.MaxRestarts, events.Count(e => e.Instrument == "incremental.service.pipeline.restarts" && e.Tags["pipeline"] == "bad"));
        Assert.DoesNotContain(events, e => e.Instrument.StartsWith("incremental.service.pipeline.") && e.Tags["pipeline"] == "ok");

        // Tags: the node, a pipeline or table name, or one of four closed sets. Nothing else.
        string[] allowed = ["node", "pipeline", "table", "state", "role", "stage", "part"];
        Assert.Equal(["history", "indexes", "rows"], seen.Where(m => m.Tags.ContainsKey("part")).Select(m => m.Tags["part"]).Distinct().Order());
        Assert.All(seen.Concat(events), m => Assert.Subset(allowed.ToHashSet(), m.Tags.Keys.ToHashSet()));
        Assert.All(seen.Concat(events), m => Assert.StartsWith("incremental.service.", m.Instrument));
    }

    [Fact]
    public async Task Standby_reports_readiness_and_lag_and_a_takeover_moves_the_role()
    {
        var (idA, idB) = (NewNode("mb"), NewNode("mc"));
        using var collected = new Collected(idA, idB);
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        await using var a = await StartAsync(cluster, idA, memory);
        await a.DeclareInstrumentsAsync(Enumerable.Range(1, 5).Select(i => new Instrument(i, "S" + i, "I" + i, 1)));
        memory.Log<Trade>("in").AppendRange(Trades(0, 50));
        await a.DeployAsync(Spec("p", "counts", "in", "out"));
        await a.Running("p");
        await using var b = await StartAsync(cluster, idB, memory);
        await Eventually(() => b.Node.Status.Standby is { TakeoverWouldBeWarm: true } && b.Pipelines.Pipelines.Count == 1
            && b.Tables.Tables.Any(t => t.State == TableState.Open), "the standby being warm");
        await Eventually(() => a.Node.Status.Peer is { Ready: true }, "the leader seeing its standby");
        await Eventually(() => b.Pipelines.Get("p").State == PipelineState.Built, "the standby building the circuit");

        var onB = await SnapshotAsync(b, collected);
        (string, string) nb = ("node", idB);
        Assert.Equal(1, Value(onB, "incremental.service.node.role", nb, ("role", "Standby")));
        Assert.Equal(1, Value(onB, "incremental.service.node.standby_ready", nb));
        Assert.Equal(0, Value(onB, "incremental.service.node.state_lag", nb));
        Assert.Equal(0, Value(onB, "incremental.service.table.lag", nb, ("table", "instruments")));
        Assert.Equal(5, Value(onB, "incremental.service.table.rows", nb, ("table", "instruments")));
        // A standby holds what the leader holds: the rows, and the state it follows.
        Assert.True(Value(onB, "incremental.service.table.memory", nb, ("table", "instruments"), ("part", "rows")) > 5 * 100);
        Assert.Equal(5, Value(onB, "incremental.service.pipeline.state_entries", nb, ("pipeline", "p")));
        Assert.True(Value(onB, "incremental.service.pipeline.state_memory", nb, ("pipeline", "p")) > 0);
        Assert.Equal(1, Value(onB, "incremental.service.pipelines", nb, ("state", "Built")));
        // A standby runs nothing: it says nothing of whether a pipeline is up.
        Assert.Null(Value(onB, "incremental.service.pipeline.up", nb, ("pipeline", "p")));
        Assert.Null(Value(onB, "incremental.service.pipeline.checkpoint_age", nb, ("pipeline", "p")));

        // The leader reports its standby too: the alert for "no standby" is on the leader.
        var onA = await SnapshotAsync(a, collected);
        Assert.Equal(1, Value(onA, "incremental.service.node.standby_ready", ("node", idA)));
        Assert.Equal(1, Value(onA, "incremental.service.pipeline.up", ("node", idA), ("pipeline", "p")));

        await b.Node.TakeOverAsync(new NodeActionOptions(), default);
        await b.Running("p");
        await Eventually(() => !a.Node.IsLeader, "the old leader knowing it is fenced");
        onB = await SnapshotAsync(b, collected);
        Assert.Equal(1, Value(onB, "incremental.service.node.role", nb, ("role", "Leader")));
        Assert.Equal(1, Value(onB, "incremental.service.pipeline.up", nb, ("pipeline", "p")));
        Assert.Equal(1, collected.Events.Count(e => e.Instrument == "incremental.service.takeovers" && e.Tags["node"] == idB));
        onA = await SnapshotAsync(a, collected);
        Assert.Equal(1, Value(onA, "incremental.service.node.role", ("node", idA), ("role", "Fenced")));
        Assert.Equal((double)HealthLevel.Unhealthy, Value(onA, "incremental.service.node.health", ("node", idA)));
        Assert.Null(Value(onA, "incremental.service.pipeline.up", ("node", idA), ("pipeline", "p")));
    }

    [Fact]
    public async Task Rolled_back_replace_is_counted_and_a_disposed_node_reports_nothing()
    {
        var node = NewNode("md");
        using var collected = new Collected(node);
        await using (var service = await StartAsync(nodeId: node))
        {
            service.Memory.Log<Trade>("in").AppendRange(Trades(0, 20));
            await service.DeployAsync(Spec("p", "counts", "in", "out"));
            await Eventually(() => service.Memory.UpsertSink<string, long>("out").Rows.Values.Sum() == 20, "the first output");
            await Assert.ThrowsAsync<ServiceException>(() => service.DeployAsync(Spec("p", "counts-by-instrument", "in", "out-2"), StateChoice.Keep));
            Assert.Equal(1, collected.Events.Count(e => e.Instrument == "incremental.service.pipeline.rollbacks" && e.Tags["pipeline"] == "p"));
            Assert.NotEmpty(await SnapshotAsync(service, collected));
        }
        Assert.Empty(collected.Observe());
    }
}
