using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Incremental.Kafka;

namespace Incremental.Service;

/// <summary>
/// The service's own instruments, on the <see cref="Meter"/> named
/// <see cref="Name"/> (<c>Incremental.Service</c>): what an operator alerts on, for a
/// collector to take out of the process. The core's meter, <c>Incremental</c>, has the
/// per-circuit rates and durations beside it; a host exports both
/// (<c>AddMeter("Incremental", "Incremental.Service")</c>).
/// <para>
/// Tags are bounded by what is deployed: <c>node</c> (this node's id), <c>pipeline</c> (a
/// pipeline id), <c>table</c> (a declared table's name), and three closed sets —
/// <c>state</c>, <c>role</c>, <c>stage</c>. Nothing is tagged by key, record, offset,
/// exception text or plugin version.
/// </para>
/// <para>
/// The gauges are read from a snapshot each node refreshes once a second (tables every
/// five), so a collection costs a walk over arrays and never touches a circuit, a table
/// or the backend. A node reports what it knows: a standby runs no pipelines, so it
/// reports them as <c>Built</c> (the circuit is ready for a takeover) or <c>Unbuilt</c>,
/// and no lag or checkpoint age for them.
/// </para>
/// </summary>
public static class ServiceMeter
{
    public const string Name = "Incremental.Service";

    private static readonly Meter Meter = new(Name);
    private static readonly ConcurrentDictionary<ServiceNode, byte> Nodes = new();

    internal static readonly Counter<long> PipelineFaults = Meter.CreateCounter<long>(
        "incremental.service.pipeline.faults", "{fault}", "Faults of a pipeline's circuit, a source or a sink delivery.");

    internal static readonly Counter<long> PipelineRestarts = Meter.CreateCounter<long>(
        "incremental.service.pipeline.restarts", "{restart}", "Restarts of a pipeline scheduled after a fault.");

    internal static readonly Counter<long> PipelineRollbacks = Meter.CreateCounter<long>(
        "incremental.service.pipeline.rollbacks", "{rollback}", "Replaces undone because the new generation could not start on the kept state.");

    internal static readonly Counter<long> Takeovers = Meter.CreateCounter<long>(
        "incremental.service.takeovers", "{takeover}", "Times this node became the leader.");

    internal static readonly Histogram<double> TakeoverDuration = Meter.CreateHistogram<double>(
        "incremental.service.takeover.duration", "s",
        "How long a stage of this node's takeover took: catalog, tables, pipelines, start_leading; on Kafka also silence, fence, storage and total.");

    static ServiceMeter()
    {
        Meter.CreateObservableGauge("incremental.service.node.role", () => Each<long>(g => Roles.Select(role =>
                new Measurement<long>(g.Role == role ? 1 : 0, Tag("node", g.Node), Tag("role", role.ToString())))),
            description: "1 for the role this node has, 0 for the others.");
        Meter.CreateObservableGauge("incremental.service.node.term", () => Each<long>(g => [new Measurement<long>(g.Term, Tag("node", g.Node))]),
            description: "The leader term this node last held or saw.");
        Meter.CreateObservableGauge("incremental.service.node.health", () => Each<long>(g => [new Measurement<long>(g.Health, Tag("node", g.Node))]),
            description: "0 healthy, 1 degraded, 2 unhealthy: /health/status as a number.");
        Meter.CreateObservableGauge("incremental.service.node.standby_ready", () => Each<long>(g => g.StandbyReady is { } ready
                ? [new Measurement<long>(ready ? 1 : 0, Tag("node", g.Node))]
                : []),
            description: "On a standby: 1 when a takeover now would be warm. On a leader with a peer configured: 1 when that peer is a ready standby.");
        Meter.CreateObservableGauge("incremental.service.node.state_lag", () => Each<long>(g => g.StateLag is { } lag
                ? [new Measurement<long>(lag, Tag("node", g.Node))]
                : []),
            "{record}", "On a standby: records of the state changelog it has not applied yet.");
        Meter.CreateObservableGauge("incremental.service.catalog.version", () => Each<long>(g => [new Measurement<long>(g.CatalogVersion, Tag("node", g.Node))]),
            description: "The catalog version this node holds.");

        Meter.CreateObservableGauge("incremental.service.pipelines", () => Each<long>(g => States.Select(state =>
                new Measurement<long>(g.PipelinesByState.GetValueOrDefault(state), Tag("node", g.Node), Tag("state", state.ToString())))),
            "{pipeline}", "Pipelines in each state, every state reported.");
        Meter.CreateObservableGauge("incremental.service.pipeline.up", () => Each<long>(g => g.Pipelines.Where(p => p.DesiredRunning && g.Role == NodeRole.Leader).Select(p =>
                new Measurement<long>(p.State == PipelineState.Running ? 1 : 0, Tag("node", g.Node), Tag("pipeline", p.Id)))),
            description: "On the leader, for a pipeline that should be running: 1 when it is.");
        Meter.CreateObservableGauge("incremental.service.pipeline.source_lag", () => Each<long>(g => g.Pipelines.Where(p => p.SourceLag is not null).Select(p =>
                new Measurement<long>(p.SourceLag!.Value, Tag("node", g.Node), Tag("pipeline", p.Id)))),
            "{record}", "Records its sources have not read yet, summed over the sources that report it.");
        Meter.CreateObservableGauge("incremental.service.pipeline.checkpoint_age", () => Each<double>(g => g.Pipelines.Where(p => p.CheckpointAgeSeconds is not null).Select(p =>
                new Measurement<double>(p.CheckpointAgeSeconds!.Value, Tag("node", g.Node), Tag("pipeline", p.Id)))),
            "s", "Time since the running generation last committed a checkpoint.");
        Meter.CreateObservableGauge("incremental.service.pipeline.sink_ack_wait", () => Each<double>(g => g.Pipelines.Where(p => p.State == PipelineState.Running).Select(p =>
                new Measurement<double>(p.SinkAckWaitSeconds, Tag("node", g.Node), Tag("pipeline", p.Id)))),
            "s", "How long the oldest sink write still in flight has been waiting for its acknowledgement; 0 when none is.");
        Meter.CreateObservableCounter("incremental.service.pipeline.dead_letters", () => Each<long>(g => g.Pipelines.Select(p =>
                new Measurement<long>(p.DeadLetters, Tag("node", g.Node), Tag("pipeline", p.Id)))),
            "{record}", "Records dropped to the pipeline's dead letters since this node first ran it.");

        Meter.CreateObservableGauge("incremental.service.tables", () => Each<long>(g => TableStates.Select(state =>
                new Measurement<long>(g.Tables.Count(t => t.State == state), Tag("node", g.Node), Tag("state", state.ToString())))),
            "{table}", "Tables in each state, every state reported.");
        Meter.CreateObservableGauge("incremental.service.table.rows", () => Each<long>(g => g.Tables.Select(t =>
                new Measurement<long>(t.Rows, Tag("node", g.Node), Tag("table", t.Name)))),
            "{row}", "Rows the table holds.");
        Meter.CreateObservableGauge("incremental.service.table.version", () => Each<long>(g => g.Tables.Select(t =>
                new Measurement<long>(t.Version, Tag("node", g.Node), Tag("table", t.Name)))),
            description: "The table's latest version on this node.");
        Meter.CreateObservableGauge("incremental.service.table.lag", () => Each<long>(g => g.Tables.Where(t => t.FollowerLag is not null).Select(t =>
                new Measurement<long>(t.FollowerLag!.Value, Tag("node", g.Node), Tag("table", t.Name)))),
            "{version}", "On a standby: versions of the table's log this node has not applied yet.");
        Meter.CreateObservableGauge("incremental.service.table.loader_lag", () => Each<long>(g => g.Tables.Where(t => t.LoaderLag is not null).Select(t =>
                new Measurement<long>(t.LoaderLag!.Value, Tag("node", g.Node), Tag("table", t.Name)))),
            "{record}", "On the leader: what the table's loader reports it has still to read from its source.");
    }

    private static readonly NodeRole[] Roles = Enum.GetValues<NodeRole>();
    private static readonly PipelineState[] States = Enum.GetValues<PipelineState>();
    private static readonly TableState[] TableStates = Enum.GetValues<TableState>();

    internal static KeyValuePair<string, object?> Tag(string name, string value) => new(name, value);

    private static IEnumerable<Measurement<T>> Each<T>(Func<GaugeSnapshot, IEnumerable<Measurement<T>>> of) where T : struct
    {
        var all = new List<Measurement<T>>();
        foreach (var node in Nodes.Keys)
            if (node.Gauges is { } gauges)
                all.AddRange(of(gauges));
        return all;
    }

    internal static void Register(ServiceNode node) => Nodes[node] = 0;

    internal static void Unregister(ServiceNode node) => Nodes.TryRemove(node, out _);

    internal static void RecordTakeover(string node, string stage, TimeSpan duration) =>
        TakeoverDuration.Record(duration.TotalSeconds, Tag("node", node), Tag("stage", stage));
}

internal readonly record struct PipelineGauge(
    string Id, PipelineState State, bool DesiredRunning, long? SourceLag, double? CheckpointAgeSeconds, double SinkAckWaitSeconds, long DeadLetters);

internal readonly record struct TableGauge(string Name, TableState State, long Rows, long Version, long? FollowerLag, long? LoaderLag);

/// <summary>What one node last knew of itself, for the gauges to read.</summary>
internal sealed record GaugeSnapshot(
    string Node, NodeRole Role, long Term, int Health, bool? StandbyReady, long? StateLag, long CatalogVersion,
    IReadOnlyDictionary<PipelineState, int> PipelinesByState, PipelineGauge[] Pipelines, TableGauge[] Tables);
