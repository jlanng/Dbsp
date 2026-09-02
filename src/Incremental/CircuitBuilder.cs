using Incremental.Operators;
using Incremental.Runtime;

namespace Incremental;

internal sealed class NodeDefinition
{
    public required int Id { get; init; }
    public required string Kind { get; init; }
    public required int[] Inputs { get; init; }
    public bool IsExchange { get; init; }
    public required Func<OperatorContext, IStepOperator> Factory { get; init; }

    /// <summary>Explicit state identity from <see cref="ZStream{T}.Named"/>, or null for positional.</summary>
    public string? StateId { get; set; }

    /// <summary>Root of the node's state store names. Positional (declaration order) by
    /// default, so any upstream topology change renumbers it; an explicit name pins it, so
    /// the node's checkpointed state survives circuit evolution.</summary>
    public string Name => StateId is null ? $"n{Id}.{Kind}" : $"named.{StateId}.{Kind}";
}

/// <summary>
/// A contiguous range of nodes the scheduler iterates to a fixed point within each outer
/// step (a <c>Recurse</c> subcircuit): [<see cref="Start"/> = feedback node … body …
/// stabilizing distinct = <see cref="ResultNode"/> … <see cref="End"/> = exit node]. Node
/// ids are assigned in creation order, so the range is contiguous and id order remains the
/// topological order within an iteration; the only back-edge — result to feedback — is
/// handed across iterations by the scheduler, never read through a slot.
/// </summary>
internal sealed class LoopRegion
{
    public required int Index { get; init; }
    public required int Start { get; init; }
    public required int End { get; init; }
    public required int ResultNode { get; init; }
    public required int MaxIterations { get; init; }
}

/// <summary>
/// Declares the dataflow graph of a circuit: inputs, operators, and outputs. Streams are
/// handles into the graph under construction; <see cref="Build"/> freezes it into a runnable
/// <see cref="Circuit"/>.
/// </summary>
public sealed class CircuitBuilder
{
    private readonly List<NodeDefinition> _nodes = [];
    private readonly List<InputRegistration> _inputs = [];
    private readonly List<OutputRegistration> _outputs = [];
    private readonly List<LoopRegion> _loopRegions = [];
    private RecursionScope? _scope;
    private bool _built;

    public CircuitBuilder(CircuitOptions? options = null)
    {
        Options = options ?? new CircuitOptions();
        ArgumentOutOfRangeException.ThrowIfLessThan(Options.Workers, 1, nameof(Options.Workers));
        ArgumentOutOfRangeException.ThrowIfLessThan(Options.CheckpointInterval, 1, nameof(Options.CheckpointInterval));
        ArgumentOutOfRangeException.ThrowIfLessThan(Options.InputChannelCapacity, 1, nameof(Options.InputChannelCapacity));
        ArgumentOutOfRangeException.ThrowIfLessThan(Options.OutputChannelCapacity, 1, nameof(Options.OutputChannelCapacity));
        ArgumentOutOfRangeException.ThrowIfLessThan(Options.MaxStepItemsPerInput, 1, nameof(Options.MaxStepItemsPerInput));
        ArgumentOutOfRangeException.ThrowIfLessThan(Options.StreamChunkRows, 1, nameof(Options.StreamChunkRows));
        if (Options.FragmentStreaming && !Options.PointToPointExchange)
            throw new NotSupportedException(
                "FragmentStreaming builds on the point-to-point exchange: set PointToPointExchange = true " +
                "(both are local-only — the cluster shuffle keeps its barrier).");
        if (Options.Cluster is { } cluster)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(cluster.ProcessCount, 1, nameof(Options.Cluster));
            ArgumentOutOfRangeException.ThrowIfNegative(cluster.ProcessIndex, nameof(Options.Cluster));
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(cluster.ProcessIndex, cluster.ProcessCount, nameof(Options.Cluster));
            if (Options.AutoStep)
                throw new NotSupportedException(
                    "Clustered circuits require AutoStep = false: processes run their steps in lockstep, so step " +
                    "initiation is explicit — call StepAsync on every process for every step.");
            if (Options.PointToPointExchange)
                throw new NotSupportedException(
                    "PointToPointExchange applies to local workers only; the cluster shuffle is barrier-based. " +
                    "Unset one of the two.");
        }
    }

    public CircuitOptions Options { get; }

    /// <summary>The circuit's shared event-time clock; created by the first <c>WithEventTime</c>.</summary>
    internal WatermarkClock? WatermarkClock { get; private set; }

    internal WatermarkClock GetOrCreateWatermarkClock() => WatermarkClock ??= new WatermarkClock(Options.Workers);

    /// <summary>Declares a named input. Redeem the returned definition after
    /// <see cref="Build"/> via <see cref="Circuit.Handle{T}(InputStream{T})"/>. The name is
    /// the durable identity of the external feed — checkpointed source offsets are keyed by
    /// it, so it must stay stable across restarts and topology edits.</summary>
    public InputStream<T> Input<T>(string name) where T : notnull
    {
        ThrowIfBuilt();
        RequireOutsideRecursion("Input");
        if (_inputs.Any(i => i.Name == name))
            throw new ArgumentException($"An input named '{name}' already exists.", nameof(name));

        var nodeId = AddNode($"input[{name}]", [], _ => new InputOperator<T>());
        _inputs.Add(new InputRegistration<T> { Name = name, NodeId = nodeId });
        return new InputStream<T>(this, nodeId, name);
    }

    /// <summary>Declares a named input with upsert/delete-by-key semantics: redeem it after
    /// <see cref="Build"/> via <see cref="Circuit.Handle{TKey,TValue}(KeyedInputStream{TKey,TValue})"/>
    /// for a handle whose <c>UpsertAsync</c> retracts the previous row for the key
    /// automatically. Caveat (design in progress): the upsert map lives on the producer side
    /// and is not checkpointed, and keyed inputs carry no source offsets — treat a keyed
    /// input as a full-replay source after a restart.</summary>
    public KeyedInputStream<TKey, TValue> KeyedInput<TKey, TValue>(string name, Func<TValue, TKey> keySelector)
        where TKey : notnull
        where TValue : notnull
    {
        ThrowIfBuilt();
        RequireOutsideRecursion("KeyedInput");
        if (_inputs.Any(i => i.Name == name))
            throw new ArgumentException($"An input named '{name}' already exists.", nameof(name));

        var nodeId = AddNode($"input[{name}]", [], _ => new InputOperator<TValue>());
        _inputs.Add(new KeyedInputRegistration<TKey, TValue> { Name = name, NodeId = nodeId, KeySelector = keySelector });
        return new KeyedInputStream<TKey, TValue>(this, nodeId, name);
    }

    internal int AddNode(string kind, int[] inputs, Func<OperatorContext, IStepOperator> factory, bool isExchange = false)
    {
        ThrowIfBuilt();
        foreach (var input in inputs)
            RequireVisible(input);

        // Inside a Recurse body, a reference to a stream defined outside the loop goes
        // through an import node (DBSP's δ0): the outer delta enters the iteration domain
        // at iteration 0 only. One import per outer node per loop.
        if (_scope is { } scope)
            for (var i = 0; i < inputs.Length; i++)
                if (inputs[i] < scope.Start)
                    inputs[i] = scope.ImportOf(inputs[i], this);

        return AddNodeCore(kind, inputs, factory, isExchange);
    }

    private int AddNodeCore(string kind, int[] inputs, Func<OperatorContext, IStepOperator> factory, bool isExchange)
    {
        var id = _nodes.Count;
        _nodes.Add(new NodeDefinition
        {
            Id = id,
            Kind = kind,
            Inputs = inputs,
            IsExchange = isExchange,
            Factory = factory,
        });
        return id;
    }

    internal void SetStateId(int nodeId, string name)
    {
        ThrowIfBuilt();
        ArgumentException.ThrowIfNullOrEmpty(name);
        if (!name.All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_'))
            throw new ArgumentException(
                $"Operator name '{name}' is invalid: use ASCII letters, digits, '-' and '_' — the name becomes part of state store names.",
                nameof(name));
        var node = _nodes[nodeId];
        if (node.StateId is not null)
            throw new InvalidOperationException($"This operator is already named '{node.StateId}'.");
        node.StateId = name;
    }

    /// <summary>Registers an output. Outputs are unnamed in the API — the definition
    /// reference is the identity — so the registration name here is auto-assigned, used only
    /// for diagnostics and handle lookup.</summary>
    internal string AddOutput<T>(int nodeId, bool snapshot) where T : notnull
    {
        ThrowIfBuilt();
        RequireOutsideRecursion("ToOutput");
        RequireVisible(nodeId);
        var name = $"out{_outputs.Count}";
        _outputs.Add(new OutputRegistration<T> { Name = name, NodeId = nodeId, Snapshot = snapshot });
        return name;
    }

    /// <summary>Streams defined inside a Recurse body carry per-iteration deltas that are
    /// meaningless outside the loop; only the stream Recurse returns is visible downstream.</summary>
    private void RequireVisible(int nodeId)
    {
        if (_loopRegions.Any(r => nodeId >= r.Start && nodeId < r.End))
            throw new InvalidOperationException(
                "A stream defined inside a Recurse body cannot be used outside its loop; use the stream Recurse returns.");
    }

    internal bool InRecursionScope => _scope is not null;

    internal void RequireOutsideRecursion(string operation)
    {
        if (_scope is not null)
            throw new NotSupportedException(
                $"{operation} is not supported inside a Recurse body. A loop body may use Select, Where, SelectMany, " +
                "Concat, Minus, Negate, GroupBy, MapValues, Values, Join, and Distinct.");
    }

    internal RecursionScope OpenRecursionScope()
    {
        ThrowIfBuilt();
        if (_scope is not null)
            throw new NotSupportedException("Nested Recurse is not supported.");
        return _scope = new RecursionScope(_nodes.Count);
    }

    internal void CloseRecursionScope(RecursionScope scope, int resultNode, int exitNode, int maxIterations)
    {
        _scope = null;
        _loopRegions.Add(new LoopRegion
        {
            Index = _loopRegions.Count,
            Start = scope.Start,
            End = exitNode,
            ResultNode = resultNode,
            MaxIterations = maxIterations,
        });
    }

    internal sealed class RecursionScope(int start)
    {
        private readonly Dictionary<int, int> _imports = [];

        /// <summary>First node id of the loop region (the feedback node).</summary>
        public int Start { get; } = start;

        public int ImportOf(int outerNode, CircuitBuilder builder) =>
            _imports.TryGetValue(outerNode, out var import)
                ? import
                : _imports[outerNode] = builder.AddNodeCore("loopImport", [outerNode], _ => new LoopImportOperator(), isExchange: false);
    }

    public Circuit Build()
    {
        ThrowIfBuilt();
        if (Options.Cluster is not null && _loopRegions.Count > 0)
            throw new NotSupportedException(
                "Recurse is not supported in clustered circuits: the per-iteration convergence decision is a global " +
                "reduction, which would need a cross-process round trip every loop iteration. Run recursive circuits " +
                "on a single process (Workers may still be > 1).");
        var duplicateNames = _nodes.Where(n => n.StateId is not null)
            .GroupBy(n => n.StateId!, StringComparer.Ordinal)
            .Where(g => g.Count() > 1)
            .Select(g => $"'{g.Key}'")
            .ToArray();
        if (duplicateNames.Length > 0)
            throw new InvalidOperationException(
                $"Operator names must be unique within a circuit — they key checkpointed state. Duplicated: {string.Join(", ", duplicateNames)}.");
        _built = true;
        return new Circuit(this, [.. _nodes], [.. _loopRegions], _inputs, _outputs, Options, WatermarkClock);
    }

    private void ThrowIfBuilt()
    {
        if (_built)
            throw new InvalidOperationException("The circuit has already been built; the builder can no longer be modified.");
    }
}
