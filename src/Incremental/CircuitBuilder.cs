using Incremental.Operators;
using Incremental.Runtime;
using Incremental.Storage;
using Incremental.Tables;

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

    /// <summary>The node's own error policy from <c>OnError</c>, or null for the circuit's.</summary>
    public RecordErrorPolicy? ErrorPolicy { get; set; }

    /// <summary>For the last node of an operator composed of several (the stream
    /// <c>LeftJoin</c> and <c>FullJoin</c> end in a concatenation that runs no delegate):
    /// the nodes that run the call's delegates, which <c>OnError</c> on the result sets
    /// instead. Null for every other node.</summary>
    public int[]? ErrorPolicyTargets { get; set; }

    /// <summary>The table nodes this node probes without consuming their changes (a
    /// <c>Lookup</c>): edges of the picture, not of the execution. Null for every other node.</summary>
    public int[]? Probes { get; set; }

    /// <summary>The keyed input whose state this node holds, or null. A keyed input's state
    /// is identified by the input's name — already the durable identity of its feed — so it
    /// survives topology edits without a <see cref="ZStream{T}.Named"/>.</summary>
    public string? InputStateOf { get; init; }

    /// <summary>Root of the node's state store names. Positional (declaration order) by
    /// default, so any upstream topology change renumbers it; an explicit name (or the input
    /// name, for a keyed input's state) pins it, so the node's checkpointed state survives
    /// circuit evolution.</summary>
    public string Name =>
        InputStateOf is not null ? $"input.{InputStateOf}.{Kind}"
        : StateId is null ? $"n{Id}.{Kind}"
        : $"named.{StateId}.{Kind}";
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

/// <summary>One node of a circuit's graph, as <see cref="CircuitBuilder.Describe"/> reports
/// it. <c>Kind</c> is the operator kind (<c>input[trades]</c>, <c>table[instruments]</c>,
/// <c>where</c>, <c>join</c>, <c>exchange</c> …); <c>StateName</c> is the root of the names
/// its state stores would get; <c>Name</c> is what pins that root (<c>.Named()</c>, or a keyed
/// input's name), null when it is positional. <c>Probes</c> are the table nodes it reads
/// rows from without consuming their changes — a <c>Lookup</c> or
/// <c>LookupAppendOnly</c> probes its table at the step's pinned version, so the table's
/// node is not among its <c>Inputs</c>; a table <c>Join</c> does consume the changes and
/// lists them there.</summary>
public sealed record CircuitGraphNode(
    int Id, string Kind, string StateName, IReadOnlyList<int> Inputs, bool IsExchange, string? Name, RecordErrorPolicy ErrorPolicy)
{
    public IReadOnlyList<int> Probes { get; init; } = [];
}

public enum CircuitOutputKind { Stream, Acknowledged, Table }

/// <summary>An output of the graph: the node it publishes and, for a derived table
/// (<c>ToTable</c>), the table it writes.</summary>
public sealed record CircuitGraphOutput(string Name, int NodeId, CircuitOutputKind Kind, string? Table);

/// <summary>A <c>Recurse</c> region: nodes <c>Start</c>..<c>End</c> iterate, and
/// <c>ResultNode</c> feeds <c>Start</c> across iterations — the one edge no node lists.</summary>
public sealed record CircuitGraphLoop(int Start, int End, int ResultNode);

public sealed record CircuitGraph(
    IReadOnlyList<CircuitGraphNode> Nodes, IReadOnlyList<CircuitGraphOutput> Outputs, IReadOnlyList<CircuitGraphLoop> Loops);

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
    private readonly Dictionary<string, object> _tables = new(StringComparer.Ordinal); // table name → its TableRef
    private readonly List<ITableNode> _readTables = [];
    private readonly List<DerivedTableRegistration> _derivedTables = [];
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
        ArgumentOutOfRangeException.ThrowIfLessThan(Options.DeadLetterCapacity, 1, nameof(Options.DeadLetterCapacity));
        if (!Enum.IsDefined(Options.ErrorPolicy))
            throw new ArgumentOutOfRangeException(nameof(Options.ErrorPolicy), Options.ErrorPolicy, "Unknown error policy.");
        if (Options.CheckpointMaxDelay is { } maxDelay && maxDelay <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(
                nameof(Options.CheckpointMaxDelay), maxDelay, "CheckpointMaxDelay must be positive, or null for no time bound.");
        if (Options.Cluster is { } cluster)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(cluster.ProcessCount, 1, nameof(Options.Cluster));
            ArgumentOutOfRangeException.ThrowIfNegative(cluster.ProcessIndex, nameof(Options.Cluster));
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(cluster.ProcessIndex, cluster.ProcessCount, nameof(Options.Cluster));
            // Processes commit in lockstep, on the same steps; one process's clock cannot
            // decide a commit.
            if (Options.CheckpointMaxDelay is not null)
                throw new NotSupportedException(
                    "CheckpointMaxDelay is not available on a clustered circuit: its processes must commit on the " +
                    "same steps, which a per-process time bound cannot guarantee. Use CheckpointInterval.");
            if (Options.AutoStep)
                throw new NotSupportedException(
                    "Clustered circuits require AutoStep = false: processes run their steps in lockstep, so step " +
                    "initiation is explicit — call StepAsync on every process for every step.");
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
    /// automatically. The current row per key is circuit state — checkpointed under the input's
    /// name, so it survives topology edits — which makes a keyed input safe to re-feed after a
    /// restart. Sharded circuits route keys by <see cref="StableHash"/>, so a key type it
    /// cannot derive a hash for is rejected here — supply one via the overload.</summary>
    public KeyedInputStream<TKey, TValue> KeyedInput<TKey, TValue>(string name, Func<TValue, TKey> keySelector)
        where TKey : notnull
        where TValue : notnull =>
        KeyedInputCore(name, keySelector, Options.IsSharded ? StableHash.RoutingHasherFor<TKey>("KeyedInput") : null);

    /// <summary>Declares a keyed input that routes keys across workers by
    /// <paramref name="stableHash"/> instead of the built-in structural hash — for key types
    /// the structural hash cannot derive. The hash must return the same value for equal keys
    /// in every process, on every machine: routing and checkpoint restore depend on it.</summary>
    public KeyedInputStream<TKey, TValue> KeyedInput<TKey, TValue>(
        string name, Func<TValue, TKey> keySelector, Func<TKey, uint> stableHash)
        where TKey : notnull
        where TValue : notnull
    {
        ArgumentNullException.ThrowIfNull(stableHash);
        return KeyedInputCore(name, keySelector, stableHash);
    }

    private KeyedInputStream<TKey, TValue> KeyedInputCore<TKey, TValue>(
        string name, Func<TValue, TKey> keySelector, Func<TKey, uint>? routingHash)
        where TKey : notnull
        where TValue : notnull
    {
        ThrowIfBuilt();
        RequireOutsideRecursion("KeyedInput");
        ArgumentNullException.ThrowIfNull(keySelector);
        if (_inputs.Any(i => i.Name == name))
            throw new ArgumentException($"An input named '{name}' already exists.", nameof(name));

        // Source (commands resolved per key) → [exchange by key, clustered only] → upsert
        // state. On a single process the port already placed each command on the worker
        // that owns its key, so no exchange is needed; across processes it only placed it
        // on some local worker.
        var source = AddNode($"input[{name}]", [], _ => new KeyedInputOperator<TKey, TValue>());
        var upstream = source;
        if (Options.Cluster is not null)
            upstream = AddNode(
                "exchange",
                [source],
                _ => new ExchangeOperator<KeyedCommand<TKey, TValue>>(c => routingHash!(c.Key)),
                isExchange: true);
        var state = AddNodeCore("upsert", [upstream], ctx => new UpsertOperator<TKey, TValue>(ctx),
            isExchange: false, inputStateOf: name);

        _inputs.Add(new KeyedInputRegistration<TKey, TValue>
        {
            Name = name,
            NodeId = source,
            KeySelector = keySelector,
            RoutingHash = routingHash,
        });
        return new KeyedInputStream<TKey, TValue>(this, state, name);
    }

    /// <summary>Attaches a shared table: the circuit can join against its rows, and receives
    /// its changes as they are published. The table lives outside the circuit and may be
    /// attached to any number of them; attaching the same table twice returns the same
    /// reference. A new circuit starts at the table's current version; a restarted one
    /// resumes from the version in its checkpoint, which commits under the table's name —
    /// so two tables with the same name cannot share a circuit.</summary>
    public TableRef<TKey, TRow> Table<TKey, TRow>(SharedTable<TKey, TRow> table)
        where TKey : notnull
        where TRow : class
    {
        ThrowIfBuilt();
        RequireOutsideRecursion("Table");
        ArgumentNullException.ThrowIfNull(table);
        if (_tables.TryGetValue(table.Name, out var attached))
            return attached is TableRef<TKey, TRow> existing && ReferenceEquals(existing.Table, table)
                ? existing
                : throw new ArgumentException($"A different table named '{table.Name}' is already attached.", nameof(table));

        var nodeId = AddNode($"table[{table.Name}]", [], _ => new InputOperator<TableChange<TKey, TRow>>());
        var reference = new TableRef<TKey, TRow>(this, table, nodeId);
        _inputs.Add(new TableRegistration<TKey, TRow> { Name = OffsetKeys.Table(table.Name), NodeId = nodeId, Ref = reference });
        _tables[table.Name] = reference;
        _readTables.Add(table);
        return reference;
    }

    /// <summary>Registers a derived table: the stream at <paramref name="nodeId"/> is
    /// written into <paramref name="table"/>. The table is claimed, and cycles rejected, at
    /// <see cref="Build"/>.</summary>
    internal void AddDerivedTable<TKey, TRow>(int nodeId, SharedTable<TKey, TRow> table)
        where TKey : notnull
        where TRow : class
    {
        ThrowIfBuilt();
        RequireOutsideRecursion("ToTable");
        ArgumentNullException.ThrowIfNull(table);
        RequireVisible(nodeId);
        if (_derivedTables.Any(d => d.Table.Name == table.Name))
            throw new ArgumentException(
                $"This circuit already writes a table named '{table.Name}'. A table has one row per key from one stream: " +
                "Concat the streams and write the result.", nameof(table));
        var registration = new DerivedTableRegistration<TKey, TRow> { Name = $"out{_outputs.Count}", NodeId = nodeId, Target = table };
        _outputs.Add(registration);
        _derivedTables.Add(registration);
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

    private int AddNodeCore(string kind, int[] inputs, Func<OperatorContext, IStepOperator> factory, bool isExchange,
        string? inputStateOf = null)
    {
        var id = _nodes.Count;
        _nodes.Add(new NodeDefinition
        {
            Id = id,
            Kind = kind,
            Inputs = inputs,
            IsExchange = isExchange,
            Factory = factory,
            InputStateOf = inputStateOf,
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
        if (node.InputStateOf is not null)
            throw new InvalidOperationException(
                $"A keyed input's state is already identified by its input name ('{node.InputStateOf}'); it cannot be renamed.");
        if (node.StateId is not null)
            throw new InvalidOperationException($"This operator is already named '{node.StateId}'.");
        node.StateId = name;
    }

    internal void SetErrorPolicy(int nodeId, RecordErrorPolicy policy)
    {
        ThrowIfBuilt();
        if (!Enum.IsDefined(policy))
            throw new ArgumentOutOfRangeException(nameof(policy), policy, "Unknown error policy.");
        // A grouping's stream is its exchange on a sharded circuit; the operator that runs
        // the key selector is the projection feeding it.
        var node = _nodes[nodeId];
        if (node.IsExchange)
            node = _nodes[node.Inputs[0]];
        node.ErrorPolicy = policy;
        if (node.ErrorPolicyTargets is { } targets)
            foreach (var target in targets)
                _nodes[target].ErrorPolicy = policy;
    }

    /// <summary>Records that <paramref name="nodeId"/> probes the table whose changes
    /// enter at <paramref name="tableNode"/>; returns <paramref name="nodeId"/>.</summary>
    internal int SetProbes(int nodeId, int tableNode)
    {
        _nodes[nodeId].Probes = [tableNode];
        return nodeId;
    }

    /// <summary>Makes <c>OnError</c> on <paramref name="nodeId"/> — the last node of an
    /// operator built from several — set the policy of the nodes that run its delegates.</summary>
    internal void SetErrorPolicyTargets(int nodeId, params int[] targets) =>
        _nodes[nodeId].ErrorPolicyTargets = targets;

    /// <summary>Whether any operator, or the circuit as a whole, dead-letters: the circuit
    /// then has a dead-letter output.</summary>
    internal bool DeadLetters =>
        Options.ErrorPolicy == RecordErrorPolicy.DeadLetter || _nodes.Any(n => n.ErrorPolicy == RecordErrorPolicy.DeadLetter);

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

    /// <summary>Registers an acknowledged output; see <see cref="AddOutput{T}"/> for the name.</summary>
    internal string AddAcknowledgedOutput<T>(int nodeId) where T : notnull
    {
        ThrowIfBuilt();
        RequireOutsideRecursion("ToAcknowledgedOutput");
        // Processes commit in lockstep, on the same steps; a commit that waits for one
        // process's consumer cannot guarantee that.
        if (Options.Cluster is not null)
            throw new NotSupportedException(
                "Acknowledged outputs (ToAcknowledgedOutput) are not available on a clustered circuit: its processes " +
                "must commit on the same steps, and a commit that waits for one process's consumer cannot guarantee that.");
        RequireVisible(nodeId);
        var name = $"out{_outputs.Count}";
        _outputs.Add(new AcknowledgedOutputRegistration<T> { Name = name, NodeId = nodeId });
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

    /// <summary>
    /// The graph as declared so far: every node with its kind and the nodes it reads, and
    /// every output with the node it publishes. Reads nothing but the builder — no table is
    /// claimed, no storage touched, no operator created — so it can be called on a builder
    /// that is never built, and before or after <see cref="Build"/>. Node ids are creation
    /// order, which is the topological order the workers execute.
    /// </summary>
    public CircuitGraph Describe()
    {
        var nodes = new CircuitGraphNode[_nodes.Count];
        for (var i = 0; i < nodes.Length; i++)
        {
            var node = _nodes[i];
            nodes[i] = new CircuitGraphNode(
                node.Id, node.Kind, node.Name, [.. node.Inputs], node.IsExchange, node.StateId ?? node.InputStateOf,
                node.ErrorPolicy ?? Options.ErrorPolicy)
            {
                Probes = node.Probes is { } probes ? [.. probes] : [],
            };
        }
        var outputs = new CircuitGraphOutput[_outputs.Count];
        for (var i = 0; i < outputs.Length; i++)
        {
            var output = _outputs[i];
            outputs[i] = output is DerivedTableRegistration derived
                ? new CircuitGraphOutput(output.Name, output.NodeId, CircuitOutputKind.Table, derived.Table.Name)
                : new CircuitGraphOutput(
                    output.Name, output.NodeId,
                    output.GetType().IsGenericType && output.GetType().GetGenericTypeDefinition() == typeof(AcknowledgedOutputRegistration<>)
                        ? CircuitOutputKind.Acknowledged
                        : CircuitOutputKind.Stream,
                    null);
        }
        return new CircuitGraph(nodes, outputs, _loopRegions.ConvertAll(l => new CircuitGraphLoop(l.Start, l.End, l.ResultNode)));
    }

    /// <summary>
    /// Every state store the circuit's operators would create, for every worker, with the
    /// types each holds. Like <see cref="Describe"/> it touches nothing outside the
    /// builder: the operators are constructed against throwaway stores and dropped, no
    /// table is claimed and <see cref="CircuitOptions.Storage"/> is not used — so it can be
    /// asked of a builder that is never built. The names are exactly those a circuit built
    /// from this builder commits in its state manifest.
    /// </summary>
    public IReadOnlyList<CircuitStateStore> DescribeState()
    {
        var recorder = new StateManifest.Recorder();
        var workers = Options.Workers;
        var first = Options.Cluster is { } cluster ? cluster.ProcessIndex * workers : 0;
        List<DeadLetter>? dropped = null;
        for (var w = 0; w < workers; w++)
        {
            foreach (var node in _nodes)
            {
                recorder.NodeId = node.Id;
                node.Factory(new OperatorContext
                {
                    WorkerId = w,
                    WorkerCount = workers,
                    GlobalWorkerId = first + w,
                    Storage = recorder,
                    NodeName = node.Name,
                    // An operator may keep more state under the dead-letter policy than
                    // without it, so it is told which it runs under.
                    Errors = (node.ErrorPolicy ?? Options.ErrorPolicy) == RecordErrorPolicy.DeadLetter
                        ? new RecordErrors(node.Name, dropped ??= [])
                        : null,
                });
            }
        }
        return recorder.Stores;
    }

    /// <summary>The state manifest a circuit built from this builder would commit.</summary>
    internal string DescribeStateManifest() =>
        StateManifest.Build(
            Options.Workers, Options.Cluster?.ProcessCount ?? 1, WatermarkClock?.Sources ?? 0,
            _readTables.Select(t => (t.Name, t.Epoch)).Concat(_derivedTables.Select(d => (d.Table.Name, d.Table.Epoch))),
            new SortedSet<string>(DescribeState().Select(s => s.Name), StringComparer.Ordinal));

    /// <summary>
    /// Whether a circuit built from this builder could resume the checkpoint
    /// <paramref name="committed"/> holds: the comparison <see cref="Circuit.Start"/> makes
    /// and refuses on, made beforehand and without building anything. Empty when nothing
    /// stands in the way — also when nothing has been committed. It covers the layout
    /// (format, worker count, table identities, store names); whether the committed bytes
    /// read as the types the stores now declare is known only when they are read.
    /// </summary>
    public IReadOnlyList<string> CheckState(ICheckpointStorageProvider committed)
    {
        ArgumentNullException.ThrowIfNull(committed);
        var scope = Options.Cluster is { } cluster ? $"p{cluster.ProcessIndex}" : "local";
        return committed.CommittedManifest(scope) is { } manifest ? StateManifest.Problems(manifest, DescribeStateManifest()) : [];
    }

    public Circuit Build()
    {
        ThrowIfBuilt();
        if (Options.Cluster is not null && _loopRegions.Count > 0)
            throw new NotSupportedException(
                "Recurse is not supported in clustered circuits: the per-iteration convergence decision is a global " +
                "reduction, which would need a cross-process round trip every loop iteration. Run recursive circuits " +
                "on a single process (Workers may still be > 1).");
        if (Options.Cluster is not null && _tables.Count > 0)
            throw new NotSupportedException(
                "Shared tables are not supported in clustered circuits: a table lives in one process, and the other " +
                "processes' workers could not read it. Run circuits that attach tables on a single process (Workers may " +
                "still be > 1).");
        if (Options.Cluster is not null && _derivedTables.Count > 0)
            throw new NotSupportedException(
                "Derived tables (ToTable) are not supported in clustered circuits: a table lives in one process, and its " +
                "version must commit before a checkpoint that every process takes on the same step. Run the circuit that " +
                "writes a table on a single process (Workers may still be > 1).");
        if (Options.Cluster is not null && Options.Storage is Storage.ChangelogStorageProvider)
            throw new NotSupportedException(
                "ChangelogStorageProvider is not supported in clustered circuits: a pipeline's commit record holds one " +
                "process's offsets, and the processes of a cluster would overwrite each other's. Run the circuit on a " +
                "single process (Workers may still be > 1), or use a provider the cluster can share.");
        // A name is a table's identity in the checkpoint, so two tables that share one
        // collide there whichever way round they are used.
        foreach (var derived in _derivedTables)
            if (_tables.ContainsKey(derived.Table.Name) && !_readTables.Any(t => ReferenceEquals(t, derived.Table)))
                throw new InvalidOperationException(
                    $"This circuit reads one table named '{derived.Table.Name}' and writes another. Both positions commit under " +
                    "the table's name; give the tables different names.");
        var duplicateNames = _nodes.Where(n => n.StateId is not null)
            .GroupBy(n => n.StateId!, StringComparer.Ordinal)
            .Where(g => g.Count() > 1)
            .Select(g => $"'{g.Key}'")
            .ToArray();
        if (duplicateNames.Length > 0)
            throw new InvalidOperationException(
                $"Operator names must be unique within a circuit — they key checkpointed state. Duplicated: {string.Join(", ", duplicateNames)}.");

        // Last, because it is the one check with an effect outside the builder: from here
        // the tables this circuit writes have it as their writer, until it is disposed or
        // faults. Reading a table it writes — directly or through other derived tables —
        // and a table that already has a writing circuit are both refused here.
        if (_derivedTables.Count > 0)
        {
            var claims = TableLineage.Claim(
                Options.Name is { } name ? $"circuit '{name}'" : "another circuit",
                [.. _readTables],
                _derivedTables.ConvertAll(d => d.Table));
            for (var i = 0; i < claims.Length; i++)
                _derivedTables[i].Claim = claims[i];
        }

        _built = true;
        try
        {
            return new Circuit(this, [.. _nodes], [.. _loopRegions], _inputs, _outputs, Options, WatermarkClock);
        }
        catch
        {
            foreach (var derived in _derivedTables)
                TableLineage.Release(derived.Table, derived.Claim!);
            throw;
        }
    }

    private void ThrowIfBuilt()
    {
        if (_built)
            throw new InvalidOperationException("The circuit has already been built; the builder can no longer be modified.");
    }
}
