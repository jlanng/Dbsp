using System.Diagnostics;
using Incremental.Cluster;
using Incremental.Diagnostics;
using Incremental.Operators;
using Incremental.Runtime;
using Incremental.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Incremental;

/// <summary>
/// A runnable dataflow circuit. Execution is step-based, as in DBSP: each step drains a
/// batch of records from the inputs, pushes the resulting Z-set deltas through every
/// operator in topological order on all workers in parallel, and publishes the per-output
/// deltas. Exchange nodes synchronize the workers on a barrier mid-step to reshuffle data
/// by key. Backpressure is end-to-end: bounded output channels stall publishing, which
/// stalls stepping, which stops input draining, which suspends producers on the bounded
/// input channels.
/// </summary>
public sealed class Circuit : IAsyncDisposable
{
    private readonly CircuitBuilder _builder;
    private readonly NodeDefinition[] _nodes;
    private readonly CircuitOptions _options;

    // Recurse loop regions, looked up by their first node. Per region, one convergence flag
    // per worker: each iteration every worker writes whether its shard still has work, the
    // exchange barrier makes the writes visible, and every worker reads all flags — so the
    // continue/stop decision is global and identical on all workers, keeping them iterating
    // in lockstep (which the exchanges inside the loop rely on for barrier alignment).
    private readonly LoopRegion?[] _loopStarts;
    private readonly bool[][] _loopContinue;
    private readonly IInputPort[] _inputs;
    private readonly IOutputPort[] _outputs;
    private readonly Worker[] _workers;
    private readonly SpinBarrier _barrier;
    private readonly WatermarkClock? _watermarkClock;

    // Single-worker circuits run their steps inline on the coordinator: no worker thread,
    // no handoff — the cheapest engine for small or latency-sensitive workloads.
    private bool RunsInline => _workers.Length == 1;

    // Exchange scratch space: _mailboxes[node][sender] holds the fragments (indexed by
    // target worker — GLOBAL worker in a cluster) that `sender` produced for that exchange
    // node this step. The exchange barrier makes writes visible before anyone reads.
    private readonly object[][][] _mailboxes;

    // Point-to-point exchange mode (CircuitOptions.PointToPointExchange, local only):
    // one published-sequence slot per (node, sender), strided a cache line apart so each
    // producer owns its line. A producer stores its fragments into _mailboxes[n][sender]
    // and release-publishes its execution count for the node; a consumer acquire-reads
    // each sender's slot until it reaches its own count, replacing the barrier rendezvous
    // (and its fences) with per-cell release/acquire. Null in barrier mode.
    private readonly long[]? _exchangeSeqs;
    private bool _exchangePoisoned;
    private const int SeqStride = 8; // longs per slot: one 64-byte cache line each

    // Park path for point-to-point waits (mirrors SpinBarrier): a waiter that exhausts the
    // spin budget registers itself in _parkedWaiters and waits on the gate; a publication
    // is a full fence followed by a read of _parkedWaiters, so it either sees the waiter
    // (and pulses) or the waiter's re-check under the lock sees the publication — the
    // Dekker pattern, never a lost wake-up. Poisoning pulses unconditionally.
    private readonly object _exchangeGate = new();
    private int _parkedWaiters;
    private readonly int _exchangeSpinBudget;

    // Fragment streaming (CircuitOptions.FragmentStreaming, point-to-point only; see
    // docs/design/fragment-streaming.md): per non-loop exchange node, a build-time plan —
    // the fused upstream linear chain, the chain's head slot, the optional sole-consumer
    // fragment sink, and every sender's fragment ring. _fragSeqs parallels _exchangeSeqs
    // with a cumulative published-fragment counter per (node, sender); the existing seq
    // publication doubles as the completion marker. _fused marks nodes whose execution
    // moved into an exchange's produce path — the sweep skips them. _produceAfter lists,
    // per node, the exchanges produced from its slot right after it runs; _emitInto marks
    // an emitting fragment sink with the exchange its per-fragment output is forwarded to.
    private readonly bool _streaming;
    private readonly StreamPlan?[] _streamPlans;
    private readonly bool[] _fused;
    private readonly StreamPlan[]?[] _produceAfter;
    private readonly StreamPlan?[] _emitInto;
    private readonly long[]? _fragSeqs;

    // Cross-process exchange (null when not clustered): the runtime shipping remote-bound
    // shards, plus scratch space _remoteShards[node][peerProcess] holding the shards each
    // peer sent this step (indexed by local worker), filled by the designated worker
    // between the two exchange barriers. _stepNumber stamps every frame so a desync faults
    // loudly instead of silently merging the wrong step's data.
    private readonly ClusterRuntime? _cluster;
    private readonly object[][][] _remoteShards;
    private long _stepNumber;
    private long _committedStepMarker = -1;

    // PHASE-STATS (temporary probe instrumentation — revert before committing). Env-gated
    // per-phase step attribution, printed to stderr at dispose.
    private static readonly bool PhaseStats =
        Environment.GetEnvironmentVariable("INCREMENTAL_PHASE_STATS") == "1";
    private long _psDrain, _psWorkerPhase, _psLaunch, _psLand, _psCheckpoint, _psPublish,
        _psTotal, _psSteps;

    private readonly CancellationTokenSource _shutdown = new();

    // Serializes steps against snapshot reads: a snapshot must never observe a half-applied
    // step, so RunStepAsync holds this for the whole step (publish included) and
    // ReadOutputSnapshotAsync reads only while no step is in flight.
    private readonly SemaphoreSlim _stepGate = new(1, 1);
    private TaskCompletionSource _stepDone = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _workersRemaining;

    // Auto-stepping circuits overlap the previous step's output publish with the current
    // step's execution (depth exactly one: a step stages its outputs only after the previous
    // flush finished, preserving per-output step order and keeping at most one step's deltas
    // outside the bounded channels — a slow consumer still stalls the circuit, one step
    // later). Manual stepping keeps the synchronous publish: StepAsync's contract is that
    // the step's deltas are published when it returns, and clustered circuits step in
    // lockstep. The flush task never faults; it parks its failure here for the coordinator
    // to surface inside the step fault domain (see AwaitOutputFlushAsync).
    private bool StagesPublishes => _options.AutoStep;
    private Task _outputFlush = Task.CompletedTask;
    private Exception? _flushFault;
    private bool _stopping;
    private bool _started;
    private bool _disposed;
    private Task? _pump;

    // Checkpointing: staged operator state and the input offsets it reflects are committed
    // together, every CheckpointInterval steps, once all workers' shards for those steps
    // are complete — one commit point for the whole circuit. The state manifest — every
    // store name this circuit's operators mint, plus the worker/process shape — is committed
    // alongside and compared on Start, so a rebuilt circuit whose layout no longer matches
    // the checkpoint (renumbered positional state, changed worker count) refuses to run
    // instead of silently reading the wrong operators' state.
    private readonly ICheckpointStorageProvider? _checkpoint;
    private readonly string _stateManifest = "";
    private Dictionary<string, long> _committedOffsets = new();
    private int _stepsSinceCommit;

    // Fault propagation: a worker that throws mid-step records its exception here and
    // poisons the exchange barrier, so peers blocked mid-shuffle bail out instead of
    // deadlocking on a participant that will never arrive. The last worker to leave the
    // step surfaces what was recorded (aggregated when several faulted together) on the
    // pending step, and the coordinator then moves the circuit to its terminal faulted
    // state — _fault is the first (or aggregated) exception, null while healthy.
    private readonly List<Exception> _stepFaults = [];
    private Exception? _fault;

    // Observability: per-circuit tag + loggers + gauge state (metrics ride the static
    // "Incremental" meter). Everything here is step-granular — never per record.
    private static int _autoNames;
    private readonly CircuitDiagnostics _diag;
    private readonly ILogger _log;

    internal Circuit(
        CircuitBuilder builder,
        NodeDefinition[] nodes,
        LoopRegion[] loopRegions,
        IReadOnlyList<InputRegistration> inputRegistrations,
        IReadOnlyList<OutputRegistration> outputRegistrations,
        CircuitOptions options,
        WatermarkClock? watermarkClock)
    {
        _builder = builder;
        _nodes = nodes;
        _options = options;

        _loopStarts = new LoopRegion?[nodes.Length];
        foreach (var region in loopRegions)
            _loopStarts[region.Start] = region;
        _loopContinue = loopRegions.Select(_ => new bool[options.Workers]).ToArray();
        _watermarkClock = watermarkClock;
        _checkpoint = options.Storage as ICheckpointStorageProvider;
        if (options.ExchangeTimeout is { } timeout && timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), "CircuitOptions.ExchangeTimeout must be positive (or null for no timeout).");
        Name = options.Name ?? $"circuit-{Interlocked.Increment(ref _autoNames)}";
        _diag = new CircuitDiagnostics(
            Name,
            options.LoggerFactory ?? NullLoggerFactory.Instance,
            options.Cluster?.ProcessCount ?? 1);
        _log = _diag.CircuitLogger;

        _barrier = new SpinBarrier(options.Workers);
        _exchangeSeqs = options.PointToPointExchange && options.Workers > 1
            ? new long[nodes.Length * options.Workers * SeqStride]
            : null;
        _streaming = options.FragmentStreaming && options.Workers > 1; // p2p is validated, so _exchangeSeqs exists
        _exchangeSpinBudget = options.ExchangeSpinBudget;
        _fragSeqs = _streaming ? new long[nodes.Length * options.Workers * SeqStride] : null;
        _cluster = options.Cluster is { } cluster
            ? new ClusterRuntime(cluster.Transport, options.Workers, _barrier, options.ExchangeTimeout, _diag)
            : null;

        // Operators create their stores in their constructors, all through
        // OperatorContext.StateName — so by the end of this loop the collected names are the
        // complete state layout of this process's slice of the circuit.
        var manifestStores = _checkpoint is null ? null : new SortedSet<string>(StringComparer.Ordinal);
        _workers = new Worker[options.Workers];
        for (var w = 0; w < options.Workers; w++)
        {
            var operators = new IStepOperator[nodes.Length];
            for (var n = 0; n < nodes.Length; n++)
            {
                var context = new OperatorContext
                {
                    WorkerId = w,
                    WorkerCount = options.Workers,
                    GlobalWorkerId = (_cluster?.FirstGlobalWorker ?? 0) + w,
                    Storage = options.Storage,
                    NodeName = nodes[n].Name,
                    ManifestObserver = manifestStores is null ? null : name => manifestStores.Add(name),
                };
                operators[n] = nodes[n].Factory(context);
            }
            // Per-node scratch, allocated once per worker rather than once per node per step.
            var peerCount = _cluster is null ? 0 : _cluster.ProcessCount - 1;
            var nodeInputs = new object?[nodes.Length][];
            var exchangeIncoming = new object?[nodes.Length][];
            for (var n = 0; n < nodes.Length; n++)
            {
                nodeInputs[n] = new object?[nodes[n].Inputs.Length];
                exchangeIncoming[n] = nodes[n].IsExchange
                    ? new object?[options.Workers + peerCount]
                    : [];
            }
            _workers[w] = new Worker(w, operators, new object?[nodes.Length], nodeInputs, exchangeIncoming);
            if (_exchangeSeqs is not null)
                _workers[w].ExchangeSeq = new long[nodes.Length];
            if (_streaming)
            {
                _workers[w].FragConsumed = new long[nodes.Length * options.Workers];
                _workers[w].FragPublished = new long[nodes.Length];
                _workers[w].FragExecCount = new int[nodes.Length];
                _workers[w].FragOpenRows = new int[nodes.Length];
                _workers[w].SinkPending = new long[nodes.Length];
                _workers[w].SenderDone = new bool[options.Workers];
                _workers[w].ChunkScratch = new IZSet?[nodes.Length][];
            }
        }
        if (manifestStores is not null)
            _stateManifest = BuildStateManifest(manifestStores);

        _mailboxes = new object[nodes.Length][][];
        _remoteShards = new object[nodes.Length][][];
        for (var n = 0; n < nodes.Length; n++)
        {
            _mailboxes[n] = nodes[n].IsExchange
                ? Enumerable.Range(0, options.Workers).Select(_ => Array.Empty<object>()).ToArray()
                : [];
            _remoteShards[n] = nodes[n].IsExchange && _cluster is not null
                ? new object[_cluster.ProcessCount][]
                : [];
        }

        _fused = new bool[nodes.Length];
        _streamPlans = new StreamPlan?[nodes.Length];
        _produceAfter = new StreamPlan[]?[nodes.Length];
        _emitInto = new StreamPlan?[nodes.Length];
        if (_streaming)
            BuildStreamPlans(nodes, loopRegions, outputRegistrations);

        _inputs = inputRegistrations.Select(r => r.CreatePort(options)).ToArray();
        _outputs = outputRegistrations.Select(r => r.CreatePort(options)).ToArray();
        foreach (var output in _outputs)
            output.BindSnapshot(this);

        _diag.RegisterGauges(_inputs, _outputs, _workers, () => CurrentWatermark, options.Cluster?.Transport);
        _log.LogInformation(
            "Circuit {Circuit} built: {Workers} workers, {Nodes} nodes, {Inputs} inputs, {Outputs} outputs, " +
            "cluster process {ProcessIndex} of {ProcessCount}, checkpointing {Checkpointing}",
            Name, options.Workers, nodes.Length, _inputs.Length, _outputs.Length,
            _cluster?.ProcessIndex ?? 0, _cluster?.ProcessCount ?? 1, _checkpoint is not null ? "on" : "off");
    }

    /// <summary>The circuit's name — <see cref="CircuitOptions.Name"/>, or the auto-assigned
    /// one — as used in its logs and as the <c>circuit</c> metric tag.</summary>
    public string Name { get; }

    /// <summary>Where a streamed exchange's production runs (fragment streaming v2; see
    /// docs/design/fragment-streaming.md).</summary>
    internal enum ProduceMode
    {
        /// <summary>From the head's slot at the exchange's own sweep position — the head is
        /// inside a loop region, so it has no single "after it ran" moment in the step.</summary>
        AtSelf,

        /// <summary>From the head's slot right after the head runs — as early in the sweep as
        /// the input exists.</summary>
        AfterHead,

        /// <summary>Fragment by fragment, before the head's own execution is complete: the head
        /// is an upstream streamed exchange whose only reader is this chain, forwarding each
        /// arriving fragment straight through, or an emitting fragment sink whose per-fragment
        /// output is final. Completion follows the head's.</summary>
        Forwarded,
    }

    /// <summary>Build-time plan for one streamed exchange node (fragment streaming).</summary>
    private sealed class StreamPlan
    {
        /// <summary>The exchange node.</summary>
        public required int Node { get; init; }

        /// <summary>Linear single-consumer nodes fused into the produce path, in execution
        /// (upstream-first) order; the sweep skips them.</summary>
        public required int[] FusedChain { get; init; }

        /// <summary>The node whose delta feeds the fused chain (the exchange's input when
        /// nothing fused).</summary>
        public required int Head { get; init; }

        public required ProduceMode Produce { get; init; }

        /// <summary>The sole-consumer <see cref="IFragmentSinkOperator"/> node fragments are
        /// fed to, or -1 when the exchange partial-merges into its own slot instead.</summary>
        public required int SinkNode { get; init; }

        /// <summary>Which of the sink's inputs this exchange is.</summary>
        public required int SinkInput { get; init; }

        /// <summary>Every sender's fragment ring, indexed [sender][ring slot][target] —
        /// the per-worker exchange operators' own rings, captured at build.</summary>
        public required object?[][][] Rings { get; init; }

        /// <summary>The downstream streamed exchange this one's consume loop forwards every
        /// arriving fragment into (through that exchange's fused chain), completing it when
        /// this one completes — set when this exchange's only reader is that chain. Null
        /// when the merge materializes in the slot or feeds a sink.</summary>
        public StreamPlan? ForwardTo { get; set; }

        public override string ToString() =>
            $"#{Node} {Produce} from #{Head}" +
            (FusedChain.Length > 0 ? $" via [{string.Join(",", FusedChain)}]" : "") +
            (SinkNode >= 0 ? $" -> sink #{SinkNode}[{SinkInput}]" : "") +
            (ForwardTo is not null ? $" -> forward #{ForwardTo.Node}" : "");
    }

    /// <summary>Plans fragment streaming for every exchange outside a <c>Recurse</c> region:
    /// walks the fused chain (linear, single-input, single-consumer, outside loops), finds
    /// the sole-consumer fragment sink if any, decides where production runs, and captures
    /// the senders' rings. Consumer counts include output ports (they read slots between
    /// steps) and the loop scheduler's direct read of each region's result slot, so fusion,
    /// sink feeding, and forwarding never bypass a reader the graph alone would not show.
    /// Nodes are visited in id (topological) order, so a head's own plan — when the head is
    /// an exchange — already exists when the exchange it forwards into is planned.</summary>
    private void BuildStreamPlans(
        NodeDefinition[] nodes, LoopRegion[] loopRegions, IReadOnlyList<OutputRegistration> outputs)
    {
        var inLoop = new bool[nodes.Length];
        foreach (var region in loopRegions)
            for (var i = region.Start; i <= region.End; i++)
                inLoop[i] = true;

        var consumers = new int[nodes.Length];
        foreach (var node in nodes)
            foreach (var input in node.Inputs)
                consumers[input]++;
        foreach (var output in outputs)
            consumers[output.NodeId]++;
        foreach (var region in loopRegions)
            consumers[region.ResultNode]++;

        var probe = _workers[0].Operators; // operator types are identical on every worker
        var produceAfter = new List<StreamPlan>?[nodes.Length];
        for (var n = 0; n < nodes.Length; n++)
        {
            if (!nodes[n].IsExchange || inLoop[n])
                continue;

            var chain = new List<int>();
            var head = nodes[n].Inputs[0];
            while (!inLoop[head] && consumers[head] == 1 && nodes[head].Inputs.Length == 1 && probe[head].IsLinear)
            {
                chain.Add(head);
                head = nodes[head].Inputs[0];
            }
            chain.Reverse();

            // A sink needs the exchange's one and only reader (a second edge, an output
            // port, or a self-join's double edge all disqualify) to integrate fragments
            // itself; anything else gets the partial merge from the slot.
            var (sinkNode, sinkInput) = (-1, -1);
            if (consumers[n] == 1)
                for (var m = 0; m < nodes.Length && sinkNode < 0; m++)
                    for (var i = 0; i < nodes[m].Inputs.Length; i++)
                        if (nodes[m].Inputs[i] == n)
                        {
                            if (!inLoop[m] && probe[m] is IFragmentSinkOperator)
                                (sinkNode, sinkInput) = (m, i);
                            break;
                        }

            // Where production runs. A head that only this chain reads can hand its delta
            // over before it is complete: an upstream streamed exchange forwards arriving
            // fragments from its consume loop; an emitting sink hands over each fragment's
            // (final) output as it is fed, and its Step's output — the non-streamed inputs —
            // completes the production. Everything else produces from the head's slot,
            // right after the head runs, or at this node when the head lives in a loop.
            var produce = ProduceMode.AfterHead;
            var forwardingHead = false;
            if (inLoop[head])
                produce = ProduceMode.AtSelf;
            else if (consumers[head] == 1 && nodes[head].IsExchange)
            {
                produce = ProduceMode.Forwarded;
                forwardingHead = true;
            }
            else if (consumers[head] == 1 && probe[head] is IEmittingFragmentSinkOperator
                     && nodes[head].Inputs.Any(input => _streamPlans[input]?.SinkNode == head))
                produce = ProduceMode.Forwarded;

            var rings = new object?[_workers.Length][][];
            for (var w = 0; w < _workers.Length; w++)
                rings[w] = ((IExchangeStepOperator)_workers[w].Operators[n]).FragmentRing;

            var plan = _streamPlans[n] = new StreamPlan
            {
                Node = n,
                FusedChain = [.. chain],
                Head = head,
                Produce = produce,
                SinkNode = sinkNode,
                SinkInput = sinkInput,
                Rings = rings,
            };
            foreach (var fused in chain)
                _fused[fused] = true;

            if (forwardingHead)
                _streamPlans[head]!.ForwardTo = plan;
            else if (produce != ProduceMode.AtSelf)
            {
                if (produce == ProduceMode.Forwarded)
                    _emitInto[head] = plan;
                (produceAfter[head] ??= []).Add(plan);
            }
        }
        for (var n = 0; n < nodes.Length; n++)
            if (produceAfter[n] is { } plans)
                _produceAfter[n] = [.. plans];
    }

    /// <summary>One line per streamed exchange: production mode, head, fused chain, sink, and
    /// forwarding — the structural decisions <see cref="BuildStreamPlans"/> made, for tests
    /// and the phase-stats report.</summary>
    /// <summary>Point-to-point waits that outlived the spin budget and parked, summed over
    /// the workers (racy read of per-worker counters; for tests and diagnostics).</summary>
    internal long ExchangeParks => _workers.Sum(w => Volatile.Read(ref w.ExchangeParks));

    internal IReadOnlyList<string> DescribeStreamPlans() =>
        _streamPlans.Where(p => p is not null).Select(p => p!.ToString()).ToArray();

    /// <summary>Starts the worker threads (and, with <see cref="CircuitOptions.AutoStep"/>, the pump).</summary>
    public void Start()
    {
        if (_started)
            throw new InvalidOperationException("The circuit has already been started.");
        _started = true;

        if (_checkpoint is not null)
        {
            // Recover before the first step: discard whatever a crashed run left staged and
            // seed every input's position from the committed offsets, so records replayed
            // with explicit offsets at or below the checkpoint are dropped as duplicates.
            var offsets = FilterOwnOffsets(_checkpoint.Recover());

            // Refuse a mismatched restore before anything runs: resuming a checkpoint whose
            // state layout this circuit no longer matches would read the wrong operators'
            // state (or none) silently. No committed manifest means a fresh store — nothing
            // to disagree with. The current manifest is staged so it lands with the next
            // commit, always describing exactly the state committed beside it.
            var scope = ManifestScope;
            if (_checkpoint.CommittedManifest(scope) is { } committedManifest)
            {
                ValidateStateManifest(committedManifest, _stateManifest);
                _log.LogInformation(
                    "Circuit {Circuit} resuming from a committed checkpoint: state manifest validated, {Offsets} committed input offsets seeded",
                    Name, offsets.Count);
            }
            _checkpoint.StageManifest(scope, _stateManifest);

            foreach (var (key, offset) in offsets)
            {
                // Plain keys are unstamped running counts; "{name}#p{partition}" keys are a
                // stamped input's per-partition positions (see OffsetKeys). Entries for
                // inputs this circuit no longer declares are ignored, as ever.
                var (name, partition) = OffsetKeys.TryDecode(key, out var decodedName, out var decodedPartition)
                    ? (decodedName, (int?)decodedPartition)
                    : (key, null);
                if (Array.Find(_inputs, p => p.Name == name) is not { } input)
                    continue;
                if (partition is { } p)
                    input.SeedPartition(p, offset);
                else
                    input.SeedUnstamped(offset);
            }
            Volatile.Write(ref _committedOffsets, offsets);
        }

        if (!RunsInline)
        {
            foreach (var worker in _workers)
            {
                var thread = new Thread(() => WorkerLoop(worker))
                {
                    IsBackground = true,
                    Name = $"circuit-worker-{worker.Id}",
                };
                worker.Thread = thread;
                thread.Start();
            }
        }

        if (_options.AutoStep)
            _pump = Task.Run(PumpAsync);

        _log.LogInformation("Circuit {Circuit} started ({Stepping})",
            Name, _options.AutoStep ? "auto-stepping" : "manual stepping");
    }

    /// <summary>The circuit's current event-time watermark: the frozen value as of the end
    /// of the last step, or null when no <c>WithWatermark</c> source exists or none has
    /// seen data yet.</summary>
    public DateTimeOffset? CurrentWatermark
    {
        get
        {
            var ticks = _watermarkClock?.FrozenTicks ?? long.MinValue;
            return ticks == long.MinValue ? null : new DateTimeOffset(ticks, TimeSpan.Zero);
        }
    }

    /// <summary>Redeems an input definition for its live handle. The definition must come
    /// from the builder that built this circuit.</summary>
    public InputHandle<T> Handle<T>(InputStream<T> input) where T : notnull
    {
        RequireOwnDefinition(input.Builder);
        var port = _inputs.First(p => p.Name == input.InputName);
        return (InputHandle<T>)port.Handle;
    }

    /// <summary>Redeems a keyed-input definition for its live upsert/delete handle. The
    /// definition must come from the builder that built this circuit.</summary>
    public KeyedInputHandle<TKey, TValue> Handle<TKey, TValue>(KeyedInputStream<TKey, TValue> input)
        where TKey : notnull
        where TValue : notnull
    {
        RequireOwnDefinition(input.Builder);
        var port = _inputs.First(p => p.Name == input.InputName);
        return (KeyedInputHandle<TKey, TValue>)port.Handle;
    }

    /// <summary>Redeems an output definition for its live handle. The definition must come
    /// from the builder that built this circuit.</summary>
    public OutputHandle<T> Handle<T>(Output<T> output) where T : notnull
    {
        RequireOwnDefinition(output.Builder);
        var port = _outputs.First(p => p.Name == output.RegistrationName);
        return (OutputHandle<T>)port.Handle;
    }

    private void RequireOwnDefinition(CircuitBuilder source)
    {
        if (!ReferenceEquals(source, _builder))
            throw new ArgumentException(
                "This definition belongs to a different CircuitBuilder; redeem definitions against the circuit " +
                "their builder built.");
    }

    /// <summary>
    /// Runs exactly one step: drains currently buffered input, propagates it through the
    /// circuit, and publishes output deltas. Only valid with <c>AutoStep = false</c>; do not
    /// call concurrently with itself.
    /// </summary>
    public async Task StepAsync(CancellationToken cancellationToken = default)
    {
        if (!_started)
            throw new InvalidOperationException("Start the circuit before stepping it.");
        if (_options.AutoStep)
            throw new InvalidOperationException("Manual stepping requires CircuitOptions.AutoStep = false.");
        await RunStepAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task RunStepAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _fault) is { } fault)
            throw new CircuitFaultedException(fault);

        await _stepGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await RunStepGatedAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _stepGate.Release();
        }
    }

    private async Task RunStepGatedAsync(CancellationToken cancellationToken)
    {
        try
        {
            // Connect the cluster mesh before the first step (peers may start in any order).
            if (_cluster is not null)
                await _cluster.EnsureStartedAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            FaultCircuit(ex);
            throw;
        }

        _stepNumber++;
        var stepStarted = Stopwatch.GetTimestamp();
        try
        {
            // Draining belongs to the step's fault domain too: it consumes records from the
            // input channels and advances replay positions, so a drain that throws (a
            // stamped/unstamped mode violation) must fault the circuit, not leave it
            // "healthy" with records silently gone.
            var drained = 0L;
            foreach (var input in _inputs)
                drained += input.DrainInto(_workers, _options.MaxStepItemsPerInput);
            _diag.RecordsDrained(drained);

            var psDrained = Stopwatch.GetTimestamp(); // PHASE-STATS

            if (RunsInline)
            {
                // Clustered inline steps block on remote frames mid-step; that must never
                // happen synchronously on the caller's thread (StepAsync would not even
                // return its task until the whole step — including remote waits — finished,
                // deadlocking a caller that steps several co-hosted processes together).
                if (_cluster is not null)
                    await Task.Run(() => RunWorkerStep(_workers[0])).ConfigureAwait(false);
                else
                    RunWorkerStep(_workers[0]);
            }
            else
            {
                // Auto-stepping: synchronous continuations, so the last worker to finish
                // runs the coordinator's land work (freeze, commit, publish staging, next
                // launch) inline instead of paying a thread-pool wake — the pump owns the
                // whole await chain, so nothing foreign runs on the worker thread. Manual
                // stepping keeps the asynchronous hop: StepAsync's caller continuation must
                // not run (or block) on a worker thread.
                _stepDone = new TaskCompletionSource(StagesPublishes
                    ? TaskCreationOptions.None
                    : TaskCreationOptions.RunContinuationsAsynchronously);
                Volatile.Write(ref _workersRemaining, _workers.Length);
                foreach (var worker in _workers)
                    worker.Go.Release();

                await _stepDone.Task.ConfigureAwait(false);
            }

            // PHASE-STATS: attribute the worker-phase block; launch = release-to-earliest
            // worker start, land = latest worker end to coordinator resumption.
            var psWorkersDone = Stopwatch.GetTimestamp();
            if (PhaseStats)
            {
                _psDrain += psDrained - stepStarted;
                _psWorkerPhase += psWorkersDone - psDrained;
                if (!RunsInline)
                {
                    long first = long.MaxValue, last = 0;
                    foreach (var w in _workers)
                    {
                        first = Math.Min(first, w.StepStartTs);
                        last = Math.Max(last, w.StepEndTs);
                    }
                    _psLaunch += Math.Max(0, first - psDrained);
                    _psLand += Math.Max(0, psWorkersDone - last);
                }
            }

            // End-of-step alignment across processes: every process finishes this step, and
            // watermark observations are min-reduced globally, before anyone freezes.
            if (_cluster is not null)
                await _cluster.AwaitStepEndsAsync(_stepNumber, _watermarkClock).ConfigureAwait(false);

            // Freeze between steps so every worker sees one consistent watermark per step.
            _watermarkClock?.Freeze();

            // Commit before publishing: durability should not wait on a slow output consumer.
            // Output deltas are transient (consumers integrate them) and not checkpointed.
            // A commit failure is a fault like any other: the provider's atomicity keeps the
            // previous checkpoint intact, and the circuit must not run on unsure durability.
            var psPreCommit = Stopwatch.GetTimestamp(); // PHASE-STATS
            if (_checkpoint is not null && ++_stepsSinceCommit >= _options.CheckpointInterval)
                CommitCheckpoint();
            var psCommitted = Stopwatch.GetTimestamp(); // PHASE-STATS

            // Publishing belongs to the step's fault domain: state has already advanced, so
            // a failed or cancelled publish would silently lose this step's deltas and
            // permanently desynchronize every consumer's integrated view. Fault instead —
            // the committed checkpoint (taken above, before publishing) stays the intact
            // resume point. Auto-stepping circuits stage the deltas here and flush them
            // overlapped with the next step (see StagesPublishes); the previous step's
            // flush — and its failure — lands first, so outputs stay in step order and a
            // flush fault surfaces at most one step late.
            if (StagesPublishes)
            {
                await AwaitOutputFlushAsync().ConfigureAwait(false);
                var staged = false;
                foreach (var output in _outputs)
                    staged |= output.Stage(_workers);
                if (staged)
                    _outputFlush = Task.Run(() => FlushOutputsAsync(cancellationToken));
            }
            else
            {
                foreach (var output in _outputs)
                    await output.PublishAsync(_workers, cancellationToken).ConfigureAwait(false);
            }

            if (PhaseStats) // PHASE-STATS
            {
                var now = Stopwatch.GetTimestamp();
                _psCheckpoint += psCommitted - psPreCommit;
                _psPublish += now - psCommitted;
                _psTotal += now - stepStarted;
                _psSteps++;
            }

            _diag.StepCompleted(Stopwatch.GetElapsedTime(stepStarted).TotalSeconds);
            if (_watermarkClock is not null && _log.IsEnabled(LogLevel.Trace))
                _log.LogTrace("Circuit {Circuit} step {Step} complete; watermark {Watermark:O}",
                    Name, _stepNumber, CurrentWatermark);
        }
        catch (Exception ex)
        {
            FaultCircuit(ex);
            throw;
        }
    }

    /// <summary>Writes every staged output delta to its bounded channel, in output order.
    /// Runs off the coordinator path (overlapped with the next step) and never faults: a
    /// failure is parked in <see cref="_flushFault"/> for <see cref="AwaitOutputFlushAsync"/>
    /// to surface inside a step's fault domain — an unawaited faulted task here would
    /// otherwise go unobserved when the step that would await it faults first.</summary>
    private async Task FlushOutputsAsync(CancellationToken cancellationToken)
    {
        try
        {
            foreach (var output in _outputs)
                await output.FlushStagedAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // The circuit faulting mid-flush completes the channels with the fault; this
            // ChannelClosedException is that fault surfacing here, not a new failure.
            if (ex is System.Threading.Channels.ChannelClosedException && Volatile.Read(ref _fault) is not null)
                return;
            _flushFault = ex;
        }
    }

    /// <summary>Awaits the in-flight output flush and rethrows its failure, if any, into the
    /// caller's fault domain (with its original stack). Callers: the next step before it
    /// stages, the pump before completing the outputs, and the snapshot read before it
    /// discards subsumed deltas.</summary>
    private async Task AwaitOutputFlushAsync()
    {
        await _outputFlush.ConfigureAwait(false);
        if (_flushFault is { } fault)
        {
            _flushFault = null;
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(fault).Throw();
        }
    }

    /// <summary>Services the bootstrap read of <see cref="OutputHandle{T}.Changes"/> for a snapshot
    /// output: merges the per-worker integrated shards of the output's snapshot node, under
    /// the step gate so the read never interleaves with a step. While waiting for the gate
    /// it discards the output's buffered deltas — they are subsumed by the snapshot about to
    /// be taken, and a step blocked publishing into this output's full channel could
    /// otherwise never finish and release the gate (deadlock).</summary>
    internal async Task<ZSet<T>> ReadOutputSnapshotAsync<T>(
        int nodeId, Action discardPending, CancellationToken cancellationToken) where T : notnull
    {
        if (!_started)
            throw new InvalidOperationException("Start the circuit before reading a snapshot.");

        while (true)
        {
            if (Volatile.Read(ref _fault) is { } fault)
                throw new CircuitFaultedException(fault);
            if (await _stepGate.WaitAsync(TimeSpan.FromMilliseconds(10), cancellationToken).ConfigureAwait(false))
                break;
            discardPending();
        }

        try
        {
            if (Volatile.Read(ref _fault) is { } fault)
                throw new CircuitFaultedException(fault);

            // A staged flush may still be delivering the previous step's deltas: let it
            // land before the discard, or deltas the snapshot subsumes could arrive after
            // it and be integrated twice by the bootstrapping consumer.
            try
            {
                await AwaitOutputFlushAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                FaultCircuit(ex);
                throw;
            }
            discardPending();

            var merged = new ZSet<T>();
            foreach (var worker in _workers)
                ((SnapshotOperator<T>)worker.Operators[nodeId]).ReadInto(merged);
            return merged;
        }
        finally
        {
            _stepGate.Release();
        }
    }

    /// <summary>The exception that faulted the circuit — the original worker fault, an
    /// <see cref="AggregateException"/> when several workers faulted in the same step — or
    /// null while the circuit is healthy. A faulted circuit's in-memory state is unusable
    /// (the faulting step is half-applied); with a checkpointing storage provider the last
    /// committed checkpoint is untouched, so rebuild the circuit against the same provider
    /// to recover.</summary>
    public Exception? Fault => Volatile.Read(ref _fault);

    /// <summary>Moves the circuit to its terminal faulted state, on the coordinator, once
    /// every worker has left the faulting step: the step's half-staged effects will never be
    /// committed (see the fault check in <see cref="CommitCheckpoint"/> callers and
    /// <see cref="DisposeAsync"/>), producers are released with a
    /// <see cref="CircuitFaultedException"/> instead of blocking on channels no step will
    /// drain again, consumers' reads observe the original fault after the pre-fault deltas,
    /// and worker threads exit. The step that faulted still throws the original exception;
    /// later <see cref="StepAsync"/> calls throw <see cref="CircuitFaultedException"/>.
    /// <see cref="DisposeAsync"/> of a faulted circuit completes quietly — the fault has
    /// already surfaced on the step, the handles, and <see cref="Fault"/>.</summary>
    private void FaultCircuit(Exception exception)
    {
        if (Interlocked.CompareExchange(ref _fault, exception, null) is null)
        {
            _diag.CircuitFaulted();
            _log.LogError(exception, "Circuit {Circuit} faulted on step {Step}; terminal — rebuild from the last committed checkpoint to recover",
                Name, _stepNumber);
        }
        var fault = _fault!;

        // Peers must learn promptly (Fault frame, backstopped by disconnect) and local
        // waits on cluster traffic must release; idempotent when a worker poisoned already.
        _cluster?.Poison(fault);

        foreach (var input in _inputs)
            input.Fail(new CircuitFaultedException(fault));
        foreach (var output in _outputs)
            output.Complete(fault);

        // Workers parked between steps exit now; ones still unwinding out of the faulting
        // step exit when they next park. Normal disposal releases again — harmless.
        Volatile.Write(ref _stopping, true);
        foreach (var worker in _workers)
            worker.Go.Release();
    }

    /// <summary>Atomically persists everything the steps since the last commit staged,
    /// together with the input offsets those steps consumed. Runs on the coordinator
    /// between steps, after every worker's shard is complete — the provider's single
    /// commit covers the whole circuit.</summary>
    private void CommitCheckpoint()
    {
        var started = Stopwatch.GetTimestamp();
        var offsets = new Dictionary<string, long>(_inputs.Length);
        foreach (var input in _inputs)
            input.AppendConsumedOffsets(offsets);

        if (_cluster is { } cluster)
        {
            // Each process commits only its own shards and its own inputs' offsets, under
            // keys that carry the process index — plus a step marker — so processes sharing
            // one physical store never clobber each other. Cross-process consistency is by
            // lockstep, not 2PC: with the same CheckpointInterval every process commits the
            // same steps, and after a crash all processes must resume from the same
            // committed step (compare CommittedStep across processes before feeding data).
            var prefixed = new Dictionary<string, long>(offsets.Count + 1);
            foreach (var (name, offset) in offsets)
                prefixed[$"p{cluster.ProcessIndex}:{name}"] = offset;
            prefixed[$"p{cluster.ProcessIndex}:{StepMarkerKey}"] = _stepNumber;
            _checkpoint!.Commit(prefixed);
            Volatile.Write(ref _committedStepMarker, _stepNumber);
        }
        else
        {
            _checkpoint!.Commit(offsets);
        }

        _stepsSinceCommit = 0;
        Volatile.Write(ref _committedOffsets, offsets);

        var elapsed = Stopwatch.GetElapsedTime(started);
        _diag.CheckpointCommitted(elapsed.TotalSeconds);
        _log.LogDebug("Circuit {Circuit} committed a checkpoint at step {Step} in {ElapsedMs:F1} ms",
            Name, _stepNumber, elapsed.TotalMilliseconds);
    }

    private const string StepMarkerKey = "$step";

    /// <summary>Manifests are per process: processes sharing one physical store commit
    /// disjoint shards (store names carry global worker ids) and validate their own slice.</summary>
    private string ManifestScope => _cluster is { } cluster ? $"p{cluster.ProcessIndex}" : "local";

    /// <summary>A stable, line-based description of this process's state layout: format
    /// version, worker/process shape, then every store name, sorted. Store names encode the
    /// operator's identity (positional, or explicit via <c>.Named</c>), kind, purpose, and
    /// worker — so any change that would misdirect a restore changes the manifest.</summary>
    private string BuildStateManifest(IEnumerable<string> storeNames)
    {
        var manifest = new System.Text.StringBuilder();
        // format 3: the 0.2 API surface rewrite — operator kinds and node layouts changed
        // (groupBy/select naming, windowed groupings' composite WindowKey state, snapshot
        // nodes on table-shaped outputs), so store names moved wholesale. No deployed
        // format-2 checkpoints existed; the bump is hygiene.
        // (format 2: the committed-offsets marker became per-partition — see OffsetKeys.)
        manifest.Append("format=3\n");
        manifest.Append($"workers={_options.Workers}\n");
        manifest.Append($"processes={_cluster?.ProcessCount ?? 1}\n");
        foreach (var store in storeNames)
            manifest.Append($"store={store}\n");
        return manifest.ToString();
    }

    private static void ValidateStateManifest(string committed, string current)
    {
        if (committed == current)
            return;

        static (Dictionary<string, string> Facts, List<string> Stores) Parse(string manifest)
        {
            var facts = new Dictionary<string, string>();
            var stores = new List<string>();
            foreach (var line in manifest.Split('\n', StringSplitOptions.RemoveEmptyEntries))
            {
                var separator = line.IndexOf('=');
                var (key, value) = (line[..separator], line[(separator + 1)..]);
                if (key == "store")
                    stores.Add(value);
                else
                    facts[key] = value;
            }
            return (facts, stores);
        }

        var (committedFacts, committedStores) = Parse(committed);
        var (currentFacts, currentStores) = Parse(current);

        var problems = new List<string>();
        foreach (var fact in new[] { "format", "workers", "processes" })
            if (committedFacts.GetValueOrDefault(fact) != currentFacts.GetValueOrDefault(fact))
                problems.Add($"{fact} changed: the checkpoint was committed with {fact}={committedFacts.GetValueOrDefault(fact)}, " +
                             $"this circuit has {fact}={currentFacts.GetValueOrDefault(fact)}");

        // New stores are fine — they start empty. Committed stores this circuit no longer
        // mints are not: their state would be orphaned, and whatever operator inherited
        // their position would be reading someone else's.
        var currentSet = new HashSet<string>(currentStores, StringComparer.Ordinal);
        var missing = committedStores.Where(s => !currentSet.Contains(s)).ToList();
        if (missing.Count > 0)
        {
            const int shown = 12;
            problems.Add($"committed state with no home in this circuit:\n      " +
                         string.Join("\n      ", missing.Take(shown)) +
                         (missing.Count > shown ? $"\n      … and {missing.Count - shown} more" : ""));
        }

        if (problems.Count == 0)
            return;

        throw new InvalidOperationException(
            "This circuit does not match the checkpoint it is resuming from — refusing to start, because a mismatched " +
            "restore silently reads the wrong operators' state:\n  - " + string.Join("\n  - ", problems) + "\n" +
            "Operator state is keyed by circuit position unless named, so inserting, removing, or reordering operators " +
            "renumbers everything downstream. Options: revert the topology or worker-count change; give stateful " +
            "operators stable identities with .Named(\"…\") (before their first checkpoint) so unrelated changes leave " +
            "them alone; or start fresh against an empty state store (for Redis, a new KeyPrefix).");
    }

    /// <summary>Clustered circuits: the step number this process's last committed checkpoint
    /// reflects — the recovered marker until the first new commit, then the current run's
    /// step count as of each commit; null before any commit (or when not clustered). After a
    /// crash, compare it across processes before feeding data: state exchanged mid-step
    /// makes shards interdependent, so a consistent resume requires every process to have
    /// committed the same step.</summary>
    public long? CommittedStep
    {
        get
        {
            var marker = Volatile.Read(ref _committedStepMarker);
            return marker < 0 ? null : marker;
        }
    }

    /// <summary>Strips a recovered offsets dictionary down to this process's entries. On a
    /// single process the committed keys are the input names themselves; in a cluster they
    /// are prefixed with the process index (see <see cref="CommitCheckpoint"/>).</summary>
    private Dictionary<string, long> FilterOwnOffsets(IReadOnlyDictionary<string, long> recovered)
    {
        if (_cluster is not { } cluster)
            return new Dictionary<string, long>(recovered);

        var prefix = $"p{cluster.ProcessIndex}:";
        var offsets = new Dictionary<string, long>();
        foreach (var (key, value) in recovered)
        {
            if (!key.StartsWith(prefix, StringComparison.Ordinal))
                continue;
            var name = key[prefix.Length..];
            if (name == StepMarkerKey)
                Volatile.Write(ref _committedStepMarker, value);
            else
                offsets[name] = value;
        }
        return offsets;
    }

    /// <summary>The offset of the last record on <paramref name="inputName"/> whose effects
    /// are in the durably committed checkpoint, or null when nothing has been committed for
    /// it (or <see cref="CircuitOptions.Storage"/> does not checkpoint). After a restart
    /// this is where a producer should resume its source; records re-sent with explicit
    /// offsets at or below it are dropped as duplicates. For an unstamped input this is the
    /// committed running count; for a stamped one it is the single committed partition's
    /// position — an input with several committed partitions has no one offset, so this
    /// throws: use <see cref="CommittedOffsets"/>.</summary>
    public long? CommittedOffset(string inputName)
    {
        RequireInput(inputName);
        var committed = Volatile.Read(ref _committedOffsets);
        if (committed.TryGetValue(inputName, out var count))
            return count;

        var partitions = 0;
        var single = 0L;
        foreach (var (key, offset) in committed)
        {
            if (OffsetKeys.TryDecode(key, out var name, out _) && name == inputName)
                (partitions, single) = (partitions + 1, offset);
        }
        return partitions switch
        {
            0 => null,
            1 => single,
            _ => throw new InvalidOperationException(
                $"Input '{inputName}' has committed offsets on {partitions} partitions; use CommittedOffsets to read them per partition."),
        };
    }

    /// <summary>The committed position of every partition of <paramref name="inputName"/>:
    /// partition → offset of the last record whose effects are in the durably committed
    /// checkpoint. Empty when nothing has been committed for it, or when the input carries
    /// unstamped records (their running count has no partitions — see
    /// <see cref="CommittedOffset"/>). After a restart, resume each partition of the source
    /// just above its committed position; replayed records at or below it are dropped.</summary>
    public IReadOnlyDictionary<int, long> CommittedOffsets(string inputName)
    {
        RequireInput(inputName);
        var offsets = new Dictionary<int, long>();
        foreach (var (key, offset) in Volatile.Read(ref _committedOffsets))
        {
            if (OffsetKeys.TryDecode(key, out var name, out var partition) && name == inputName)
                offsets[partition] = offset;
        }
        return offsets;
    }

    private void RequireInput(string inputName)
    {
        if (_inputs.All(p => p.Name != inputName))
            throw new KeyNotFoundException($"No input named '{inputName}'.");
    }

    private async Task PumpAsync()
    {
        var cancellationToken = _shutdown.Token;
        var waits = new Task<bool>?[_inputs.Length];
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (_inputs.Any(p => p.HasPending))
                {
                    await RunStepAsync(cancellationToken).ConfigureAwait(false);
                    continue;
                }

                if (_inputs.All(p => p.IsCompleted))
                    break;

                for (var i = 0; i < _inputs.Length; i++)
                    if (!_inputs[i].IsCompleted)
                        waits[i] ??= _inputs[i].WaitToReadAsync(cancellationToken);

                await Task.WhenAny(waits.Where(t => t is not null).Cast<Task>()).ConfigureAwait(false);

                for (var i = 0; i < waits.Length; i++)
                    if (waits[i] is { IsCompleted: true })
                        waits[i] = null;
            }

            // The watermark freezes only after the last data-bearing step, so emissions it
            // unlocks (window-close notifications) need one final step to reach the outputs.
            if (_watermarkClock is not null && !cancellationToken.IsCancellationRequested)
                await RunStepAsync(cancellationToken).ConfigureAwait(false);

            // The last step's staged flush must land before the outputs complete; its
            // failure is a lost delta — a circuit fault, exactly as on a step.
            try
            {
                await AwaitOutputFlushAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                FaultCircuit(ex);
                throw;
            }

            foreach (var output in _outputs)
                output.Complete(null);
        }
        catch (Exception ex)
        {
            foreach (var output in _outputs)
                output.Complete(ex is OperationCanceledException ? null : ex);
        }
    }

    // SpinOnce iterations a worker between steps spends polling its Go semaphore before
    // parking on it: the coordinator gap between steps is usually short (drain, freeze,
    // publish staging — the every-Nth-step commit is the long pole), and the semaphore's
    // kernel wake is a measurable slice of a sparse step. Same shape as SpinBarrier's
    // budget: a short burst of spins, then yields, then a real park — a quiet circuit
    // costs nothing.
    private const int LaunchSpinBudget = 128;

    private void WorkerLoop(Worker worker)
    {
        while (true)
        {
            if (!worker.Go.Wait(0))
            {
                var spinner = new SpinWait();
                while (!worker.Go.Wait(0))
                {
                    if (spinner.Count >= LaunchSpinBudget)
                    {
                        worker.Go.Wait();
                        break;
                    }
                    spinner.SpinOnce(sleep1Threshold: -1);
                }
            }
            if (Volatile.Read(ref _stopping))
                return;

            worker.StepStartTs = Stopwatch.GetTimestamp(); // PHASE-STATS

            try
            {
                RunWorkerStep(worker);
            }
            catch (BarrierPoisonedException)
            {
                // A peer faulted and poisoned the barrier: bail out of the step promptly.
                // The peer recorded the real exception; this worker just stops shuffling.
            }
            catch (Exception ex)
            {
                // This worker faulted: record the exception, then poison the barrier —
                // in that order, so a peer released by the poison can never find the
                // fault list empty. Peers blocked mid-shuffle unwind via the poison;
                // peers that already finished the step are parked and are released by
                // the coordinator's fault transition.
                lock (_stepFaults)
                    _stepFaults.Add(ex);
                // Point-to-point waiters poll this flag: peers spinning on a publication
                // this worker will never make unwind instead of spinning forever — and
                // parked ones are pulsed awake to find it.
                Volatile.Write(ref _exchangePoisoned, true);
                lock (_exchangeGate)
                    Monitor.PulseAll(_exchangeGate);
                _barrier.Poison();
                // Remote peers must not wait on shards this worker will never ship, and the
                // designated worker may be blocked on inbound cluster traffic.
                _cluster?.Poison(ex);
            }

            // PHASE-STATS
            worker.StepEndTs = Stopwatch.GetTimestamp();
            worker.SpanTicks += worker.StepEndTs - worker.StepStartTs;

            // Every worker — clean, faulted, or bailed — leaves the step through this
            // decrement, so the last one out sees every fault recorded this step and can
            // surface them (aggregated if several workers faulted together) on the pending
            // step. The coordinator then runs the circuit-wide fault transition; this
            // thread loops around, parks, and exits via the _stopping release it sends.
            if (Interlocked.Decrement(ref _workersRemaining) == 0)
            {
                Exception? fault;
                lock (_stepFaults)
                    fault = _stepFaults.Count switch
                    {
                        0 => null,
                        1 => _stepFaults[0],
                        _ => new AggregateException(_stepFaults),
                    };
                if (fault is null)
                    _stepDone.TrySetResult();
                else
                    _stepDone.TrySetException(fault);
            }
        }
    }

    private void RunWorkerStep(Worker worker)
    {
        var n = 0;
        while (n < _nodes.Length)
        {
            if (_loopStarts[n] is { } region)
            {
                RunLoop(worker, region);
                n = region.End + 1;
            }
            else
            {
                // A fused node's work happens inside its exchange's produce path; its slot
                // has no reader (fusion requires a single consumer, which is the chain). A
                // node heading one or more streamed exchanges' chains produces them from
                // its slot as soon as it has run.
                if (!_fused[n])
                {
                    RunNode(worker, n);
                    if (_produceAfter[n] is not null)
                        ProduceAfterNode(worker, n);
                }
                n++;
            }
        }
    }

    /// <summary>Waits on the exchange barrier, attributing the wait to the worker: the
    /// accumulated ticks feed the barrier-wait gauge, which is what separates
    /// synchronization cost from compute in a step. Accumulates even on the poison unwind
    /// so a faulting step does not under-report.</summary>
    private void BarrierWait(Worker worker)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            _barrier.SignalAndWait();
        }
        finally
        {
            worker.BarrierWaitTicks += Stopwatch.GetTimestamp() - started;
        }
    }

    /// <summary>Point-to-point exchange wait: acquire-spins until one sender's published
    /// sequence reaches <paramref name="expected"/>, attributing time to the same
    /// barrier-wait counter as barrier mode so the gauge means "synchronization wait" in
    /// both. The already-published fast path costs one volatile read and no timestamps.
    /// Waiters poll the poison flag so a faulted peer's never-coming publication unwinds
    /// the step (mirroring <see cref="SpinBarrier.Poison"/>) instead of hanging it. Past
    /// the spin budget the waiter parks on the exchange gate (see <see cref="PublishSeq"/>).
    /// Returns the published value, whose low bit carries the sender's "I shuffled data"
    /// flag — exactly this execution's, never a later one, because a sender's next
    /// publication is ordered behind this worker's completion of the current execution
    /// (the same handoff argument that makes mailbox reuse safe).</summary>
    private long AwaitPublication(Worker worker, long[] seqs, int slot, long expected)
    {
        var observed = Volatile.Read(ref seqs[slot]);
        if (observed >= expected)
            return observed;
        worker.SlowWaits++; // PHASE-STATS
        var started = Stopwatch.GetTimestamp();
        try
        {
            var spinner = new SpinWait();
            while ((observed = Volatile.Read(ref seqs[slot])) < expected)
            {
                if (Volatile.Read(ref _exchangePoisoned))
                    throw new BarrierPoisonedException();
                if (spinner.Count < _exchangeSpinBudget)
                {
                    // Pure spins, then yields — never Sleep(1), whose ≥1 ms oversleep
                    // would dwarf the waits it replaces.
                    spinner.SpinOnce(sleep1Threshold: -1);
                    continue;
                }
                return ParkUntilPublished(worker, seqs, slot, expected);
            }
            return observed;
        }
        finally
        {
            worker.BarrierWaitTicks += Stopwatch.GetTimestamp() - started;
        }
    }

    /// <summary>Publishes a point-to-point sequence or fragment counter and wakes parked
    /// waiters if there are any. The exchange is a full fence, which is what lets the
    /// parked-waiter read that follows it pair with the waiter's registration (a full fence
    /// too) — one of the two must see the other. The fast path — nobody parked, the normal
    /// state of a healthy step — costs the fence and one cache-line read.</summary>
    private void PublishSeq(long[] slots, int slot, long value)
    {
        Interlocked.Exchange(ref slots[slot], value);
        if (Volatile.Read(ref _parkedWaiters) != 0)
            lock (_exchangeGate)
                Monitor.PulseAll(_exchangeGate);
    }

    /// <summary>Parks until one sender's sequence reaches <paramref name="expected"/> or the
    /// exchange is poisoned. The condition is re-checked under the gate after registering,
    /// so a publication that raced the registration is seen here instead of slept
    /// through; every later publication pulses because it sees the registration.</summary>
    private long ParkUntilPublished(Worker worker, long[] seqs, int slot, long expected)
    {
        worker.ExchangeParks++;
        Interlocked.Increment(ref _parkedWaiters);
        try
        {
            lock (_exchangeGate)
            {
                long observed;
                while ((observed = Volatile.Read(ref seqs[slot])) < expected)
                {
                    if (Volatile.Read(ref _exchangePoisoned))
                        throw new BarrierPoisonedException();
                    Monitor.Wait(_exchangeGate);
                }
                return observed;
            }
        }
        finally
        {
            Interlocked.Decrement(ref _parkedWaiters);
        }
    }

    /// <summary>The streamed consume loop's park: until any not-yet-complete sender has a
    /// fragment this worker has not consumed or has published this execution's completion
    /// (either is progress for the loop), or the exchange is poisoned.</summary>
    private void ParkUntilStreamedProgress(Worker worker, int n, long expected)
    {
        worker.ExchangeParks++;
        Interlocked.Increment(ref _parkedWaiters);
        try
        {
            lock (_exchangeGate)
            {
                while (!StreamedProgressAvailable(worker, n, expected))
                {
                    if (Volatile.Read(ref _exchangePoisoned))
                        throw new BarrierPoisonedException();
                    Monitor.Wait(_exchangeGate);
                }
            }
        }
        finally
        {
            Interlocked.Decrement(ref _parkedWaiters);
        }
    }

    private bool StreamedProgressAvailable(Worker worker, int n, long expected)
    {
        var parts = _workers.Length;
        var seqs = _exchangeSeqs!;
        var fragSeqs = _fragSeqs!;
        var done = worker.SenderDone;
        var consumed = worker.FragConsumed;
        for (var s = 0; s < parts; s++)
        {
            if (done[s])
                continue;
            var slot = (n * parts + s) * SeqStride;
            if (Volatile.Read(ref fragSeqs[slot]) > consumed[n * parts + s] || Volatile.Read(ref seqs[slot]) >= expected)
                return true;
        }
        return false;
    }

    /// <summary>
    /// The consume half of a streamed exchange execution (fragment streaming;
    /// docs/design/fragment-streaming.md). Production — running the fused chain over the
    /// head's delta in chunks, partitioning each into the ring and release-publishing it under
    /// the cumulative per-(node, sender) counter, then the execution's completion seq — has
    /// already happened at the head's sweep position (<see cref="ProduceFromSlot"/> after the
    /// head ran, or forwarded fragment by fragment from an upstream exchange's consume loop or
    /// an emitting sink), except for a head inside a loop region, which is produced here from
    /// its slot. Every worker publishes before it waits, at a sweep position no later than
    /// any wait for it, which keeps the mesh deadlock-free. Consuming polls every sender (self
    /// included) and processes fragments in arrival order — forwarding them through the next
    /// exchange's chain, feeding the sole-consumer sink (taking its per-fragment output
    /// onward when it emits), or accumulating the partial merge — until every sender's
    /// completion seq for this execution is visible and everything it published is consumed.
    /// Completion is release-published after the sender's last counter write, so a counter
    /// read taken after seeing it is final. Idle polls flush the open forwarded fragment,
    /// check the poison flag and escalate to a park like <see cref="AwaitPublication"/>, and
    /// count toward the same barrier-wait gauge.
    /// </summary>
    private void RunStreamedExchange(Worker worker, int n, StreamPlan plan)
    {
        var slots = worker.Slots;
        var op = worker.Operators[n];
        var exchange = (IExchangeStepOperator)op;
        var parts = _workers.Length;
        var seqs = _exchangeSeqs!;
        var fragSeqs = _fragSeqs!;

        if (plan.Produce == ProduceMode.AtSelf)
        {
            var psP = PhaseStats ? Stopwatch.GetTimestamp() : 0; // PHASE-STATS
            ProduceFromSlot(worker, plan, slots[plan.Head]);
            CompleteProduce(worker, plan);
            if (PhaseStats) // PHASE-STATS
                worker.PartitionTicks += Stopwatch.GetTimestamp() - psP;
        }
        // Every other mode completed this node's production earlier in the sweep, bumping
        // the execution count then; the count is the seq every sender must have reached.
        var expected = worker.ExchangeSeq[n] << 1;

        var psT1 = PhaseStats ? Stopwatch.GetTimestamp() : 0; // PHASE-STATS

        var sink = plan.SinkNode >= 0 ? (IFragmentSinkOperator)worker.Operators[plan.SinkNode] : null;
        var emitInto = plan.SinkNode >= 0 ? _emitInto[plan.SinkNode] : null;
        var emitter = emitInto is null ? null : (IEmittingFragmentSinkOperator)sink!;
        var forwardTo = plan.ForwardTo;
        var flushInto = forwardTo ?? emitInto;
        var consumed = worker.FragConsumed;
        var done = worker.SenderDone;
        Array.Clear(done, 0, parts);
        var remaining = parts;
        var consumedAny = false;
        var waited = 0L;
        var waitStarted = 0L;
        var spinner = new SpinWait();
        while (remaining > 0)
        {
            var progressed = false;
            for (var s = 0; s < parts; s++)
            {
                if (done[s])
                    continue;
                var slot = (n * parts + s) * SeqStride;
                var cursor = n * parts + s;
                while (consumed[cursor] < Volatile.Read(ref fragSeqs[slot]))
                {
                    progressed = true;
                    var f = consumed[cursor]++;
                    worker.FragmentsConsumed++;
                    var fragment = plan.Rings[s][(int)(f % IExchangeStepOperator.FragmentRingSize)][worker.Id];
                    if (fragment is IZSet { IsEmpty: false })
                    {
                        consumedAny = true;
                        if (forwardTo is not null)
                        {
                            ForwardChunk(worker, forwardTo, fragment);
                        }
                        else if (emitter is not null)
                        {
                            ForwardChunk(worker, emitInto!, emitter.OnFragmentEmitting(plan.SinkInput, fragment));
                            worker.SinkPending[plan.SinkNode]++;
                        }
                        else if (sink is not null)
                        {
                            sink.OnFragment(plan.SinkInput, fragment);
                            worker.SinkPending[plan.SinkNode]++;
                        }
                        else
                        {
                            exchange.AccumulateFragment(fragment);
                        }
                    }
                }
                if (Volatile.Read(ref seqs[slot]) >= expected && consumed[cursor] >= Volatile.Read(ref fragSeqs[slot]))
                {
                    done[s] = true;
                    remaining--;
                    progressed = true;
                }
            }

            if (progressed)
            {
                if (waitStarted != 0)
                {
                    waited += Stopwatch.GetTimestamp() - waitStarted;
                    waitStarted = 0;
                    spinner = new SpinWait();
                }
                continue;
            }
            // About to idle: whatever has been forwarded so far is worth more to the
            // downstream consumers now than coalesced later.
            if (flushInto is not null)
                FlushOpenFragment(worker, flushInto);
            if (waitStarted == 0)
            {
                worker.SlowWaits++; // PHASE-STATS
                waitStarted = Stopwatch.GetTimestamp();
            }
            if (Volatile.Read(ref _exchangePoisoned))
            {
                worker.BarrierWaitTicks += waited + Stopwatch.GetTimestamp() - waitStarted;
                throw new BarrierPoisonedException();
            }
            // The same escalation as AwaitPublication: spins, then yields, then a park
            // that any publication wakes — the loop then re-polls every sender.
            if (spinner.Count < _exchangeSpinBudget)
                spinner.SpinOnce(sleep1Threshold: -1);
            else
                ParkUntilStreamedProgress(worker, n, expected);
        }
        if (waitStarted != 0)
            waited += Stopwatch.GetTimestamp() - waitStarted;
        worker.BarrierWaitTicks += waited;

        // Forwarding an exchange completes the next one's production here — nothing else
        // contributes to it. An emitting sink's completes after its own Step instead (its
        // non-streamed inputs still flow through Step), so only the open fragment is flushed.
        if (forwardTo is not null)
            CompleteProduce(worker, forwardTo);
        else if (emitInto is not null)
            FlushOpenFragment(worker, emitInto);

        // A sink consumed its fragments already and a forwarded exchange has no reader;
        // anything else takes the accumulated merge, or the canonical empty when nothing
        // shipped — which the downstream empty-input skip then propagates, exactly like the
        // plain path's empty-exchange skip.
        slots[n] = sink is null && forwardTo is null && consumedAny
            ? exchange.CompleteMerge()
            : op.EmptyStepResult!;
        if (PhaseStats) // PHASE-STATS
        {
            worker.MergeTicks += Stopwatch.GetTimestamp() - psT1 - waited;
            if (sink is null && forwardTo is null && !consumedAny)
                worker.EmptyExchangeSkips++;
        }
    }

    /// <summary>Produces a streamed exchange's fragments from a complete delta (its head's
    /// slot, or an emitting sink's <c>Step</c> output): split into chunks of about
    /// <see cref="CircuitOptions.StreamChunkRows"/>, bounded by what is left of the ring for
    /// this execution, and forwarded one by one. Does not complete the execution — the
    /// caller does, once nothing more can contribute.</summary>
    private void ProduceFromSlot(Worker worker, StreamPlan plan, object? delta)
    {
        if (delta is not IZSet { IsEmpty: false } head)
            return;
        var budget = IExchangeStepOperator.FragmentRingSize - worker.FragExecCount[plan.Node];
        var chunks = Math.Clamp(
            (head.DistinctCount + _options.StreamChunkRows - 1) / _options.StreamChunkRows, 1, budget);
        if (chunks == 1)
        {
            ForwardChunk(worker, plan, head);
            return;
        }
        var scratch = worker.ChunkScratch[plan.Node] ??= new IZSet?[IExchangeStepOperator.FragmentRingSize];
        head.SplitInto(scratch, chunks);
        for (var c = 0; c < chunks; c++)
            ForwardChunk(worker, plan, scratch[c]!);
    }

    /// <summary>Runs the exchange's fused chain over one chunk and partitions the result into
    /// the open ring slot — appending to whatever earlier chunks routed there since the last
    /// publication, so many small forwarded fragments coalesce into few published ones. The
    /// slot is published once it holds a chunk's worth of rows, unless this execution has
    /// used all but the last ring slot: the last one then absorbs everything until
    /// completion, so an execution never publishes more fragments than the ring holds. An
    /// all-empty result (a fused Where can filter a whole chunk away) routes nothing.</summary>
    private void ForwardChunk(Worker worker, StreamPlan plan, object chunk)
    {
        foreach (var m in plan.FusedChain)
        {
            var inputs = worker.NodeInputs[m];
            inputs[0] = chunk;
            chunk = worker.Operators[m].Step(inputs);
        }
        if (chunk is not IZSet { IsEmpty: false } rows)
            return;
        var x = plan.Node;
        var open = worker.FragOpenRows[x];
        ((IExchangeStepOperator)worker.Operators[x]).PartitionFragment(
            chunk, _workers.Length, (int)(worker.FragPublished[x] % IExchangeStepOperator.FragmentRingSize), append: open > 0);
        open += rows.DistinctCount;
        worker.FragOpenRows[x] = open;
        if (open >= _options.StreamChunkRows && worker.FragExecCount[x] < IExchangeStepOperator.FragmentRingSize - 1)
            PublishOpenFragment(worker, x);
    }

    /// <summary>Publishes the open fragment if it holds anything and the ring still has a
    /// slot to spare for it (see <see cref="ForwardChunk"/>).</summary>
    private void FlushOpenFragment(Worker worker, StreamPlan plan)
    {
        var x = plan.Node;
        if (worker.FragOpenRows[x] > 0 && worker.FragExecCount[x] < IExchangeStepOperator.FragmentRingSize - 1)
            PublishOpenFragment(worker, x);
    }

    /// <summary>Release-publishes the open ring slot under the next cumulative fragment
    /// counter value — its Z-sets were written before this, so the counter's release/acquire
    /// pair publishes them.</summary>
    private void PublishOpenFragment(Worker worker, int x)
    {
        var published = ++worker.FragPublished[x];
        PublishSeq(_fragSeqs!, (x * _workers.Length + worker.Id) * SeqStride, published);
        worker.FragExecCount[x]++;
        worker.FragOpenRows[x] = 0;
        worker.FragmentsPublished++;
    }

    /// <summary>Completes this worker's production of a streamed exchange for the step:
    /// publishes the open fragment (the ring always has a slot left for it), then the
    /// execution-count completion seq — the same publication as the plain point-to-point
    /// path, the low bit saying whether any fragment actually shipped.</summary>
    private void CompleteProduce(Worker worker, StreamPlan plan)
    {
        var x = plan.Node;
        if (worker.FragOpenRows[x] > 0)
            PublishOpenFragment(worker, x);
        var shipped = worker.FragExecCount[x] > 0;
        worker.FragExecCount[x] = 0;
        var expected = ++worker.ExchangeSeq[x] << 1;
        PublishSeq(_exchangeSeqs!, (x * _workers.Length + worker.Id) * SeqStride, shipped ? expected | 1 : expected);
        worker.ExchangeExecutions++; // PHASE-STATS
    }

    /// <summary>Production that runs right after a head node's own execution, from its now
    /// complete slot: every streamed exchange whose chain starts at this node, produced and
    /// completed here — as early in the sweep as its input exists, so peers can consume while
    /// this worker gets on with the nodes in between.</summary>
    private void ProduceAfterNode(Worker worker, int n)
    {
        var psT0 = PhaseStats ? Stopwatch.GetTimestamp() : 0; // PHASE-STATS
        var delta = worker.Slots[n];
        foreach (var plan in _produceAfter[n]!)
        {
            ProduceFromSlot(worker, plan, delta);
            CompleteProduce(worker, plan);
        }
        if (PhaseStats) // PHASE-STATS
            worker.PartitionTicks += Stopwatch.GetTimestamp() - psT0;
    }

    private void RunNode(Worker worker, int n)
    {
        var slots = worker.Slots;
        var node = _nodes[n];
        var op = worker.Operators[n];

        if (node.IsExchange && (_workers.Length > 1 || _cluster is not null))
        {
            if (_streamPlans[n] is { } plan)
            {
                RunStreamedExchange(worker, n, plan);
                return;
            }

            // Partition across the GLOBAL worker set (equal to the local one when not
            // clustered); the barrier makes every local worker's fragments visible.
            var exchange = (IExchangeStepOperator)op;
            var input = slots[node.Inputs[0]];
            var psT0 = PhaseStats ? Stopwatch.GetTimestamp() : 0; // PHASE-STATS
            _mailboxes[n][worker.Id] = exchange.Partition(input, _cluster?.GlobalWorkers ?? _workers.Length);
            if (PhaseStats) // PHASE-STATS
            {
                worker.PartitionTicks += Stopwatch.GetTimestamp() - psT0;
                worker.ExchangeExecutions++;
            }

            if (_exchangeSeqs is { } seqs)
            {
                // Point-to-point: release-publish this worker's fragments under its
                // execution count for the node, then wait for each peer's publication of
                // the same count — never for peers to arrive anywhere. A worker's own
                // count doubles as the expected sequence because every worker executes
                // every node the same number of times (loop iteration counts are a global
                // decision), and mailbox reuse is safe because a node's next execution is
                // always ordered behind every peer's consumption of the current one (the
                // step handoff between steps, the loop reduction barriers between
                // iterations).
                //
                // The published value is the count shifted up one, with the low bit set
                // when this worker actually shuffled data. In the sparse regime most
                // exchanges move nothing for anyone: when every sender's bit is clear the
                // whole shuffle — reading peers' mailbox lines, the merge — is skipped and
                // the node yields its canonical empty delta, which the ordinary
                // empty-input skip then propagates through the downstream segment.
                var count = ++worker.ExchangeSeq[n];
                var expected = count << 1;
                var shuffled = input is not IZSet { IsEmpty: true };
                PublishSeq(seqs, (n * _workers.Length + worker.Id) * SeqStride,
                    shuffled ? expected | 1 : expected);
                for (var sender = 0; sender < _workers.Length; sender++)
                    if (sender != worker.Id)
                        shuffled |= (AwaitPublication(worker, seqs,
                            (n * _workers.Length + sender) * SeqStride, expected) & 1) != 0;

                if (!shuffled && op.EmptyStepResult is { } nothing)
                {
                    if (PhaseStats) // PHASE-STATS
                        worker.EmptyExchangeSkips++;
                    slots[n] = nothing;
                    return;
                }
            }
            else
            {
                BarrierWait(worker);
            }

            if (_cluster is { } cluster)
            {
                // The designated worker ships the remote-bound shards and collects the
                // inbound ones while its local peers hold at the second barrier. Every
                // (step, node) sends exactly one frame to every peer — empty shards
                // included — so the per-peer frame sequence is dense and step/exchange
                // alignment never depends on data being present.
                if (worker.Id == 0)
                {
                    var step = _stepNumber;
                    for (var p = 0; p < cluster.ProcessCount; p++)
                        if (p != cluster.ProcessIndex)
                            cluster.SendShards(step, n, p,
                                exchange.SerializeShards(_mailboxes[n], p * _workers.Length, _workers.Length));
                    for (var p = 0; p < cluster.ProcessCount; p++)
                        if (p != cluster.ProcessIndex)
                            _remoteShards[n][p] = exchange.DeserializeShards(cluster.ReceiveShards(step, n, p), p);
                }
                BarrierWait(worker);
            }

            var psT1 = PhaseStats ? Stopwatch.GetTimestamp() : 0; // PHASE-STATS
            var myShard = (_cluster?.FirstGlobalWorker ?? 0) + worker.Id;
            var incoming = worker.ExchangeIncoming[n];
            var i = 0;
            for (var sender = 0; sender < _workers.Length; sender++)
                incoming[i++] = _mailboxes[n][sender][myShard];
            if (_cluster is { } c)
                for (var p = 0; p < c.ProcessCount; p++)
                    if (p != c.ProcessIndex)
                        incoming[i++] = _remoteShards[n][p][worker.Id];
            slots[n] = exchange.Merge(incoming);
            if (PhaseStats) // PHASE-STATS
                worker.MergeTicks += Stopwatch.GetTimestamp() - psT1;
        }
        else
        {
            // An operator whose every input is empty this step can usually be skipped
            // outright (see IStepOperator.EmptyStepResult). Worth the check: a single-row
            // change reaches one shard, so every other worker runs the whole downstream
            // circuit on nothing. Exchanges are never skipped — their barrier keeps the
            // workers aligned whether or not there is data to shuffle.
            var inputs = worker.NodeInputs[n];
            var allEmpty = node.Inputs.Length > 0;
            for (var i = 0; i < node.Inputs.Length; i++)
            {
                var value = slots[node.Inputs[i]];
                inputs[i] = value;
                allEmpty &= value is IZSet { IsEmpty: true };
            }
            // A fragment sink's streamed inputs arrive out of band, leaving their slots
            // empty — fragments fed since its last completion force the Step that emits.
            if (_streaming && worker.SinkPending[n] != 0)
            {
                worker.SinkPending[n] = 0;
                allEmpty = false;
            }
            slots[n] = allEmpty && op.EmptyStepResult is { } unchanged
                ? unchanged
                : op.Step(inputs);
        }
    }

    /// <summary>
    /// Runs a Recurse region to its fixed point for this step. Every worker executes the
    /// same iterations over the same nodes — exchanges inside the loop keep their barrier
    /// alignment because the continue/stop decision is a global reduction, not a local one:
    /// the loop stops only when no worker has a circulating delta or pending
    /// iteration-profile work. The feedback node hands each iteration's stabilized delta to
    /// the next; the exit node accumulates them into the step's outer delta.
    /// </summary>
    private void RunLoop(Worker worker, LoopRegion region)
    {
        var operators = worker.Operators;
        for (var n = region.Start; n <= region.End; n++)
            (operators[n] as ILoopOperator)?.BeginOuterStep();

        var feedback = (ILoopFeedbackOperator)operators[region.Start];
        var flags = _loopContinue[region.Index];

        for (var iteration = 0; ; iteration++)
        {
            for (var n = region.Start; n <= region.End; n++)
                (operators[n] as ILoopOperator)?.BeginIteration(iteration);

            for (var n = region.Start; n <= region.End; n++)
                RunNode(worker, n);

            feedback.SetPending(worker.Slots[region.ResultNode]);

            var more = feedback.PendingIsNonEmpty;
            for (var n = region.Start; n <= region.End && !more; n++)
                more = operators[n] is ILoopOperator op && op.HasPendingWork(iteration);

            if (_workers.Length > 1)
            {
                flags[worker.Id] = more;
                BarrierWait(worker); // own flag written before anyone reads
                more = false;
                for (var w = 0; w < flags.Length; w++)
                    more |= flags[w];
                BarrierWait(worker); // all flags read before anyone writes the next iteration's
            }

            if (!more)
                break;

            if (iteration + 1 >= region.MaxIterations)
                throw new InvalidOperationException(
                    $"Recurse did not reach a fixed point within {region.MaxIterations} iterations. " +
                    "The loop body may not converge (is it monotone?); otherwise raise maxIterations.");
        }

        for (var n = region.Start; n <= region.End; n++)
            (operators[n] as ILoopOperator)?.EndOuterStep();
    }

    /// <summary>Completes all inputs, drains and processes everything still buffered,
    /// completes the outputs, and stops the workers. Disposing a faulted circuit does not
    /// rethrow: the fault already surfaced on the faulting step, the handles, and
    /// <see cref="Fault"/> — disposal just skips draining and the final checkpoint commit
    /// (the faulting step must never be committed) and completes quietly.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        foreach (var input in _inputs)
            input.CompleteWriter();

        if (_pump is not null)
        {
            try
            {
                await _pump.ConfigureAwait(false);
            }
            catch
            {
                // Failures were already forwarded to the output channels.
            }
        }
        else if (_started && Volatile.Read(ref _fault) is null && _cluster is null)
        {
            // Clustered circuits skip this implicit draining: an extra step here could
            // never be matched by the peers (steps are lockstep) — run the final steps
            // explicitly on every process before disposing.
            try
            {
                while (_inputs.Any(p => p.HasPending))
                    await RunStepAsync(CancellationToken.None).ConfigureAwait(false);
                // As in the pump: flush watermark-driven emissions unlocked by the last step.
                if (_watermarkClock is not null)
                    await RunStepAsync(CancellationToken.None).ConfigureAwait(false);
                foreach (var output in _outputs)
                    output.Complete(null);
            }
            catch (Exception ex)
            {
                foreach (var output in _outputs)
                    output.Complete(ex);
            }
        }
        else
        {
            // Never started, faulted (the fault transition already completed the outputs
            // with the fault; TryComplete makes this a no-op then), or clustered (no
            // implicit steps — the user stepped every process explicitly).
            foreach (var output in _outputs)
                output.Complete(null);
        }

        // Steps still staged (CheckpointInterval > 1) commit on clean shutdown — but never
        // after a fault, which may have left a step half-staged across workers.
        if (_checkpoint is not null && Volatile.Read(ref _fault) is null && _stepsSinceCommit > 0)
        {
            try
            {
                CommitCheckpoint();
            }
            catch
            {
                // Shutdown must complete; the previous checkpoint remains the resume point.
            }
        }

        Volatile.Write(ref _stopping, true);
        foreach (var worker in _workers)
            worker.Go.Release();
        foreach (var worker in _workers)
            worker.Thread?.Join(TimeSpan.FromSeconds(1));

        if (_cluster is not null)
            await _cluster.DisposeAsync().ConfigureAwait(false);

        if (PhaseStats && _psSteps > 0) // PHASE-STATS
        {
            double Us(long ticks) => ticks * 1e6 / Stopwatch.Frequency / _psSteps;
            var exchanges = _nodes.Count(n => n.IsExchange);
            Console.Error.WriteLine(
                $"[phase-stats] {Name}: steps {_psSteps}, nodes {_nodes.Length}, exchanges {exchanges}, workers {_workers.Length}" +
                (_streaming
                    ? $", streamed {_streamPlans.Count(p => p is not null)} (forwarded {_streamPlans.Count(p => p?.Produce == ProduceMode.Forwarded)}" +
                      $", sinks {_streamPlans.Count(p => p?.SinkNode >= 0)}, emitting {_emitInto.Count(p => p is not null)})"
                    : ""));
            Console.Error.WriteLine(
                $"[phase-stats]   coord us/step: drain {Us(_psDrain):F1} workerPhase {Us(_psWorkerPhase):F1} " +
                $"(launch {Us(_psLaunch):F1} land {Us(_psLand):F1}) checkpoint {Us(_psCheckpoint):F1} " +
                $"publish {Us(_psPublish):F1} total {Us(_psTotal):F1}");
            foreach (var w in _workers)
                Console.Error.WriteLine(
                    $"[phase-stats]   w{w.Id} us/step: span {Us(w.SpanTicks):F1} wait {Us(w.BarrierWaitTicks):F1} " +
                    $"partition {Us(w.PartitionTicks):F1} merge {Us(w.MergeTicks):F1} " +
                    $"compute {Us(w.SpanTicks - w.BarrierWaitTicks - w.PartitionTicks - w.MergeTicks):F1} " +
                    $"slowWaits/step {(double)w.SlowWaits / _psSteps:F1} of {(double)w.ExchangeExecutions * Math.Max(0, _workers.Length - 1) / _psSteps:F1} " +
                    $"emptySkips/step {(double)w.EmptyExchangeSkips / _psSteps:F1} of {(double)w.ExchangeExecutions / _psSteps:F1} " +
                    $"parks/step {(double)w.ExchangeParks / _psSteps:F2}" +
                    (_streaming ? $" frags/step {(double)w.FragmentsPublished / _psSteps:F1} out {(double)w.FragmentsConsumed / _psSteps:F1} in" : ""));
        }

        _shutdown.Cancel();
        _shutdown.Dispose();
        _diag.Dispose();
        _log.LogInformation("Circuit {Circuit} disposed after {Steps} steps", Name, _stepNumber);
    }
}
