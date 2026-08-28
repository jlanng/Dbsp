using Incremental.Cluster;
using Incremental.Operators;
using Incremental.Runtime;
using Incremental.Storage;

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

    // Cross-process exchange (null when not clustered): the runtime shipping remote-bound
    // shards, plus scratch space _remoteShards[node][peerProcess] holding the shards each
    // peer sent this step (indexed by local worker), filled by the designated worker
    // between the two exchange barriers. _stepNumber stamps every frame so a desync faults
    // loudly instead of silently merging the wrong step's data.
    private readonly ClusterRuntime? _cluster;
    private readonly object[][][] _remoteShards;
    private long _stepNumber;
    private long _committedStepMarker = -1;

    private readonly CancellationTokenSource _shutdown = new();
    private TaskCompletionSource _stepDone = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _workersRemaining;
    private bool _stopping;
    private bool _started;
    private bool _disposed;
    private Task? _pump;

    // Checkpointing: staged operator state and the input offsets it reflects are committed
    // together, every CheckpointInterval steps, once all workers' shards for those steps
    // are complete — one commit point for the whole circuit.
    private readonly ICheckpointStorageProvider? _checkpoint;
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

    internal Circuit(
        NodeDefinition[] nodes,
        LoopRegion[] loopRegions,
        IReadOnlyList<InputRegistration> inputRegistrations,
        IReadOnlyList<OutputRegistration> outputRegistrations,
        CircuitOptions options,
        WatermarkClock? watermarkClock)
    {
        _nodes = nodes;
        _options = options;

        _loopStarts = new LoopRegion?[nodes.Length];
        foreach (var region in loopRegions)
            _loopStarts[region.Start] = region;
        _loopContinue = loopRegions.Select(_ => new bool[options.Workers]).ToArray();
        _watermarkClock = watermarkClock;
        _checkpoint = options.Storage as ICheckpointStorageProvider;
        _barrier = new SpinBarrier(options.Workers);
        _cluster = options.Cluster is { } cluster ? new ClusterRuntime(cluster.Transport, options.Workers, _barrier) : null;

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
                };
                operators[n] = nodes[n].Factory(context);
            }
            _workers[w] = new Worker(w, operators, new object?[nodes.Length]);
        }

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

        _inputs = inputRegistrations.Select(r => r.CreatePort(options)).ToArray();
        _outputs = outputRegistrations.Select(r => r.CreatePort(options)).ToArray();
    }

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
            foreach (var input in _inputs)
                if (offsets.TryGetValue(input.Name, out var offset))
                    input.SeedOffset(offset);
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

    public InputHandle<T> Input<T>(string name) where T : notnull
    {
        var port = _inputs.FirstOrDefault(p => p.Name == name)
            ?? throw new KeyNotFoundException($"No input named '{name}'.");
        return port.Handle as InputHandle<T>
            ?? throw new InvalidOperationException($"Input '{name}' does not carry elements of type {typeof(T).Name}.");
    }

    public KeyedInputHandle<TKey, TValue> KeyedInput<TKey, TValue>(string name)
        where TKey : notnull
        where TValue : notnull
    {
        var port = _inputs.FirstOrDefault(p => p.Name == name)
            ?? throw new KeyNotFoundException($"No input named '{name}'.");
        return port.Handle as KeyedInputHandle<TKey, TValue>
            ?? throw new InvalidOperationException(
                $"Input '{name}' is not a keyed input of ({typeof(TKey).Name}, {typeof(TValue).Name}); declare it with CircuitBuilder.KeyedInput.");
    }

    public OutputHandle<T> Output<T>(string name) where T : notnull
    {
        var port = _outputs.FirstOrDefault(p => p.Name == name)
            ?? throw new KeyNotFoundException($"No output named '{name}'.");
        return port.Handle as OutputHandle<T>
            ?? throw new InvalidOperationException($"Output '{name}' does not carry elements of type {typeof(T).Name}.");
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
        foreach (var input in _inputs)
            input.DrainInto(_workers, _options.MaxStepItemsPerInput);

        try
        {
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
                _stepDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                Volatile.Write(ref _workersRemaining, _workers.Length);
                foreach (var worker in _workers)
                    worker.Go.Release();

                await _stepDone.Task.ConfigureAwait(false);
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
            if (_checkpoint is not null && ++_stepsSinceCommit >= _options.CheckpointInterval)
                CommitCheckpoint();
        }
        catch (Exception ex)
        {
            FaultCircuit(ex);
            throw;
        }

        foreach (var output in _outputs)
            await output.PublishAsync(_workers, cancellationToken).ConfigureAwait(false);
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
        Interlocked.CompareExchange(ref _fault, exception, null);
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
        var offsets = new Dictionary<string, long>(_inputs.Length);
        foreach (var input in _inputs)
            if (input.ConsumedOffset >= 0)
                offsets[input.Name] = input.ConsumedOffset;

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
    }

    private const string StepMarkerKey = "$step";

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
    /// offsets at or below it are dropped as duplicates.</summary>
    public long? CommittedOffset(string inputName)
    {
        if (_inputs.All(p => p.Name != inputName))
            throw new KeyNotFoundException($"No input named '{inputName}'.");
        return Volatile.Read(ref _committedOffsets).TryGetValue(inputName, out var offset) ? offset : null;
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

            foreach (var output in _outputs)
                output.Complete(null);
        }
        catch (Exception ex)
        {
            foreach (var output in _outputs)
                output.Complete(ex is OperationCanceledException ? null : ex);
        }
    }

    private void WorkerLoop(Worker worker)
    {
        while (true)
        {
            worker.Go.Wait();
            if (Volatile.Read(ref _stopping))
                return;

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
                _barrier.Poison();
                // Remote peers must not wait on shards this worker will never ship, and the
                // designated worker may be blocked on inbound cluster traffic.
                _cluster?.Poison(ex);
            }

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
                RunNode(worker, n);
                n++;
            }
        }
    }

    private void RunNode(Worker worker, int n)
    {
        var slots = worker.Slots;
        var node = _nodes[n];
        var op = worker.Operators[n];

        if (node.IsExchange && (_workers.Length > 1 || _cluster is not null))
        {
            // Partition across the GLOBAL worker set (equal to the local one when not
            // clustered); the barrier makes every local worker's fragments visible.
            var exchange = (IExchangeStepOperator)op;
            _mailboxes[n][worker.Id] = exchange.Partition(slots[node.Inputs[0]], _cluster?.GlobalWorkers ?? _workers.Length);

            _barrier.SignalAndWait();

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
                            _remoteShards[n][p] = exchange.DeserializeShards(cluster.ReceiveShards(step, n, p));
                }
                _barrier.SignalAndWait();
            }

            var myShard = (_cluster?.FirstGlobalWorker ?? 0) + worker.Id;
            var peerCount = _cluster is null ? 0 : _cluster.ProcessCount - 1;
            var incoming = new object?[_workers.Length + peerCount];
            var i = 0;
            for (var sender = 0; sender < _workers.Length; sender++)
                incoming[i++] = _mailboxes[n][sender][myShard];
            if (_cluster is { } c)
                for (var p = 0; p < c.ProcessCount; p++)
                    if (p != c.ProcessIndex)
                        incoming[i++] = _remoteShards[n][p][worker.Id];
            slots[n] = exchange.Merge(incoming);
        }
        else
        {
            var inputs = new object?[node.Inputs.Length];
            for (var i = 0; i < node.Inputs.Length; i++)
                inputs[i] = slots[node.Inputs[i]];
            slots[n] = op.Step(inputs);
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
                _barrier.SignalAndWait(); // own flag written before anyone reads
                more = false;
                for (var w = 0; w < flags.Length; w++)
                    more |= flags[w];
                _barrier.SignalAndWait(); // all flags read before anyone writes the next iteration's
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

        _shutdown.Cancel();
        _shutdown.Dispose();
    }
}
