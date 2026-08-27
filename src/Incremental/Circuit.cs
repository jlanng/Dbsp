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
    private readonly IInputPort[] _inputs;
    private readonly IOutputPort[] _outputs;
    private readonly Worker[] _workers;
    private readonly SpinBarrier _barrier;
    private readonly WatermarkClock? _watermarkClock;

    // Single-worker circuits run their steps inline on the coordinator: no worker thread,
    // no handoff — the cheapest engine for small or latency-sensitive workloads.
    private bool RunsInline => _workers.Length == 1;

    // Exchange scratch space: _mailboxes[node][sender] holds the fragments (indexed by
    // target worker) that `sender` produced for that exchange node this step. The exchange
    // barrier makes writes visible before anyone reads.
    private readonly object[][][] _mailboxes;

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
    private bool _faulted;

    internal Circuit(
        NodeDefinition[] nodes,
        IReadOnlyList<InputRegistration> inputRegistrations,
        IReadOnlyList<OutputRegistration> outputRegistrations,
        CircuitOptions options,
        WatermarkClock? watermarkClock)
    {
        _nodes = nodes;
        _options = options;
        _watermarkClock = watermarkClock;
        _checkpoint = options.Storage as ICheckpointStorageProvider;
        _barrier = new SpinBarrier(options.Workers);

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
                    Storage = options.Storage,
                    NodeName = nodes[n].Name,
                };
                operators[n] = nodes[n].Factory(context);
            }
            _workers[w] = new Worker(w, operators, new object?[nodes.Length]);
        }

        _mailboxes = new object[nodes.Length][][];
        for (var n = 0; n < nodes.Length; n++)
            _mailboxes[n] = nodes[n].IsExchange
                ? Enumerable.Range(0, options.Workers).Select(_ => Array.Empty<object>()).ToArray()
                : [];

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
            var offsets = _checkpoint.Recover();
            foreach (var input in _inputs)
                if (offsets.TryGetValue(input.Name, out var offset))
                    input.SeedOffset(offset);
            Volatile.Write(ref _committedOffsets, new Dictionary<string, long>(offsets));
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
        foreach (var input in _inputs)
            input.DrainInto(_workers, _options.MaxStepItemsPerInput);

        try
        {
            if (RunsInline)
            {
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
        }
        catch
        {
            // A fault can leave the step's writes half-staged across workers; they must
            // never reach a checkpoint, so commits (the final one on dispose included)
            // stop here. The previous checkpoint remains the resume point.
            _faulted = true;
            throw;
        }

        // Freeze between steps so every worker sees one consistent watermark per step.
        _watermarkClock?.Freeze();

        // Commit before publishing: durability should not wait on a slow output consumer.
        // Output deltas are transient (consumers integrate them) and not checkpointed.
        if (_checkpoint is not null && ++_stepsSinceCommit >= _options.CheckpointInterval)
            CommitCheckpoint();

        foreach (var output in _outputs)
            await output.PublishAsync(_workers, cancellationToken).ConfigureAwait(false);
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

        _checkpoint!.Commit(offsets);
        _stepsSinceCommit = 0;
        Volatile.Write(ref _committedOffsets, offsets);
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
                if (Interlocked.Decrement(ref _workersRemaining) == 0)
                    _stepDone.TrySetResult();
            }
            catch (Exception ex)
            {
                // A faulted worker poisons the circuit: it leaves the exchange barrier so
                // peers don't deadlock mid-shuffle, and the failure surfaces on the pending
                // step. The circuit must be disposed after a fault.
                // TODO: cooperative fault propagation so peers finish the step cleanly.
                _barrier.RemoveParticipant();
                _stepDone.TrySetException(ex);
                return;
            }
        }
    }

    private void RunWorkerStep(Worker worker)
    {
        var slots = worker.Slots;
        for (var n = 0; n < _nodes.Length; n++)
        {
            var node = _nodes[n];
            var op = worker.Operators[n];

            if (node.IsExchange && _workers.Length > 1)
            {
                var exchange = (IExchangeStepOperator)op;
                _mailboxes[n][worker.Id] = exchange.Partition(slots[node.Inputs[0]], _workers.Length);

                _barrier.SignalAndWait();

                var incoming = new object?[_workers.Length];
                for (var sender = 0; sender < _workers.Length; sender++)
                    incoming[sender] = _mailboxes[n][sender][worker.Id];
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
    }

    /// <summary>Completes all inputs, drains and processes everything still buffered,
    /// completes the outputs, and stops the workers.</summary>
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
        else if (_started)
        {
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
            foreach (var output in _outputs)
                output.Complete(null);
        }

        // Steps still staged (CheckpointInterval > 1) commit on clean shutdown — but never
        // after a fault, which may have left a step half-staged across workers.
        if (_checkpoint is not null && !_faulted && _stepsSinceCommit > 0)
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

        _shutdown.Cancel();
        _shutdown.Dispose();
    }
}
