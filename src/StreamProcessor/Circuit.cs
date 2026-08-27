using StreamProcessor.Operators;
using StreamProcessor.Runtime;

namespace StreamProcessor;

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
    private readonly Barrier _barrier;
    private readonly WatermarkClock? _watermarkClock;

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
        _barrier = new Barrier(options.Workers);

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

        _stepDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Volatile.Write(ref _workersRemaining, _workers.Length);
        foreach (var worker in _workers)
            worker.Go.Release();

        await _stepDone.Task.ConfigureAwait(false);

        // Freeze between steps so every worker sees one consistent watermark per step.
        _watermarkClock?.Freeze();

        foreach (var output in _outputs)
            await output.PublishAsync(_workers, cancellationToken).ConfigureAwait(false);
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

        Volatile.Write(ref _stopping, true);
        foreach (var worker in _workers)
            worker.Go.Release();
        foreach (var worker in _workers)
            worker.Thread?.Join(TimeSpan.FromSeconds(1));

        _shutdown.Cancel();
        _shutdown.Dispose();
        _barrier.Dispose();
    }
}
