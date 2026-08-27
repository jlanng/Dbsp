using System.Threading.Channels;
using Incremental.Operators;

namespace Incremental.Runtime;

internal abstract class InputRegistration
{
    public required string Name { get; init; }
    public required int NodeId { get; init; }
    public abstract IInputPort CreatePort(CircuitOptions options);
}

internal sealed class InputRegistration<T> : InputRegistration where T : notnull
{
    public override IInputPort CreatePort(CircuitOptions options) => new InputPort<T>(Name, NodeId, options);
}

internal sealed class KeyedInputRegistration<TKey, TValue> : InputRegistration
    where TKey : notnull
    where TValue : notnull
{
    public required Func<TValue, TKey> KeySelector { get; init; }

    public override IInputPort CreatePort(CircuitOptions options) =>
        new InputPort<TValue>(Name, NodeId, options,
            writer => new KeyedInputHandle<TKey, TValue>(new InputHandle<TValue>(writer), KeySelector));
}

internal interface IInputPort
{
    string Name { get; }
    object Handle { get; }
    bool HasPending { get; }
    bool IsCompleted { get; }
    Task<bool> WaitToReadAsync(CancellationToken cancellationToken);
    void DrainInto(Worker[] workers, int maxItems);
    void CompleteWriter();
}

internal sealed class InputPort<T> : IInputPort where T : notnull
{
    private readonly Channel<WeightedValue<T>> _channel;
    private readonly int _nodeId;

    public InputPort(string name, int nodeId, CircuitOptions options,
        Func<ChannelWriter<WeightedValue<T>>, object>? handleFactory = null)
    {
        Name = name;
        _nodeId = nodeId;
        _channel = Channel.CreateBounded<WeightedValue<T>>(new BoundedChannelOptions(options.InputChannelCapacity)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
        Handle = handleFactory?.Invoke(_channel.Writer) ?? new InputHandle<T>(_channel.Writer);
    }

    public string Name { get; }
    public object Handle { get; }
    public bool HasPending => _channel.Reader.TryPeek(out _);
    public bool IsCompleted => _channel.Reader.Completion.IsCompleted;

    public Task<bool> WaitToReadAsync(CancellationToken cancellationToken) =>
        _channel.Reader.WaitToReadAsync(cancellationToken).AsTask();

    /// <summary>Moves up to <paramref name="maxItems"/> buffered records into the workers'
    /// input operators for the next step, round-robin. Keyed operators re-shard via exchange
    /// anyway, so initial placement only needs to spread load.</summary>
    public void DrainInto(Worker[] workers, int maxItems)
    {
        ZSet<T>?[]? buffers = null;
        var target = 0;
        var drained = 0;
        while (drained < maxItems && _channel.Reader.TryRead(out var entry))
        {
            buffers ??= new ZSet<T>?[workers.Length];
            (buffers[target] ??= new ZSet<T>()).Add(entry.Value, entry.Weight);
            target = (target + 1) % workers.Length;
            drained++;
        }

        if (buffers is null)
            return;

        for (var i = 0; i < workers.Length; i++)
            if (buffers[i] is { } batch)
                ((InputOperator<T>)workers[i].Operators[_nodeId]).Enqueue(batch);
    }

    public void CompleteWriter() => _channel.Writer.TryComplete();
}

internal abstract class OutputRegistration
{
    public required string Name { get; init; }
    public required int NodeId { get; init; }
    public abstract IOutputPort CreatePort(CircuitOptions options);
}

internal sealed class OutputRegistration<T> : OutputRegistration where T : notnull
{
    public override IOutputPort CreatePort(CircuitOptions options) => new OutputPort<T>(Name, NodeId, options);
}

internal interface IOutputPort
{
    string Name { get; }
    object Handle { get; }
    ValueTask PublishAsync(Worker[] workers, CancellationToken cancellationToken);
    void Complete(Exception? error);
}

internal sealed class OutputPort<T> : IOutputPort where T : notnull
{
    private readonly Channel<ZSet<T>> _channel;
    private readonly int _nodeId;

    public OutputPort(string name, int nodeId, CircuitOptions options)
    {
        Name = name;
        _nodeId = nodeId;
        _channel = Channel.CreateBounded<ZSet<T>>(new BoundedChannelOptions(options.OutputChannelCapacity)
        {
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
        Handle = new OutputHandle<T>(_channel.Reader);
    }

    public string Name { get; }
    public object Handle { get; }

    /// <summary>Merges the per-worker deltas of this output's node for the step just run and,
    /// if non-empty, publishes them. Awaiting the bounded write is what propagates a slow
    /// consumer back into the circuit as backpressure.</summary>
    public async ValueTask PublishAsync(Worker[] workers, CancellationToken cancellationToken)
    {
        ZSet<T>? merged = null;
        foreach (var worker in workers)
        {
            if (worker.Slots[_nodeId] is ZSet<T> { IsEmpty: false } delta)
            {
                merged ??= new ZSet<T>();
                merged.AddAll(delta);
            }
        }

        if (merged is { IsEmpty: false })
            await _channel.Writer.WriteAsync(merged, cancellationToken).ConfigureAwait(false);
    }

    public void Complete(Exception? error) => _channel.Writer.TryComplete(error);
}
