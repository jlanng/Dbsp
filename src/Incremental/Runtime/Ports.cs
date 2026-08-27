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

    /// <summary>The offset of the last record consumed into a step: the producer's source
    /// offset when records are stamped, a running record count otherwise. -1 when nothing
    /// has been consumed (and no checkpoint was recovered).</summary>
    long ConsumedOffset { get; }

    Task<bool> WaitToReadAsync(CancellationToken cancellationToken);

    /// <summary>Seeds the input's position from a recovered checkpoint, before any step runs:
    /// replayed records stamped at or below it are dropped, and unstamped records continue
    /// the numbering from it.</summary>
    void SeedOffset(long offset);

    void DrainInto(Worker[] workers, int maxItems);
    void CompleteWriter();

    /// <summary>Completes the input's channel with <paramref name="error"/> (the fault path):
    /// producers blocked on a full channel are released with it, and further sends fail fast
    /// instead of filling a channel no step will ever drain again.</summary>
    void Fail(Exception error);
}

internal sealed class InputPort<T> : IInputPort where T : notnull
{
    private readonly Channel<InputEntry<T>> _channel;
    private readonly int _nodeId;
    private long _offset = -1;

    public InputPort(string name, int nodeId, CircuitOptions options,
        Func<ChannelWriter<InputEntry<T>>, object>? handleFactory = null)
    {
        Name = name;
        _nodeId = nodeId;
        _channel = Channel.CreateBounded<InputEntry<T>>(new BoundedChannelOptions(options.InputChannelCapacity)
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
    public long ConsumedOffset => _offset;

    public Task<bool> WaitToReadAsync(CancellationToken cancellationToken) =>
        _channel.Reader.WaitToReadAsync(cancellationToken).AsTask();

    public void SeedOffset(long offset) => _offset = offset;

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
            drained++;
            if (entry.Offset >= 0)
            {
                // Stamped records must advance the position: one at or below it — in
                // particular one replayed after a restart, when the position starts at the
                // committed checkpoint — is a duplicate and is dropped.
                if (entry.Offset <= _offset)
                    continue;
                _offset = entry.Offset;
            }
            else
            {
                _offset++; // unstamped records are numbered as they are consumed
            }

            buffers ??= new ZSet<T>?[workers.Length];
            (buffers[target] ??= new ZSet<T>()).Add(entry.Value, entry.Weight);
            target = (target + 1) % workers.Length;
        }

        if (buffers is null)
            return;

        for (var i = 0; i < workers.Length; i++)
            if (buffers[i] is { } batch)
                ((InputOperator<T>)workers[i].Operators[_nodeId]).Enqueue(batch);
    }

    public void CompleteWriter() => _channel.Writer.TryComplete();

    public void Fail(Exception error) => _channel.Writer.TryComplete(error);
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
