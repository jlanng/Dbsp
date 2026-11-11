namespace Incremental.Operators;

/// <summary>
/// Window-close notifications: announces each key of its keyed input exactly once, with
/// weight +1, in the first step whose watermark has passed the key's deadline. "Closed"
/// means exactly what eviction means — the watermark guarantees no event for the key can
/// get past <c>WithEventTime</c> anymore, so the key's results are final. Notifications are
/// therefore never retracted (the watermark is monotone). The input is partitioned by key,
/// so each key is tracked and announced by the one worker that owns it — exactly once even
/// with several workers. Retractions track a key like insertions do: a window whose contents
/// were fully retracted still closes (it closes by time, not by contents).
/// </summary>
internal sealed class WindowCloseOperator<TKey, TValue> : IStepOperator
    where TKey : notnull
    where TValue : notnull
{
    // The keys still to announce are state: a window open at a restart must still be
    // announced when it closes, though nothing touches it again. So they are held in a
    // deadline store, through the same tracker that drives eviction.
    private readonly KeyEviction<TKey> _policy;
    private readonly EvictionTracker<TKey> _open;
    private readonly OutputBuffer<KeyedValue<TKey, DateTimeOffset>> _output = new();

    public WindowCloseOperator(OperatorContext context, KeyEviction<TKey> policy)
    {
        _policy = policy;
        _open = new EvictionTracker<TKey>(context, policy);
    }

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<KeyedValue<TKey, TValue>>)inputs[0]!;
        foreach (var ((key, _), _) in delta)
            _open.Track(key);

        var closed = _open.DrainClosed();
        var output = _output.Next(closed.Count);
        foreach (var key in closed)
            output.Add(new KeyedValue<TKey, DateTimeOffset>(key, _policy.ClosesAt(key)), 1);
        return output;
    }
}
