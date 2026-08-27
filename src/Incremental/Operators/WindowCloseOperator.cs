namespace Incremental.Operators;

/// <summary>
/// Window-close notifications: announces each key of its keyed input exactly once, with
/// weight +1, in the first step whose watermark has passed the key's deadline. "Closed"
/// means exactly what eviction means — the watermark guarantees no event for the key can
/// get past <c>WithWatermark</c> anymore, so the key's results are final. Notifications are
/// therefore never retracted (the watermark is monotone). The input is partitioned by key,
/// so each key is tracked and announced by the one worker that owns it — exactly once even
/// with several workers. Retractions track a key like insertions do: a window whose contents
/// were fully retracted still closes (it closes by time, not by contents).
/// </summary>
internal sealed class WindowCloseOperator<TKey, TValue>(KeyEviction<TKey> policy) : IStepOperator
    where TKey : notnull
    where TValue : notnull
{
    private readonly EvictionTracker<TKey> _deadlines = new(policy);

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<KeyedValue<TKey, TValue>>)inputs[0]!;
        foreach (var ((key, _), _) in delta)
            _deadlines.Track(key);

        var output = new ZSet<KeyedValue<TKey, DateTimeOffset>>();
        foreach (var key in _deadlines.DrainClosed())
            output.Add(new KeyedValue<TKey, DateTimeOffset>(key, policy.ClosesAt(key)), 1);
        return output;
    }
}
