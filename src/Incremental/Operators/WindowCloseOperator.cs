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
internal sealed class WindowCloseOperator<TKey, TValue>(KeyEviction<TKey> policy) : IStepOperator
    where TKey : notnull
    where TValue : notnull
{
    // Own deadline queue rather than an EvictionTracker: the notification carries the
    // deadline, so ClosesAt runs once per key and its result is reused at close.
    private readonly PriorityQueue<(TKey Key, DateTimeOffset ClosesAt), long> _deadlines = new();
    private readonly HashSet<TKey> _tracked = [];
    private readonly OutputBuffer<KeyedValue<TKey, DateTimeOffset>> _output = new();

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<KeyedValue<TKey, TValue>>)inputs[0]!;
        foreach (var ((key, _), _) in delta)
        {
            if (_tracked.Add(key))
            {
                var closesAt = policy.ClosesAt(key);
                _deadlines.Enqueue((key, closesAt), closesAt.UtcTicks);
            }
        }

        var watermark = policy.Clock.FrozenTicks;
        var output = _output.Next(0);
        while (_deadlines.TryPeek(out var entry, out var deadline) && deadline <= watermark)
        {
            _deadlines.Dequeue();
            _tracked.Remove(entry.Key);
            output.Add(new KeyedValue<TKey, DateTimeOffset>(entry.Key, entry.ClosesAt), 1);
        }
        return output;
    }
}
