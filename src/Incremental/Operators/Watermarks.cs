namespace Incremental.Operators;

/// <summary>
/// The circuit's event-time clock. Each <c>WithWatermark</c> call registers a tracker;
/// workers record the max event time they have seen per tracker during a step, and the
/// coordinator freezes the global watermark between steps as
/// min over trackers of (max event time seen − allowed lateness), monotonically
/// non-decreasing. Freezing between steps means every worker sees the same watermark for
/// the whole of a step, so shard-local eviction decisions are consistent.
/// </summary>
internal sealed class WatermarkClock(int workers)
{
    private sealed class Tracker(long latenessTicks, long[] maxSeenTicks)
    {
        public long LatenessTicks { get; } = latenessTicks;
        public long[] MaxSeenTicks { get; } = maxSeenTicks;
    }

    private readonly List<Tracker> _trackers = [];
    private long _frozenTicks = long.MinValue;

    /// <summary>Watermark as of the end of the previous step (ticks, UTC); <see cref="long.MinValue"/> until every source has seen data.</summary>
    public long FrozenTicks => Volatile.Read(ref _frozenTicks);

    public int RegisterTracker(long latenessTicks)
    {
        var slots = new long[workers];
        Array.Fill(slots, long.MinValue);
        _trackers.Add(new Tracker(latenessTicks, slots));
        return _trackers.Count - 1;
    }

    public void Observe(int tracker, int worker, long eventTicks)
    {
        ref var slot = ref _trackers[tracker].MaxSeenTicks[worker];
        if (eventTicks > slot)
            Volatile.Write(ref slot, eventTicks);
    }

    /// <summary>Called by the coordinator between steps, after all workers finish.</summary>
    public void Freeze()
    {
        var candidate = long.MaxValue;
        foreach (var tracker in _trackers)
        {
            var maxSeen = long.MinValue;
            for (var w = 0; w < tracker.MaxSeenTicks.Length; w++)
                maxSeen = Math.Max(maxSeen, Volatile.Read(ref tracker.MaxSeenTicks[w]));
            if (maxSeen == long.MinValue)
                return; // a source has seen nothing yet: no watermark can be claimed
            candidate = Math.Min(candidate, maxSeen - tracker.LatenessTicks);
        }
        if (_trackers.Count > 0 && candidate > _frozenTicks)
            Volatile.Write(ref _frozenTicks, candidate);
    }
}

/// <summary>
/// Observes event times for the clock and drops records older than the current watermark.
/// Dropping here is what makes downstream eviction safe: state for a closed key can never
/// be needed again, because nothing that would touch it can get past this operator.
/// </summary>
internal sealed class WatermarkOperator<T>(
    OperatorContext context,
    WatermarkClock clock,
    int tracker,
    Func<T, DateTimeOffset> eventTime) : IStepOperator
    where T : notnull
{
    private readonly int _worker = context.WorkerId;

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<T>)inputs[0]!;
        var watermark = clock.FrozenTicks;
        var output = new ZSet<T>();
        var maxSeen = long.MinValue;

        foreach (var (value, weight) in delta)
        {
            var ticks = eventTime(value).UtcTicks;
            maxSeen = Math.Max(maxSeen, ticks);
            if (ticks >= watermark)
                output.Add(value, weight);
        }

        if (maxSeen != long.MinValue)
            clock.Observe(tracker, _worker, maxSeen);
        return output;
    }
}

/// <summary>An eviction policy attached to a keyed stream: when the watermark passes
/// <c>ClosesAt(key)</c>, stateful operators may drop everything they hold for that key.</summary>
internal sealed class KeyEviction<TKey>(WatermarkClock clock, Func<TKey, DateTimeOffset> closesAt)
    where TKey : notnull
{
    public WatermarkClock Clock { get; } = clock;
    public Func<TKey, DateTimeOffset> ClosesAt { get; } = closesAt;
}

/// <summary>Per-operator, per-worker deadline queue driving state eviction.</summary>
internal sealed class EvictionTracker<TKey>(KeyEviction<TKey> policy) where TKey : notnull
{
    private readonly PriorityQueue<TKey, long> _deadlines = new();
    private readonly HashSet<TKey> _tracked = [];

    public void Track(TKey key)
    {
        if (_tracked.Add(key))
            _deadlines.Enqueue(key, policy.ClosesAt(key).UtcTicks);
    }

    /// <summary>Removes and returns every tracked key whose deadline the watermark has passed,
    /// as one list so callers can evict their state in a single batched store call.</summary>
    public List<TKey> DrainClosed()
    {
        var watermark = policy.Clock.FrozenTicks;
        var closed = new List<TKey>();
        while (_deadlines.TryPeek(out var key, out var deadline) && deadline <= watermark)
        {
            _deadlines.Dequeue();
            _tracked.Remove(key);
            closed.Add(key);
        }
        return closed;
    }
}
