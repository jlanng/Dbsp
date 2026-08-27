using Incremental.Storage;

namespace Incremental.Operators;

/// <summary>
/// Event-time session windows: each key's events are grouped into sessions separated by at
/// least <c>gap</c> of inactivity. Unlike tumbling/hopping windows this is not a pure
/// flat-map — session assignment is stateful: an insertion can extend a session or merge two
/// neighbours (a bridging or late event), and a retraction can shrink or split one. The
/// operator keeps the integrated events per key; when a step touches a key it re-sessionizes
/// that key's events (touched keys only — never a full rescan, matching how the non-linear
/// aggregate recomputes touched groups), retracting the previous session rows and asserting
/// the new ones — rows whose bounds did not change cancel in the output Z-set, so the net
/// delta is only what actually moved. A session spans [first event, last event + gap);
/// consecutive events belong to the same session iff they are strictly less than the gap
/// apart, so an event exactly at a session's end starts a new one — which keeps sessionization
/// consistent with eviction: an admitted event is at or above the watermark, so it can never
/// merge into a key evicted at (last event + gap) &lt;= watermark. When the circuit has a
/// watermark, a key's state is dropped once the watermark passes its last event + gap. That
/// deadline moves with the session as events arrive, unlike the static
/// <see cref="KeyEviction{TKey}"/> policies, so the operator tracks it itself (stale queue
/// entries are skipped lazily). The input is partitioned by key, so state and eviction
/// decisions are worker-local and consistent (the watermark is frozen for the whole step).
/// </summary>
internal sealed class SessionWindowOperator<TKey, TValue> : IStepOperator
    where TKey : notnull
    where TValue : notnull
{
    private readonly IIndexedWeightStore<TKey, TValue> _events;
    private readonly Func<TValue, DateTimeOffset> _eventTime;
    private readonly long _gapTicks;
    private readonly WatermarkClock? _clock;
    private readonly PriorityQueue<TKey, long> _deadlines = new();
    private readonly Dictionary<TKey, long> _currentDeadline = [];

    public SessionWindowOperator(
        OperatorContext context,
        long gapTicks,
        Func<TValue, DateTimeOffset> eventTime,
        WatermarkClock? clock)
    {
        _events = context.Storage.CreateIndexedWeightStore<TKey, TValue>(context.StateName("events"));
        _eventTime = eventTime;
        _gapTicks = gapTicks;
        _clock = clock;
    }

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<KeyedValue<TKey, TValue>>)inputs[0]!;
        var output = new ZSet<KeyedValue<TKey, Windowed<TValue>>>();

        var entries = new List<(TKey, TValue, long)>(delta.DistinctCount);
        var touched = new List<TKey>();
        var deltasByKey = new Dictionary<TKey, List<(TValue Value, long Weight)>>();
        foreach (var ((key, value), weight) in delta)
        {
            entries.Add((key, value, weight));
            if (!deltasByKey.TryGetValue(key, out var forKey))
            {
                deltasByKey[key] = forKey = [];
                touched.Add(key);
            }
            forKey.Add((value, weight));
        }

        // The retractions must reflect the state as it stood before this delta, so the
        // touched groups are read first, then the delta is persisted in one batched call.
        var oldGroups = _events.GetGroups(touched);
        _events.UpdateAll(entries);

        for (var i = 0; i < touched.Count; i++)
        {
            var key = touched[i];
            var oldGroup = oldGroups[i];

            var oldItems = new List<(long Ticks, TValue Value, long Weight)>(oldGroup.Count);
            foreach (var (value, weight) in oldGroup)
                oldItems.Add((_eventTime(value).UtcTicks, value, weight));

            var merged = new Dictionary<TValue, long>(oldGroup.Count);
            foreach (var (value, weight) in oldGroup)
                merged[value] = weight;
            foreach (var (value, weight) in deltasByKey[key])
            {
                var updated = merged.GetValueOrDefault(value) + weight;
                if (updated == 0)
                    merged.Remove(value);
                else
                    merged[value] = updated;
            }

            var newItems = new List<(long Ticks, TValue Value, long Weight)>(merged.Count);
            foreach (var (value, weight) in merged)
                newItems.Add((_eventTime(value).UtcTicks, value, weight));

            // Retract the previous sessionization, assert the new one; unchanged rows cancel.
            Sessionize(key, oldItems, output, sign: -1);
            Sessionize(key, newItems, output, sign: +1);
            TrackDeadline(key, newItems);
        }

        // Closed keys can never be touched again (their events are dropped at the
        // watermark, and anything admitted is at least a gap away from every evicted
        // event), so their state is deleted without emitting retractions: session rows
        // already published for them simply remain final.
        if (_clock is not null)
            EvictClosed();

        return output;
    }

    /// <summary>Partitions <paramref name="items"/> (sorted here by event time) into sessions —
    /// consecutive items strictly less than the gap apart share one — and adds each item's
    /// session row to <paramref name="output"/> with its weight times <paramref name="sign"/>.</summary>
    private void Sessionize(
        TKey key,
        List<(long Ticks, TValue Value, long Weight)> items,
        ZSet<KeyedValue<TKey, Windowed<TValue>>> output,
        int sign)
    {
        if (items.Count == 0)
            return;

        items.Sort(static (a, b) => a.Ticks.CompareTo(b.Ticks));
        var sessionStart = 0;
        for (var i = 1; i <= items.Count; i++)
        {
            if (i < items.Count && items[i].Ticks - items[i - 1].Ticks < _gapTicks)
                continue;

            var start = new DateTimeOffset(items[sessionStart].Ticks, TimeSpan.Zero);
            var end = new DateTimeOffset(items[i - 1].Ticks + _gapTicks, TimeSpan.Zero);
            for (var j = sessionStart; j < i; j++)
                output.Add(
                    new KeyedValue<TKey, Windowed<TValue>>(key, new Windowed<TValue>(start, end, items[j].Value)),
                    sign * items[j].Weight);
            sessionStart = i;
        }
    }

    /// <summary>Re-tracks the key's eviction deadline (last event time + gap) after a touch.
    /// <paramref name="newItems"/> is sorted by <see cref="Sessionize"/> before this runs.</summary>
    private void TrackDeadline(TKey key, List<(long Ticks, TValue Value, long Weight)> newItems)
    {
        if (_clock is null)
            return;

        if (newItems.Count == 0)
        {
            _currentDeadline.Remove(key); // fully retracted; stale queue entries are skipped lazily
            return;
        }

        var deadline = newItems[^1].Ticks + _gapTicks;
        if (_currentDeadline.TryGetValue(key, out var existing) && existing == deadline)
            return;
        _currentDeadline[key] = deadline;
        _deadlines.Enqueue(key, deadline);
    }

    private void EvictClosed()
    {
        var watermark = _clock!.FrozenTicks;
        List<TKey>? closed = null;
        while (_deadlines.TryPeek(out var key, out var deadline) && deadline <= watermark)
        {
            _deadlines.Dequeue();
            if (_currentDeadline.TryGetValue(key, out var current) && current == deadline)
            {
                _currentDeadline.Remove(key);
                (closed ??= []).Add(key);
            }
            // Otherwise the entry is stale: the key was touched again (or fully retracted)
            // after this deadline was queued.
        }
        if (closed is { Count: > 0 })
            _events.RemoveGroups(closed);
    }
}
