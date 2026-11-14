using Incremental.Runtime;
using Incremental.Storage;

namespace Incremental.Operators;

/// <summary>
/// The circuit's event-time clock. Each <c>WithEventTime</c> call registers a tracker;
/// workers record the max event time they have seen per tracker during a step, and the
/// coordinator freezes the global watermark between steps as
/// min over trackers of (max event time seen − allowed lateness), monotonically
/// non-decreasing. Freezing between steps means every worker sees the same watermark for
/// the whole of a step, so shard-local eviction decisions are consistent.
/// <para>
/// The clock is checkpointed: every commit carries the frozen watermark and each source's
/// max event time in the offsets marker (<see cref="AppendCheckpoint"/>), and a circuit
/// that resumes puts them back before its first step (<see cref="Restore"/>). So the
/// watermark never regresses across a restart or a takeover — a record that was late
/// before it is late after it — and the first step after resumes from exactly where the
/// committed step left off.
/// </para>
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

    /// <summary>Watermark as of the end of the previous step (ticks, UTC);
    /// <see cref="long.MinValue"/> until every source has seen data.</summary>
    public long FrozenTicks => Volatile.Read(ref _frozenTicks);

    public int RegisterTracker(long latenessTicks)
    {
        var slots = new long[workers];
        Array.Fill(slots, long.MinValue);
        _trackers.Add(new Tracker(latenessTicks, slots));
        _sourceKeys.Add(OffsetKeys.WatermarkSource(_trackers.Count - 1));
        return _trackers.Count - 1;
    }

    /// <summary>How many <c>WithEventTime</c> sources feed the clock.</summary>
    public int Sources => _trackers.Count;

    private readonly List<string> _sourceKeys = [];

    /// <summary>Adds the clock's state to a checkpoint's offsets marker: the frozen
    /// watermark, and each source's max event time seen (the maximum across workers — which
    /// worker saw it does not matter to <see cref="Freeze"/>). A value still unset is left
    /// out. Called by the coordinator between steps, after <see cref="Freeze"/>.</summary>
    public void AppendCheckpoint(Dictionary<string, long> offsets)
    {
        if (_frozenTicks != long.MinValue)
            offsets[OffsetKeys.Watermark] = _frozenTicks;
        for (var t = 0; t < _trackers.Count; t++)
        {
            var maxSeen = long.MinValue;
            var slots = _trackers[t].MaxSeenTicks;
            for (var w = 0; w < slots.Length; w++)
                maxSeen = Math.Max(maxSeen, Volatile.Read(ref slots[w]));
            if (maxSeen != long.MinValue)
                offsets[_sourceKeys[t]] = maxSeen;
        }
    }

    /// <summary>Puts the clock back to what <see cref="AppendCheckpoint"/> wrote, and to
    /// nothing where it wrote nothing. Called before the first step of a circuit that
    /// starts on a checkpointing provider, so whatever an earlier, failed start of the same
    /// definition observed is gone too.</summary>
    public void Restore(IReadOnlyDictionary<string, long> offsets)
    {
        Volatile.Write(ref _frozenTicks, offsets.GetValueOrDefault(OffsetKeys.Watermark, long.MinValue));
        for (var t = 0; t < _trackers.Count; t++)
        {
            var slots = _trackers[t].MaxSeenTicks;
            Array.Fill(slots, long.MinValue);
            Volatile.Write(ref slots[0], offsets.GetValueOrDefault(_sourceKeys[t], long.MinValue));
        }
    }

    public void Observe(int tracker, int worker, long eventTicks)
    {
        ref var slot = ref _trackers[tracker].MaxSeenTicks[worker];
        if (eventTicks > slot)
            Volatile.Write(ref slot, eventTicks);
    }

    /// <summary>Cluster support: this process's max event time seen per tracker
    /// (<see cref="long.MinValue"/> where a source has seen nothing), in tracker
    /// registration order — deterministic, since every process builds the same circuit.
    /// Collected by the coordinator after all local workers finish a step.</summary>
    public long[] CollectMaxSeen()
    {
        var result = new long[_trackers.Count];
        for (var t = 0; t < _trackers.Count; t++)
        {
            var maxSeen = long.MinValue;
            var slots = _trackers[t].MaxSeenTicks;
            for (var w = 0; w < slots.Length; w++)
                maxSeen = Math.Max(maxSeen, Volatile.Read(ref slots[w]));
            result[t] = maxSeen;
        }
        return result;
    }

    /// <summary>Cluster support: folds a peer process's <see cref="CollectMaxSeen"/> in
    /// before <see cref="Freeze"/>, so the frozen watermark is the same global min-reduction
    /// on every process.</summary>
    public void ObserveRemote(long[] maxSeenPerTracker)
    {
        if (maxSeenPerTracker.Length != _trackers.Count)
            throw new InvalidOperationException(
                "Cluster desync: a peer process reported a different number of watermark sources; " +
                "every process must run the same circuit definition.");
        for (var t = 0; t < maxSeenPerTracker.Length; t++)
            if (maxSeenPerTracker[t] != long.MinValue)
                Observe(t, 0, maxSeenPerTracker[t]);
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
/// A negative <c>tracker</c> is a re-declaration (<c>WithEventTime</c> without a lateness):
/// it drops, and observes nothing.
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
    private readonly RecordErrors? _errors = context.Errors;
    private readonly OutputBuffer<T> _output = new();

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<T>)inputs[0]!;
        var watermark = clock.FrozenTicks;
        var output = _output.Next(delta.DistinctCount);
        var maxSeen = long.MinValue;

        if (_errors is not null)
            maxSeen = StepGuarded(delta, output, watermark, _errors);
        else
            foreach (var (value, weight) in delta)
            {
                var ticks = eventTime(value).UtcTicks;
                maxSeen = Math.Max(maxSeen, ticks);
                if (ticks >= watermark)
                    output.Add(value, weight);
            }

        // A re-declaration (no tracker of its own) filters on the watermark and feeds nothing.
        if (maxSeen != long.MinValue && tracker >= 0)
            clock.Observe(tracker, _worker, maxSeen);
        return output;
    }

    // The dead-letter policy's loop; see MapOperator.StepGuarded. A record whose event time
    // cannot be read never reaches the clock, so it cannot move the watermark either.
    private long StepGuarded(ZSet<T> delta, ZSet<T> output, long watermark, RecordErrors errors)
    {
        var maxSeen = long.MinValue;
        foreach (var (value, weight) in delta)
        {
            long ticks;
            try
            {
                ticks = eventTime(value).UtcTicks;
            }
            catch (Exception ex)
            {
                errors.Add(value, weight, ex);
                continue;
            }
            maxSeen = Math.Max(maxSeen, ticks);
            if (ticks >= watermark)
                output.Add(value, weight);
        }
        return maxSeen;
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

/// <summary>
/// An eviction policy per record rather than per key (<c>EvictWhenFinal</c>): a record is
/// final once the circuit's watermark has passed its own event time, and an operator that
/// holds it may then forget it. Sound for the same reason <see cref="KeyEviction{TKey}"/>
/// is: the <see cref="WatermarkOperator{T}"/> upstream drops every record whose event time
/// is behind the watermark — the record's retraction and every later version of it
/// included — so nothing that would touch a final record can arrive. The watermark keeps
/// a record with ticks at or above it and <see cref="IDeadlineStore{TKey}.TakeDue"/> takes
/// a deadline at or below it, hence one tick after the event time.
/// </summary>
internal sealed class RecordEviction<TValue>(WatermarkClock clock, Func<TValue, DateTimeOffset> eventTime)
    where TValue : notnull
{
    public WatermarkClock Clock { get; } = clock;

    public DateTimeOffset ClosesAt(TValue value) => eventTime(value).AddTicks(1);

    /// <summary>Whether <paramref name="value"/> is final as of the running step's
    /// watermark: an operator that applies a table change before it drains its deadlines
    /// leaves such a record alone, so that a change corrects exactly the records whose
    /// event time is at or after the watermark, whenever the eviction itself runs.</summary>
    public bool IsFinal(TValue value) => eventTime(value).UtcTicks < Clock.FrozenTicks;

    /// <summary>The same policy for an operator whose state holds records under a key.</summary>
    public KeyEviction<KeyedValue<TKey, TValue>> ForKeyed<TKey>() where TKey : notnull =>
        new(Clock, record => ClosesAt(record.Value));
}

/// <summary>
/// The facts of a retroactive table join, forgotten one by one as they become final
/// (<see cref="RecordEviction{TValue}"/>). The deadlines are an
/// <see cref="EvictionTracker{TKey}"/> keyed by (key, fact), so they are checkpointed with
/// the facts; a deadline outlives its fact only until it falls due, which bounds them by
/// the same horizon as the facts.
/// </summary>
internal sealed class FactRetention<TKey, TFact>
    where TKey : notnull
    where TFact : notnull
{
    private readonly RecordEviction<TFact> _policy;
    private readonly EvictionTracker<KeyedValue<TKey, TFact>> _tracker;
    private readonly IIndexedWeightStore<TKey, TFact> _facts;

    public FactRetention(OperatorContext context, RecordEviction<TFact> policy, IIndexedWeightStore<TKey, TFact> facts)
    {
        _policy = policy;
        _tracker = new EvictionTracker<KeyedValue<TKey, TFact>>(context, policy.ForKeyed<TKey>());
        _facts = facts;
    }

    public bool IsFinal(TFact fact) => _policy.IsFinal(fact);

    public void Track(TKey key, TFact fact) => _tracker.Track(new KeyedValue<TKey, TFact>(key, fact));

    /// <summary>Once per step, on every way out, after the last <see cref="Track"/>.</summary>
    public void EvictFinal()
    {
        if (_tracker.DrainClosed() is { Count: > 0 } closed)
            _facts.RemoveEntries(closed);
    }
}

/// <summary>
/// Per-operator, per-worker deadlines driving state eviction. The deadlines are a store of
/// the operator's (<c>{node}.deadlines.w{n}</c>), so they are checkpointed with the state
/// they bound: a circuit restored from a checkpoint evicts exactly the keys an
/// uninterrupted run would have, including the ones nothing touches again.
/// </summary>
internal sealed class EvictionTracker<TKey> where TKey : notnull
{
    private readonly WatermarkClock _clock;
    private readonly IDeadlineStore<TKey> _deadlines;
    private readonly Func<TKey, long> _deadlineOf;

    // The keys this step's delta rows carried, repeats and all, handed to the store in one
    // call when the step drains: the store finds the new ones with the probe a set of
    // tracked keys would have cost here, and a provider with per-call latency pays one
    // round trip a step instead of one a row.
    private readonly List<TKey> _touched = [];
    private readonly List<TKey> _closed = [];

    public EvictionTracker(OperatorContext context, KeyEviction<TKey> policy)
    {
        _clock = policy.Clock;
        _deadlines = context.Storage.CreateDeadlineStore<TKey>(context.StateName("deadlines"));
        var closesAt = policy.ClosesAt;
        _deadlineOf = key => closesAt(key).UtcTicks;
    }

    public void Track(TKey key) => _touched.Add(key);

    /// <summary>Drops the deadline of a key the operator holds nothing for any more. For
    /// an operator whose tracked keys are not the grouping's but something that comes and
    /// goes under one (the remembered lookup tracks facts): without it every key ever
    /// tracked keeps a deadline, and a record in the changelog, until its group closes.
    /// A key forgotten and tracked again later gets its deadline again.</summary>
    public void Forget(TKey key) => _deadlines.Remove(key);

    /// <summary>Removes and returns every tracked key whose deadline the watermark has
    /// passed, as one list so callers can evict their state in a single batched store call.
    /// Every operator calls this once per step, on every way out of the step, after its
    /// last <see cref="Track"/>. The list is this tracker's own, valid until the next
    /// call; callers read it and never mutate it.</summary>
    public List<TKey> DrainClosed()
    {
        if (_touched.Count > 0)
        {
            _deadlines.AddAll(_touched, _deadlineOf);
            _touched.Clear();
        }
        _closed.Clear();
        _deadlines.TakeDue(_clock.FrozenTicks, _closed);
        return _closed;
    }
}
