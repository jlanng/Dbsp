using Incremental.Runtime;
using Incremental.Storage;

namespace Incremental.Operators;

/// <summary>
/// The circuit's event-time clock. Each <c>WithEventTime</c> call registers a tracker;
/// workers record the max event time they have seen per tracker during a step, and the
/// coordinator freezes the global watermark between steps as
/// min over trackers of (the tracker's event time − allowed lateness), monotonically
/// non-decreasing. Freezing between steps means every worker sees the same watermark for
/// the whole of a step, so shard-local eviction decisions are consistent.
/// <para>
/// <b>Partitions.</b> A tracker's event time is the newest it has seen — unless its input
/// feeds it by partition (<see cref="InputEventTime{T}"/>: event time declared
/// on the input itself, and a source that names its partitions). Then each named
/// partition keeps a max of its own and the tracker's time is the <i>least</i> over the
/// partitions that are not idle: a partition that is behind the others holds the
/// watermark where its own records are, so they are not late. A partition named and not
/// yet heard from holds every claim back. One the source calls idle stops counting until
/// its next record; with every partition idle the tracker's time is the newest seen
/// anywhere, as without partitions. Only the coordinator touches the partitions, between
/// steps.
/// </para>
/// <para>
/// The clock is checkpointed: every commit carries the frozen watermark, each source's
/// max event time and each named partition's in the offsets marker
/// (<see cref="AppendCheckpoint"/>), and a circuit that resumes puts them back before its
/// first step (<see cref="Restore"/>). So the watermark never regresses across a restart
/// or a takeover — a record that was late before it is late after it — and the first
/// step after resumes from exactly where the committed step left off.
/// </para>
/// </summary>
internal sealed class WatermarkClock(int workers)
{
    private sealed class PartitionTime
    {
        public long MaxSeenTicks = long.MinValue;
        public bool Idle;
    }

    private sealed class Tracker(long latenessTicks, long[] maxSeenTicks)
    {
        public long LatenessTicks { get; } = latenessTicks;
        public long[] MaxSeenTicks { get; } = maxSeenTicks;

        /// <summary>The partitions the input's source has named, in partition order (the
        /// checkpoint's keys are written in it). Null until one is named.</summary>
        public SortedDictionary<int, PartitionTime>? Partitions;

        /// <summary>The newest event time the input has observed for this tracker, in any
        /// partition or none: what <see cref="CircuitOptions.MaxEventTimeAhead"/> is
        /// measured from. Kept only for a tracker its input feeds.</summary>
        public long NewestTicks = long.MinValue;

        public string? InputName;
    }

    private readonly List<Tracker> _trackers = [];
    private long _frozenTicks = long.MinValue;

    // Records dropped as late, per worker: each written by its worker only.
    private readonly long[] _late = new long[workers];

    /// <summary>Counts <paramref name="count"/> records an event-time declaration on
    /// <paramref name="worker"/> dropped for being behind the watermark.</summary>
    public void NoteLate(int worker, long count) => Volatile.Write(ref _late[worker], _late[worker] + count);

    /// <summary>Records dropped as late since the circuit was built, over all workers.
    /// Not part of the checkpoint: a count for people, of this run.</summary>
    public long LateRecords
    {
        get
        {
            long total = 0;
            for (var w = 0; w < _late.Length; w++)
                total += Volatile.Read(ref _late[w]);
            return total;
        }
    }

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
    /// watermark, each source's max event time seen (the maximum across workers — which
    /// worker saw it does not matter to <see cref="Freeze"/>), and each named partition's
    /// max and whether it is idle (<see cref="OffsetKeys.WatermarkPartition"/>). A source's value still unset is
    /// left out; a partition named and not yet heard from is written, as
    /// <see cref="long.MinValue"/>, because that it was named is state.
    /// Called by the coordinator between steps, after <see cref="Freeze"/>.</summary>
    public void AppendCheckpoint(Dictionary<string, long> offsets, bool partitionTimes = true)
    {
        if (_frozenTicks != long.MinValue)
            offsets[OffsetKeys.Watermark] = _frozenTicks;
        for (var t = 0; t < _trackers.Count; t++)
        {
            var maxSeen = MaxAcrossWorkers(_trackers[t]);
            if (_trackers[t].Partitions is { } partitions)
            {
                if (partitionTimes)
                {
                    foreach (var (partition, time) in partitions)
                    {
                        offsets[OffsetKeys.WatermarkPartition(t, partition, idleFlag: false)] = time.MaxSeenTicks;
                        offsets[OffsetKeys.WatermarkPartition(t, partition, idleFlag: true)] = time.Idle ? 1 : 0;
                    }
                }
                else
                {
                    // Below format level 2 the checkpoint has one time per source, and a
                    // build of that level takes it for the newest event time the source
                    // has read from any partition: that is what it is given. Such a build
                    // resumes as it always did; this one, resuming the same checkpoint,
                    // holds the watermark where it was committed until its source has
                    // named the partitions again and each has brought a record or is idle.
                    foreach (var (_, time) in partitions)
                        maxSeen = Math.Max(maxSeen, time.MaxSeenTicks);
                }
            }
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
            var tracker = _trackers[t];
            var slots = tracker.MaxSeenTicks;
            Array.Fill(slots, long.MinValue);
            Volatile.Write(ref slots[0], offsets.GetValueOrDefault(_sourceKeys[t], long.MinValue));
            tracker.Partitions = null;
            tracker.NewestTicks = slots[0];
        }

        // The partitions' keys are found by what they are, exactly: a source's key, then
        // '#', which no input's name may hold (see OffsetKeys).
        foreach (var (key, ticks) in offsets)
        {
            if (!OffsetKeys.TryDecodeWatermarkPartition(key, out var source, out var partition, out var idleFlag)
                || source >= _trackers.Count)
                continue;
            var tracker = _trackers[source];
            var partitions = tracker.Partitions ??= [];
            if (!partitions.TryGetValue(partition, out var time))
                partitions[partition] = time = new PartitionTime();
            if (idleFlag)
                time.Idle = ticks != 0;
            else
            {
                time.MaxSeenTicks = ticks;
                tracker.NewestTicks = Math.Max(tracker.NewestTicks, ticks);
            }
        }
    }

    public void Observe(int tracker, int worker, long eventTicks)
    {
        ref var slot = ref _trackers[tracker].MaxSeenTicks[worker];
        if (eventTicks > slot)
            Volatile.Write(ref slot, eventTicks);
    }

    // ── fed by the input, between steps, on the coordinator ────────────────────────────

    /// <summary>The source reads <paramref name="partition"/> and its records may be on
    /// their way: it has a time of its own from here and holds the tracker's back until
    /// it has brought a record or is idle. Said of an idle partition, it counts again.</summary>
    public void ExpectPartition(int tracker, int partition)
    {
        var partitions = _trackers[tracker].Partitions ??= [];
        if (partitions.TryGetValue(partition, out var time))
            time.Idle = false;
        else
            partitions[partition] = new PartitionTime();
    }

    /// <summary>The source stands at the end of <paramref name="partition"/>: it stops
    /// holding the tracker's time back until its next record.</summary>
    public void IdlePartition(int tracker, int partition)
    {
        var partitions = _trackers[tracker].Partitions ??= [];
        if (partitions.TryGetValue(partition, out var time))
            time.Idle = true;
        else
            partitions[partition] = new PartitionTime { Idle = true };
    }

    /// <summary>One record's event time, seen by the input as it drains: into its
    /// partition's time if the source has named the partition (which is then no longer
    /// idle), else into the tracker's own, with the records of no partition.</summary>
    public void ObserveFromInput(int tracker, int partition, long eventTicks)
    {
        var state = _trackers[tracker];
        if (eventTicks > state.NewestTicks)
            state.NewestTicks = eventTicks;
        if (partition >= 0 && state.Partitions is { } partitions && partitions.TryGetValue(partition, out var time))
        {
            time.Idle = false;
            if (eventTicks > time.MaxSeenTicks)
                time.MaxSeenTicks = eventTicks;
            return;
        }
        Observe(tracker, 0, eventTicks);
    }

    /// <summary>The newest event time the input has observed for
    /// <paramref name="tracker"/>, this run or before the checkpoint it resumed;
    /// <see cref="long.MinValue"/> before the first.</summary>
    public long NewestFromInput(int tracker) => _trackers[tracker].NewestTicks;

    private static long MaxAcrossWorkers(Tracker tracker)
    {
        var maxSeen = long.MinValue;
        var slots = tracker.MaxSeenTicks;
        for (var w = 0; w < slots.Length; w++)
            maxSeen = Math.Max(maxSeen, Volatile.Read(ref slots[w]));
        return maxSeen;
    }

    /// <summary>The tracker's event time, or <see cref="long.MinValue"/> while it can
    /// claim none: the least over its partitions that are not idle — none at all if one
    /// of them has brought nothing yet — and, with no such partition, the newest seen in
    /// any partition or outside them. <paramref name="least"/> is the partition that
    /// time is, or −1.</summary>
    private static long TimeOf(Tracker tracker, out int least)
    {
        least = -1;
        var newest = MaxAcrossWorkers(tracker);
        if (tracker.Partitions is not { } partitions)
            return newest;

        var leastTicks = long.MaxValue;
        foreach (var (partition, time) in partitions)
        {
            if (time.Idle)
                newest = Math.Max(newest, time.MaxSeenTicks);
            else if (time.MaxSeenTicks < leastTicks)
                (leastTicks, least) = (time.MaxSeenTicks, partition);
        }
        return least < 0 ? newest : leastTicks;
    }

    /// <summary>Cluster support: this process's max event time seen per tracker
    /// (<see cref="long.MinValue"/> where a source has seen nothing), in tracker
    /// registration order — deterministic, since every process builds the same circuit.
    /// Collected by the coordinator after all local workers finish a step.</summary>
    public long[] CollectMaxSeen()
    {
        var result = new long[_trackers.Count];
        for (var t = 0; t < _trackers.Count; t++)
            result[t] = MaxAcrossWorkers(_trackers[t]);
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
        long heldBy = -1;
        for (var t = 0; t < _trackers.Count; t++)
        {
            var tracker = _trackers[t];
            var time = TimeOf(tracker, out var partition);
            if (time == long.MinValue)
            {
                // A source, or a partition of one, has seen nothing yet: no watermark can
                // be claimed, and that is what the clock waits for.
                Volatile.Write(ref _heldBy, ((long)t << 32) | (uint)(partition + 1));
                return;
            }
            if (time - tracker.LatenessTicks < candidate)
                (candidate, heldBy) = (time - tracker.LatenessTicks, ((long)t << 32) | (uint)(partition + 1));
        }
        Volatile.Write(ref _heldBy, heldBy);
        if (_trackers.Count > 0 && candidate > _frozenTicks)
            Volatile.Write(ref _frozenTicks, candidate);
    }

    // Which source, and which of its partitions (−1: none), the last freeze found
    // lowest, packed into one word so that a reader on another thread sees a pair that
    // belongs together. −1 before the first freeze that could claim a watermark.
    private long _heldBy = -1;

    /// <summary>The source whose time was the lowest at the last freeze — the one the
    /// watermark waits for — and the partition of it that was, or −1 where the source
    /// has no partitions or all of them are idle. Null before any watermark.</summary>
    public (int Source, int Partition)? HeldBy =>
        Volatile.Read(ref _heldBy) is var held && held < 0 ? null : ((int)(held >> 32), (int)(held & 0xFFFFFFFF) - 1);

    /// <summary>The name of the input that feeds <paramref name="source"/> by itself
    /// (event time declared on the input), or null.</summary>
    public string? InputOf(int source) => _trackers[source].InputName;

    public void BindInput(int tracker, string inputName) => _trackers[tracker].InputName = inputName;
}

/// <summary>
/// Observes event times for the clock and drops records older than the current watermark,
/// counting the insertions it drops (<see cref="WatermarkClock.LateRecords"/>): a record is
/// late by its time alone, whether or not the circuit has seen it before.
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
        long late = 0;

        if (_errors is not null)
            maxSeen = StepGuarded(delta, output, watermark, _errors, ref late);
        else
            foreach (var (value, weight) in delta)
            {
                var ticks = eventTime(value).UtcTicks;
                maxSeen = Math.Max(maxSeen, ticks);
                if (ticks >= watermark)
                    output.Add(value, weight);
                else if (weight > 0)
                    late += weight;
            }

        // A re-declaration (no tracker of its own) filters on the watermark and feeds nothing.
        if (maxSeen != long.MinValue && tracker >= 0)
            clock.Observe(tracker, _worker, maxSeen);
        // What was dropped is counted — insertions only: the retraction of a record that is
        // final is the other half of an amendment whose insertion is counted, or of a delete.
        if (late > 0)
            clock.NoteLate(_worker, late);
        return output;
    }

    // The dead-letter policy's loop; see MapOperator.StepGuarded. A record whose event time
    // cannot be read never reaches the clock, so it cannot move the watermark either.
    private long StepGuarded(ZSet<T> delta, ZSet<T> output, long watermark, RecordErrors errors, ref long late)
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
            else if (weight > 0)
                late += weight;
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

    /// <summary>For a keyed input that forgets its final rows: how long a row stays open
    /// behind the newest event time — the lateness of the input's declaration.</summary>
    public long OpenTicks { get; init; }

    /// <summary>
    /// Whether an upsert with event time <paramref name="nextTicks"/> begins a new record
    /// of a key whose row has event time <paramref name="previousTicks"/>, rather than
    /// amending that row: it does when its own time puts the earlier row behind the
    /// watermark — when it is later by more than the rows stay open. The earlier row is
    /// then final by the later one's own time, whatever the circuit's watermark says at
    /// the step the two meet in: it is not taken back, and the two are never in one step
    /// (the input's port sees to that). So what a key's records give does not depend on
    /// whether the watermark had already passed the earlier one when the later arrived —
    /// which depends on how the records fell into steps.
    /// </summary>
    public bool Supersedes(long previousTicks, long nextTicks) => nextTicks - OpenTicks > previousTicks;

    public bool Supersedes(TValue previous, TValue next) =>
        Supersedes(eventTime(previous).UtcTicks, eventTime(next).UtcTicks);

    /// <summary>The row's event time, or false where the selector throws: the row then has
    /// no time here, and whoever asks treats it as an amendment.</summary>
    public bool TryEventTicks(TValue value, out long ticks)
    {
        try
        {
            ticks = eventTime(value).UtcTicks;
            return true;
        }
        catch (Exception)
        {
            ticks = long.MinValue;
            return false;
        }
    }

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
