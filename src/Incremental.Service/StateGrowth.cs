namespace Incremental.Service;

/// <summary>
/// Whether one pipeline's state has grown without a pause for
/// <see cref="ServiceOptions.StateGrowthAlarmAfter"/>. Fed once a second with the entry
/// count the node already reads for its gauges, it keeps the last sixteen samples, one per
/// fifteenth of the period, in a fixed ring. The rule: across those samples — the whole
/// period — none is lower than the one before it, and at least twelve of the fifteen steps
/// rose. Steps that stay level are allowed, so a feed with quiet minutes is still caught,
/// but not many of them, so a state that filled to its bound and then stopped because its
/// input went quiet (the watermark, and with it eviction, waits for data) is not. A fall
/// clears the flag: the state evicts. Only the sampler's thread writes it.
/// </summary>
internal sealed class StateGrowth
{
    private const int Samples = 16;
    private const int RisesNeeded = 12;

    private readonly long[] _entries = new long[Samples];
    private readonly long[] _times = new long[Samples];
    private int _count;
    private int _next;
    private long _flaggedTicks;

    /// <summary>When the run of growth that is flagged began; null while none is.</summary>
    public DateTimeOffset? Since => Volatile.Read(ref _flaggedTicks) is var ticks and not 0
        ? new DateTimeOffset(ticks, TimeSpan.Zero)
        : null;

    /// <summary>Entries at the oldest sample held: where the flagged run began.</summary>
    public long OldestEntries => _count == 0 ? 0 : _entries[_count < Samples ? 0 : _next];

    /// <summary>Takes one reading. Returns +1 when this reading flags the pipeline, −1
    /// when it clears a flag, 0 otherwise.</summary>
    public int Observe(DateTimeOffset now, long entries, TimeSpan period)
    {
        var flagged = Volatile.Read(ref _flaggedTicks) != 0;
        if (period <= TimeSpan.Zero)
        {
            _count = 0;
            _next = 0;
            Volatile.Write(ref _flaggedTicks, 0);
            return flagged ? -1 : 0;
        }
        var interval = Math.Max(TimeSpan.TicksPerSecond, period.Ticks / (Samples - 1));
        var ticks = now.UtcTicks;
        var last = (_next + Samples - 1) % Samples;
        if (_count > 0 && ticks - _times[last] < interval)
            return 0;

        var fell = _count > 0 && entries < _entries[last];
        _entries[_next] = entries;
        _times[_next] = ticks;
        _next = (_next + 1) % Samples;
        _count = Math.Min(Samples, _count + 1);

        if (fell)
        {
            Volatile.Write(ref _flaggedTicks, 0);
            return flagged ? -1 : 0;
        }
        if (flagged || _count < Samples)
            return 0;

        var rises = 0;
        for (var i = 1; i < Samples; i++)
        {
            var before = _entries[(_next + i - 1) % Samples];
            var after = _entries[(_next + i) % Samples];
            if (after < before)
                return 0;
            if (after > before)
                rises++;
        }
        if (rises < RisesNeeded)
            return 0;
        Volatile.Write(ref _flaggedTicks, _times[_next]);
        return 1;
    }
}
