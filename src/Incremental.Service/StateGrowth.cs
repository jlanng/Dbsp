using Incremental.Storage;

namespace Incremental.Service;

/// <summary>
/// Whether one pipeline holds a store that has grown without a pause for
/// <see cref="ServiceOptions.StateGrowthAlarmAfter"/>. Fed once a second with the entry
/// counts the node already reads for its gauges, it keeps for each store the last sixteen
/// samples, one per fifteenth of the period, in a fixed ring.
/// <para>
/// The rule, store by store: across those samples — the whole period — none is lower than
/// the one before it, and at least twelve of the fifteen steps rose. Steps that stay level
/// are allowed, so a feed with quiet minutes is still caught, but not many of them, so a
/// store that filled to its bound and then stopped because its input went quiet is not.
/// It is judged per store because a total hides a leak behind a store that churns: an
/// evicting join's level moves by more between two samples than a slow leak adds.
/// </para>
/// <para>
/// A store that closes on the watermark — one whose operator keeps deadlines beside it
/// (<c>{operator}.deadlines.w{n}</c>), and that store of deadlines itself — is not held to
/// that rule while the watermark moves. It rises at every sample until the watermark has
/// passed the first record it holds, which is the lateness or the window the definition
/// declares and may be hours; what it holds then is the definition's arithmetic, not a
/// leak. It is flagged only when the committed watermark has not moved in the whole
/// period while the store grew: then nothing it holds can close.
/// </para>
/// <para>
/// A flag clears when its store falls, when the store has not risen in a whole period,
/// or — for one that closes on the watermark — when the watermark moves. The pipeline is
/// flagged while any of its stores is. Only the sampler's thread writes it.
/// </para>
/// </summary>
internal sealed class StateGrowth
{
    private const int Samples = 16;
    private const int RisesNeeded = 12;

    /// <summary>One store's samples.</summary>
    private sealed class Series
    {
        public readonly long[] Entries = new long[Samples];
        public readonly long[] Times = new long[Samples];
        public readonly long[] Watermarks = new long[Samples];
        public int Count;
        public int Next;
        public long FlaggedTicks;
        public long FlaggedFrom;
        public bool Stalled;
        public long SeenAt;

        public int Oldest => Count < Samples ? 0 : Next;

        /// <summary>+1 when this reading flags the store, −1 when it clears its flag.</summary>
        public int Observe(long ticks, long entries, long watermark, bool closesOnWatermark)
        {
            var flagged = FlaggedTicks != 0;
            var last = (Next + Samples - 1) % Samples;
            var fell = Count > 0 && entries < Entries[last];
            Entries[Next] = entries;
            Times[Next] = ticks;
            Watermarks[Next] = watermark;
            Next = (Next + 1) % Samples;
            Count = Math.Min(Samples, Count + 1);

            if (fell || (flagged && Stalled && watermark != Watermarks[last]))
                return Clear(flagged);
            if (Count < Samples)
                return 0;

            var rises = 0;
            for (var i = 1; i < Samples; i++)
            {
                var before = Entries[(Next + i - 1) % Samples];
                var after = Entries[(Next + i) % Samples];
                if (after < before)
                    return Clear(flagged);
                if (after > before)
                    rises++;
            }
            if (flagged)
                return rises == 0 ? Clear(true) : 0;
            if (rises < RisesNeeded)
                return 0;
            // Closing on a watermark that moved in the period: filling what the definition
            // keeps open, however long that is.
            var stalled = closesOnWatermark;
            if (stalled && Watermarks[Next] != watermark)
                return 0;
            FlaggedTicks = Times[Next];
            FlaggedFrom = Entries[Next];
            Stalled = stalled;
            return 1;
        }

        private int Clear(bool flagged)
        {
            FlaggedTicks = 0;
            return flagged ? -1 : 0;
        }
    }

    private readonly Dictionary<string, Series> _stores = new(StringComparer.Ordinal);
    private readonly HashSet<string> _closing = new(StringComparer.Ordinal);
    private readonly List<string> _gone = [];
    private long _lastSampleTicks;
    private long _round;
    private volatile Flag? _flag;

    /// <summary>What is flagged: the store that has been growing longest.</summary>
    public sealed record Flag(string Store, DateTimeOffset Since, long FromEntries, bool WatermarkStalled);

    /// <summary>The flag; null while no store of the pipeline is flagged.</summary>
    public Flag? Current => _flag;

    /// <summary>When the run of growth that is flagged began; null while none is.</summary>
    public DateTimeOffset? Since => _flag?.Since;

    /// <summary>Takes one reading of every store. Returns +1 when this reading flags the
    /// pipeline, −1 when it clears the last flag, 0 otherwise. <paramref name="watermark"/>
    /// is the pipeline's committed watermark in ticks, <see cref="long.MinValue"/> for none.</summary>
    public int Observe(DateTimeOffset now, IReadOnlyList<StateStoreSize> stores, long watermark, TimeSpan period)
    {
        var was = _flag;
        if (period <= TimeSpan.Zero)
        {
            _stores.Clear();
            _lastSampleTicks = 0;
            _flag = null;
            return was is null ? 0 : -1;
        }
        var interval = Math.Max(TimeSpan.TicksPerSecond, period.Ticks / (Samples - 1));
        var ticks = now.UtcTicks;
        if (_lastSampleTicks != 0 && ticks - _lastSampleTicks < interval)
            return 0;
        _lastSampleTicks = ticks;
        _round++;

        // The stores that close on the watermark: every store of an operator that keeps deadlines.
        _closing.Clear();
        foreach (var store in stores)
            if (DeadlinesOwner(store.Name) is { } owner)
                _closing.Add(owner);

        Flag? flag = null;
        foreach (var store in stores)
        {
            if (!_stores.TryGetValue(store.Name, out var series))
                _stores[store.Name] = series = new Series();
            series.SeenAt = _round;
            series.Observe(ticks, store.Entries, watermark, _closing.Count > 0 && _closing.Contains(OwnerOf(store.Name)));
            if (series.FlaggedTicks != 0 && (flag is null || series.FlaggedTicks < flag.Since.UtcTicks))
                flag = new Flag(store.Name, new DateTimeOffset(series.FlaggedTicks, TimeSpan.Zero), series.FlaggedFrom, series.Stalled);
        }

        // A store the pipeline no longer has (another definition, another worker count).
        if (_stores.Count > stores.Count)
        {
            _gone.Clear();
            foreach (var (name, series) in _stores)
                if (series.SeenAt != _round)
                    _gone.Add(name);
            foreach (var name in _gone)
                _stores.Remove(name);
        }

        if (flag is null)
        {
            _flag = null;
            return was is null ? 0 : -1;
        }
        // The same store flagged since the same moment is the same flag.
        if (was is not null && was.Store == flag.Store && was.Since == flag.Since)
            return 0;
        _flag = flag;
        return was is null ? 1 : 0;
    }

    /// <summary>Whether <see cref="Observe"/> would take a reading at <paramref name="now"/>:
    /// what its caller asks before it reads the watermark for one.</summary>
    public bool Due(DateTimeOffset now, TimeSpan period) =>
        period <= TimeSpan.Zero
            ? _flag is not null || _stores.Count > 0
            : _lastSampleTicks == 0 || now.UtcTicks - _lastSampleTicks >= Math.Max(TimeSpan.TicksPerSecond, period.Ticks / (Samples - 1));

    /// <summary>The operator a store belongs to: its name less the purpose and the worker
    /// (<c>{operator}.{purpose}.w{n}</c>). The name itself when it is not of that shape.</summary>
    internal static string OwnerOf(string store)
    {
        var worker = store.LastIndexOf(".w", StringComparison.Ordinal);
        if (worker <= 0 || worker + 2 >= store.Length)
            return store;
        for (var i = worker + 2; i < store.Length; i++)
            if (!char.IsAsciiDigit(store[i]))
                return store;
        var purpose = store.LastIndexOf('.', worker - 1);
        return purpose <= 0 ? store : store[..purpose];
    }

    private static string? DeadlinesOwner(string store)
    {
        var owner = OwnerOf(store);
        return owner.Length < store.Length && store.AsSpan(owner.Length).StartsWith(".deadlines.w", StringComparison.Ordinal) ? owner : null;
    }
}
