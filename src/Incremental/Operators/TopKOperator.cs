using Incremental.Storage;

namespace Incremental.Operators;

/// <summary>
/// Incremental per-key TOP-K: keeps the integrated group per key and the set of rows it
/// last emitted, and on a touched key re-ranks the group and emits only the membership
/// difference — displaced rows are retracted, newly ranked rows asserted. Multiplicities
/// count toward K (a value with weight 3 occupies three ranks). Ties are broken
/// arbitrarily.
///
/// Threshold short-circuit: when the emitted top is full and every one of a key's delta
/// rows ranks strictly worse than the current K-th entry, the top provably cannot change —
/// inserts below the cutoff can't enter it, and retractions below the cutoff can't be part
/// of it — so the group scan and sort are skipped entirely. This keeps steady-state cost
/// O(delta) even for huge groups; full re-ranks happen only when the top actually moves.
/// TODO: deterministic tie-break option, and an ordered store so even those re-ranks don't
/// scan the whole group.
/// </summary>
internal sealed class TopKOperator<TKey, TValue, TOrder> : IStepOperator
    where TKey : notnull
    where TValue : notnull
{
    private readonly IIndexedWeightStore<TKey, TValue> _groups;
    private readonly IValueStore<TKey, WeightedValue<TValue>[]> _lastEmitted;
    private readonly Func<TValue, TOrder> _orderBy;
    private readonly int _count;
    private readonly int _direction; // -1: largest first (descending), +1: smallest first
    private readonly EvictionTracker<TKey>? _eviction;

    public TopKOperator(OperatorContext context, int count, Func<TValue, TOrder> orderBy, bool descending, KeyEviction<TKey>? eviction)
    {
        _groups = context.Storage.CreateIndexedWeightStore<TKey, TValue>(context.StateName("groups"));
        _lastEmitted = context.Storage.CreateValueStore<TKey, WeightedValue<TValue>[]>(context.StateName("emitted"));
        _orderBy = orderBy;
        _count = count;
        _direction = descending ? -1 : 1;
        _eviction = eviction is null ? null : new EvictionTracker<TKey>(context, eviction);
        var comparer = Comparer<TOrder>.Default;
        var direction = _direction;
        _rankComparison = (a, b) => direction * comparer.Compare(a.Order, b.Order);
    }

    private readonly ZSet<KeyedValue<TKey, TValue>> _empty = new();

    // Every state change below is driven by a delta row, so an all-empty step is a
    // no-op — unless a KeyEviction is attached, whose DrainClosed fires off the
    // watermark instead and must run whether or not data arrived.
    public object? EmptyStepResult => _eviction is null ? _empty : null;

    // Per-step scratch, reused across steps (single-threaded per worker). The delta rows of a
    // touched key are held as a chain through _nextRow — index-linked lists over one flat
    // buffer — so a step that touches many keys allocates no per-key list.
    private readonly List<(TKey, TValue, long)> _entries = [];
    private readonly List<TKey> _touchedKeys = [];
    private readonly Dictionary<TKey, int> _firstRow = [];
    private readonly List<WeightedValue<TValue>> _rows = [];
    private readonly List<int> _nextRow = [];
    private readonly List<int> _chainHead = [];
    private readonly List<TKey> _rerankKeys = [];
    private readonly List<WeightedValue<TValue>[]?> _rerankPrevious = [];
    private readonly List<(TOrder Order, WeightedValue<TValue> Entry)> _ranked = [];
    private readonly List<WeightedValue<TValue>> _top = [];
    private readonly List<(TKey, WeightedValue<TValue>[])> _puts = [];
    private readonly List<TKey> _removes = [];
    private readonly OutputBuffer<KeyedValue<TKey, TValue>> _output = new();
    private readonly List<WeightedValue<TValue>> _groupRows = [];
    private readonly List<int> _groupBounds = [];

    /// <summary>Held rather than built per sort: a lambda closing over the comparer allocates
    /// a delegate (and a display class) on every re-rank otherwise.</summary>
    private readonly Comparison<(TOrder Order, WeightedValue<TValue> Entry)> _rankComparison;

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<KeyedValue<TKey, TValue>>)inputs[0]!;

        var entries = _entries;
        var touchedKeys = _touchedKeys;
        entries.Clear();
        touchedKeys.Clear();
        _firstRow.Clear();
        _rows.Clear();
        _nextRow.Clear();
        _chainHead.Clear();
        entries.EnsureCapacity(delta.DistinctCount);
        foreach (var ((key, value), weight) in delta)
        {
            entries.Add((key, value, weight));
            ref var head = ref System.Runtime.InteropServices.CollectionsMarshal
                .GetValueRefOrAddDefault(_firstRow, key, out var known);
            if (!known)
            {
                head = touchedKeys.Count;
                touchedKeys.Add(key);
                _chainHead.Add(-1);
            }
            var slot = head;
            _rows.Add(new WeightedValue<TValue>(value, weight));
            _nextRow.Add(_chainHead[slot]);
            _chainHead[slot] = _rows.Count - 1;
            _eviction?.Track(key);
        }
        _groups.ApplyAll(entries);

        var comparer = Comparer<TOrder>.Default;
        var lastResults = _lastEmitted.TryGetAll(touchedKeys);

        // Filled only with the keys that actually need re-ranking. The threshold short-circuit
        // means the usual outcome is that none does — a delta below the cutoff cannot move
        // the top — and that path should cost nothing beyond the state update above.
        var rerankKeys = _rerankKeys;
        var rerankPrevious = _rerankPrevious;
        rerankKeys.Clear();
        rerankPrevious.Clear();
        for (var i = 0; i < touchedKeys.Count; i++)
        {
            var (hadPrevious, emitted) = lastResults[i];
            if (hadPrevious && TopUnaffected(emitted!, i, comparer))
                continue;
            rerankKeys.Add(touchedKeys[i]);
            rerankPrevious.Add(hadPrevious ? emitted : null);
        }

        if (rerankKeys.Count == 0)
        {
            EvictClosed();
            return _empty;
        }

        var output = _output.Next(rerankKeys.Count * _count * 2);
        // A re-rank reads the whole group, which is the largest read this operator makes;
        // GetGroupsInto keeps it in one reused buffer rather than an array per key.
        _groupRows.Clear();
        _groupBounds.Clear();
        _groups.GetGroupsInto(rerankKeys, _groupRows, _groupBounds);
        var puts = _puts;
        var removes = _removes;
        puts.Clear();
        removes.Clear();

        var ranked = _ranked;
        var top = _top;
        for (var i = 0; i < rerankKeys.Count; i++)
        {
            var key = rerankKeys[i];
            var previous = rerankPrevious[i];

            // Select the best K entries rather than sorting the whole group: a re-rank reads
            // a group that can hold thousands of rows to publish three of them, and every
            // entry carries at least one rank, so K entries always suffice. The bounded
            // insert is O(K) and only runs for a row that beats the current K-th.
            ranked.Clear();
            for (var g = i == 0 ? 0 : _groupBounds[i - 1]; g < _groupBounds[i]; g++)
            {
                var entry = _groupRows[g];
                if (entry.Weight <= 0)
                    continue;
                var order = _orderBy(entry.Value);
                if (ranked.Count == _count && _rankComparison((order, entry), ranked[^1]) >= 0)
                    continue;
                var at = ranked.Count;
                while (at > 0 && _rankComparison((order, entry), ranked[at - 1]) < 0)
                    at--;
                if (ranked.Count == _count)
                    ranked.RemoveAt(ranked.Count - 1);
                ranked.Insert(at, (order, entry));
            }

            top.Clear();
            long remaining = _count;
            foreach (var (_, entry) in ranked)
            {
                if (remaining == 0)
                    break;
                var take = Math.Min(entry.Weight, remaining);
                top.Add(new WeightedValue<TValue>(entry.Value, take));
                remaining -= take;
            }

            foreach (var (value, weight) in top)
                output.Add(new KeyedValue<TKey, TValue>(key, value), weight);
            if (previous is not null)
                foreach (var (value, weight) in previous)
                    output.Add(new KeyedValue<TKey, TValue>(key, value), -weight);

            if (top.Count == 0)
            {
                if (previous is not null)
                    removes.Add(key);
            }
            else if (previous is null || !SameTop(top, previous))
            {
                puts.Add((key, [.. top]));
            }
        }

        _lastEmitted.PutAll(puts);
        _lastEmitted.RemoveAll(removes);

        EvictClosed();

        return output;
    }

    /// <summary>Drops the state of every key the watermark has closed. Runs on both exits from
    /// <see cref="Step"/>, including the short-circuit — eviction is driven by the watermark,
    /// not by whether this step's delta happened to move a top.</summary>
    private void EvictClosed()
    {
        if (_eviction?.DrainClosed() is { Count: > 0 } closed)
        {
            _groups.RemoveGroups(closed);
            _lastEmitted.RemoveAll(closed);
        }
    }

    /// <summary>Element-wise comparison of the freshly ranked top against the stored one,
    /// without the enumerator pair a LINQ SequenceEqual would allocate on every re-rank.</summary>
    private static bool SameTop(List<WeightedValue<TValue>> top, WeightedValue<TValue>[] previous)
    {
        if (top.Count != previous.Length)
            return false;
        for (var i = 0; i < previous.Length; i++)
            if (!top[i].Equals(previous[i]))
                return false;
        return true;
    }

    /// <summary>True when this step's delta rows provably cannot change the emitted top:
    /// it is full (K rows counting multiplicity) and every delta row ranks strictly worse
    /// than the K-th entry. Ties with the cutoff conservatively force a re-rank.</summary>
    private bool TopUnaffected(WeightedValue<TValue>[] emitted, int keyIndex, Comparer<TOrder> comparer)
    {
        long emittedWeight = 0;
        foreach (var (_, weight) in emitted)
            emittedWeight += weight;
        if (emittedWeight < _count)
            return false; // room left in the top: any insert may enter it

        // Emitted rows are stored in rank order; the last one is the K-th (worst) entry.
        var cutoff = _orderBy(emitted[^1].Value);
        for (var row = _chainHead[keyIndex]; row >= 0; row = _nextRow[row])
            if (_direction * comparer.Compare(_orderBy(_rows[row].Value), cutoff) <= 0)
                return false; // ranks at or above the cutoff: could enter or leave the top
        return true;
    }
}
