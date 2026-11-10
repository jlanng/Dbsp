using Incremental.Storage;

namespace Incremental.Operators;

/// <summary>Address of one bucket in an <see cref="ExtremumOperator{TKey,TValue,TProjected}"/>
/// tournament tree: the group key, the tree level (0 = leaves, <c>LeafLevels</c> = root) and
/// the slot within that level.</summary>
internal readonly record struct ExtremumSlot<TKey>(TKey Key, byte Level, uint Slot) where TKey : notnull;

/// <summary>
/// Incremental grouped MIN/MAX maintained by a tournament tree, so that updates — including
/// deleting the current extremum — cost a handful of small-bucket scans instead of a rescan
/// of the whole group.
///
/// Per key, projected values are partitioned into leaf buckets by a stable hash; each level
/// above holds the extremum of every non-empty child bucket, up to a single root bucket
/// whose extremum is the group's result. A delta row touches its leaf bucket and then
/// propagates upward only while a bucket's extremum actually changed, so steady-state cost
/// is O(levels) buckets of at most <c>Fanout</c> entries each, independent of group size.
/// The tree lives in an ordinary <see cref="IIndexedWeightStore{TKey,TValue}"/> keyed by
/// <see cref="ExtremumSlot{TKey}"/>, so every storage provider supports it unchanged.
///
/// Because a bucket's extremum is an element of that bucket, and a value's slot at every
/// level is derived from its hash, each entry of a parent bucket identifies exactly one
/// non-empty child — eviction walks the tree from the root deleting every reachable bucket.
///
/// Output semantics match <see cref="AggregateOperator{TKey,TValue,TResult}"/>: when the
/// result changes, the stale result is retracted (-1) and the new one asserted (+1); a
/// vanished group retracts without asserting. Rows whose stored weight is not positive are
/// ignored rather than rejected.
/// </summary>
internal sealed class ExtremumOperator<TKey, TValue, TProjected> : IStepOperator
    where TKey : notnull
    where TValue : notnull
    where TProjected : notnull
{
    private const int BitsPerLevel = 4; // fanout 16
    private const int LeafLevels = 6;   // 16^6 leaf slots: groups of millions keep tiny buckets
    private const byte RootLevel = LeafLevels;
    private const uint LeafMask = (1u << (BitsPerLevel * LeafLevels)) - 1;

    private readonly IIndexedWeightStore<ExtremumSlot<TKey>, TProjected> _tree;
    private readonly Func<TValue, TProjected> _selector;
    private readonly Func<TProjected, ulong> _slotHash;
    private readonly int _sign; // -1: MIN, +1: MAX (a candidate wins when Compare has this sign)
    private readonly EvictionTracker<TKey>? _eviction;

    public ExtremumOperator(OperatorContext context, Func<TValue, TProjected> selector, int sign, KeyEviction<TKey>? eviction)
    {
        _tree = context.Storage.CreateIndexedWeightStore<ExtremumSlot<TKey>, TProjected>(context.StateName("tree"));
        _selector = selector;
        _sign = sign;
        _eviction = eviction is null ? null : new EvictionTracker<TKey>(eviction);
        _errors = context.Errors;
        try
        {
            // Tree slots are derived from the hash and may live in persistent storage a
            // later process reattaches, so the hash must be process-stable.
            _slotHash = StableHash.HasherFor<TProjected>();
        }
        catch (NotSupportedException e)
        {
            throw new InvalidOperationException(
                $"Min/Max derives its persistent tournament-tree slots from a process-stable hash of the projected " +
                $"type '{typeof(TProjected)}', which could not be derived. {e.Message}", e);
        }
    }

    private readonly ZSet<KeyedValue<TKey, TProjected>> _empty = new();

    // Every state change below is driven by a delta row, so an all-empty step is a
    // no-op — unless a KeyEviction is attached, whose DrainClosed fires off the
    // watermark instead and must run whether or not data arrived.
    public object? EmptyStepResult => _eviction is null ? _empty : null;

    /// <summary>
    /// One level's pending bucket changes, held as index chains over flat buffers rather than
    /// a list per bucket. A delta row walks all seven levels, so anything allocated per bucket
    /// per level is allocated several times per record — which is what this shape avoids: the
    /// buffers are cleared and refilled, and two of them ping-pong (this level's buckets are
    /// read while the parent's are written).
    /// </summary>
    private sealed class Level
    {
        public readonly List<TKey> Keys = [];
        public readonly List<uint> Slots = [];
        public readonly List<int> Head = [];    // index into Rows of this bucket's first row, or -1
        public readonly List<(TProjected Value, long Weight, int Next)> Rows = [];
        private readonly Dictionary<(TKey Key, uint Slot), int> _index = [];

        public int Count => Keys.Count;

        public void Clear()
        {
            Keys.Clear();
            Slots.Clear();
            Head.Clear();
            Rows.Clear();
            _index.Clear();
        }

        public void Add(TKey key, uint slot, TProjected value, long weight)
        {
            ref var bucket = ref System.Runtime.InteropServices.CollectionsMarshal
                .GetValueRefOrAddDefault(_index, (key, slot), out var known);
            if (!known)
            {
                bucket = Keys.Count;
                Keys.Add(key);
                Slots.Add(slot);
                Head.Add(-1);
            }
            Rows.Add((value, weight, Head[bucket]));
            Head[bucket] = Rows.Count - 1;
        }
    }

    private readonly Level _levelA = new();
    private readonly Level _levelB = new();
    private readonly List<ExtremumSlot<TKey>> _slots = [];
    private readonly List<WeightedValue<TProjected>> _before = [];
    private readonly List<int> _bounds = [];
    private readonly List<WeightedValue<TProjected>> _merged = [];
    private readonly List<(ExtremumSlot<TKey>, TProjected, long)> _updates = [];
    private readonly OutputBuffer<KeyedValue<TKey, TProjected>> _output = new();

    private readonly RecordErrors? _errors; // null under the default policy

    // The dead-letter policy's projection. The selector runs once per row, before the row
    // touches the tree, so a row it throws for (or projects to null) never enters the
    // group — nor, the selector being deterministic, does its retraction.
    private void ProjectGuarded(ZSet<KeyedValue<TKey, TValue>> delta, Level current, RecordErrors errors)
    {
        foreach (var (row, weight) in delta)
        {
            TProjected projected;
            uint slot;
            try
            {
                projected = RecordErrors.Result(_selector(row.Value));
                slot = (uint)_slotHash(projected) & LeafMask;
            }
            catch (Exception ex)
            {
                errors.Add(row, weight, ex);
                continue;
            }
            current.Add(row.Key, slot, projected, weight);
            _eviction?.Track(row.Key);
        }
    }

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<KeyedValue<TKey, TValue>>)inputs[0]!;
        var output = _output.Next(delta.DistinctCount);

        // Level-major walk (all keys advance together) so each level is one batched read
        // and one batched write against the store, regardless of how many keys the delta
        // touches: `current` holds the pending entries per (key, slot) bucket at the
        // current level, `next` collects what propagates to the parents.
        var current = _levelA;
        var next = _levelB;
        current.Clear();
        if (_errors is not null)
            ProjectGuarded(delta, current, _errors);
        else
            foreach (var ((key, value), weight) in delta)
            {
                var projected = _selector(value);
                current.Add(key, (uint)_slotHash(projected) & LeafMask, projected, weight);
                _eviction?.Track(key);
            }

        var comparer = Comparer<TProjected>.Default;
        for (byte level = 0; level <= RootLevel && current.Count > 0; level++)
        {
            _slots.Clear();
            for (var i = 0; i < current.Count; i++)
                _slots.Add(new ExtremumSlot<TKey>(current.Keys[i], level, current.Slots[i]));

            _before.Clear();
            _bounds.Clear();
            _tree.GetGroupsInto(_slots, _before, _bounds);

            _updates.Clear();
            next.Clear();

            for (var i = 0; i < _slots.Count; i++)
            {
                var start = i == 0 ? 0 : _bounds[i - 1];
                var end = _bounds[i];
                var hadOld = TryExtremum(_before, start, end, comparer, out var oldBest);

                // The bucket after this step's rows are folded in. A bucket holds at most
                // one entry per child (fanout 16), and leaf buckets are 1-in-16M, so this
                // stays a handful of entries and a linear merge beats a hash table on it.
                _merged.Clear();
                for (var b = start; b < end; b++)
                    _merged.Add(_before[b]);
                for (var row = current.Head[i]; row >= 0; row = current.Rows[row].Next)
                {
                    var (value, weight, _) = current.Rows[row];
                    _updates.Add((_slots[i], value, weight));
                    Fold(_merged, value, weight);
                }
                var hasNew = TryExtremum(_merged, 0, _merged.Count, comparer, out var newBest);

                if (hadOld == hasNew && (!hadOld || EqualityComparer<TProjected>.Default.Equals(oldBest!, newBest!)))
                    continue; // extremum unchanged: nothing propagates past this bucket

                if (level == RootLevel)
                {
                    // The root's extremum is the emitted result, so its old value is
                    // exactly what was published last — no separate result store needed.
                    if (hadOld)
                        output.Add(new KeyedValue<TKey, TProjected>(current.Keys[i], oldBest!), -1);
                    if (hasNew)
                        output.Add(new KeyedValue<TKey, TProjected>(current.Keys[i], newBest!), 1);
                }
                else
                {
                    var parentSlot = current.Slots[i] >> BitsPerLevel;
                    if (hadOld)
                        next.Add(current.Keys[i], parentSlot, oldBest!, -1);
                    if (hasNew)
                        next.Add(current.Keys[i], parentSlot, newBest!, 1);
                }
            }

            // Buckets at one level are disjoint from the next level's reads, so the
            // whole level's updates flush as one wave after its reads.
            _tree.ApplyAll(_updates);
            (current, next) = (next, current);
        }

        if (_eviction?.DrainClosed() is { Count: > 0 } closed)
            RemoveTrees(closed);

        return output;
    }

    /// <summary>Adds <paramref name="weight"/> to <paramref name="value"/>'s entry in a
    /// bucket-sized multiset, dropping it if the sum reaches zero.</summary>
    private static void Fold(List<WeightedValue<TProjected>> bucket, TProjected value, long weight)
    {
        for (var i = 0; i < bucket.Count; i++)
        {
            if (!EqualityComparer<TProjected>.Default.Equals(bucket[i].Value, value))
                continue;
            var updated = bucket[i].Weight + weight;
            if (updated == 0)
                bucket.RemoveAt(i);
            else
                bucket[i] = new WeightedValue<TProjected>(value, updated);
            return;
        }
        bucket.Add(new WeightedValue<TProjected>(value, weight));
    }

    private bool TryExtremum(
        List<WeightedValue<TProjected>> entries, int start, int end, Comparer<TProjected> comparer, out TProjected best)
    {
        var found = false;
        best = default!;
        for (var i = start; i < end; i++)
        {
            var (value, weight) = entries[i];
            if (weight <= 0)
                continue;
            if (!found || Math.Sign(comparer.Compare(value, best)) == _sign)
            {
                best = value;
                found = true;
            }
        }
        return found;
    }

    /// <summary>Deletes the whole tree of every closed key, walking all trees down together:
    /// one batched read + one batched delete per level, since a bucket's entries identify
    /// its non-empty children (leaf buckets need no read — they have no children).</summary>
    private void RemoveTrees(List<TKey> keys)
    {
        var buckets = new List<ExtremumSlot<TKey>>(keys.Count);
        foreach (var key in keys)
            buckets.Add(new ExtremumSlot<TKey>(key, RootLevel, 0));

        for (var level = RootLevel; level >= 1 && buckets.Count > 0; level--)
        {
            var entriesPerBucket = _tree.GetGroups(buckets);
            _tree.RemoveGroups(buckets);

            var children = new List<ExtremumSlot<TKey>>();
            for (var i = 0; i < buckets.Count; i++)
            {
                foreach (var (value, _) in entriesPerBucket[i])
                {
                    var leafSlot = (uint)_slotHash(value) & LeafMask;
                    children.Add(new ExtremumSlot<TKey>(buckets[i].Key, (byte)(level - 1), leafSlot >> (BitsPerLevel * (level - 1))));
                }
            }
            buckets = children;
        }

        _tree.RemoveGroups(buckets);
    }
}
