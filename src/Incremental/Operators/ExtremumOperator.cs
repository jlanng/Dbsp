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

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<KeyedValue<TKey, TValue>>)inputs[0]!;
        var output = new ZSet<KeyedValue<TKey, TProjected>>();

        // Level-major walk (all keys advance together) so each level is one batched read
        // and one batched write against the store, regardless of how many keys the delta
        // touches: changes holds the pending entries per (key, slot) bucket at the
        // current level.
        var changes = new Dictionary<(TKey Key, uint Slot), List<WeightedValue<TProjected>>>();
        foreach (var ((key, value), weight) in delta)
        {
            var projected = _selector(value);
            var slot = (uint)_slotHash(projected) & LeafMask;
            if (!changes.TryGetValue((key, slot), out var rows))
                changes[(key, slot)] = rows = [];
            rows.Add(new WeightedValue<TProjected>(projected, weight));
            _eviction?.Track(key);
        }

        var comparer = Comparer<TProjected>.Default;
        for (byte level = 0; level <= RootLevel && changes.Count > 0; level++)
        {
            var buckets = new List<ExtremumSlot<TKey>>(changes.Count);
            var bucketEntries = new List<List<WeightedValue<TProjected>>>(changes.Count);
            foreach (var ((key, slot), entries) in changes)
            {
                buckets.Add(new ExtremumSlot<TKey>(key, level, slot));
                bucketEntries.Add(entries);
            }

            var befores = _tree.GetGroups(buckets);
            var updates = new List<(ExtremumSlot<TKey>, TProjected, long)>();
            var parentChanges = new Dictionary<(TKey Key, uint Slot), List<WeightedValue<TProjected>>>();

            for (var i = 0; i < buckets.Count; i++)
            {
                var bucket = buckets[i];
                var before = befores[i];
                var hadOld = TryExtremum(before, comparer, out var oldBest);

                var merged = new Dictionary<TProjected, long>(before.Count + bucketEntries[i].Count);
                foreach (var (v, w) in before)
                    merged[v] = w;
                foreach (var (v, w) in bucketEntries[i])
                {
                    updates.Add((bucket, v, w));
                    var updated = merged.GetValueOrDefault(v) + w;
                    if (updated == 0)
                        merged.Remove(v);
                    else
                        merged[v] = updated;
                }
                var hasNew = TryExtremum(merged, comparer, out var newBest);

                if (hadOld == hasNew && (!hadOld || EqualityComparer<TProjected>.Default.Equals(oldBest!, newBest!)))
                    continue; // extremum unchanged: nothing propagates past this bucket

                if (level == RootLevel)
                {
                    // The root's extremum is the emitted result, so its old value is
                    // exactly what was published last — no separate result store needed.
                    if (hadOld)
                        output.Add(new KeyedValue<TKey, TProjected>(bucket.Key, oldBest!), -1);
                    if (hasNew)
                        output.Add(new KeyedValue<TKey, TProjected>(bucket.Key, newBest!), 1);
                }
                else
                {
                    var parent = (bucket.Key, bucket.Slot >> BitsPerLevel);
                    if (!parentChanges.TryGetValue(parent, out var list))
                        parentChanges[parent] = list = [];
                    if (hadOld)
                        list.Add(new WeightedValue<TProjected>(oldBest!, -1));
                    if (hasNew)
                        list.Add(new WeightedValue<TProjected>(newBest!, 1));
                }
            }

            // Buckets at one level are disjoint from the next level's reads, so the
            // whole level's updates flush as one wave after its reads.
            _tree.UpdateAll(updates);
            changes = parentChanges;
        }

        if (_eviction?.DrainClosed() is { Count: > 0 } closed)
            RemoveTrees(closed);

        return output;
    }

    private bool TryExtremum(IReadOnlyList<WeightedValue<TProjected>> entries, Comparer<TProjected> comparer, out TProjected best)
    {
        var found = false;
        best = default!;
        foreach (var (value, weight) in entries)
        {
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

    private bool TryExtremum(Dictionary<TProjected, long> entries, Comparer<TProjected> comparer, out TProjected best)
    {
        var found = false;
        best = default!;
        foreach (var (value, weight) in entries)
        {
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
