using System.Collections;
using System.Runtime.InteropServices;

namespace Incremental;

/// <summary>A value paired with its (signed) multiplicity in a Z-set.</summary>
public readonly record struct WeightedValue<T>(T Value, long Weight) where T : notnull;

/// <summary>A key/value pair flowing through a keyed (indexed) stream.</summary>
public readonly record struct KeyedValue<TKey, TValue>(TKey Key, TValue Value)
    where TKey : notnull
    where TValue : notnull;

/// <summary>Type-erased view of a <see cref="ZSet{T}"/>, so the runtime — which moves deltas
/// around as <see cref="object"/> — can ask about one without knowing its element type.
/// Deliberately minimal: emptiness and size are all the scheduler needs.</summary>
internal interface IZSet
{
    bool IsEmpty { get; }
    int DistinctCount { get; }
}

/// <summary>
/// A Z-set: a finite map from values to signed 64-bit weights, the core collection type of
/// DBSP-style processing. A weight of +1 represents one insertion, -1 one deletion
/// (retraction); weights compose additively and entries whose weight reaches zero are
/// dropped. Streams in a circuit carry Z-set <em>deltas</em>, and integrating (summing) the
/// deltas of a stream yields the current state of the collection it describes.
/// </summary>
public sealed class ZSet<T> : IZSet, IEnumerable<WeightedValue<T>> where T : notnull
{
    private readonly Dictionary<T, long> _weights;

    public ZSet() => _weights = new Dictionary<T, long>();

    public ZSet(int capacity) => _weights = new Dictionary<T, long>(capacity);

    public ZSet(IEnumerable<WeightedValue<T>> entries) : this()
    {
        foreach (var (value, weight) in entries)
            Add(value, weight);
    }

    /// <summary>Number of values with a non-zero weight.</summary>
    public int DistinctCount => _weights.Count;

    public bool IsEmpty => _weights.Count == 0;

    /// <summary>The weight of <paramref name="value"/>, or 0 if absent.</summary>
    public long WeightOf(T value) => _weights.GetValueOrDefault(value);

    /// <summary>Adds <paramref name="weight"/> to the weight of <paramref name="value"/>,
    /// removing the entry if the sum reaches zero. One hash lookup in the common case: this
    /// is the innermost loop of every operator in the library, so the probe that finds the
    /// slot is also the probe that writes it.</summary>
    public void Add(T value, long weight = 1)
    {
        if (weight == 0)
            return;

        ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(_weights, value, out var existed);
        if (!existed)
        {
            slot = weight;
            return;
        }

        var updated = checked(slot + weight);
        if (updated == 0)
            _weights.Remove(value);  // `slot` is dead past this point
        else
            slot = updated;
    }

    /// <summary>Adds every entry of <paramref name="other"/> into this Z-set (Z-set addition, in place).</summary>
    public void AddAll(ZSet<T> other)
    {
        if (other._weights.Count == 0)
            return;
        _weights.EnsureCapacity(_weights.Count + other._weights.Count);
        foreach (var (value, weight) in other)
            Add(value, weight);
    }

    /// <summary>Reserves room for <paramref name="count"/> entries, so filling a Z-set whose
    /// size is known (or bounded) up front does not walk the dictionary's doubling ladder —
    /// which allocates, and then abandons, every intermediate size on the way up.</summary>
    internal void EnsureCapacity(int count)
    {
        if (count > _weights.Count)
            _weights.EnsureCapacity(count);
    }

    /// <summary>Empties the Z-set while keeping its capacity, so a buffer that is refilled
    /// every step stops re-growing after the first few. Only for Z-sets whose owner knows no
    /// one else still holds them — operators treat their inputs as read-only, which is what
    /// makes reuse safe where it is used (see <c>ExchangeOperator.Partition</c> and
    /// <c>OutputBuffer</c>, which extends the argument to every operator's output).
    /// <para>
    /// Capacity that has gone unused for a while is given back. Emptying a dictionary costs
    /// time proportional to its capacity, not its count, so a buffer that one enormous delta
    /// once passed through — a table's bootstrap, the catch-up after a long stop — would
    /// otherwise charge every later step for it, and hold the memory for good. Only after
    /// <see cref="SlackLimit"/> uses in a row at a small fraction of the capacity, though: a
    /// bursty workload keeps its buffers, which is the point of reusing them.
    /// </para></summary>
    internal void Clear()
    {
        var capacity = Capacity;
        if (capacity < ShrinkFloor || _weights.Count >= capacity / SlackRatio)
            _slack = 0;
        else if (++_slack == SlackLimit)
        {
            _slack = 0;
            _weights.Clear();
            _weights.TrimExcess();
            return;
        }
        _weights.Clear();
    }

    /// <summary>Entries the Z-set has room for without growing.</summary>
    internal int Capacity => _weights.EnsureCapacity(0);

    // Below the floor a clear is too cheap to be worth a reallocation.
    private const int ShrinkFloor = 1024;
    private const int SlackRatio = 16;
    private const int SlackLimit = 64;

    // Consecutive clears that found the set filled to less than 1/SlackRatio of its capacity.
    private int _slack;

    /// <summary>Returns a new Z-set with every weight negated (turns insertions into retractions).</summary>
    public ZSet<T> Negated()
    {
        var result = new ZSet<T>(_weights.Count);
        foreach (var (value, weight) in this)
            result.Add(value, -weight);
        return result;
    }

    /// <summary>Allocation-free enumeration. <c>foreach</c> binds to this by pattern rather
    /// than through <see cref="IEnumerable{T}"/>, so iterating a Z-set — which every operator
    /// does several times per step, most often over an empty shard — costs nothing. The
    /// interface implementations below still allocate an iterator; they exist for LINQ and
    /// other <c>IEnumerable</c> consumers, which are never on the step path.</summary>
    public Enumerator GetEnumerator() => new(_weights);

    IEnumerator<WeightedValue<T>> IEnumerable<WeightedValue<T>>.GetEnumerator() => Enumerate();

    IEnumerator IEnumerable.GetEnumerator() => Enumerate();

    private IEnumerator<WeightedValue<T>> Enumerate()
    {
        foreach (var kv in _weights)
            yield return new WeightedValue<T>(kv.Key, kv.Value);
    }

    /// <summary>A by-value cursor over the Z-set's entries, wrapping the underlying
    /// dictionary's own struct enumerator.</summary>
    public struct Enumerator
    {
        private Dictionary<T, long>.Enumerator _inner;

        internal Enumerator(Dictionary<T, long> weights) => _inner = weights.GetEnumerator();

        public WeightedValue<T> Current { get; private set; }

        public bool MoveNext()
        {
            if (!_inner.MoveNext())
                return false;
            var (value, weight) = _inner.Current;
            Current = new WeightedValue<T>(value, weight);
            return true;
        }
    }

    public override string ToString() =>
        "{" + string.Join(", ", _weights.Select(kv => $"{kv.Key} => {kv.Value:+#;-#;0}")) + "}";
}
