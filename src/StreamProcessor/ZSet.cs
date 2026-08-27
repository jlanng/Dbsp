using System.Collections;

namespace StreamProcessor;

/// <summary>A value paired with its (signed) multiplicity in a Z-set.</summary>
public readonly record struct WeightedValue<T>(T Value, long Weight) where T : notnull;

/// <summary>A key/value pair flowing through a keyed (indexed) stream.</summary>
public readonly record struct KeyedValue<TKey, TValue>(TKey Key, TValue Value)
    where TKey : notnull
    where TValue : notnull;

/// <summary>
/// A Z-set: a finite map from values to signed 64-bit weights, the core collection type of
/// DBSP-style processing. A weight of +1 represents one insertion, -1 one deletion
/// (retraction); weights compose additively and entries whose weight reaches zero are
/// dropped. Streams in a circuit carry Z-set <em>deltas</em>, and integrating (summing) the
/// deltas of a stream yields the current state of the collection it describes.
/// </summary>
public sealed class ZSet<T> : IEnumerable<WeightedValue<T>> where T : notnull
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
    /// removing the entry if the sum reaches zero.</summary>
    public void Add(T value, long weight = 1)
    {
        if (weight == 0)
            return;

        if (_weights.TryGetValue(value, out var existing))
        {
            var updated = checked(existing + weight);
            if (updated == 0)
                _weights.Remove(value);
            else
                _weights[value] = updated;
        }
        else
        {
            _weights[value] = weight;
        }
    }

    /// <summary>Adds every entry of <paramref name="other"/> into this Z-set (Z-set addition, in place).</summary>
    public void AddAll(ZSet<T> other)
    {
        foreach (var (value, weight) in other)
            Add(value, weight);
    }

    /// <summary>Returns a new Z-set with every weight negated (turns insertions into retractions).</summary>
    public ZSet<T> Negated()
    {
        var result = new ZSet<T>(_weights.Count);
        foreach (var (value, weight) in this)
            result.Add(value, -weight);
        return result;
    }

    public IEnumerator<WeightedValue<T>> GetEnumerator()
    {
        foreach (var kv in _weights)
            yield return new WeightedValue<T>(kv.Key, kv.Value);
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public override string ToString() =>
        "{" + string.Join(", ", _weights.Select(kv => $"{kv.Key} => {kv.Value:+#;-#;0}")) + "}";
}
