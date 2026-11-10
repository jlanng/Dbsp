using System.Numerics;
using BenchmarkDotNet.Attributes;
using Incremental.Tables;

namespace Incremental.Benchmarks;

/// <summary>
/// The shared table's versioned view (a concurrent map of short version chains) against the
/// alternative the design considered — a persistent, structure-sharing map where each
/// version is an immutable root — and against a plain <see cref="Dictionary{TKey,TValue}"/>,
/// which is what a join's private copy of the same rows costs to probe today.
/// </summary>
[MemoryDiagnoser]
public class TableBenchmarks
{
    public sealed record Row(int Id, string Symbol, long Stamp);

    private const int Probes = 100_000;
    private const int BatchSize = 1_000;

    [Params(100_000, 1_000_000)]
    public int N;

    private SharedTable<int, Row> _table = null!;
    private TableSnapshot<int, Row> _snapshot;
    private PersistentMap<Row> _persistent = null!;
    private Dictionary<int, Row> _dictionary = null!;
    private int[] _probeKeys = null!;
    private int[] _batchKeys = null!;
    private long _stamp;

    [GlobalSetup]
    public void Setup()
    {
        // The probe benchmarks read through one snapshot for the whole run, so it must
        // never be revoked. With no log every writer call completes synchronously.
        _table = new SharedTable<int, Row>("rows", r => r.Id, new SharedTableOptions { MaxRetainedVersions = int.MaxValue });
        _persistent = PersistentMap<Row>.Empty;
        _dictionary = new Dictionary<int, Row>(N);
        for (var i = 0; i < N; i++)
        {
            var row = new Row(i, "S" + i, 0);
            _table.Writer.UpsertAsync(row).GetAwaiter().GetResult();
            _persistent = _persistent.Set(i, row);
            _dictionary[i] = row;
        }
        _table.Writer.CommitAsync().GetAwaiter().GetResult();
        _snapshot = _table.Snapshot();

        var random = new Random(42);
        _probeKeys = Enumerable.Range(0, Probes).Select(_ => random.Next(N)).ToArray();
        _batchKeys = Enumerable.Range(0, BatchSize).Select(_ => random.Next(N)).Distinct().ToArray();
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = Probes)]
    public long Probe_Dictionary()
    {
        long sum = 0;
        foreach (var key in _probeKeys)
            if (_dictionary.TryGetValue(key, out var row))
                sum += row.Stamp;
        return sum;
    }

    [Benchmark(OperationsPerInvoke = Probes)]
    public long Probe_VersionChains()
    {
        long sum = 0;
        foreach (var key in _probeKeys)
            if (_snapshot.TryGet(key, out var row))
                sum += row.Stamp;
        return sum;
    }

    [Benchmark(OperationsPerInvoke = Probes)]
    public long Probe_PersistentMap()
    {
        long sum = 0;
        var map = _persistent;
        foreach (var key in _probeKeys)
            if (map.TryGet(key, out var row))
                sum += row.Stamp;
        return sum;
    }

    /// <summary>One version changing <see cref="BatchSize"/> existing rows. The row objects
    /// are allocated in both cases; the difference is what the structure adds per change.</summary>
    [Benchmark(OperationsPerInvoke = BatchSize)]
    public long Write_VersionChains()
    {
        var stamp = ++_stamp;
        foreach (var key in _batchKeys)
            _table.Writer.UpsertAsync(new Row(key, "S" + key, stamp)).GetAwaiter().GetResult();
        return _table.Writer.CommitAsync().GetAwaiter().GetResult();
    }

    [Benchmark(OperationsPerInvoke = BatchSize)]
    public PersistentMap<Row> Write_PersistentMap()
    {
        var stamp = ++_stamp;
        var map = _persistent;
        foreach (var key in _batchKeys)
            map = map.Set(key, new Row(key, "S" + key, stamp));
        return _persistent = map;
    }
}

/// <summary>
/// A minimal hash array mapped trie over <c>int</c> keys: 5 bits per level, path-copying
/// writes, immutable roots. Only what the comparison needs (get and set). An int's hash is
/// itself, so two keys never collide in all 32 bits and no collision node is needed.
/// </summary>
public sealed class PersistentMap<TValue> where TValue : class
{
    public static readonly PersistentMap<TValue> Empty = new(new Branch(0, []));

    private readonly Branch _root;

    private PersistentMap(Branch root) => _root = root;

    private sealed class Leaf(int key, TValue value)
    {
        public readonly int Key = key;
        public readonly TValue Value = value;
    }

    /// <summary>Children are <see cref="Leaf"/> or <see cref="Branch"/>, packed in bitmap order.</summary>
    private sealed class Branch(uint bitmap, object[] children)
    {
        public readonly uint Bitmap = bitmap;
        public readonly object[] Children = children;
    }

    private static uint Mix(int key) => (uint)key * 0x9E3779B1u;

    public bool TryGet(int key, out TValue value)
    {
        var hash = Mix(key);
        var node = _root;
        for (var shift = 0; ; shift += 5)
        {
            var bit = 1u << (int)((hash >> shift) & 31);
            if ((node.Bitmap & bit) == 0)
                break;
            var child = node.Children[BitOperations.PopCount(node.Bitmap & (bit - 1))];
            if (child is Leaf leaf)
            {
                if (leaf.Key == key)
                {
                    value = leaf.Value;
                    return true;
                }
                break;
            }
            node = (Branch)child;
        }
        value = null!;
        return false;
    }

    public PersistentMap<TValue> Set(int key, TValue value) => new(Set(_root, 0, Mix(key), new Leaf(key, value)));

    private static Branch Set(Branch node, int shift, uint hash, Leaf leaf)
    {
        var bit = 1u << (int)((hash >> shift) & 31);
        var index = BitOperations.PopCount(node.Bitmap & (bit - 1));
        if ((node.Bitmap & bit) == 0)
        {
            var grown = new object[node.Children.Length + 1];
            Array.Copy(node.Children, 0, grown, 0, index);
            grown[index] = leaf;
            Array.Copy(node.Children, index, grown, index + 1, node.Children.Length - index);
            return new Branch(node.Bitmap | bit, grown);
        }

        var copy = (object[])node.Children.Clone();
        copy[index] = node.Children[index] switch
        {
            Leaf existing when existing.Key == leaf.Key => leaf,
            Leaf existing => Set(Set(new Branch(0, []), shift + 5, Mix(existing.Key), existing), shift + 5, hash, leaf),
            var branch => Set((Branch)branch, shift + 5, hash, leaf),
        };
        return new Branch(node.Bitmap, copy);
    }
}
