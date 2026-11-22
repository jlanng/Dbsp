using BenchmarkDotNet.Attributes;

namespace Dbsp.Benchmarks;

/// <summary>The core collection: per-entry cost of building, negating, and cancelling Z-sets.</summary>
[MemoryDiagnoser]
public class ZSetBenchmarks
{
    [Params(100_000)]
    public int N;

    private int[] _values = null!;
    private ZSet<int> _set = null!;
    private ZSet<int> _negated = null!;

    [GlobalSetup]
    public void Setup()
    {
        var random = new Random(42);
        _values = Enumerable.Range(0, N).OrderBy(_ => random.Next()).ToArray();
        _set = new ZSet<int>(N);
        foreach (var value in _values)
            _set.Add(value);
        _negated = _set.Negated();
    }

    [Benchmark(OperationsPerInvoke = 100_000)]
    public ZSet<int> Build()
    {
        var set = new ZSet<int>();
        foreach (var value in _values)
            set.Add(value);
        return set;
    }

    [Benchmark(OperationsPerInvoke = 100_000)]
    public ZSet<int> Negate() => _set.Negated();

    /// <summary>Z-set addition where every entry cancels: the retraction fast path.</summary>
    [Benchmark(OperationsPerInvoke = 100_000)]
    public ZSet<int> AddAllCancelling()
    {
        var set = new ZSet<int>(N);
        set.AddAll(_set);
        set.AddAll(_negated);
        return set;
    }
}
