using BenchmarkDotNet.Attributes;

namespace Incremental.Benchmarks;

/// <summary>
/// Linear aggregation (SUM): a fixed-size delta against a varying number of groups.
/// State is O(1) per key, so per-op cost should not scale with GroupCount.
/// </summary>
[MemoryDiagnoser]
public class SumBenchmarks
{
    private const int DeltaSize = 1_000;

    [Params(1_000, 100_000)]
    public int GroupCount;

    private Rig _rig = null!;
    private InputHandle<(int Key, long Amount)> _input = null!;
    private WeightedValue<(int Key, long Amount)>[] _insert = null!;
    private WeightedValue<(int Key, long Amount)>[] _retract = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        var builder = new CircuitBuilder(Rig.Options());
        var rows = builder.Input<(int Key, long Amount)>("rows");
        var sums = rows
            .GroupBy(r => r.Key)
            .Sum(r => r.Amount)
            .ToOutput(bootstrap: false); // measure the aggregate, not snapshot maintenance

        var circuit = builder.Build();
        circuit.Start();
        _rig = new Rig(circuit);
        _rig.Watch(sums);
        _input = circuit.Handle(rows);

        await _rig.FeedAsync(_input, Enumerable.Range(0, GroupCount)
            .Select(i => new WeightedValue<(int, long)>((i, i), 1)));

        _insert = Enumerable.Range(0, DeltaSize)
            .Select(i => new WeightedValue<(int, long)>((i * 37 % GroupCount, 5), 1))
            .ToArray();
        _retract = _insert.Select(w => w with { Weight = -1 }).ToArray();
    }

    [Benchmark(OperationsPerInvoke = DeltaSize * 2)]
    public async Task InsertThenRetractDelta()
    {
        await _input.SendAsync(_insert);
        await _rig.StepAsync();
        await _input.SendAsync(_retract);
        await _rig.StepAsync();
    }

    [GlobalCleanup]
    public Task Cleanup() => _rig.DisposeAsync().AsTask();
}

/// <summary>
/// Tournament-tree MIN, worst case: insert a new group minimum, then delete it. Per-op cost
/// should be independent of GroupSize (a handful of small-bucket scans, not a group rescan).
/// </summary>
[MemoryDiagnoser]
public class MinBenchmarks
{
    private const int Groups = 16;

    [Params(100, 10_000)]
    public int GroupSize;

    private Rig _rig = null!;
    private InputHandle<(int Key, int Value)> _input = null!;
    private WeightedValue<(int Key, int Value)>[] _insert = null!;
    private WeightedValue<(int Key, int Value)>[] _retract = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        var builder = new CircuitBuilder(Rig.Options());
        var rows = builder.Input<(int Key, int Value)>("rows");
        var mins = rows
            .GroupBy(r => r.Key)
            .Min(r => r.Value)
            .ToOutput(bootstrap: false); // measure the aggregate, not snapshot maintenance

        var circuit = builder.Build();
        circuit.Start();
        _rig = new Rig(circuit);
        _rig.Watch(mins);
        _input = circuit.Handle(rows);

        await _rig.FeedAsync(_input,
            from key in Enumerable.Range(0, Groups)
            from value in Enumerable.Range(1, GroupSize)
            select new WeightedValue<(int, int)>((key, value), 1));

        _insert = Enumerable.Range(0, Groups)
            .Select(key => new WeightedValue<(int, int)>((key, 0), 1))
            .ToArray();
        _retract = _insert.Select(w => w with { Weight = -1 }).ToArray();
    }

    [Benchmark(OperationsPerInvoke = Groups * 2)]
    public async Task NewMinimumThenDeleteIt()
    {
        await _input.SendAsync(_insert);
        await _rig.StepAsync();
        await _input.SendAsync(_retract);
        await _rig.StepAsync();
    }

    [GlobalCleanup]
    public Task Cleanup() => _rig.DisposeAsync().AsTask();
}

/// <summary>TopK membership churn: a new top row enters (displacing the K-th) and leaves again.</summary>
[MemoryDiagnoser]
public class TopKBenchmarks
{
    private const int Groups = 16;
    private const int K = 10;

    [Params(1_000, 10_000)]
    public int GroupSize;

    private Rig _rig = null!;
    private InputHandle<(int Key, int Score)> _input = null!;
    private WeightedValue<(int Key, int Score)>[] _insert = null!;
    private WeightedValue<(int Key, int Score)>[] _retract = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        var builder = new CircuitBuilder(Rig.Options());
        var rows = builder.Input<(int Key, int Score)>("rows");
        var top = rows
            .GroupBy(r => r.Key)
            .TopK(K, r => r.Score)
            .ToOutput(bootstrap: false); // measure the operator, not snapshot maintenance

        var circuit = builder.Build();
        circuit.Start();
        _rig = new Rig(circuit);
        _rig.Watch(top);
        _input = circuit.Handle(rows);

        await _rig.FeedAsync(_input,
            from key in Enumerable.Range(0, Groups)
            from score in Enumerable.Range(1, GroupSize)
            select new WeightedValue<(int, int)>((key, score), 1));

        _insert = Enumerable.Range(0, Groups)
            .Select(key => new WeightedValue<(int, int)>((key, GroupSize + 1), 1))
            .ToArray();
        _retract = _insert.Select(w => w with { Weight = -1 }).ToArray();
    }

    [Benchmark(OperationsPerInvoke = Groups * 2)]
    public async Task NewTopRowThenDeleteIt()
    {
        await _input.SendAsync(_insert);
        await _rig.StepAsync();
        await _input.SendAsync(_retract);
        await _rig.StepAsync();
    }

    [GlobalCleanup]
    public Task Cleanup() => _rig.DisposeAsync().AsTask();
}
