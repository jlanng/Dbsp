using BenchmarkDotNet.Attributes;

namespace Incremental.Benchmarks;

/// <summary>Stateless operator throughput: one step pushing a batch through filter + map.</summary>
[MemoryDiagnoser]
public class PipelineBenchmarks
{
    private const int BatchSize = 10_000;

    private Rig _rig = null!;
    private InputHandle<int> _input = null!;
    private WeightedValue<int>[] _batch = null!;

    [GlobalSetup]
    public void Setup()
    {
        var builder = new CircuitBuilder(Rig.Options());
        builder.Input<int>("numbers")
            .Filter(n => (n & 1) == 0)
            .Map(n => n * 3)
            .Output("result");

        var circuit = builder.Build();
        circuit.Start();
        _rig = new Rig(circuit);
        _rig.Watch<int>("result");
        _input = circuit.Input<int>("numbers");
        _batch = Enumerable.Range(0, BatchSize).Select(i => new WeightedValue<int>(i, 1)).ToArray();
    }

    [Benchmark(OperationsPerInvoke = BatchSize)]
    public async Task FilterMapBatch()
    {
        await _input.SendAsync(_batch);
        await _rig.StepAsync();
    }

    [GlobalCleanup]
    public Task Cleanup() => _rig.DisposeAsync().AsTask();
}
