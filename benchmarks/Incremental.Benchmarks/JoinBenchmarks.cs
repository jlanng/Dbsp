using BenchmarkDotNet.Attributes;

namespace Incremental.Benchmarks;

/// <summary>
/// Incremental join: cost of a fixed-size delta (insert + retract, so state returns to its
/// baseline every invocation) against preloaded state of varying size. The two StateSize
/// rows should report near-identical per-op cost — delta cost is O(delta), not O(state).
/// </summary>
[MemoryDiagnoser]
public class JoinBenchmarks
{
    private const int DeltaSize = 1_000;

    [Params(1_000, 100_000)]
    public int StateSize;

    private Rig _rig = null!;
    private InputHandle<(int OrderId, int CustomerId)> _orders = null!;
    private WeightedValue<(int OrderId, int CustomerId)>[] _insert = null!;
    private WeightedValue<(int OrderId, int CustomerId)>[] _retract = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        var builder = new CircuitBuilder(Rig.Options());
        var orders = builder.Input<(int OrderId, int CustomerId)>("orders");
        var customers = builder.Input<(int CustomerId, string Name)>("customers");

        var joined = orders.GroupBy(o => o.CustomerId)
            .Join(customers.GroupBy(c => c.CustomerId), (_, order, customer) => (order.OrderId, customer.Name))
            .ToOutput();

        var circuit = builder.Build();
        circuit.Start();
        _rig = new Rig(circuit);
        _rig.Watch(joined);
        _orders = circuit.Handle(orders);

        var customersIn = circuit.Handle(customers);
        await _rig.FeedAsync(customersIn, Enumerable.Range(0, StateSize)
            .Select(i => new WeightedValue<(int, string)>((i, "customer"), 1)));
        await _rig.FeedAsync(_orders, Enumerable.Range(0, StateSize)
            .Select(i => new WeightedValue<(int, int)>((i, i), 1)));

        // Each delta order matches exactly one customer, so output work is state-independent.
        _insert = Enumerable.Range(0, DeltaSize)
            .Select(i => new WeightedValue<(int, int)>((StateSize + i, i * 37 % StateSize), 1))
            .ToArray();
        _retract = _insert.Select(w => w with { Weight = -1 }).ToArray();
    }

    [Benchmark(OperationsPerInvoke = DeltaSize * 2)]
    public async Task InsertThenRetractDelta()
    {
        await _orders.SendAsync(_insert);
        await _rig.StepAsync();
        await _orders.SendAsync(_retract);
        await _rig.StepAsync();
    }

    [GlobalCleanup]
    public Task Cleanup() => _rig.DisposeAsync().AsTask();
}
