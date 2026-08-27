namespace Incremental.Benchmarks.Contracts;

/// <summary>
/// One measurable workload for a performance contract: a circuit with preloaded operator
/// state whose size is the scenario's scale, plus a fixed-size delta that
/// <see cref="RunCycleAsync"/> asserts and retracts, leaving state at its baseline.
/// </summary>
internal interface IDeltaScenario : IAsyncDisposable
{
    Task RunCycleAsync();
}

/// <summary>Shared shape: send an insert batch, step, send its retraction, step.</summary>
internal sealed class CycleScenario<T> : IDeltaScenario where T : notnull
{
    private readonly Rig _rig;
    private readonly InputHandle<T> _input;
    private readonly WeightedValue<T>[] _insert;
    private readonly WeightedValue<T>[] _retract;

    public CycleScenario(Rig rig, InputHandle<T> input, WeightedValue<T>[] insert)
    {
        _rig = rig;
        _input = input;
        _insert = insert;
        _retract = insert.Select(w => w with { Weight = -w.Weight }).ToArray();
    }

    public async Task RunCycleAsync()
    {
        await _input.SendAsync(_insert);
        await _rig.StepAsync();
        await _input.SendAsync(_retract);
        await _rig.StepAsync();
    }

    public ValueTask DisposeAsync() => _rig.DisposeAsync();
}

internal static class Scenarios
{
    private const int DeltaSize = 256;

    /// <summary>Join delta against <paramref name="stateSize"/> rows per side; every delta
    /// order matches exactly one customer, so output work is identical at both scales.</summary>
    public static async Task<IDeltaScenario> JoinAsync(int stateSize)
    {
        var builder = new CircuitBuilder(Rig.Options());
        var orders = builder.Input<(int OrderId, int CustomerId)>("orders");
        var customers = builder.Input<(int CustomerId, int Rank)>("customers");
        orders.IndexBy(o => o.CustomerId)
            .Join(customers.IndexBy(c => c.CustomerId), (_, order, customer) => (order.OrderId, customer.Rank))
            .Output("joined");

        var circuit = builder.Build();
        circuit.Start();
        var rig = new Rig(circuit);
        rig.Watch<(int OrderId, int Rank)>("joined");

        var ordersIn = circuit.Input<(int OrderId, int CustomerId)>("orders");
        await rig.FeedAsync(circuit.Input<(int CustomerId, int Rank)>("customers"),
            Enumerable.Range(0, stateSize).Select(i => new WeightedValue<(int, int)>((i, i), 1)));
        await rig.FeedAsync(ordersIn,
            Enumerable.Range(0, stateSize).Select(i => new WeightedValue<(int, int)>((i, i), 1)));

        var insert = Enumerable.Range(0, DeltaSize)
            .Select(i => new WeightedValue<(int, int)>((stateSize + i, i * 37 % stateSize), 1))
            .ToArray();
        return new CycleScenario<(int OrderId, int CustomerId)>(rig, ordersIn, insert);
    }

    /// <summary>Linear SUM delta touching a fixed set of keys among <paramref name="groupCount"/> groups.</summary>
    public static async Task<IDeltaScenario> SumAsync(int groupCount)
    {
        var builder = new CircuitBuilder(Rig.Options());
        builder.Input<(int Key, long Amount)>("rows")
            .IndexBy(r => r.Key)
            .Sum(r => r.Amount)
            .Output("sums");

        var circuit = builder.Build();
        circuit.Start();
        var rig = new Rig(circuit);
        rig.Watch<KeyedValue<int, long>>("sums");

        var input = circuit.Input<(int Key, long Amount)>("rows");
        await rig.FeedAsync(input,
            Enumerable.Range(0, groupCount).Select(i => new WeightedValue<(int, long)>((i, i), 1)));

        var insert = Enumerable.Range(0, DeltaSize)
            .Select(i => new WeightedValue<(int, long)>((i * 37 % groupCount, 5), 1))
            .ToArray();
        return new CycleScenario<(int Key, long Amount)>(rig, input, insert);
    }

    /// <summary>Tournament-tree MIN worst case (new minimum in, then deleted) against groups
    /// of <paramref name="groupSize"/> values.</summary>
    public static async Task<IDeltaScenario> MinAsync(int groupSize)
    {
        const int groups = 16;
        var builder = new CircuitBuilder(Rig.Options());
        builder.Input<(int Key, int Value)>("rows")
            .IndexBy(r => r.Key)
            .Min(r => r.Value)
            .Output("mins");

        var circuit = builder.Build();
        circuit.Start();
        var rig = new Rig(circuit);
        rig.Watch<KeyedValue<int, int>>("mins");

        var input = circuit.Input<(int Key, int Value)>("rows");
        await rig.FeedAsync(input,
            from key in Enumerable.Range(0, groups)
            from value in Enumerable.Range(1, groupSize)
            select new WeightedValue<(int, int)>((key, value), 1));

        var insert = Enumerable.Range(0, groups)
            .Select(key => new WeightedValue<(int, int)>((key, 0), 1))
            .ToArray();
        return new CycleScenario<(int Key, int Value)>(rig, input, insert);
    }

    /// <summary>Keyed distinct: new values entering and leaving keys that already hold
    /// <paramref name="valuesPerKey"/> other values.</summary>
    public static async Task<IDeltaScenario> KeyedDistinctAsync(int valuesPerKey)
    {
        const int keys = 16;
        var builder = new CircuitBuilder(Rig.Options());
        builder.Input<(int Key, int Value)>("rows")
            .IndexBy(r => r.Key)
            .Distinct()
            .Output("distinct");

        var circuit = builder.Build();
        circuit.Start();
        var rig = new Rig(circuit);
        rig.Watch<KeyedValue<int, (int Key, int Value)>>("distinct");

        var input = circuit.Input<(int Key, int Value)>("rows");
        await rig.FeedAsync(input,
            from key in Enumerable.Range(0, keys)
            from value in Enumerable.Range(0, valuesPerKey)
            select new WeightedValue<(int, int)>((key, value), 1));

        var insert = Enumerable.Range(0, keys)
            .Select(key => new WeightedValue<(int, int)>((key, -1 - key), 1))
            .ToArray();
        return new CycleScenario<(int Key, int Value)>(rig, input, insert);
    }
}
