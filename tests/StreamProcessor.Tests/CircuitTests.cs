namespace StreamProcessor.Tests;

/// <summary>
/// Single-worker circuits driven with AutoStep = false, so each test controls step
/// boundaries and can assert exact per-step deltas.
/// </summary>
public class CircuitTests
{
    private static CircuitBuilder ManualBuilder() =>
        new(new CircuitOptions { AutoStep = false });

    [Fact]
    public async Task Map_and_filter_transform_deltas()
    {
        var builder = ManualBuilder();
        builder.Input<int>("numbers")
            .Filter(n => n % 2 == 0)
            .Map(n => n * 10)
            .Output("result");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<int>("numbers");
        var output = circuit.Output<int>("result");

        await input.InsertAsync(1);
        await input.InsertAsync(2);
        await input.InsertAsync(4);
        await circuit.StepAsync();

        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(20));
        Assert.Equal(1, delta.WeightOf(40));
        Assert.Equal(2, delta.DistinctCount);
    }

    [Fact]
    public async Task Join_is_incremental_across_steps()
    {
        var builder = ManualBuilder();
        var orders = builder.Input<(int OrderId, int CustomerId)>("orders");
        var customers = builder.Input<(int Id, string Name)>("customers");

        orders.IndexBy(o => o.CustomerId)
            .Join(customers.IndexBy(c => c.Id), (_, order, customer) => (order.OrderId, customer.Name))
            .Output("joined");

        await using var circuit = builder.Build();
        circuit.Start();
        var ordersIn = circuit.Input<(int OrderId, int CustomerId)>("orders");
        var customersIn = circuit.Input<(int Id, string Name)>("customers");
        var joined = circuit.Output<(int OrderId, string Name)>("joined");

        // Left side arrives first: no matches yet, so no delta is published.
        await ordersIn.InsertAsync((100, 1));
        await circuit.StepAsync();

        // The matching right side arrives a step later and joins against integrated state.
        await customersIn.InsertAsync((1, "ada"));
        await circuit.StepAsync();
        var delta = await joined.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf((100, "ada")));

        // A second order joins immediately.
        await ordersIn.InsertAsync((101, 1));
        await circuit.StepAsync();
        delta = await joined.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf((101, "ada")));

        // Deleting the customer retracts every joined row.
        await customersIn.RemoveAsync((1, "ada"));
        await circuit.StepAsync();
        delta = await joined.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf((100, "ada")));
        Assert.Equal(-1, delta.WeightOf((101, "ada")));
    }

    [Fact]
    public async Task Count_retracts_stale_results()
    {
        var builder = ManualBuilder();
        builder.Input<string>("words")
            .IndexBy(w => w)
            .Count()
            .Output("counts");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<string>("words");
        var output = circuit.Output<KeyedValue<string, long>>("counts");

        await input.InsertAsync("a");
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, long>("a", 1)));

        await input.InsertAsync("a");
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(new KeyedValue<string, long>("a", 1)));
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, long>("a", 2)));

        await input.RemoveAsync("a");
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(new KeyedValue<string, long>("a", 2)));
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, long>("a", 1)));
    }

    [Fact]
    public async Task Min_recovers_after_deleting_the_minimum()
    {
        var builder = ManualBuilder();
        builder.Input<(string Key, int Value)>("readings")
            .IndexBy(r => r.Key)
            .Min(r => r.Value)
            .Output("minimums");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string Key, int Value)>("readings");
        var output = circuit.Output<KeyedValue<string, int>>("minimums");

        await input.InsertAsync(("k", 5));
        await input.InsertAsync(("k", 3));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, int>("k", 3)));

        // Deleting the current minimum is the classic non-invertible case: the operator must
        // consult the remaining group state to find the new minimum.
        await input.RemoveAsync(("k", 3));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(new KeyedValue<string, int>("k", 3)));
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, int>("k", 5)));
    }

    [Fact]
    public async Task Distinct_collapses_duplicates_and_tracks_presence()
    {
        var builder = ManualBuilder();
        builder.Input<string>("values").Distinct().Output("distinct");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<string>("values");
        var output = circuit.Output<string>("distinct");

        await input.InsertAsync("x");
        await input.InsertAsync("x");
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf("x"));

        // Removing one copy leaves the value present: no delta at all.
        await input.RemoveAsync("x");
        await circuit.StepAsync();
        Assert.False(output.Reader.TryRead(out _));

        // Removing the last copy retracts it.
        await input.RemoveAsync("x");
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf("x"));
    }

    [Fact]
    public async Task Full_input_channel_suspends_the_producer()
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false,
            InputChannelCapacity = 2,
        });
        builder.Input<int>("numbers").Output("result");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<int>("numbers");

        await input.InsertAsync(1);
        await input.InsertAsync(2);

        // The channel is full: the third write must not complete until a step drains it.
        var blocked = input.InsertAsync(3);
        Assert.False(blocked.IsCompleted);

        await circuit.StepAsync();
        await blocked;
    }
}
