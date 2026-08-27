namespace Incremental.Tests;

public class AntiJoinTests
{
    private static CircuitBuilder ManualBuilder() =>
        new(new CircuitOptions { AutoStep = false });

    [Fact]
    public async Task AntiJoin_tracks_match_presence_with_retractions()
    {
        var builder = ManualBuilder();
        var orders = builder.Input<(int Id, int Cust)>("orders");
        var customers = builder.Input<int>("customers");

        orders.IndexBy(o => o.Cust)
            .AntiJoin(customers.IndexBy(c => c))
            .Output("orphans");

        await using var circuit = builder.Build();
        circuit.Start();
        var ordersIn = circuit.Input<(int Id, int Cust)>("orders");
        var customersIn = circuit.Input<int>("customers");
        var output = circuit.Output<KeyedValue<int, (int Id, int Cust)>>("orphans");

        static KeyedValue<int, (int, int)> Row(int id) => new(7, (id, 7));

        // No matching customer: the order is an orphan.
        await ordersIn.InsertAsync((1, 7));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Row(1)));

        // The customer arrives: all of the key's orphans are retracted.
        await ordersIn.InsertAsync((2, 7));
        await circuit.StepAsync();
        _ = await output.Reader.ReadAsync();
        await customersIn.InsertAsync(7);
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(Row(1)));
        Assert.Equal(-1, delta.WeightOf(Row(2)));

        // The customer is deleted again: the orphans come back.
        await customersIn.RemoveAsync(7);
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Row(1)));
        Assert.Equal(1, delta.WeightOf(Row(2)));
    }

    [Fact]
    public async Task LeftJoin_switches_between_unmatched_and_matched_rows()
    {
        var builder = ManualBuilder();
        var orders = builder.Input<(int Id, int Cust)>("orders");
        var customers = builder.Input<(int Id, string Name)>("customers");

        orders.IndexBy(o => o.Cust)
            .LeftJoin(
                customers.IndexBy(c => c.Id),
                matched: (_, order, customer) => (order.Id, customer.Name),
                unmatched: (_, order) => (order.Id, "<none>"))
            .Output("joined");

        await using var circuit = builder.Build();
        circuit.Start();
        var ordersIn = circuit.Input<(int Id, int Cust)>("orders");
        var customersIn = circuit.Input<(int Id, string Name)>("customers");
        var output = circuit.Output<(int Id, string Name)>("joined");

        await ordersIn.InsertAsync((1, 7));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf((1, "<none>")));

        // Match appears: the placeholder row is swapped for the joined row.
        await customersIn.InsertAsync((7, "ada"));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf((1, "<none>")));
        Assert.Equal(1, delta.WeightOf((1, "ada")));
    }

    [Fact]
    public async Task FullJoin_covers_both_unmatched_sides()
    {
        var builder = ManualBuilder();
        var left = builder.Input<(int Key, string Val)>("left");
        var right = builder.Input<(int Key, string Val)>("right");

        left.IndexBy(l => l.Key)
            .FullJoin(
                right.IndexBy(r => r.Key),
                matched: (key, l, r) => $"{key}:{l.Val}+{r.Val}",
                leftOnly: (key, l) => $"{key}:{l.Val}+?",
                rightOnly: (key, r) => $"{key}:?+{r.Val}")
            .Output("joined");

        await using var circuit = builder.Build();
        circuit.Start();
        var leftIn = circuit.Input<(int Key, string Val)>("left");
        var rightIn = circuit.Input<(int Key, string Val)>("right");
        var output = circuit.Output<string>("joined");

        await leftIn.InsertAsync((1, "a"));
        await rightIn.InsertAsync((2, "b"));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf("1:a+?"));
        Assert.Equal(1, delta.WeightOf("2:?+b"));

        // Key 2 gains a left row: right-only placeholder swaps for the matched row.
        await leftIn.InsertAsync((2, "c"));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf("2:?+b"));
        Assert.Equal(1, delta.WeightOf("2:c+b"));
    }
}
