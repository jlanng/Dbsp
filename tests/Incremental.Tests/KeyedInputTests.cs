namespace Incremental.Tests;

public class KeyedInputTests
{
    [Fact]
    public async Task Upsert_retracts_the_previous_row_and_delete_retracts_the_current_one()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        builder.KeyedInput<int, (int Id, string Name)>("rows", r => r.Id).Output("out");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.KeyedInput<int, (int Id, string Name)>("rows");
        var output = circuit.Output<(int Id, string Name)>("out");

        await input.UpsertAsync((1, "a"));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf((1, "a")));

        // Replacing the row emits the retraction automatically.
        await input.UpsertAsync((1, "b"));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf((1, "a")));
        Assert.Equal(1, delta.WeightOf((1, "b")));

        // Upserting an identical row is a no-op: the step produces no delta.
        await input.UpsertAsync((1, "b"));
        await circuit.StepAsync();
        Assert.False(output.Reader.TryRead(out _));

        await input.DeleteAsync(1);
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf((1, "b")));

        // Deleting an absent key is a no-op.
        await input.DeleteAsync(1);
        await circuit.StepAsync();
        Assert.False(output.Reader.TryRead(out _));
    }

    [Fact]
    public async Task Upserted_reference_rows_reattribute_joined_results()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var orders = builder.Input<(int Id, int Cust)>("orders");
        var customers = builder.KeyedInput<int, (int Id, string Region)>("customers", c => c.Id);

        orders.IndexBy(o => o.Cust)
            .Join(customers.IndexBy(c => c.Id), (_, o, c) => (o.Id, c.Region))
            .IndexBy(x => x.Region)
            .Count()
            .Output("by-region");

        await using var circuit = builder.Build();
        circuit.Start();
        var ordersIn = circuit.Input<(int Id, int Cust)>("orders");
        var customersIn = circuit.KeyedInput<int, (int Id, string Region)>("customers");
        var output = circuit.Output<KeyedValue<string, long>>("by-region");

        await customersIn.UpsertAsync((7, "emea"));
        await ordersIn.InsertAsync((1, 7));
        await ordersIn.InsertAsync((2, 7));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, long>("emea", 2)));

        // The customer moves region: one upsert re-attributes both historical orders.
        await customersIn.UpsertAsync((7, "apac"));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(new KeyedValue<string, long>("emea", 2)));
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, long>("apac", 2)));
    }
}
