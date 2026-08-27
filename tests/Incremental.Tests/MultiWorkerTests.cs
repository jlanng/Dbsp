namespace Incremental.Tests;

/// <summary>
/// Circuits running with several workers and the auto-stepping pump. Step boundaries are
/// nondeterministic here, so assertions integrate all output deltas and check the final
/// state — which must be identical regardless of how the input was batched or sharded.
/// </summary>
public class MultiWorkerTests
{
    [Fact]
    public async Task Grouped_count_is_correct_across_workers()
    {
        var builder = new CircuitBuilder(new CircuitOptions { Workers = 4 });
        builder.Input<string>("words")
            .IndexBy(w => w)
            .Count()
            .Output("counts");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<string>("words");
        var output = circuit.Output<KeyedValue<string, long>>("counts");

        var expected = new Dictionary<string, long>();
        for (var i = 0; i < 5000; i++)
        {
            var word = $"word-{i % 37}";
            expected[word] = expected.GetValueOrDefault(word) + 1;
            await input.InsertAsync(word);
        }
        input.Complete();

        var integrated = new ZSet<KeyedValue<string, long>>();
        await foreach (var delta in output.ReadAllAsync())
            integrated.AddAll(delta);

        // After integration exactly one (word, count) row per word remains, with weight +1.
        Assert.Equal(expected.Count, integrated.DistinctCount);
        foreach (var (word, count) in expected)
            Assert.Equal(1, integrated.WeightOf(new KeyedValue<string, long>(word, count)));
    }

    [Fact]
    public async Task Join_after_exchange_matches_across_workers()
    {
        var builder = new CircuitBuilder(new CircuitOptions { Workers = 4 });
        var left = builder.Input<(int Key, string Tag)>("left");
        var right = builder.Input<(int Key, int Value)>("right");

        left.IndexBy(l => l.Key)
            .Join(right.IndexBy(r => r.Key), (key, l, r) => (key, l.Tag, r.Value))
            .Output("joined");

        await using var circuit = builder.Build();
        circuit.Start();
        var leftIn = circuit.Input<(int Key, string Tag)>("left");
        var rightIn = circuit.Input<(int Key, int Value)>("right");
        var output = circuit.Output<(int Key, string Tag, int Value)>("joined");

        const int keys = 100;
        for (var k = 0; k < keys; k++)
        {
            await leftIn.InsertAsync((k, $"tag-{k}"));
            await rightIn.InsertAsync((k, k * 2));
        }
        leftIn.Complete();
        rightIn.Complete();

        var integrated = new ZSet<(int, string, int)>();
        await foreach (var delta in output.ReadAllAsync())
            integrated.AddAll(delta);

        Assert.Equal(keys, integrated.DistinctCount);
        for (var k = 0; k < keys; k++)
            Assert.Equal(1, integrated.WeightOf((k, $"tag-{k}", k * 2)));
    }

    [Fact]
    public async Task Distinct_state_is_consistent_across_workers()
    {
        var builder = new CircuitBuilder(new CircuitOptions { Workers = 4 });
        builder.Input<int>("values").Distinct().Output("distinct");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<int>("values");
        var output = circuit.Output<int>("distinct");

        // Every value inserted three times; round-robin input placement would split copies
        // across workers if Distinct did not shuffle by value first.
        for (var round = 0; round < 3; round++)
            for (var v = 0; v < 200; v++)
                await input.InsertAsync(v);
        input.Complete();

        var integrated = new ZSet<int>();
        await foreach (var delta in output.ReadAllAsync())
            integrated.AddAll(delta);

        Assert.Equal(200, integrated.DistinctCount);
        for (var v = 0; v < 200; v++)
            Assert.Equal(1, integrated.WeightOf(v));
    }
}
