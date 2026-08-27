namespace Incremental.Tests;

public class WindowingAndTopKTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static CircuitBuilder ManualBuilder() =>
        new(new CircuitOptions { AutoStep = false });

    [Fact]
    public async Task Tumbling_window_counts_update_on_late_events()
    {
        var builder = ManualBuilder();
        builder.Input<(string Page, DateTimeOffset At)>("views")
            .TumblingWindow(TimeSpan.FromSeconds(5), v => v.At)
            .IndexBy(w => (w.WindowStart, w.Value.Page))
            .Count()
            .Output("counts");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string Page, DateTimeOffset At)>("views");
        var output = circuit.Output<KeyedValue<(DateTimeOffset, string), long>>("counts");

        await input.InsertAsync(("home", T0.AddSeconds(1)));
        await input.InsertAsync(("home", T0.AddSeconds(3)));
        await input.InsertAsync(("home", T0.AddSeconds(7)));
        await circuit.StepAsync();

        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new KeyedValue<(DateTimeOffset, string), long>((T0, "home"), 2)));
        Assert.Equal(1, delta.WeightOf(new KeyedValue<(DateTimeOffset, string), long>((T0.AddSeconds(5), "home"), 1)));

        // A late event for the first window: its count is corrected via retraction.
        await input.InsertAsync(("home", T0.AddSeconds(2)));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(new KeyedValue<(DateTimeOffset, string), long>((T0, "home"), 2)));
        Assert.Equal(1, delta.WeightOf(new KeyedValue<(DateTimeOffset, string), long>((T0, "home"), 3)));
    }

    [Fact]
    public async Task Hopping_windows_assign_a_record_to_every_covering_window()
    {
        var builder = ManualBuilder();
        builder.Input<DateTimeOffset>("events")
            .HoppingWindow(TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(5), t => t)
            .Map(w => w.WindowStart)
            .Output("windows");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<DateTimeOffset>("events");
        var output = circuit.Output<DateTimeOffset>("windows");

        await input.InsertAsync(T0.AddSeconds(2));
        await circuit.StepAsync();

        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(T0));
        Assert.Equal(1, delta.WeightOf(T0.AddSeconds(-5)));
        Assert.Equal(2, delta.DistinctCount);
    }

    [Fact]
    public async Task TopK_retracts_displaced_rows_and_recovers_on_deletion()
    {
        var builder = ManualBuilder();
        builder.Input<(string Grp, string Name, int Score)>("scores")
            .IndexBy(s => s.Grp)
            .TopK(2, s => s.Score)
            .Output("top");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string Grp, string Name, int Score)>("scores");
        var output = circuit.Output<KeyedValue<string, (string Grp, string Name, int Score)>>("top");

        static KeyedValue<string, (string, string, int)> Row(string name, int score) =>
            new("g", ("g", name, score));

        await input.InsertAsync(("g", "a", 10));
        await input.InsertAsync(("g", "b", 20));
        await input.InsertAsync(("g", "c", 30));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Row("c", 30)));
        Assert.Equal(1, delta.WeightOf(Row("b", 20)));
        Assert.Equal(2, delta.DistinctCount);

        // A new leader displaces the current second place.
        await input.InsertAsync(("g", "d", 40));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Row("d", 40)));
        Assert.Equal(-1, delta.WeightOf(Row("b", 20)));

        // Deleting the leader lets the runner-up back in.
        await input.RemoveAsync(("g", "d", 40));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(Row("d", 40)));
        Assert.Equal(1, delta.WeightOf(Row("b", 20)));
    }

    [Fact]
    public async Task TopK_below_cutoff_changes_are_absorbed_silently_but_integrated()
    {
        var builder = ManualBuilder();
        builder.Input<(string Grp, string Name, int Score)>("scores")
            .IndexBy(s => s.Grp)
            .TopK(2, s => s.Score)
            .Output("top");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string Grp, string Name, int Score)>("scores");
        var output = circuit.Output<KeyedValue<string, (string Grp, string Name, int Score)>>("top");

        await input.InsertAsync(("g", "a", 30));
        await input.InsertAsync(("g", "b", 20));
        await circuit.StepAsync();
        _ = await output.Reader.ReadAsync();

        // Strictly below the cutoff (20) with a full top: the short-circuit path — no delta.
        await input.InsertAsync(("g", "c", 10));
        await circuit.StepAsync();
        Assert.False(output.Reader.TryRead(out _));

        // But the group state was still integrated: deleting a top row promotes c correctly.
        await input.RemoveAsync(("g", "a", 30));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(new KeyedValue<string, (string, string, int)>("g", ("g", "a", 30))));
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, (string, string, int)>("g", ("g", "c", 10))));
    }

    [Fact]
    public async Task TopK_counts_multiplicity_toward_k()
    {
        var builder = ManualBuilder();
        builder.Input<(string Grp, int Score)>("scores")
            .IndexBy(s => s.Grp)
            .TopK(3, s => s.Score)
            .Output("top");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string Grp, int Score)>("scores");
        var output = circuit.Output<KeyedValue<string, (string Grp, int Score)>>("top");

        // Two copies of the best row occupy two of the three ranks.
        await input.InsertAsync(("g", 50));
        await input.InsertAsync(("g", 50));
        await input.InsertAsync(("g", 40));
        await input.InsertAsync(("g", 30));
        await circuit.StepAsync();

        var delta = await output.Reader.ReadAsync();
        Assert.Equal(2, delta.WeightOf(new KeyedValue<string, (string, int)>("g", ("g", 50))));
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, (string, int)>("g", ("g", 40))));
        Assert.Equal(0, delta.WeightOf(new KeyedValue<string, (string, int)>("g", ("g", 30))));
    }
}
