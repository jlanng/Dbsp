namespace Dbsp.Tests;

public class WindowingAndTopKTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static CircuitBuilder ManualBuilder() =>
        new(new CircuitOptions { AutoStep = false });

    [Fact]
    public async Task Tumbling_window_counts_update_on_late_events()
    {
        var builder = ManualBuilder();
        var views = builder.Input<(string Page, DateTimeOffset At)>("views");
        var countsDef = views
            // Generous lateness: this test is about correction, not dropping.
            .WithEventTime(v => v.At, lateness: TimeSpan.FromHours(1))
            .GroupBy(v => v.Page)
            .TumblingWindow(TimeSpan.FromSeconds(5))
            .Count()
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(views);
        var output = circuit.Handle(countsDef);

        static WindowedValue<string, long> Row(DateTimeOffset start, long count) =>
            new(start, start.AddSeconds(5), "home", count);

        await input.InsertAsync(("home", T0.AddSeconds(1)));
        await input.InsertAsync(("home", T0.AddSeconds(3)));
        await input.InsertAsync(("home", T0.AddSeconds(7)));
        await circuit.StepAsync();

        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Row(T0, 2)));
        Assert.Equal(1, delta.WeightOf(Row(T0.AddSeconds(5), 1)));

        // A late event for the first window: its count is corrected via retraction.
        await input.InsertAsync(("home", T0.AddSeconds(2)));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(Row(T0, 2)));
        Assert.Equal(1, delta.WeightOf(Row(T0, 3)));
    }

    [Fact]
    public async Task Hopping_windows_assign_a_record_to_every_covering_window()
    {
        var builder = ManualBuilder();
        var events = builder.Input<DateTimeOffset>("events");
        var windowsDef = events
            .WithEventTime(t => t, lateness: TimeSpan.FromHours(1))
            .GroupBy(_ => 0)
            .HoppingWindow(TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(5))
            .Count()
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(events);
        var output = circuit.Handle(windowsDef);

        await input.InsertAsync(T0.AddSeconds(2));
        await circuit.StepAsync();

        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new WindowedValue<int, long>(T0, T0.AddSeconds(10), 0, 1)));
        Assert.Equal(1, delta.WeightOf(new WindowedValue<int, long>(T0.AddSeconds(-5), T0.AddSeconds(5), 0, 1)));
        Assert.Equal(2, delta.DistinctCount);
    }

    [Fact]
    public async Task TopK_retracts_displaced_rows_and_recovers_on_deletion()
    {
        var builder = ManualBuilder();
        var scores = builder.Input<(string Grp, string Name, int Score)>("scores");
        var topDef = scores
            .GroupBy(s => s.Grp)
            .TopK(2, s => s.Score)
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(scores);
        var output = circuit.Handle(topDef);

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
        var scores = builder.Input<(string Grp, string Name, int Score)>("scores");
        var topDef = scores
            .GroupBy(s => s.Grp)
            .TopK(2, s => s.Score)
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(scores);
        var output = circuit.Handle(topDef);

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
        var scores = builder.Input<(string Grp, int Score)>("scores");
        var topDef = scores
            .GroupBy(s => s.Grp)
            .TopK(3, s => s.Score)
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(scores);
        var output = circuit.Handle(topDef);

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

    [Fact]
    public async Task Windowed_TopK_ranks_within_each_window_across_keys()
    {
        var builder = ManualBuilder();
        var views = builder.Input<(string Page, DateTimeOffset At)>("views");
        var topDef = views
            .WithEventTime(v => v.At, lateness: TimeSpan.FromHours(1))
            .GroupBy(v => v.Page)
            .TumblingWindow(TimeSpan.FromSeconds(5))
            .Count()
            .TopK(2, row => row.Value)
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(views);
        var output = circuit.Handle(topDef);

        static WindowedValue<string, long> Row(string page, long count) =>
            new(T0, T0.AddSeconds(5), page, count);

        // Three pages in one window: only the top two survive the ranking.
        await input.InsertAsync(("home", T0.AddSeconds(1)));
        await input.InsertAsync(("home", T0.AddSeconds(2)));
        await input.InsertAsync(("home", T0.AddSeconds(3)));
        await input.InsertAsync(("docs", T0.AddSeconds(1)));
        await input.InsertAsync(("docs", T0.AddSeconds(2)));
        await input.InsertAsync(("blog", T0.AddSeconds(4)));
        await circuit.StepAsync();

        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Row("home", 3)));
        Assert.Equal(1, delta.WeightOf(Row("docs", 2)));
        Assert.Equal(0, delta.WeightOf(Row("blog", 1)));

        // Blog overtakes docs: the leaderboard re-ranks incrementally.
        await input.InsertAsync(("blog", T0.AddSeconds(1)));
        await input.InsertAsync(("blog", T0.AddSeconds(2)));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Row("blog", 3)));
        Assert.Equal(-1, delta.WeightOf(Row("docs", 2)));
    }
}
