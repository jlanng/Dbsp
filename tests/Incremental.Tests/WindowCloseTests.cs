namespace Incremental.Tests;

public class WindowCloseTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static CircuitBuilder ManualBuilder() =>
        new(new CircuitOptions { AutoStep = false });

    [Fact]
    public async Task Close_fires_once_the_watermark_passes_the_deadline_and_never_again()
    {
        var builder = ManualBuilder();
        var views = builder.Input<(string Page, DateTimeOffset At)>("views");
        var closesDef = views
            .WithEventTime(v => v.At, lateness: TimeSpan.Zero)
            .Tumble(TimeSpan.FromSeconds(5), v => v.At)
            .GroupBy(w => w.WindowStart)
            .WindowCloses(windowStart => windowStart.AddSeconds(5))
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(views);
        var output = circuit.Handle(closesDef);

        // Step 1 populates window T0; no watermark is frozen yet, so nothing closes.
        await input.InsertAsync(("home", T0.AddSeconds(1)));
        await circuit.StepAsync();
        Assert.False(output.Reader.TryRead(out _));

        // Step 2 sees watermark T0+1s (< T0+5s): window T0 is still open. Freezes T0+7s.
        await input.InsertAsync(("home", T0.AddSeconds(7)));
        await circuit.StepAsync();
        Assert.False(output.Reader.TryRead(out _));

        // Step 3 sees watermark T0+7s (>= T0+5s): window T0 closes, carrying its deadline.
        await input.InsertAsync(("home", T0.AddSeconds(12)));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new KeyedValue<DateTimeOffset, DateTimeOffset>(T0, T0.AddSeconds(5))));
        Assert.Equal(1, delta.DistinctCount);

        // Step 4 sees watermark T0+12s: window T0+5s closes; window T0 does not close again.
        await input.InsertAsync(("home", T0.AddSeconds(13)));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new KeyedValue<DateTimeOffset, DateTimeOffset>(T0.AddSeconds(5), T0.AddSeconds(10))));
        Assert.Equal(1, delta.DistinctCount);

        // Step 5 sees watermark T0+13s (< T0+15s): nothing new closes, nothing repeats.
        await input.InsertAsync(("home", T0.AddSeconds(14)));
        await circuit.StepAsync();
        Assert.False(output.Reader.TryRead(out _));
    }

    [Fact]
    public async Task Allowed_lateness_delays_the_close()
    {
        var builder = ManualBuilder();
        var views = builder.Input<(string Page, DateTimeOffset At)>("views");
        var closesDef = views
            .WithEventTime(v => v.At, lateness: TimeSpan.FromSeconds(5))
            .Tumble(TimeSpan.FromSeconds(5), v => v.At)
            .GroupBy(w => w.WindowStart)
            .WindowCloses(windowStart => windowStart.AddSeconds(5))
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(views);
        var output = circuit.Handle(closesDef);

        // The watermark lags max event time by 5s, so window T0 (deadline T0+5s) only
        // closes once an event at T0+10s or later has been seen — and then one step later,
        // because the watermark freezes between steps.
        await input.InsertAsync(("home", T0.AddSeconds(1)));
        await circuit.StepAsync(); // freezes T0-4s
        await input.InsertAsync(("home", T0.AddSeconds(7)));
        await circuit.StepAsync(); // freezes T0+2s
        Assert.False(output.Reader.TryRead(out _));

        // A late event for window T0 is still admitted (T0+3s >= watermark T0+2s).
        await input.InsertAsync(("home", T0.AddSeconds(3)));
        await input.InsertAsync(("home", T0.AddSeconds(9)));
        await circuit.StepAsync(); // freezes T0+4s
        Assert.False(output.Reader.TryRead(out _));

        await input.InsertAsync(("home", T0.AddSeconds(10)));
        await circuit.StepAsync(); // watermark T0+4s < T0+5s: still open; freezes T0+5s
        Assert.False(output.Reader.TryRead(out _));

        await input.InsertAsync(("home", T0.AddSeconds(11)));
        await circuit.StepAsync(); // watermark T0+5s >= T0+5s: window T0 closes
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new KeyedValue<DateTimeOffset, DateTimeOffset>(T0, T0.AddSeconds(5))));
        Assert.Equal(1, delta.DistinctCount);
    }

    [Fact]
    public async Task Joining_closes_with_the_aggregate_publishes_finalized_results()
    {
        var builder = ManualBuilder();
        var views = builder.Input<(string User, DateTimeOffset At)>("views");
        var windowed = views
            .WithEventTime(v => v.At, lateness: TimeSpan.FromSeconds(5))
            .Tumble(TimeSpan.FromSeconds(5), v => v.At)
            .GroupBy(w => w.WindowStart)
            .EvictWhen(windowStart => windowStart.AddSeconds(5));
        var counts = windowed.Count();
        var finalsDef = windowed
            .WindowCloses() // parameterless: closes at the eviction policy's deadline
            .Grouped()
            .Join(counts.Grouped(), (window, closedAt, count) => (window, count))
            .ToOutput();

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(views);
        var output = circuit.Handle(finalsDef);

        // Two events in window T0, then a late correction: the running count goes 2 -> 3,
        // but nothing is published — only the finalized value will be.
        await input.InsertAsync(("alice", T0.AddSeconds(1)));
        await input.InsertAsync(("bob", T0.AddSeconds(2)));
        await circuit.StepAsync(); // freezes T0-3s
        await input.InsertAsync(("carol", T0.AddSeconds(4))); // late, admitted
        await input.InsertAsync(("dave", T0.AddSeconds(8)));
        await circuit.StepAsync(); // freezes T0+3s
        await input.InsertAsync(("erin", T0.AddSeconds(12)));
        await circuit.StepAsync(); // watermark T0+3s < T0+5s: not final yet; freezes T0+7s
        Assert.False(output.Reader.TryRead(out _));

        // Watermark T0+7s closes window T0: the join pairs the close notification with the
        // count state — which includes the late correction — and evicts it in the same step.
        await input.InsertAsync(("frank", T0.AddSeconds(20)));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf((T0, 3L)));
        Assert.Equal(1, delta.DistinctCount);

        // Watermark T0+15s closes windows T0+5s and T0+10s; window T0 stays closed silently.
        await input.InsertAsync(("grace", T0.AddSeconds(21)));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf((T0.AddSeconds(5), 1L)));
        Assert.Equal(1, delta.WeightOf((T0.AddSeconds(10), 1L)));
        Assert.Equal(2, delta.DistinctCount);
    }

    [Fact]
    public async Task OnClose_emits_each_windowed_row_once_and_final()
    {
        var builder = ManualBuilder();
        var views = builder.Input<(string Page, DateTimeOffset At)>("views");
        var finalsDef = views
            .WithEventTime(v => v.At, lateness: TimeSpan.Zero)
            .GroupBy(v => v.Page)
            .TumblingWindow(TimeSpan.FromSeconds(5))
            .Count()
            .OnClose()
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(views);
        var output = circuit.Handle(finalsDef);

        // Window T0 flaps 1 -> 2 for "home" while open: nothing is published.
        await input.InsertAsync(("home", T0.AddSeconds(1)));
        await circuit.StepAsync(); // freezes T0+1s
        await input.InsertAsync(("home", T0.AddSeconds(2)));
        await input.InsertAsync(("docs", T0.AddSeconds(3)));
        await circuit.StepAsync(); // freezes T0+3s
        Assert.False(output.Reader.TryRead(out _));

        // Advance the watermark past T0+5s: both pages' final counts emit exactly once.
        await input.InsertAsync(("home", T0.AddSeconds(7)));
        await circuit.StepAsync(); // freezes T0+7s
        await input.InsertAsync(("home", T0.AddSeconds(8)));
        await circuit.StepAsync(); // watermark T0+7s >= T0+5s: window T0 closes
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new WindowedValue<string, long>(T0, T0.AddSeconds(5), "home", 2)));
        Assert.Equal(1, delta.WeightOf(new WindowedValue<string, long>(T0, T0.AddSeconds(5), "docs", 1)));
        Assert.Equal(2, delta.DistinctCount);

        // The closed window never re-emits.
        await input.InsertAsync(("home", T0.AddSeconds(9)));
        await circuit.StepAsync();
        Assert.False(output.Reader.TryRead(out _));
    }

    [Fact]
    public async Task Fully_retracted_window_still_closes_and_is_never_retracted()
    {
        var builder = ManualBuilder();
        var views = builder.Input<(string Page, DateTimeOffset At)>("views");
        var closesDef = views
            .WithEventTime(v => v.At, lateness: TimeSpan.Zero)
            .Tumble(TimeSpan.FromSeconds(5), v => v.At)
            .GroupBy(w => w.WindowStart)
            .WindowCloses(windowStart => windowStart.AddSeconds(5))
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(views);
        var output = circuit.Handle(closesDef);

        // Window T0's only event is inserted, then deleted: it closes by time, not contents.
        await input.InsertAsync(("home", T0.AddSeconds(1)));
        await circuit.StepAsync();
        await input.RemoveAsync(("home", T0.AddSeconds(1)));
        await circuit.StepAsync();
        await input.InsertAsync(("home", T0.AddSeconds(12)));
        await circuit.StepAsync(); // watermark still T0+1s: open
        await input.InsertAsync(("home", T0.AddSeconds(13)));
        await circuit.StepAsync(); // watermark T0+12s: closes T0 and T0+5s? no data in T0+5s — only T0

        var integrated = new ZSet<KeyedValue<DateTimeOffset, DateTimeOffset>>();
        while (output.Reader.TryRead(out var delta))
        {
            foreach (var (_, weight) in delta)
                Assert.True(weight > 0); // closes are notifications: never retracted
            integrated.AddAll(delta);
        }
        Assert.Equal(1, integrated.WeightOf(new KeyedValue<DateTimeOffset, DateTimeOffset>(T0, T0.AddSeconds(5))));
        Assert.Equal(1, integrated.DistinctCount); // no close for windows that never saw data
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Window_closes_on_a_step_where_every_exchange_is_empty(bool pointToPoint)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false, Workers = 4, PointToPointExchange = pointToPoint,
        });
        var views = builder.Input<(string Page, DateTimeOffset At)>("views");
        var closesDef = views
            .WithEventTime(v => v.At, lateness: TimeSpan.Zero)
            .Tumble(TimeSpan.FromSeconds(5), v => v.At)
            .GroupBy(w => w.WindowStart)
            .WindowCloses(windowStart => windowStart.AddSeconds(5))
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(views);
        var output = circuit.Handle(closesDef);

        // Populate window T0, then freeze a watermark past its deadline.
        await input.InsertAsync(("home", T0.AddSeconds(1)));
        await circuit.StepAsync();
        await input.InsertAsync(("home", T0.AddSeconds(7)));
        await circuit.StepAsync(); // sees watermark T0+1s (window still open); freezes T0+7s
        Assert.False(output.Reader.TryRead(out _));

        // The closing step carries no data at all: every exchange in the circuit is
        // globally empty, so an exchange fast path that skipped too much — the shuffle is
        // skippable, its watermark-driven downstream (EmptyStepResult == null) is not —
        // would swallow this close.
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new KeyedValue<DateTimeOffset, DateTimeOffset>(T0, T0.AddSeconds(5))));
        Assert.Equal(1, delta.DistinctCount);
    }

    [Fact]
    public async Task Pending_closes_are_flushed_when_inputs_complete()
    {
        var builder = new CircuitBuilder(); // auto-stepping
        var events = builder.Input<DateTimeOffset>("events");
        var closesDef = events
            .WithEventTime(t => t, lateness: TimeSpan.Zero)
            .Tumble(TimeSpan.FromSeconds(5), t => t)
            .GroupBy(w => w.WindowStart)
            .WindowCloses(windowStart => windowStart.AddSeconds(5))
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(events);
        var output = circuit.Handle(closesDef);

        // The last event advances the watermark past window T0's deadline, but that freeze
        // happens after the final data step — the shutdown flush step must still deliver it.
        await input.InsertAsync(T0.AddSeconds(1));
        await input.InsertAsync(T0.AddSeconds(12));
        input.Complete();

        var integrated = new ZSet<KeyedValue<DateTimeOffset, DateTimeOffset>>();
        await foreach (var delta in output.Changes())
            integrated.AddAll(delta);
        Assert.Equal(1, integrated.WeightOf(new KeyedValue<DateTimeOffset, DateTimeOffset>(T0, T0.AddSeconds(5))));
        Assert.Equal(1, integrated.DistinctCount); // window [T0+10s, T0+15s) is still open
    }

    [Fact]
    public async Task Each_window_closes_exactly_once_across_workers()
    {
        const int windows = 150;
        var builder = new CircuitBuilder(new CircuitOptions { Workers = 4 });
        var events = builder.Input<DateTimeOffset>("events");
        var closesDef = events
            .WithEventTime(t => t, lateness: TimeSpan.Zero)
            .Tumble(TimeSpan.FromSeconds(5), t => t)
            .GroupBy(w => w.WindowStart)
            .WindowCloses(windowStart => windowStart.AddSeconds(5))
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(events);
        var output = circuit.Handle(closesDef);

        // In-order events, three per window, so every window is populated on some worker.
        for (var i = 0; i < windows; i++)
            for (var offset = 1; offset <= 3; offset++)
                await input.InsertAsync(T0.AddSeconds(i * 5 + offset));
        input.Complete();

        var integrated = new ZSet<KeyedValue<DateTimeOffset, DateTimeOffset>>();
        await foreach (var delta in output.Changes())
            integrated.AddAll(delta);

        // The final watermark passes every deadline but the last window's. Each closed
        // window has weight exactly +1: a duplicate notification anywhere would show as 2.
        Assert.Equal(windows - 1, integrated.DistinctCount);
        for (var i = 0; i < windows - 1; i++)
        {
            var windowStart = T0.AddSeconds(i * 5);
            Assert.Equal(1, integrated.WeightOf(
                new KeyedValue<DateTimeOffset, DateTimeOffset>(windowStart, windowStart.AddSeconds(5))));
        }
    }

    [Fact]
    public void WindowCloses_requires_a_watermark()
    {
        var builder = new CircuitBuilder();
        var keyed = builder.Input<(string Key, DateTimeOffset At)>("in").GroupBy(v => v.Key);
        Assert.Throws<InvalidOperationException>(() => keyed.WindowCloses(_ => DateTimeOffset.MaxValue));
    }

    [Fact]
    public void WindowCloses_without_a_deadline_requires_an_eviction_policy()
    {
        var builder = new CircuitBuilder();
        var keyed = builder.Input<(string Key, DateTimeOffset At)>("in")
            .WithEventTime(v => v.At, lateness: TimeSpan.Zero)
            .GroupBy(v => v.Key);
        Assert.Throws<InvalidOperationException>(() => keyed.WindowCloses());
    }
}
