using Incremental.Storage;
using Xunit;

namespace Incremental.Tests;

/// <summary>
/// Event time where records enter (round five, K1, K3 and K6): the watermark kept per
/// partition of an input, the bound on how far ahead a record's time may be, and a keyed
/// input's rule for a key that comes back as another record.
/// </summary>
public class PartitionWatermarkTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Lateness = TimeSpan.FromSeconds(2);

    /// <summary>A clock a test moves.</summary>
    private sealed class FakeTimeProvider(DateTimeOffset now) : TimeProvider
    {
        private DateTimeOffset _now = now;
        public override DateTimeOffset GetUtcNow() => _now;
        public void Advance(TimeSpan by) => _now += by;
    }

    private sealed record Ev(int Id, int Key, long AtMs)
    {
        public DateTimeOffset At => T0.AddMilliseconds(AtMs);
    }

    private sealed class Counting(Circuit circuit, InputStream<Ev> input, Output<Ev> output) : IAsyncDisposable
    {
        public Circuit Circuit => circuit;
        public InputHandle<Ev> Input => circuit.Handle(input);
        public readonly List<int> Passed = [];

        public async Task StepAsync()
        {
            await circuit.StepAsync().WaitAsync(Timeout);
            while (circuit.Handle(output).Reader.TryRead(out var delta))
                foreach (var (value, weight) in delta)
                    for (var i = 0; i < weight; i++)
                        Passed.Add(value.Id);
        }

        public ValueTask DisposeAsync() => circuit.DisposeAsync();
    }

    /// <summary>An input with event time declared on it, and what gets past the
    /// declaration as its output.</summary>
    private static Counting Declared(IStorageProvider? storage = null, Action<CircuitOptions>? check = null, CircuitOptions? options = null)
    {
        options ??= new CircuitOptions { AutoStep = false, Storage = storage ?? new InMemoryStorageProvider() };
        var builder = new CircuitBuilder(options);
        var input = builder.Input<Ev>("events");
        var output = input.WithEventTime(e => e.At, Lateness).ToOutput();
        var circuit = builder.Build();
        circuit.Start();
        return new Counting(circuit, input, output);
    }

    // ── K1: the watermark is the least over an input's partitions ──────────────────────

    [Fact]
    public async Task A_partition_behind_the_others_holds_the_watermark_and_its_records_are_not_late()
    {
        await using var run = Declared();
        await run.Input.ExpectPartitionAsync(0);
        await run.Input.ExpectPartitionAsync(1);

        // Partition 0 is a minute ahead before partition 1 has brought anything: a
        // backlog read one partition first.
        for (var i = 0; i < 60; i++)
            await run.Input.InsertAsync(new Ev(i, 0, i * 1000), partition: 0, offset: i);
        await run.StepAsync();
        Assert.Null(run.Circuit.CurrentWatermark);
        Assert.Equal(new WatermarkHold(0, "events", 1), run.Circuit.WatermarkHeldBy);

        // Partition 1's records are a minute old by partition 0's time, and all count.
        for (var i = 0; i < 30; i++)
            await run.Input.InsertAsync(new Ev(100 + i, 1, i * 1000), partition: 1, offset: i);
        await run.StepAsync();
        Assert.Equal(T0.AddSeconds(29) - Lateness, run.Circuit.CurrentWatermark);
        Assert.Equal(new WatermarkHold(0, "events", 1), run.Circuit.WatermarkHeldBy);
        for (var i = 30; i < 60; i++)
            await run.Input.InsertAsync(new Ev(100 + i, 1, i * 1000), partition: 1, offset: i);
        await run.StepAsync();

        Assert.Equal(0, run.Circuit.LateRecords);
        Assert.Equal(120, run.Passed.Count);
        Assert.Equal(T0.AddSeconds(59) - Lateness, run.Circuit.CurrentWatermark);

        // Late is still late: behind the least of the partitions.
        await run.Input.InsertAsync(new Ev(999, 1, 10_000), partition: 1, offset: 60);
        await run.StepAsync();
        Assert.Equal(1, run.Circuit.LateRecords);
        Assert.DoesNotContain(999, run.Passed);
    }

    [Fact]
    public async Task Partitions_the_source_does_not_name_are_timed_together_as_before()
    {
        // Stamped, and nothing said about partitions: the newest event time read from
        // any of them is the input's, and the other partition's backlog is late.
        await using var run = Declared();
        for (var i = 0; i < 60; i++)
            await run.Input.InsertAsync(new Ev(i, 0, i * 1000), partition: 0, offset: i);
        await run.StepAsync();
        Assert.Equal(T0.AddSeconds(59) - Lateness, run.Circuit.CurrentWatermark);
        Assert.Equal(new WatermarkHold(0, "events", null), run.Circuit.WatermarkHeldBy);

        for (var i = 0; i < 30; i++)
            await run.Input.InsertAsync(new Ev(100 + i, 1, i * 1000), partition: 1, offset: i);
        await run.StepAsync();
        Assert.Equal(30, run.Circuit.LateRecords);
    }

    [Fact]
    public async Task An_idle_partition_stops_holding_the_watermark_until_its_next_record()
    {
        await using var run = Declared();
        await run.Input.ExpectPartitionAsync(0);
        await run.Input.ExpectPartitionAsync(1);
        await run.Input.InsertAsync(new Ev(1, 0, 10_000), partition: 0, offset: 0);
        await run.StepAsync();
        Assert.Null(run.Circuit.CurrentWatermark);

        // Read to its end with nothing in it: it has no time, and needs none.
        await run.Input.IdlePartitionAsync(1);
        await run.StepAsync();
        Assert.Equal(T0.AddSeconds(8), run.Circuit.CurrentWatermark);

        // Its next record counts it again: from here the watermark waits for it.
        await run.Input.InsertAsync(new Ev(2, 1, 9_000), partition: 1, offset: 0);
        await run.Input.InsertAsync(new Ev(3, 0, 60_000), partition: 0, offset: 1);
        await run.StepAsync();
        Assert.Equal(T0.AddSeconds(8), run.Circuit.CurrentWatermark);
        Assert.Equal(new WatermarkHold(0, "events", 1), run.Circuit.WatermarkHeldBy);
        Assert.Equal(0, run.Circuit.LateRecords);

        // Idle again, and a record behind what the other partition has made of the
        // watermark meanwhile is late — as it is on a topic of one partition.
        await run.Input.IdlePartitionAsync(1);
        await run.StepAsync();
        Assert.Equal(T0.AddSeconds(58), run.Circuit.CurrentWatermark);
        await run.Input.InsertAsync(new Ev(4, 1, 20_000), partition: 1, offset: 1);
        await run.StepAsync();
        Assert.Equal(1, run.Circuit.LateRecords);

        // Every partition idle: the newest time seen anywhere, as without partitions.
        await run.Input.IdlePartitionAsync(0);
        await run.Input.IdlePartitionAsync(1);
        await run.StepAsync();
        Assert.Equal(T0.AddSeconds(58), run.Circuit.CurrentWatermark);

        // Named again while idle — the source no longer knows that it is at its end —
        // it holds the watermark where its own records are.
        await run.Input.ExpectPartitionAsync(1);
        await run.Input.InsertAsync(new Ev(5, 0, 120_000), partition: 0, offset: 2);
        await run.StepAsync();
        Assert.Equal(T0.AddSeconds(58), run.Circuit.CurrentWatermark);
        Assert.Equal(new WatermarkHold(0, "events", 1), run.Circuit.WatermarkHeldBy);
    }

    [Fact]
    public async Task The_partitions_times_are_in_the_checkpoint_and_a_restored_circuit_goes_on_from_them()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        await using (var run = Declared(storage))
        {
            for (var partition = 0; partition < 3; partition++)
                await run.Input.ExpectPartitionAsync(partition);
            await run.Input.InsertAsync(new Ev(1, 0, 60_000), partition: 0, offset: 0);
            await run.Input.InsertAsync(new Ev(2, 1, 20_000), partition: 1, offset: 0);
            await run.Input.IdlePartitionAsync(2);
            await run.StepAsync();
            Assert.Equal(T0.AddSeconds(18), run.Circuit.CurrentWatermark);
        }

        var committed = storage.Recover();
        Assert.Equal(T0.AddSeconds(60).UtcTicks, committed["$watermark#s0#m0"]);
        Assert.Equal(T0.AddSeconds(20).UtcTicks, committed["$watermark#s0#m1"]);
        Assert.Equal(long.MinValue, committed["$watermark#s0#m2"]);
        Assert.Equal([0L, 0L, 1L], new[] { committed["$watermark#s0#i0"], committed["$watermark#s0#i1"], committed["$watermark#s0#i2"] });
        Assert.Contains("format=8", storage.CommittedManifest("local"));

        // Restored, and the source says nothing new: partition 1 still holds the
        // watermark at its own time, so its records are not late though partition 0 is
        // forty seconds on; and partition 2 is still idle.
        await using var restored = Declared(storage);
        Assert.Equal(T0.AddSeconds(18), restored.Circuit.CurrentWatermark);
        await restored.Input.InsertAsync(new Ev(3, 0, 90_000), partition: 0, offset: 1);
        await restored.Input.InsertAsync(new Ev(4, 1, 21_000), partition: 1, offset: 1);
        await restored.StepAsync();
        Assert.Equal(0, restored.Circuit.LateRecords);
        Assert.Equal([3, 4], restored.Passed.Order());
        Assert.Equal(T0.AddSeconds(19), restored.Circuit.CurrentWatermark);
        Assert.Equal(new WatermarkHold(0, "events", 1), restored.Circuit.WatermarkHeldBy);
    }

    [Fact]
    public async Task A_checkpoint_from_before_partitions_had_times_resumes_and_its_watermark_does_not_fall()
    {
        // Format 7: the source's one time, the newest read from any partition.
        var storage = new InMemoryCheckpointStorageProvider();
        await using (var run = Declared(storage))
        {
            await run.Input.InsertAsync(new Ev(1, 0, 60_000), partition: 0, offset: 0);
            await run.StepAsync();
        }
        var offsets = new Dictionary<string, long>(storage.Recover());
        Assert.DoesNotContain(offsets.Keys, key => key.Contains("#m") || key.Contains("#i"));
        storage.StageManifest("local", storage.CommittedManifest("local")!.Replace("format=8", "format=7"));
        storage.Commit(offsets);

        // This version resumes it, and the source now names its partitions: each holds
        // every claim back until it has brought a record or is idle, and the watermark
        // stays where it was committed.
        await using var resumed = Declared(storage);
        Assert.Equal(T0.AddSeconds(58), resumed.Circuit.CurrentWatermark);
        await resumed.Input.ExpectPartitionAsync(0);
        await resumed.Input.ExpectPartitionAsync(1);
        await resumed.Input.InsertAsync(new Ev(2, 0, 90_000), partition: 0, offset: 1);
        await resumed.StepAsync();
        Assert.Equal(T0.AddSeconds(58), resumed.Circuit.CurrentWatermark);
        await resumed.Input.InsertAsync(new Ev(3, 1, 70_000), partition: 1, offset: 0);
        await resumed.StepAsync();
        Assert.Equal(T0.AddSeconds(68), resumed.Circuit.CurrentWatermark);
        Assert.Equal(0, resumed.Circuit.LateRecords);
        Assert.Contains("format=8", storage.CommittedManifest("local"));
    }

    [Fact]
    public async Task A_declaration_that_is_not_on_the_input_has_one_time_whatever_the_source_says()
    {
        // After a Select the declaration sees records, not where they came from.
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var input = builder.Input<Ev>("events");
        input.Select(e => e with { Id = e.Id + 1 }).WithEventTime(e => e.At, Lateness).ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var handle = circuit.Handle(input);
        await handle.ExpectPartitionAsync(0);
        await handle.ExpectPartitionAsync(1);
        await handle.InsertAsync(new Ev(1, 0, 60_000), partition: 0, offset: 0);
        await circuit.StepAsync().WaitAsync(Timeout);
        Assert.Equal(T0.AddSeconds(58), circuit.CurrentWatermark);
        Assert.Equal(new WatermarkHold(0, null, null), circuit.WatermarkHeldBy);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task A_keyed_input_keeps_a_time_per_partition_too(int workers)
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = workers });
        var input = builder.KeyedInput<int, Ev>("rows", e => e.Id);
        var output = input.WithEventTime(e => e.At, Lateness, evictWhenFinal: true).ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var handle = circuit.Handle(input);
        await handle.ExpectPartitionAsync(0);
        await handle.ExpectPartitionAsync(1);
        for (var i = 0; i < 50; i++)
            await handle.UpsertAsync(new Ev(i, 0, i * 1000), partition: 0, offset: i);
        await circuit.StepAsync().WaitAsync(Timeout);
        for (var i = 0; i < 50; i++)
            await handle.UpsertAsync(new Ev(100 + i, 1, i * 1000), partition: 1, offset: i);
        await circuit.StepAsync().WaitAsync(Timeout);
        await circuit.StepAsync().WaitAsync(Timeout);

        var passed = 0L;
        while (circuit.Handle(output).Reader.TryRead(out var delta))
            passed += delta.Sum(entry => entry.Weight);
        Assert.Equal(100, passed);
        Assert.Equal(0, circuit.LateRecords);
        Assert.Equal(T0.AddSeconds(47), circuit.CurrentWatermark);
    }

    // ── K3: how far ahead a record's time may be ───────────────────────────────────────

    private static CircuitOptions Bounded(FakeTimeProvider clock, RecordErrorPolicy policy) => new()
    {
        AutoStep = false,
        ErrorPolicy = policy,
        MaxEventTimeAhead = TimeSpan.FromHours(1),
        TimeProvider = clock,
    };

    [Fact]
    public async Task A_record_stamped_beyond_the_bound_is_a_dead_letter_and_moves_nothing()
    {
        var clock = new FakeTimeProvider(T0.AddSeconds(100));
        await using var run = Declared(options: Bounded(clock, RecordErrorPolicy.DeadLetter));
        await run.Input.InsertAsync(new Ev(1, 0, 100_000), partition: 0, offset: 0);
        await run.StepAsync();
        var watermark = run.Circuit.CurrentWatermark;

        // A year ahead. Without the bound the watermark goes there and everything after
        // is late until the feed's time has caught up.
        var wrong = new Ev(2, 0, (long)TimeSpan.FromDays(365).TotalMilliseconds);
        await run.Input.InsertAsync(wrong, partition: 0, offset: 1);
        await run.Input.InsertAsync(new Ev(3, 0, 101_000), partition: 0, offset: 2);
        await run.StepAsync();

        Assert.Equal([1, 3], run.Passed);
        Assert.Equal(0, run.Circuit.LateRecords);
        Assert.Equal(T0.AddSeconds(99), run.Circuit.CurrentWatermark);
        Assert.True(run.Circuit.CurrentWatermark > watermark);
        Assert.True(run.Circuit.DeadLetters.TryRead(out var letter));
        Assert.Equal(wrong, letter!.Record);
        Assert.IsType<EventTimeAheadException>(letter.Exception);
        Assert.Contains("MaxEventTimeAhead", letter.Exception.Message);

        // Its offset is consumed: a source that sends it again after a restart finds it
        // dropped as read.
        Assert.False(run.Circuit.DeadLetters.TryRead(out _));
        await run.Input.InsertAsync(wrong, partition: 0, offset: 1);
        await run.StepAsync();
        Assert.False(run.Circuit.DeadLetters.TryRead(out _));
    }

    [Fact]
    public async Task Under_the_fault_policy_a_record_beyond_the_bound_faults_the_circuit()
    {
        var clock = new FakeTimeProvider(T0);
        await using var run = Declared(options: Bounded(clock, RecordErrorPolicy.Fault));
        await run.Input.InsertAsync(new Ev(1, 0, (long)TimeSpan.FromHours(2).TotalMilliseconds), partition: 3, offset: 7);
        var fault = await Assert.ThrowsAnyAsync<Exception>(() => run.Circuit.StepAsync().WaitAsync(Timeout));
        var all = fault.ToString();
        Assert.Contains("Input 'events'", all);
        Assert.Contains("partition 3, offset 7", all);
        Assert.Contains("MaxEventTimeAhead", all);
    }

    [Fact]
    public async Task The_bound_is_from_the_later_of_the_nodes_clock_and_the_newest_time_the_input_has_brought()
    {
        var clock = new FakeTimeProvider(T0);
        await using var run = Declared(options: Bounded(clock, RecordErrorPolicy.DeadLetter));

        // Within the bound of the node's clock; then a feed whose time runs ahead of the
        // node's, in steps shorter than the bound: each is measured from the last.
        for (var i = 1; i <= 5; i++)
            await run.Input.InsertAsync(new Ev(i, 0, i * (long)TimeSpan.FromMinutes(50).TotalMilliseconds));
        await run.StepAsync();
        Assert.Equal([1, 2, 3, 4, 5], run.Passed);

        // One step longer than the bound is refused, and the feed goes on from where it was.
        await run.Input.InsertAsync(new Ev(6, 0, (long)TimeSpan.FromMinutes(250 + 61).TotalMilliseconds));
        await run.Input.InsertAsync(new Ev(7, 0, (long)TimeSpan.FromMinutes(250 + 59).TotalMilliseconds));
        await run.StepAsync();
        Assert.Equal([1, 2, 3, 4, 5, 7], run.Passed);

        // A feed that has been quiet for longer than the bound is taken up again: the
        // node's clock has moved with it.
        clock.Advance(TimeSpan.FromDays(3));
        await run.Input.InsertAsync(new Ev(8, 0, (long)TimeSpan.FromDays(3).TotalMilliseconds));
        await run.StepAsync();
        Assert.Contains(8, run.Passed);
    }

    [Fact]
    public async Task The_newest_time_the_bound_is_measured_from_is_restored_with_the_checkpoint()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var clock = new FakeTimeProvider(T0);
        CircuitOptions Options() => new()
        {
            AutoStep = false, Storage = storage, ErrorPolicy = RecordErrorPolicy.DeadLetter,
            MaxEventTimeAhead = TimeSpan.FromHours(1), TimeProvider = clock,
        };
        await using (var run = Declared(options: Options()))
        {
            await run.Input.ExpectPartitionAsync(0);
            for (var i = 1; i <= 5; i++)
                await run.Input.InsertAsync(new Ev(i, 0, i * (long)TimeSpan.FromMinutes(50).TotalMilliseconds), partition: 0, offset: i);
            await run.StepAsync();
        }

        await using var restored = Declared(options: Options());
        await restored.Input.InsertAsync(new Ev(6, 0, (long)TimeSpan.FromMinutes(300).TotalMilliseconds), partition: 0, offset: 6);
        await restored.StepAsync();
        Assert.Equal([6], restored.Passed);
    }

    [Fact]
    public async Task A_keyed_inputs_upsert_beyond_the_bound_is_not_applied()
    {
        var clock = new FakeTimeProvider(T0);
        var builder = new CircuitBuilder(Bounded(clock, RecordErrorPolicy.DeadLetter));
        var input = builder.KeyedInput<int, Ev>("rows", e => e.Id);
        var output = input.WithEventTime(e => e.At, Lateness).ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var handle = circuit.Handle(input);

        // The row, then a version of it stamped a day ahead, then a correction. Were the
        // refused version applied and dropped later, the correction would take it back
        // and the first row would be left standing.
        await handle.UpsertAsync(new Ev(1, 1, 1000));
        await circuit.StepAsync().WaitAsync(Timeout);
        await handle.UpsertAsync(new Ev(1, 2, (long)TimeSpan.FromDays(1).TotalMilliseconds));
        await circuit.StepAsync().WaitAsync(Timeout);
        await handle.UpsertAsync(new Ev(1, 3, 2000));
        await circuit.StepAsync().WaitAsync(Timeout);

        var rows = new Dictionary<Ev, long>();
        while (circuit.Handle(output).Reader.TryRead(out var delta))
            foreach (var (value, weight) in delta)
                rows[value] = rows.GetValueOrDefault(value) + weight;
        Assert.Equal([new Ev(1, 3, 2000)], rows.Where(row => row.Value != 0).Select(row => row.Key));
        Assert.All(rows.Values, weight => Assert.InRange(weight, 0, 1));
        Assert.True(circuit.DeadLetters.TryRead(out var letter));
        Assert.IsType<EventTimeAheadException>(letter!.Exception);
    }

    [Fact]
    public void A_negative_bound_is_refused()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new CircuitBuilder(new CircuitOptions { MaxEventTimeAhead = TimeSpan.FromSeconds(-1) }));
    }

    // ── K6: a key that comes back as another record ────────────────────────────────────

    private sealed record Trade(long Id, long AtMs, int Quantity)
    {
        public DateTimeOffset At => T0.AddMilliseconds(AtMs);
    }

    /// <summary>Feeds <paramref name="trades"/> to a keyed input that forgets its final
    /// rows, cut into steps as <paramref name="cuts"/> says (the indexes a step ends
    /// after), and returns what a sink keyed by the id would hold after each step — the
    /// rows in the order they were delivered — and the net rows at the end.</summary>
    private static async Task<(List<string> Delivered, List<Trade> Net, long Late)> FeedAsync(
        IReadOnlyList<Trade> trades, Func<int, bool> endsStep, int workers = 1)
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = workers });
        var input = builder.KeyedInput<long, Trade>("trades", t => t.Id);
        var output = input.WithEventTime(t => t.At, TimeSpan.FromSeconds(30), evictWhenFinal: true).ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var handle = circuit.Handle(input);
        var delivered = new List<string>();
        var net = new Dictionary<Trade, long>();

        async Task StepsAsync()
        {
            // As a pump would: until the input has nothing more — it may leave a record
            // for the next step — and then for what the last freeze unlocked.
            for (var quiet = 0; quiet < 3;)
            {
                await circuit.StepAsync().WaitAsync(Timeout);
                quiet++;
                while (circuit.Handle(output).Reader.TryRead(out var delta))
                {
                    quiet = 0;
                    foreach (var (value, weight) in delta)
                        net[value] = net.GetValueOrDefault(value) + weight;
                    // What an upsert sink keyed by the id makes of one step's delta: a
                    // step that carries two rows of one id has no order to give them.
                    foreach (var byId in delta.Where(entry => entry.Weight > 0).GroupBy(entry => entry.Value.Id))
                        delivered.Add(byId.Count() == 1 ? byId.Single().Value.ToString() : $"AMBIGUOUS {byId.Key}");
                }
            }
        }

        for (var i = 0; i < trades.Count; i++)
        {
            await handle.UpsertAsync(trades[i]);
            if (endsStep(i))
                await StepsAsync();
        }
        await StepsAsync();
        return (delivered, [.. net.Where(row => row.Value != 0).Select(row => row.Key).OrderBy(t => t.AtMs)], circuit.LateRecords);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task An_id_that_comes_back_after_its_window_gives_the_same_rows_however_the_records_fall_into_steps(int workers)
    {
        // Ids come round every 40 trades; a trade a second, so an id comes back 40 s
        // after its last use and the rows stay open for 30. Every tenth trade is also
        // amended two seconds later.
        var trades = new List<Trade>();
        for (var i = 0; i < 400; i++)
        {
            trades.Add(new Trade(i % 40, i * 1000, 1));
            if (i % 10 == 3)
                trades.Add(new Trade(i % 40, i * 1000, 2));
        }

        var oneByOne = await FeedAsync(trades, _ => true, workers);
        Assert.Equal(0, oneByOne.Late);
        // Every trade is delivered, the amended ones as amended.
        Assert.Equal(400, oneByOne.Net.Count);
        Assert.DoesNotContain(oneByOne.Delivered, row => row.StartsWith("AMBIGUOUS"));

        var random = new Random(71);
        var layouts = new (string Name, Func<int, bool> EndsStep)[]
        {
            ("all in one step: a backlog", _ => false),
            ("steps of seven", i => i % 7 == 6),
            ("steps of a hundred", i => i % 100 == 99),
            ("at random", _ => random.Next(5) == 0),
        };
        foreach (var (name, endsStep) in layouts)
        {
            var (delivered, net, late) = await FeedAsync(trades, endsStep, workers);
            Assert.True(late == 0, name);
            Assert.True(oneByOne.Net.SequenceEqual(net), $"{name}: the rows that stand differ");
            Assert.DoesNotContain(delivered, row => row.StartsWith("AMBIGUOUS"));
            // The last row delivered for each id — what an upsert sink ends with.
            static Dictionary<long, string> Last(List<string> rows) =>
                rows.GroupBy(row => row[..row.IndexOf(',')]).ToDictionary(g => long.Parse(g.Key[(g.Key.IndexOf('=') + 2)..]), g => g.Last());
            Assert.True(Last(oneByOne.Delivered).OrderBy(r => r.Key).SequenceEqual(Last(delivered).OrderBy(r => r.Key)), name);
        }
    }

    [Fact]
    public async Task An_upsert_within_the_open_window_amends_and_one_beyond_it_is_a_new_record()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var input = builder.KeyedInput<long, Trade>("trades", t => t.Id);
        var output = input.WithEventTime(t => t.At, TimeSpan.FromSeconds(30), evictWhenFinal: true).ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var handle = circuit.Handle(input);

        async Task<List<string>> StepAsync()
        {
            await circuit.StepAsync().WaitAsync(Timeout);
            var lines = new List<string>();
            while (circuit.Handle(output).Reader.TryRead(out var delta))
                foreach (var (value, weight) in delta)
                    lines.Add($"{value.AtMs}/{value.Quantity} x{weight}");
            lines.Sort(StringComparer.Ordinal);
            return lines;
        }

        await handle.UpsertAsync(new Trade(7, 1000, 1));
        Assert.Equal(["1000/1 x1"], await StepAsync());

        // Thirty seconds later exactly: still within the window, an amendment.
        await handle.UpsertAsync(new Trade(7, 31_000, 2));
        Assert.Equal(["1000/1 x-1", "31000/2 x1"], await StepAsync());

        // The watermark is at 1 s: the row at 31 s is as open as can be. A record of the
        // id more than the window later is another record all the same, and the row it
        // finds is not taken back.
        Assert.Equal(T0.AddSeconds(1), circuit.CurrentWatermark);
        await handle.UpsertAsync(new Trade(7, 61_001, 3));
        Assert.Equal(["61001/3 x1"], await StepAsync());

        // Two records of the id in the channel at once, more than the window apart: two
        // steps, in order.
        await handle.UpsertAsync(new Trade(8, 70_000, 1));
        await handle.UpsertAsync(new Trade(8, 70_000, 2));
        await handle.UpsertAsync(new Trade(8, 110_000, 5));
        Assert.Equal(["70000/2 x1"], await StepAsync());
        Assert.Equal(["110000/5 x1"], await StepAsync());

        // A delete between them: the key is gone, and what comes is new in any case.
        await handle.UpsertAsync(new Trade(9, 120_000, 1));
        await handle.DeleteAsync(9);
        await handle.UpsertAsync(new Trade(9, 200_000, 1));
        Assert.Equal(["200000/1 x1"], await StepAsync());
    }

    [Fact]
    public async Task A_late_version_of_a_record_does_not_take_back_the_open_row_its_key_holds()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var input = builder.KeyedInput<long, Trade>("trades", t => t.Id);
        var output = input.WithEventTime(t => t.At, TimeSpan.FromSeconds(30), evictWhenFinal: true).ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var handle = circuit.Handle(input);
        var net = new Dictionary<Trade, long>();

        async Task StepAsync()
        {
            await circuit.StepAsync().WaitAsync(Timeout);
            while (circuit.Handle(output).Reader.TryRead(out var delta))
                foreach (var (value, weight) in delta)
                    net[value] = net.GetValueOrDefault(value) + weight;
        }

        // Trade 7 at 200 s; the watermark is at 180 s, so it is open.
        await handle.UpsertAsync(new Trade(7, 200_000, 1));
        await handle.UpsertAsync(new Trade(8, 210_000, 1));
        await StepAsync();
        Assert.Equal(T0.AddSeconds(180), circuit.CurrentWatermark);

        // A version of trade 7 stamped 105 s: behind the watermark, late. Before round
        // five the input applied it — the open row's retraction passed, the late row's
        // insertion was dropped, and trade 7 was gone from everything downstream.
        await handle.UpsertAsync(new Trade(7, 105_000, 9));
        await StepAsync();
        Assert.Equal(1, circuit.LateRecords);
        Assert.Equal([new Trade(7, 200_000, 1), new Trade(8, 210_000, 1)], net.Where(row => row.Value != 0).Select(row => row.Key).OrderBy(t => t.Id));

        // The row it holds is still the one an amendment amends and a delete deletes.
        await handle.UpsertAsync(new Trade(7, 200_000, 2));
        await StepAsync();
        await handle.DeleteAsync(8);
        await StepAsync();
        Assert.Equal([new Trade(7, 200_000, 2)], net.Where(row => row.Value != 0).Select(row => row.Key));
        Assert.All(net.Values, weight => Assert.InRange(weight, 0, 1));
    }
}
