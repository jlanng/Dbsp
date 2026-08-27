using Incremental.Storage;

namespace Incremental.Tests;

public class SessionWindowTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static CircuitBuilder ManualBuilder() =>
        new(new CircuitOptions { AutoStep = false });

    private static KeyedValue<string, Windowed<(string User, DateTimeOffset At)>> Row(
        string user, double startSeconds, double endSeconds, double atSeconds) =>
        new(user, new Windowed<(string, DateTimeOffset)>(
            T0.AddSeconds(startSeconds), T0.AddSeconds(endSeconds), (user, T0.AddSeconds(atSeconds))));

    [Fact]
    public async Task Events_within_the_gap_form_one_session_per_key()
    {
        var builder = ManualBuilder();
        builder.Input<(string User, DateTimeOffset At)>("clicks")
            .SessionWindow(TimeSpan.FromSeconds(5), v => v.At, v => v.User)
            .Output("sessions");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string User, DateTimeOffset At)>("clicks");
        var output = circuit.Output<KeyedValue<string, Windowed<(string User, DateTimeOffset At)>>>("sessions");

        // alice: two events 2s apart (one session) plus one 17s later (a second session);
        // bob's lone event must not interfere with alice's sessions.
        await input.InsertAsync(("alice", T0.AddSeconds(1)));
        await input.InsertAsync(("alice", T0.AddSeconds(3)));
        await input.InsertAsync(("alice", T0.AddSeconds(20)));
        await input.InsertAsync(("bob", T0.AddSeconds(2)));
        await circuit.StepAsync();

        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Row("alice", 1, 8, 1)));
        Assert.Equal(1, delta.WeightOf(Row("alice", 1, 8, 3)));
        Assert.Equal(1, delta.WeightOf(Row("alice", 20, 25, 20)));
        Assert.Equal(1, delta.WeightOf(Row("bob", 2, 7, 2)));
        Assert.Equal(4, delta.DistinctCount);
    }

    [Fact]
    public async Task An_event_exactly_at_the_gap_edge_starts_a_new_session()
    {
        var builder = ManualBuilder();
        builder.Input<(string User, DateTimeOffset At)>("clicks")
            .SessionWindow(TimeSpan.FromSeconds(5), v => v.At, v => v.User)
            .Output("sessions");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string User, DateTimeOffset At)>("clicks");
        var output = circuit.Output<KeyedValue<string, Windowed<(string User, DateTimeOffset At)>>>("sessions");

        // Exactly gap apart: [T0, T0+5) does not contain T0+5, so two sessions.
        await input.InsertAsync(("alice", T0));
        await input.InsertAsync(("alice", T0.AddSeconds(5)));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Row("alice", 0, 5, 0)));
        Assert.Equal(1, delta.WeightOf(Row("alice", 5, 10, 5)));
        Assert.Equal(2, delta.DistinctCount);

        // Strictly within the gap (4s < 5s): extends the second session to [5, 14).
        await input.InsertAsync(("alice", T0.AddSeconds(9)));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(Row("alice", 5, 10, 5)));
        Assert.Equal(1, delta.WeightOf(Row("alice", 5, 14, 5)));
        Assert.Equal(1, delta.WeightOf(Row("alice", 5, 14, 9)));
        Assert.Equal(3, delta.DistinctCount); // the untouched first session's row cancels out
    }

    [Fact]
    public async Task A_bridging_event_merges_two_sessions()
    {
        var builder = ManualBuilder();
        builder.Input<(string User, DateTimeOffset At)>("clicks")
            .SessionWindow(TimeSpan.FromSeconds(6), v => v.At, v => v.User)
            .Output("sessions");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string User, DateTimeOffset At)>("clicks");
        var output = circuit.Output<KeyedValue<string, Windowed<(string User, DateTimeOffset At)>>>("sessions");

        await input.InsertAsync(("alice", T0));
        await input.InsertAsync(("alice", T0.AddSeconds(10)));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Row("alice", 0, 6, 0)));
        Assert.Equal(1, delta.WeightOf(Row("alice", 10, 16, 10)));
        Assert.Equal(2, delta.DistinctCount);

        // T0+5 is within the gap of both neighbours: the two sessions merge into [0, 16),
        // retracting both old rows and asserting all three with the merged bounds.
        await input.InsertAsync(("alice", T0.AddSeconds(5)));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(Row("alice", 0, 6, 0)));
        Assert.Equal(-1, delta.WeightOf(Row("alice", 10, 16, 10)));
        Assert.Equal(1, delta.WeightOf(Row("alice", 0, 16, 0)));
        Assert.Equal(1, delta.WeightOf(Row("alice", 0, 16, 5)));
        Assert.Equal(1, delta.WeightOf(Row("alice", 0, 16, 10)));
        Assert.Equal(5, delta.DistinctCount);
    }

    [Fact]
    public async Task Retracting_a_bridging_event_splits_the_session()
    {
        var builder = ManualBuilder();
        builder.Input<(string User, DateTimeOffset At)>("clicks")
            .SessionWindow(TimeSpan.FromSeconds(6), v => v.At, v => v.User)
            .Output("sessions");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string User, DateTimeOffset At)>("clicks");
        var output = circuit.Output<KeyedValue<string, Windowed<(string User, DateTimeOffset At)>>>("sessions");

        await input.InsertAsync(("alice", T0));
        await input.InsertAsync(("alice", T0.AddSeconds(5)));
        await input.InsertAsync(("alice", T0.AddSeconds(10)));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Row("alice", 0, 16, 0)));
        Assert.Equal(1, delta.WeightOf(Row("alice", 0, 16, 5)));
        Assert.Equal(1, delta.WeightOf(Row("alice", 0, 16, 10)));
        Assert.Equal(3, delta.DistinctCount);

        // Deleting the middle event leaves the survivors more than a gap apart: the
        // session splits, and every stale row is retracted.
        await input.RemoveAsync(("alice", T0.AddSeconds(5)));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(Row("alice", 0, 16, 0)));
        Assert.Equal(-1, delta.WeightOf(Row("alice", 0, 16, 5)));
        Assert.Equal(-1, delta.WeightOf(Row("alice", 0, 16, 10)));
        Assert.Equal(1, delta.WeightOf(Row("alice", 0, 6, 0)));
        Assert.Equal(1, delta.WeightOf(Row("alice", 10, 16, 10)));
        Assert.Equal(5, delta.DistinctCount);
    }

    [Fact]
    public async Task A_late_event_admitted_by_the_watermark_merges_sessions()
    {
        var builder = ManualBuilder();
        builder.Input<(string User, DateTimeOffset At)>("clicks")
            .WithWatermark(v => v.At, allowedLateness: TimeSpan.FromSeconds(30))
            .SessionWindow(TimeSpan.FromSeconds(6), v => v.At, v => v.User)
            .Output("sessions");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string User, DateTimeOffset At)>("clicks");
        var output = circuit.Output<KeyedValue<string, Windowed<(string User, DateTimeOffset At)>>>("sessions");

        await input.InsertAsync(("alice", T0.AddSeconds(10)));
        await input.InsertAsync(("alice", T0.AddSeconds(20)));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Row("alice", 10, 16, 10)));
        Assert.Equal(1, delta.WeightOf(Row("alice", 20, 26, 20)));

        // T0+15 is out of order but within lateness: it bridges the two sessions.
        await input.InsertAsync(("alice", T0.AddSeconds(15)));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(Row("alice", 10, 16, 10)));
        Assert.Equal(-1, delta.WeightOf(Row("alice", 20, 26, 20)));
        Assert.Equal(1, delta.WeightOf(Row("alice", 10, 26, 10)));
        Assert.Equal(1, delta.WeightOf(Row("alice", 10, 26, 15)));
        Assert.Equal(1, delta.WeightOf(Row("alice", 10, 26, 20)));
        Assert.Equal(5, delta.DistinctCount);
    }

    [Fact]
    public async Task Session_counts_compose_with_an_aggregate()
    {
        var builder = ManualBuilder();
        builder.Input<(string User, DateTimeOffset At)>("clicks")
            .SessionWindow(TimeSpan.FromSeconds(5), v => v.At, v => v.User)
            .IndexBy(kv => (User: kv.Key, Start: kv.Value.WindowStart, End: kv.Value.WindowEnd))
            .Count()
            .Output("counts");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string User, DateTimeOffset At)>("clicks");
        var output = circuit.Output<KeyedValue<(string User, DateTimeOffset Start, DateTimeOffset End), long>>("counts");

        static KeyedValue<(string, DateTimeOffset, DateTimeOffset), long> CountRow(
            string user, double startSeconds, double endSeconds, long count) =>
            new((user, T0.AddSeconds(startSeconds), T0.AddSeconds(endSeconds)), count);

        await input.InsertAsync(("alice", T0.AddSeconds(1)));
        await input.InsertAsync(("alice", T0.AddSeconds(3)));
        await input.InsertAsync(("alice", T0.AddSeconds(20)));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(CountRow("alice", 1, 8, 2)));
        Assert.Equal(1, delta.WeightOf(CountRow("alice", 20, 25, 1)));
        Assert.Equal(2, delta.DistinctCount);

        // Extending the first session moves its bounds: the old session key's count is
        // retracted (its group vanished) and the new session's count asserted.
        await input.InsertAsync(("alice", T0.AddSeconds(6)));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(CountRow("alice", 1, 8, 2)));
        Assert.Equal(1, delta.WeightOf(CountRow("alice", 1, 11, 3)));
        Assert.Equal(2, delta.DistinctCount);
    }

    [Fact]
    public async Task Closed_key_state_is_evicted_once_the_watermark_passes()
    {
        var storage = new GroupEvictionRecordingStorage();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        builder.Input<(string User, DateTimeOffset At)>("clicks")
            .WithWatermark(v => v.At, allowedLateness: TimeSpan.Zero)
            .SessionWindow(TimeSpan.FromSeconds(5), v => v.At, v => v.User)
            .Output("sessions");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string User, DateTimeOffset At)>("clicks");
        var output = circuit.Output<KeyedValue<string, Windowed<(string User, DateTimeOffset At)>>>("sessions");

        // Step 1: alice's session [1, 6). Freezes watermark T0+1s.
        await input.InsertAsync(("alice", T0.AddSeconds(1)));
        await circuit.StepAsync();
        Assert.Empty(storage.RemovedGroups);

        // Step 2: a second alice session moves her deadline to T0+25s. Freezes T0+20s.
        await input.InsertAsync(("alice", T0.AddSeconds(20)));
        await circuit.StepAsync();
        Assert.Empty(storage.RemovedGroups);

        // Step 3 sees watermark T0+20s < T0+25s: alice must survive (her first session's
        // stale deadline of T0+6s is skipped — the key is only closed once the watermark
        // passes her *last* event + gap). Freezes T0+40s.
        await input.InsertAsync(("bob", T0.AddSeconds(40)));
        await circuit.StepAsync();
        Assert.Empty(storage.RemovedGroups);

        // Step 4 sees watermark T0+40s >= T0+25s: alice's events are deleted; bob (deadline
        // T0+45s) survives.
        await input.InsertAsync(("bob", T0.AddSeconds(50)));
        await circuit.StepAsync();
        Assert.Equal("alice", Assert.Single(storage.RemovedGroups));

        // Eviction emits no retractions: alice's published sessions stay final.
        var integrated = new ZSet<KeyedValue<string, Windowed<(string User, DateTimeOffset At)>>>();
        while (output.Reader.TryRead(out var delta))
            integrated.AddAll(delta);
        Assert.Equal(1, integrated.WeightOf(Row("alice", 1, 6, 1)));
        Assert.Equal(1, integrated.WeightOf(Row("alice", 20, 25, 20)));
    }

    [Fact]
    public async Task Multi_worker_results_match_single_worker()
    {
        var expected = await RunSessionPipeline(workers: 1);
        var actual = await RunSessionPipeline(workers: 4);

        Assert.Equal(expected.DistinctCount, actual.DistinctCount);
        foreach (var (row, weight) in expected)
            Assert.Equal(weight, actual.WeightOf(row));
    }

    private static async Task<ZSet<KeyedValue<string, Windowed<(string User, DateTimeOffset At)>>>> RunSessionPipeline(int workers)
    {
        var builder = new CircuitBuilder(new CircuitOptions { Workers = workers });
        builder.Input<(string User, DateTimeOffset At)>("clicks")
            .SessionWindow(TimeSpan.FromSeconds(5), v => v.At, v => v.User)
            .Output("sessions");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string User, DateTimeOffset At)>("clicks");
        var output = circuit.Output<KeyedValue<string, Windowed<(string User, DateTimeOffset At)>>>("sessions");

        // 40 users, three bursts each, interleaved so multi-worker runs must shuffle by
        // key; then out-of-order events extend half the users' first sessions, and some
        // events are retracted again — every touch re-sessionizes with retractions, so any
        // sharding bug (state split across workers) would corrupt the integrated result.
        for (var burst = 0; burst < 3; burst++)
            for (var user = 0; user < 40; user++)
                for (var offset = 0; offset < 3; offset++)
                    await input.InsertAsync(($"user-{user}", T0.AddSeconds(burst * 20 + user + offset)));
        for (var user = 0; user < 40; user += 2)
            await input.InsertAsync(($"user-{user}", T0.AddSeconds(user + 6))); // 4s after the first burst: extends it
        for (var user = 0; user < 20; user++)
            await input.RemoveAsync(($"user-{user}", T0.AddSeconds(user + 1))); // retract a mid-burst event
        input.Complete();

        var integrated = new ZSet<KeyedValue<string, Windowed<(string User, DateTimeOffset At)>>>();
        await foreach (var delta in output.ReadAllAsync())
            integrated.AddAll(delta);
        return integrated;
    }

    [Fact]
    public void SessionWindow_rejects_a_non_positive_gap()
    {
        var builder = new CircuitBuilder();
        var keyed = builder.Input<(string User, DateTimeOffset At)>("clicks").IndexBy(v => v.User);
        Assert.Throws<ArgumentOutOfRangeException>(() => keyed.SessionWindow(TimeSpan.Zero, v => v.At));
    }

    /// <summary>In-memory storage that records group removals from indexed stores — how the
    /// session operator evicts a closed key's events.</summary>
    private sealed class GroupEvictionRecordingStorage : IStorageProvider
    {
        private readonly InMemoryStorageProvider _inner = new();
        private readonly List<object> _removedGroups = [];

        public IReadOnlyList<object> RemovedGroups
        {
            get { lock (_removedGroups) return [.. _removedGroups]; }
        }

        public IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull =>
            _inner.CreateWeightStore<T>(name);

        public IIndexedWeightStore<TKey, TValue> CreateIndexedWeightStore<TKey, TValue>(string name)
            where TKey : notnull
            where TValue : notnull =>
            new RecordingIndexed<TKey, TValue>(_inner.CreateIndexedWeightStore<TKey, TValue>(name), this);

        public IValueStore<TKey, TValue> CreateValueStore<TKey, TValue>(string name) where TKey : notnull =>
            _inner.CreateValueStore<TKey, TValue>(name);

        private void Record(object key)
        {
            lock (_removedGroups)
                _removedGroups.Add(key);
        }

        private sealed class RecordingIndexed<TKey, TValue>(
            IIndexedWeightStore<TKey, TValue> inner,
            GroupEvictionRecordingStorage owner) : IIndexedWeightStore<TKey, TValue>
            where TKey : notnull
            where TValue : notnull
        {
            public long Update(TKey key, TValue value, long delta) => inner.Update(key, value, delta);
            public IReadOnlyList<WeightedValue<TValue>> GetGroup(TKey key) => inner.GetGroup(key);

            public void RemoveGroup(TKey key)
            {
                owner.Record(key);
                inner.RemoveGroup(key);
            }
        }
    }
}
