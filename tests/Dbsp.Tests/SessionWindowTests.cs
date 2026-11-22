using Dbsp.Storage;

namespace Dbsp.Tests;

/// <summary>
/// Session semantics observed through per-session counts: a session's bounds are part of
/// its identity, so extends, merges, and splits all surface as retract/assert pairs on
/// (session window, key, count) rows.
/// </summary>
public class SessionWindowTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Generous lateness for the tests that are about sessionization, not dropping.</summary>
    private static readonly TimeSpan NoDrops = TimeSpan.FromDays(1);

    private static CircuitBuilder ManualBuilder() =>
        new(new CircuitOptions { AutoStep = false });

    private static WindowedValue<string, long> Session(string user, double startSeconds, double endSeconds, long count) =>
        new(T0.AddSeconds(startSeconds), T0.AddSeconds(endSeconds), user, count);

    private static (InputStream<(string User, DateTimeOffset At)> Clicks, Output<WindowedValue<string, long>> Sessions)
        SessionCounts(CircuitBuilder builder, TimeSpan gap, TimeSpan lateness)
    {
        var clicks = builder.Input<(string User, DateTimeOffset At)>("clicks");
        var sessions = clicks
            .WithEventTime(v => v.At, lateness)
            .GroupBy(v => v.User)
            .SessionWindow(gap)
            .Count()
            .ToOutput(bootstrap: false);
        return (clicks, sessions);
    }

    [Fact]
    public async Task Events_within_the_gap_form_one_session_per_key()
    {
        var builder = ManualBuilder();
        var (clicks, sessions) = SessionCounts(builder, TimeSpan.FromSeconds(5), NoDrops);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(clicks);
        var output = circuit.Handle(sessions);

        // alice: two events 2s apart (one session) plus one 17s later (a second session);
        // bob's lone event must not interfere with alice's sessions.
        await input.InsertAsync(("alice", T0.AddSeconds(1)));
        await input.InsertAsync(("alice", T0.AddSeconds(3)));
        await input.InsertAsync(("alice", T0.AddSeconds(20)));
        await input.InsertAsync(("bob", T0.AddSeconds(2)));
        await circuit.StepAsync();

        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Session("alice", 1, 8, 2)));
        Assert.Equal(1, delta.WeightOf(Session("alice", 20, 25, 1)));
        Assert.Equal(1, delta.WeightOf(Session("bob", 2, 7, 1)));
        Assert.Equal(3, delta.DistinctCount);
    }

    [Fact]
    public async Task An_event_exactly_at_the_gap_edge_starts_a_new_session()
    {
        var builder = ManualBuilder();
        var (clicks, sessions) = SessionCounts(builder, TimeSpan.FromSeconds(5), NoDrops);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(clicks);
        var output = circuit.Handle(sessions);

        // Exactly gap apart: [T0, T0+5) does not contain T0+5, so two sessions.
        await input.InsertAsync(("alice", T0));
        await input.InsertAsync(("alice", T0.AddSeconds(5)));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Session("alice", 0, 5, 1)));
        Assert.Equal(1, delta.WeightOf(Session("alice", 5, 10, 1)));
        Assert.Equal(2, delta.DistinctCount);

        // Strictly within the gap (4s < 5s): extends the second session to [5, 14). The
        // session's bounds are part of its identity, so the old count row is retracted.
        await input.InsertAsync(("alice", T0.AddSeconds(9)));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(Session("alice", 5, 10, 1)));
        Assert.Equal(1, delta.WeightOf(Session("alice", 5, 14, 2)));
        Assert.Equal(2, delta.DistinctCount); // the untouched first session stays silent
    }

    [Fact]
    public async Task A_bridging_event_merges_two_sessions()
    {
        var builder = ManualBuilder();
        var (clicks, sessions) = SessionCounts(builder, TimeSpan.FromSeconds(6), NoDrops);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(clicks);
        var output = circuit.Handle(sessions);

        await input.InsertAsync(("alice", T0));
        await input.InsertAsync(("alice", T0.AddSeconds(10)));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Session("alice", 0, 6, 1)));
        Assert.Equal(1, delta.WeightOf(Session("alice", 10, 16, 1)));
        Assert.Equal(2, delta.DistinctCount);

        // T0+5 is within the gap of both neighbours: the two sessions merge into [0, 16),
        // retracting both old counts and asserting the merged one.
        await input.InsertAsync(("alice", T0.AddSeconds(5)));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(Session("alice", 0, 6, 1)));
        Assert.Equal(-1, delta.WeightOf(Session("alice", 10, 16, 1)));
        Assert.Equal(1, delta.WeightOf(Session("alice", 0, 16, 3)));
        Assert.Equal(3, delta.DistinctCount);
    }

    [Fact]
    public async Task Retracting_a_bridging_event_splits_the_session()
    {
        var builder = ManualBuilder();
        var (clicks, sessions) = SessionCounts(builder, TimeSpan.FromSeconds(6), NoDrops);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(clicks);
        var output = circuit.Handle(sessions);

        await input.InsertAsync(("alice", T0));
        await input.InsertAsync(("alice", T0.AddSeconds(5)));
        await input.InsertAsync(("alice", T0.AddSeconds(10)));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Session("alice", 0, 16, 3)));
        Assert.Equal(1, delta.DistinctCount);

        // Deleting the middle event leaves the survivors more than a gap apart: the
        // session splits, and the stale merged count is retracted.
        await input.RemoveAsync(("alice", T0.AddSeconds(5)));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(Session("alice", 0, 16, 3)));
        Assert.Equal(1, delta.WeightOf(Session("alice", 0, 6, 1)));
        Assert.Equal(1, delta.WeightOf(Session("alice", 10, 16, 1)));
        Assert.Equal(3, delta.DistinctCount);
    }

    [Fact]
    public async Task A_late_event_admitted_by_the_watermark_merges_sessions()
    {
        var builder = ManualBuilder();
        var (clicks, sessions) = SessionCounts(builder, TimeSpan.FromSeconds(6), TimeSpan.FromSeconds(30));

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(clicks);
        var output = circuit.Handle(sessions);

        await input.InsertAsync(("alice", T0.AddSeconds(10)));
        await input.InsertAsync(("alice", T0.AddSeconds(20)));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Session("alice", 10, 16, 1)));
        Assert.Equal(1, delta.WeightOf(Session("alice", 20, 26, 1)));

        // T0+15 is out of order but within lateness: it bridges the two sessions.
        await input.InsertAsync(("alice", T0.AddSeconds(15)));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(Session("alice", 10, 16, 1)));
        Assert.Equal(-1, delta.WeightOf(Session("alice", 20, 26, 1)));
        Assert.Equal(1, delta.WeightOf(Session("alice", 10, 26, 3)));
        Assert.Equal(3, delta.DistinctCount);
    }

    [Fact]
    public async Task Extending_a_session_moves_its_aggregate_to_the_new_bounds()
    {
        var builder = ManualBuilder();
        var (clicks, sessions) = SessionCounts(builder, TimeSpan.FromSeconds(5), NoDrops);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(clicks);
        var output = circuit.Handle(sessions);

        await input.InsertAsync(("alice", T0.AddSeconds(1)));
        await input.InsertAsync(("alice", T0.AddSeconds(3)));
        await input.InsertAsync(("alice", T0.AddSeconds(20)));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(Session("alice", 1, 8, 2)));
        Assert.Equal(1, delta.WeightOf(Session("alice", 20, 25, 1)));
        Assert.Equal(2, delta.DistinctCount);

        // Extending the first session moves its bounds: the old session key's count is
        // retracted (its group vanished) and the new session's count asserted.
        await input.InsertAsync(("alice", T0.AddSeconds(6)));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(Session("alice", 1, 8, 2)));
        Assert.Equal(1, delta.WeightOf(Session("alice", 1, 11, 3)));
        Assert.Equal(2, delta.DistinctCount);
    }

    [Fact]
    public async Task Closed_key_state_is_evicted_once_the_watermark_passes()
    {
        var storage = new GroupEvictionRecordingStorage();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        var (clicks, sessions) = SessionCounts(builder, TimeSpan.FromSeconds(5), TimeSpan.Zero);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(clicks);
        var output = circuit.Handle(sessions);

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

        // Eviction emits no retractions: alice's published session counts stay final.
        var integrated = new ZSet<WindowedValue<string, long>>();
        while (output.Reader.TryRead(out var delta))
            integrated.AddAll(delta);
        Assert.Equal(1, integrated.WeightOf(Session("alice", 1, 6, 1)));
        Assert.Equal(1, integrated.WeightOf(Session("alice", 20, 25, 1)));
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

    private static async Task<ZSet<WindowedValue<string, long>>> RunSessionPipeline(int workers)
    {
        var builder = new CircuitBuilder(new CircuitOptions { Workers = workers });
        var (clicks, sessions) = SessionCounts(builder, TimeSpan.FromSeconds(5), NoDrops);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(clicks);
        var output = circuit.Handle(sessions);

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

        var integrated = new ZSet<WindowedValue<string, long>>();
        await foreach (var delta in output.Changes())
            integrated.AddAll(delta);
        return integrated;
    }

    [Fact]
    public void SessionWindow_rejects_a_non_positive_gap()
    {
        var builder = new CircuitBuilder();
        var grouped = builder.Input<(string User, DateTimeOffset At)>("clicks")
            .WithEventTime(v => v.At, lateness: TimeSpan.Zero)
            .GroupBy(v => v.User);
        Assert.Throws<ArgumentOutOfRangeException>(() => grouped.SessionWindow(TimeSpan.Zero));
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
