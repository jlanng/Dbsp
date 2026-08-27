using Incremental.Storage;

namespace Incremental.Tests;

/// <summary>
/// Grouped MIN/MAX via the tournament-tree operator. Single-worker tests drive steps
/// manually to assert exact per-step deltas; the churn test checks the integrated result
/// against an oracle multiset across many mixed insert/delete steps.
/// </summary>
public class ExtremumTests
{
    private static CircuitBuilder ManualBuilder() =>
        new(new CircuitOptions { AutoStep = false });

    [Fact]
    public async Task Max_updates_and_recovers_after_deleting_the_maximum()
    {
        var builder = ManualBuilder();
        builder.Input<(string Key, int Value)>("readings")
            .IndexBy(r => r.Key)
            .Max(r => r.Value)
            .Output("maximums");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string Key, int Value)>("readings");
        var output = circuit.Output<KeyedValue<string, int>>("maximums");

        await input.InsertAsync(("k", 5));
        await input.InsertAsync(("k", 3));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, int>("k", 5)));

        // A larger value displaces the current maximum.
        await input.InsertAsync(("k", 9));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(new KeyedValue<string, int>("k", 5)));
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, int>("k", 9)));

        // Deleting the maximum falls back to the runner-up.
        await input.RemoveAsync(("k", 9));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(new KeyedValue<string, int>("k", 9)));
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, int>("k", 5)));
    }

    [Fact]
    public async Task Duplicate_minimum_survives_removing_one_instance()
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

        await input.InsertAsync(("k", 3));
        await input.InsertAsync(("k", 3));
        await input.InsertAsync(("k", 7));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, int>("k", 3)));

        // One copy of the minimum remains: the result is unchanged, so no delta at all.
        await input.RemoveAsync(("k", 3));
        await circuit.StepAsync();
        Assert.False(output.Reader.TryRead(out _));

        // Removing the last copy moves the minimum.
        await input.RemoveAsync(("k", 3));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(new KeyedValue<string, int>("k", 3)));
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, int>("k", 7)));
    }

    [Fact]
    public async Task Vanished_group_retracts_without_asserting()
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

        await input.InsertAsync(("k", 4));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new KeyedValue<string, int>("k", 4)));

        await input.RemoveAsync(("k", 4));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(-1, delta.WeightOf(new KeyedValue<string, int>("k", 4)));
        Assert.Equal(1, delta.DistinctCount);
    }

    [Fact]
    public async Task Insert_and_remove_in_one_step_nets_to_no_output()
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

        await input.InsertAsync(("k", 8));
        await circuit.StepAsync();
        _ = await output.Reader.ReadAsync();

        // A would-be new minimum appears and disappears within the same step.
        await input.InsertAsync(("k", 1));
        await input.RemoveAsync(("k", 1));
        await circuit.StepAsync();
        Assert.False(output.Reader.TryRead(out _));
    }

    [Fact]
    public async Task Min_over_large_churning_group_matches_oracle()
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

        var oracle = new SortedDictionary<int, int>(); // value -> multiplicity
        var integrated = new ZSet<KeyedValue<string, int>>();
        var random = new Random(42);

        async Task Insert(int value)
        {
            await input.InsertAsync(("k", value));
            oracle[value] = oracle.GetValueOrDefault(value) + 1;
        }

        async Task Remove(int value)
        {
            await input.RemoveAsync(("k", value));
            if (oracle[value] == 1)
                oracle.Remove(value);
            else
                oracle[value]--;
        }

        async Task StepAndVerify()
        {
            await circuit.StepAsync();
            while (output.Reader.TryRead(out var delta))
                integrated.AddAll(delta);
            if (oracle.Count == 0)
            {
                Assert.Equal(0, integrated.DistinctCount);
            }
            else
            {
                Assert.Equal(1, integrated.DistinctCount);
                Assert.Equal(1, integrated.WeightOf(new KeyedValue<string, int>("k", oracle.First().Key)));
            }
        }

        // Seed a large group in one step, duplicates included.
        for (var i = 0; i < 2000; i++)
            await Insert(random.Next(0, 1000));
        await StepAndVerify();

        // Churn: delete the current minimum often (the non-invertible case), mixed with
        // random inserts and deletes, several operations per step.
        for (var step = 0; step < 300; step++)
        {
            var ops = random.Next(1, 5);
            for (var i = 0; i < ops && oracle.Count > 0; i++)
            {
                switch (random.Next(3))
                {
                    case 0:
                        await Remove(oracle.First().Key);
                        break;
                    case 1:
                        await Insert(random.Next(0, 1000));
                        break;
                    default:
                        await Remove(oracle.ElementAt(random.Next(oracle.Count)).Key);
                        break;
                }
            }
            await StepAndVerify();
        }

        // Drain to empty: the final retraction must leave no rows behind.
        while (oracle.Count > 0)
        {
            await Remove(oracle.First().Key);
            await StepAndVerify();
        }
    }

    [Fact]
    public async Task Min_is_consistent_across_workers()
    {
        var builder = new CircuitBuilder(new CircuitOptions { Workers = 4 });
        builder.Input<(int Key, int Value)>("readings")
            .IndexBy(r => r.Key)
            .Min(r => r.Value)
            .Output("minimums");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(int Key, int Value)>("readings");
        var output = circuit.Output<KeyedValue<int, int>>("minimums");

        var expected = new Dictionary<int, int>();
        var random = new Random(7);
        for (var i = 0; i < 5000; i++)
        {
            var key = i % 41;
            var value = random.Next(100000);
            expected[key] = expected.TryGetValue(key, out var min) ? Math.Min(min, value) : value;
            await input.InsertAsync((key, value));
        }
        input.Complete();

        var integrated = new ZSet<KeyedValue<int, int>>();
        await foreach (var delta in output.ReadAllAsync())
            integrated.AddAll(delta);

        Assert.Equal(expected.Count, integrated.DistinctCount);
        foreach (var (key, min) in expected)
            Assert.Equal(1, integrated.WeightOf(new KeyedValue<int, int>(key, min)));
    }

    [Fact]
    public async Task Min_state_is_evicted_once_the_watermark_passes()
    {
        var T0 = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var storage = new GroupRemovalRecordingStorage();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        builder.Input<(int Value, DateTimeOffset At)>("readings")
            .WithWatermark(v => v.At, allowedLateness: TimeSpan.Zero)
            .TumblingWindow(TimeSpan.FromSeconds(5), v => v.At)
            .IndexBy(w => w.WindowStart)
            .WithEviction(windowStart => windowStart.AddSeconds(5))
            .Min(w => w.Value.Value)
            .Output("minimums");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(int Value, DateTimeOffset At)>("readings");
        var output = circuit.Output<KeyedValue<DateTimeOffset, int>>("minimums");

        // Step 1 populates window T0; the watermark freezes at T0+2s afterwards.
        await input.InsertAsync((10, T0.AddSeconds(1)));
        await input.InsertAsync((7, T0.AddSeconds(2)));
        await circuit.StepAsync();
        Assert.Empty(storage.RemovedGroupKeys);

        // Step 2 sees watermark T0+2s (< T0+5s): window T0 must survive. Freezes T0+7s.
        await input.InsertAsync((3, T0.AddSeconds(7)));
        await circuit.StepAsync();
        Assert.Empty(storage.RemovedGroupKeys);

        // Step 3 sees watermark T0+7s (>= T0+5s): every tree bucket of window T0 is deleted.
        await input.InsertAsync((5, T0.AddSeconds(12)));
        await circuit.StepAsync();
        Assert.Contains(storage.RemovedGroupKeys, slot => T0.Equals(GroupKeyOf(slot)));

        // Eviction emits no retractions: the closed window's published minimum stays final.
        var integrated = new ZSet<KeyedValue<DateTimeOffset, int>>();
        while (output.Reader.TryRead(out var delta))
            integrated.AddAll(delta);
        Assert.Equal(1, integrated.WeightOf(new KeyedValue<DateTimeOffset, int>(T0, 7)));
    }

    /// <summary>The tree store's group keys are internal bucket addresses carrying the
    /// evicted stream key in their <c>Key</c> property; read it reflectively.</summary>
    private static object? GroupKeyOf(object slot) =>
        slot.GetType().GetProperty("Key")?.GetValue(slot);

    /// <summary>In-memory storage that records the group keys removed from indexed stores.</summary>
    private sealed class GroupRemovalRecordingStorage : IStorageProvider
    {
        private readonly InMemoryStorageProvider _inner = new();
        private readonly List<object> _removedGroupKeys = [];

        public IReadOnlyList<object> RemovedGroupKeys
        {
            get { lock (_removedGroupKeys) return [.. _removedGroupKeys]; }
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
            lock (_removedGroupKeys)
                _removedGroupKeys.Add(key);
        }

        private sealed class RecordingIndexed<TKey, TValue>(
            IIndexedWeightStore<TKey, TValue> inner,
            GroupRemovalRecordingStorage owner) : IIndexedWeightStore<TKey, TValue>
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
