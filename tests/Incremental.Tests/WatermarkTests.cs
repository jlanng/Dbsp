using Incremental.Storage;

namespace Incremental.Tests;

public class WatermarkTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Events_older_than_the_watermark_are_dropped()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        builder.Input<(string Page, DateTimeOffset At)>("views")
            .WithWatermark(v => v.At, allowedLateness: TimeSpan.FromSeconds(5))
            .TumblingWindow(TimeSpan.FromSeconds(5), v => v.At)
            .IndexBy(w => w.WindowStart)
            .Count()
            .Output("counts");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string Page, DateTimeOffset At)>("views");
        var output = circuit.Output<KeyedValue<DateTimeOffset, long>>("counts");

        // First step advances the watermark to T0+20s − 5s lateness = T0+15s.
        await input.InsertAsync(("home", T0.AddSeconds(20)));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new KeyedValue<DateTimeOffset, long>(T0.AddSeconds(20), 1)));

        // T0+2s is far below the watermark and must be dropped; T0+16s is within lateness.
        await input.InsertAsync(("home", T0.AddSeconds(2)));
        await input.InsertAsync(("home", T0.AddSeconds(16)));
        await circuit.StepAsync();
        delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new KeyedValue<DateTimeOffset, long>(T0.AddSeconds(15), 1)));
        Assert.Equal(1, delta.DistinctCount); // no delta for the dropped event's window
    }

    [Fact]
    public async Task Closed_window_state_is_evicted_once_the_watermark_passes()
    {
        var storage = new EvictionRecordingStorage();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        builder.Input<(string Page, DateTimeOffset At)>("views")
            .WithWatermark(v => v.At, allowedLateness: TimeSpan.Zero)
            .TumblingWindow(TimeSpan.FromSeconds(5), v => v.At)
            .IndexBy(w => w.WindowStart)
            .WithEviction(windowStart => windowStart.AddSeconds(5))
            .Count()
            .Output("counts");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<(string Page, DateTimeOffset At)>("views");
        var output = circuit.Output<KeyedValue<DateTimeOffset, long>>("counts");

        // Step 1 populates window T0; the watermark freezes at T0+1s afterwards.
        await input.InsertAsync(("home", T0.AddSeconds(1)));
        await circuit.StepAsync();
        Assert.Empty(storage.RemovedKeys);

        // Step 2 sees watermark T0+1s (< T0+5s): window T0 must survive. Freezes T0+7s.
        await input.InsertAsync(("home", T0.AddSeconds(7)));
        await circuit.StepAsync();
        Assert.Empty(storage.RemovedKeys);

        // Step 3 sees watermark T0+7s (>= T0+5s): window T0's state is deleted.
        await input.InsertAsync(("home", T0.AddSeconds(12)));
        await circuit.StepAsync();
        Assert.Contains(T0, storage.RemovedKeys);

        // Eviction emits no retractions: the closed window's published count stays final.
        var integrated = new ZSet<KeyedValue<DateTimeOffset, long>>();
        while (output.Reader.TryRead(out var delta))
            integrated.AddAll(delta);
        Assert.Equal(1, integrated.WeightOf(new KeyedValue<DateTimeOffset, long>(T0, 1)));
    }

    /// <summary>In-memory storage that records evicted keys: group removals from indexed
    /// stores, and entry removals from value stores (how linear aggregates evict).</summary>
    private sealed class EvictionRecordingStorage : IStorageProvider
    {
        private readonly InMemoryStorageProvider _inner = new();
        private readonly List<object> _removedKeys = [];

        public IReadOnlyList<object> RemovedKeys
        {
            get { lock (_removedKeys) return [.. _removedKeys]; }
        }

        public IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull =>
            _inner.CreateWeightStore<T>(name);

        public IIndexedWeightStore<TKey, TValue> CreateIndexedWeightStore<TKey, TValue>(string name)
            where TKey : notnull
            where TValue : notnull =>
            new RecordingIndexed<TKey, TValue>(_inner.CreateIndexedWeightStore<TKey, TValue>(name), this);

        public IValueStore<TKey, TValue> CreateValueStore<TKey, TValue>(string name) where TKey : notnull =>
            new RecordingValues<TKey, TValue>(_inner.CreateValueStore<TKey, TValue>(name), this);

        private void Record(object key)
        {
            lock (_removedKeys)
                _removedKeys.Add(key);
        }

        private sealed class RecordingIndexed<TKey, TValue>(
            IIndexedWeightStore<TKey, TValue> inner,
            EvictionRecordingStorage owner) : IIndexedWeightStore<TKey, TValue>
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

        private sealed class RecordingValues<TKey, TValue>(
            IValueStore<TKey, TValue> inner,
            EvictionRecordingStorage owner) : IValueStore<TKey, TValue>
            where TKey : notnull
        {
            public bool TryGet(TKey key, [System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] out TValue value) =>
                inner.TryGet(key, out value);

            public void Put(TKey key, TValue value) => inner.Put(key, value);

            public void Remove(TKey key)
            {
                owner.Record(key);
                inner.Remove(key);
            }
        }
    }
}
