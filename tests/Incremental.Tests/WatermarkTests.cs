using Incremental.Storage;

namespace Incremental.Tests;

/// <summary>
/// Watermark and eviction mechanics exercised through the manual window-as-data escape
/// hatch (<c>SelectMany</c> into <see cref="Windowed{T}"/> + <c>GroupBy</c> +
/// <c>EvictWhen</c>): these tests assert on the generic keyed eviction path, and the
/// escape hatch deserves coverage of its own. The paved windowed-grouping road is covered
/// in <see cref="WindowingAndTopKTests"/> and <see cref="WindowCloseTests"/>.
/// </summary>
public class WatermarkTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Events_older_than_the_watermark_are_dropped()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var views = builder.Input<(string Page, DateTimeOffset At)>("views");
        var countsDef = views.WithEventTime(v => v.At, lateness: TimeSpan.FromSeconds(5))
                .Tumble(TimeSpan.FromSeconds(5), v => v.At)
            .GroupBy(w => w.WindowStart)
            .Count()
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(views);
        var output = circuit.Handle(countsDef);

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
        var views = builder.Input<(string Page, DateTimeOffset At)>("views");
        var countsDef = views.WithEventTime(v => v.At, lateness: TimeSpan.Zero)
                .Tumble(TimeSpan.FromSeconds(5), v => v.At)
            .GroupBy(w => w.WindowStart)
            .EvictWhen(windowStart => windowStart.AddSeconds(5))
            .Count()
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(views);
        var output = circuit.Handle(countsDef);

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

    [Fact]
    public async Task Join_state_is_evicted_once_the_watermark_passes()
    {
        var storage = new EvictionRecordingStorage();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        var users = builder.Input<(string User, DateTimeOffset At)>("users");
        var pages = builder.Input<(string Page, DateTimeOffset At)>("pages");
        var usersByWindow = users.WithEventTime(v => v.At, lateness: TimeSpan.Zero)
                .Tumble(TimeSpan.FromSeconds(5), v => v.At)
            .GroupBy(w => w.WindowStart)
            .EvictWhen(windowStart => windowStart.AddSeconds(5));
        var pagesByWindow = pages.WithEventTime(v => v.At, lateness: TimeSpan.Zero)
                .Tumble(TimeSpan.FromSeconds(5), v => v.At)
            .GroupBy(w => w.WindowStart);
        var joinedDef = usersByWindow
            .Join(pagesByWindow, (window, user, page) => (window, user.Value.User, page.Value.Page))
            .ToOutput();

        await using var circuit = builder.Build();
        circuit.Start();
        var userInput = circuit.Handle(users);
        var pageInput = circuit.Handle(pages);
        var output = circuit.Handle(joinedDef);

        // Step 1 joins within window T0; no watermark is frozen yet, nothing evicts.
        await userInput.InsertAsync(("alice", T0.AddSeconds(1)));
        await pageInput.InsertAsync(("home", T0.AddSeconds(2)));
        await circuit.StepAsync();
        Assert.Empty(storage.RemovedKeys);

        // Step 2 sees watermark T0+1s (< T0+5s): window T0's join state must survive.
        await userInput.InsertAsync(("bob", T0.AddSeconds(7)));
        await pageInput.InsertAsync(("about", T0.AddSeconds(8)));
        await circuit.StepAsync();
        Assert.Empty(storage.RemovedKeys);

        // Step 3 sees watermark T0+7s (>= T0+5s): both sides of window T0 are deleted.
        await userInput.InsertAsync(("carol", T0.AddSeconds(12)));
        await pageInput.InsertAsync(("news", T0.AddSeconds(12)));
        await circuit.StepAsync();
        Assert.Equal(2, storage.RemovedKeys.Count(k => T0.Equals(k)));

        // Eviction emits no retractions: the closed window's join rows stay final.
        var integrated = new ZSet<(DateTimeOffset Window, string User, string Page)>();
        while (output.Reader.TryRead(out var delta))
            integrated.AddAll(delta);
        Assert.Equal(1, integrated.WeightOf((T0, "alice", "home")));
        Assert.Equal(1, integrated.WeightOf((T0.AddSeconds(5), "bob", "about")));
        Assert.Equal(1, integrated.WeightOf((T0.AddSeconds(10), "carol", "news")));
    }

    [Fact]
    public async Task AntiJoin_state_is_evicted_once_the_watermark_passes()
    {
        var storage = new EvictionRecordingStorage();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        var users = builder.Input<(string User, DateTimeOffset At)>("users");
        var blocks = builder.Input<(string Block, DateTimeOffset At)>("blocks");
        var usersByWindow = users.WithEventTime(v => v.At, lateness: TimeSpan.Zero)
                .Tumble(TimeSpan.FromSeconds(5), v => v.At)
            .GroupBy(w => w.WindowStart)
            .EvictWhen(windowStart => windowStart.AddSeconds(5));
        var blocksByWindow = blocks.WithEventTime(v => v.At, lateness: TimeSpan.Zero)
                .Tumble(TimeSpan.FromSeconds(5), v => v.At)
            .GroupBy(w => w.WindowStart);
        var unmatchedDef = usersByWindow
            .AntiJoin(blocksByWindow)
            .MapValues(w => w.Value.User)
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var userInput = circuit.Handle(users);
        var blockInput = circuit.Handle(blocks);
        var output = circuit.Handle(unmatchedDef);

        // Step 1: window T0 has no right rows, so alice is emitted as unmatched.
        await userInput.InsertAsync(("alice", T0.AddSeconds(1)));
        await blockInput.InsertAsync(("b1", T0.AddSeconds(7)));
        await circuit.StepAsync();
        Assert.Empty(storage.RemovedKeys);

        // Step 2: bob lands in window T0+5s, which is blocked, so nothing is emitted.
        await userInput.InsertAsync(("bob", T0.AddSeconds(8)));
        await blockInput.InsertAsync(("b2", T0.AddSeconds(12)));
        await circuit.StepAsync();
        Assert.Empty(storage.RemovedKeys);

        // Step 3 sees watermark T0+8s (>= T0+5s): window T0's left rows and right row
        // count are both deleted.
        await userInput.InsertAsync(("carol", T0.AddSeconds(14)));
        await blockInput.InsertAsync(("b3", T0.AddSeconds(14)));
        await circuit.StepAsync();
        Assert.Equal(2, storage.RemovedKeys.Count(k => T0.Equals(k)));

        // Eviction emits no retractions: alice's unmatched row stays final; bob never appears.
        var integrated = new ZSet<KeyedValue<DateTimeOffset, string>>();
        while (output.Reader.TryRead(out var delta))
            integrated.AddAll(delta);
        Assert.Equal(1, integrated.WeightOf(new KeyedValue<DateTimeOffset, string>(T0, "alice")));
        Assert.Equal(0, integrated.WeightOf(new KeyedValue<DateTimeOffset, string>(T0.AddSeconds(5), "bob")));
    }

    [Fact]
    public async Task Keyed_distinct_state_is_evicted_once_the_watermark_passes()
    {
        var storage = new EvictionRecordingStorage();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        var views = builder.Input<(string User, DateTimeOffset At)>("views");
        var distinctDef = views.WithEventTime(v => v.At, lateness: TimeSpan.Zero)
                .Tumble(TimeSpan.FromSeconds(5), v => v.At)
            .Select(w => (Window: w.WindowStart, User: w.Value.User))
            .GroupBy(x => x.Window)
            .EvictWhen(windowStart => windowStart.AddSeconds(5))
            .Distinct()
            .MapValues(x => x.User)
            .ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(views);
        var output = circuit.Handle(distinctDef);

        // Step 1: alice appears twice in window T0 but is emitted once.
        await input.InsertAsync(("alice", T0.AddSeconds(1)));
        await input.InsertAsync(("alice", T0.AddSeconds(2)));
        await circuit.StepAsync();
        var delta = await output.Reader.ReadAsync();
        Assert.Equal(1, delta.WeightOf(new KeyedValue<DateTimeOffset, string>(T0, "alice")));
        Assert.Equal(1, delta.DistinctCount);
        Assert.Empty(storage.RemovedKeys);

        // Step 2 sees watermark T0+2s (< T0+5s): window T0 must survive. Freezes T0+7s.
        await input.InsertAsync(("alice", T0.AddSeconds(7)));
        await circuit.StepAsync();
        Assert.Empty(storage.RemovedKeys);

        // Step 3 sees watermark T0+7s (>= T0+5s): window T0's integrated weights are deleted.
        await input.InsertAsync(("bob", T0.AddSeconds(12)));
        await circuit.StepAsync();
        Assert.Contains(T0, storage.RemovedKeys);

        // Eviction emits no retractions: alice's presence in window T0 stays final.
        var integrated = new ZSet<KeyedValue<DateTimeOffset, string>>();
        integrated.AddAll(delta);
        while (output.Reader.TryRead(out var later))
            integrated.AddAll(later);
        Assert.Equal(1, integrated.WeightOf(new KeyedValue<DateTimeOffset, string>(T0, "alice")));
    }

    /// <summary>In-memory storage that records evicted keys: group removals from indexed
    /// stores, entry removals from value stores (how linear aggregates evict), and entry
    /// removals from weight stores (how the anti-join drops its right-side row counts).</summary>
    private sealed class EvictionRecordingStorage : IStorageProvider
    {
        private readonly InMemoryStorageProvider _inner = new();
        private readonly List<object> _removedKeys = [];

        public IReadOnlyList<object> RemovedKeys
        {
            get { lock (_removedKeys) return [.. _removedKeys]; }
        }

        public IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull =>
            new RecordingWeights<T>(_inner.CreateWeightStore<T>(name), this);

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

        private sealed class RecordingWeights<T>(
            IWeightStore<T> inner,
            EvictionRecordingStorage owner) : IWeightStore<T>
            where T : notnull
        {
            public long Update(T value, long delta) => inner.Update(value, delta);
            public long WeightOf(T value) => inner.WeightOf(value);

            public void Remove(T value)
            {
                owner.Record(value);
                inner.Remove(value);
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
