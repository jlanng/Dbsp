using Incremental.Diagnostics;
using Incremental.Storage;
using Incremental.Tables;

namespace Incremental.Tests;

/// <summary>
/// The estimates an operator sizes a node by: what an object costs, what a table holds
/// (rows, indexes, history) and what a pipeline's state holds. Each is set against what the
/// collector itself reports, so a change that makes an estimate wrong fails here and not in
/// someone's capacity plan. The class runs alone: the heap figures are the process's.
/// </summary>
[Collection(Name)]
public class MemoryEstimateTests
{
    public const string Name = "Memory";
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private sealed record Wide(
        long Id, string Name, string LegalName, string Country, string Rating, string Address, decimal Limit, decimal Exposure,
        bool Active, DateTimeOffset Onboarded, string Notes);

    private sealed record Narrow(long Id, int Bucket, decimal Amount);

    private sealed record Nested(int Id, Narrow Inner, string[] Tags, List<int> Numbers);

    private static Wide MakeWide(long id) => new(
        id, "Party " + id.ToString("D9"), "Party " + id.ToString("D9") + " Holdings International Limited", new string("GB".AsSpan()),
        new string("AA+".AsSpan()), (id % 900 + 1) + " Reference Data Street, Floor " + id % 40, id * 10m, id * 3.5m, id % 2 == 0,
        DateTimeOffset.UnixEpoch.AddDays(id % 3000), "Reviewed annually; revision " + id % 7);

    /// <summary>The bytes the collector hands out for <paramref name="make"/>'s objects,
    /// counted on this thread and for nothing else.</summary>
    private static (long Allocated, T[] Kept) Allocate<T>(int count, Func<int, T> make)
    {
        var kept = new T[count];
        make(-1); // whatever a first call allocates once (a static, a cached delegate) is not the object's
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < count; i++)
            kept[i] = make(i);
        return (GC.GetAllocatedBytesForCurrentThread() - before, kept);
    }

    private static void AssertWithin(double expected, double actual, double tolerance, string what)
    {
        var off = Math.Abs(actual - expected) / expected;
        Assert.True(off <= tolerance, $"{what}: estimated {actual:N0} bytes, measured {expected:N0} ({off:P1} apart, {tolerance:P0} allowed).");
    }

    [Fact]
    public void An_object_s_estimate_is_what_the_collector_allocates_for_it()
    {
        var size = new ObjectSize();

        // Strings made once each and held: no builder, no garbage between the readings.
        var (strings, heldStrings) = Allocate(2000, i => new string('x', 1 + Math.Abs(i) % 61));
        Assert.Equal(strings, heldStrings.Sum(s => size.Of(s)));

        var (arrays, heldArrays) = Allocate(500, i => new long[Math.Abs(i) % 17]);
        Assert.Equal(arrays, heldArrays.Sum(a => size.Of(a)));

        var (narrow, heldNarrow) = Allocate(2000, i => new Narrow(i, i % 7, i * 1.5m));
        Assert.Equal(narrow, heldNarrow.Sum(n => size.Of(n)));
        Assert.Equal(48, size.Of(heldNarrow[0])); // header 16, long 8, int 4 (+4), decimal 16

        // A record of strings: the strings' own bytes are most of it, and what building
        // them throws away is not the row's.
        var wide = MakeWide(123456);
        long expected = 16 + 8 + 6 * 8 + 16 + 16 + 1 + 16;
        expected = (expected + 7) / 8 * 8;
        foreach (var text in new[] { wide.Name, wide.LegalName, wide.Country, wide.Rating, wide.Address, wide.Notes })
            expected += (22 + 2 * text.Length + 7) / 8 * 8;
        Assert.Equal(expected, size.Of(wide));

        // Collections inside a row: arrays of references, and a list's backing array at
        // its capacity, not its count.
        var (nested, heldNested) = Allocate(500, i =>
        {
            var numbers = new List<int>(8) { 1, 2, 3 };
            return new Nested(i, new Narrow(i, 1, 2m), [new string('a', 5), new string('b', 9)], numbers);
        });
        Assert.Equal(nested, heldNested.Sum(n => size.Of(n)));
    }

    [Fact]
    public void An_object_reached_twice_is_counted_once_a_null_not_at_all_and_a_cycle_ends()
    {
        var size = new ObjectSize();
        var shared = new string('s', 100);
        var once = size.Of(new[] { shared });
        Assert.Equal(once, size.Of(new[] { shared, shared }) - 8);
        Assert.Equal(0, size.Of<string?>(null));
        Assert.Equal(0, size.Of(""));
        Assert.Equal(0, size.Of(42L));
        Assert.Equal(0, size.Of((1L, 2m)));
        // A tuple key that holds a reference costs what it refers to, not itself.
        Assert.Equal(size.Of(shared), size.Of((7L, shared)));

        var a = new Node();
        var b = new Node { Next = a };
        a.Next = b;
        Assert.Equal(2 * 24, size.Of(a));
    }

    private sealed class Node
    {
        public Node? Next;
    }

    private sealed class Vast
    {
        public object[] Children = [];
    }

    [Fact]
    public void A_walk_of_something_vast_stops_at_a_bound()
    {
        // A row that leads to something vast — sixty children of sixty children of
        // sixty, 220,000 objects and some 60 MB — is sized from the few thousand objects
        // a walk visits: it answers, and what it answers is that much and no more.
        Vast Level(int depth) => new() { Children = depth == 0 ? [] : Enumerable.Range(0, 60).Select(object (_) => Level(depth - 1)).ToArray() };
        var vast = new Vast { Children = Enumerable.Range(0, 60).Select(object (_) => Level(2)).ToArray() };
        var bytes = new ObjectSize().Of(vast);
        Assert.InRange(bytes, 100_000, 4L << 20);
    }

    [Fact]
    public void A_dictionary_inside_a_value_is_sized_by_its_arrays()
    {
        var size = new ObjectSize();
        var (allocated, held) = Allocate(200, i =>
        {
            var map = new Dictionary<string, int>(4);
            map[new string('k', 6)] = i;
            map[new string('q', 3)] = i;
            return map;
        });
        Assert.Equal(allocated, held.Sum(d => size.Of(d)));
    }

    /// <summary>What the heap holds more of once <paramref name="build"/> has run, with
    /// everything it built still reachable.</summary>
    private static async Task<(long Bytes, T Kept)> RetainedAsync<T>(Func<Task<T>> build)
    {
        for (var i = 0; i < 3; i++)
        {
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
        }
        var before = GC.GetTotalMemory(forceFullCollection: true);
        var kept = await build();
        for (var i = 0; i < 3; i++)
        {
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
        }
        var after = GC.GetTotalMemory(forceFullCollection: true);
        GC.KeepAlive(kept);
        return (after - before, kept);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_table_s_estimate_is_what_the_heap_holds_for_it(bool wide)
    {
        const int rows = 120_000;
        if (wide)
        {
            var (bytes, table) = await RetainedAsync(async () =>
            {
                var built = new SharedTable<long, Wide>("wide", r => r.Id);
                built.AddIndex("country", (Wide r) => r.Country);
                built.AddIndex("legal-name", (Wide r) => r.LegalName);
                for (var id = 0; id < rows; id++)
                {
                    await built.Writer.UpsertAsync(MakeWide(id));
                    if (id % 5000 == 4999)
                        await built.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
                }
                await built.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
                return built;
            });
            var estimate = table.EstimateMemory();
            Assert.Equal(rows, estimate.Rows);
            Assert.True(estimate.SampledRows >= 256, $"{estimate.SampledRows} rows sized");
            Assert.True(estimate.IndexBytes > rows * 100L, $"a unique index of {rows} rows estimated at {estimate.IndexBytes:N0} bytes");
            AssertWithin(bytes, estimate.TotalBytes, 0.10, "wide table with two indexes");
        }
        else
        {
            var (bytes, table) = await RetainedAsync(async () =>
            {
                var built = new SharedTable<long, Narrow>("narrow", r => r.Id);
                for (var id = 0; id < rows; id++)
                {
                    await built.Writer.UpsertAsync(new Narrow(id, id % 7, id));
                    if (id % 5000 == 4999)
                        await built.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
                }
                await built.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
                return built;
            });
            var estimate = table.EstimateMemory();
            Assert.Equal(48, estimate.MeanRowBytes);
            Assert.Equal(0, estimate.IndexBytes);
            AssertWithin(bytes, estimate.TotalBytes, 0.10, "narrow table");
        }
    }

    private sealed record Coded(string Code, string Name, long Number);

    private static Coded MakeCoded(long id) => new("LEI-" + id.ToString("D16"), "Counterparty " + id.ToString("D9"), id);

    /// <summary>A log that reads a key and a row apart, as one that keeps them as bytes
    /// does: every key it hands over is an object of its own, equal to the row's.</summary>
    private sealed class ApartLog(InMemoryTableLog<string, Coded> inner) : ITableLog<string, Coded>
    {
        private static string Copy(string text) => new(text.AsSpan());
        private static Coded? Copy(Coded? row) => row is null ? null : new Coded(Copy(row.Code), Copy(row.Name), row.Number);

        public string Epoch => inner.Epoch;
        public long OldestRetainedVersion => inner.OldestRetainedVersion;
        public ValueTask<long> LastVersionAsync(CancellationToken cancellationToken = default) => inner.LastVersionAsync(cancellationToken);
        public ValueTask AppendAsync(TableVersion<string, Coded> version, CancellationToken cancellationToken = default) => inner.AppendAsync(version, cancellationToken);
        public ValueTask TrimBeforeAsync(long version, CancellationToken cancellationToken = default) => inner.TrimBeforeAsync(version, cancellationToken);

        public ValueTask<TableHead> RestoreAsync(Action<string, Coded> row, CancellationToken cancellationToken = default) =>
            inner.RestoreAsync((key, held) => row(Copy(key), Copy(held)!), cancellationToken);

        private static TableVersion<string, Coded> Apart(TableVersion<string, Coded> version) => new(
            version.Version,
            version.Changes.Select(change => new TableChange<string, Coded>(Copy(change.Key), Copy(change.Old), Copy(change.New))).ToList(),
            version.Offsets);

        public async IAsyncEnumerable<TableVersion<string, Coded>> ReadAsync(
            long fromExclusive, long toInclusive, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (var version in inner.ReadAsync(fromExclusive, toInclusive, cancellationToken))
                yield return Apart(version);
        }

        public async IAsyncEnumerable<TableVersion<string, Coded>> FollowAsync(
            long fromExclusive, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (var version in inner.FollowAsync(fromExclusive, cancellationToken))
                yield return Apart(version);
        }
    }

    [Fact]
    public async Task A_table_restored_from_a_log_holds_each_row_under_the_rows_own_key_and_its_estimate_says_what_it_holds()
    {
        // Keyed by a string: a log hands the key over as an object beside the row's own.
        // Held as given, that is a string per row which the estimate — a reference per
        // key — does not count, and which a writer that built the same table never had.
        const int rows = 120_000;
        var log = new InMemoryTableLog<string, Coded>();
        var written = await SharedTable<string, Coded>.OpenAsync("coded", r => r.Code, log);
        for (var id = 0; id < rows; id++)
        {
            await written.Writer.UpsertAsync(MakeCoded(id));
            if (id % 5000 == 4999)
                await written.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
        }
        await written.Writer.CommitAsync().AsTask().WaitAsync(Timeout);

        var (bytes, restored) = await RetainedAsync(
            async () => await SharedTable<string, Coded>.OpenAsync("coded", r => r.Code, new ApartLog(log.Connect())));
        var estimate = restored.EstimateMemory();
        Assert.Equal(rows, estimate.Rows);
        AssertWithin(bytes, estimate.TotalBytes, 0.10, "a table keyed by a string, restored from a log");
        AssertWithin(written.EstimateMemory().RowBytes, estimate.RowBytes, 0.02, "the restored table against the one its writer holds");
        foreach (var (key, row) in restored.CurrentRows())
            Assert.Same(row.Code, key);

        // A follower applies the log's versions the same way: new keys are the rows' own.
        await using var follower = await SharedTable<string, Coded>.FollowAsync("coded", r => r.Code, new ApartLog(log.Connect()));
        for (var id = rows; id < rows + 500; id++)
            await written.Writer.UpsertAsync(MakeCoded(id));
        await written.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
        for (var i = 0; i < 500 && follower.LatestVersion < written.LatestVersion; i++)
            await Task.Delay(10);
        Assert.Equal(rows + 500, follower.Count);
        foreach (var (key, row) in follower.CurrentRows())
            Assert.Same(row.Code, key);
    }

    [Fact]
    public async Task A_table_opened_with_another_key_selector_than_it_was_written_with_keeps_the_logs_keys()
    {
        // The row's own key is held only where it equals the log's. A selector that has
        // changed since the rows were written gives another key, and the log's stands:
        // it is what the log's later changes will name.
        var log = new InMemoryTableLog<string, Coded>();
        var written = await SharedTable<string, Coded>.OpenAsync("coded", r => r.Code, log);
        await written.Writer.UpsertAsync(new Coded("lei-1", "One", 1));
        await written.Writer.CommitAsync().AsTask().WaitAsync(Timeout);

        var restored = await SharedTable<string, Coded>.OpenAsync("coded", r => r.Code.ToUpperInvariant(), new ApartLog(log.Connect()));
        using var snapshot = restored.Snapshot();
        Assert.True(snapshot.TryGet("lei-1", out var row));
        Assert.Equal("One", row!.Name);
        Assert.False(snapshot.TryGet("LEI-1", out _));
    }

    [Fact]
    public async Task A_table_s_history_is_the_rows_its_recent_versions_replaced_and_no_more_than_the_tail_holds()
    {
        var table = new SharedTable<long, Wide>("history", r => r.Id, new SharedTableOptions { TailChanges = 3000, TailVersions = 1000 });
        for (var id = 0; id < 10_000; id++)
            await table.Writer.UpsertAsync(MakeWide(id));
        await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);

        // A load replaces nothing: the one version the tail keeps holds no superseded row.
        var loaded = table.EstimateMemory();
        Assert.Equal(0, loaded.HistoryRows);
        Assert.Equal(10_000, loaded.HistoryChanges);

        // Ten versions of 200 replaced rows each: the load leaves the tail (it is over the
        // bound and nobody reads it) and each replaced row is held once.
        for (var version = 0; version < 10; version++)
        {
            for (var id = 0; id < 200; id++)
                await table.Writer.UpsertAsync(MakeWide(id + version * 200) with { Notes = "revised " + version });
            await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
        }
        var revised = table.EstimateMemory();
        Assert.Equal(2000, revised.HistoryRows);
        Assert.Equal(2000, revised.HistoryChanges);
        Assert.InRange(revised.HistoryBytes, (long)(2000 * revised.MeanRowBytes), (long)(2000 * (revised.MeanRowBytes + 64)));
        Assert.Equal(10_000, revised.Rows);

        // Deletes hold the row they removed; past the bound the oldest versions go.
        for (var version = 0; version < 20; version++)
        {
            for (var id = 0; id < 200; id++)
                await table.Writer.DeleteAsync(5000 + id + version * 200);
            await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
        }
        var deleted = table.EstimateMemory();
        Assert.Equal(6000, deleted.Rows);
        Assert.InRange(deleted.HistoryRows, 2800, 3000);
        Assert.Equal(deleted.HistoryRows, deleted.HistoryChanges);
    }

    [Fact]
    public async Task An_index_s_estimate_follows_its_groups_as_rows_come_move_and_go()
    {
        var table = new SharedTable<long, Wide>("indexed", r => r.Id);
        table.AddIndex("country", (Wide r) => r.Country);
        Assert.Equal(0, table.EstimateMemory().IndexBytes);

        for (var id = 0; id < 1000; id++)
            await table.Writer.UpsertAsync(MakeWide(id) with { Country = id % 2 == 0 ? "GB" : "US" });
        await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
        var two = table.EstimateMemory().IndexBytes;
        // Two groups, a thousand primary keys of eight bytes between them.
        Assert.InRange(two, 8000, 8000 + 2 * 200);

        // Every row to a group of its own: a thousand groups.
        for (var id = 0; id < 1000; id++)
            await table.Writer.UpsertAsync(MakeWide(id) with { Country = "C" + id });
        await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
        var thousand = table.EstimateMemory().IndexBytes;
        Assert.InRange(thousand, 1000 * 100, 1000 * 200);

        for (var id = 0; id < 1000; id++)
            await table.Writer.DeleteAsync(id);
        await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
        Assert.Equal(0, table.EstimateMemory().IndexBytes);
    }

    private sealed record Order(long Id, long PartyId, string Symbol, decimal Quantity, decimal Price, string Trader);

    private static Order MakeOrder(long id) => new(id, id % 5000, "SYM" + id % 300, id % 97, 100 + id % 13, "trader-" + id % 50);

    private static Dictionary<string, long> Offsets(long n) => new() { ["in"] = n };

    [Fact]
    public async Task A_pipeline_s_state_estimate_counts_its_entries_and_is_what_the_heap_holds_for_them()
    {
        const int orders = 100_000;
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");

        var (bytes, _) = await RetainedAsync(async () =>
        {
            var byId = provider.CreateValueStore<long, Order>("orders");
            var byParty = provider.CreateIndexedWeightStore<long, Order>("by-party");
            var seen = provider.CreateWeightStore<Order>("seen");
            var deadlines = provider.CreateDeadlineStore<long>("deadlines");
            provider.Recover();
            for (var id = 0; id < orders; id++)
            {
                // Each store its own instance, as a node that restored the state has.
                byId.Put(id, MakeOrder(id));
                byParty.Update(id % 5000, MakeOrder(id), 1);
                if (id % 4 == 0)
                    seen.Update(MakeOrder(id), 1);
                if (id % 10 == 0)
                    deadlines.Set(id, 1000 + id);
                if (id % 10_000 == 9999)
                    await provider.CommitAsync(Offsets(id));
            }
            await provider.CommitAsync(Offsets(orders));
            return (byId, byParty, seen, deadlines);
        });

        var estimate = provider.EstimateSize();
        Assert.Equal(provider.CountEntries(), estimate.Entries);
        Assert.Equal(orders + orders + orders / 4 + orders / 10, estimate.Entries);
        Assert.Equal(0, estimate.EntriesAsBytes);
        Assert.Equal(0, estimate.EntriesHeldTwice);
        Assert.Equal(["by-party", "deadlines", "orders", "seen"], estimate.Stores.Select(s => s.Name).Order());
        Assert.Equal(["deadline", "indexed", "value", "weight"], estimate.Stores.Select(s => s.Kind).Order());
        Assert.All(estimate.Stores, s => Assert.True(s.SampledEntries >= 256 && s.MeanRecordBytes > 0, s.ToString()));
        var perOrder = new ObjectSize().Of(MakeOrder(12345));
        Assert.InRange(estimate.Stores.Single(s => s.Name == "orders").MeanEntryBytes, perOrder - 8, perOrder + 8);
        Assert.Equal(0, estimate.Stores.Single(s => s.Name == "deadlines").MeanEntryBytes);

        // A store's own maps are counted beside the objects: a deadline is nothing but a
        // slot in a map and a place in the queue.
        var deadlineStore = estimate.Stores.Single(s => s.Name == "deadlines");
        Assert.InRange(deadlineStore.EstimatedBytes, deadlineStore.Entries * 40, deadlineStore.Entries * 120);
        var orderStore = estimate.Stores.Single(s => s.Name == "orders");
        Assert.InRange(orderStore.EstimatedBytes, (long)(orderStore.Entries * (orderStore.MeanEntryBytes + 28)), (long)(orderStore.Entries * (orderStore.MeanEntryBytes + 90)));
        var seenStore = estimate.Stores.Single(s => s.Name == "seen");
        Assert.InRange(seenStore.EstimatedBytes, (long)(seenStore.Entries * (seenStore.MeanEntryBytes + 24)), (long)(seenStore.Entries * (seenStore.MeanEntryBytes + 90)));
        var partyStore = estimate.Stores.Single(s => s.Name == "by-party");
        Assert.InRange(partyStore.EstimatedBytes, (long)(partyStore.Entries * (partyStore.MeanEntryBytes + 40)), (long)(partyStore.Entries * (partyStore.MeanEntryBytes + 160)));

        // The in-memory changelog holds the records too; they are not the pipeline's
        // state on the heap, so they come off what was measured.
        var logBytes = estimate.Stores.Sum(s => (long)(s.Entries * (s.MeanRecordBytes + 64 + 2 * 24 + 40)));
        AssertWithin(bytes - logBytes, estimate.EstimatedBytes, 0.25, "state of four stores");
    }

    [Fact]
    public async Task State_that_waits_as_bytes_is_counted_as_bytes_and_state_held_twice_says_so_until_its_types_commit()
    {
        const int orders = 20_000;
        var log = new InMemoryStateChangelog(partitions: 1);
        await using (var storage = await ChangelogStorage.OpenAsync(log))
        {
            var writer = storage.Pipeline("p");
            var store = writer.CreateValueStore<long, Order>("orders");
            writer.Recover();
            for (var id = 0; id < orders; id++)
                store.Put(id, MakeOrder(id));
            await writer.CommitAsync(Offsets(orders));
        }

        // A node that has read the log and built nothing: every entry waits as bytes.
        await using var restored = await ChangelogStorage.OpenAsync(log);
        var provider = restored.Pipeline("p");
        var waiting = provider.EstimateSize();
        Assert.Equal(orders, waiting.Entries);
        Assert.Equal(orders, waiting.EntriesAsBytes);
        Assert.Equal(0, waiting.EntriesHeldTwice);
        var bytes = Assert.Single(waiting.Stores);
        Assert.Equal(("orders", "bytes", (long)orders), (bytes.Name, bytes.Kind, bytes.Entries));
        Assert.InRange(bytes.MeanRecordBytes, 60, 200);
        Assert.InRange(waiting.EstimatedBytes, (long)(orders * bytes.MeanRecordBytes), (long)(orders * (bytes.MeanRecordBytes + 120)));

        // A circuit built on it holds the entries typed and, until its types have
        // committed, the bytes they were made from beside them.
        var typed = provider.CreateValueStore<long, Order>("orders");
        provider.Recover();
        var twice = provider.EstimateSize();
        Assert.Equal(orders, twice.Entries);
        Assert.Equal(0, twice.EntriesAsBytes);
        Assert.Equal(orders, twice.EntriesHeldTwice);
        Assert.InRange(twice.BytesHeldTwice, (long)(orders * bytes.MeanRecordBytes), (long)(orders * (bytes.MeanRecordBytes + 120)));
        // Entries read into a store are sized as they are read, like entries written.
        var read = Assert.Single(twice.Stores);
        Assert.Equal("value", read.Kind);
        Assert.True(read.SampledEntries >= 256, $"{read.SampledEntries} entries sized");
        var perOrder = new ObjectSize().Of(MakeOrder(12345));
        Assert.InRange(read.MeanEntryBytes, perOrder - 8, perOrder + 8);
        Assert.InRange(read.MeanRecordBytes, bytes.MeanRecordBytes - 1, bytes.MeanRecordBytes + 1);
        Assert.True(twice.EstimatedBytes > twice.BytesHeldTwice * 2, "the typed entries are counted beside the bytes");

        typed.Put(1, MakeOrder(1) with { Symbol = "CHANGED" });
        await provider.CommitAsync(Offsets(orders + 1));
        var committed = provider.EstimateSize();
        Assert.Equal(0, committed.EntriesHeldTwice);
        Assert.Equal(0, committed.BytesHeldTwice);
        Assert.Equal(twice.EstimatedBytes - twice.BytesHeldTwice, committed.EstimatedBytes, tolerance: 4096.0);

        // Given up, the types' entries are bytes again.
        provider.ReleaseStores();
        var released = provider.EstimateSize();
        Assert.Equal(orders, released.EntriesAsBytes);
        Assert.Equal("bytes", Assert.Single(released.Stores).Kind);
    }

    [Fact]
    public void The_indexed_store_counts_its_entries_through_every_way_one_comes_and_goes()
    {
        var store = new InMemoryStorageProvider.IndexedWeightStore<int, string>();
        var model = new Dictionary<(int, string), long>();
        var random = new Random(5);
        void Check()
        {
            Assert.Equal(model.Count, store.EntryCount);
            Assert.Equal(model.Count, store.Entries().LongCount());
            var shape = store.Shape;
            Assert.Equal(model.Keys.Select(k => k.Item1).Distinct().Count(), shape.Groups);
        }
        for (var step = 0; step < 20_000; step++)
        {
            var key = random.Next(40);
            var value = "v" + random.Next(4);
            switch (random.Next(10))
            {
                case 0:
                    store.RemoveGroup(key);
                    foreach (var gone in model.Keys.Where(k => k.Item1 == key).ToList())
                        model.Remove(gone);
                    break;
                case 1:
                    store.Set(key, value, 0);
                    model.Remove((key, value));
                    break;
                case 2:
                    var entries = Enumerable.Range(0, 5).Select(i => (key, "v" + i, (long)random.Next(-1, 2))).ToList();
                    store.ApplyAll(entries);
                    foreach (var (k, v, delta) in entries)
                    {
                        var now = model.GetValueOrDefault((k, v)) + delta;
                        if (now == 0)
                            model.Remove((k, v));
                        else
                            model[(k, v)] = now;
                    }
                    break;
                default:
                    var change = random.Next(-2, 3);
                    store.Update(key, value, change);
                    var updated = model.GetValueOrDefault((key, value)) + change;
                    if (updated == 0)
                        model.Remove((key, value));
                    else
                        model[(key, value)] = updated;
                    break;
            }
            if (step % 97 == 0)
                Check();
        }
        Check();
        store.Clear();
        Assert.Equal(0, store.EntryCount);
        Assert.Equal((0, 0L), (store.Shape.Groups, store.Shape.ManyGroups));
    }
}

[CollectionDefinition(MemoryEstimateTests.Name, DisableParallelization = true)]
public sealed class MemoryCollection;
