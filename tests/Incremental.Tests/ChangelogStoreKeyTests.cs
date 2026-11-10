using Incremental.Storage;

namespace Incremental.Tests;

/// <summary>
/// Two things about how a <see cref="ChangelogStorageProvider"/> store tracks its entries.
/// An entry's record is written under one instance however many equal instances touch it —
/// <c>10.0m</c> equals <c>10.00m</c> and serializes differently — so that removing the entry
/// removes its record. And the map of entries touched since the last commit keeps its
/// capacity between commits.
/// </summary>
public class ChangelogStoreKeyTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private static Dictionary<string, long> Offsets(long value) => new() { ["in"] = value };

    private sealed record Priced(int Id, decimal Amount);

    private sealed record Plain(int Id, string Region);

    private sealed record Tolerant(int Id, string Note)
    {
        public bool Equals(Tolerant? other) => other is not null && other.Id == Id;
        public override int GetHashCode() => Id;
    }

    private readonly record struct Pair(int A, long B);

    private struct Bare(int a, string b)
    {
        public int A = a;
        public string B = b;
    }

    private enum Colour { Red, Green }

    /// <summary>What a process that restores from the log alone holds, before and after the
    /// log is compacted.</summary>
    private static async Task<List<string>[]> RestoredAsync(InMemoryStateChangelog log, ChangelogStorage writer)
    {
        var states = new List<string>[2];
        for (var pass = 0; pass < 2; pass++)
        {
            if (pass == 1)
            {
                await writer.WhenCaughtUpAsync().WaitAsync(Timeout);
                log.Compact();
            }
            await using var copy = ChangelogStorage.Follow(log);
            await copy.WhenCaughtUpAsync().WaitAsync(Timeout);
            states[pass] = copy.Pipeline("p").DescribeState();
        }
        return states;
    }

    private static async Task AssertRestoresAsAsync(InMemoryStateChangelog log, ChangelogStorage writer, string when)
    {
        var expected = writer.Pipeline("p").DescribeState();
        var restored = await RestoredAsync(log, writer);
        Assert.True(expected.SequenceEqual(restored[0]),
            $"{when}: the writer holds [{string.Join("; ", expected)}] but the log restores [{string.Join("; ", restored[0])}]");
        Assert.True(expected.SequenceEqual(restored[1]),
            $"{when}, compacted: the writer holds [{string.Join("; ", expected)}] but the log restores [{string.Join("; ", restored[1])}]");
    }

    // ── which types need it ────────────────────────────────────────────────────────────

    [Fact]
    public void Equal_values_serialize_alike_for_structural_types_of_exact_parts_only()
    {
        Assert.True(ByteStableEquality.Holds<int>());
        Assert.True(ByteStableEquality.Holds<string>());
        Assert.True(ByteStableEquality.Holds<Guid>());
        Assert.True(ByteStableEquality.Holds<Colour>());
        Assert.True(ByteStableEquality.Holds<long?>());
        Assert.True(ByteStableEquality.Holds<Plain>());
        Assert.True(ByteStableEquality.Holds<Pair>());
        Assert.True(ByteStableEquality.Holds<Bare>());
        Assert.True(ByteStableEquality.Holds<(int, string)>());
        Assert.True(ByteStableEquality.Holds<KeyedValue<string, Plain>>());
        Assert.True(ByteStableEquality.Holds<WindowKey<int>>());
        Assert.True(ByteStableEquality.Holds<KeyedValue<WindowKey<string>, long>>());

        Assert.False(ByteStableEquality.Holds<decimal>());
        Assert.False(ByteStableEquality.Holds<double>());
        Assert.False(ByteStableEquality.Holds<DateTime>());
        Assert.False(ByteStableEquality.Holds<DateTimeOffset>());
        Assert.False(ByteStableEquality.Holds<decimal?>());
        Assert.False(ByteStableEquality.Holds<Priced>());
        Assert.False(ByteStableEquality.Holds<Tolerant>());
        Assert.False(ByteStableEquality.Holds<(int, decimal)>());
        Assert.False(ByteStableEquality.Holds<KeyedValue<int, Priced>>());
        Assert.False(ByteStableEquality.Holds<WindowKey<decimal>>());
        Assert.False(ByteStableEquality.Holds<int[]>());
        Assert.False(ByteStableEquality.Holds<object>());
    }

    [Fact]
    public async Task Stores_of_types_that_serialize_alike_hold_no_instances()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var weights = provider.CreateWeightStore<Plain>("w");
        var indexed = provider.CreateIndexedWeightStore<int, Plain>("i");
        var values = provider.CreateValueStore<string, decimal>("v"); // the value is not a key
        provider.Recover();
        for (var n = 0; n < 100; n++)
        {
            weights.Update(new Plain(n, "x"), 1);
            indexed.Update(n % 7, new Plain(n, "x"), 1);
            values.Put("k" + n, n);
        }
        await provider.CommitAsync(Offsets(1));
        Assert.Equal(0, provider.HeldInstances);

        var prices = provider.CreateWeightStore<decimal>("d");
        var byPrice = provider.CreateIndexedWeightStore<decimal, int>("di");
        var named = provider.CreateValueStore<decimal, string>("dv");
        foreach (var price in new[] { 1.0m, 2.0m })
        {
            prices.Update(price, 1);
            byPrice.Update(price, 1, 1);
            named.Put(price, "n");
        }
        await provider.CommitAsync(Offsets(2));
        Assert.Equal(6, provider.HeldInstances);
        prices.Update(2.00m, -1);
        byPrice.Update(2.00m, 1, -1);
        named.Remove(2.00m);
        await provider.CommitAsync(Offsets(3));
        Assert.Equal(3, provider.HeldInstances); // nothing is held for an entry that is gone
    }

    // ── one record per entry, whichever equal instance touches it ──────────────────────

    [Fact]
    public async Task Weight_entry_retracted_by_an_equal_value_that_serializes_differently_is_gone_from_the_log()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var store = provider.CreateWeightStore<Priced>("s");
        provider.Recover();

        store.Update(new Priced(1, 10.0m), 1);
        store.Update(new Priced(2, 5.0m), 1);
        await provider.CommitAsync(Offsets(1));

        // Raised by an equal value: still one record, under the first one's bytes.
        store.Update(new Priced(2, 5.00m), 2);
        await provider.CommitAsync(Offsets(2));
        Assert.Equal(["s|{\"Id\":1,\"Amount\":10.0}=1", "s|{\"Id\":2,\"Amount\":5.0}=3"], provider.DescribeState());
        await AssertRestoresAsAsync(log, storage, "after an update by an equal value");

        store.Update(new Priced(1, 10.00m), -1);
        store.Remove(new Priced(2, 5.000m));
        await provider.CommitAsync(Offsets(3));
        Assert.Empty(provider.DescribeState());
        await AssertRestoresAsAsync(log, storage, "after retractions by equal values");
    }

    [Fact]
    public async Task Entry_removed_and_added_again_within_one_commit_keeps_its_one_record()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var weights = provider.CreateWeightStore<decimal>("w");
        var indexed = provider.CreateIndexedWeightStore<decimal, decimal>("i");
        var values = provider.CreateValueStore<decimal, string>("v");
        provider.Recover();
        weights.Update(10.0m, 1);
        indexed.Update(1.0m, 2.50m, 1);
        values.Put(3.0m, "a");
        await provider.CommitAsync(Offsets(1));

        weights.Update(10.00m, -1);
        weights.Update(10.000m, 2);
        indexed.RemoveGroup(1.00m);
        indexed.Update(1.000m, 2.5m, 4);
        values.Remove(3.00m);
        values.Put(3.000m, "b");
        await provider.CommitAsync(Offsets(2));

        Assert.Equal(["i|1.0␀2.50=4", "v|3.0=\"b\"", "w|10.0=2"], provider.DescribeState());
        await AssertRestoresAsAsync(log, storage, "after a remove and re-add in one commit");

        // …and each is still removable by any equal instance afterwards.
        weights.Update(10.0000m, -2);
        indexed.Update(1.0000m, 2.500m, -4);
        values.Remove(3.0000m);
        await provider.CommitAsync(Offsets(3));
        Assert.Empty((await RestoredAsync(log, storage))[1]);
    }

    [Fact]
    public async Task Indexed_and_value_entries_are_removed_under_the_instance_they_were_written_under()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var indexed = provider.CreateIndexedWeightStore<DateTimeOffset, Priced>("i");
        var values = provider.CreateValueStore<DateTimeOffset, long>("v");
        provider.Recover();

        var utc = new DateTimeOffset(2026, 3, 1, 12, 0, 0, TimeSpan.Zero);
        var local = utc.ToOffset(TimeSpan.FromHours(2)); // the same instant, and Equal
        indexed.Update(utc, new Priced(1, 1.50m), 1);
        indexed.Update(utc, new Priced(2, 7m), 1);
        values.Put(utc, 1);
        await provider.CommitAsync(Offsets(1));

        indexed.Update(local, new Priced(1, 1.5m), -1);
        values.Put(local, 2);
        await provider.CommitAsync(Offsets(2));
        await AssertRestoresAsAsync(log, storage, "after one of two is retracted");
        Assert.Equal(2, (await RestoredAsync(log, storage))[1].Count);

        indexed.RemoveGroup(local);
        values.Remove(local);
        await provider.CommitAsync(Offsets(3));
        Assert.Empty((await RestoredAsync(log, storage))[1]);
    }

    [Fact]
    public async Task Rolled_back_writes_leave_the_held_instances_as_they_were()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var weights = provider.CreateWeightStore<decimal>("w");
        var values = provider.CreateValueStore<decimal, string>("v");
        provider.Recover();
        weights.Update(10.0m, 1);
        values.Put(3.0m, "a");
        await provider.CommitAsync(Offsets(1));

        // Removed, and something new added, under other instances — then undone.
        weights.Update(10.00m, -1);
        weights.Update(7.00m, 1);
        values.Remove(3.00m);
        values.Put(4.00m, "x");
        provider.Recover();
        Assert.Equal(2, provider.HeldInstances);

        weights.Update(7.0m, 1);
        values.Put(4.0m, "y");
        await provider.CommitAsync(Offsets(2));
        Assert.Equal(["v|3.0=\"a\"", "v|4.0=\"y\"", "w|10.0=1", "w|7.0=1"], provider.DescribeState());
        await AssertRestoresAsAsync(log, storage, "after a rollback");

        weights.Update(10.000m, -1);
        weights.Update(7.000m, -1);
        values.Remove(3.000m);
        values.Remove(4.000m);
        await provider.CommitAsync(Offsets(3));
        Assert.Empty((await RestoredAsync(log, storage))[1]);
    }

    [Fact]
    public async Task Promoted_follower_and_a_restart_write_under_the_instances_the_log_holds()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        var leader = await ChangelogStorage.OpenAsync(log);
        var written = leader.Pipeline("p");
        var weights = written.CreateWeightStore<decimal>("w");
        var values = written.CreateValueStore<decimal, string>("v");
        var indexed = written.CreateIndexedWeightStore<int, decimal>("i");
        written.Recover();
        weights.Update(10.0m, 1);
        values.Put(3.0m, "a");
        indexed.Update(1, 2.50m, 1);
        await written.CommitAsync(Offsets(1));

        // One standby has its stores while it follows; the other gets them at promotion.
        await using var warm = ChangelogStorage.Follow(log);
        var warmWeights = warm.Pipeline("p").CreateWeightStore<decimal>("w");
        var warmValues = warm.Pipeline("p").CreateValueStore<decimal, string>("v");
        var warmIndexed = warm.Pipeline("p").CreateIndexedWeightStore<int, decimal>("i");
        await warm.WhenCaughtUpAsync().WaitAsync(Timeout);
        await leader.DisposeAsync();
        await warm.PromoteAsync().WaitAsync(Timeout);
        warm.Pipeline("p").Recover();
        warmWeights.Update(10.00m, -1);
        warmValues.Remove(3.00m);
        await warm.Pipeline("p").CommitAsync(Offsets(2));
        await AssertRestoresAsAsync(log, warm, "after the promoted standby's commit");

        await using var cold = ChangelogStorage.Follow(log);
        await cold.WhenCaughtUpAsync().WaitAsync(Timeout);
        await cold.PromoteAsync().WaitAsync(Timeout);
        var coldIndexed = cold.Pipeline("p").CreateIndexedWeightStore<int, decimal>("i");
        cold.Pipeline("p").Recover();
        coldIndexed.Update(1, 2.5m, -1);
        await cold.Pipeline("p").CommitAsync(Offsets(3));
        Assert.Empty((await RestoredAsync(log, cold))[1]);
        _ = warmIndexed;
    }

    [Fact]
    public async Task New_incarnation_sweeps_each_record_under_the_instance_it_was_written_under()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var weights = provider.CreateWeightStore<decimal>("w");
        var indexed = provider.CreateIndexedWeightStore<decimal, decimal>("i");
        var values = provider.CreateValueStore<decimal, string>("v");
        provider.Recover();
        weights.Update(10.0m, 1);
        indexed.Update(1.0m, 2.50m, 1);
        values.Put(3.0m, "a");
        await provider.CommitAsync(Offsets(1));

        // The live stores now hold other instances than the records were written under.
        weights.Update(10.00m, -1);
        weights.Update(10.000m, 2);
        indexed.RemoveGroup(1.00m);
        indexed.Update(1.000m, 2.5m, 4);
        values.Remove(3.00m);
        values.Put(3.000m, "b");
        await provider.CommitAsync(Offsets(2));

        await provider.StartNewIncarnationAsync();
        log.Compact();
        var end = await log.EndOffsetAsync(0);
        var keys = new List<string>();
        using var stop = new CancellationTokenSource(Timeout);
        await foreach (var batch in log.ReadAsync(0, 0, stop.Token))
        {
            keys.AddRange(batch.Records.Select(r => System.Text.Encoding.UTF8.GetString(r.Key.Span).Replace('\0', '|')));
            if (batch.NextOffset >= end)
                break;
        }
        Assert.Equal(["p|2|$commit"], keys);
    }

    // ── through a circuit ──────────────────────────────────────────────────────────────

    private sealed record Order(int Id, string Region, decimal Amount);

    [Fact]
    public async Task Row_retracted_by_an_equal_record_does_not_come_back_after_a_restart()
    {
        static (CircuitBuilder, InputStream<Order>, Output<Order>, Output<KeyedValue<decimal, long>>) Define(IStorageProvider storage)
        {
            var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
            var orders = builder.Input<Order>("orders");
            var distinct = orders.Distinct().ToOutput();
            var byAmount = orders.GroupBy(o => o.Amount).Count().ToOutput(bootstrap: false);
            return (builder, orders, distinct, byAmount);
        }

        static ZSet<T> Drain<T>(OutputHandle<T> output) where T : notnull
        {
            var sum = new ZSet<T>();
            while (output.Reader.TryRead(out var delta))
                sum.AddAll(delta);
            return sum;
        }

        var log = new InMemoryStateChangelog(partitions: 1);
        await using (var storage = await ChangelogStorage.OpenAsync(log))
        {
            var (builder, orders, distinct, byAmount) = Define(storage.Pipeline("p"));
            await using var circuit = builder.Build();
            circuit.Start();
            await circuit.Handle(orders).InsertAsync(new Order(1, "emea", 10.0m));
            await circuit.StepAsync().WaitAsync(Timeout);
            await circuit.Handle(orders).RemoveAsync(new Order(1, "emea", 10.00m));
            await circuit.StepAsync().WaitAsync(Timeout);
            Assert.True(Drain(circuit.Handle(distinct)).IsEmpty);
            Assert.True(Drain(circuit.Handle(byAmount)).IsEmpty);
        }

        await using var reopened = await ChangelogStorage.OpenAsync(log);
        var (again, againOrders, againDistinct, againByAmount) = Define(reopened.Pipeline("p"));
        await using var restarted = again.Build();
        restarted.Start();
        await restarted.Handle(againOrders).InsertAsync(new Order(1, "emea", 10.0m));
        await restarted.StepAsync().WaitAsync(Timeout);

        // Both operators start from nothing: the row is new, and it is the first of its amount.
        Assert.Equal(1, Drain(restarted.Handle(againDistinct)).WeightOf(new Order(1, "emea", 10.0m)));
        Assert.Equal(1, Drain(restarted.Handle(againByAmount)).WeightOf(new KeyedValue<decimal, long>(10.0m, 1)));
    }

    // ── the touched-entry map ──────────────────────────────────────────────────────────

    private static async Task<double> BytesPerEntryAsync(int entries)
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var weights = provider.CreateWeightStore<int>("w");
        var indexed = provider.CreateIndexedWeightStore<int, int>("i");
        var values = provider.CreateValueStore<int, long>("v");
        provider.Recover();
        var offsets = Offsets(1);

        long Commits(int count)
        {
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var commit = 0; commit < count; commit++)
            {
                for (var n = 0; n < entries; n++)
                {
                    weights.Update(n, 1);
                    indexed.Update(n, n, 1);
                    values.Put(n, commit);
                }
                var pending = provider.CommitAsync(offsets);
                Assert.True(pending.IsCompletedSuccessfully);
            }
            return GC.GetAllocatedBytesForCurrentThread() - before;
        }

        Commits(20);
        log.Compact();
        const int measured = 40;
        return (double)Commits(measured) / measured / entries;
    }

    [Fact]
    public async Task Commits_that_touch_many_entries_allocate_no_more_per_entry_than_small_ones()
    {
        // Either side of what used to be the threshold at which every commit threw its
        // touched-entry maps away and grew them again from nothing.
        var small = await BytesPerEntryAsync(1000);
        var large = await BytesPerEntryAsync(4096);
        Assert.True(large <= small * 1.25,
            $"a commit of 4096 entries allocates {large:F0} B/entry against {small:F0} B/entry for one of 1000");
    }

    [Fact]
    public async Task Capacity_left_by_one_large_commit_is_given_back_once_commits_stay_small()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var weights = provider.CreateWeightStore<int>("w");
        var values = provider.CreateValueStore<int, long>("v");
        provider.Recover();

        for (var n = 0; n < 50_000; n++)
        {
            weights.Update(n, 1);
            values.Put(n, n);
        }
        await provider.CommitAsync(Offsets(1));
        Assert.True(provider.TouchedCapacity >= 100_000);

        // A few small commits keep it: a bursty circuit keeps its maps…
        for (var commit = 0; commit < 10; commit++)
        {
            weights.Update(commit, 1);
            values.Put(commit, -1);
            await provider.CommitAsync(Offsets(2));
        }
        Assert.True(provider.TouchedCapacity >= 100_000);

        // …and a long run of them gives it back.
        for (var commit = 0; commit < 100; commit++)
        {
            weights.Update(commit, 1);
            values.Put(commit, -2);
            await provider.CommitAsync(Offsets(3));
        }
        Assert.True(provider.TouchedCapacity < 1000, $"the touched maps still have room for {provider.TouchedCapacity} entries");
    }
}
