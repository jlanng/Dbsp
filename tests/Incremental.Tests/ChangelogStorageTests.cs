using System.Text;
using CsCheck;
using Incremental.Storage;

namespace Incremental.Tests;

/// <summary>
/// The state changelog seam, the in-memory changelog, and <see cref="ChangelogStorage"/> /
/// <see cref="ChangelogStorageProvider"/> on their own, without a circuit: what a commit
/// writes, what a restore and a follower rebuild, fencing, compaction and incarnations.
/// Circuits on top of it are in <see cref="ChangelogFailoverTests"/>.
/// </summary>
public class ChangelogStorageTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private static readonly IReadOnlyDictionary<string, long> NoOffsets = new Dictionary<string, long>();

    private static Dictionary<string, long> Offsets(long value) => new() { ["in"] = value };

    private static async Task<List<ChangelogRecord>> ReadAllAsync(IStateChangelog log, int partition)
    {
        var end = await log.EndOffsetAsync(partition);
        var records = new List<ChangelogRecord>();
        if (end == 0)
            return records;
        using var stop = new CancellationTokenSource(Timeout);
        await foreach (var batch in log.ReadAsync(partition, 0, stop.Token))
        {
            records.AddRange(batch.Records);
            if (batch.NextOffset >= end)
                break;
        }
        return records;
    }

    private static async Task<ChangelogStorage> ColdCopyAsync(IStateChangelog log)
    {
        var copy = ChangelogStorage.Follow(log);
        await copy.WhenCaughtUpAsync().WaitAsync(Timeout);
        return copy;
    }

    // ── the changelog ──────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Transaction_is_readable_only_once_committed_and_then_whole()
    {
        var log = new InMemoryStateChangelog(partitions: 2);
        var writer = await log.AcquireWriterAsync();

        using (var transaction = writer.BeginTransaction())
        {
            transaction.Append(0, "a"u8, "1"u8);
            transaction.Append(0, "b"u8, "2"u8);
            transaction.AppendTombstone(1, "c"u8);
            Assert.Equal(0, await log.EndOffsetAsync(0));
            Assert.Equal(0, log.RecordCount);

            await transaction.CommitAsync();
        }

        var first = await ReadAllAsync(log, 0);
        Assert.Equal(["a", "b"], first.Select(r => Encoding.UTF8.GetString(r.Key.Span)));
        Assert.Equal(["1", "2"], first.Select(r => Encoding.UTF8.GetString(r.Value.Span)));
        var second = Assert.Single(await ReadAllAsync(log, 1));
        Assert.True(second.IsTombstone);
        Assert.Equal(1, log.TransactionsCommitted);
        Assert.Equal(3, log.RecordsAppended);
    }

    [Fact]
    public async Task Transaction_disposed_without_commit_leaves_nothing()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        var writer = await log.AcquireWriterAsync();
        using (var transaction = writer.BeginTransaction())
            transaction.Append(0, "a"u8, "1"u8);

        // The next transaction reuses the aborted one's buffers; nothing of it may leak in.
        using (var transaction = writer.BeginTransaction())
        {
            transaction.Append(0, "b"u8, "2"u8);
            await transaction.CommitAsync();
        }

        var record = Assert.Single(await ReadAllAsync(log, 0));
        Assert.Equal("b", Encoding.UTF8.GetString(record.Key.Span));
    }

    [Fact]
    public async Task Offsets_are_not_dense_and_a_reader_still_reaches_the_end()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        var writer = await log.AcquireWriterAsync();
        for (var i = 0; i < 3; i++)
        {
            using var transaction = writer.BeginTransaction();
            transaction.Append(0, Encoding.UTF8.GetBytes($"k{i}"), "v"u8);
            await transaction.CommitAsync();
        }

        // Three records and three commit markers.
        Assert.Equal(6, await log.EndOffsetAsync(0));
        var records = await ReadAllAsync(log, 0);
        Assert.Equal([0L, 2L, 4L], records.Select(r => r.Offset));
    }

    [Fact]
    public async Task Reader_waits_at_the_end_and_resumes_with_the_next_commit()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        var writer = await log.AcquireWriterAsync();
        using var stop = new CancellationTokenSource(Timeout);
        var reader = log.ReadAsync(0, 0, stop.Token).GetAsyncEnumerator();

        var next = reader.MoveNextAsync().AsTask();
        await Task.Delay(50);
        Assert.False(next.IsCompleted);

        using (var transaction = writer.BeginTransaction())
        {
            transaction.Append(0, "a"u8, "1"u8);
            await transaction.CommitAsync();
        }
        Assert.True(await next.WaitAsync(Timeout));
        Assert.Single(reader.Current.Records);
        Assert.Equal(await log.EndOffsetAsync(0), reader.Current.NextOffset);

        stop.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.MoveNextAsync());
    }

    [Fact]
    public async Task Acquiring_the_writer_fences_the_previous_one()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        var first = await log.AcquireWriterAsync();
        using (var transaction = first.BeginTransaction())
        {
            transaction.Append(0, "a"u8, "1"u8);
            await transaction.CommitAsync();
        }

        var second = await log.AcquireWriterAsync();
        Assert.True(second.Term > first.Term);
        Assert.Equal(second.Term, log.Term);

        using (var transaction = first.BeginTransaction())
        {
            transaction.Append(0, "b"u8, "2"u8);
            await Assert.ThrowsAsync<ChangelogFencedException>(async () => await transaction.CommitAsync());
        }
        Assert.Single(await ReadAllAsync(log, 0));

        using (var transaction = second.BeginTransaction())
        {
            transaction.Append(0, "c"u8, "3"u8);
            await transaction.CommitAsync();
        }
        Assert.Equal(["a", "c"], (await ReadAllAsync(log, 0)).Select(r => Encoding.UTF8.GetString(r.Key.Span)));
    }

    [Fact]
    public async Task Compaction_keeps_the_last_record_per_key_and_drops_tombstoned_keys()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        var writer = await log.AcquireWriterAsync();
        foreach (var (key, value) in new (string, string?)[] { ("a", "1"), ("b", "1"), ("a", "2"), ("b", null), ("c", "1") })
        {
            using var transaction = writer.BeginTransaction();
            if (value is null)
                transaction.AppendTombstone(0, Encoding.UTF8.GetBytes(key));
            else
                transaction.Append(0, Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value));
            await transaction.CommitAsync();
        }

        log.Compact(dropTombstones: false);
        var kept = await ReadAllAsync(log, 0);
        Assert.Equal(["a=2", "b=", "c=1"], kept.Select(r => $"{Encoding.UTF8.GetString(r.Key.Span)}={Encoding.UTF8.GetString(r.Value.Span)}"));
        Assert.Equal([4L, 6L, 8L], kept.Select(r => r.Offset)); // offsets survive compaction

        log.Compact();
        Assert.Equal(["a", "c"], (await ReadAllAsync(log, 0)).Select(r => Encoding.UTF8.GetString(r.Key.Span)));
        Assert.Equal(10, await log.EndOffsetAsync(0));
    }

    // ── a provider against the staged reference ────────────────────────────────────────

    private sealed record Row(int Id, string Tag);

    /// <summary>The three stores of one provider, typed the way operators type theirs.</summary>
    private sealed class Stores(ICheckpointStorageProvider provider)
    {
        public readonly IWeightStore<Row> Weights = provider.CreateWeightStore<Row>("weights.w0");
        public readonly IIndexedWeightStore<int, Row> Indexed = provider.CreateIndexedWeightStore<int, Row>("indexed.w0");
        public readonly IValueStore<int, (long Count, string Label)> Values = provider.CreateValueStore<int, (long, string)>("values.w0");

        /// <summary>Everything readable through the store interfaces over the key space the
        /// generator uses, as text.</summary>
        public string Read()
        {
            var text = new StringBuilder();
            for (var id = 0; id < 5; id++)
            {
                foreach (var tag in new[] { "x", "y" })
                    text.Append($"w{id}{tag}={Weights.WeightOf(new Row(id, tag))};");
                text.Append($"g{id}=[{string.Join(",", Indexed.GetGroup(id).Select(v => $"{v.Value.Id}{v.Value.Tag}:{v.Weight}").Order())}];");
                text.Append(Values.TryGet(id, out var value) ? $"v{id}={value};" : $"v{id}=-;");
            }
            return text.ToString();
        }
    }

    private static void Apply(Stores stores, int kind, int id, int alt, int amount)
    {
        var row = new Row(alt, id % 2 == 0 ? "x" : "y");
        var delta = amount - 2; // -2 … 2, zero included
        switch (kind)
        {
            case 0: stores.Weights.Update(row, delta); break;
            case 1: stores.Weights.ApplyAll([(row, delta), (new Row(id, "x"), 1)]); break;
            case 2: stores.Weights.Remove(row); break;
            case 3: stores.Indexed.Update(id, row, delta); break;
            case 4: stores.Indexed.ApplyAll([(id, row, delta), (id, new Row(alt, "y"), 1), (alt, row, -1)]); break;
            case 5: stores.Indexed.RemoveGroup(id); break;
            case 6: stores.Values.Put(id, (amount, row.Tag)); break;
            case 7: stores.Values.Remove(id); break;
        }
    }

    /// <summary>Kind 8 commits, 9 recovers (a rebuilt circuit after a fault), 10 compacts the
    /// log, 11 starts a new incarnation; the rest are store operations.</summary>
    private static readonly Gen<List<(int Kind, int Id, int Alt, int Amount)>> GenOperations =
        Gen.Select(Gen.Int[0, 11], Gen.Int[0, 4], Gen.Int[0, 4], Gen.Int[0, 4]).List[1, 80];

    private static void AssertProviderMatchesReference(List<(int Kind, int Id, int Alt, int Amount)> operations)
    {
        RunAsync().GetAwaiter().GetResult();
        async Task RunAsync()
        {
            var log = new InMemoryStateChangelog(partitions: 3);
            await using var storage = await ChangelogStorage.OpenAsync(log);
            var provider = storage.Pipeline("p");
            var stores = new Stores(provider);
            provider.Recover();

            var reference = new InMemoryCheckpointStorageProvider();
            var referenceStores = new Stores(reference);
            reference.Recover();

            // One follower has its stores from the start, so records apply to them as they
            // arrive; another has none, so everything it reads waits as bytes.
            await using var typed = ChangelogStorage.Follow(log);
            var typedStores = new Stores(typed.Pipeline("p"));
            await using var untyped = ChangelogStorage.Follow(log);

            var commits = 0L;
            foreach (var (kind, id, alt, amount) in operations)
            {
                switch (kind)
                {
                    case 8:
                        commits++;
                        await provider.CommitAsync(Offsets(commits));
                        reference.Commit(Offsets(commits));

                        // The followers hold the writer's state at every commit.
                        await typed.WhenCaughtUpAsync().WaitAsync(Timeout);
                        await untyped.WhenCaughtUpAsync().WaitAsync(Timeout);
                        Assert.Equal(stores.Read(), typedStores.Read());
                        Assert.Equal(provider.DescribeState(), typed.Pipeline("p").DescribeState());
                        Assert.Equal(provider.DescribeState(), untyped.Pipeline("p").DescribeState());
                        Assert.Equal(provider.CommitSequence, typed.Pipeline("p").CommitSequence);
                        Assert.Equal(provider.Incarnation, untyped.Pipeline("p").Incarnation);
                        break;
                    case 9:
                        Assert.Equal(reference.Recover(), provider.Recover());
                        break;
                    case 10:
                        // Tombstones stay: the followers may be anywhere in the log.
                        log.Compact(dropTombstones: false);
                        break;
                    case 11:
                        await provider.StartNewIncarnationAsync();
                        reference = new InMemoryCheckpointStorageProvider();
                        referenceStores = new Stores(reference);
                        reference.Recover();
                        Assert.Empty(provider.Recover());
                        Assert.Empty(provider.DescribeState());
                        break;
                    default:
                        Apply(stores, kind, id, alt, amount);
                        Apply(referenceStores, kind, id, alt, amount);
                        break;
                }
                Assert.Equal(referenceStores.Read(), stores.Read());
            }

            // What is in the log is the last commit: uncommitted writes are not.
            provider.Recover();
            reference.Recover();
            Assert.Equal(referenceStores.Read(), stores.Read());
            var committed = provider.DescribeState();

            // A provider rebuilt from the log alone equals the one that wrote it — before
            // and after the log is compacted down to its live records.
            foreach (var compact in new[] { false, true })
            {
                if (compact)
                {
                    await typed.WhenCaughtUpAsync().WaitAsync(Timeout);
                    await untyped.WhenCaughtUpAsync().WaitAsync(Timeout);
                    log.Compact();
                }
                await using var restored = await ColdCopyAsync(log);
                Assert.Equal(committed, restored.Pipeline("p").DescribeState());
                Assert.Equal(referenceStores.Read(), new Stores(restored.Pipeline("p")).Read());
                Assert.Equal(provider.CommitSequence, restored.Pipeline("p").CommitSequence);
                Assert.Equal(provider.Incarnation, restored.Pipeline("p").Incarnation);
                await restored.PromoteAsync().WaitAsync(Timeout);
                Assert.Equal(reference.Recover(), restored.Pipeline("p").Recover());
            }
        }
    }

    [Fact]
    public void Provider_reads_like_the_staged_provider_and_the_log_rebuilds_it() =>
        GenOperations.Sample(AssertProviderMatchesReference, iter: 300);

    // ── what a commit writes ───────────────────────────────────────────────────────────

    [Fact]
    public async Task Commit_writes_the_entries_that_changed_and_one_commit_record()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var stores = new Stores(provider);
        provider.Recover();

        for (var id = 0; id < 1000; id++)
        {
            stores.Weights.Update(new Row(id, "x"), 1);
            stores.Indexed.Update(id % 10, new Row(id, "x"), 1);
            stores.Values.Put(id, (id, "x"));
        }
        await provider.CommitAsync(Offsets(1));
        Assert.Equal(3001, log.RecordsAppended);
        Assert.Equal(0, provider.UncommittedEntries);

        // Three entries change, in a state of three thousand.
        stores.Weights.Update(new Row(7, "x"), 1);
        stores.Indexed.Update(7, new Row(7, "x"), -1);
        stores.Values.Put(7, (70, "y"));
        Assert.Equal(3, provider.UncommittedEntries);
        await provider.CommitAsync(Offsets(2));
        Assert.Equal(3001 + 4, log.RecordsAppended);
        Assert.Equal(2, log.TransactionsCommitted);

        // Nothing changed: the commit record alone, carrying the offsets.
        await provider.CommitAsync(Offsets(3));
        Assert.Equal(3001 + 4 + 1, log.RecordsAppended);
    }

    [Fact]
    public async Task Entry_back_at_its_committed_value_writes_no_record()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var stores = new Stores(provider);
        provider.Recover();
        stores.Weights.Update(new Row(1, "x"), 2);
        stores.Indexed.Update(1, new Row(1, "x"), 2);
        await provider.CommitAsync(Offsets(1));
        var before = log.RecordsAppended;

        // Inserted and retracted within one checkpoint interval: net nothing.
        stores.Weights.Update(new Row(1, "x"), 3);
        stores.Weights.Update(new Row(1, "x"), -3);
        stores.Weights.Update(new Row(2, "x"), 1);
        stores.Weights.Update(new Row(2, "x"), -1);
        stores.Indexed.Update(1, new Row(1, "x"), -2);
        stores.Indexed.Update(1, new Row(1, "x"), 2);
        stores.Indexed.Update(9, new Row(9, "x"), 1);
        stores.Indexed.RemoveGroup(9);
        await provider.CommitAsync(Offsets(2));

        Assert.Equal(before + 1, log.RecordsAppended);
    }

    [Fact]
    public async Task Record_layout_is_the_documented_one()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("pipe");
        var stores = new Stores(provider);
        provider.Recover();
        provider.StageManifest("local", "format=6\n");
        stores.Weights.Update(new Row(1, "x"), 2);
        stores.Indexed.Update(7, new Row(1, "y"), 3);
        stores.Values.Put(4, (5, "z"));
        await provider.CommitAsync(new Dictionary<string, long> { ["orders"] = 41, ["$step"] = 9 });

        static string Text(ReadOnlyMemory<byte> bytes) => Encoding.UTF8.GetString(bytes.Span).Replace('\0', '|').Replace('\u0001', '#');
        var records = (await ReadAllAsync(log, 0)).ToDictionary(r => Text(r.Key), r => Text(r.Value));

        Assert.Equal("#2", records["pipe|1|weights.w0|{\"Id\":1,\"Tag\":\"x\"}"]);
        Assert.Equal("#3", records["pipe|1|indexed.w0|7|{\"Id\":1,\"Tag\":\"y\"}"]);
        Assert.Equal("#{\"Item1\":5,\"Item2\":\"z\"}", records["pipe|1|values.w0|4"]);
        Assert.Equal(
            $"#{{\"sequence\":1,\"term\":{storage.Term},\"offsets\":{{\"orders\":41,\"$step\":9}},\"manifests\":{{\"local\":\"format=6\\n\"}}}}",
            records["pipe|1|$commit"]);
        Assert.Equal(4, records.Count);

        // The commit record is the transaction's last record.
        Assert.Equal("pipe|1|$commit", Text((await ReadAllAsync(log, 0))[^1].Key));

        // Removal is a tombstone, one per member of a removed group.
        stores.Weights.Remove(new Row(1, "x"));
        stores.Indexed.Update(7, new Row(2, "y"), 1);
        await provider.CommitAsync(NoOffsets);
        stores.Indexed.RemoveGroup(7);
        stores.Values.Remove(4);
        await provider.CommitAsync(NoOffsets);
        var tombstones = (await ReadAllAsync(log, 0)).Where(r => r.IsTombstone).Select(r => Text(r.Key)).Order().ToList();
        Assert.Equal(
            [
                "pipe|1|indexed.w0|7|{\"Id\":1,\"Tag\":\"y\"}",
                "pipe|1|indexed.w0|7|{\"Id\":2,\"Tag\":\"y\"}",
                "pipe|1|values.w0|4",
                "pipe|1|weights.w0|{\"Id\":1,\"Tag\":\"x\"}",
            ],
            tombstones);
    }

    /// <summary>The same JSON, but not the type the provider has a direct path for: it goes
    /// through strings.</summary>
    private sealed class ViaStrings : IStateSerializer
    {
        public string Serialize<T>(T value) => JsonStateSerializer.Instance.Serialize(value);
        public T Deserialize<T>(string data) => JsonStateSerializer.Instance.Deserialize<T>(data);
    }

    [Fact]
    public async Task Direct_utf8_json_writes_the_same_bytes_as_the_string_serializer()
    {
        async Task<List<string>> WriteAsync(IStateSerializer serializer)
        {
            var log = new InMemoryStateChangelog(partitions: 1);
            await using var storage = await ChangelogStorage.OpenAsync(log, new ChangelogStorageOptions { Serializer = serializer });
            var provider = storage.Pipeline("p");
            var rows = provider.CreateIndexedWeightStore<string, Row>("s");
            var tuples = provider.CreateValueStore<(int, string), KeyedValue<string, double[]>>("t");
            provider.Recover();
            rows.Update("naïve \"quoted\" <tag> \u0000 é中", new Row(1, "tab\there"), 1);
            rows.Update("plain", new Row(-5, ""), -3);
            tuples.Put((1, "a/b"), new KeyedValue<string, double[]>("k", [1.5, -0.0, 1e20]));
            await provider.CommitAsync(Offsets(1));

            // And read back through the same serializer.
            await using var copy = await ColdCopyAsync(log);
            Assert.Equal(provider.DescribeState(), copy.Pipeline("p").DescribeState());
            var restored = copy.Pipeline("p").CreateIndexedWeightStore<string, Row>("s");
            Assert.Equal([new WeightedValue<Row>(new Row(-5, ""), -3)], restored.GetGroup("plain"));

            return (await ReadAllAsync(log, 0))
                .Select(r => Convert.ToHexString(r.Key.Span) + "=" + Convert.ToHexString(r.Value.Span))
                .Order()
                .ToList();
        }

        Assert.Equal(await WriteAsync(new ViaStrings()), await WriteAsync(JsonStateSerializer.Instance));
    }

    [Fact]
    public async Task Record_with_an_unknown_format_byte_is_refused()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        var writer = await log.AcquireWriterAsync();
        using (var transaction = writer.BeginTransaction())
        {
            transaction.Append(0, "p\u00001\u0000s\u00007"u8, [9, (byte)'1']);
            await transaction.CommitAsync();
        }

        await using var follower = ChangelogStorage.Follow(log);
        await follower.WhenCaughtUpAsync().WaitAsync(Timeout);
        var failure = Assert.Throws<InvalidDataException>(() => follower.Pipeline("p").CreateWeightStore<int>("s"));
        Assert.Contains("format byte 9", failure.Message);
    }

    [Fact]
    public async Task Malformed_key_fails_the_follower_and_its_promotion()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        var writer = await log.AcquireWriterAsync();
        using (var transaction = writer.BeginTransaction())
        {
            transaction.Append(0, "no-separators"u8, [1]);
            await transaction.CommitAsync();
        }

        await using var follower = ChangelogStorage.Follow(log);
        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => follower.PromoteAsync()).WaitAsync(Timeout);
        Assert.IsType<InvalidDataException>(failure.InnerException);
        Assert.False(follower.IsLeader);
    }

    // ── stores created after the records arrived ───────────────────────────────────────

    [Fact]
    public async Task Store_created_after_restore_receives_what_was_read_for_it()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using (var storage = await ChangelogStorage.OpenAsync(log))
        {
            var provider = storage.Pipeline("p");
            var stores = new Stores(provider);
            provider.Recover();
            stores.Indexed.Update(1, new Row(1, "x"), 2);
            stores.Indexed.Update(1, new Row(2, "x"), 1);
            stores.Values.Put(3, (4, "v"));
            stores.Weights.Update(new Row(9, "y"), 5);
            await provider.CommitAsync(Offsets(1));
            stores.Indexed.Update(1, new Row(2, "x"), -1);
            await provider.CommitAsync(Offsets(2));
        }

        await using var reopened = await ChangelogStorage.OpenAsync(log);
        var restored = new Stores(reopened.Pipeline("p"));
        Assert.Equal([new WeightedValue<Row>(new Row(1, "x"), 2)], restored.Indexed.GetGroup(1));
        Assert.True(restored.Values.TryGet(3, out var value));
        Assert.Equal((4, "v"), value);
        Assert.Equal(5, restored.Weights.WeightOf(new Row(9, "y")));
        Assert.Equal(Offsets(2), reopened.Pipeline("p").Recover());

        // Asking for the same store again returns it; asking with other types is refused.
        Assert.Same(restored.Weights, reopened.Pipeline("p").CreateWeightStore<Row>("weights.w0"));
        Assert.Throws<InvalidOperationException>(() => reopened.Pipeline("p").CreateWeightStore<int>("weights.w0"));
    }

    [Fact]
    public async Task Store_whose_records_do_not_deserialize_fails_to_create_and_keeps_them()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using (var storage = await ChangelogStorage.OpenAsync(log))
        {
            var provider = storage.Pipeline("p");
            var store = provider.CreateWeightStore<string>("s");
            provider.Recover();
            store.Update("text", 1);
            await provider.CommitAsync(Offsets(1));
        }

        await using var reopened = await ChangelogStorage.OpenAsync(log);
        Assert.ThrowsAny<Exception>(() => reopened.Pipeline("p").CreateWeightStore<int>("s"));
        var right = reopened.Pipeline("p").CreateWeightStore<string>("s");
        Assert.Equal(1, right.WeightOf("text"));
    }

    // ── roles ──────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Follower_refuses_to_recover_or_commit_until_promoted()
    {
        var log = new InMemoryStateChangelog(partitions: 2);
        await using var leader = await ChangelogStorage.OpenAsync(log);
        var written = leader.Pipeline("p");
        var store = written.CreateWeightStore<int>("s");
        written.Recover();
        store.Update(1, 1);
        written.StageManifest("local", "m1");
        await written.CommitAsync(Offsets(5));

        await using var follower = ChangelogStorage.Follow(log);
        Assert.False(follower.IsLeader);
        Assert.Equal(0, follower.Term);
        var provider = follower.Pipeline("p");
        var followed = provider.CreateWeightStore<int>("s");
        await follower.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.Equal(1, followed.WeightOf(1));
        Assert.Equal("m1", provider.CommittedManifest("local"));

        Assert.Contains("follower", Assert.Throws<InvalidOperationException>(() => provider.Recover()).Message);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await provider.CommitAsync(Offsets(6)));
        await Assert.ThrowsAsync<InvalidOperationException>(() => provider.StartNewIncarnationAsync());

        Assert.True(follower.IsReading);
        await follower.PromoteAsync().WaitAsync(Timeout);
        Assert.True(follower.IsLeader);
        Assert.False(follower.IsReading); // the stores now belong to the circuit alone
        Assert.Equal(leader.Term + 1, follower.Term);
        Assert.Equal(Offsets(5), provider.Recover());
        followed.Update(2, 1);
        await provider.CommitAsync(Offsets(6));
        Assert.Equal(2, provider.CommitSequence);
        Assert.Equal(follower.Term, provider.CommitTerm);
        await Assert.ThrowsAsync<InvalidOperationException>(() => follower.PromoteAsync()).WaitAsync(Timeout);
    }

    [Fact]
    public async Task Promotion_racing_the_old_leaders_last_commit_takes_it_whole_or_not_at_all()
    {
        // The old leader commits at the moment the standby takes over. Either the commit
        // landed before the fence and the new leader has it, or it was refused and the new
        // leader does not: never half, and the promotion never waits on it.
        await Task.WhenAll(Enumerable.Range(0, 4).Select(thread => Task.Run(async () =>
        {
            var random = new Random(thread);
            for (var round = 0; round < 300; round++)
            {
                var log = new InMemoryStateChangelog(partitions: 3);
                await using var leader = await ChangelogStorage.OpenAsync(log).WaitAsync(Timeout);
                var provider = leader.Pipeline("p");
                var store = provider.CreateIndexedWeightStore<int, int>("s");
                provider.Recover();
                var commits = random.Next(0, 6);
                for (var i = 1; i <= commits; i++)
                {
                    store.Update(i, round, 1);
                    await provider.CommitAsync(Offsets(i));
                }

                // Sometimes the standby has built its circuit's stores, sometimes not;
                // sometimes it has caught up before the takeover, sometimes not.
                await using var standby = ChangelogStorage.Follow(log);
                if (random.Next(2) == 0)
                    standby.Pipeline("p").CreateIndexedWeightStore<int, int>("s");

                var landed = false;
                var last = Task.Run(async () =>
                {
                    store.Update(99, round, 1);
                    store.Update(98, round, 1);
                    try
                    {
                        await provider.CommitAsync(Offsets(99));
                        landed = true;
                    }
                    catch (ChangelogFencedException)
                    {
                    }
                });
                await standby.PromoteAsync().WaitAsync(Timeout);
                await last.WaitAsync(Timeout);

                var promoted = standby.Pipeline("p");
                Assert.Equal(commits + (landed ? 1 : 0), promoted.CommitSequence);
                Assert.Equal(landed ? Offsets(99) : commits == 0 ? NoOffsets : Offsets(commits), promoted.Recover());
                Assert.Equal(commits + (landed ? 2 : 0), promoted.DescribeState().Count);
            }
        }))).WaitAsync(TimeSpan.FromSeconds(60));
    }

    [Fact]
    public async Task Promotion_applies_everything_the_old_leader_committed_before_it_was_fenced()
    {
        var log = new InMemoryStateChangelog(partitions: 4);
        await using var leader = await ChangelogStorage.OpenAsync(log);
        var written = leader.Pipeline("p");
        var store = written.CreateIndexedWeightStore<int, int>("s");
        written.Recover();

        await using var follower = ChangelogStorage.Follow(log);
        var followed = follower.Pipeline("p").CreateIndexedWeightStore<int, int>("s");

        // Promote straight after a burst of commits: nothing waits for the follower first.
        for (var i = 1; i <= 200; i++)
        {
            store.Update(i % 7, i, 1);
            await written.CommitAsync(Offsets(i));
        }
        await follower.PromoteAsync().WaitAsync(Timeout);

        Assert.Equal(Offsets(200), follower.Pipeline("p").Recover());
        Assert.Equal(written.DescribeState(), follower.Pipeline("p").DescribeState());
        Assert.Equal(200, followed.GetGroups([0, 1, 2, 3, 4, 5, 6]).Sum(g => g.Count));
    }

    [Fact]
    public async Task Fenced_leader_cannot_commit_and_says_so_from_then_on()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var old = await ChangelogStorage.OpenAsync(log);
        var first = old.Pipeline("a");
        var second = old.Pipeline("b");
        var store = first.CreateWeightStore<int>("s");
        first.Recover();
        second.Recover();
        store.Update(1, 1);
        await first.CommitAsync(Offsets(1));

        await using var successor = ChangelogStorage.Follow(log);
        await successor.PromoteAsync().WaitAsync(Timeout);
        Assert.False(old.WhenFenced.IsCompleted);

        store.Update(2, 1);
        await Assert.ThrowsAsync<ChangelogFencedException>(async () => await first.CommitAsync(Offsets(2)));
        Assert.True(old.IsFenced);
        Assert.False(old.IsLeader);
        await old.WhenFenced.WaitAsync(Timeout);

        // Every other pipeline on the storage is finished too, without asking the log.
        var appended = log.RecordsAppended;
        await Assert.ThrowsAsync<ChangelogFencedException>(async () => await second.CommitAsync(Offsets(1)));
        Assert.Throws<ChangelogFencedException>(() => first.Recover());
        Assert.Equal(appended, log.RecordsAppended);

        // The successor never saw the fenced write.
        Assert.Equal(Offsets(1), successor.Pipeline("a").Recover());
        Assert.Equal(0, successor.Pipeline("a").CreateWeightStore<int>("s").WeightOf(2));
    }

    [Fact]
    public async Task Commit_in_flight_when_the_writer_is_fenced_does_not_land()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var old = await ChangelogStorage.OpenAsync(log);
        var provider = old.Pipeline("p");
        var store = provider.CreateWeightStore<int>("s");
        provider.Recover();
        store.Update(1, 1);
        await provider.CommitAsync(Offsets(1));

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        log.OnCommitting = async () =>
        {
            entered.TrySetResult();
            await release.Task;
        };
        store.Update(2, 1);
        var commit = provider.CommitAsync(Offsets(2)).AsTask();
        await entered.Task.WaitAsync(Timeout);
        Assert.False(commit.IsCompleted); // awaited, not blocked on
        log.OnCommitting = null;

        await using var successor = ChangelogStorage.Follow(log);
        await successor.PromoteAsync().WaitAsync(Timeout);
        release.SetResult();

        await Assert.ThrowsAsync<ChangelogFencedException>(() => commit).WaitAsync(Timeout);
        Assert.True(old.IsFenced);
        await using var copy = await ColdCopyAsync(log);
        Assert.Equal(successor.Pipeline("p").DescribeState(), copy.Pipeline("p").DescribeState());
        Assert.Equal(1, copy.Pipeline("p").CommitSequence);
    }

    [Fact]
    public async Task Failed_commit_leaves_the_previous_checkpoint_and_recover_undoes_the_writes()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var stores = new Stores(provider);
        provider.Recover();
        stores.Weights.Update(new Row(1, "x"), 1);
        stores.Indexed.Update(1, new Row(1, "x"), 1);
        stores.Indexed.Update(2, new Row(2, "x"), 1);
        stores.Values.Put(1, (1, "a"));
        provider.StageManifest("local", "m1");
        await provider.CommitAsync(Offsets(1));
        var committed = stores.Read();
        var appended = log.RecordsAppended;

        stores.Weights.Update(new Row(1, "x"), 4);
        stores.Weights.Remove(new Row(1, "x"));
        stores.Weights.Update(new Row(2, "y"), 1);
        stores.Indexed.RemoveGroup(1);
        stores.Indexed.Update(2, new Row(3, "x"), 1);
        stores.Values.Put(1, (2, "b"));
        stores.Values.Put(2, (2, "b"));
        stores.Values.Remove(1);
        provider.StageManifest("local", "m2");
        log.OnCommitting = () => throw new IOException("log unavailable");
        await Assert.ThrowsAsync<IOException>(async () => await provider.CommitAsync(Offsets(2)));
        log.OnCommitting = null;

        Assert.False(storage.IsFenced);
        Assert.Equal(appended, log.RecordsAppended);
        Assert.Equal(Offsets(1), provider.Recover());
        Assert.Equal(committed, stores.Read());
        Assert.Equal("m1", provider.CommittedManifest("local"));
        Assert.Equal(0, provider.UncommittedEntries);

        // The provider carries on, and the staged manifest of the failed commit is gone.
        stores.Weights.Update(new Row(5, "x"), 1);
        await provider.CommitAsync(Offsets(3));
        Assert.Equal(2, provider.CommitSequence);
        await using var copy = await ColdCopyAsync(log);
        Assert.Equal(provider.DescribeState(), copy.Pipeline("p").DescribeState());
        Assert.Equal("m1", copy.Pipeline("p").CommittedManifest("local"));
    }

    [Fact]
    public async Task Synchronous_commit_is_the_same_commit()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var store = provider.CreateWeightStore<int>("s");
        provider.Recover();
        store.Update(1, 3);
        provider.Commit(Offsets(1));

        await using var copy = await ColdCopyAsync(log);
        Assert.Equal(provider.DescribeState(), copy.Pipeline("p").DescribeState());
        Assert.Equal(1, copy.Pipeline("p").CommitSequence);
    }

    // ── many pipelines, one changelog ──────────────────────────────────────────────────

    [Fact]
    public async Task Pipelines_share_the_changelog_and_keep_their_own_state()
    {
        var log = new InMemoryStateChangelog(partitions: 3);
        var ids = Enumerable.Range(0, 20).Select(i => $"pipeline-{i}").ToArray();
        await using (var storage = await ChangelogStorage.OpenAsync(log))
        {
            Assert.Same(storage.Pipeline(ids[0]), storage.Pipeline(ids[0]));

            // Committing concurrently, as the circuits of one process do.
            await Task.WhenAll(ids.Select((id, n) => Task.Run(async () =>
            {
                var provider = storage.Pipeline(id);
                var store = provider.CreateValueStore<string, int>("same-name");
                provider.Recover();
                for (var round = 1; round <= 10; round++)
                {
                    store.Put("k", n * 100 + round);
                    await provider.CommitAsync(Offsets(round));
                }
            })));
        }

        // Every partition is in use, and a pipeline's records are all in its own.
        Assert.Equal(3, ids.Select(id => ChangelogStorage.PartitionOf(id, 3)).Distinct().Count());
        for (var partition = 0; partition < 3; partition++)
            foreach (var record in await ReadAllAsync(log, partition))
            {
                var key = Encoding.UTF8.GetString(record.Key.Span);
                Assert.Equal(partition, ChangelogStorage.PartitionOf(key[..key.IndexOf('\0')], 3));
            }

        await using var reopened = await ChangelogStorage.OpenAsync(log);
        for (var n = 0; n < ids.Length; n++)
        {
            var provider = reopened.Pipeline(ids[n]);
            Assert.Equal(ChangelogStorage.PartitionOf(ids[n], 3), provider.Partition);
            Assert.True(provider.CreateValueStore<string, int>("same-name").TryGet("k", out var value));
            Assert.Equal(n * 100 + 10, value);
            Assert.Equal(Offsets(10), provider.Recover());
            Assert.Equal(10, provider.CommitSequence);
        }
    }

    [Fact]
    public void Pipeline_partition_is_the_stable_hash_of_its_id()
    {
        // Process-stable: a pipeline must land on the partition another process wrote it to.
        Assert.Equal((int)(StableHash.Of("orders-by-region") % 12), ChangelogStorage.PartitionOf("orders-by-region", 12));
    }

    [Fact]
    public async Task Pipeline_ids_and_store_names_cannot_hold_the_separator()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        Assert.Throws<ArgumentException>(() => storage.Pipeline("a\0b"));
        Assert.Throws<ArgumentException>(() => storage.Pipeline(""));
        Assert.Throws<ArgumentException>(() => storage.Pipeline("p").CreateWeightStore<int>("s\0t"));
    }

    // ── incarnations ───────────────────────────────────────────────────────────────────

    [Fact]
    public async Task New_incarnation_starts_empty_durably_and_followers_reset()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var other = storage.Pipeline("q");
        var stores = new Stores(provider);
        var otherStore = other.CreateWeightStore<int>("s");
        provider.Recover();
        other.Recover();

        await provider.StartNewIncarnationAsync(); // never committed: nothing to discard
        Assert.Equal(0, provider.Incarnation);
        Assert.Equal(0, log.RecordsAppended);

        stores.Weights.Update(new Row(1, "x"), 1);
        stores.Indexed.Update(1, new Row(1, "x"), 1);
        stores.Values.Put(1, (1, "a"));
        provider.StageManifest("local", "m1");
        await provider.CommitAsync(Offsets(7));
        otherStore.Update(1, 1);
        await other.CommitAsync(Offsets(3));
        Assert.Equal(1, provider.Incarnation);

        await using var follower = ChangelogStorage.Follow(log);
        var followed = new Stores(follower.Pipeline("p"));
        await follower.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.Equal(stores.Read(), followed.Read());

        // Uncommitted writes are discarded with the rest — and an uncommitted removal must
        // not hide the committed record from the sweep.
        stores.Weights.Update(new Row(2, "x"), 1);
        stores.Weights.Remove(new Row(1, "x"));
        stores.Values.Remove(1);
        stores.Indexed.RemoveGroup(1);
        await provider.StartNewIncarnationAsync();

        Assert.Equal(2, provider.Incarnation);
        Assert.Equal(0, provider.CommitSequence);
        Assert.Empty(provider.Recover());
        Assert.Null(provider.CommittedManifest("local"));
        Assert.Empty(provider.DescribeState());
        Assert.Equal(0, stores.Weights.WeightOf(new Row(1, "x")));

        await follower.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.Equal(2, follower.Pipeline("p").Incarnation);
        Assert.Empty(follower.Pipeline("p").DescribeState());
        Assert.Null(follower.Pipeline("p").CommittedManifest("local"));
        Assert.Equal(stores.Read(), followed.Read());

        // The neighbour is untouched.
        Assert.Equal(Offsets(3), other.Recover());
        Assert.Equal(1, otherStore.WeightOf(1));

        // The old incarnation's records are tombstoned: once compacted, the log holds the
        // new incarnation's commit record and the neighbour.
        log.Compact();
        var keys = (await ReadAllAsync(log, 0)).Select(r => Encoding.UTF8.GetString(r.Key.Span).Replace('\0', '|')).Order().ToList();
        Assert.Equal(["p|2|$commit", "q|1|$commit", "q|1|s|1"], keys);

        // The new incarnation runs on, and a cold reader sees only it.
        stores.Weights.Update(new Row(3, "y"), 1);
        await provider.CommitAsync(Offsets(1));
        await using var copy = await ColdCopyAsync(log);
        Assert.Equal(provider.DescribeState(), copy.Pipeline("p").DescribeState());
        Assert.Equal(2, copy.Pipeline("p").Incarnation);
        Assert.Equal(1, copy.Pipeline("p").CommitSequence);
    }

    [Fact]
    public async Task New_incarnation_discards_state_no_circuit_has_claimed_yet()
    {
        // The leader restored the pipeline but built nothing on it: its records are still
        // held as bytes. A new incarnation must drop those too, here and in the log.
        var log = new InMemoryStateChangelog(partitions: 1);
        await using (var storage = await ChangelogStorage.OpenAsync(log))
        {
            var provider = storage.Pipeline("p");
            var stores = new Stores(provider);
            provider.Recover();
            stores.Weights.Update(new Row(1, "x"), 1);
            stores.Indexed.Update(1, new Row(1, "x"), 1);
            stores.Values.Put(1, (1, "a"));
            await provider.CommitAsync(Offsets(1));
        }

        await using var reopened = await ChangelogStorage.OpenAsync(log);
        var restored = reopened.Pipeline("p");
        Assert.Equal(3, restored.DescribeState().Count);
        await restored.StartNewIncarnationAsync();

        Assert.Empty(restored.DescribeState());
        Assert.Empty(restored.Recover());
        var fresh = new Stores(restored);
        Assert.Equal(0, fresh.Weights.WeightOf(new Row(1, "x")));
        Assert.Empty(fresh.Indexed.GetGroup(1));
        Assert.False(fresh.Values.TryGet(1, out _));

        log.Compact();
        var keys = (await ReadAllAsync(log, 0)).Select(r => Encoding.UTF8.GetString(r.Key.Span).Replace('\0', '|'));
        Assert.Equal(["p|2|$commit"], keys);
    }

    [Fact]
    public async Task Reader_ignores_an_older_incarnation_that_compaction_has_not_removed()
    {
        // An uncompacted log still holds incarnation 1 in full, ahead of incarnation 2.
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var store = provider.CreateWeightStore<int>("s");
        provider.Recover();
        store.Update(1, 1);
        store.Update(2, 1);
        await provider.CommitAsync(Offsets(1));
        await provider.StartNewIncarnationAsync();
        store.Update(3, 1);
        await provider.CommitAsync(Offsets(1));

        await using var copy = await ColdCopyAsync(log);
        var restored = copy.Pipeline("p").CreateWeightStore<int>("s");
        Assert.Equal((0, 0, 1), (restored.WeightOf(1), restored.WeightOf(2), restored.WeightOf(3)));

        // A stale record of the old incarnation arriving late changes nothing either.
        var writer = await log.AcquireWriterAsync();
        using (var transaction = writer.BeginTransaction())
        {
            transaction.Append(0, "p\u00001\u0000s\u00001"u8, [1, (byte)'9']);
            await transaction.CommitAsync();
        }
        await copy.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.Equal(0, restored.WeightOf(1));
    }
}
