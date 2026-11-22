using System.Diagnostics;
using Confluent.Kafka;
using Dbsp.Storage;
using Dbsp.Tables;

namespace Dbsp.Kafka.Tests;

/// <summary>
/// <see cref="KafkaTableLog{TKey,TRow}"/> against a real broker, alone and under a
/// <see cref="SharedTable{TKey,TRow}"/>: versions, the snapshot, range reads, follow mode,
/// retention, chunking, compaction and a large restore.
/// </summary>
public class KafkaTableLogTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Timeout = KafkaEnv.Timeout;

    private sealed record Row(int Id, string Name);

    /// <summary>A wide reference row, the shape the enrichment service joins against.</summary>
    internal sealed record Instrument(
        int Id, string Symbol, string Isin, string Name, string Currency, string Exchange, string Sector, string Country,
        decimal TickSize, decimal LotSize, decimal Multiplier, DateOnly Listed, DateOnly? Expiry, bool Active,
        string Issuer, string Category, int RiskBucket, long SharesOutstanding, string Notes)
    {
        public static Instrument Make(int id, int revision = 0) => new(
            id, $"SYM{id:D6}", $"US{id:D10}", $"Instrument number {id} common stock", (id % 3) switch { 0 => "USD", 1 => "EUR", _ => "GBP" },
            (id % 4) switch { 0 => "XNYS", 1 => "XNAS", 2 => "XLON", _ => "XETR" }, $"sector-{id % 11}", $"C{id % 40:D2}",
            0.01m, 100m, 1m, new DateOnly(1990 + id % 30, 1 + id % 12, 1 + id % 28), id % 7 == 0 ? new DateOnly(2030, 12, 31) : null,
            id % 13 != 0, $"Issuer {id % 500} Holdings plc", $"category-{id % 17}", id % 9, 1_000_000L * (1 + id % 97) + revision,
            $"revision {revision}");
    }

    private static TableVersion<int, Row> Version(long version, IReadOnlyDictionary<int, long>? offsets, params TableChange<int, Row>[] changes) =>
        new(version, changes, offsets ?? new Dictionary<int, long>());

    private static TableChange<int, Row> Put(int id, string name, string? old = null) =>
        new(id, old is null ? null : new Row(id, old), new Row(id, name));

    private static TableChange<int, Row> Remove(int id, string old) => new(id, new Row(id, old), null);

    private static async Task<(Dictionary<TKey, TRow> Rows, TableHead Head)> RestoreAsync<TKey, TRow>(ITableLog<TKey, TRow> log)
        where TKey : notnull
        where TRow : class
    {
        var rows = new Dictionary<TKey, TRow>();
        var head = await log.RestoreAsync((key, row) => rows.Add(key, row)).AsTask().WaitAsync(Timeout);
        return (rows, head);
    }

    private static async Task<List<TableVersion<TKey, TRow>>> ReadAsync<TKey, TRow>(ITableLog<TKey, TRow> log, long from, long to)
        where TKey : notnull
        where TRow : class
    {
        var versions = new List<TableVersion<TKey, TRow>>();
        using var stop = new CancellationTokenSource(Timeout);
        await foreach (var version in log.ReadAsync(from, to, stop.Token))
            versions.Add(version);
        return versions;
    }

    private static async Task<long> CountRecordsAsync(KafkaLog log, string topic, int partitions)
    {
        var count = 0L;
        for (var p = 0; p < partitions; p++)
        {
            var partition = new TopicPartition(topic, p);
            var end = await log.EndOffsetAsync(partition, default);
            if (end == 0)
                continue;
            using var stop = new CancellationTokenSource(Timeout);
            await foreach (var batch in log.ReadBatchesAsync(partition, Offset.Beginning, stop.Token))
            {
                count += batch.Records.Count;
                if (batch.NextOffset >= end)
                    break;
            }
        }
        return count;
    }

    [Fact]
    public async Task Versions_are_restored_read_and_followed_by_another_process()
    {
        await using var env = KafkaEnv.Create();
        var active = await env.ConnectWriterAsync();
        var log = await active.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        Assert.Equal(env.Prefix + ".table.rows.changes", log.ChangesTopic);
        Assert.Equal(env.Prefix + ".table.rows.rows", log.RowsTopic);
        Assert.Equal(2, log.RowPartitions);
        Assert.Equal(32, log.Epoch.Length);
        Assert.Equal(0, await log.LastVersionAsync());
        Assert.Equal(1, log.OldestRetainedVersion);
        Assert.Same(log, await active.OpenTableLogAsync<int, Row>("rows"));

        // The standby: a reader on the same topics. It finds the same epoch.
        var standby = await env.ConnectAsync();
        var followed = await standby.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        Assert.Equal(log.Epoch, followed.Epoch);
        var (emptyRows, emptyHead) = await RestoreAsync(followed);
        Assert.Empty(emptyRows);
        Assert.Equal(0, emptyHead.Version);

        using var stop = new CancellationTokenSource(Timeout);
        var seen = System.Threading.Channels.Channel.CreateUnbounded<TableVersion<int, Row>>();
        var following = Task.Run(async () =>
        {
            await foreach (var version in followed.FollowAsync(0, stop.Token))
                seen.Writer.TryWrite(version);
        });

        await log.AppendAsync(Version(1, new Dictionary<int, long> { [0] = 10 }, Put(1, "one"), Put(2, "two"), Put(3, "three")));
        await log.AppendAsync(Version(2, new Dictionary<int, long> { [0] = 12, [1] = 4 }, Put(2, "TWO", old: "two"), Remove(3, "three")));
        await log.AppendAsync(Version(3, new Dictionary<int, long> { [0] = 15, [1] = 4 }, Put(4, "four")));
        Assert.Equal(3, await log.LastVersionAsync());

        // Follow mode hands over each version, whole, in order, with Old and New.
        for (var v = 1; v <= 3; v++)
        {
            var version = await seen.Reader.ReadAsync(stop.Token);
            Assert.Equal(v, version.Version);
        }
        var second = (await ReadAsync(log, 1, 2)).Single();
        Assert.Equal([Put(2, "TWO", old: "two"), Remove(3, "three")], second.Changes);
        Assert.Equal(new Dictionary<int, long> { [0] = 12, [1] = 4 }, second.Offsets);
        Assert.Equal([1L, 2L, 3L], (await ReadAsync(log, 0, 3)).Select(v => v.Version));
        Assert.Empty(await ReadAsync(log, 3, 3));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => ReadAsync(log, 0, 4));

        // A third process restores from the rows topic alone.
        var successor = await env.ConnectWriterAsync();
        var reopened = await successor.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        Assert.Equal(log.Epoch, reopened.Epoch);
        var (rows, head) = await RestoreAsync(reopened);
        Assert.Equal(new Dictionary<int, Row> { [1] = new(1, "one"), [2] = new(2, "TWO"), [4] = new(4, "four") }, rows);
        Assert.Equal(3, head.Version);
        Assert.Equal(new Dictionary<int, long> { [0] = 15, [1] = 4 }, head.Offsets);
        Assert.Equal(3, await reopened.LastVersionAsync());
        Assert.Equal([2L, 3L], (await ReadAsync(reopened, 1, 3)).Select(v => v.Version));

        // It carries on at the next version; anything else is refused. The follower, still
        // reading, gets it.
        await Assert.ThrowsAsync<InvalidOperationException>(() => reopened.AppendAsync(Version(5, null, Put(9, "nine"))).AsTask());
        await reopened.AppendAsync(Version(4, null, Put(9, "nine")));
        Assert.Equal(4, (await seen.Reader.ReadAsync(stop.Token)).Version);
        Assert.Equal(4, await followed.LastVersionAsync());

        stop.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => following).WaitAsync(Timeout);
    }

    [Fact]
    public async Task A_reader_waits_for_the_writer_to_create_the_table()
    {
        await using var env = KafkaEnv.Create();
        var standby = await env.ConnectAsync();
        var opening = standby.OpenTableLogAsync<int, Row>("late");
        await Task.Delay(1500);
        Assert.False(opening.IsCompleted); // nothing has given the table an identity yet

        var active = await env.ConnectWriterAsync();
        var created = await active.OpenTableLogAsync<int, Row>("late").WaitAsync(Timeout);
        var followed = await opening.WaitAsync(Timeout);
        Assert.Equal(created.Epoch, followed.Epoch);
    }

    [Fact]
    public async Task A_row_too_large_for_one_record_is_refused_by_name_before_anything_is_written()
    {
        await using var env = KafkaEnv.Create();
        var active = await env.ConnectWriterAsync(env.Options(maxRecordBytes: 4096));
        var log = await active.OpenTableLogAsync<int, Instrument>("instruments").WaitAsync(Timeout);

        var huge = Instrument.Make(7) with { Notes = new string('x', 5000) };
        var changes = Enumerable.Range(0, 20)
            .Select(i => new TableChange<int, Instrument>(i, null, i == 7 ? huge : Instrument.Make(i))).ToList();
        var refused = await Assert.ThrowsAsync<KafkaLogException>(
            async () => await log.AppendAsync(new TableVersion<int, Instrument>(1, changes, new Dictionary<int, long>())));
        Assert.IsNotType<KafkaLogFencedException>(refused);
        Assert.Contains("key 7 ", refused.Message);
        Assert.Contains("MaxRecordBytes (4096)", refused.Message);

        // Nothing of the version was produced: the log is where it was, and the same
        // version number is still the next one.
        Assert.Equal(0, await log.LastVersionAsync());
        Assert.True(active.IsWriter);
        var small = changes.Where(c => c.Key != 7).ToList();
        await log.AppendAsync(new TableVersion<int, Instrument>(1, small, new Dictionary<int, long>()));

        // A transaction's markers are written just after its commit returns, and on three
        // brokers that is long enough for a read made at once to stop short of the
        // version (this test failed that way there twice, in a handful of full runs).
        await KafkaEnv.EventuallyAsync(async () => (await RestoreAsync(log)).Item2.Version == 1, "version 1 to be readable");
        var (rows, head) = await RestoreAsync(log);
        Assert.Equal(1, head.Version);
        Assert.Equal(19, rows.Count);

        // A change is its old row and its new one: two rows that each fit can still be too much.
        var wide = Instrument.Make(3) with { Notes = new string('y', 2500) };
        await log.AppendAsync(new TableVersion<int, Instrument>(2, [new TableChange<int, Instrument>(3, Instrument.Make(3), wide)], new Dictionary<int, long>()));
        var both = await Assert.ThrowsAsync<KafkaLogException>(async () => await log.AppendAsync(new TableVersion<int, Instrument>(
            3, [new TableChange<int, Instrument>(3, wide, wide with { Notes = new string('z', 2500) })], new Dictionary<int, long>())));
        Assert.Contains("key 3 ", both.Message);
    }

    [Fact]
    public async Task A_large_version_is_split_into_chunks_and_read_back_whole()
    {
        await using var env = KafkaEnv.Create();
        var active = await env.ConnectWriterAsync(env.Options(maxRecordBytes: 4096));
        var log = await active.OpenTableLogAsync<int, Instrument>("instruments").WaitAsync(Timeout);

        var first = Enumerable.Range(0, 400).Select(i => new TableChange<int, Instrument>(i, null, Instrument.Make(i))).ToList();
        await log.AppendAsync(new TableVersion<int, Instrument>(1, first, new Dictionary<int, long> { [0] = 399 }));
        var second = Enumerable.Range(0, 400)
            .Select(i => i % 4 == 0
                ? new TableChange<int, Instrument>(i, Instrument.Make(i), null)
                : new TableChange<int, Instrument>(i, Instrument.Make(i), Instrument.Make(i, revision: 1)))
            .ToList();
        await log.AppendAsync(new TableVersion<int, Instrument>(2, second, new Dictionary<int, long> { [0] = 799 }));

        // Two versions, many records: each chunk stayed near the limit.
        var successor = await env.ConnectWriterAsync(env.Options(maxRecordBytes: 4096));
        var chunks = await CountRecordsAsync(successor, log.ChangesTopic, 1);
        Assert.True(chunks > 40, $"400 wide rows at 4 KiB a chunk should take many records; took {chunks}.");

        var reopened = await successor.OpenTableLogAsync<int, Instrument>("instruments").WaitAsync(Timeout);
        var versions = await ReadAsync(reopened, 0, 2);
        Assert.Equal(first, versions[0].Changes);
        Assert.Equal(second, versions[1].Changes);
        Assert.Equal(new Dictionary<int, long> { [0] = 799 }, versions[1].Offsets);
        Assert.Equal([second], (await ReadAsync(reopened, 1, 2)).Select(v => v.Changes)); // seeks to a version's first chunk

        var (rows, head) = await RestoreAsync(reopened);
        Assert.Equal(2, head.Version);
        Assert.Equal(
            Enumerable.Range(0, 400).Where(i => i % 4 != 0).ToDictionary(i => i, i => Instrument.Make(i, revision: 1)), rows);
    }

    [Fact]
    public async Task Trimming_deletes_history_and_leaves_the_rows()
    {
        await using var env = KafkaEnv.Create();
        var active = await env.ConnectWriterAsync();
        var log = await active.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        for (var v = 1; v <= 6; v++)
            await log.AppendAsync(Version(v, null, Put(v, $"row{v}"), Put(0, $"v{v}", old: v == 1 ? null : $"v{v - 1}")));
        Assert.Equal(1, log.OldestRetainedVersion);

        await log.TrimBeforeAsync(4).AsTask().WaitAsync(Timeout);
        Assert.Equal(4, log.OldestRetainedVersion);
        Assert.Equal([4L, 5L, 6L], (await ReadAsync(log, 3, 6)).Select(v => v.Version));
        var lost = await Assert.ThrowsAsync<InvalidOperationException>(() => ReadAsync(log, 2, 6));
        Assert.Contains("no longer retains version 3", lost.Message);
        await log.TrimBeforeAsync(2).AsTask().WaitAsync(Timeout); // floors only rise
        Assert.Equal(4, log.OldestRetainedVersion);

        // Kafka really dropped them: a process that has never seen the table finds the
        // history starting at 4, and a follower asking for older fails rather than skip.
        var successor = await env.ConnectWriterAsync();
        var reopened = await successor.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        Assert.Equal(4, reopened.OldestRetainedVersion);
        Assert.Equal(3, await CountRecordsAsync(successor, log.ChangesTopic, 1));
        using var stop = new CancellationTokenSource(Timeout);
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in reopened.FollowAsync(1, stop.Token))
            {
            }
        });

        // The snapshot is whole.
        var (rows, head) = await RestoreAsync(reopened);
        Assert.Equal(6, head.Version);
        Assert.Equal(7, rows.Count);
        Assert.Equal(new Row(0, "v6"), rows[0]);

        // Past the end: everything goes.
        await reopened.TrimBeforeAsync(100).AsTask().WaitAsync(Timeout);
        Assert.Equal(7, reopened.OldestRetainedVersion);

        // A process that starts now is told by the head where recent history began — at an
        // offset the trim has just deleted. Its reads must start from what is left, not
        // fail on a position that no longer exists.
        var late = await env.ConnectAsync();
        var afterTrim = await late.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        Assert.Equal(7, afterTrim.OldestRetainedVersion);
        Assert.NotNull(afterTrim.KnownOffsetAtOrBelow(7));
        using var following = new CancellationTokenSource(Timeout);
        var next = Task.Run(async () =>
        {
            await foreach (var version in afterTrim.FollowAsync(6, following.Token))
                return version.Version;
            return -1;
        });

        // The next version is appended and read as usual.
        await reopened.AppendAsync(Version(7, null, Put(7, "row7")));
        Assert.Equal([7L], (await ReadAsync(reopened, 6, 7)).Select(v => v.Version));
        Assert.Equal(7, await next.WaitAsync(Timeout));
    }

    /// <summary>
    /// A trim "of everything" while the writer keeps committing. The version committed
    /// after the trim read the last version, and before it asked where the topic ends, is
    /// not part of what was asked to go: it must still be there, as the log says it is.
    /// </summary>
    [Fact]
    public async Task A_trim_past_the_last_version_keeps_a_version_committed_while_it_ran()
    {
        await using var env = KafkaEnv.Create();
        var active = await env.ConnectWriterAsync();
        var log = await active.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        for (var v = 1; v <= 5; v++)
            await log.AppendAsync(Version(v, null, Put(v, "v" + v)));

        log.AfterTrimReadsLast = async () =>
        {
            await log.AppendAsync(Version(6, null, Put(6, "v6")));
            await Task.Delay(200); // until the commit's markers are written and the end has moved
        };
        await log.TrimBeforeAsync(long.MaxValue).AsTask().WaitAsync(Timeout);
        log.AfterTrimReadsLast = null;

        Assert.Equal(6, log.OldestRetainedVersion);
        Assert.Equal(6, await log.LastVersionAsync());
        Assert.Equal(1, await CountRecordsAsync(active, log.ChangesTopic, 1));
        var kept = Assert.Single(await ReadAsync(log, 5, 6));
        Assert.Equal(6, kept.Version);

        // Another process finds the same: its history starts at 6 and holds it.
        var successor = await env.ConnectWriterAsync();
        var reopened = await successor.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        Assert.Equal(6, reopened.OldestRetainedVersion);
        Assert.Equal(6, Assert.Single(await ReadAsync(reopened, 5, 6)).Version);

        // And with nothing committed meanwhile, a trim past the end leaves nothing.
        await reopened.TrimBeforeAsync(long.MaxValue).AsTask().WaitAsync(Timeout);
        Assert.Equal(7, reopened.OldestRetainedVersion);
        await KafkaEnv.EventuallyAsync(
            async () => await CountRecordsAsync(successor, log.ChangesTopic, 1) == 0, "the changes topic is empty");
    }

    /// <summary>Two parts of one process open the same new table at the same moment. One
    /// identity may come of it: a second epoch in the rows topic is a table no standby can open.</summary>
    [Fact]
    public async Task Concurrent_first_opens_of_a_table_mint_one_identity()
    {
        await using var env = KafkaEnv.Create();
        var active = await env.ConnectWriterAsync();
        var standby = await env.ConnectAsync();
        for (var t = 0; t < 4; t++)
        {
            var name = "t" + t;
            var opened = await Task.WhenAll(Enumerable.Range(0, 6).Select(_ =>
                Task.Run(() => active.OpenTableLogAsync<int, Row>(name)))).WaitAsync(Timeout);
            Assert.All(opened, log => Assert.Same(opened[0], log));

            var seen = await standby.OpenTableLogAsync<int, Row>(name).WaitAsync(Timeout);
            Assert.Equal(opened[0].Epoch, seen.Epoch);
            await using var followed = await SharedTable<int, Row>.FollowAsync(name, r => r.Id, seen).WaitAsync(Timeout);
            Assert.Equal(0, followed.LatestVersion);

            // One head per rows partition and no more: nobody minted twice.
            Assert.Equal(seen.RowPartitions, await CountRecordsAsync(standby, seen.RowsTopic, seen.RowPartitions));
        }

        // A failed or cancelled open is forgotten, so the name can be opened again.
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => active.OpenTableLogAsync<int, Row>("later", cancelled.Token));
        Assert.Equal(0, await (await active.OpenTableLogAsync<int, Row>("later").WaitAsync(Timeout)).LastVersionAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => active.OpenTableLogAsync<string, Row>("later"));
    }

    [Fact]
    public async Task A_new_process_follows_from_the_head_without_reading_the_history()
    {
        await using var env = KafkaEnv.Create();
        var active = await env.ConnectWriterAsync();
        var log = await active.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        const int Versions = 60;
        for (var v = 1; v <= Versions; v++)
            await log.AppendAsync(Version(v, null, Put(v % 7, $"v{v}", old: v > 7 ? $"v{v - 7}" : null)));

        // The standby restores the rows, and with them learns from the head where the
        // version before the last starts in the changes topic: its follower seeks there
        // instead of reading sixty versions to skip them. (One record and one marker per
        // version: version v starts at offset 2(v - 1).)
        var standby = await env.ConnectAsync();
        var followed = await standby.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        TableHead head = null!;
        await KafkaEnv.EventuallyAsync(async () => (head = (await RestoreAsync(followed)).Head).Version == Versions, "the restore sees the last version");
        Assert.Equal(2 * (Versions - 2), followed.KnownOffsetAtOrBelow(head.Version + 1));

        using var stop = new CancellationTokenSource(Timeout);
        var next = Task.Run(async () =>
        {
            await foreach (var version in followed.FollowAsync(head.Version, stop.Token))
                return version;
            throw new InvalidOperationException("the log ended");
        });
        await log.AppendAsync(Version(Versions + 1, null, Put(100, "new")));
        var seen = await next.WaitAsync(Timeout);
        Assert.Equal(Versions + 1, seen.Version);
        Assert.Equal([Put(100, "new")], seen.Changes);

        // A range read below the known offset still works: it starts from the beginning.
        Assert.Equal([3L, 4L, 5L], (await ReadAsync(followed, 2, 5)).Select(v => v.Version));
    }

    [Fact]
    public async Task Key_types_whose_equal_values_serialize_differently_are_refused()
    {
        await using var env = KafkaEnv.Create();
        var active = await env.ConnectWriterAsync();
        var failure = await Assert.ThrowsAsync<NotSupportedException>(() => active.OpenTableLogAsync<decimal, Row>("by-decimal"));
        Assert.Contains("Decimal", failure.Message);
        await Assert.ThrowsAsync<ArgumentException>(() => active.OpenTableLogAsync<int, Row>("bad name"));

        // Composite keys of stable parts are fine, and are found again by their bytes.
        var log = await active.OpenTableLogAsync<(string Desk, int Book), Row>("by-tuple").WaitAsync(Timeout);
        await log.AppendAsync(new TableVersion<(string, int), Row>(
            1, [new(("fx", 1), null, new Row(1, "a")), new(("fx", 2), null, new Row(2, "b"))], new Dictionary<int, long>()));
        await log.AppendAsync(new TableVersion<(string, int), Row>(
            2, [new(("fx", 1), new Row(1, "a"), null)], new Dictionary<int, long>()));
        var successor = await env.ConnectWriterAsync();
        var (rows, _) = await RestoreAsync(await successor.OpenTableLogAsync<(string Desk, int Book), Row>("by-tuple"));
        Assert.Equal(new Dictionary<(string, int), Row> { [("fx", 2)] = new(2, "b") }, rows);
    }

    /// <summary>Not <see cref="JsonStateSerializer"/>, so the log goes through its strings.</summary>
    private sealed class PipeSerializer : IStateSerializer
    {
        public string Serialize<T>(T value) => "|" + JsonStateSerializer.Instance.Serialize(value);

        public T Deserialize<T>(string data) => JsonStateSerializer.Instance.Deserialize<T>(data[1..]);
    }

    [Fact]
    public async Task A_custom_serializer_round_trips_through_its_strings()
    {
        await using var env = KafkaEnv.Create();
        var options = env.Options(serializer: new PipeSerializer());
        var active = await env.ConnectWriterAsync(options);
        var log = await active.OpenTableLogAsync<string, Row>("rows").WaitAsync(Timeout);
        await log.AppendAsync(new TableVersion<string, Row>(
            1, [new("a\"quoted\"", null, new Row(1, "one")), new("b", null, new Row(2, "two"))], new Dictionary<int, long>()));
        await log.AppendAsync(new TableVersion<string, Row>(
            2, [new("b", new Row(2, "two"), null), new("c", null, new Row(3, "three"))], new Dictionary<int, long>()));

        var successor = await env.ConnectWriterAsync(options);
        var reopened = await successor.OpenTableLogAsync<string, Row>("rows").WaitAsync(Timeout);
        var (rows, _) = await RestoreAsync(reopened);
        Assert.Equal(new Dictionary<string, Row> { ["a\"quoted\""] = new(1, "one"), ["c"] = new(3, "three") }, rows);
        var versions = await ReadAsync(reopened, 0, 2);
        Assert.Equal(new TableChange<string, Row>("b", new Row(2, "two"), null), versions[1].Changes[0]);
    }

    [Fact]
    public async Task A_restore_taken_while_the_writer_is_busy_is_one_version_not_a_mixture()
    {
        await using var env = KafkaEnv.Create();
        var active = await env.ConnectWriterAsync(env.Options(rowPartitions: 4));
        var log = await active.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);

        // Every version rewrites all 64 keys — some in every partition — with the version
        // number, so a snapshot that mixed two versions would hold two different names.
        const int Keys = 64;
        using var stop = new CancellationTokenSource();
        var written = 0L;
        var writing = Task.Run(async () =>
        {
            for (var v = 1L; !stop.IsCancellationRequested; v++)
            {
                var changes = Enumerable.Range(0, Keys)
                    .Select(k => new TableChange<int, Row>(k, v == 1 ? null : new Row(k, $"v{v - 1}"), new Row(k, $"v{v}"))).ToList();
                await log.AppendAsync(new TableVersion<int, Row>(v, changes, new Dictionary<int, long>()));
                Volatile.Write(ref written, v);
            }
        });
        await KafkaEnv.EventuallyAsync(() => Task.FromResult(Volatile.Read(ref written) >= 5), "the writer is under way");

        var standby = await env.ConnectAsync(env.Options(rowPartitions: 4));
        var followed = await standby.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        var versionsSeen = new HashSet<long>();
        for (var i = 0; i < 12; i++)
        {
            var (rows, head) = await RestoreAsync(followed);
            Assert.Equal(Keys, rows.Count);
            Assert.All(rows.Values, row => Assert.Equal($"v{head.Version}", row.Name));
            versionsSeen.Add(head.Version);
        }
        Assert.True(versionsSeen.Count > 1, "the writer did not advance between restores, so nothing was put to the test");

        stop.Cancel();
        await writing.WaitAsync(Timeout);
    }

    [Fact]
    public async Task A_shared_table_survives_its_process_and_a_standby_follows_and_takes_over()
    {
        await using var env = KafkaEnv.Create();
        var first = await env.ConnectWriterAsync();
        var table = await SharedTable<int, Row>.OpenAsync("rows", r => r.Id, await first.OpenTableLogAsync<int, Row>("rows"));
        await table.Writer.UpsertAsync(new Row(1, "one"), partition: 0, offset: 0);
        await table.Writer.UpsertAsync(new Row(2, "two"), partition: 0, offset: 1);
        await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);

        // The standby restores, then follows.
        var standbyLog = await env.ConnectAsync();
        await using var standby = await SharedTable<int, Row>.FollowAsync(
            "rows", r => r.Id, await standbyLog.OpenTableLogAsync<int, Row>("rows")).WaitAsync(Timeout);
        Assert.Equal(table.Epoch, standby.Epoch);
        Assert.Equal(1, standby.LatestVersion);

        await table.Writer.DeleteAsync(1, partition: 0, offset: 2);
        await table.Writer.UpsertAsync(new Row(3, "three"), partition: 0, offset: 3);
        await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
        await KafkaEnv.EventuallyAsync(() => Task.FromResult(standby.LatestVersion == 2), "the standby's table has followed version 2");
        Assert.Equal(2, standby.Count);

        // Takeover: fence, then promote the table. The old writer is finished.
        await standbyLog.AcquireWriterAsync().WaitAsync(Timeout);
        await standby.PromoteAsync().WaitAsync(Timeout);
        await table.Writer.UpsertAsync(new Row(9, "zombie"), partition: 0, offset: 4);
        await Assert.ThrowsAsync<TableLogFencedException>(() => table.Writer.CommitAsync().AsTask()).WaitAsync(Timeout);

        // The new writer resumes from the source offsets the last version recorded: a
        // change at or below them is a replay and is dropped.
        Assert.Equal(new Dictionary<int, long> { [0] = 3 }, standby.CommittedOffsets);
        await standby.Writer.UpsertAsync(new Row(3, "replayed"), partition: 0, offset: 3);
        await standby.Writer.UpsertAsync(new Row(4, "four"), partition: 0, offset: 4);
        Assert.Equal(3, await standby.Writer.CommitAsync().AsTask().WaitAsync(Timeout));

        var third = await env.ConnectWriterAsync();
        var reopened = await SharedTable<int, Row>.OpenAsync("rows", r => r.Id, await third.OpenTableLogAsync<int, Row>("rows"));
        Assert.Equal(3, reopened.LatestVersion);
        Assert.Equal(3, reopened.Count);
        using var snapshot = reopened.Snapshot();
        Assert.False(snapshot.TryGet(1, out _));
        Assert.True(snapshot.TryGet(3, out var three) && three == new Row(3, "three"));
        Assert.True(snapshot.TryGet(4, out var four) && four == new Row(4, "four"));
        Assert.False(snapshot.TryGet(9, out _));
    }

    [Fact]
    public async Task Restore_after_compaction_equals_restore_before_it()
    {
        await using var env = KafkaEnv.Create();
        // Small segments that roll quickly and a cleaner that runs on any dirt at all.
        var options = env.Options(rowPartitions: 1, rowsTopic: new Dictionary<string, string>
        {
            ["segment.bytes"] = "32768",
            ["min.cleanable.dirty.ratio"] = "0.01",
            ["min.compaction.lag.ms"] = "0",
            ["max.compaction.lag.ms"] = "1000",
        });
        var active = await env.ConnectWriterAsync(options);
        var log = await active.OpenTableLogAsync<int, Instrument>("instruments").WaitAsync(Timeout);

        // 60 versions over 50 keys: each key rewritten 60 times, a fifth deleted at the end.
        const int Keys = 50, Versions = 60;
        for (var v = 1; v <= Versions; v++)
        {
            var changes = Enumerable.Range(0, Keys)
                .Select(k => new TableChange<int, Instrument>(k, v == 1 ? null : Instrument.Make(k, v - 1), Instrument.Make(k, v))).ToList();
            await log.AppendAsync(new TableVersion<int, Instrument>(v, changes, new Dictionary<int, long> { [0] = v }));
        }
        var deletes = Enumerable.Range(0, Keys).Where(k => k % 5 == 0)
            .Select(k => new TableChange<int, Instrument>(k, Instrument.Make(k, Versions), null)).ToList();
        await log.AppendAsync(new TableVersion<int, Instrument>(Versions + 1, deletes, new Dictionary<int, long> { [0] = Versions + 1 }));

        var successor = await env.ConnectWriterAsync(options);
        var reopened = await successor.OpenTableLogAsync<int, Instrument>("instruments").WaitAsync(Timeout);
        // What was written, by arithmetic and not by reading: a row record per change, a
        // head per version (version 0 included). With these settings a closed segment is
        // cleanable at once, so a count read here could already be a compacted one.
        long written = Keys * Versions + deletes.Count + (Versions + 2);
        var (before, headBefore) = await RestoreAsync(reopened);
        Assert.Equal(Keys - deletes.Count, before.Count);

        // Keep the active segment rolling so the cleaner has closed segments to work on,
        // and wait for it. The broker's cleaner wakes every 15 s by default.
        var compacted = written;
        var waited = Stopwatch.StartNew();
        for (var v = Versions + 2; compacted > written / 4 && waited.Elapsed < TimeSpan.FromSeconds(90); v++)
        {
            await reopened.AppendAsync(new TableVersion<int, Instrument>(
                v, [new(1, Instrument.Make(1, v - 1), Instrument.Make(1, v))], new Dictionary<int, long> { [0] = v }));
            await Task.Delay(1000);
            compacted = await CountRecordsAsync(successor, log.RowsTopic, 1);
        }
        if (compacted > written / 4)
            Assert.Skip($"The broker's log cleaner did not compact the rows topic within 90 s ({written} records, still {compacted}).");
        KafkaEnv.Measured(output, $"rows topic: {written} records before compaction, {compacted} after ({waited.Elapsed.TotalSeconds:F0} s)");

        var last = await reopened.LastVersionAsync();
        var third = await env.ConnectWriterAsync(options);
        var (after, headAfter) = await RestoreAsync(await third.OpenTableLogAsync<int, Instrument>("instruments"));
        Assert.Equal(last, headAfter.Version);
        Assert.True(headAfter.Version > headBefore.Version);
        Assert.Equal(before.Keys.Order(), after.Keys.Order());
        foreach (var (key, row) in before)
            Assert.Equal(key == 1 ? Instrument.Make(1, (int)last) : row, after[key]);
    }

    [Fact]
    public async Task Large_table_restores_from_the_rows_topic()
    {
        await using var env = KafkaEnv.Create();
        const int Rows = 200_000, Chunk = 5_000;
        var active = await env.ConnectWriterAsync(env.Options(rowPartitions: 4));
        var table = await SharedTable<int, Instrument>.OpenAsync(
            "instruments", i => i.Id, await active.OpenTableLogAsync<int, Instrument>("instruments"));

        var loading = Stopwatch.StartNew();
        for (var start = 0; start < Rows; start += Chunk)
        {
            for (var id = start; id < start + Chunk; id++)
                await table.Writer.UpsertAsync(Instrument.Make(id));
            await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
        }
        loading.Stop();
        Assert.Equal(Rows, table.Count);

        // A cold start: a new process, nothing in memory.
        var successor = await env.ConnectWriterAsync(env.Options(rowPartitions: 4));
        var restoring = Stopwatch.StartNew();
        var restored = await SharedTable<int, Instrument>.OpenAsync(
            "instruments", i => i.Id, await successor.OpenTableLogAsync<int, Instrument>("instruments")).WaitAsync(TimeSpan.FromMinutes(3));
        restoring.Stop();

        Assert.Equal(Rows, restored.Count);
        Assert.Equal(table.LatestVersion, restored.LatestVersion);
        using (var snapshot = restored.Snapshot())
        {
            foreach (var id in new[] { 0, 1, 4_999, 5_000, 123_456, Rows - 1 })
                Assert.True(snapshot.TryGet(id, out var row) && row == Instrument.Make(id));
            Assert.False(snapshot.TryGet(Rows, out _));
        }
        KafkaEnv.Measured(output,
            $"{Rows:N0} wide rows: loaded in {loading.Elapsed.TotalSeconds:F1} s ({Rows / Chunk} versions of {Chunk:N0}), " +
            $"restored in {restoring.Elapsed.TotalSeconds:F2} s ({Rows / restoring.Elapsed.TotalSeconds:N0} rows/s)");
    }
}
