using System.Text;
using Dbsp.Storage;
using Dbsp.Tables;

namespace Dbsp.Tests;

/// <summary>The in-memory logs stay proportional to what is live: the state changelog by
/// compacting itself, the table log by a cap on the row changes its history holds.</summary>
public class InMemoryLogBoundsTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    private static IReadOnlyDictionary<string, long> Offsets(long n) => new Dictionary<string, long> { ["in"] = n };

    [Fact]
    public async Task Self_compacting_changelog_holds_records_in_proportion_to_live_keys_however_many_commits()
    {
        const int threshold = 256;
        const int keys = 300;
        var log = new InMemoryStateChangelog(partitions: 2, compactAbove: threshold);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var values = provider.CreateValueStore<int, string>("values");
        var weights = provider.CreateWeightStore<string>("weights");

        // A follower that reads throughout, and so is somewhere in the log at every compaction.
        await using var follower = ChangelogStorage.Follow(log);

        var random = new Random(11);
        var peak = 0L;
        for (var commit = 1; commit <= 4000; commit++)
        {
            for (var i = 0; i < 20; i++)
            {
                var key = random.Next(keys);
                if (random.Next(4) == 0)
                    values.Remove(key);
                else
                    values.Put(key, $"v{commit}");
                // A weight that comes and goes: its record is a tombstone half the time.
                var word = "w" + random.Next(keys);
                weights.Update(word, weights.WeightOf(word) == 0 ? 1 : -1);
            }
            await provider.CommitAsync(Offsets(commit));
            peak = Math.Max(peak, log.RecordCount);
        }

        Assert.True(log.Compactions > 10, $"only {log.Compactions} compactions");
        Assert.True(log.RecordsAppended > 100_000, $"{log.RecordsAppended} appended");

        // Live entries are at most `keys` values + `keys` weights + the commit record. A
        // partition is compacted at max(threshold, 2 × what survived the last one).
        var live = 2 * keys + 1;
        Assert.True(peak <= 2 * live + 2 * threshold + 64, $"peak {peak} records for {live} live entries");

        // The follower read through every compaction and holds exactly the writer's state.
        await follower.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.Equal(provider.DescribeState(), follower.Pipeline("p").DescribeState());

        // And so does a copy made from what the log holds now.
        await using var cold = ChangelogStorage.Follow(log);
        await cold.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.Equal(provider.DescribeState(), cold.Pipeline("p").DescribeState());
    }

    [Fact]
    public async Task Self_compaction_keeps_a_tombstone_until_every_reader_has_passed_it()
    {
        var log = new InMemoryStateChangelog(partitions: 1, compactAbove: 4);
        var writer = await log.AcquireWriterAsync();

        async Task Commit(Action<IStateChangelogTransaction> write)
        {
            using var transaction = writer.BeginTransaction();
            write(transaction);
            await transaction.CommitAsync();
        }

        await Commit(t => t.Append(0, "a"u8, "1"u8));

        // A reader that has seen a=1 and stands just behind what comes next.
        using var stop = new CancellationTokenSource(Timeout);
        var reader = log.ReadAsync(0, 0, stop.Token).GetAsyncEnumerator(stop.Token);
        Assert.True(await reader.MoveNextAsync());
        Assert.Equal("a", Encoding.UTF8.GetString(reader.Current.Records.Single().Key.Span));

        // a is removed, and enough else is written to compact twice over.
        await Commit(t => t.AppendTombstone(0, "a"u8));
        for (var i = 0; i < 12; i++)
            await Commit(t => t.Append(0, "b"u8, Encoding.UTF8.GetBytes(i.ToString())));
        Assert.True(log.Compactions >= 1);

        // The reader still gets the tombstone: without it, it would keep a=1 for good.
        var seen = new List<(string Key, bool Tombstone)>();
        var end = await log.EndOffsetAsync(0);
        while (await reader.MoveNextAsync())
        {
            seen.AddRange(reader.Current.Records.Select(r => (Encoding.UTF8.GetString(r.Key.Span), r.IsTombstone)));
            if (reader.Current.NextOffset >= end)
                break;
        }
        Assert.Contains(("a", true), seen);
        await reader.DisposeAsync();

        // With the reader gone the next compaction drops it: a later reader starts from 0
        // and never sees a at all.
        for (var i = 0; i < 12; i++)
            await Commit(t => t.Append(0, "c"u8, Encoding.UTF8.GetBytes(i.ToString())));
        Assert.True(log.RecordCount <= 6, $"{log.RecordCount} records held");
        using var stop2 = new CancellationTokenSource(Timeout);
        var keysNow = new List<string>();
        end = await log.EndOffsetAsync(0);
        await foreach (var batch in log.ReadAsync(0, 0, stop2.Token))
        {
            keysNow.AddRange(batch.Records.Select(r => Encoding.UTF8.GetString(r.Key.Span)));
            if (batch.NextOffset >= end)
                break;
        }
        Assert.DoesNotContain("a", keysNow);
        Assert.Contains("b", keysNow);
        Assert.Contains("c", keysNow);
    }

    [Fact]
    public async Task Changelog_without_a_threshold_never_compacts_by_itself()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        var writer = await log.AcquireWriterAsync();
        for (var i = 0; i < 50; i++)
        {
            using var transaction = writer.BeginTransaction();
            transaction.Append(0, "k"u8, "v"u8);
            await transaction.CommitAsync();
        }
        Assert.Equal(50, log.RecordCount);
        Assert.Equal(0, log.Compactions);
    }

    private sealed record Row(int Id, string Text);

    private static TableVersion<int, Row> Version(long version, int from, int count) => new(
        version,
        Enumerable.Range(from, count).Select(i => new TableChange<int, Row>(i, null, new Row(i, "r" + version))).ToList(),
        new Dictionary<int, long>());

    [Fact]
    public async Task Table_log_history_is_capped_by_row_changes_and_always_keeps_the_newest_version()
    {
        var log = new InMemoryTableLog<int, Row>(maxRetainedVersions: 1000, maxRetainedChanges: 100);

        for (var v = 1; v <= 30; v++)
            await log.AppendAsync(Version(v, v * 10, 10));
        // 10 versions of 10 changes fit under 100.
        Assert.Equal(100, log.RetainedChanges);
        Assert.Equal(21, log.OldestRetainedVersion);

        // One version larger than the whole cap is still kept: a follower that is up to
        // date reads it rather than being cut loose by the very next append.
        await log.AppendAsync(Version(31, 1000, 500));
        Assert.Equal(31, log.OldestRetainedVersion);
        Assert.Equal(500, log.RetainedChanges);
        using var stop = new CancellationTokenSource(Timeout);
        var read = new List<long>();
        await foreach (var version in log.ReadAsync(30, 31, stop.Token))
            read.Add(version.Version);
        Assert.Equal([31L], read);

        // A reader further back finds its range gone, as with the version cap.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in log.ReadAsync(10, 31, stop.Token))
            {
            }
        });

        // The rows themselves are all there.
        var rows = 0;
        await log.RestoreAsync((_, _) => rows++);
        Assert.Equal(30 * 10 + 500, rows);

        await log.TrimBeforeAsync(32);
        Assert.Equal(0, log.RetainedChanges);
    }
}
