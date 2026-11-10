using Incremental.Tables;

namespace Incremental.Tests;

/// <summary>
/// A shared table on the standby: it follows the log the active's table writes
/// (<see cref="SharedTable{TKey,TRow}.FollowAsync"/>), refuses writes, and becomes the
/// writer on <see cref="SharedTable{TKey,TRow}.PromoteAsync"/>. Every wait is fenced.
/// </summary>
public class TableFollowerTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private sealed record Instrument(int Id, string Symbol, string Sector = "none");

    private static async Task Eventually(Func<bool> done)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!done())
        {
            Assert.True(DateTime.UtcNow < deadline, "Timed out waiting for the expected state.");
            await Task.Delay(5);
        }
    }

    private static Dictionary<int, Instrument> Rows(SharedTable<int, Instrument> table)
    {
        using var snapshot = table.Snapshot();
        var rows = new Dictionary<int, Instrument>();
        for (var id = 0; id < 50; id++)
            if (snapshot.TryGet(id, out var row))
                rows[id] = row;
        return rows;
    }

    [Fact]
    public async Task Follower_restores_then_mirrors_every_version_and_refuses_writes()
    {
        var log = new InMemoryTableLog<int, Instrument>();
        var active = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, log);
        await active.Writer.UpsertAsync(new Instrument(1, "ACME"), partition: 0, offset: 10);
        await active.Writer.UpsertAsync(new Instrument(2, "BOLT"), partition: 1, offset: 4);
        await active.Writer.CommitAsync();

        await using var standby = await SharedTable<int, Instrument>.FollowAsync("instruments", i => i.Id, log.Connect());
        Assert.True(standby.IsFollowing);
        Assert.False(active.IsFollowing);
        Assert.Equal(active.Epoch, standby.Epoch);
        Assert.Equal(1, standby.LatestVersion);
        Assert.Equal(Rows(active), Rows(standby));

        await active.Writer.UpsertAsync(new Instrument(1, "ACME2"), partition: 0, offset: 11);
        await active.Writer.DeleteAsync(2, partition: 1, offset: 5);
        await active.Writer.UpsertAsync(new Instrument(3, "CORE"));
        await active.Writer.CommitAsync();
        await active.Writer.UpsertAsync(new Instrument(4, "DELT"));
        await active.Writer.CommitAsync();

        await Eventually(() => standby.LatestVersion == 3);
        Assert.Equal(Rows(active), Rows(standby));
        Assert.Equal(active.Count, standby.Count);
        Assert.Equal(active.CommittedOffsets, standby.CommittedOffsets);

        // A reader pinned on the standby keeps its version while the follower moves on.
        using (var pinned = standby.Snapshot())
        {
            await active.Writer.DeleteAsync(4);
            await active.Writer.CommitAsync();
            await Eventually(() => standby.LatestVersion == 4);
            Assert.True(pinned.TryGet(4, out _));
        }
        Assert.DoesNotContain(4, Rows(standby).Keys);

        var refused = await Assert.ThrowsAsync<InvalidOperationException>(async () => await standby.Writer.UpsertAsync(new Instrument(9, "NOPE")));
        Assert.Contains("following its log", refused.Message);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await standby.Writer.DeleteAsync(1));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await standby.Writer.CommitAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await standby.Writer.ReplaceAllAsync(Array.Empty<Instrument>()));
    }

    [Fact]
    public async Task Promoted_follower_continues_the_log_and_the_old_writer_is_fenced()
    {
        var log = new InMemoryTableLog<int, Instrument>();
        var active = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, log);
        var standbyLog = log.Connect();
        await using var standby = await SharedTable<int, Instrument>.FollowAsync("instruments", i => i.Id, standbyLog);

        // Promote straight after a burst: the promotion itself reads to the log's end.
        for (var i = 1; i <= 100; i++)
        {
            await active.Writer.UpsertAsync(new Instrument(i % 10, $"S{i}"), partition: 0, offset: i);
            await active.Writer.CommitAsync();
        }

        standbyLog.TakeOver();
        await standby.PromoteAsync().WaitAsync(Timeout);
        Assert.False(standby.IsFollowing);
        Assert.Equal(100, standby.LatestVersion);
        Assert.Equal(Rows(active), Rows(standby));

        // The new writer carries on from the head: next version, and the source positions.
        await standby.Writer.UpsertAsync(new Instrument(3, "replayed"), partition: 0, offset: 100); // already consumed
        await standby.Writer.UpsertAsync(new Instrument(5, "NEW"), partition: 0, offset: 101);
        Assert.Equal(101, await standby.Writer.CommitAsync());
        Assert.Equal("S93", Rows(standby)[3].Symbol);
        Assert.Equal("NEW", Rows(standby)[5].Symbol);
        Assert.Equal(101, standby.CommittedOffsets[0]);
        Assert.Equal(101, await log.LastVersionAsync());

        // The old active is a zombie: its append is refused and its writer faults.
        await active.Writer.UpsertAsync(new Instrument(7, "ZOMBIE"));
        await Assert.ThrowsAsync<TableLogFencedException>(async () => await active.Writer.CommitAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await active.Writer.UpsertAsync(new Instrument(8, "AGAIN")));
        Assert.Equal(101, await log.LastVersionAsync());
        Assert.NotEqual("ZOMBIE", Rows(standby)[7].Symbol);

        await Assert.ThrowsAsync<InvalidOperationException>(() => standby.PromoteAsync()).WaitAsync(Timeout);

        // A table opened from the log afterwards sees the successor's history.
        var reopened = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, log.Connect());
        Assert.Equal(Rows(standby), Rows(reopened));
    }

    [Fact]
    public async Task Promotion_never_misses_the_followers_last_version()
    {
        // The follower reaches the log's end while the promotion is deciding to wait for
        // it. Run many times on several threads, because the window is a few instructions.
        await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            for (var round = 0; round < 250; round++)
            {
                var log = new InMemoryTableLog<int, Instrument>();
                var active = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, log);
                var standbyLog = log.Connect();
                await using var standby = await SharedTable<int, Instrument>.FollowAsync("instruments", i => i.Id, standbyLog);
                for (var i = 1; i <= 20; i++)
                {
                    await active.Writer.UpsertAsync(new Instrument(i % 10, $"S{i}"));
                    await active.Writer.CommitAsync();
                }

                standbyLog.TakeOver();
                await standby.PromoteAsync().WaitAsync(Timeout);
                Assert.Equal(20, standby.LatestVersion);
                Assert.Equal(Rows(active), Rows(standby));
            }
        }))).WaitAsync(TimeSpan.FromSeconds(60));
    }

    [Fact]
    public async Task Follower_maintains_secondary_indexes_and_feeds_a_circuit_built_on_it()
    {
        var log = new InMemoryTableLog<int, Instrument>();
        var active = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, log);
        await active.Writer.UpsertAsync(new Instrument(1, "ACME", "tech"));
        await active.Writer.CommitAsync();

        var standbyLog = log.Connect();
        await using var standby = await SharedTable<int, Instrument>.FollowAsync("instruments", i => i.Id, standbyLog);

        // The standby builds its circuit — index and all — while it follows.
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var quotes = builder.Input<(string Sector, int Px)>("quotes");
        var joined = quotes.GroupBy(q => q.Sector)
            .Join(builder.Table(standby).By("sector", i => i.Sector), (sector, q, i) => (i.Symbol, q.Px))
            .ToOutput();
        await using var circuit = builder.Build();

        await active.Writer.UpsertAsync(new Instrument(2, "BOLT", "tech"));
        await active.Writer.UpsertAsync(new Instrument(1, "ACME", "energy")); // leaves the sector
        await active.Writer.CommitAsync();

        standbyLog.TakeOver();
        await standby.PromoteAsync().WaitAsync(Timeout);
        circuit.Start();
        await circuit.Handle(quotes).InsertAsync(("tech", 5));
        await circuit.StepAsync().WaitAsync(Timeout);
        var delta = await circuit.Handle(joined).Reader.ReadAsync().AsTask().WaitAsync(Timeout);
        Assert.Equal([("BOLT", 5)], delta.Select(e => e.Value));

        // And stays maintained by the promoted writer.
        await standby.Writer.UpsertAsync(new Instrument(3, "CORE", "tech"));
        await standby.Writer.CommitAsync();
        await circuit.StepAsync().WaitAsync(Timeout);
        delta = await circuit.Handle(joined).Reader.ReadAsync().AsTask().WaitAsync(Timeout);
        Assert.Equal([("CORE", 5)], delta.Select(e => e.Value));
    }

    [Fact]
    public async Task Follower_that_lost_its_place_cannot_be_promoted()
    {
        // The log keeps two versions; a follower held up while five more arrive has a gap.
        var log = new InMemoryTableLog<int, Instrument>(maxRetainedVersions: 2);
        var active = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, log);
        var held = new HeldLog(log.Connect());
        await using var standby = await SharedTable<int, Instrument>.FollowAsync("instruments", i => i.Id, held);

        for (var i = 1; i <= 5; i++)
        {
            await active.Writer.UpsertAsync(new Instrument(i, $"S{i}"));
            await active.Writer.CommitAsync();
        }
        held.Release.SetResult();

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => standby.PromoteAsync()).WaitAsync(Timeout);
        Assert.Contains("stopped following", failure.Message);
        Assert.True(standby.IsFollowing);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await standby.Writer.CommitAsync());
    }

    [Fact]
    public async Task Follower_handed_a_version_out_of_order_stops_rather_than_apply_it()
    {
        // A log that skips a version: applying what follows would publish rows of a version
        // the view never saw the predecessor of.
        var log = new InMemoryTableLog<int, Instrument>();
        var active = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, log);
        var skipping = new HeldLog(log.Connect()) { SkipVersion = 2 };
        skipping.Release.SetResult();
        await using var standby = await SharedTable<int, Instrument>.FollowAsync("instruments", i => i.Id, skipping);

        for (var i = 1; i <= 3; i++)
        {
            await active.Writer.UpsertAsync(new Instrument(i, $"S{i}"));
            await active.Writer.CommitAsync();
        }

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => standby.PromoteAsync()).WaitAsync(Timeout);
        Assert.Contains("without gaps", failure.Message);
        Assert.Equal(1, standby.LatestVersion);
        Assert.Equal([1], Rows(standby).Keys);
    }

    [Fact]
    public async Task Disposing_a_follower_stops_it()
    {
        var log = new InMemoryTableLog<int, Instrument>();
        var active = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, log);
        var standby = await SharedTable<int, Instrument>.FollowAsync("instruments", i => i.Id, log.Connect());
        await standby.DisposeAsync().AsTask().WaitAsync(Timeout);

        await active.Writer.UpsertAsync(new Instrument(1, "ACME"));
        await active.Writer.CommitAsync();
        await Task.Delay(50);
        Assert.Equal(0, standby.LatestVersion);

        // A table that writes has nothing to stop.
        await active.DisposeAsync().AsTask().WaitAsync(Timeout);
    }

    /// <summary>A log whose follow does not begin until released.</summary>
    private sealed class HeldLog(ITableLog<int, Instrument> inner) : ITableLog<int, Instrument>
    {
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public long? SkipVersion { get; init; }

        public string Epoch => inner.Epoch;
        public long OldestRetainedVersion => inner.OldestRetainedVersion;
        public ValueTask<long> LastVersionAsync(CancellationToken cancellationToken = default) => inner.LastVersionAsync(cancellationToken);
        public ValueTask AppendAsync(TableVersion<int, Instrument> version, CancellationToken cancellationToken = default) =>
            inner.AppendAsync(version, cancellationToken);
        public IAsyncEnumerable<TableVersion<int, Instrument>> ReadAsync(long fromExclusive, long toInclusive, CancellationToken cancellationToken = default) =>
            inner.ReadAsync(fromExclusive, toInclusive, cancellationToken);

        public async IAsyncEnumerable<TableVersion<int, Instrument>> FollowAsync(
            long fromExclusive, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Release.Task.WaitAsync(cancellationToken);
            await foreach (var version in inner.FollowAsync(fromExclusive, cancellationToken))
                if (version.Version != SkipVersion)
                    yield return version;
        }

        public ValueTask<TableHead> RestoreAsync(Action<int, Instrument> row, CancellationToken cancellationToken = default) =>
            inner.RestoreAsync(row, cancellationToken);
        public ValueTask TrimBeforeAsync(long version, CancellationToken cancellationToken = default) =>
            inner.TrimBeforeAsync(version, cancellationToken);
    }
}
