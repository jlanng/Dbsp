using Dbsp.Tables;

namespace Dbsp.Tests;

/// <summary>
/// The table's change log seam: what <see cref="InMemoryTableLog{TKey,TRow}"/> promises as an
/// <see cref="ITableLog{TKey,TRow}"/>, and what the table's writer does with a log — append
/// before publish ("visible means durable"), fault on a failed append, restore from one, and
/// record the source offsets a version consumed.
/// </summary>
public class TableLogTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Settle = TimeSpan.FromMilliseconds(150);

    private sealed record Instrument(int Id, string Symbol);

    private static readonly IReadOnlyDictionary<int, long> NoOffsets = new Dictionary<int, long>();

    private static TableVersion<int, Instrument> Version(long version, params TableChange<int, Instrument>[] changes) =>
        new(version, changes, NoOffsets);

    private static TableChange<int, Instrument> Insert(int id, string symbol) => new(id, null, new Instrument(id, symbol));

    private static async Task<List<long>> ReadVersions(IAsyncEnumerable<TableVersion<int, Instrument>> versions)
    {
        var read = new List<long>();
        await foreach (var version in versions)
            read.Add(version.Version);
        return read;
    }

    /// <summary>A log whose appends can be held open or made to fail, in front of a real one.</summary>
    private sealed class GatedLog(ITableLog<int, Instrument> inner) : ITableLog<int, Instrument>
    {
        public TaskCompletionSource? Gate;
        public Exception? Failure;
        public readonly TaskCompletionSource AppendStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public string Epoch => inner.Epoch;
        public long OldestRetainedVersion => inner.OldestRetainedVersion;
        public ValueTask<long> LastVersionAsync(CancellationToken cancellationToken = default) => inner.LastVersionAsync(cancellationToken);

        public async ValueTask AppendAsync(TableVersion<int, Instrument> version, CancellationToken cancellationToken = default)
        {
            AppendStarted.TrySetResult();
            if (Gate is { } gate)
                await gate.Task.WaitAsync(cancellationToken);
            if (Failure is { } failure)
                throw failure;
            await inner.AppendAsync(version, cancellationToken);
        }

        public IAsyncEnumerable<TableVersion<int, Instrument>> ReadAsync(
            long fromExclusive, long toInclusive, CancellationToken cancellationToken = default) =>
            inner.ReadAsync(fromExclusive, toInclusive, cancellationToken);

        public IAsyncEnumerable<TableVersion<int, Instrument>> FollowAsync(
            long fromExclusive, CancellationToken cancellationToken = default) =>
            inner.FollowAsync(fromExclusive, cancellationToken);

        public ValueTask<TableHead> RestoreAsync(Action<int, Instrument> row, CancellationToken cancellationToken = default) =>
            inner.RestoreAsync(row, cancellationToken);

        public ValueTask TrimBeforeAsync(long version, CancellationToken cancellationToken = default) =>
            inner.TrimBeforeAsync(version, cancellationToken);
    }

    // ── the in-memory log ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task Appended_versions_read_back_in_order_over_any_range()
    {
        var log = new InMemoryTableLog<int, Instrument>();
        Assert.Equal(1, log.OldestRetainedVersion); // nothing retained: one past the last version
        for (var v = 1; v <= 5; v++)
            await log.AppendAsync(Version(v, Insert(v, "S" + v)));

        Assert.Equal(1, log.OldestRetainedVersion);
        Assert.Equal([1, 2, 3, 4, 5], await ReadVersions(log.ReadAsync(0, 5)));
        Assert.Equal([3, 4], await ReadVersions(log.ReadAsync(2, 4)));
        Assert.Empty(await ReadVersions(log.ReadAsync(5, 5)));
    }

    [Fact]
    public async Task An_append_must_carry_the_next_version_number()
    {
        var log = new InMemoryTableLog<int, Instrument>();
        await log.AppendAsync(Version(1, Insert(1, "A")));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await log.AppendAsync(Version(3, Insert(2, "B"))));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await log.AppendAsync(Version(1, Insert(2, "B"))));
    }

    [Fact]
    public async Task Reading_past_the_last_version_is_an_error()
    {
        var log = new InMemoryTableLog<int, Instrument>();
        await log.AppendAsync(Version(1, Insert(1, "A")));
        Assert.Throws<ArgumentOutOfRangeException>(() => log.ReadAsync(0, 2));
    }

    [Fact]
    public async Task History_is_capped_and_a_read_below_it_fails()
    {
        var log = new InMemoryTableLog<int, Instrument>(maxRetainedVersions: 3);
        for (var v = 1; v <= 5; v++)
            await log.AppendAsync(Version(v, Insert(v, "S" + v)));

        Assert.Equal(3, log.OldestRetainedVersion);
        Assert.Equal([3, 4, 5], await ReadVersions(log.ReadAsync(2, 5)));
        await Assert.ThrowsAsync<InvalidOperationException>(() => ReadVersions(log.ReadAsync(1, 5)));
    }

    [Fact]
    public async Task Trimming_drops_history_but_not_rows()
    {
        var log = new InMemoryTableLog<int, Instrument>();
        for (var v = 1; v <= 5; v++)
            await log.AppendAsync(Version(v, Insert(v, "S" + v)));

        await log.TrimBeforeAsync(4);
        Assert.Equal(4, log.OldestRetainedVersion);
        Assert.Equal([4, 5], await ReadVersions(log.ReadAsync(3, 5)));
        await Assert.ThrowsAsync<InvalidOperationException>(() => ReadVersions(log.ReadAsync(2, 5)));

        await log.TrimBeforeAsync(2); // floors only rise
        Assert.Equal(4, log.OldestRetainedVersion);

        var rows = new Dictionary<int, Instrument>();
        var head = await log.RestoreAsync(rows.Add);
        Assert.Equal(5, head.Version);
        Assert.Equal(5, rows.Count);
    }

    [Fact]
    public async Task Restore_returns_the_current_rows_and_the_head()
    {
        var log = new InMemoryTableLog<int, Instrument>(maxRetainedVersions: 0);
        var a = new Instrument(1, "A");
        var a2 = new Instrument(1, "A2");
        var b = new Instrument(2, "B");
        await log.AppendAsync(new TableVersion<int, Instrument>(
            1, [new(1, null, a), new(2, null, b)], new Dictionary<int, long> { [0] = 10 }));
        await log.AppendAsync(new TableVersion<int, Instrument>(
            2, [new(1, a, a2), new(2, b, null)], new Dictionary<int, long> { [0] = 12, [1] = 3 }));

        var rows = new Dictionary<int, Instrument>();
        var head = await log.RestoreAsync(rows.Add);

        Assert.Equal(2, head.Version);
        Assert.Equal(12, head.Offsets[0]);
        Assert.Equal(3, head.Offsets[1]);
        Assert.Equal(new Dictionary<int, Instrument> { [1] = a2 }, rows);
        Assert.Equal(3, log.OldestRetainedVersion); // rows and head only: no history to read
    }

    [Fact]
    public async Task Following_waits_for_versions_that_have_not_been_appended_yet()
    {
        var log = new InMemoryTableLog<int, Instrument>();
        await log.AppendAsync(Version(1, Insert(1, "A")));
        await log.AppendAsync(Version(2, Insert(2, "B")));

        using var stop = new CancellationTokenSource();
        await using var follower = log.FollowAsync(1, stop.Token).GetAsyncEnumerator();

        Assert.True(await follower.MoveNextAsync().AsTask().WaitAsync(Timeout));
        Assert.Equal(2, follower.Current.Version);

        var next = follower.MoveNextAsync().AsTask();
        await Task.Delay(Settle);
        Assert.False(next.IsCompleted); // caught up: waiting, not finished

        await log.AppendAsync(Version(3, Insert(3, "C")));
        Assert.True(await next.WaitAsync(Timeout));
        Assert.Equal(3, follower.Current.Version);

        var waiting = follower.MoveNextAsync().AsTask();
        stop.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting.WaitAsync(Timeout));
    }

    [Fact]
    public void Two_logs_never_share_an_epoch() =>
        Assert.NotEqual(new InMemoryTableLog<int, Instrument>().Epoch, new InMemoryTableLog<int, Instrument>().Epoch);

    // ── the writer and its log ─────────────────────────────────────────────────────────

    [Fact]
    public async Task A_version_is_visible_only_after_its_append_completes()
    {
        var inner = new InMemoryTableLog<int, Instrument>();
        var log = new GatedLog(inner) { Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };
        var table = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, log);

        await table.Writer.UpsertAsync(new Instrument(1, "AAPL"));
        var commit = table.Writer.CommitAsync().AsTask();
        await log.AppendStarted.Task.WaitAsync(Timeout);
        await Task.Delay(Settle);

        // The append is in flight: not durable, so not visible — to a version read, a
        // snapshot, or the committed offsets.
        Assert.False(commit.IsCompleted);
        Assert.Equal(0, table.LatestVersion);
        Assert.Equal(0, table.Count);
        using (var before = table.Snapshot())
            Assert.False(before.TryGet(1, out _));

        // What arrives meanwhile is staged for the next version, not folded into this one.
        await table.Writer.UpsertAsync(new Instrument(2, "MSFT"));

        log.Gate.SetResult();
        Assert.Equal(1, await commit.WaitAsync(Timeout));
        Assert.Equal(1, table.LatestVersion);
        using (var after = table.Snapshot())
        {
            Assert.True(after.TryGet(1, out _));
            Assert.False(after.TryGet(2, out _));
        }
        Assert.Equal([1], await ReadVersions(inner.ReadAsync(0, 1)));

        log.Gate = null;
        Assert.Equal(2, await table.Writer.CommitAsync());
        using var latest = table.Snapshot();
        Assert.True(latest.TryGet(2, out _));
    }

    [Fact]
    public async Task Cancelling_a_commit_whose_append_is_in_flight_neither_abandons_the_append_nor_faults_the_writer()
    {
        var log = new GatedLog(new InMemoryTableLog<int, Instrument>())
        {
            Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously),
        };
        var table = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, log);

        using var cancel = new CancellationTokenSource();
        await table.Writer.UpsertAsync(new Instrument(1, "AAPL"));
        var commit = table.Writer.CommitAsync(cancel.Token).AsTask();
        await log.AppendStarted.Task.WaitAsync(Timeout);

        // The batch has been taken and handed to the log. Abandoning the append now would
        // leave nobody knowing whether the version is durable, so the cancellation is not
        // passed on: the commit runs to its end.
        cancel.Cancel();
        await Task.Delay(Settle);
        Assert.False(commit.IsCompleted);

        log.Gate.SetResult();
        Assert.Equal(1, await commit.WaitAsync(Timeout));
        using (var snapshot = table.Snapshot())
            Assert.True(snapshot.TryGet(1, out _));

        log.Gate = null;
        await table.Writer.UpsertAsync(new Instrument(2, "MSFT"));
        Assert.Equal(2, await table.Writer.CommitAsync());
    }

    [Fact]
    public async Task A_fenced_append_publishes_nothing_and_faults_the_writer()
    {
        var log = new GatedLog(new InMemoryTableLog<int, Instrument>());
        var table = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, log);
        await table.Writer.UpsertAsync(new Instrument(1, "AAPL"));
        await table.Writer.CommitAsync();

        log.Failure = new TableLogFencedException("another writer took over");
        await table.Writer.UpsertAsync(new Instrument(2, "MSFT"));
        await Assert.ThrowsAsync<TableLogFencedException>(async () => await table.Writer.CommitAsync());

        Assert.Equal(1, table.LatestVersion);
        using (var snapshot = table.Snapshot())
            Assert.False(snapshot.TryGet(2, out _));

        // Terminal, even once the log would accept appends again: the lost version's fate is unknown.
        log.Failure = null;
        var staged = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await table.Writer.UpsertAsync(new Instrument(3, "GOOG")));
        Assert.IsType<TableLogFencedException>(staged.InnerException);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await table.Writer.CommitAsync());
    }

    [Fact]
    public async Task A_table_opened_over_a_log_restores_its_rows_version_and_offsets()
    {
        var log = new InMemoryTableLog<int, Instrument>();
        var first = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, log);
        await first.Writer.UpsertAsync(new Instrument(1, "AAPL"), partition: 0, offset: 0);
        await first.Writer.UpsertAsync(new Instrument(2, "MSFT"), partition: 0, offset: 1);
        await first.Writer.CommitAsync();
        await first.Writer.DeleteAsync(2, partition: 0, offset: 2);
        await first.Writer.UpsertAsync(new Instrument(3, "GOOG"), partition: 1, offset: 7);
        await first.Writer.CommitAsync();

        var restored = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, log);

        Assert.Equal(first.Epoch, restored.Epoch);
        Assert.Equal(log.Epoch, restored.Epoch);
        Assert.Equal(2, restored.LatestVersion);
        Assert.Equal(2, restored.Count);
        Assert.Equal(2, restored.CommittedOffsets[0]);
        Assert.Equal(7, restored.CommittedOffsets[1]);
        using (var snapshot = restored.Snapshot())
        {
            Assert.True(snapshot.TryGet(1, out var row));
            Assert.Equal("AAPL", row.Symbol);
            Assert.False(snapshot.TryGet(2, out _));
            Assert.True(snapshot.TryGet(3, out _));
        }

        // The new writer continues the log's numbering, and its replay positions.
        await restored.Writer.UpsertAsync(new Instrument(9, "stale"), partition: 1, offset: 7); // replayed
        await restored.Writer.UpsertAsync(new Instrument(4, "AMZN"), partition: 1, offset: 8);
        Assert.Equal(3, await restored.Writer.CommitAsync());
        using var latest = restored.Snapshot();
        Assert.False(latest.TryGet(9, out _));
        Assert.True(latest.TryGet(4, out _));
        Assert.Equal([1, 2, 3], await ReadVersions(log.ReadAsync(0, 3)));
    }

    [Fact]
    public void A_table_without_a_log_has_a_new_epoch_every_time_it_is_created() =>
        Assert.NotEqual(
            new SharedTable<int, Instrument>("instruments", i => i.Id).Epoch,
            new SharedTable<int, Instrument>("instruments", i => i.Id).Epoch);

    [Fact]
    public async Task Committed_offsets_are_those_of_the_latest_published_version()
    {
        var table = new SharedTable<int, Instrument>("instruments", i => i.Id);
        Assert.Empty(table.CommittedOffsets);

        await table.Writer.UpsertAsync(new Instrument(1, "A"), partition: 0, offset: 4);
        await table.Writer.UpsertAsync(new Instrument(2, "B"), partition: 2, offset: 9);
        Assert.Empty(table.CommittedOffsets); // staged, not published
        await table.Writer.CommitAsync();
        Assert.Equal(new Dictionary<int, long> { [0] = 4, [2] = 9 }, table.CommittedOffsets);

        await table.Writer.UpsertAsync(new Instrument(1, "A2"), partition: 0, offset: 5);
        await table.Writer.CommitAsync();
        Assert.Equal(new Dictionary<int, long> { [0] = 5, [2] = 9 }, table.CommittedOffsets);
    }

    [Fact]
    public async Task A_record_stamped_at_or_below_its_partitions_position_is_dropped()
    {
        var table = new SharedTable<int, Instrument>("instruments", i => i.Id);
        await table.Writer.UpsertAsync(new Instrument(1, "new"), partition: 0, offset: 5);
        await table.Writer.CommitAsync();

        // A source replaying from before the committed position must not step the row back.
        await table.Writer.UpsertAsync(new Instrument(1, "old"), partition: 0, offset: 3);
        await table.Writer.DeleteAsync(1, partition: 0, offset: 5);
        Assert.Equal(1, await table.Writer.CommitAsync());

        // Positions are per partition.
        await table.Writer.UpsertAsync(new Instrument(1, "other partition"), partition: 1, offset: 3);
        Assert.Equal(2, await table.Writer.CommitAsync());
        using var snapshot = table.Snapshot();
        Assert.True(snapshot.TryGet(1, out var row));
        Assert.Equal("other partition", row.Symbol);
    }

    [Fact]
    public async Task Offsets_of_records_that_change_nothing_ride_with_the_next_version()
    {
        var table = new SharedTable<int, Instrument>("instruments", i => i.Id);
        await table.Writer.UpsertAsync(new Instrument(1, "A"), partition: 0, offset: 1);
        await table.Writer.CommitAsync();

        await table.Writer.UpsertAsync(new Instrument(1, "A"), partition: 0, offset: 2); // equal row: no version
        Assert.Equal(1, await table.Writer.CommitAsync());
        Assert.Equal(1, table.CommittedOffsets[0]);

        await table.Writer.UpsertAsync(new Instrument(2, "B"), partition: 1, offset: 0);
        Assert.Equal(2, await table.Writer.CommitAsync());
        Assert.Equal(2, table.CommittedOffsets[0]);
        Assert.Equal(0, table.CommittedOffsets[1]);
    }
}
