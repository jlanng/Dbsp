using System.Text.Json;
using Incremental.Storage;
using Incremental.Tables;

namespace Incremental.Tests;

/// <summary>The seams the enrichment service is written against: a provider's typed stores
/// released back to bytes and rebuilt with other types, a serializer per generation, the
/// committed offsets with no circuit, a table snapshot's scan and a follower's fault.</summary>
public class ChangelogReleaseTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private static Dictionary<string, long> Offsets(long n) => new() { ["in"] = n };

    private sealed record V1(int Id, string Name);

    // Another type of the same shape: what a second load of the same assembly gives.
    private sealed record V1Again(int Id, string Name);

    // A shape change. Safe where the type is a value (a value store's value): the entry's
    // record key does not involve it. Where a type is part of an entry's key (a weight
    // store's value, an indexed store's value) a new member changes the key's bytes, and
    // the old record would never be overwritten — keeping state across that is not safe.
    private sealed record V2(int Id, string Name, string? Extra = null);

    [Fact]
    public async Task Released_stores_come_back_with_other_types_and_the_same_entries()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var weights = provider.CreateWeightStore<V1>("w");
        var indexed = provider.CreateIndexedWeightStore<int, V1>("i");
        var values = provider.CreateValueStore<int, V1>("v");
        provider.Recover();
        weights.Update(new V1(1, "a"), 2);
        indexed.Update(7, new V1(1, "a"), 1);
        indexed.Update(7, new V1(2, "b"), 3);
        values.Put(5, new V1(5, "e"));
        await provider.CommitAsync(Offsets(1));
        var before = provider.DescribeState();

        // An uncommitted write is undone by the release, not carried into the bytes.
        weights.Update(new V1(9, "uncommitted"), 1);

        // Another type under the same name is refused while the typed store stands...
        Assert.Throws<InvalidOperationException>(() => provider.CreateWeightStore<V2>("w"));
        provider.ReleaseStores();
        Assert.Equal(before, provider.DescribeState());

        // ...and accepted once it has been released: the bytes are read as the new type.
        var weights2 = provider.CreateWeightStore<V1Again>("w");
        var indexed2 = provider.CreateIndexedWeightStore<int, V1Again>("i");
        var values2 = provider.CreateValueStore<int, V2>("v");
        provider.Recover();
        Assert.Equal(2, weights2.WeightOf(new V1Again(1, "a")));
        Assert.Equal(0, weights2.WeightOf(new V1Again(9, "uncommitted")));
        Assert.Equal(2, indexed2.GetGroup(7).Count);
        Assert.True(values2.TryGet(5, out var row));
        Assert.Equal(new V2(5, "e"), row);

        // The new generation commits over the old records; a cold reader agrees with it.
        values2.Put(5, new V2(5, "e", "x"));
        weights2.Update(new V1Again(1, "a"), -2);
        indexed2.Update(7, new V1Again(2, "b"), -3);
        await provider.CommitAsync(Offsets(2));
        await using var cold = ChangelogStorage.Follow(log);
        await cold.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.Equal(provider.DescribeState(), cold.Pipeline("p").DescribeState());
        Assert.Equal(2, cold.Pipeline("p").CommittedOffsets["in"]);
    }

    // Fewer members than V2: read into this, a V2's Extra is skipped.
    private sealed record Narrow(int Id);

    // A member of another type under the same name: V1's Name cannot be read into it.
    private sealed record Clash(int Id, int Name);

    [Fact]
    public async Task Stores_released_before_they_commit_give_back_the_bytes_they_were_made_of()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var values = provider.CreateValueStore<int, V2>("v");
        provider.Recover();
        values.Put(5, new V2(5, "e", "kept"));
        await provider.CommitAsync(Offsets(1));
        var committed = provider.DescribeState();
        provider.ReleaseStores();

        // A version that does not model Name or Extra is built on the state, writes
        // without committing, and is released: it never ran, so it dropped nothing.
        var narrow = provider.CreateValueStore<int, Narrow>("v");
        provider.Recover();
        Assert.True(narrow.TryGet(5, out var seen));
        Assert.Equal(new Narrow(5), seen);
        narrow.Put(6, new Narrow(6));
        provider.ReleaseStores();
        Assert.Equal(committed, provider.DescribeState());
        var back = provider.CreateValueStore<int, V2>("v");
        Assert.True(back.TryGet(5, out var row));
        Assert.Equal(new V2(5, "e", "kept"), row);
        Assert.False(back.TryGet(6, out _));
        provider.ReleaseStores();

        // Once it has committed, what it holds is the state: the members are gone.
        narrow = provider.CreateValueStore<int, Narrow>("v");
        provider.Recover();
        narrow.Put(6, new Narrow(6));
        await provider.CommitAsync(Offsets(2));
        provider.ReleaseStores();
        back = provider.CreateValueStore<int, V2>("v");
        Assert.True(back.TryGet(5, out row));
        Assert.Equal(new V2(5, null!, null), row);
        Assert.True(back.TryGet(6, out _));
    }

    [Fact]
    public async Task State_that_cannot_be_read_as_the_declared_types_is_a_typed_mismatch_and_stays_as_it_was()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var other = provider.CreateWeightStore<int>("other");
        var values = provider.CreateValueStore<int, V1>("v");
        provider.Recover();
        other.Update(3, 1);
        values.Put(5, new V1(5, "e"));
        await provider.CommitAsync(Offsets(1));
        var committed = provider.DescribeState();
        provider.ReleaseStores();

        // One store reads, the next does not: the failure is typed and names the store.
        provider.CreateWeightStore<int>("other");
        var mismatch = Assert.Throws<CircuitStateMismatchException>(() => provider.CreateValueStore<int, Clash>("v"));
        Assert.Contains("store 'v'", mismatch.Message);
        Assert.Contains("Clash", mismatch.Message);
        Assert.Single(mismatch.Problems);
        Assert.IsType<JsonException>(mismatch.InnerException);

        // Nothing was changed, so the types that wrote the state still read it.
        provider.ReleaseStores();
        Assert.Equal(committed, provider.DescribeState());
        var again = provider.CreateValueStore<int, V1>("v");
        Assert.True(again.TryGet(5, out var row));
        Assert.Equal(new V1(5, "e"), row);
    }

    [Fact]
    public async Task Release_on_a_follower_keeps_following_into_bytes()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var leader = await ChangelogStorage.OpenAsync(log);
        var written = leader.Pipeline("p");
        var store = written.CreateWeightStore<int>("s");
        written.Recover();
        store.Update(1, 1);
        await written.CommitAsync(Offsets(1));

        await using var follower = ChangelogStorage.Follow(log);
        var provider = follower.Pipeline("p");
        var followed = provider.CreateWeightStore<int>("s");
        await follower.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.Equal(1, followed.WeightOf(1));
        Assert.Equal(1, provider.CommittedOffsets["in"]);

        provider.ReleaseStores();
        store.Update(2, 5);
        await written.CommitAsync(Offsets(2));
        await follower.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.Equal(written.DescribeState(), provider.DescribeState());
        Assert.Equal(5, provider.CreateWeightStore<int>("s").WeightOf(2));
    }

    [Fact]
    public async Task Serializer_can_be_replaced_only_while_no_typed_store_exists()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var first = new CountingSerializer();
        provider.UseSerializer(first);
        var store = provider.CreateValueStore<int, V1>("v");
        provider.Recover();
        store.Put(1, new V1(1, "a"));
        await provider.CommitAsync(Offsets(1));
        Assert.True(first.Calls > 0);

        var second = new CountingSerializer();
        Assert.Throws<InvalidOperationException>(() => provider.UseSerializer(second));
        provider.ReleaseStores();
        provider.UseSerializer(second);
        var again = provider.CreateValueStore<int, V1>("v");
        var seen = first.Calls;
        Assert.True(again.TryGet(1, out var row));
        Assert.Equal("a", row!.Name);
        Assert.True(second.Calls > 0);
        Assert.Equal(seen, first.Calls);
    }

    private sealed class CountingSerializer : IStateSerializer
    {
        private readonly JsonSerializerOptions _options = new() { IncludeFields = true };
        public int Calls;

        public string Serialize<T>(T value)
        {
            Calls++;
            return JsonSerializer.Serialize(value, _options);
        }

        public T Deserialize<T>(string data)
        {
            Calls++;
            return JsonSerializer.Deserialize<T>(data, _options)!;
        }
    }

    private sealed record Row(int Id, string Name);

    [Fact]
    public async Task Snapshot_rows_scan_the_pinned_version()
    {
        var table = new SharedTable<int, Row>("rows", r => r.Id);
        await table.Writer.UpsertAsync(new Row(1, "a"));
        await table.Writer.UpsertAsync(new Row(2, "b"));
        await table.Writer.CommitAsync();
        using var snapshot = table.Snapshot();
        await table.Writer.UpsertAsync(new Row(3, "c"));
        await table.Writer.DeleteAsync(1);
        await table.Writer.CommitAsync();

        Assert.Equal([new Row(1, "a"), new Row(2, "b")], snapshot.Rows().Select(r => r.Value).OrderBy(r => r.Id));
        using var latest = table.Snapshot();
        Assert.Equal([2, 3], latest.Rows().Select(r => r.Key).Order());
    }

    [Fact]
    public async Task A_follower_that_loses_its_history_reports_the_fault()
    {
        var log = new InMemoryTableLog<int, Row>(maxRetainedVersions: 2);
        var writer = await SharedTable<int, Row>.OpenAsync("rows", r => r.Id, log);
        await writer.Writer.UpsertAsync(new Row(1, "a"));
        await writer.Writer.CommitAsync();

        // A connection that stops reading before the follower starts stands in for a
        // standby paused while the log moved on past what it retains.
        var connection = new StalledLog<int, Row>(log.Connect());
        await using var follower = await SharedTable<int, Row>.FollowAsync("rows", r => r.Id, connection);
        Assert.Null(follower.FollowFault);
        for (var i = 2; i < 10; i++)
        {
            await writer.Writer.UpsertAsync(new Row(i, "x"));
            await writer.Writer.CommitAsync();
        }
        connection.Release();

        var deadline = DateTime.UtcNow + Timeout;
        while (follower.FollowFault is null && DateTime.UtcNow < deadline)
            await Task.Delay(10);
        Assert.NotNull(follower.FollowFault);
        Assert.Equal(1, follower.LatestVersion);
    }

    /// <summary>Holds <c>FollowAsync</c> back until released.</summary>
    private sealed class StalledLog<TKey, TRow>(ITableLog<TKey, TRow> inner) : ITableLog<TKey, TRow>
        where TKey : notnull
        where TRow : class
    {
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Release() => _release.TrySetResult();

        public string Epoch => inner.Epoch;
        public long OldestRetainedVersion => inner.OldestRetainedVersion;
        public ValueTask<long> LastVersionAsync(CancellationToken cancellationToken = default) => inner.LastVersionAsync(cancellationToken);
        public ValueTask AppendAsync(TableVersion<TKey, TRow> version, CancellationToken cancellationToken = default) => inner.AppendAsync(version, cancellationToken);
        public IAsyncEnumerable<TableVersion<TKey, TRow>> ReadAsync(long fromExclusive, long toInclusive, CancellationToken cancellationToken = default) =>
            inner.ReadAsync(fromExclusive, toInclusive, cancellationToken);

        public async IAsyncEnumerable<TableVersion<TKey, TRow>> FollowAsync(
            long fromExclusive, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await _release.Task.WaitAsync(cancellationToken);
            await foreach (var version in inner.FollowAsync(fromExclusive, cancellationToken))
                yield return version;
        }

        public ValueTask<TableHead> RestoreAsync(Action<TKey, TRow> row, CancellationToken cancellationToken = default) => inner.RestoreAsync(row, cancellationToken);
        public ValueTask TrimBeforeAsync(long version, CancellationToken cancellationToken = default) => inner.TrimBeforeAsync(version, cancellationToken);
    }
}
