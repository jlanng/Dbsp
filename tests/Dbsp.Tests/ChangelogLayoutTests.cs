using Dbsp.Storage;

namespace Dbsp.Tests;

/// <summary>
/// Layouts: the tag in front of every value a provider's typed stores write, and what it
/// buys — a follower may hold typed stores, because the first record of another layout
/// turns them back into bytes before it is applied.
/// </summary>
public class ChangelogLayoutTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private static Dictionary<string, long> Offsets(long n) => new() { ["in"] = n };

    private sealed record V1(int Id, string Name);

    // V1 with a member more: what V1's types would drop if they re-wrote a V2 value.
    private sealed record V2(int Id, string Name, string? Extra = null);

    // A key type V1's keys do not read as.
    private sealed record OtherKey(string Text, bool Flag);

    /// <summary>Every record the log's one partition holds.</summary>
    private static async Task<List<ChangelogRecord>> RecordsAsync(InMemoryStateChangelog log)
    {
        var end = await log.EndOffsetAsync(0);
        var records = new List<ChangelogRecord>();
        if (end == 0)
            return records;
        using var stop = new CancellationTokenSource(Timeout);
        await foreach (var batch in log.ReadAsync(0, 0, stop.Token))
        {
            records.AddRange(batch.Records);
            if (batch.NextOffset >= end)
                break;
        }
        return records;
    }

    private static async Task<ChangelogStorageProvider> LeaderAsync(ChangelogStorage storage, string layout)
    {
        var provider = storage.Pipeline("p");
        provider.UseLayout(layout);
        await provider.CommitLayoutAsync();
        return provider;
    }

    [Fact]
    public async Task Values_carry_the_layout_and_read_back_the_same_with_or_without_one()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");

        // Untagged first: the format a log written before layouts holds.
        var values = provider.CreateValueStore<int, V1>("v");
        var weights = provider.CreateWeightStore<V1>("w");
        provider.Recover();
        values.Put(1, new V1(1, "plain"));
        weights.Update(new V1(1, "plain"), 3);
        await provider.CommitAsync(Offsets(1));
        Assert.Null(provider.CommittedLayout);
        Assert.All(await RecordsAsync(log), r => Assert.Equal(1, r.Value.Span[0]));

        provider.ReleaseStores();
        provider.UseLayout("layout-a");
        Assert.Equal("layout-a", provider.Layout);
        values = provider.CreateValueStore<int, V1>("v");
        weights = provider.CreateWeightStore<V1>("w");
        provider.Recover();
        Assert.True(values.TryGet(1, out var plain));
        Assert.Equal(new V1(1, "plain"), plain);
        values.Put(2, new V1(2, "tagged"));
        weights.Update(new V1(1, "plain"), 1);
        await provider.CommitAsync(Offsets(2));
        Assert.Equal("layout-a", provider.CommittedLayout);

        // Format 2: the format byte, the tag's length, the tag, then the serializer's output.
        var tagged = (await RecordsAsync(log)).Last(r => !r.IsTombstone).Value.ToArray();
        Assert.Equal(2, tagged[0]);
        Assert.Equal("layout-a".Length, tagged[1]);
        Assert.Equal("layout-a", System.Text.Encoding.UTF8.GetString(tagged, 2, tagged[1]));

        // A cold reader holds both formats as one state.
        await using var cold = ChangelogStorage.Follow(log);
        await cold.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.Equal(provider.DescribeState(), cold.Pipeline("p").DescribeState());
        Assert.Equal("layout-a", cold.Pipeline("p").CommittedLayout);
        var read = cold.Pipeline("p").CreateWeightStore<V1>("w");
        Assert.Equal(4, read.WeightOf(new V1(1, "plain")));
    }

    [Fact]
    public async Task A_layout_is_announced_once_and_changes_no_offset_or_sequence()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = await LeaderAsync(storage, "a");
        Assert.Equal("a", provider.CommittedLayout);
        var announced = (await RecordsAsync(log)).Count;
        Assert.Equal(1, announced);

        var values = provider.CreateValueStore<int, V1>("v");
        provider.Recover();
        values.Put(1, new V1(1, "x"));
        await provider.CommitAsync(Offsets(7));
        var sequence = provider.CommitSequence;
        var records = (await RecordsAsync(log)).Count;

        // The layout is already the committed one: nothing is written.
        await provider.CommitLayoutAsync();
        Assert.Equal(records, (await RecordsAsync(log)).Count);

        // Another layout: one record, the same checkpoint under it.
        provider.ReleaseStores();
        provider.UseLayout("b");
        await provider.CommitLayoutAsync();
        Assert.Equal(records + 1, (await RecordsAsync(log)).Count);
        Assert.Equal("b", provider.CommittedLayout);
        Assert.Equal(sequence, provider.CommitSequence);
        Assert.Equal(7, provider.CommittedOffsets["in"]);

        await using var follower = ChangelogStorage.Follow(log);
        await follower.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.Equal("b", follower.Pipeline("p").CommittedLayout);
        Assert.Equal(sequence, follower.Pipeline("p").CommitSequence);
        Assert.Equal(7, follower.Pipeline("p").CommittedOffsets["in"]);

        // A layout cannot change under typed stores.
        provider.CreateValueStore<int, V1>("v");
        Assert.Throws<InvalidOperationException>(() => provider.UseLayout("c"));
        Assert.Throws<ArgumentException>(() => storage.Pipeline("q").UseLayout(""));
        Assert.Throws<ArgumentException>(() => storage.Pipeline("q").UseLayout(new string('x', 256)));
    }

    [Fact]
    public async Task A_follower_with_typed_stores_follows_its_own_layout_and_takes_over_without_reading_the_state_again()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var leaderStorage = await ChangelogStorage.OpenAsync(log);
        var leader = await LeaderAsync(leaderStorage, "a");
        var values = leader.CreateValueStore<int, V1>("v");
        var indexed = leader.CreateIndexedWeightStore<int, V1>("i");
        leader.Recover();
        for (var i = 0; i < 100; i++)
        {
            values.Put(i, new V1(i, "n" + i));
            indexed.Update(i % 10, new V1(i, "n" + i), 1);
        }
        await leader.CommitAsync(Offsets(100));

        await using var followerStorage = ChangelogStorage.Follow(log);
        await followerStorage.WhenCaughtUpAsync().WaitAsync(Timeout);
        var follower = followerStorage.Pipeline("p");
        Assert.Equal("a", follower.CommittedLayout);
        follower.UseLayout("a");
        var epoch = follower.StoreEpoch;
        var followed = follower.CreateValueStore<int, V1>("v");
        var followedIndex = follower.CreateIndexedWeightStore<int, V1>("i");

        // What the leader commits next lands in the follower's typed stores.
        values.Put(5, new V1(5, "changed"));
        values.Remove(6);
        indexed.Update(3, new V1(3, "n3"), -1);
        await leader.CommitAsync(Offsets(103));
        await followerStorage.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.True(followed.TryGet(5, out var changed));
        Assert.Equal("changed", changed!.Name);
        Assert.False(followed.TryGet(6, out _));
        Assert.Equal(9, followedIndex.GetGroup(3).Count);
        Assert.Equal(epoch, follower.StoreEpoch);
        Assert.Equal(leader.DescribeState(), follower.DescribeState());

        // The takeover: the same store objects, now the writer's.
        await leaderStorage.DisposeAsync();
        await followerStorage.PromoteAsync().WaitAsync(Timeout);
        Assert.Same(followed, follower.CreateValueStore<int, V1>("v"));
        Assert.Equal(103, follower.Recover()["in"]);
        followed.Put(200, new V1(200, "after"));
        await follower.CommitAsync(Offsets(104));
        await using var cold = ChangelogStorage.Follow(log);
        await cold.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.Equal(follower.DescribeState(), cold.Pipeline("p").DescribeState());
    }

    [Fact]
    public async Task A_record_of_another_layout_turns_a_followers_typed_stores_back_into_bytes_before_it_is_applied()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var leaderStorage = await ChangelogStorage.OpenAsync(log);
        var leader = await LeaderAsync(leaderStorage, "v1");
        var values = leader.CreateValueStore<int, V1>("v");
        leader.Recover();
        values.Put(1, new V1(1, "one"));
        values.Put(2, new V1(2, "two"));
        await leader.CommitAsync(Offsets(2));

        await using var followerStorage = ChangelogStorage.Follow(log);
        await followerStorage.WhenCaughtUpAsync().WaitAsync(Timeout);
        var follower = followerStorage.Pipeline("p");
        follower.UseLayout("v1");
        var typed = follower.CreateValueStore<int, V1>("v");
        var epoch = follower.StoreEpoch;

        // The leader moves to another version of its types, which knows a member more,
        // and writes a value with it. No announcement here: the record alone must do.
        leader.ReleaseStores();
        leader.UseLayout("v2");
        var wider = leader.CreateValueStore<int, V2>("v");
        leader.Recover();
        wider.Put(2, new V2(2, "two", "the member V1 does not model"));
        wider.Put(3, new V2(3, "three", "another"));
        await leader.CommitAsync(Offsets(4));
        await followerStorage.WhenCaughtUpAsync().WaitAsync(Timeout);

        // The follower gave its typed stores up at that record: it holds the leader's
        // bytes, the new member included, and the old store object is nobody's.
        Assert.True(follower.StoreEpoch > epoch);
        Assert.Equal("v2", follower.CommittedLayout);
        await using var cold = ChangelogStorage.Follow(log);
        await cold.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.Equal(cold.Pipeline("p").DescribeState(), follower.DescribeState());
        Assert.Contains(follower.DescribeState(), line => line.Contains("the member V1 does not model"));
        Assert.True(typed.TryGet(1, out _));
        Assert.False(typed.TryGet(3, out _));

        // No store of the old layout can be created on it until it is told the new one.
        Assert.Throws<InvalidOperationException>(() => follower.CreateValueStore<int, V1>("v"));
        follower.UseLayout("v2");
        var rebuilt = follower.CreateValueStore<int, V2>("v");
        Assert.True(rebuilt.TryGet(2, out var two));
        Assert.Equal("the member V1 does not model", two!.Extra);
    }

    [Fact]
    public async Task An_announcement_of_another_layout_releases_a_followers_stores_as_the_bytes_they_were_made_of()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var leaderStorage = await ChangelogStorage.OpenAsync(log);
        var leader = await LeaderAsync(leaderStorage, "wide");
        var values = leader.CreateValueStore<int, V2>("v");
        leader.Recover();
        values.Put(1, new V2(1, "one", "kept"));
        await leader.CommitAsync(Offsets(1));
        var committed = leader.DescribeState();

        // A replace that keeps state announces the next layout before its circuit starts...
        leader.ReleaseStores();
        leader.UseLayout("narrow");
        await leader.CommitLayoutAsync();

        // ...a follower builds that layout's stores, which do not model Extra...
        await using var followerStorage = ChangelogStorage.Follow(log);
        await followerStorage.WhenCaughtUpAsync().WaitAsync(Timeout);
        var follower = followerStorage.Pipeline("p");
        Assert.Equal("narrow", follower.CommittedLayout);
        follower.UseLayout("narrow");
        var narrow = follower.CreateValueStore<int, V1>("v");
        Assert.True(narrow.TryGet(1, out _));

        // ...and the leader's circuit refuses the state, so the old layout comes back.
        // Nothing was written under "narrow": the follower's stores go back to the very
        // bytes they were made of, not to what their narrower types would write.
        leader.ReleaseStores();
        leader.UseLayout("wide");
        await leader.CommitLayoutAsync();
        await followerStorage.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.Equal("wide", follower.CommittedLayout);
        Assert.Equal(committed, follower.DescribeState());
        follower.UseLayout("wide");
        var back = follower.CreateValueStore<int, V2>("v");
        Assert.True(back.TryGet(1, out var row));
        Assert.Equal("kept", row!.Extra);
    }

    [Fact]
    public async Task Once_the_leader_has_checkpointed_under_a_layout_a_followers_stores_hold_what_the_leaders_do()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var leaderStorage = await ChangelogStorage.OpenAsync(log);
        var leader = await LeaderAsync(leaderStorage, "wide");
        var values = leader.CreateValueStore<int, V2>("v");
        leader.Recover();
        values.Put(1, new V2(1, "one", "kept"));
        await leader.CommitAsync(Offsets(1));

        // The next layout does not model Extra, and this time its circuit runs: it
        // checkpoints, without touching the entry.
        leader.ReleaseStores();
        leader.UseLayout("narrow");
        await leader.CommitLayoutAsync();
        await using var followerStorage = ChangelogStorage.Follow(log);
        await followerStorage.WhenCaughtUpAsync().WaitAsync(Timeout);
        var follower = followerStorage.Pipeline("p");
        follower.UseLayout("narrow");
        follower.CreateValueStore<int, V1>("v");
        var narrow = leader.CreateValueStore<int, V1>("v");
        leader.Recover();
        Assert.True(narrow.TryGet(1, out _));
        await leader.CommitAsync(Offsets(2));
        await followerStorage.WhenCaughtUpAsync().WaitAsync(Timeout);

        // From that checkpoint on the state is what the narrow types hold, on the leader
        // and on the follower alike: released, both give what those types write, and the
        // member they do not model is gone from both. (The follower no longer keeps the
        // bytes it was built from: for an idle pipeline that would be its state twice.)
        leader.ReleaseStores();
        follower.ReleaseStores();
        Assert.Equal(leader.DescribeState(), follower.DescribeState());
        Assert.DoesNotContain(follower.DescribeState(), line => line.Contains("kept"));
    }

    [Fact]
    public async Task A_tombstone_these_types_cannot_read_releases_the_stores_instead_of_failing_the_follower()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        var leaderStorage = await ChangelogStorage.OpenAsync(log);
        var leader = await LeaderAsync(leaderStorage, "a");
        var values = leader.CreateValueStore<int, V1>("v");
        leader.Recover();
        values.Put(1, new V1(1, "one"));
        await leader.CommitAsync(Offsets(1));
        await leaderStorage.DisposeAsync();

        await using var followerStorage = ChangelogStorage.Follow(log);
        await followerStorage.WhenCaughtUpAsync().WaitAsync(Timeout);
        var follower = followerStorage.Pipeline("p");
        follower.UseLayout("a");
        follower.CreateValueStore<int, V1>("v");
        var epoch = follower.StoreEpoch;
        var before = follower.DescribeState();

        // A tombstone carries no value, so no layout to tell it by. One whose key these
        // types cannot read was written by other types — here by hand, ahead of anything
        // that says so, as compaction can leave it.
        await using (var writer = await log.AcquireWriterAsync())
        {
            using var transaction = writer.BeginTransaction();
            transaction.AppendTombstone(0, "p\01\0v\0{\"Text\":\"k\",\"Flag\":true}"u8);
            await transaction.CommitAsync();
        }
        await followerStorage.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.True(follower.StoreEpoch > epoch);
        Assert.Equal(before, follower.DescribeState());
        Assert.Throws<InvalidOperationException>(() => follower.CreateValueStore<int, V1>("v"));
    }
}
