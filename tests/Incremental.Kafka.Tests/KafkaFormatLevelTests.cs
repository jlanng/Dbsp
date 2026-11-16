using Incremental.Storage;
using Incremental.Tables;

namespace Incremental.Kafka.Tests;

/// <summary>
/// Format levels on the Kafka logs (<see cref="PersistedFormat"/>): the writer announces
/// the level it writes in the leader topic, a new writer goes on at it, a raise is durable
/// before anything is written under it, and a node that does not read a level is refused
/// before it has fenced anyone. The proving level stands in for a change of the format
/// byte of the state log's values and the table logs' records.
/// </summary>
public class KafkaFormatLevelTests
{
    private static readonly TimeSpan Timeout = KafkaEnv.Timeout;

    private sealed record Row(int Id, string Name);

    private static KafkaLogOptions With(KafkaEnv env, PersistedFormat formats, string node)
    {
        var options = env.Options(nodeId: node, heartbeat: TimeSpan.FromMilliseconds(250));
        options.Formats = formats;
        return options;
    }

    private static TableVersion<int, Row> Version(long version, params (int Id, string Name)[] rows) =>
        new(version, [.. rows.Select(r => new TableChange<int, Row>(r.Id, null, new Row(r.Id, r.Name)))], new Dictionary<int, long>());

    private static async Task<Dictionary<int, Row>> RestoreAsync(ITableLog<int, Row> log)
    {
        var rows = new Dictionary<int, Row>();
        await log.RestoreAsync((key, row) => rows[key] = row).AsTask().WaitAsync(Timeout);
        return rows;
    }

    [Fact]
    public async Task A_writer_goes_on_at_the_level_the_last_one_announced_and_a_raise_outlives_it()
    {
        await using var env = KafkaEnv.Create();

        // A deployment's first writer writes the base level, whatever it reads.
        var first = await env.ConnectAsync(With(env, new PersistedFormat(), "a"));
        await first.AcquireWriterAsync().WaitAsync(Timeout);
        Assert.Equal(FormatLevel.Base, first.Formats.Writes);

        // A node of the base level takes over from it and is taken over from: nothing
        // above its level was written.
        var old = await env.ConnectAsync(With(env, new PersistedFormat(reads: FormatLevel.Base), "old"));
        await old.AcquireWriterAsync().WaitAsync(Timeout);
        Assert.Equal(FormatLevel.Base, old.Formats.Writes);
        await Assert.ThrowsAsync<FormatLevelException>(() => old.RaiseFormatAsync(FormatLevel.PartitionTimes)).WaitAsync(Timeout);

        var second = await env.ConnectAsync(With(env, new PersistedFormat(), "b"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => second.RaiseFormatAsync(FormatLevel.PartitionTimes)).WaitAsync(Timeout);
        await second.AcquireWriterAsync().WaitAsync(Timeout);
        Assert.Equal(FormatLevel.Base, second.Formats.Writes);

        await second.RaiseFormatAsync(FormatLevel.PartitionTimes).WaitAsync(Timeout);
        Assert.Equal(FormatLevel.PartitionTimes, second.Formats.Writes);
        Assert.Equal(FormatLevel.PartitionTimes, second.FormatLevelSeen);

        // The next writer has never heard this one: it reads the level with the term.
        var third = await env.ConnectAsync(With(env, new PersistedFormat(), "c"));
        await third.AcquireWriterAsync().WaitAsync(Timeout);
        Assert.Equal(FormatLevel.PartitionTimes, third.Formats.Writes);
        await second.WhenFenced.WaitAsync(Timeout);

        // A node from before the level is refused before it fences: the writer it would
        // have replaced still commits.
        var late = await env.ConnectAsync(With(env, new PersistedFormat(reads: FormatLevel.Base), "late"));
        var refused = await Assert.ThrowsAsync<FormatLevelException>(() => late.AcquireWriterAsync()).WaitAsync(Timeout);
        Assert.Equal((FormatLevel.PartitionTimes, FormatLevel.Base), (refused.Level, refused.Reads));
        Assert.False(late.IsWriter);
        var table = await third.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        await table.AppendAsync(Version(1, (1, "one"))).AsTask().WaitAsync(Timeout);
        Assert.True(third.IsWriter);
        Assert.False(third.IsFenced);
    }

    [Fact]
    public async Task A_change_of_the_format_byte_is_written_only_once_raised_and_read_only_by_a_node_of_the_level()
    {
        await using var env = KafkaEnv.Create();
        var writer = await env.ConnectAsync(With(env, PersistedFormat.ForTesting(FormatLevel.Proving), "new"));
        await writer.AcquireWriterAsync().WaitAsync(Timeout);
        var table = await writer.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        await using var storage = ChangelogStorage.Follow(writer.State, new ChangelogStorageOptions { Formats = writer.Formats });
        await storage.PromoteAsync().WaitAsync(Timeout);
        var state = storage.Pipeline("p");
        var counts = state.CreateValueStore<string, long>("counts");

        // Held at the level the older node reads: the older node restores the table and
        // follows the state.
        await table.AppendAsync(Version(1, (1, "one"), (2, "two"))).AsTask().WaitAsync(Timeout);
        counts.Put("a", 1);
        state.Commit(new Dictionary<string, long> { ["in"] = 1 });

        var old = await env.ConnectAsync(With(env, PersistedFormat.ForTesting(FormatLevel.Current), "old"));
        var oldTable = await old.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        Assert.Equal(2, (await RestoreAsync(oldTable)).Count);
        await using (var follower = ChangelogStorage.Follow(old.State, new ChangelogStorageOptions { Formats = old.Formats }))
        {
            await follower.WhenCaughtUpAsync().WaitAsync(Timeout);
            Assert.Equal(new Dictionary<string, long> { ["in"] = 1 }, follower.Pipeline("p").CommittedOffsets);
        }

        // Raised: every record from here carries the new byte.
        await writer.RaiseFormatAsync(FormatLevel.Proving).WaitAsync(Timeout);
        await table.AppendAsync(Version(2, (3, "three"))).AsTask().WaitAsync(Timeout);
        counts.Put("b", 2);
        state.Commit(new Dictionary<string, long> { ["in"] = 2 });
        var values = new List<byte>();
        for (var partition = 0; partition < writer.State.PartitionCount; partition++)
            values.AddRange((await KafkaEnv.ReadToEndAsync(writer.State, partition)).Where(r => !r.IsTombstone).Select(r => r.Value.Span[0]));
        Assert.Contains(FormatLevel.EnvelopeByte, values);
        Assert.Contains(ChangelogStorageProvider.FormatSerializer, values);

        // A node of the level reads both; the older one reads neither log any more, and
        // cannot take them.
        var peer = await env.ConnectAsync(With(env, PersistedFormat.ForTesting(FormatLevel.Proving), "peer"));
        var peerTable = await peer.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        Assert.Equal(3, (await RestoreAsync(peerTable)).Count);
        await using (var follower = ChangelogStorage.Follow(peer.State, new ChangelogStorageOptions { Formats = peer.Formats }))
        {
            await follower.WhenCaughtUpAsync().WaitAsync(Timeout);
            Assert.Equal(new Dictionary<string, long> { ["in"] = 2 }, follower.Pipeline("p").CommittedOffsets);
        }

        var stale = await env.ConnectAsync(With(env, PersistedFormat.ForTesting(FormatLevel.Current), "stale"));
        await Assert.ThrowsAsync<FormatLevelException>(() => stale.OpenTableLogAsync<int, Row>("rows")).WaitAsync(Timeout);

        // And the one that was following when the level was raised stops at the first
        // record of it.
        await Assert.ThrowsAsync<FormatLevelException>(() => RestoreAsync(oldTable)).WaitAsync(Timeout);
        await using (var follower = ChangelogStorage.Follow(stale.State, new ChangelogStorageOptions { Formats = stale.Formats }))
        {
            var failed = await Assert.ThrowsAsync<InvalidOperationException>(() => follower.WhenCaughtUpAsync()).WaitAsync(Timeout);
            Assert.IsType<FormatLevelException>(failed.InnerException);
        }
        await Assert.ThrowsAsync<FormatLevelException>(() => stale.AcquireWriterAsync()).WaitAsync(Timeout);
        Assert.True(writer.IsWriter);
    }
}
