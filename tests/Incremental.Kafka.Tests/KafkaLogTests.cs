using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Incremental.Storage;
using Incremental.Tables;

namespace Incremental.Kafka.Tests;

/// <summary>
/// The log writer and the state changelog against a real broker: topic set-up, the
/// changelog's read contract, group commit, an aborted commit, and fencing.
/// </summary>
public class KafkaLogTests
{
    private static readonly TimeSpan Timeout = KafkaEnv.Timeout;

    private sealed record Row(int Id, string Name);

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static string Text(ReadOnlyMemory<byte> bytes) => Encoding.UTF8.GetString(bytes.Span);

    private static async Task CommitAsync(IStateChangelogWriter writer, int partition, params (string Key, string? Value)[] records)
    {
        using var transaction = writer.BeginTransaction();
        foreach (var (key, value) in records)
        {
            if (value is null)
                transaction.AppendTombstone(partition, Bytes(key));
            else
                transaction.Append(partition, Bytes(key), Bytes(value));
        }
        await transaction.CommitAsync().AsTask().WaitAsync(Timeout);
    }

    [Fact]
    public async Task Topics_are_created_compacted_with_the_configured_partitions()
    {
        await using var env = KafkaEnv.Create();
        var log = await env.ConnectAsync(env.Options(statePartitions: 5));
        Assert.Equal(5, log.State.PartitionCount);
        Assert.Equal(env.Prefix + ".state", log.State.Topic);

        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = KafkaEnv.Bootstrap }).Build();
        var described = await admin.DescribeTopicsAsync(TopicCollection.OfTopicNames([log.State.Topic, env.Prefix + ".leader"]));
        Assert.Equal(5, described.TopicDescriptions[0].Partitions.Count);
        Assert.Single(described.TopicDescriptions[1].Partitions);
        var configs = await admin.DescribeConfigsAsync([new ConfigResource { Type = ResourceType.Topic, Name = log.State.Topic }]);
        Assert.Equal("compact", configs[0].Entries["cleanup.policy"].Value);
        Assert.Equal("86400000", configs[0].Entries["delete.retention.ms"].Value);

        // Connecting again finds them as they are.
        var again = await env.ConnectAsync(env.Options(statePartitions: 5));
        Assert.Equal(5, again.State.PartitionCount);
    }

    [Fact]
    public async Task State_topic_with_a_different_partition_count_is_refused()
    {
        await using var env = KafkaEnv.Create();
        await env.ConnectAsync(env.Options(statePartitions: 3));

        var failure = await Assert.ThrowsAsync<KafkaLogException>(() => KafkaLog.ConnectAsync(env.Options(statePartitions: 4)));
        Assert.Contains("3 partition(s)", failure.Message);
        Assert.Contains("configured for 4", failure.Message);
    }

    [Fact]
    public async Task State_topic_that_is_not_compacted_is_refused()
    {
        await using var env = KafkaEnv.Create();
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = KafkaEnv.Bootstrap }).Build())
            await admin.CreateTopicsAsync([new TopicSpecification { Name = env.Prefix + ".state", NumPartitions = 3, ReplicationFactor = 1 }]);

        var failure = await Assert.ThrowsAsync<KafkaLogException>(() => KafkaLog.ConnectAsync(env.Options(statePartitions: 3)));
        Assert.Contains("cleanup.policy=delete", failure.Message);
    }

    [Fact]
    public async Task A_reader_cannot_write()
    {
        await using var env = KafkaEnv.Create();
        var log = await env.ConnectAsync();
        Assert.False(log.IsWriter);
        Assert.Equal(0, log.Term);

        var commit = new LogCommit();
        commit.Add(new TopicPartition(log.State.Topic, 0), Bytes("k"), Bytes("v"));
        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => log.CommitAsync(commit));
        Assert.Contains("not the writer", failure.Message);
    }

    [Fact]
    public async Task Committed_records_are_read_back_in_order_with_tombstones_and_a_position_at_the_end()
    {
        await using var env = KafkaEnv.Create();
        var log = await env.ConnectWriterAsync();
        var writer = await log.State.AcquireWriterAsync();
        Assert.Equal(1, writer.Term);

        await CommitAsync(writer, 1, ("a", "1"), ("b", "2"));
        await CommitAsync(writer, 1, ("a", null), ("c", "3"));
        await CommitAsync(writer, 2, ("z", "26"));

        // A second process takes over: from then on the end offset is exact, and a reader
        // that reaches it has seen everything the first ever committed.
        var successor = await env.ConnectWriterAsync();
        var records = await KafkaEnv.ReadToEndAsync(successor.State, 1);
        Assert.Equal(["a=1", "b=2", "a=<removed>", "c=3"],
            records.Select(r => $"{Text(r.Key)}={(r.IsTombstone ? "<removed>" : Text(r.Value))}"));
        Assert.True(records.Zip(records.Skip(1)).All(pair => pair.First.Offset < pair.Second.Offset));

        // Offsets are not dense: each transaction left a marker behind its records, and the
        // end of committed data is past the last one.
        var end = await successor.State.EndOffsetAsync(1);
        Assert.Equal(records[^1].Offset + 2, end);
        Assert.Equal(4, records[^1].Offset - records[0].Offset); // a,b,marker,a,c
        Assert.Empty(await KafkaEnv.ReadToEndAsync(successor.State, 0));
        Assert.Equal(0, await successor.State.EndOffsetAsync(0));
        Assert.Single(await KafkaEnv.ReadToEndAsync(successor.State, 2));
    }

    [Fact]
    public async Task A_reader_at_the_end_is_handed_later_commits()
    {
        await using var env = KafkaEnv.Create();
        var active = await env.ConnectWriterAsync();
        var writer = await active.State.AcquireWriterAsync();
        var standby = await env.ConnectAsync();

        using var stop = new CancellationTokenSource(Timeout);
        var seen = new List<string>();
        var positions = new List<long>();
        var following = Task.Run(async () =>
        {
            await foreach (var batch in standby.State.ReadAsync(0, 0, stop.Token))
            {
                lock (seen)
                {
                    seen.AddRange(batch.Records.Select(r => Text(r.Key)));
                    positions.Add(batch.NextOffset);
                }
            }
        });

        for (var i = 0; i < 5; i++)
        {
            await CommitAsync(writer, 0, ($"k{i}", "v"));
            var expected = i + 1;
            await KafkaEnv.EventuallyAsync(() => Task.FromResult(Count() == expected), $"the follower has read commit {i}");
        }

        // The follower's position passes the last record's marker without another record
        // arriving: that is what lets a "read to the end" finish.
        var end = 0L;
        await KafkaEnv.EventuallyAsync(async () => (end = await standby.State.EndOffsetAsync(0)) == 10, "the end offset covers five transactions");
        await KafkaEnv.EventuallyAsync(() => Task.FromResult(Last() >= end), "the follower's position reaches the end offset");
        Assert.Equal(["k0", "k1", "k2", "k3", "k4"], seen);

        stop.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => following).WaitAsync(Timeout);

        int Count() { lock (seen) return seen.Count; }
        long Last() { lock (seen) return positions.Count == 0 ? -1 : positions[^1]; }
    }

    [Fact]
    public async Task Concurrent_commits_share_transactions()
    {
        await using var env = KafkaEnv.Create();
        var log = await env.ConnectWriterAsync();
        var writer = await log.State.AcquireWriterAsync();
        var transactionsBefore = log.TransactionsCommitted;
        var commitsBefore = log.CommitsCompleted;

        // 300 circuits committing at once, three rounds each.
        const int Committers = 300, Rounds = 3;
        await Task.WhenAll(Enumerable.Range(0, Committers).Select(c => Task.Run(async () =>
        {
            for (var round = 0; round < Rounds; round++)
                await CommitAsync(writer, c % 3, ($"c{c}/r{round}/a", "1"), ($"c{c}/r{round}/b", "2"));
        }))).WaitAsync(Timeout);

        var commits = log.CommitsCompleted - commitsBefore;
        var transactions = log.TransactionsCommitted - transactionsBefore;
        Assert.Equal(Committers * Rounds, commits);
        Assert.True(transactions * 4 <= commits,
            $"{commits} commits took {transactions} Kafka transactions: they were not batched.");
        Assert.Equal(0, log.TransactionsAborted);

        // Every commit landed whole, and a committer's rounds kept their order.
        var successor = await env.ConnectWriterAsync();
        var keys = new List<string>();
        for (var p = 0; p < 3; p++)
            keys.AddRange((await KafkaEnv.ReadToEndAsync(successor.State, p)).Select(r => Text(r.Key)));
        Assert.Equal(Committers * Rounds * 2, keys.Count);
        Assert.Equal(keys.Count, keys.Distinct().Count());
        for (var c = 0; c < Committers; c++)
        {
            var mine = keys.Where(k => k.StartsWith($"c{c}/", StringComparison.Ordinal)).ToList();
            Assert.Equal(Enumerable.Range(0, Rounds).SelectMany(r => new[] { $"c{c}/r{r}/a", $"c{c}/r{r}/b" }), mine);
        }
    }

    [Fact]
    public async Task A_commit_the_broker_refuses_is_aborted_alone_and_the_writer_carries_on()
    {
        await using var env = KafkaEnv.Create();
        // A linger long enough that the refused commit and its neighbours share a transaction.
        var log = await env.ConnectWriterAsync(env.Options(linger: TimeSpan.FromMilliseconds(300)));
        var writer = await log.State.AcquireWriterAsync();
        var transactionsBefore = log.TransactionsCommitted;

        var before = CommitAsync(writer, 0, ("before", "1"));
        using var poisoned = writer.BeginTransaction();
        poisoned.Append(0, Bytes("poisoned/small"), Bytes("1"));
        poisoned.Append(0, Bytes("poisoned/huge"), new byte[4_000_000]); // over the client's message.max.bytes
        var refused = poisoned.CommitAsync().AsTask();
        var after = CommitAsync(writer, 0, ("after", "2"));

        var failure = await Assert.ThrowsAsync<KafkaLogException>(() => refused).WaitAsync(Timeout);
        Assert.IsNotType<KafkaLogFencedException>(failure);
        Assert.Contains("aborted", failure.Message);
        await before;
        await after;
        Assert.Equal(1, log.TransactionsCommitted - transactionsBefore); // the neighbours rode together
        Assert.Equal(1, log.TransactionsAborted);
        Assert.False(log.IsFenced);
        Assert.True(log.IsWriter);

        // Aborted means nothing of it is readable — not even the record that was fine.
        await CommitAsync(writer, 0, ("later", "3"));
        var successor = await env.ConnectWriterAsync();
        var keys = (await KafkaEnv.ReadToEndAsync(successor.State, 0)).Select(r => Text(r.Key)).ToList();
        Assert.Equal(["after", "before", "later"], keys.Order());
    }

    [Fact]
    public async Task Second_writer_on_the_same_transactional_id_fences_the_first()
    {
        await using var env = KafkaEnv.Create();
        var first = await env.ConnectWriterAsync();
        var firstWriter = await first.State.AcquireWriterAsync();
        var firstTable = await first.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        await CommitAsync(firstWriter, 0, ("committed", "by the first"));
        await firstTable.AppendAsync(Version(1, new TableChange<int, Row>(1, null, new Row(1, "one"))));
        Assert.Equal(1, first.Term);

        // The takeover: nothing but InitTransactions on the shared transactional.id.
        var second = await env.ConnectAsync();
        Assert.False(first.WhenFenced.IsCompleted);
        await second.AcquireWriterAsync().WaitAsync(Timeout);
        Assert.Equal(2, second.Term);
        Assert.True(second.IsWriter);

        // The first writer's next checkpoint commit fails as fenced …
        using (var transaction = firstWriter.BeginTransaction())
        {
            transaction.Append(0, Bytes("zombie"), Bytes("never readable"));
            var fenced = await Assert.ThrowsAsync<ChangelogFencedException>(() => transaction.CommitAsync().AsTask()).WaitAsync(Timeout);
            var inner = Assert.IsType<KafkaLogFencedException>(fenced.InnerException);
            Assert.Contains("taken the log over", inner.Message);

            // How Confluent.Kafka reports it: Local_Fenced.
            var kafka = Assert.IsAssignableFrom<KafkaException>(inner.InnerException);
            Assert.Equal(ErrorCode.Local_Fenced, kafka.Error.Code);
        }
        await first.WhenFenced.WaitAsync(Timeout);
        Assert.True(first.IsFenced);
        Assert.False(first.IsWriter);

        // … and so does its next table version, and every write after that.
        var tableFenced = await Assert.ThrowsAsync<TableLogFencedException>(
            () => firstTable.AppendAsync(Version(2, new TableChange<int, Row>(2, null, new Row(2, "zombie")))).AsTask()).WaitAsync(Timeout);
        Assert.IsType<KafkaLogFencedException>(tableFenced.InnerException);
        using (var transaction = firstWriter.BeginTransaction())
        {
            transaction.Append(0, Bytes("zombie2"), Bytes("x"));
            await Assert.ThrowsAsync<ChangelogFencedException>(() => transaction.CommitAsync().AsTask()).WaitAsync(Timeout);
        }
        await Assert.ThrowsAsync<ChangelogFencedException>(() => first.State.AcquireWriterAsync().AsTask()).WaitAsync(Timeout);

        // The second writes, and a reader sees the first's committed work, the second's,
        // and nothing of the zombie's.
        var secondWriter = await second.State.AcquireWriterAsync();
        Assert.Equal(2, secondWriter.Term);
        await CommitAsync(secondWriter, 0, ("committed2", "by the second"));
        var secondTable = await second.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        Assert.Equal(firstTable.Epoch, secondTable.Epoch);
        Assert.Equal(1, await secondTable.LastVersionAsync());
        await secondTable.AppendAsync(Version(2, new TableChange<int, Row>(3, null, new Row(3, "three"))));

        var third = await env.ConnectWriterAsync();
        Assert.Equal(3, third.Term);
        Assert.Equal(["committed", "committed2"], (await KafkaEnv.ReadToEndAsync(third.State, 0)).Select(r => Text(r.Key)));
        var thirdTable = await third.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        var rows = new Dictionary<int, Row>();
        var head = await thirdTable.RestoreAsync((key, row) => rows.Add(key, row)).AsTask().WaitAsync(Timeout);
        Assert.Equal(2, head.Version);
        Assert.Equal(new Dictionary<int, Row> { [1] = new(1, "one"), [3] = new(3, "three") }, rows);
    }

    private static TableVersion<int, Row> Version(long version, params TableChange<int, Row>[] changes) =>
        new(version, changes, new Dictionary<int, long>());
}
