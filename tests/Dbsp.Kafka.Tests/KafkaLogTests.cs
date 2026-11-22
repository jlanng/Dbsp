using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Dbsp.Storage;
using Dbsp.Tables;

namespace Dbsp.Kafka.Tests;

/// <summary>
/// The log writer and the state changelog against a real broker: topic set-up, the
/// changelog's read contract, group commit, an aborted commit, and fencing.
/// </summary>
public class KafkaLogTests(ITestOutputHelper output)
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
        // A topic created a moment ago can be unknown to the broker this client asks,
        // on a busy broker for a second or two: the test is about what is created, not when.
        DescribeTopicsResult? described = null;
        for (var deadline = DateTime.UtcNow + Timeout; described is null;)
        {
            try
            {
                described = await admin.DescribeTopicsAsync(TopicCollection.OfTopicNames([log.State.Topic, env.Prefix + ".leader"]));
            }
            catch (DescribeTopicsException) when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(100);
            }
        }
        Assert.Equal(5, described.TopicDescriptions[0].Partitions.Count);
        Assert.Single(described.TopicDescriptions[1].Partitions);
        var configs = await admin.DescribeConfigsAsync([new ConfigResource { Type = ResourceType.Topic, Name = log.State.Topic }]);
        Assert.Equal("compact", configs[0].Entries["cleanup.policy"].Value);
        Assert.Equal("86400000", configs[0].Entries["delete.retention.ms"].Value);

        // Committed state never goes on from a replica that was behind, whatever the
        // cluster's own default says (MSK's says it may): each topic says so itself, as
        // a setting of the topic's and not one it inherits.
        foreach (var topic in new[] { log.State.Topic, env.Prefix + ".leader" })
        {
            var own = (await admin.DescribeConfigsAsync([new ConfigResource { Type = ResourceType.Topic, Name = topic }]))[0].Entries[KafkaLog.UncleanElection];
            Assert.Equal("false", own.Value);
            Assert.Equal(ConfigSource.DynamicTopicConfig, own.Source);
        }

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

    /// <summary>
    /// The transaction timeout is how long a leader rides out brokers that cannot commit
    /// (durability-and-standby.md, <i>A leader does not outlive its transaction
    /// timeout</i>), and the brokers cap it (<c>transaction.max.timeout.ms</c>, fifteen
    /// minutes unless the cluster says otherwise). The default must be under that cap on
    /// a cluster nobody configured, and a value over it must be refused as what it is.
    /// The client reports the refusal as a fatal error, which this log would otherwise
    /// read as a fence: "another process has taken the log over", from a cluster on
    /// which no process can — and for every later attempt, since a client that has
    /// failed fatally answers every call with its first error.
    /// </summary>
    [Fact]
    public async Task A_transaction_timeout_the_brokers_refuse_is_said_by_name_and_is_not_a_fence()
    {
        await using var env = KafkaEnv.Create();
        Assert.Equal(TimeSpan.FromMinutes(5), new KafkaLogOptions { BootstrapServers = KafkaEnv.Bootstrap }.TransactionTimeout);

        var log = await env.ConnectAsync(env.Options(transactionTimeout: TimeSpan.FromHours(1)));
        for (var attempt = 0; attempt < 2; attempt++)
        {
            var failure = await Assert.ThrowsAsync<KafkaLogException>(() => log.AcquireWriterAsync().WaitAsync(Timeout));
            Assert.Contains("transaction.max.timeout.ms", failure.Message);
            Assert.Contains("01:00:00", failure.Message);
            Assert.False(log.IsFenced, $"a refused setting was read as a fence on attempt {attempt + 1}: {log.FencedBy?.Message}");
            Assert.False(log.IsWriter);
        }
        Assert.Equal(2, log.WriterResets);

        // Nothing was taken: a log with a timeout the brokers allow acquires the id as
        // its first writer, and the default is such a timeout.
        var other = await env.ConnectWriterAsync(env.Options(nodeId: "other"));
        Assert.Equal(1, other.Term);
        await CommitAsync(await other.State.AcquireWriterAsync(), 0, ("k", (string?)"v"));

        // And nothing is taken from a writer that is there: the brokers refuse the
        // timeout before they touch the id, so a node configured with one they will not
        // have cannot fence the leader it tries to replace.
        await Assert.ThrowsAsync<KafkaLogException>(() => log.AcquireWriterAsync().WaitAsync(Timeout));
        await CommitAsync(await other.State.AcquireWriterAsync(), 0, ("k", (string?)"again"));
        Assert.False(other.IsFenced);
        Assert.Equal(3, log.WriterResets);
    }

    [Fact]
    public async Task State_topic_that_is_not_compacted_is_refused()
    {
        await using var env = KafkaEnv.Create();
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = KafkaEnv.Bootstrap }).Build())
            await admin.CreateTopicsAsync([new TopicSpecification { Name = env.Prefix + ".state", NumPartitions = 3, ReplicationFactor = KafkaEnv.Replication }]);

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
        KafkaEnv.Measured(output, $"group commit: {commits} commits from {Committers} concurrent committers in {transactions} Kafka transactions");

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

    /// <summary>
    /// The broker, not the client, refuses one record: under the client's limit, over the
    /// topic's. The refusal arrives while a neighbour in the same transaction is still
    /// producing, and from then on every Produce fails with "erroneous state". That says
    /// nothing about the neighbour: only the commit whose record was refused may fail.
    /// </summary>
    [Fact]
    public async Task A_record_the_broker_refuses_does_not_fail_the_neighbour_that_was_producing_when_the_news_came()
    {
        await using var env = KafkaEnv.Create();
        var log = await env.ConnectWriterAsync(env.Options(
            linger: TimeSpan.FromMilliseconds(300), maxAbortRetries: 1,
            stateTopic: new Dictionary<string, string> { ["max.message.bytes"] = "60000" }));

        var refused = new LogCommit();
        var huge = new byte[500_000];
        new Random(11).NextBytes(huge); // incompressible: the batch stays over the topic's limit
        refused.Add(new TopicPartition(log.State.Topic, 0), Bytes("huge"), huge);

        // Large enough to be still producing when the broker's answer to the first arrives.
        var innocent = new LogCommit();
        var value = Bytes("v");
        for (var i = 0; i < 400_000; i++)
            innocent.Add(new TopicPartition(log.State.Topic, 1), Bytes("k" + i), value);

        var refusedTask = log.CommitAsync(refused);
        var innocentTask = log.CommitAsync(innocent);

        var failure = await Assert.ThrowsAsync<KafkaLogException>(() => refusedTask).WaitAsync(Timeout * 2);
        Assert.IsNotType<KafkaLogFencedException>(failure);
        Assert.Contains("too large", failure.Message, StringComparison.OrdinalIgnoreCase);
        await innocentTask.WaitAsync(Timeout * 2);
        Assert.True(log.TransactionsAborted >= 2, "the two commits never shared a transaction, so this proved nothing");
        Assert.True(log.IsWriter);

        var successor = await env.ConnectWriterAsync(env.Options(
            stateTopic: new Dictionary<string, string> { ["max.message.bytes"] = "60000" }));
        Assert.Equal(400_000, (await KafkaEnv.ReadToEndAsync(successor.State, 1)).Count);
        Assert.Empty(await KafkaEnv.ReadToEndAsync(successor.State, 0));
    }

    [Fact]
    public void Only_an_error_about_the_record_itself_names_a_culprit()
    {
        static KafkaException Error(ErrorCode code) => new(new Error(code));
        Assert.True(KafkaLog.IsRecordRefusal(Error(ErrorCode.MsgSizeTooLarge)));
        Assert.True(KafkaLog.IsRecordRefusal(Error(ErrorCode.Local_UnknownPartition)));
        Assert.False(KafkaLog.IsRecordRefusal(Error(ErrorCode.Local_State)));
        Assert.False(KafkaLog.IsRecordRefusal(Error(ErrorCode.Local_MsgTimedOut)));
    }

    /// <summary>"Failed to query partition leaders: No leaders found" is what the client's
    /// ListOffsets and DeleteRecords say of a partition whose leader its metadata does not
    /// hold yet. It passes; it must be retried, not thrown into a takeover.</summary>
    [Fact]
    public void A_missing_partition_leader_is_a_transient_admin_error()
    {
        Assert.True(KafkaLog.IsTransient(new KafkaException(new Error(ErrorCode.Local_NoEnt, "Failed to query partition leaders: No leaders found"))));
        Assert.True(KafkaLog.IsTransient(new KafkaException(new Error(ErrorCode.NotLeaderForPartition))));
        Assert.True(KafkaLog.IsTransient(new KafkaException(new Error(ErrorCode.LeaderNotAvailable))));
        Assert.False(KafkaLog.IsTransient(new KafkaException(new Error(ErrorCode.TopicAuthorizationFailed))));
        Assert.False(KafkaLog.IsTransient(new KafkaException(new Error(ErrorCode.InvalidConfig))));
    }

    /// <summary>Tables opened the moment the log exists, many at once, on one broker: each
    /// open creates two topics and at once asks for their offsets. Before
    /// <c>Local_NoEnt</c> was retried a good share of these failed.</summary>
    [Fact]
    public async Task Many_tables_opened_at_once_on_new_topics_all_open()
    {
        await using var env = KafkaEnv.Create();
        var log = await env.ConnectWriterAsync(env.Options(rowPartitions: 6));
        var opened = await Task.WhenAll(Enumerable.Range(0, 32).Select(i =>
            Task.Run(() => log.OpenTableLogAsync<int, Row>("t" + i)))).WaitAsync(Timeout * 2);
        Assert.Equal(32, opened.Select(t => t.Epoch).Distinct().Count());
        foreach (var table in opened)
            Assert.Equal(0, await table.LastVersionAsync());
    }

    [Fact]
    public async Task A_transaction_takes_waiting_commits_only_up_to_MaxTransactionRecords()
    {
        await using var env = KafkaEnv.Create();
        // The linger makes sure all thirty commits are waiting when the first transaction begins.
        var log = await env.ConnectWriterAsync(env.Options(linger: TimeSpan.FromMilliseconds(300), maxTransactionRecords: 10));
        var transactionsBefore = log.TransactionsCommitted;

        var commits = new List<Task>();
        for (var c = 0; c < 30; c++)
        {
            var commit = new LogCommit();
            for (var r = 0; r < 4; r++)
                commit.Add(new TopicPartition(log.State.Topic, 0), Bytes($"c{c}/r{r}"), Bytes("v"));
            commits.Add(log.CommitAsync(commit));
        }
        await Task.WhenAll(commits).WaitAsync(Timeout);

        // Four records a commit, ten a transaction: a transaction is full at three commits.
        var transactions = log.TransactionsCommitted - transactionsBefore;
        Assert.InRange(transactions, 10, 30);
        Assert.Equal(0, log.TransactionsAborted);

        // And a single commit over the cap is not split: it rides alone, whole.
        var large = new LogCommit();
        for (var r = 0; r < 55; r++)
            large.Add(new TopicPartition(log.State.Topic, 1), Bytes($"large/{r}"), Bytes("v"));
        transactionsBefore = log.TransactionsCommitted;
        await log.CommitAsync(large).WaitAsync(Timeout);
        Assert.Equal(1, log.TransactionsCommitted - transactionsBefore);
    }

    /// <summary>
    /// A follower that has stopped applying, under a writer that has not stopped writing:
    /// what it holds is two batches in its channel and the one in its hand on the managed
    /// side, and the client's fetch buffer on the native side — whatever the topic holds.
    /// </summary>
    [Fact]
    public async Task A_stalled_follower_holds_a_bounded_amount_of_the_log()
    {
        const int Commits = 60, PerCommit = 1000, PayloadBytes = 300, BufferKilobytes = 1024;
        await using var env = KafkaEnv.Create();
        var active = await env.ConnectWriterAsync();

        long peak = 0;
        var samples = 0;
        var standby = await env.ConnectAsync(env.Options(
            fetchBufferKilobytes: BufferKilobytes,
            clientConfig: new Dictionary<string, string> { ["statistics.interval.ms"] = "100" },
            configureConsumer: consumer => consumer.SetStatisticsHandler((_, json) =>
            {
                using var statistics = System.Text.Json.JsonDocument.Parse(json);
                foreach (var t in statistics.RootElement.GetProperty("topics").EnumerateObject())
                    foreach (var p in t.Value.GetProperty("partitions").EnumerateObject())
                        peak = Math.Max(peak, p.Value.GetProperty("fetchq_size").GetInt64());
                Interlocked.Increment(ref samples);
            })));

        var random = new Random(5);
        for (var c = 0; c < Commits; c++)
        {
            var commit = new LogCommit();
            for (var r = 0; r < PerCommit; r++)
            {
                var payload = new byte[PayloadBytes];
                random.NextBytes(payload);
                commit.Add(new TopicPartition(active.State.Topic, 0), Bytes($"c{c}/r{r}"), payload);
            }
            await active.CommitAsync(commit).WaitAsync(Timeout);
        }
        var topicBytes = (long)Commits * PerCommit * PayloadBytes;
        var end = 0L;
        await KafkaEnv.EventuallyAsync(
            async () => (end = await standby.EndOffsetAsync(new TopicPartition(standby.State.Topic, 0), default)) >= Commits * (PerCommit + 1),
            "every commit is readable");

        using var stop = new CancellationTokenSource(Timeout);
        await using var batches = standby.ReadBatchesAsync(new TopicPartition(standby.State.Topic, 0), 0, stop.Token).GetAsyncEnumerator();
        Assert.True(await batches.MoveNextAsync());
        var read = (long)batches.Current.Records.Count;

        // Stalled with one batch in hand. The reader thread fills the channel and stops.
        await Task.Delay(TimeSpan.FromSeconds(2));
        var taken = standby.ReaderRecordsTaken;
        Assert.InRange(taken, read, 4 * 1024); // in hand, two in the channel, one waiting to be written
        await Task.Delay(TimeSpan.FromSeconds(1));
        Assert.Equal(taken, standby.ReaderRecordsTaken);

        // Released: everything arrives, once.
        while (batches.Current.NextOffset < end)
        {
            Assert.True(await batches.MoveNextAsync());
            read += batches.Current.Records.Count;
        }
        Assert.Equal(Commits * PerCommit, read);

        // The client's limit plus the fetch (a megabyte) in flight when it was reached.
        var bound = (BufferKilobytes + 1024 + 512) * 1024L;
        Assert.True(Volatile.Read(ref samples) > 10, "the client reported no statistics");
        Assert.InRange(peak, 1, bound);
        Assert.True(topicBytes > 5 * bound, "the topic must dwarf the bound for this to mean anything");
        KafkaEnv.Measured(output, $"stalled follower: topic {topicBytes / 1048576} MB, held {taken} records in managed memory, " +
            $"client buffer limit {BufferKilobytes} KB, peak held by the client {peak / 1024} KB");
    }

    [Fact]
    public async Task A_read_to_the_end_gets_past_an_aborted_transaction_at_the_tail()
    {
        await using var env = KafkaEnv.Create();
        var log = await env.ConnectWriterAsync();
        var writer = await log.State.AcquireWriterAsync();
        await CommitAsync(writer, 0, ("kept", "1"));

        // Records that reached the broker and were then aborted, as the last thing in the
        // partition. (Written with a producer of the test's own, under another
        // transactional id, because the log writer aborts before anything is sent whenever
        // it can.) The end of committed data is now past records no reader is ever handed.
        using (var other = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
        {
            BootstrapServers = KafkaEnv.Bootstrap, TransactionalId = env.Prefix + ".other",
        }).Build())
        {
            other.InitTransactions(Timeout);
            other.BeginTransaction();
            for (var i = 0; i < 3; i++)
                other.Produce(new TopicPartition(log.State.Topic, 0), new Message<byte[], byte[]> { Key = Bytes($"aborted{i}"), Value = Bytes("x") });
            other.Flush(Timeout);
            other.AbortTransaction(Timeout);
        }

        var successor = await env.ConnectWriterAsync();
        var end = await successor.State.EndOffsetAsync(0);
        var records = await KafkaEnv.ReadToEndAsync(successor.State, 0); // returns only when the position reaches the end
        Assert.Equal(["kept"], records.Select(r => Text(r.Key)));
        Assert.Equal(records[0].Offset + 6, end);                         // marker, three aborted records, marker

        // The leader term is read the same way, across the same kind of tail.
        Assert.Equal(2, successor.Term);
    }

    [Fact]
    public async Task A_follower_at_the_end_knows_it_without_waiting_for_an_empty_fetch()
    {
        // After a commit the follower holds the last record, but its position is "at the
        // end" only once it is past the transaction's marker. The client says so on an
        // empty fetch — fetch.wait.max.ms, half a second, later. A takeover waits on
        // exactly this, so the reader works it out from the record's own fetch.
        await using var env = KafkaEnv.Create();
        var active = await env.ConnectWriterAsync();
        var writer = await active.State.AcquireWriterAsync();
        var standby = await env.ConnectAsync();

        using var stop = new CancellationTokenSource(Timeout);
        var position = 0L;
        var following = Task.Run(async () =>
        {
            await foreach (var batch in standby.State.ReadAsync(0, 0, stop.Token))
                Volatile.Write(ref position, batch.NextOffset);
        });
        await CommitAsync(writer, 0, ("warm-up", "v"));
        await KafkaEnv.EventuallyAsync(() => Task.FromResult(Volatile.Read(ref position) >= 2), "the follower is at the end");
        await Task.Delay(700); // let the follower settle into a long poll

        var waits = new List<double>();
        for (var i = 0; i < 7; i++)
        {
            await CommitAsync(writer, 0, ($"k{i}", "v"));
            var clock = System.Diagnostics.Stopwatch.StartNew();
            var end = 2 * (i + 2);                    // each transaction: one record, one marker
            while (Volatile.Read(ref position) < end && clock.Elapsed < Timeout)
                await Task.Delay(1);
            waits.Add(clock.Elapsed.TotalMilliseconds);
            await Task.Delay(50);
        }
        waits.Sort();
        KafkaEnv.Measured(output, $"follower reaches the end after a commit returns: median {waits[3]:F0} ms, worst {waits[^1]:F0} ms");
        Assert.True(waits[3] < 250, $"the follower took {waits[3]:F0} ms (median) to stand at the end; an empty fetch takes 500");

        stop.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => following).WaitAsync(Timeout);
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

    /// <summary>
    /// A follower that has read every committed record stands in front of whatever an
    /// aborted transaction left behind them, and has to learn that this is the end: a
    /// takeover waits for its followers to reach the end of the state before it promotes
    /// it. The client tells an application so with an end-of-partition event — after a
    /// fetch that came back empty, which the broker holds for <c>fetch.wait.max.ms</c>,
    /// half a second by default. A leader killed inside a transaction that had written
    /// state cost every takeover that wait: fence and storage stage took 507 to 510 ms
    /// together in six of six, however quick the fence. The reader now also reports
    /// where the client itself stands when nothing arrives, which is behind the aborted
    /// records and their marker as soon as it has read them. Here the empty fetch is
    /// held for four seconds, so that only the position can bring the reader to the end
    /// in the two the test allows.
    /// </summary>
    [Fact]
    public async Task A_follower_reaches_the_end_behind_an_aborted_transaction_without_waiting_for_an_empty_fetch()
    {
        await using var env = KafkaEnv.Create();
        var log = await env.ConnectWriterAsync();
        var writer = await log.State.AcquireWriterAsync();
        await CommitAsync(writer, 0, ("kept", "1"));

        var held = TimeSpan.FromSeconds(4);
        var follower = await env.ConnectAsync(env.Options(
            clientConfig: new Dictionary<string, string> { ["fetch.wait.max.ms"] = ((int)held.TotalMilliseconds).ToString() }));
        using var stop = new CancellationTokenSource(Timeout);
        var position = 0L;
        var fell = false;
        var seen = new List<string>();
        var following = Task.Run(async () =>
        {
            await foreach (var batch in follower.State.ReadAsync(0, 0, stop.Token))
            {
                lock (seen)
                    seen.AddRange(batch.Records.Select(r => Text(r.Key)));
                fell |= batch.NextOffset < Volatile.Read(ref position);
                Volatile.Write(ref position, batch.NextOffset);
            }
        });
        var committed = 0L;
        await KafkaEnv.EventuallyAsync(async () => (committed = await log.State.EndOffsetAsync(0)) > 0 && Volatile.Read(ref position) >= committed, "the follower at the end of what is committed");

        // Three records that reach the broker and are aborted, behind the follower.
        using (var other = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
        {
            BootstrapServers = KafkaEnv.Bootstrap, TransactionalId = env.Prefix + ".other",
        }).Build())
        {
            other.InitTransactions(Timeout);
            other.BeginTransaction();
            for (var i = 0; i < 3; i++)
                other.Produce(new TopicPartition(log.State.Topic, 0), new Message<byte[], byte[]> { Key = Bytes($"aborted{i}"), Value = Bytes("x") });
            other.Flush(Timeout);
            other.AbortTransaction(Timeout);
        }
        var end = 0L;
        await KafkaEnv.EventuallyAsync(async () => (end = await log.State.EndOffsetAsync(0)) == committed + 4, "the end of committed data past the aborted records and their marker");

        var clock = System.Diagnostics.Stopwatch.StartNew();
        while (Volatile.Read(ref position) < end && clock.Elapsed < held)
            await Task.Delay(10);
        clock.Stop();
        Assert.True(Volatile.Read(ref position) >= end && clock.Elapsed < held / 2,
            $"the follower stood at {Volatile.Read(ref position)} of {end} after {clock.ElapsedMilliseconds} ms; an empty fetch is held for {held.TotalMilliseconds:F0} ms here, and the follower must not need one");
        lock (seen)
            Assert.Equal(["kept"], seen);

        // And it stands at the end, not past it — a position is a promise about every
        // offset below it — and has never gone back.
        await Task.Delay(300);
        Assert.Equal(end, Volatile.Read(ref position));
        Assert.False(fell, "a position the follower reported was below one it had reported before");
        KafkaEnv.Measured(output, $"follower behind an aborted transaction: at the end {clock.ElapsedMilliseconds} ms after the end had moved (an empty fetch is held {held.TotalMilliseconds:F0} ms)");
        stop.Cancel();
        await following.ContinueWith(_ => { });
    }

    private static TableVersion<int, Row> Version(long version, params TableChange<int, Row>[] changes) =>
        new(version, changes, new Dictionary<int, long>());

    /// <summary>The log's one consumer for short reads is what a takeover reads each
    /// table's head with, and what it has learnt of the topics keeps those reads short.
    /// A read whose caller stops waiting — a status that gives a table's lag two seconds
    /// — must not cost the next reads that: the consumer is kept. One that failed is not.</summary>
    [Fact]
    public async Task A_short_read_its_caller_calls_off_keeps_the_shared_reader_and_one_that_fails_does_not()
    {
        await using var env = KafkaEnv.Create();
        var log = await env.ConnectWriterAsync();

        // The writer's term is committed, and the end of committed data moves past it
        // when the broker has written the transaction's marker — a moment after the
        // commit returned (KafkaStateChangelog.EndOffsetAsync says so). Asked at once the
        // end was still 0 on a busy runner, and with the broker held to a third of a
        // processor in 5 of 40 tries.
        var end = 0L;
        await KafkaEnv.EventuallyAsync(async () => (end = await log.EndOffsetAsync(log.LeaderPartition, default)) > 0, "the writer's term is in the leader topic");

        int Records(IConsumer<byte[], byte[]> consumer, CancellationToken token)
        {
            var seen = 0;
            log.AssignForShortRead(consumer, new TopicPartitionOffset(log.LeaderPartition, 0));
            log.ReadTo(consumer, end, _ => { seen++; }, token);
            return seen;
        }

        var all = await log.ShortReadAsync(consumer => Records(consumer, default), default).WaitAsync(Timeout);
        Assert.True(all > 0);
        Assert.Equal(1, log.ShortReadersCreated);

        // Called off in the middle: assigned, and stopped at the read's first look.
        using var off = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => log.ShortReadAsync(consumer =>
        {
            log.AssignForShortRead(consumer, new TopicPartitionOffset(log.LeaderPartition, 0));
            off.Cancel();
            log.ReadTo(consumer, end, _ => { }, off.Token);
            return 0;
        }, off.Token)).WaitAsync(Timeout);

        // The same consumer serves the next read, and reads what the first one read.
        Assert.Equal(all, await log.ShortReadAsync(consumer => Records(consumer, default), default).WaitAsync(Timeout));
        Assert.Equal(1, log.ShortReadersCreated);

        // A read that failed leaves a consumer in a state nobody knows: the next gets a new one.
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => log.ShortReadAsync<int>(_ => throw new InvalidOperationException("the read broke"), default)).WaitAsync(Timeout);
        Assert.Equal(all, await log.ShortReadAsync(consumer => Records(consumer, default), default).WaitAsync(Timeout));
        Assert.Equal(2, log.ShortReadersCreated);
    }

    /// <summary>
    /// Before the short reader is assigned to a topic it has not met, the topic's metadata
    /// is asked for on its handle (or the running consumer waits a second for it). That
    /// question was put through an admin client made for it and disposed at once — and
    /// disposing an admin client waits for its poll loop, which looks up every 100 ms. So
    /// every topic cost a tenth of a second the first time, one read at a time: nothing
    /// for a standby whose statistics had read each table's head long before, and a
    /// tenth of a second a table for a takeover whose short reader was new — replaced
    /// after a read that failed, or not yet asked anything. On a runner that was
    /// 2,164 ms of table stage for 21 tables, against 110 to 190 ms. The client is now
    /// the consumer's for its whole life: one for each consumer, however many topics.
    /// </summary>
    [Fact]
    public async Task A_short_reader_that_meets_many_topics_makes_one_client_to_ask_about_them()
    {
        const int Tables = 12;
        await using var env = KafkaEnv.Create();
        var writer = await env.ConnectWriterAsync();
        for (var t = 0; t < Tables; t++)
        {
            var written = await writer.OpenTableLogAsync<int, Row>("rows" + t).WaitAsync(Timeout);
            await written.AppendAsync(Version(1, new TableChange<int, Row>(1, null, new Row(1, "one"))));
        }

        // A standby opens them: its short reader meets every table's topics for the first time.
        var standby = await env.ConnectAsync();
        var opening = System.Diagnostics.Stopwatch.StartNew();
        var logs = new List<KafkaTableLog<int, Row>>();
        for (var t = 0; t < Tables; t++)
            logs.Add(await standby.OpenTableLogAsync<int, Row>("rows" + t).WaitAsync(Timeout));
        opening.Stop();
        Assert.Equal(1, standby.ShortReadersCreated);
        Assert.Equal(1, standby.ShortReadAdminsCreated);

        // A read fails, the reader is replaced, and the next thing is a takeover's read
        // of every table's head: every topic is new to the reader again.
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => standby.ShortReadAsync<int>(_ => throw new InvalidOperationException("the read broke"), default)).WaitAsync(Timeout);
        var reading = System.Diagnostics.Stopwatch.StartNew();
        foreach (var log in logs)
            Assert.Equal(1, await log.LastVersionAsync().AsTask().WaitAsync(Timeout));
        reading.Stop();
        Assert.Equal(2, standby.ShortReadersCreated);
        Assert.Equal(2, standby.ShortReadAdminsCreated);

        KafkaEnv.Measured(output,
            $"short reads of {Tables} tables whose topics are new to the reader: opened in {opening.ElapsedMilliseconds} ms, " +
            $"last versions after the reader was replaced in {reading.ElapsedMilliseconds} ms (a client per topic cost 100 ms a topic)");
    }

    /// <summary>
    /// Between reads the short reader is assigned to nothing, and what it knows of a
    /// topic's leaders ages. A takeover that follows the loss of a broker reads its
    /// tables' heads the moment the brokers have moved that broker's partitions: a
    /// reader that still believes in the old leader fetches from it first and then waits
    /// out the client's pause after a fetch error (half a second) — the catalog's head
    /// took 0.9 s that way in one takeover of nine with a broker killed beside the leader.
    /// So a read of a topic the reader has met asks for its leaders again when its answer
    /// is over a second old, and not when it is fresh: a takeover's two reads of one
    /// table are one question, and so are a node's reads in the same second.
    /// What moving leaders cost needs brokers that can be killed; this holds the rule.
    /// </summary>
    [Fact]
    public async Task A_short_read_asks_for_a_topics_leaders_again_once_its_answer_is_a_second_old()
    {
        await using var env = KafkaEnv.Create();
        var writer = await env.ConnectWriterAsync();
        var written = await writer.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        await written.AppendAsync(Version(1, new TableChange<int, Row>(1, null, new Row(1, "one"))));

        // "Fresh" is a second on the clock, so the test must know that its own reads
        // were made inside one: on a machine too slow for that it holds the half of
        // the rule that does not depend on it, and says so.
        var standby = await env.ConnectAsync();
        var sinceFirstAsk = System.Diagnostics.Stopwatch.StartNew();
        var log = await standby.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        Assert.Equal(1, await log.LastVersionAsync().AsTask().WaitAsync(Timeout));
        var met = standby.ShortReadLeadersAsked;
        Assert.True(met >= 1, "the reader was not told the leaders of a topic it had not met");

        // At once: what it was told is fresh, and nothing is asked.
        Assert.Equal(1, await log.LastVersionAsync().AsTask().WaitAsync(Timeout));
        Assert.Equal(1, await log.LastVersionAsync().AsTask().WaitAsync(Timeout));
        var inTime = sinceFirstAsk.Elapsed < KafkaLog.ShortReadLeadersFor - TimeSpan.FromMilliseconds(100);
        if (inTime)
            Assert.Equal(met, standby.ShortReadLeadersAsked);

        // Past the age: each topic the read touches is asked about once, and the reads
        // that follow in the same moment ask nothing.
        await Task.Delay(KafkaLog.ShortReadLeadersFor + TimeSpan.FromMilliseconds(200));
        var before = standby.ShortReadLeadersAsked;
        var sinceSecondAsk = System.Diagnostics.Stopwatch.StartNew();
        Assert.Equal(1, await log.LastVersionAsync().AsTask().WaitAsync(Timeout));
        var again = standby.ShortReadLeadersAsked;
        Assert.InRange(again - before, 1, met);
        Assert.Equal(1, await log.LastVersionAsync().AsTask().WaitAsync(Timeout));
        if (sinceSecondAsk.Elapsed < KafkaLog.ShortReadLeadersFor - TimeSpan.FromMilliseconds(100))
            Assert.Equal(again, standby.ShortReadLeadersAsked);
        else
            inTime = false;
        Assert.Equal(1, standby.ShortReadersCreated);
        Assert.Equal(1, standby.ShortReadAdminsCreated);
        if (!inTime)
            output.WriteLine("This machine took over a second for reads that take milliseconds: 'a fresh answer is not asked for again' was not checked.");
    }
}
