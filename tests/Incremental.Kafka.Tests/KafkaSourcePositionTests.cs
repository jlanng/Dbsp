using System.Text;
using Confluent.Kafka;
using Incremental.Storage;
using Incremental.Tables;

namespace Incremental.Kafka.Tests;

/// <summary>
/// Where a <see cref="KafkaSource"/> stands when the topic gives it nothing to stamp:
/// offsets with no record behind them, records that come to nothing, a start at the end,
/// transactions that never commit, and a topic that loses its beginning under a running
/// source. Each of these once ended in a position reported lost that was not, or in
/// records skipped without a word.
/// </summary>
public class KafkaSourcePositionTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Timeout = KafkaEnv.Timeout;

    private sealed record Product(int Id, string Name, int Price);
    private sealed record Event(int Id, string Key, int Amount);

    private static Message<byte[], byte[]> Json<TKey, TValue>(TKey key, TValue? value) where TValue : class =>
        new() { Key = KafkaEnv.Json(key), Value = value is null ? null! : KafkaEnv.Json(value) };

    private sealed record Sums(Circuit Circuit, InputStream<Event> Events, InMemoryUpsertSink<string, long> Sink);

    private static Sums SumCircuit(IStorageProvider storage, InMemoryUpsertSink<string, long>? sink = null, bool autoStep = true, int capacity = 8192)
    {
        sink ??= new InMemoryUpsertSink<string, long>();
        var builder = new CircuitBuilder(new CircuitOptions
        {
            Name = "sums", Storage = storage, AutoStep = autoStep, InputChannelCapacity = capacity, MaxStepItemsPerInput = 64,
        });
        var events = builder.Input<Event>("events");
        var totals = events.GroupBy(e => e.Key).Sum(e => (long)e.Amount).ToAcknowledgedOutput();
        var circuit = builder.Build();
        circuit.Start();
        _ = circuit.Handle(totals).DeliverToAsync(sink);
        return new Sums(circuit, events, sink);
    }

    private static Task TotalIsAsync(Sums sums, string key, long total, string what) =>
        KafkaEnv.EventuallyAsync(() => Task.FromResult(sums.Sink.Rows.TryGetValue(key, out var row) && row == total), what);

    // ── offsets that yield nothing ─────────────────────────────────────────────────────

    /// <summary>
    /// The finding as reported: a transactional producer writes one record, so offset 0 is
    /// the record and offset 1 its transaction's marker. The loader reads to offset 2; the
    /// table commits offset 0. Retention then takes the whole (idle) topic. Nothing was
    /// lost — but "committed 0, first retained 2" said otherwise, and with a reload allowed
    /// the table was emptied.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ALoader_RecordsThePositionPastOffsetsThatChangedNothing_SoRetentionCannotOvertakeIt(bool reloadAllowed)
    {
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("products");
        using (var producer = env.TransactionalProducer())
        {
            producer.BeginTransaction();
            producer.Produce(new TopicPartition(topic, 0), Json(1, new Product(1, "one", 10)));
            producer.CommitTransaction(Timeout);
        }

        var table = new SharedTable<int, Product>("products", p => p.Id);
        var options = new KafkaSourceOptions
        {
            BootstrapServers = KafkaEnv.Bootstrap, Topic = topic, CommitIdlePositionAfter = TimeSpan.FromMilliseconds(300),
            ReloadTableWhenPositionLost = reloadAllowed,
        };
        await using (var loader = await KafkaSource.StartTableLoaderAsync(table, options).WaitAsync(Timeout))
        {
            await loader.WhenCaughtUpAsync().WaitAsync(Timeout);
            Assert.Equal(2, loader.Positions[0]);
            Assert.Equal(0, table.CommittedOffsets[0]);   // the record; the marker is not yet covered
            Assert.Equal(1, table.LatestVersion);

            // Idle for the delay: the loader records where it stands, in a version of no changes.
            await KafkaEnv.EventuallyAsync(() => Task.FromResult(table.CommittedOffsets[0] == 1), "the position covers the marker");
            Assert.Equal(2, table.LatestVersion);
            Assert.Equal(1, table.Count);

            // The same for a record that changes nothing: the row already there, again.
            env.ProduceJson(topic, 0, 1, new Product(1, "one", 10));
            await env.FlushAsync();
            await KafkaEnv.EventuallyAsync(() => Task.FromResult(table.CommittedOffsets[0] == 2), "the position covers the record that changed nothing");
            Assert.Equal(3, table.LatestVersion);

            // And then nothing more: standing at the recorded position mints no versions.
            await Task.Delay(TimeSpan.FromSeconds(1));
            Assert.Equal(3, table.LatestVersion);
        }

        // Retention removes everything the idle topic held.
        await KafkaEnv.DeleteRecordsBeforeAsync(topic, 0, 3);

        await using (var loader = await KafkaSource.StartTableLoaderAsync(table, options).WaitAsync(Timeout))
        {
            await loader.WhenCaughtUpAsync().WaitAsync(Timeout);
            Assert.Equal(0, loader.RecordsRead);
            Assert.Equal(1, table.Count);                 // not emptied by a needless reload
            Assert.Equal(3, table.LatestVersion);

            env.ProduceJson(topic, 0, 2, new Product(2, "two", 20));
            await env.FlushAsync();
            await KafkaEnv.EventuallyAsync(() => Task.FromResult(table.Count == 2), "the loader carries on");
        }
    }

    /// <summary>The delay is what keeps a busy transactional topic from costing two
    /// versions a transaction: a marker behind every commit is not worth a version until
    /// the topic has gone quiet.</summary>
    [Fact]
    public async Task ALoader_OnABusyTransactionalTopic_MintsNoVersionsForItsMarkers()
    {
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("products");
        var table = new SharedTable<int, Product>("products", p => p.Id);
        await using var loader = await KafkaSource.StartTableLoaderAsync(table, env.Source(topic)).WaitAsync(Timeout);

        using var producer = env.TransactionalProducer();
        for (var i = 0; i < 12; i++)
        {
            producer.BeginTransaction();
            producer.Produce(new TopicPartition(topic, 0), Json(i, new Product(i, "p" + i, i)));
            producer.CommitTransaction(Timeout);
            await KafkaEnv.EventuallyAsync(() => Task.FromResult(table.Count == i + 1), $"row {i} is loaded");
        }
        await loader.WhenCaughtUpAsync().WaitAsync(Timeout);
        await Task.Delay(TimeSpan.FromSeconds(1));
        Assert.Equal(12, table.LatestVersion);            // one version a transaction, none for the markers
        Assert.Equal(22, table.CommittedOffsets[0]);      // the last record; its marker waits for the delay
        Assert.Equal(24, loader.Positions[0]);
    }

    [Fact]
    public async Task AStreamSource_RecordsThePositionPastTombstonesAndMarkers()
    {
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("events", partitions: 2);
        var storage = new InMemoryCheckpointStorageProvider();

        // Partition 0. Offsets 0-2 events, 3 a tombstone; then a transaction: 4 an event,
        // 5 a tombstone, 6 its marker. Partition 1, no transactions: an event, then two
        // tombstones — records, and nothing to hand on.
        for (var i = 0; i < 3; i++)
            env.ProduceJson(topic, 0, i, new Event(i, "k", 1));
        env.ProduceJson<int, Event>(topic, 0, 3, null);
        env.ProduceJson(topic, 1, 50, new Event(50, "other", 1));
        env.ProduceJson<int, Event>(topic, 1, 51, null);
        env.ProduceJson<int, Event>(topic, 1, 52, null);
        await env.FlushAsync();
        using (var producer = env.TransactionalProducer())
        {
            producer.BeginTransaction();
            producer.Produce(new TopicPartition(topic, 0), Json(4, new Event(4, "k", 1)));
            producer.Produce(new TopicPartition(topic, 0), Json<int, Event>(5, null));
            producer.CommitTransaction(Timeout);
        }

        var first = SumCircuit(storage);
        await using (var source = await KafkaSource.StartAsync(first.Circuit, first.Events, env.Source(topic)).WaitAsync(Timeout))
        {
            await TotalIsAsync(first, "k", 4, "the four events are summed");
            await KafkaEnv.EventuallyAsync(() => Task.FromResult(source.Positions[0] == 7), "the source stands at the end, past the marker");
            await KafkaEnv.EventuallyAsync(
                () => Task.FromResult(first.Circuit.CommittedOffsets("events").GetValueOrDefault(0, -1) == 6),
                "the checkpoint covers the tombstone and the marker");
            await KafkaEnv.EventuallyAsync(
                () => Task.FromResult(first.Circuit.CommittedOffsets("events").GetValueOrDefault(1, -1) == 2),
                "the checkpoint covers the trailing tombstones");
        }
        await first.Circuit.DisposeAsync();

        await KafkaEnv.DeleteRecordsBeforeAsync(topic, 0, 7);
        await KafkaEnv.DeleteRecordsBeforeAsync(topic, 1, 3);

        var second = SumCircuit(storage, first.Sink);
        Assert.Equal(new Dictionary<int, long> { [0] = 6, [1] = 2 }, second.Circuit.CommittedOffsets("events"));
        await using (var source = await KafkaSource.StartAsync(second.Circuit, second.Events, env.Source(topic)).WaitAsync(Timeout))
        {
            env.ProduceJson(topic, 0, 9, new Event(9, "k", 10));
            await env.FlushAsync();
            await TotalIsAsync(second, "k", 14, "the restarted source carries on from the end");
            Assert.Equal(1, source.RecordsRead);
        }
        await second.Circuit.DisposeAsync();
    }

    [Fact]
    public async Task AKeyedSource_RecordsThePositionPastRecordsItsDecoderPassesOver()
    {
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("products");
        var storage = new InMemoryCheckpointStorageProvider();
        var sink = new InMemoryUpsertSink<int, Product>();

        (Circuit Circuit, KeyedInputStream<int, Product> Products) Build()
        {
            var builder = new CircuitBuilder(new CircuitOptions { Name = "products", Storage = storage });
            var products = builder.KeyedInput<int, Product>("products", p => p.Id);
            var rows = products.ToAcknowledgedOutput();
            var circuit = builder.Build();
            circuit.Start();
            _ = circuit.Handle(rows).DeliverToAsync(sink, p => p.Id);
            return (circuit, products);
        }

        var keyed = KafkaDeserializers.Keyed<int, Product>();
        KafkaChangeDecoder<int, Product> decoder = (in KafkaRecord record) =>
        {
            var change = keyed(in record);
            return change.IsUpsert && change.Value.Name == "noise" ? KafkaChange<int, Product>.Skip : change;
        };

        env.ProduceJson(topic, 0, 1, new Product(1, "one", 10));
        env.ProduceJson(topic, 0, 2, new Product(2, "noise", 0));
        env.ProduceJson(topic, 0, 3, new Product(3, "noise", 0));
        await env.FlushAsync();

        var first = Build();
        await using (var source = await KafkaSource.StartAsync(first.Circuit, first.Products, env.Source(topic), decoder).WaitAsync(Timeout))
        {
            await source.WhenCaughtUpAsync().WaitAsync(Timeout);
            await KafkaEnv.EventuallyAsync(
                () => Task.FromResult(first.Circuit.CommittedOffsets("products").GetValueOrDefault(0, -1) == 2),
                "the checkpoint covers the records passed over");
        }
        await first.Circuit.DisposeAsync();
        Assert.Equal(new Product(1, "one", 10), Assert.Single(sink.Rows).Value);

        await KafkaEnv.DeleteRecordsBeforeAsync(topic, 0, 3);
        var second = Build();
        await using (var source = await KafkaSource.StartAsync(second.Circuit, second.Products, env.Source(topic), decoder).WaitAsync(Timeout))
            await source.WhenCaughtUpAsync().WaitAsync(Timeout);
        await second.Circuit.DisposeAsync();
    }

    // ── a start at the end ─────────────────────────────────────────────────────────────

    /// <summary>The finding as reported: with <c>StartFrom = Latest</c>, a partition that
    /// had been quiet took "the end" afresh at every start, and whatever it received while
    /// the loader was down was skipped without an error.</summary>
    [Fact]
    public async Task ALoaderStartedAtTheEnd_TakesTheEndOnce_AndASuccessorReadsWhatAQuietPartitionReceived()
    {
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("products", partitions: 3);
        for (var id = 0; id < 6; id++)
            env.ProduceJson(topic, id % 2, id, new Product(id, "history", id)); // partitions 0 and 1; partition 2 is empty
        await env.FlushAsync();

        var table = new SharedTable<int, Product>("products", p => p.Id);
        var options = new KafkaSourceOptions { BootstrapServers = KafkaEnv.Bootstrap, Topic = topic, StartFrom = KafkaSourceStart.Latest };
        await using (var loader = await KafkaSource.StartTableLoaderAsync(table, options).WaitAsync(Timeout))
        {
            // The ends it started from are on record before it has read anything.
            Assert.Equal(new Dictionary<int, long> { [0] = 2, [1] = 2 }, table.CommittedOffsets);
            Assert.Equal(new Dictionary<int, long> { [0] = 3, [1] = 3, [2] = 0 }, loader.Positions);
            Assert.Equal(0, table.Count);

            env.ProduceJson(topic, 0, 10, new Product(10, "new", 1));
            await env.FlushAsync();
            await KafkaEnv.EventuallyAsync(() => Task.FromResult(table.Count == 1), "the record produced after the start is loaded");
        }

        // While the loader is down: one record on the partition that was quiet, one on the
        // partition that was empty at the first start.
        env.ProduceJson(topic, 1, 11, new Product(11, "while down", 2));
        env.ProduceJson(topic, 2, 12, new Product(12, "while down", 3));
        await env.FlushAsync();

        await using (var loader = await KafkaSource.StartTableLoaderAsync(table, options).WaitAsync(Timeout))
        {
            await loader.WhenCaughtUpAsync().WaitAsync(Timeout);
            Assert.Equal(2, loader.RecordsRead);
            Assert.Equal(3, table.Count);
            using var snapshot = table.Snapshot();
            Assert.True(snapshot.TryGet(11, out _) && snapshot.TryGet(12, out _));
            Assert.False(snapshot.TryGet(0, out _));      // history stays unread
        }
    }

    [Fact]
    public async Task AStreamSourceStartedAtTheEnd_CheckpointsTheEndsItStartedFrom()
    {
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("events", partitions: 2);
        for (var i = 0; i < 40; i++)
            env.ProduceJson(topic, i % 2, i, new Event(i, "old", 1));
        await env.FlushAsync();

        var storage = new InMemoryCheckpointStorageProvider();
        var options = new KafkaSourceOptions { BootstrapServers = KafkaEnv.Bootstrap, Topic = topic, StartFrom = KafkaSourceStart.Latest };
        var first = SumCircuit(storage);
        await using (var source = await KafkaSource.StartAsync(first.Circuit, first.Events, options).WaitAsync(Timeout))
        {
            Assert.Equal(new Dictionary<int, long> { [0] = 20, [1] = 20 }, source.Positions);
            env.ProduceJson(topic, 0, 100, new Event(100, "new", 5));
            await env.FlushAsync();
            await TotalIsAsync(first, "new", 5, "the record produced after the start is summed");
        }
        await first.Circuit.DisposeAsync();

        // Partition 1 handed on no record, yet it has a position: the end it started from.
        env.ProduceJson(topic, 1, 101, new Event(101, "new", 7));
        await env.FlushAsync();
        var second = SumCircuit(storage, first.Sink);
        Assert.Equal(new Dictionary<int, long> { [0] = 20, [1] = 19 }, second.Circuit.CommittedOffsets("events"));
        await using (var source = await KafkaSource.StartAsync(second.Circuit, second.Events, options).WaitAsync(Timeout))
        {
            await TotalIsAsync(second, "new", 12, "the record a quiet partition received while the source was down is summed");
            Assert.Equal(1, source.RecordsRead);
        }
        await second.Circuit.DisposeAsync();
        Assert.False(first.Sink.Rows.ContainsKey("old"));
    }

    // ── transactions in the topic ──────────────────────────────────────────────────────

    /// <summary>A source reads what was committed and nothing else: not an aborted
    /// transaction's records, and not an open one's until it commits.</summary>
    [Fact]
    public async Task ASource_HandsOnCommittedRecordsOnly()
    {
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("events");
        var partition = new TopicPartition(topic, 0);
        using var producer = env.TransactionalProducer();

        producer.BeginTransaction();
        for (var i = 0; i < 5; i++)
            producer.Produce(partition, Json(i, new Event(i, "committed", 1)));
        producer.CommitTransaction(Timeout);

        producer.BeginTransaction();
        for (var i = 0; i < 5; i++)
            producer.Produce(partition, Json(100 + i, new Event(100 + i, "aborted", 1)));
        producer.Flush(Timeout);
        producer.AbortTransaction(Timeout);

        producer.BeginTransaction();
        for (var i = 0; i < 5; i++)
            producer.Produce(partition, Json(200 + i, new Event(200 + i, "open", 1)));
        producer.Flush(Timeout); // written, and undecided

        var sums = SumCircuit(new InMemoryCheckpointStorageProvider());
        await using var source = await KafkaSource.StartAsync(sums.Circuit, sums.Events, env.Source(topic)).WaitAsync(Timeout);
        await TotalIsAsync(sums, "committed", 5, "the committed transaction is summed");
        await Task.Delay(TimeSpan.FromSeconds(1.5));
        Assert.Equal(5, source.RecordsRead);
        Assert.Equal(["committed"], sums.Sink.Rows.Keys);
        // The lag is measured the same way: nothing readable is waiting.
        Assert.True((await source.MeasureLagAsync()).IsCaughtUp);

        producer.CommitTransaction(Timeout);
        await TotalIsAsync(sums, "open", 5, "the open transaction is summed once it commits");
        Assert.Equal(10, source.RecordsRead);
        Assert.False(sums.Sink.Rows.ContainsKey("aborted"));
        await sums.Circuit.DisposeAsync();
    }

    // ── a position lost while the source runs ──────────────────────────────────────────

    /// <summary>A source held back by its circuit while retention takes the records it has
    /// not read yet. When it reads on, the place it stood at is gone: that is lost data, and
    /// the source must say so and stop, not carry on from wherever the topic now begins.</summary>
    [Fact]
    public async Task ASource_OvertakenByRetentionWhileItRuns_StopsWithPositionLost()
    {
        const int Records = 30_000, Capacity = 16;
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("events");
        var filler = new string('x', 200);
        for (var i = 0; i < Records; i++)
        {
            env.ProduceJson(topic, 0, i, new Event(i, filler, 1));
            if (i % 5000 == 0)
                await env.FlushAsync();
        }
        await env.FlushAsync();

        var sums = SumCircuit(new InMemoryCheckpointStorageProvider(), autoStep: false, capacity: Capacity);
        var options = new KafkaSourceOptions
        {
            BootstrapServers = KafkaEnv.Bootstrap, Topic = topic, FetchBufferKilobytes = 64, PauseAfter = TimeSpan.FromMilliseconds(200),
        };
        await using var source = await KafkaSource.StartAsync(sums.Circuit, sums.Events, options).WaitAsync(Timeout);
        await KafkaEnv.EventuallyAsync(() => Task.FromResult(source.RecordsRead == Capacity && source.IsPaused), "the source is held back and paused");

        // Retention passes the source: everything it has not read is gone.
        await KafkaEnv.DeleteRecordsBeforeAsync(topic, 0, Records);
        env.ProduceJson(topic, 0, Records, new Event(Records, "after", 1));
        await env.FlushAsync();

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (!source.Completion.IsCompleted && DateTime.UtcNow < deadline)
        {
            await sums.Circuit.StepAsync().WaitAsync(Timeout);
            await Task.Delay(5);
        }
        Assert.True(source.Completion.IsCompleted, $"the source read on after its position was removed (it has read {source.RecordsRead} records)");
        var lost = await Assert.ThrowsAsync<KafkaSourcePositionLostException>(() => source.Completion);
        Assert.Contains("no longer holds the position", lost.Message);
        Assert.True(source.RecordsRead < Records / 2, $"the source read {source.RecordsRead} records, which it cannot have");
        KafkaEnv.Measured(output, $"position lost while running: the source stopped after {source.RecordsRead} of {Records} records");
        await sums.Circuit.DisposeAsync();
    }

    // ── starting ───────────────────────────────────────────────────────────────────────

    /// <summary>
    /// With OAUTHBEARER (MSK IAM) a consumer gets its token from a refresh handler that the
    /// client calls only when the consumer is polled — and the source's first requests, for
    /// the topic's partitions and offsets, go out before it has anything to poll for. The
    /// local broker speaks no SASL, so this start cannot succeed; what it shows is that the
    /// handler is served while the source starts, without which it could not succeed anywhere.
    /// </summary>
    [Fact]
    public async Task ASource_ServesItsTokenRefreshHandler_WhileItStarts()
    {
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("events");
        var calls = 0;
        var clock = System.Diagnostics.Stopwatch.StartNew();
        var firstCall = TimeSpan.MaxValue;
        var options = new KafkaSourceOptions
        {
            BootstrapServers = KafkaEnv.Bootstrap,
            Topic = topic,
            OperationTimeout = TimeSpan.FromSeconds(3),
            ClientConfig = new Dictionary<string, string> { ["security.protocol"] = "SASL_PLAINTEXT", ["sasl.mechanism"] = "OAUTHBEARER" },
            ConfigureConsumer = consumer => consumer.SetOAuthBearerTokenRefreshHandler((client, _) =>
            {
                if (Interlocked.Increment(ref calls) == 1)
                    firstCall = clock.Elapsed;
                client.OAuthBearerSetToken("not-a-real-token", DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeMilliseconds(), "test");
            }),
        };
        var table = new SharedTable<int, Product>("products", p => p.Id);
        await Assert.ThrowsAsync<KafkaSourceException>(() => KafkaSource.StartTableLoaderAsync(table, options)).WaitAsync(Timeout);
        Assert.True(Volatile.Read(ref calls) >= 1, "the token refresh handler was never called");

        // Early: while the first request was waiting, not when the failed source was closed
        // (closing a consumer polls it, which would call the handler three seconds late).
        Assert.True(firstCall < TimeSpan.FromSeconds(2), $"the token refresh handler was first called after {firstCall}, not while the source started");
    }

    /// <summary>The retry both the log's and the source's admin calls go through, against
    /// the errors a partition without a known leader gives.</summary>
    [Fact]
    public async Task ARequestThatFailsForWantOfALeader_IsMadeAgain_UntilTheTimeout()
    {
        var failures = new Queue<KafkaException>();
        var calls = 0;
        Task<int> Ask()
        {
            calls++;
            return failures.TryDequeue(out var failure) ? Task.FromException<int>(failure) : Task.FromResult(42);
        }

        failures.Enqueue(new KafkaException(new Error(ErrorCode.Local_NoEnt, "Failed to query partition leaders: No leaders found")));
        failures.Enqueue(new KafkaException(new Error(ErrorCode.NotLeaderForPartition)));
        failures.Enqueue(new KafkaException(new Error(ErrorCode.LeaderNotAvailable)));
        Assert.Equal(42, await KafkaLog.RetryTransientAsync(Ask, TimeSpan.FromSeconds(20), default).WaitAsync(Timeout));
        Assert.Equal(4, calls);

        // An error that will not pass is not asked about twice …
        failures.Enqueue(new KafkaException(new Error(ErrorCode.TopicAuthorizationFailed)));
        await Assert.ThrowsAsync<KafkaException>(() => KafkaLog.RetryTransientAsync(Ask, TimeSpan.FromSeconds(20), default));
        Assert.Equal(5, calls);

        // … and one that does not pass in time is given up on at the timeout.
        for (var i = 0; i < 100; i++)
            failures.Enqueue(new KafkaException(new Error(ErrorCode.Local_NoEnt)));
        var gaveUp = await Assert.ThrowsAsync<KafkaException>(() => KafkaLog.RetryTransientAsync(Ask, TimeSpan.FromMilliseconds(400), default));
        Assert.Equal(ErrorCode.Local_NoEnt, gaveUp.Error.Code);
        Assert.InRange(calls, 7, 12);
    }

    /// <summary>Sources started the moment their topics exist, many at once: on one busy
    /// broker a just-created partition often has no leader the client knows of yet, and a
    /// source that did not ask again failed to start.</summary>
    [Fact]
    public async Task SourcesStartedTheMomentTheirTopicsExist_AllStart()
    {
        await using var env = KafkaEnv.Create();
        var started = await Task.WhenAll(Enumerable.Range(0, 48).Select(i => Task.Run(async () =>
        {
            var topic = await env.CreateTopicAsync("fresh" + i, partitions: 6);
            var table = new SharedTable<int, Product>("t" + i, p => p.Id);
            await using var loader = await KafkaSource.StartTableLoaderAsync(table, env.Source(topic));
            await loader.MeasureLagAsync();
            return loader.Positions.Count;
        }))).WaitAsync(Timeout * 2);
        Assert.All(started, partitions => Assert.Equal(6, partitions));
    }
}
