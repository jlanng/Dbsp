using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Incremental.Storage;

namespace Incremental.Kafka.Tests;

/// <summary>
/// <see cref="KafkaSource"/> into circuit inputs, on the local broker: every partition is
/// read, the position is the circuit's checkpoint and nothing else, a slow circuit holds
/// the source back within fixed bounds, and a record that cannot be read goes the way the
/// circuit's error policy says.
/// </summary>
public class KafkaSourceTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Timeout = KafkaEnv.Timeout;

    private sealed record Event(int Id, string Key, int Amount);
    private sealed record Account(int Id, string Name);

    private static Event EventAt(int i) => new(i, "k" + i % 7, i % 13 + 1);

    private static Dictionary<string, long> Totals(IEnumerable<Event> events) =>
        events.GroupBy(e => e.Key).ToDictionary(g => g.Key, g => g.Sum(e => (long)e.Amount));

    private static void ProduceEvents(KafkaEnv env, string topic, int partitions, int from, int to)
    {
        for (var i = from; i < to; i++)
            env.ProduceJson(topic, i % partitions, i, EventAt(i));
    }

    private sealed record Sums(Circuit Circuit, InputStream<Event> Events, Task Delivery);

    /// <summary>Events summed per key into a sink that outlives the circuit.</summary>
    private static Sums SumCircuit(
        IStorageProvider storage, InMemoryUpsertSink<string, long> sink, RecordErrorPolicy policy = RecordErrorPolicy.Fault,
        bool autoStep = true, int inputCapacity = 8192)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            Name = "sums",
            Storage = storage,
            ErrorPolicy = policy,
            AutoStep = autoStep,
            InputChannelCapacity = inputCapacity,
            MaxStepItemsPerInput = 64,
        });
        var events = builder.Input<Event>("events");
        var totals = events.GroupBy(e => e.Key).Sum(e => (long)e.Amount).ToAcknowledgedOutput();
        var circuit = builder.Build();
        circuit.Start();
        var delivery = circuit.Handle(totals).DeliverToAsync(sink);
        return new Sums(circuit, events, delivery);
    }

    private static Task SinkEqualsAsync<TKey, TRow>(InMemoryUpsertSink<TKey, TRow> sink, Dictionary<TKey, TRow> expected, string what)
        where TKey : notnull =>
        KafkaEnv.EventuallyAsync(
            () => Task.FromResult(sink.Rows.Count == expected.Count && expected.All(e => sink.Rows.TryGetValue(e.Key, out var row) && Equals(row, e.Value))),
            what);

    // ── position ───────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task AStreamSource_ReadsEveryPartition_AndResumesOneRecordAfterTheCheckpoint()
    {
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("events", partitions: 3);
        var storage = new InMemoryCheckpointStorageProvider();
        var sink = new InMemoryUpsertSink<string, long>();
        ProduceEvents(env, topic, 3, 0, 300);
        await env.FlushAsync();

        var first = SumCircuit(storage, sink);
        await using (var source = await KafkaSource.StartAsync(first.Circuit, first.Events, env.Source(topic)).WaitAsync(Timeout))
        {
            await source.WhenCaughtUpAsync().WaitAsync(Timeout);
            await SinkEqualsAsync(sink, Totals(Enumerable.Range(0, 300).Select(EventAt)), "the first run's totals are in the sink");
            Assert.Equal(300, source.RecordsRead);
            Assert.Equal(new Dictionary<int, long> { [0] = 100, [1] = 100, [2] = 100 }, source.Positions);
            var lag = await source.MeasureLagAsync();
            Assert.True(lag.IsCaughtUp);
        }
        await first.Circuit.DisposeAsync();
        await first.Delivery.WaitAsync(Timeout);

        ProduceEvents(env, topic, 3, 300, 450);
        await env.FlushAsync();

        // A second process: the same storage, nothing else carried over. The deserializer
        // notes every offset it is shown — which must be the 150 new records and no other.
        var seen = new ConcurrentBag<(int Partition, long Offset)>();
        var json = KafkaDeserializers.Json<Event>();
        KafkaDeserializer<Event> noting = (ReadOnlySpan<byte> data, in KafkaRecord record) =>
        {
            seen.Add((record.Partition, record.Offset));
            return json(data, in record);
        };
        var second = SumCircuit(storage, sink);
        Assert.Equal(new Dictionary<int, long> { [0] = 99, [1] = 99, [2] = 99 }, second.Circuit.CommittedOffsets("events"));
        await using (var source = await KafkaSource.StartAsync(second.Circuit, second.Events, env.Source(topic), noting).WaitAsync(Timeout))
        {
            await source.WhenCaughtUpAsync().WaitAsync(Timeout);
            await SinkEqualsAsync(sink, Totals(Enumerable.Range(0, 450).Select(EventAt)), "the second run's totals are in the sink");
            Assert.Equal(150, source.RecordsRead);
            Assert.Equal(150, seen.Count);
            Assert.Equal(100, seen.Min(s => s.Offset));
            Assert.Equal(149, seen.Max(s => s.Offset));
        }
        await second.Circuit.DisposeAsync();
    }

    [Fact]
    public async Task APartitionWithNoCommittedPosition_StartsWhereTheOptionsSay()
    {
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("events", partitions: 2);
        ProduceEvents(env, topic, 2, 0, 40);
        await env.FlushAsync();

        var sink = new InMemoryUpsertSink<string, long>();
        var sums = SumCircuit(new InMemoryCheckpointStorageProvider(), sink);
        var options = new KafkaSourceOptions { BootstrapServers = KafkaEnv.Bootstrap, Topic = topic, StartFrom = KafkaSourceStart.Latest };
        await using var source = await KafkaSource.StartAsync(sums.Circuit, sums.Events, options).WaitAsync(Timeout);
        Assert.Equal(new Dictionary<int, long> { [0] = 20, [1] = 20 }, source.Positions);

        ProduceEvents(env, topic, 2, 40, 60);
        await env.FlushAsync();
        await SinkEqualsAsync(sink, Totals(Enumerable.Range(40, 20).Select(EventAt)), "only the records produced after the start are summed");
        Assert.Equal(20, source.RecordsRead);
        await source.DisposeAsync();
        await sums.Circuit.DisposeAsync();
    }

    [Fact]
    public async Task ASource_WhoseTopicIsMissing_OrWhosePositionIsGone_Refuses()
    {
        await using var env = KafkaEnv.Create();
        var storage = new InMemoryCheckpointStorageProvider();
        var sink = new InMemoryUpsertSink<string, long>();

        var sums = SumCircuit(storage, sink);
        var missing = await Assert.ThrowsAsync<KafkaSourceException>(
            () => KafkaSource.StartAsync(sums.Circuit, sums.Events, env.Source(env.Prefix + ".nowhere"))).WaitAsync(Timeout);
        Assert.Contains("does not exist", missing.Message);

        var topic = await env.CreateTopicAsync("events");
        ProduceEvents(env, topic, 1, 0, 50);
        await env.FlushAsync();
        await using (var source = await KafkaSource.StartAsync(sums.Circuit, sums.Events, env.Source(topic)).WaitAsync(Timeout))
            await source.WhenCaughtUpAsync().WaitAsync(Timeout);
        await sums.Circuit.DisposeAsync(); // committed position: 49

        // Retention takes the records after the committed position away before the next start.
        ProduceEvents(env, topic, 1, 50, 80);
        await env.FlushAsync();
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = KafkaEnv.Bootstrap }).Build())
            await admin.DeleteRecordsAsync([new TopicPartitionOffset(topic, 0, 60)]);

        var restarted = SumCircuit(storage, sink);
        var lost = await Assert.ThrowsAsync<KafkaSourcePositionLostException>(
            () => KafkaSource.StartAsync(restarted.Circuit, restarted.Events, env.Source(topic))).WaitAsync(Timeout);
        Assert.Contains("committed 49, first retained 60", lost.Message);
        await restarted.Circuit.DisposeAsync();

        // And a position past the topic's end means it is not the same topic.
        var other = await env.CreateTopicAsync("other");
        var elsewhere = SumCircuit(storage, sink);
        var foreign = await Assert.ThrowsAsync<KafkaSourcePositionLostException>(
            () => KafkaSource.StartAsync(elsewhere.Circuit, elsewhere.Events, env.Source(other))).WaitAsync(Timeout);
        Assert.Contains("not the topic", foreign.Message);
        await elsewhere.Circuit.DisposeAsync();
    }

    // ── keyed input ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task AKeyedSource_Upserts_AndDeletesOnTombstones_AndResumes()
    {
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("accounts", partitions: 2, compacted: true);
        var storage = new InMemoryCheckpointStorageProvider();
        var sink = new InMemoryUpsertSink<int, Account>();

        (Circuit Circuit, KeyedInputStream<int, Account> Accounts) Build()
        {
            var builder = new CircuitBuilder(new CircuitOptions { Name = "accounts", Storage = storage });
            var accounts = builder.KeyedInput<int, Account>("accounts", a => a.Id);
            var rows = accounts.ToAcknowledgedOutput();
            var circuit = builder.Build();
            circuit.Start();
            _ = circuit.Handle(rows).DeliverToAsync(sink, a => a.Id);
            return (circuit, accounts);
        }

        for (var id = 0; id < 40; id++)
            env.ProduceJson(topic, id % 2, id, new Account(id, "first-" + id));
        for (var id = 0; id < 40; id += 4)
            env.ProduceJson(topic, id % 2, id, new Account(id, "second-" + id));
        for (var id = 1; id < 40; id += 5)
            env.ProduceJson<int, Account>(topic, id % 2, id, null); // tombstones
        await env.FlushAsync();

        var model = Enumerable.Range(0, 40).Where(id => id % 5 != 1)
            .ToDictionary(id => id, id => new Account(id, (id % 4 == 0 ? "second-" : "first-") + id));

        var first = Build();
        await using (var source = await KafkaSource.StartAsync(first.Circuit, first.Accounts, env.Source(topic)).WaitAsync(Timeout))
        {
            await SinkEqualsAsync(sink, model, "the accounts are in the sink, deleted ones gone");
            Assert.Equal(58, source.RecordsRead);
        }
        await first.Circuit.DisposeAsync();

        env.ProduceJson<int, Account>(topic, 0, 2, null);
        env.ProduceJson(topic, 1, 1, new Account(1, "back"));
        await env.FlushAsync();
        model.Remove(2);
        model[1] = new Account(1, "back");

        var second = Build();
        await using (var source = await KafkaSource.StartAsync(second.Circuit, second.Accounts, env.Source(topic)).WaitAsync(Timeout))
        {
            await SinkEqualsAsync(sink, model, "the restarted circuit applied the two new commands");
            Assert.Equal(2, source.RecordsRead);
        }
        await second.Circuit.DisposeAsync();
    }

    // ── unreadable records ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task AnUnreadableRecord_IsDeadLettered_WithWhereItCameFrom_AndCheckpointed()
    {
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("events");
        var keyed = await env.CreateTopicAsync("accounts");
        var storage = new InMemoryCheckpointStorageProvider();
        var sink = new InMemoryUpsertSink<string, long>();

        (Circuit, InputStream<Event>, KeyedInputStream<int, Account>) Build()
        {
            var builder = new CircuitBuilder(new CircuitOptions { Name = "poison", Storage = storage, ErrorPolicy = RecordErrorPolicy.DeadLetter });
            var events = builder.Input<Event>("events");
            var accounts = builder.KeyedInput<int, Account>("accounts", a => a.Id);
            var totals = events.GroupBy(e => e.Key).Sum(e => (long)e.Amount).ToAcknowledgedOutput();
            accounts.ToOutput();
            var circuit = builder.Build();
            circuit.Start();
            _ = circuit.Handle(totals).DeliverToAsync(sink);
            return (circuit, events, accounts);
        }

        ProduceEvents(env, topic, 1, 0, 5);
        env.Produce(topic, 0, "bad"u8.ToArray(), "{ this is not json"u8.ToArray());
        ProduceEvents(env, topic, 1, 5, 10);
        env.Produce(topic, 0, null, "null"u8.ToArray());
        env.ProduceJson(keyed, 0, 1, new Account(1, "one"));
        env.Produce(keyed, 0, "not-a-number"u8.ToArray(), null); // a tombstone whose key does not parse
        await env.FlushAsync();

        var (circuit, events, accounts) = Build();
        await using (var source = await KafkaSource.StartAsync(circuit, events, env.Source(topic)).WaitAsync(Timeout))
        await using (var keyedSource = await KafkaSource.StartAsync(circuit, accounts, env.Source(keyed)).WaitAsync(Timeout))
        {
            var letters = new List<DeadLetter>();
            using var stop = new CancellationTokenSource(Timeout);
            while (letters.Count < 3)
                letters.Add(await circuit.DeadLetters.ReadAsync(stop.Token));

            var bad = letters.Single(l => l.Record is KafkaRecord { Offset: 5 } r && r.Topic == topic);
            var record = (KafkaRecord)bad.Record!;
            Assert.Equal((topic, 0, 5L), (record.Topic, record.Partition, record.Offset));
            Assert.Equal("bad", Encoding.UTF8.GetString(record.Key!));
            Assert.Equal("{ this is not json", Encoding.UTF8.GetString(record.Value!));
            Assert.IsAssignableFrom<JsonException>(bad.Exception);
            Assert.Equal("input[events]", bad.Operator);
            Assert.Contains($"{topic}[0]@5", record.ToString());

            var nullValue = letters.Single(l => l.Record is KafkaRecord { Offset: 11 } r && r.Topic == topic);
            Assert.IsAssignableFrom<JsonException>(nullValue.Exception);

            var badKey = letters.Single(l => l.Record is KafkaRecord r && r.Topic == keyed);
            Assert.Equal(1, ((KafkaRecord)badKey.Record!).Offset);
            Assert.Equal("input[accounts]", badKey.Operator);

            // The records around the bad ones went through.
            await SinkEqualsAsync(sink, Totals(Enumerable.Range(0, 10).Select(EventAt)), "the readable events are summed");
            Assert.Equal(2, source.RecordsUnreadable);
            Assert.Equal(1, keyedSource.RecordsUnreadable);
        }
        await circuit.DisposeAsync();

        // The bad records' offsets are in the checkpoint, so a restart reads none of them again.
        var (restarted, restartedEvents, restartedAccounts) = Build();
        Assert.Equal(11, restarted.CommittedOffsets("events")[0]);
        Assert.Equal(1, restarted.CommittedOffsets("accounts")[0]);
        await using (var source = await KafkaSource.StartAsync(restarted, restartedEvents, env.Source(topic)).WaitAsync(Timeout))
        await using (var keyedSource = await KafkaSource.StartAsync(restarted, restartedAccounts, env.Source(keyed)).WaitAsync(Timeout))
        {
            await source.WhenCaughtUpAsync().WaitAsync(Timeout);
            await keyedSource.WhenCaughtUpAsync().WaitAsync(Timeout);
            Assert.Equal(0, source.RecordsRead + keyedSource.RecordsRead);
            Assert.False(restarted.DeadLetters.TryRead(out _));
        }
        await restarted.DisposeAsync();
    }

    [Fact]
    public async Task AnUnreadableRecord_FaultsTheCircuit_UnderTheDefaultPolicy()
    {
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("events");
        ProduceEvents(env, topic, 1, 0, 3);
        env.Produce(topic, 0, null, "nonsense"u8.ToArray());
        await env.FlushAsync();

        var sums = SumCircuit(new InMemoryCheckpointStorageProvider(), new InMemoryUpsertSink<string, long>());
        await using var source = await KafkaSource.StartAsync(sums.Circuit, sums.Events, env.Source(topic)).WaitAsync(Timeout);
        var fault = await Assert.ThrowsAnyAsync<Exception>(() => sums.Circuit.Completion).WaitAsync(Timeout);
        Assert.Contains("partition 0, offset 3", fault.ToString());
        Assert.NotNull(sums.Circuit.Fault);

        // The source learns of it at its next write, or sooner, and ends with the fault.
        env.Produce(topic, 0, null, KafkaEnv.Json(EventAt(4)));
        await env.FlushAsync();
        await Assert.ThrowsAsync<CircuitFaultedException>(() => source.Completion).WaitAsync(Timeout);
        await sums.Circuit.DisposeAsync();
    }

    // ── backpressure ───────────────────────────────────────────────────────────────────

    /// <summary>
    /// A topic far larger than every buffer, and a circuit that does not step. The source
    /// may hold what the input's channel holds plus what the client was told it may fetch
    /// ahead, and no more — however long the stall lasts. Then the circuit runs and every
    /// record arrives once.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ASlowCircuit_HoldsTheSourceBack_WithinFixedBounds(bool pause)
    {
        const int Records = 40_000, PayloadBytes = 1500, Capacity = 256, BufferKilobytes = 2048;
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("big", partitions: 2);
        var random = new Random(7);
        var filler = new byte[PayloadBytes];
        for (var i = 0; i < Records; i++)
        {
            random.NextBytes(filler); // incompressible, so a megabyte fetched is a megabyte held
            env.Produce(topic, i % 2, null, Encoding.UTF8.GetBytes($"{i}|{Convert.ToBase64String(filler)}"));
            if (i % 2000 == 0)
                await env.FlushAsync();
        }
        await env.FlushAsync();
        var topicBytes = (long)Records * PayloadBytes * 4 / 3;

        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, InputChannelCapacity = Capacity, MaxStepItemsPerInput = 512 });
        var input = builder.Input<int>("big");
        var ids = input.ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();

        // What the client holds fetched and not yet handed on, from its own statistics:
        // the bytes in its queue (one queue for all partitions, reported under each). The
        // client hands statistics over only when polled, so the samples taken during the
        // stall arrive once the circuit runs again; the peak is read at the end.
        long peak = 0;
        var samples = 0;
        var options = new KafkaSourceOptions
        {
            BootstrapServers = KafkaEnv.Bootstrap,
            Topic = topic,
            FetchBufferKilobytes = BufferKilobytes,
            PauseAfter = pause ? TimeSpan.FromMilliseconds(200) : TimeSpan.FromHours(1),
            ClientConfig = new Dictionary<string, string> { ["statistics.interval.ms"] = "100" },
            ConfigureConsumer = consumer => consumer.SetStatisticsHandler((_, json) =>
            {
                using var statistics = JsonDocument.Parse(json);
                foreach (var t in statistics.RootElement.GetProperty("topics").EnumerateObject())
                    foreach (var p in t.Value.GetProperty("partitions").EnumerateObject())
                        peak = Math.Max(peak, p.Value.GetProperty("fetchq_size").GetInt64());
                samples++;
            }),
        };
        KafkaDeserializer<int> id = (ReadOnlySpan<byte> data, in KafkaRecord _) =>
            int.Parse(Encoding.UTF8.GetString(data[..data.IndexOf((byte)'|')]));
        await using var source = await KafkaSource.StartAsync(circuit, input, options, id).WaitAsync(Timeout);

        // Stalled: the channel fills, and the source has read exactly one record more than
        // it holds — the one it is waiting to write.
        await KafkaEnv.EventuallyAsync(() => Task.FromResult(source.RecordsRead == Capacity), "the source filled the input and stopped");
        await Task.Delay(TimeSpan.FromSeconds(2));
        Assert.Equal(Capacity, source.RecordsRead);
        Assert.Equal(pause, source.IsPaused);
        Assert.Equal(pause ? 1 : 0, source.Pauses);

        // Released: every record arrives exactly once, in order within its partition.
        var seen = new int[Records];
        var release = System.Diagnostics.Stopwatch.StartNew();
        var reader = circuit.Handle(ids).Reader;
        var received = 0;
        var deadline = DateTime.UtcNow + Timeout;
        while (received < Records && DateTime.UtcNow < deadline)
        {
            await circuit.StepAsync();
            while (reader.TryRead(out var batch))
                foreach (var entry in batch)
                {
                    seen[entry.Value] += (int)entry.Weight;
                    received += (int)entry.Weight;
                }
        }
        Assert.Equal(Records, received);
        Assert.All(seen, count => Assert.Equal(1, count));
        // Handed on once each, too: the input would have hidden a record read twice.
        Assert.Equal(Records, source.RecordsRead);
        Assert.False(source.IsPaused);

        // The client's limit, plus the fetches (a megabyte a partition) that can be in
        // flight when it is reached — against a topic many times that.
        var bound = (BufferKilobytes + 2 * 1024 + 512) * 1024L;
        Assert.True(samples > 20, "the client reported no statistics");
        Assert.InRange(peak, 1, bound);
        Assert.True(topicBytes > 15 * bound, "the topic must dwarf the bound for this to mean anything");
        KafkaEnv.Measured(output, $"backpressure (pause {pause}): topic {topicBytes / 1048576} MB, channel {Capacity} records, " +
            $"client buffer limit {BufferKilobytes} KB, peak held by the client {peak / 1024} KB, {source.Pauses} pause(s), drained in {release.Elapsed.TotalSeconds:F1} s");
    }
}
