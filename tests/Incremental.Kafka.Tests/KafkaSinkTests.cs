using System.Text;
using Confluent.Kafka;

namespace Incremental.Kafka.Tests;

/// <summary>
/// The Kafka sinks on the local broker: an upsert sink writes a topic a keyed source reads
/// back as the same table (the round trip between two circuits), deletes are tombstones,
/// keys are placed as the Java client places them, an append sink stamps the step on every
/// record — and both work through a producer of their own or through the log's, which is
/// fenced.
/// </summary>
public class KafkaSinkTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Timeout = KafkaEnv.Timeout;

    private sealed record Account(int Id, string Name, long Balance);
    private sealed record Line(int Id, string Text);

    // ── placement ──────────────────────────────────────────────────────────────────────

    [Fact]
    public void Murmur2_IsTheJavaClientsHash()
    {
        // The vectors of Kafka's own UtilsTest.testMurmur2.
        Assert.Equal(-973932308, (int)Murmur2.Hash("21"u8));
        Assert.Equal(-790332482, (int)Murmur2.Hash("foobar"u8));
        Assert.Equal(-985981536, (int)Murmur2.Hash("a-little-bit-long-string"u8));
        Assert.Equal(-1486304829, (int)Murmur2.Hash("a-little-bit-longer-string"u8));
        Assert.Equal(-58897971, (int)Murmur2.Hash("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8"u8));
        Assert.Equal(479470107, (int)Murmur2.Hash("abc"u8));
    }

    [Fact]
    public void ASink_NeedsExactlyOneProducer()
    {
        Assert.Throws<ArgumentException>(() => new KafkaUpsertSink<int, Account>(new KafkaSinkOptions { Topic = "t" }));
        Assert.Throws<ArgumentException>(() => new KafkaAppendSink<Line>(new KafkaSinkOptions { Topic = "" , BootstrapServers = "x" }));
    }

    // ── the round trip ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Circuit A's result goes to a compacted topic through a
    /// <see cref="KafkaUpsertSink{TKey,TRow}"/>; circuit B reads the topic as a keyed input.
    /// B's rows must be A's, through updates and deletes.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnUpsertSinksTopic_ReadBackByAKeyedSource_IsTheSameTable(bool throughTheLog)
    {
        const int Partitions = 3;
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("balances", Partitions, compacted: true);
        var log = throughTheLog ? await env.ConnectWriterAsync() : null;
        var term = 7L;
        await using var sink = new KafkaUpsertSink<int, Account>(new KafkaSinkOptions
        {
            Topic = topic,
            BootstrapServers = throughTheLog ? null : KafkaEnv.Bootstrap,
            Log = log,
            Term = () => term,
        });

        // A: accounts in, the same rows out to the topic.
        var builderA = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var accountsA = builderA.KeyedInput<int, Account>("accounts", a => a.Id);
        var outA = accountsA.ToAcknowledgedOutput();
        await using var a = builderA.Build();
        a.Start();
        var delivery = a.Handle(outA).DeliverToAsync(sink, row => row.Id);
        var feed = a.Handle(accountsA);

        // B: the topic in, summed per name's first letter, into an in-memory sink.
        var builderB = new CircuitBuilder(new CircuitOptions());
        var accountsB = builderB.KeyedInput<int, Account>("balances", x => x.Id);
        var rowsB = accountsB.ToAcknowledgedOutput();
        await using var b = builderB.Build();
        b.Start();
        var seenByB = new InMemoryUpsertSink<int, Account>();
        _ = b.Handle(rowsB).DeliverToAsync(seenByB, row => row.Id);
        await using var source = await KafkaSource.StartAsync(b, accountsB, env.Source(topic)).WaitAsync(Timeout);

        var model = new Dictionary<int, Account>();
        var random = new Random(11);
        for (var step = 1; step <= 30; step++)
        {
            for (var i = 0; i < 20; i++)
            {
                var id = random.Next(60);
                if (random.Next(4) == 0)
                {
                    model.Remove(id);
                    await feed.DeleteAsync(id);
                }
                else
                {
                    model[id] = new Account(id, "n" + random.Next(5), random.Next(1000));
                    await feed.UpsertAsync(model[id]);
                }
            }
            await a.StepAsync().WaitAsync(Timeout);
        }
        var delivered = a.Handle(outA);
        await KafkaEnv.EventuallyAsync(() => Task.FromResult(delivered.AcknowledgedSequence == 30), "every step's batch is acknowledged");
        Assert.Equal(sink.RecordsWritten, KafkaEnv.ReadTopic(topic).Count);

        await KafkaEnv.EventuallyAsync(
            () => Task.FromResult(seenByB.Rows.Count == model.Count && model.All(m => seenByB.Rows.TryGetValue(m.Key, out var row) && row == m.Value)),
            "circuit B holds circuit A's rows");

        // The topic itself: a table when read as one, deletes as tombstones, every record
        // stamped, every key in the partition the Java client would choose.
        Assert.Equal(model, KafkaEnv.ReadTable<int, Account>(topic));
        var records = KafkaEnv.ReadTopic(topic);
        Assert.Contains(records, r => r.Message.Value is null);
        var steps = new List<long>();
        foreach (var record in records)
        {
            Assert.Equal(7, KafkaSinkHeaders.Read(record.Message.Headers, KafkaSinkHeaders.Term));
            steps.Add(KafkaSinkHeaders.Read(record.Message.Headers, KafkaSinkHeaders.Step)!.Value);
        }
        Assert.Equal(Enumerable.Range(1, 30).Select(s => (long)s), steps.Distinct().Order());

        // librdkafka's own murmur2 partitioner is the reference for the placement.
        var reference = await env.CreateTopicAsync("reference", Partitions);
        using (var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
        {
            BootstrapServers = KafkaEnv.Bootstrap,
            Partitioner = Partitioner.Murmur2,
        }).Build())
        {
            foreach (var key in records.Select(r => Encoding.UTF8.GetString(r.Message.Key)).Distinct())
                producer.Produce(reference, new Message<byte[], byte[]> { Key = Encoding.UTF8.GetBytes(key), Value = [1] });
            producer.Flush(Timeout);
        }
        var expectedPartition = KafkaEnv.ReadTopic(reference).ToDictionary(r => Encoding.UTF8.GetString(r.Message.Key), r => r.Partition.Value);
        Assert.True(expectedPartition.Values.Distinct().Count() > 1);
        foreach (var record in records)
            Assert.Equal(expectedPartition[Encoding.UTF8.GetString(record.Message.Key)], record.Partition.Value);

        if (throughTheLog)
        {
            // Another process takes the log over: the sink is fenced with it. The step's
            // batch does not reach the topic, and the delivery fails.
            var before = KafkaEnv.ReadTopic(topic).Count;
            await env.ConnectWriterAsync(env.Options(nodeId: "successor"));
            await feed.UpsertAsync(new Account(999, "zombie", 1));
            await a.StepAsync().WaitAsync(Timeout);
            var failure = await Assert.ThrowsAsync<SinkDeliveryException>(() => delivery).WaitAsync(Timeout);
            Assert.IsType<KafkaLogFencedException>(failure.InnerException);
            Assert.Equal(before, KafkaEnv.ReadTopic(topic).Count);
            Assert.DoesNotContain(999, KafkaEnv.ReadTable<int, Account>(topic).Keys);
        }
        KafkaEnv.Measured(output, $"upsert sink (through the log: {throughTheLog}): {records.Count} records in 30 steps");
    }

    // ── append ─────────────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnAppendSink_StampsTheStepOnEveryRecord(bool throughTheLog)
    {
        const int Partitions = 2;
        await using var env = KafkaEnv.Create();
        var unkeyedTopic = await env.CreateTopicAsync("lines", Partitions);
        var keyedTopic = await env.CreateTopicAsync("keyed-lines", Partitions);
        var log = throughTheLog ? await env.ConnectWriterAsync() : null;
        KafkaSinkOptions Options(string topic) => new()
        {
            Topic = topic,
            BootstrapServers = throughTheLog ? null : KafkaEnv.Bootstrap,
            Log = log,
        };
        await using var unkeyed = new KafkaAppendSink<Line>(Options(unkeyedTopic));
        await using var keyed = new KafkaAppendSink<Line>(Options(keyedTopic), key: line => KafkaEnv.Json(line.Id % 5));

        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var lines = builder.Input<Line>("lines");
        var first = lines.ToAcknowledgedOutput();
        var second = lines.Select(l => l with { Text = l.Text + "!" }).ToAcknowledgedOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        _ = circuit.Handle(first).DeliverToAsync(unkeyed);
        _ = circuit.Handle(second).DeliverToAsync(keyed);

        var id = 0;
        var stepOf = new Dictionary<int, long>();
        for (var step = 1; step <= 6; step++)
        {
            for (var i = 0; i < 10; i++)
            {
                stepOf[id] = step;
                await circuit.Handle(lines).InsertAsync(new Line(id, "line " + id++));
            }
            await circuit.StepAsync().WaitAsync(Timeout);
        }
        await KafkaEnv.EventuallyAsync(
            () => Task.FromResult(circuit.Handle(first).AcknowledgedSequence == 6 && circuit.Handle(second).AcknowledgedSequence == 6),
            "every step's batch is acknowledged");
        Assert.Equal(60, unkeyed.RecordsWritten);

        var written = KafkaEnv.ReadTopic(unkeyedTopic);
        Assert.Equal(60, written.Count);
        foreach (var record in written)
        {
            var line = System.Text.Json.JsonSerializer.Deserialize<Line>(record.Message.Value, System.Text.Json.JsonSerializerOptions.Web)!;
            var step = KafkaSinkHeaders.Read(record.Message.Headers, KafkaSinkHeaders.Step);
            Assert.Equal(stepOf[line.Id], step);
            Assert.Null(KafkaSinkHeaders.Read(record.Message.Headers, KafkaSinkHeaders.Term));
            // No key: a step's records share the partition "step mod partitions".
            Assert.Equal((int)(step!.Value % Partitions), record.Partition.Value);
            Assert.Null(record.Message.Key);
        }

        var keyedRecords = KafkaEnv.ReadTopic(keyedTopic);
        Assert.Equal(60, keyedRecords.Count);
        foreach (var partition in keyedRecords.GroupBy(r => r.Partition.Value))
        {
            // Along a partition the step never falls (no restart here).
            var steps = partition.Select(r => KafkaSinkHeaders.Read(r.Message.Headers, KafkaSinkHeaders.Step)!.Value).ToList();
            Assert.Equal(steps.Order(), steps);
        }
        foreach (var record in keyedRecords)
        {
            var line = System.Text.Json.JsonSerializer.Deserialize<Line>(record.Message.Value, System.Text.Json.JsonSerializerOptions.Web)!;
            Assert.EndsWith("!", line.Text);
            Assert.Equal(Murmur2.Partition(KafkaEnv.Json(line.Id % 5), Partitions), record.Partition.Value);
        }
    }

    [Fact]
    public async Task ASink_WhoseTopicIsMissing_FailsTheDelivery()
    {
        await using var env = KafkaEnv.Create();
        await using var sink = new KafkaAppendSink<Line>(new KafkaSinkOptions
        {
            Topic = env.Prefix + ".nowhere",
            BootstrapServers = KafkaEnv.Bootstrap,
        });
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var lines = builder.Input<Line>("lines");
        var appended = lines.ToAcknowledgedOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var delivery = circuit.Handle(appended).DeliverToAsync(sink, new SinkDeliveryOptions { MaxRetries = 0 });
        await circuit.Handle(lines).InsertAsync(new Line(1, "x"));
        await circuit.StepAsync().WaitAsync(Timeout);
        var failure = await Assert.ThrowsAsync<SinkDeliveryException>(() => delivery).WaitAsync(Timeout);
        Assert.Contains("does not exist", Assert.IsType<KafkaSinkException>(failure.InnerException).Message);

        // And asking did not make the broker create it.
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = KafkaEnv.Bootstrap }).Build();
        Assert.DoesNotContain(admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics, t => t.Topic == env.Prefix + ".nowhere");
    }

    /// <summary>A write returns when the broker has the records, and not before: a batch
    /// the broker refuses must fail the write, or the checkpoint would pass it.</summary>
    [Fact]
    public async Task ABatchTheBrokerRefuses_FailsTheWrite_AndIsNotAcknowledged()
    {
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("small", config: new Dictionary<string, string> { ["max.message.bytes"] = "2000" });
        await using var sink = new KafkaAppendSink<Line>(new KafkaSinkOptions
        {
            Topic = topic,
            BootstrapServers = KafkaEnv.Bootstrap,
            Compression = CompressionType.None,
        });
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var lines = builder.Input<Line>("lines");
        var appended = lines.ToAcknowledgedOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var handle = circuit.Handle(appended);
        var delivery = handle.DeliverToAsync(sink, new SinkDeliveryOptions { MaxRetries = 0 });

        await circuit.Handle(lines).InsertAsync(new Line(1, "fits"));
        await circuit.StepAsync().WaitAsync(Timeout);
        await KafkaEnv.EventuallyAsync(() => Task.FromResult(handle.AcknowledgedSequence == 1), "the small record is acknowledged");

        await circuit.Handle(lines).InsertAsync(new Line(2, new string('x', 20_000)));
        await circuit.StepAsync().WaitAsync(Timeout);
        var failure = await Assert.ThrowsAsync<SinkDeliveryException>(() => delivery).WaitAsync(Timeout);
        Assert.IsType<KafkaSinkException>(failure.InnerException);
        Assert.Equal(1, handle.AcknowledgedSequence);
        Assert.Single(KafkaEnv.ReadTopic(topic));
        Assert.Equal(1, sink.RecordsWritten);
    }
}
