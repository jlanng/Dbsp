using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Incremental.Kafka;

namespace Incremental.Service.Tests;

/// <summary>The Kafka-backed tests measure and wait on wall-clock leadership, so they do
/// not share the machine with each other or with the rest of the suite.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class KafkaServiceCollection
{
    public const string Name = "kafka service";
}

/// <summary>
/// One test's corner of the local broker (<c>docker compose --profile connectors up -d</c>):
/// a topic prefix nobody else has used, plain topics for sources and sinks, and the
/// clean-up. A test that cannot reach the broker is skipped, as in
/// <c>tests/Incremental.Kafka.Tests/KafkaEnv.cs</c>.
/// </summary>
internal sealed class KafkaTestEnv : IAsyncDisposable
{
    public static readonly string Bootstrap =
        Environment.GetEnvironmentVariable("INCREMENTAL_KAFKA_BOOTSTRAP") is { Length: > 0 } configured ? configured : "localhost:9092";
    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    private IProducer<byte[], byte[]>? _producer;

    private KafkaTestEnv()
    {
    }

    /// <summary>Unique per test and per run, so reruns never meet old topics.</summary>
    public string Prefix { get; } = "s" + Guid.NewGuid().ToString("N")[..16];

    public static KafkaTestEnv Create()
    {
        try
        {
            var first = Bootstrap.Split(',')[0];
            var colon = first.LastIndexOf(':');
            using var probe = new TcpClient();
            if (!probe.ConnectAsync(first[..colon], int.Parse(first[(colon + 1)..])).Wait(TimeSpan.FromSeconds(2)))
                throw new SocketException();
        }
        catch (Exception ex) when (ex is SocketException or AggregateException)
        {
            Assert.Skip($"Kafka is not reachable on {Bootstrap} — run `docker compose --profile connectors up -d`.");
        }
        return new KafkaTestEnv();
    }

    /// <summary>The log options of one node of this test's deployment.</summary>
    public KafkaLogOptions Log(string nodeId) => new()
    {
        BootstrapServers = Bootstrap,
        TopicPrefix = Prefix,
        NodeId = nodeId,
        HeartbeatInterval = TimeSpan.FromMilliseconds(250),
        ReplicationFactor = 1,
        StatePartitions = 3,
        TableRowPartitions = 2,
    };

    public KafkaServiceBackendOptions Backend(
        string nodeId, bool automaticTakeover = false, int probePort = 0, int peerPort = 0, int retainedHistoryVersions = 100_000) => new()
    {
        RetainedHistoryVersions = retainedHistoryVersions,
        Log = Log(nodeId),
        TakeoverAfter = TimeSpan.FromMilliseconds(1500),
        AutomaticTakeover = automaticTakeover,
        ProbeListen = probePort > 0 ? new IPEndPoint(IPAddress.Loopback, probePort) : null,
        PeerProbe = peerPort > 0 ? ("127.0.0.1", peerPort) : null,
    };

    /// <summary>A node on this test's deployment. With <paramref name="lead"/> it takes
    /// over at once rather than waiting out the silence of a deployment nobody leads.</summary>
    public async Task<TestService> StartNodeAsync(
        string nodeId, MemoryConnector? memory = null, bool lead = false, bool automaticTakeover = false,
        int probePort = 0, int peerPort = 0, Action<ServiceOptions>? options = null, Action<IncrementalServiceBuilder>? configure = null,
        int retainedHistoryVersions = 100_000)
    {
        var service = await TestService.StartAsync(null, nodeId, memory, options, builder =>
        {
            builder.UseKafkaBackend(Backend(nodeId, automaticTakeover, probePort, peerPort, retainedHistoryVersions));
            configure?.Invoke(builder);
        });
        if (lead)
        {
            await service.Node.TakeOverAsync(new NodeActionOptions(Force: true), default).WaitAsync(Timeout);
            Assert.True(service.Node.IsLeader);
        }
        return service;
    }

    public static int[] FreePorts(int count)
    {
        var listeners = Enumerable.Range(0, count).Select(_ => new TcpListener(IPAddress.Loopback, 0)).ToArray();
        foreach (var listener in listeners)
            listener.Start();
        var ports = listeners.Select(l => ((IPEndPoint)l.LocalEndpoint).Port).ToArray();
        foreach (var listener in listeners)
            listener.Stop();
        return ports;
    }

    // ── plain topics, for the sources and sinks ────────────────────────────────────────

    /// <summary>Creates <c>{prefix}.{name}</c> and returns its full name.</summary>
    public async Task<string> CreateTopicAsync(string name, int partitions = 1, bool compacted = false)
    {
        var topic = $"{Prefix}.{name}";
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap }).Build();
        await admin.CreateTopicsAsync(
        [
            new TopicSpecification
            {
                Name = topic,
                NumPartitions = partitions,
                ReplicationFactor = 1,
                Configs = compacted ? new Dictionary<string, string> { ["cleanup.policy"] = "compact" } : [],
            },
        ]);
        return topic;
    }

    public static async Task<bool> TopicExistsAsync(string topic)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap }).Build();
        return await Task.Run(() => admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics.Any(t => t.Topic == topic));
    }

    /// <summary>A plain producer, as some other system writing a source topic would be.</summary>
    public IProducer<byte[], byte[]> Producer =>
        _producer ??= new ProducerBuilder<byte[], byte[]>(new ProducerConfig { BootstrapServers = Bootstrap, LingerMs = 2 }).Build();

    public static byte[] Json<T>(T value) => JsonSerializer.SerializeToUtf8Bytes(value, JsonSerializerOptions.Web);

    /// <summary>Queues one record, keyed (its partition follows from the key);
    /// <see cref="FlushAsync"/> makes them durable.</summary>
    public void ProduceJson<TKey, TValue>(string topic, TKey key, TValue? value) where TValue : class =>
        Producer.Produce(topic, new Message<byte[], byte[]> { Key = Json(key), Value = value is null ? null! : Json(value) });

    public Task FlushAsync() => Task.Run(() => Producer.Flush(Timeout));

    /// <summary>Every record a topic holds when asked, in offset order within a partition.</summary>
    public static List<ConsumeResult<byte[], byte[]>> ReadTopic(string topic)
    {
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = Bootstrap,
            GroupId = "service-test-reader",
            EnableAutoCommit = false,
            IsolationLevel = IsolationLevel.ReadCommitted,
            EnablePartitionEof = true,
        }).Build();
        using var admin = new DependentAdminClientBuilder(consumer.Handle).Build();
        var partitions = admin.GetMetadata(topic, TimeSpan.FromSeconds(10)).Topics[0].Partitions.Count;
        consumer.Assign(Enumerable.Range(0, partitions).Select(p => new TopicPartitionOffset(topic, p, Offset.Beginning)));
        var records = new List<ConsumeResult<byte[], byte[]>>();
        var open = partitions;
        var deadline = DateTime.UtcNow + Timeout;
        while (open > 0 && DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromMilliseconds(200));
            if (result is null)
                continue;
            if (result.IsPartitionEOF)
            {
                open--;
                consumer.Pause([result.TopicPartition]);
            }
            else
                records.Add(result);
        }
        Assert.Equal(0, open);
        return records;
    }

    /// <summary>A keyed topic read as the table it describes: the last value per key,
    /// tombstoned keys gone.</summary>
    public static Dictionary<TKey, TValue> ReadTable<TKey, TValue>(string topic) where TKey : notnull
    {
        var table = new Dictionary<TKey, TValue>();
        foreach (var partition in ReadTopic(topic).GroupBy(r => r.Partition.Value))
            foreach (var record in partition)
            {
                var key = JsonSerializer.Deserialize<TKey>(record.Message.Key, JsonSerializerOptions.Web)!;
                if (record.Message.Value is null)
                    table.Remove(key);
                else
                    table[key] = JsonSerializer.Deserialize<TValue>(record.Message.Value, JsonSerializerOptions.Web)!;
            }
        return table;
    }

    /// <summary>Polls a keyed sink topic until it describes exactly <paramref name="expected"/>.</summary>
    public static async Task TopicEqualsAsync<TKey, TValue>(string topic, IReadOnlyDictionary<TKey, TValue> expected, string what) where TKey : notnull
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (true)
        {
            var actual = await Task.Run(() => ReadTable<TKey, TValue>(topic));
            if (actual.Count == expected.Count && expected.All(e => actual.TryGetValue(e.Key, out var row) && Equals(row, e.Value)))
                return;
            if (DateTime.UtcNow > deadline)
            {
                var missing = expected.Keys.Except(actual.Keys).Take(5);
                var extra = actual.Keys.Except(expected.Keys).Take(5);
                var wrong = expected.Where(e => actual.TryGetValue(e.Key, out var row) && !Equals(row, e.Value)).Select(e => e.Key).Take(5);
                Assert.Fail(
                    $"Timed out waiting for {what}: {actual.Count} rows of {expected.Count}; missing [{string.Join(", ", missing)}], " +
                    $"extra [{string.Join(", ", extra)}], wrong [{string.Join(", ", wrong)}].");
            }
            await Task.Delay(200);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _producer?.Dispose();

        // Best effort: the prefix is unique, so a topic left behind harms nothing.
        try
        {
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap }).Build();
            var topics = admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics
                .Select(t => t.Topic).Where(t => t.StartsWith(Prefix + ".", StringComparison.Ordinal)).ToList();
            if (topics.Count > 0)
                await admin.DeleteTopicsAsync(topics);
        }
        catch (KafkaException)
        {
        }
    }
}
