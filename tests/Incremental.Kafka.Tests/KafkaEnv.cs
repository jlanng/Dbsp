using System.Net.Sockets;
using Confluent.Kafka;
using Incremental.Storage;

namespace Incremental.Kafka.Tests;

/// <summary>
/// One test's corner of the local broker (<c>docker compose --profile connectors up -d</c>):
/// a topic prefix nobody else has used, the logs opened under it, and the clean-up. A test
/// that cannot reach the broker is skipped, as the Redis tests are.
/// </summary>
internal sealed class KafkaEnv : IAsyncDisposable
{
    /// <summary>The broker under test: the local stack's, unless
    /// <c>INCREMENTAL_KAFKA_BOOTSTRAP</c> names another (<c>host:port</c>).</summary>
    public static readonly string Bootstrap =
        Environment.GetEnvironmentVariable("INCREMENTAL_KAFKA_BOOTSTRAP") is { Length: > 0 } configured ? configured : "localhost:9092";
    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);

    private readonly List<KafkaLog> _logs = [];

    private KafkaEnv()
    {
    }

    /// <summary>Unique per test and per run, so reruns never meet old topics.</summary>
    public string Prefix { get; } = "t" + Guid.NewGuid().ToString("N")[..16];

    public static KafkaEnv Create()
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
        return new KafkaEnv();
    }

    public KafkaLogOptions Options(
        int statePartitions = 3,
        int rowPartitions = 2,
        int maxRecordBytes = 900_000,
        TimeSpan? linger = null,
        IReadOnlyDictionary<string, string>? stateTopic = null,
        IReadOnlyDictionary<string, string>? rowsTopic = null,
        IStateSerializer? serializer = null) => new()
        {
            BootstrapServers = Bootstrap,
            TopicPrefix = Prefix,
            ReplicationFactor = 1,
            StatePartitions = statePartitions,
            TableRowPartitions = rowPartitions,
            MaxRecordBytes = maxRecordBytes,
            GroupCommitLinger = linger ?? TimeSpan.Zero,
            StateTopicConfig = stateTopic ?? new Dictionary<string, string>(),
            TableRowsTopicConfig = rowsTopic ?? new Dictionary<string, string>(),
            Serializer = serializer ?? JsonStateSerializer.Instance,
        };

    /// <summary>A process's connection: a reader until it acquires the writer.</summary>
    public async Task<KafkaLog> ConnectAsync(KafkaLogOptions? options = null)
    {
        var log = await KafkaLog.ConnectAsync(options ?? Options()).WaitAsync(Timeout);
        _logs.Add(log);
        return log;
    }

    /// <summary>A connection that is the writer: a cold-started active.</summary>
    public async Task<KafkaLog> ConnectWriterAsync(KafkaLogOptions? options = null)
    {
        var log = await ConnectAsync(options);
        await log.AcquireWriterAsync().WaitAsync(Timeout);
        return log;
    }

    /// <summary>Every committed record of a state partition, read to the end of committed
    /// data as it stands when asked.</summary>
    public static async Task<List<ChangelogRecord>> ReadToEndAsync(IStateChangelog changelog, int partition)
    {
        var end = await changelog.EndOffsetAsync(partition);
        var records = new List<ChangelogRecord>();
        if (end == 0)
            return records;
        using var stop = new CancellationTokenSource(Timeout);
        await foreach (var batch in changelog.ReadAsync(partition, 0, stop.Token))
        {
            records.AddRange(batch.Records);
            if (batch.NextOffset >= end)
                break;
        }
        return records;
    }

    /// <summary>Polls until <paramref name="condition"/> holds. Kafka writes a
    /// transaction's markers just after the commit call returns, so a read made at once can
    /// trail the commit by a moment.</summary>
    public static async Task EventuallyAsync(Func<Task<bool>> condition, string what)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
                Assert.Fail($"Timed out waiting until {what}.");
            await Task.Delay(50);
        }
    }

    /// <summary>Writes a measured number to the test's output and to
    /// <c>measurements.log</c> beside the test assembly, where it can be read after a run
    /// whose console shows only pass and fail.</summary>
    public static void Measured(ITestOutputHelper output, string line)
    {
        output.WriteLine(line);
        lock (typeof(KafkaEnv))
            File.AppendAllText(Path.Combine(AppContext.BaseDirectory, "measurements.log"), $"{DateTime.UtcNow:O} {line}{Environment.NewLine}");
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var log in _logs)
            await log.DisposeAsync();

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
