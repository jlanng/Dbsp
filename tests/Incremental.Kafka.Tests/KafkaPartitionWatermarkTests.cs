using System.Diagnostics;
using Confluent.Kafka;
using Incremental.Storage;
using Xunit;

namespace Incremental.Kafka.Tests;

/// <summary>
/// The watermark over a topic of several partitions (round five, K1). A source names its
/// partitions to the input and says when it stands at the end of one; the input's
/// watermark is the least over the partitions that are not idle. So a partition that is
/// behind the others — a backlog read one partition first, what a restart finds, what a
/// broker's loss delays — holds the watermark where its own records are, and they are not
/// dropped as late.
/// </summary>
public class KafkaPartitionWatermarkTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan Lateness = TimeSpan.FromSeconds(5);
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private sealed record Reading(int Id, int Partition, DateTimeOffset At);

    private sealed class Readings(Circuit circuit, InputStream<Reading> input, Output<Reading> output) : IAsyncDisposable
    {
        public Circuit Circuit => circuit;
        public InputStream<Reading> Input => input;
        private readonly HashSet<int> _seen = [];
        private int _twice;

        /// <summary>Ids that got past the event-time declaration, so far.</summary>
        public int Passed
        {
            get
            {
                while (circuit.Handle(output).Reader.TryRead(out var delta))
                    foreach (var (value, weight) in delta)
                        if (weight != 1 || !_seen.Add(value.Id))
                            _twice++;
                return _seen.Count;
            }
        }

        public int Twice => _twice;

        public ValueTask DisposeAsync() => circuit.DisposeAsync();
    }

    /// <summary>Event time declared on the input (the watermark is kept per partition) or
    /// after a <c>Select</c> (it cannot be: one time for the input, the newest read).</summary>
    private static Readings Circuit(IStorageProvider? storage = null, bool onTheInput = true)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            Name = "readings", Storage = storage ?? new InMemoryStorageProvider(), OutputChannelCapacity = 1 << 16,
        });
        var input = builder.Input<Reading>("readings");
        var timed = onTheInput
            ? input.WithEventTime(r => r.At, Lateness)
            : input.Select(r => r with { }).WithEventTime(r => r.At, Lateness);
        var passed = timed.ToOutput();
        var circuit = builder.Build();
        circuit.Start();
        return new Readings(circuit, input, passed);
    }

    private static KafkaSourceOptions Fast(KafkaEnv env, string topic) => new()
    {
        BootstrapServers = KafkaEnv.Bootstrap,
        Topic = topic,
        IdlePartitionAfter = TimeSpan.FromMilliseconds(500),
        IdlePartitionConfirmEvery = TimeSpan.FromMilliseconds(300),
    };

    /// <summary>
    /// Three partitions with ten minutes of readings each, there before the source starts.
    /// The client hands a backlog over a partition at a time, so by the newest event time
    /// read the second partition's readings are minutes late. Kept per partition, the
    /// watermark waits for the partition that is furthest behind and nothing is late; and
    /// when the source has stood at the end of all three it lets the watermark go to the
    /// newest reading.
    /// </summary>
    [Fact]
    public async Task ABacklogOnSeveralPartitions_IsReadWithoutALateRecord_AndTheWatermarkEndsAtTheNewest()
    {
        const int PerPartition = 600, Partitions = 3;
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("readings", partitions: Partitions);
        for (var partition = 0; partition < Partitions; partition++)
            for (var i = 0; i < PerPartition; i++)
                env.ProduceJson(topic, partition, i, new Reading(partition * PerPartition + i, partition, T0.AddSeconds(i)));
        await env.FlushAsync();

        // What the same backlog costs where the declaration is not on the input.
        await using (var blind = Circuit(onTheInput: false))
        {
            await using var blindSource = await KafkaSource.StartAsync(blind.Circuit, blind.Input, Fast(env, topic)).WaitAsync(Timeout);
            await KafkaEnv.EventuallyAsync(
                () => Task.FromResult(blind.Passed + blind.Circuit.LateRecords == Partitions * PerPartition), "the backlog read, one time for the input");
            KafkaEnv.Measured(output,
                $"a backlog of {Partitions} x {PerPartition} readings, event time declared after a Select: {blind.Circuit.LateRecords} dropped as late");
        }

        await using var readings = Circuit();
        await using var source = await KafkaSource.StartAsync(readings.Circuit, readings.Input, Fast(env, topic)).WaitAsync(Timeout);
        await KafkaEnv.EventuallyAsync(() => Task.FromResult(readings.Passed == Partitions * PerPartition), "every reading through");
        Assert.Equal(0, readings.Circuit.LateRecords);
        Assert.Equal(0, readings.Twice);

        // Idle, all three: the watermark is the newest reading's, less the lateness.
        await KafkaEnv.EventuallyAsync(
            () => Task.FromResult(readings.Circuit.CurrentWatermark == T0.AddSeconds(PerPartition - 1) - Lateness), "the watermark at the newest reading");

        // One partition goes on alone: the others are at their ends and do not hold it.
        env.ProduceJson(topic, 1, 0, new Reading(100_000, 1, T0.AddSeconds(900)));
        await env.FlushAsync();
        await KafkaEnv.EventuallyAsync(
            () => Task.FromResult(readings.Circuit.CurrentWatermark == T0.AddSeconds(900) - Lateness), "the watermark follows the one busy partition");
        Assert.Equal(0, readings.Circuit.LateRecords);
        KafkaEnv.Measured(output,
            $"a backlog of {Partitions} x {PerPartition} readings, event time declared on the input: 0 dropped as late, watermark at the newest once all partitions were idle");
    }

    /// <summary>
    /// A partition with nothing in it has no time and must not hold the watermark for
    /// ever; a partition that is merely quiet holds it for
    /// <see cref="KafkaSourceOptions.IdlePartitionAfter"/> and no longer.
    /// </summary>
    [Fact]
    public async Task AnEmptyPartition_AndAQuietOne_StopHoldingTheWatermark_OnceTheSourceHasStoodAtTheirEnds()
    {
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("readings", partitions: 3);
        await using var readings = Circuit();
        var options = new KafkaSourceOptions
        {
            BootstrapServers = KafkaEnv.Bootstrap, Topic = topic,
            IdlePartitionAfter = TimeSpan.FromSeconds(2), IdlePartitionConfirmEvery = TimeSpan.FromMilliseconds(300),
        };
        await using var source = await KafkaSource.StartAsync(readings.Circuit, readings.Input, options).WaitAsync(Timeout);

        // Partition 0 only. Partitions 1 and 2 are empty: named, never heard from.
        var clock = Stopwatch.StartNew();
        env.ProduceJson(topic, 0, 0, new Reading(1, 0, T0.AddSeconds(100)));
        await env.FlushAsync();
        await KafkaEnv.EventuallyAsync(() => Task.FromResult(readings.Passed == 1), "the reading through");
        Assert.Null(readings.Circuit.CurrentWatermark);
        Assert.Equal("readings", readings.Circuit.WatermarkHeldBy?.Input);
        Assert.NotNull(readings.Circuit.WatermarkHeldBy?.Partition);

        await KafkaEnv.EventuallyAsync(
            () => Task.FromResult(readings.Circuit.CurrentWatermark == T0.AddSeconds(100) - Lateness), "the empty partitions idle");
        Assert.InRange(clock.Elapsed, TimeSpan.FromSeconds(1.5), TimeSpan.FromSeconds(20));
        KafkaEnv.Measured(output, $"two empty partitions stopped holding the watermark {clock.Elapsed.TotalSeconds:F1} s after the start (IdlePartitionAfter 2 s)");
    }

    /// <summary>
    /// What a restart finds. The source had read all three partitions to their ends; while
    /// it was down each received five minutes more. A source that trusted what the
    /// checkpoint says of the partitions' ends would let the first partition it reads move
    /// the watermark past the others' backlog. It names every partition again instead.
    /// </summary>
    [Fact]
    public async Task ARestartWithABacklogOnEveryPartition_DropsNothingAsLate()
    {
        const int Partitions = 3, Before = 100, While = 300;
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("readings", partitions: Partitions);
        var storage = new InMemoryCheckpointStorageProvider();
        for (var partition = 0; partition < Partitions; partition++)
            for (var i = 0; i < Before; i++)
                env.ProduceJson(topic, partition, i, new Reading(partition * 10_000 + i, partition, T0.AddSeconds(i)));
        await env.FlushAsync();

        await using (var first = Circuit(storage))
        {
            await using var source = await KafkaSource.StartAsync(first.Circuit, first.Input, Fast(env, topic)).WaitAsync(Timeout);
            await KafkaEnv.EventuallyAsync(() => Task.FromResult(first.Passed == Partitions * Before), "the first readings through");
            await KafkaEnv.EventuallyAsync(
                () => Task.FromResult(first.Circuit.CurrentWatermark == T0.AddSeconds(Before - 1) - Lateness), "all three partitions idle");
            // Idle is in the checkpoint.
            await KafkaEnv.EventuallyAsync(
                () => Task.FromResult(Enumerable.Range(0, Partitions).All(p => storage.Recover().GetValueOrDefault($"$watermark#s0#i{p}") == 1)),
                "the checkpoint says the partitions are idle");
        }

        for (var partition = 0; partition < Partitions; partition++)
            for (var i = Before; i < Before + While; i++)
                env.ProduceJson(topic, partition, i, new Reading(partition * 10_000 + i, partition, T0.AddSeconds(i)));
        await env.FlushAsync();

        await using var second = Circuit(storage);
        await using var restarted = await KafkaSource.StartAsync(second.Circuit, second.Input, Fast(env, topic)).WaitAsync(Timeout);
        await KafkaEnv.EventuallyAsync(
            () => Task.FromResult(second.Passed + second.Circuit.LateRecords == Partitions * While), "the backlog read");
        Assert.Equal(0, second.Circuit.LateRecords);
        Assert.Equal(Partitions * While, second.Passed);
        await KafkaEnv.EventuallyAsync(
            () => Task.FromResult(second.Circuit.CurrentWatermark == T0.AddSeconds(Before + While - 1) - Lateness), "the watermark at the newest reading");
        KafkaEnv.Measured(output, $"a restart onto a backlog of {Partitions} x {While} readings behind partitions the checkpoint called idle: 0 dropped as late");
    }

    // ── a broker's loss ────────────────────────────────────────────────────────────────
    //
    // On the kafka3 profile, by hand, as KafkaServiceProcessTests' broker tests are:
    //   INCREMENTAL_KAFKA_BOOTSTRAP=localhost:9192,localhost:9292,localhost:9392
    //   INCREMENTAL_KAFKA_REPLICATION=3
    //   INCREMENTAL_KAFKA_BROKERS=incremental-kafka3-1-1,incremental-kafka3-2-1,incremental-kafka3-3-1
    //   dotnet test --project tests/Incremental.Kafka.Tests -- --filter-method "*Broker*"

    private static string[] Brokers =>
        (Environment.GetEnvironmentVariable("INCREMENTAL_KAFKA_BROKERS") ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    private static async Task DockerAsync(params string[] arguments)
    {
        var info = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var argument in arguments)
            info.ArgumentList.Add(argument);
        using var process = Process.Start(info)!;
        var error = process.StandardError.ReadToEndAsync();
        await process.StandardOutput.ReadToEndAsync();
        await process.WaitForExitAsync();
        Assert.True(process.ExitCode == 0, $"docker {string.Join(' ', arguments)}: {await error}");
    }

    /// <summary>
    /// Readings stamped with the time they are made, on six partitions of a topic on three
    /// brokers, a few hundred a second; then one broker is killed (or frozen) for twenty
    /// seconds. The partitions it led have no leader for some seconds: what is produced
    /// to them waits in the producer and arrives when a leader is there again, stamped
    /// with times the other partitions passed seconds ago — more seconds than the
    /// lateness. The source stops calling those partitions idle when they stop answering,
    /// so they hold the watermark, and when their readings come none is late.
    /// </summary>
    [Theory]
    [InlineData("kill")]
    [InlineData("pause")]
    public async Task ABrokerLost_DelaysItsPartitions_AndTheirRecordsAreNotLate(string fault)
    {
        Assert.SkipUnless(
            Brokers.Length >= 3 && KafkaEnv.Replication >= 3,
            "Needs a three-broker cluster whose containers can be stopped: the kafka3 compose profile, with INCREMENTAL_KAFKA_BROKERS naming them and INCREMENTAL_KAFKA_REPLICATION=3.");
        foreach (var each in Brokers)
            await DockerAsync("start", each);

        const int Partitions = 6;
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("readings", partitions: Partitions, config: new Dictionary<string, string> { ["min.insync.replicas"] = "2" });

        // The broker to lose: the leader of partition 0.
        // (A broker started a moment ago may answer without the partition: ask again.)
        var leader = -1;
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = KafkaEnv.Bootstrap }).Build())
            await KafkaEnv.EventuallyAsync(() => Task.FromResult(
                (leader = admin.GetMetadata(topic, TimeSpan.FromSeconds(10)).Topics.FirstOrDefault(t => t.Topic == topic)?
                    .Partitions.FirstOrDefault(p => p.PartitionId == 0)?.Leader ?? -1) >= 1), "partition 0 has a leader");
        var broker = Brokers[leader - 1];

        await using var readings = Circuit();
        // The source's own defaults: this is what a deployment runs with.
        await using var source = await KafkaSource.StartAsync(
            readings.Circuit, readings.Input, new KafkaSourceOptions { BootstrapServers = KafkaEnv.Bootstrap, Topic = topic }).WaitAsync(Timeout);

        // Beside it, the same topic read with one time for the input (the declaration is
        // not on the input): what the loss cost before, measured and not asserted.
        await using var blind = Circuit(onTheInput: false);
        await using var blindSource = await KafkaSource.StartAsync(
            blind.Circuit, blind.Input, new KafkaSourceOptions { BootstrapServers = KafkaEnv.Bootstrap, Topic = topic }).WaitAsync(Timeout);

        using var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
        {
            BootstrapServers = KafkaEnv.Bootstrap, Acks = Acks.All, EnableIdempotence = true, LingerMs = 5,
            MessageTimeoutMs = 120_000, SocketTimeoutMs = 6000, ReconnectBackoffMaxMs = 1000, TopicMetadataRefreshIntervalMs = 2000,
        }).Build();
        var produced = 0;
        var failed = 0;
        using var stop = new CancellationTokenSource();
        var producing = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                for (var partition = 0; partition < Partitions; partition++)
                {
                    var id = Interlocked.Increment(ref produced);
                    producer.Produce(
                        new TopicPartition(topic, partition),
                        new Message<byte[], byte[]> { Key = KafkaEnv.Json(id), Value = KafkaEnv.Json(new Reading(id, partition, DateTimeOffset.UtcNow)) },
                        report => { if (report.Error.IsError) Interlocked.Increment(ref failed); });
                }
                await Task.Delay(20);
            }
        });

        // Running, with every partition busy and the watermark a few seconds behind now.
        await KafkaEnv.EventuallyAsync(() => Task.FromResult(readings.Passed > 1000 && readings.Circuit.CurrentWatermark is not null), "the feed running");
        Assert.Equal(0, readings.Circuit.LateRecords);

        var away = Stopwatch.StartNew();
        await DockerAsync(fault, broker);
        await Task.Delay(TimeSpan.FromSeconds(20));
        await DockerAsync(fault == "pause" ? "unpause" : "start", broker);
        await Task.Delay(TimeSpan.FromSeconds(10));
        stop.Cancel();
        await producing;
        producer.Flush(TimeSpan.FromSeconds(60));

        var total = Volatile.Read(ref produced);
        await KafkaEnv.EventuallyAsync(
            () => Task.FromResult(readings.Passed + readings.Circuit.LateRecords >= total - Volatile.Read(ref failed)), "everything produced read");
        await KafkaEnv.EventuallyAsync(
            () => Task.FromResult(blind.Passed + blind.Circuit.LateRecords >= total - Volatile.Read(ref failed)), "everything produced read by the other circuit");
        var line = $"broker {broker} {fault} for 20 s under {Partitions} partitions, {total} readings stamped as produced, lateness {Lateness.TotalSeconds} s: " +
                   $"{readings.Circuit.LateRecords} dropped as late ({blind.Circuit.LateRecords} with one time for the input), {failed} not delivered by the producer";
        KafkaEnv.Measured(output, line);
        Assert.Equal(0, failed);
        Assert.Equal(0, readings.Circuit.LateRecords);
        Assert.Equal(total, readings.Passed);
        Assert.Equal(0, readings.Twice);
    }
}
