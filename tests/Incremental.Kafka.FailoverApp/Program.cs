// One node of the kill -9 failover tests (KafkaFailoverProcessTests): a real process that
// follows the Kafka logs as a standby and leads when its KafkaLeadership says so. The
// pipeline, its input and the answer it must reach live in FailoverScenario, source-shared
// with the test assembly. This file is the wiring a service would write: connect, build as
// a standby, let the leadership decide, exit when fenced.
//
// It talks to the test over stdout (one JSON event per line) and stdin (takeover,
// stepdown, status, quit). Exit codes: 0 asked to stop or stepped down, 3 fenced,
// 5 anything else.
using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Incremental;
using Incremental.Kafka;
using Incremental.Kafka.FailoverApp;
using Incremental.Storage;
using Incremental.Tables;
using static Incremental.Kafka.FailoverApp.FailoverScenario;

var arguments = ParseArgs(args);
var bootstrap = arguments["--bootstrap"];
var prefix = arguments["--prefix"];
var nodeId = arguments["--node"];
var totalSteps = long.Parse(arguments["--steps"], CultureInfo.InvariantCulture);
var stepDelay = TimeSpan.FromMilliseconds(Number("--step-delay-ms", 10));
var takeoverAfter = TimeSpan.FromMilliseconds(Number("--takeover-after-ms", 3000));
var heartbeat = TimeSpan.FromMilliseconds(Number("--heartbeat-ms", 1000));
var probePort = Number("--probe-port", 0);
var peerPort = Number("--peer-port", 0);
var sinkTopic = prefix + ".sink";

var emitLock = new object();
void Emit(string kind, object? data = null)
{
    var line = new Dictionary<string, object?> { ["e"] = kind, ["node"] = nodeId, ["t"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() };
    if (data is not null)
    {
        foreach (var property in JsonSerializer.SerializeToElement(data, Json).EnumerateObject())
            line[property.Name] = property.Value;
    }
    lock (emitLock)
        Console.Out.WriteLine(JsonSerializer.Serialize(line, Json));
}

try
{
    await using var log = await KafkaLog.ConnectAsync(new KafkaLogOptions
    {
        BootstrapServers = bootstrap,
        TopicPrefix = prefix,
        NodeId = nodeId,
        HeartbeatInterval = heartbeat,
        ReplicationFactor = 1,
        StatePartitions = 3,
        TableRowPartitions = 2,
    });

    // The sink is outside the logs: its own topic, its own producer, no transaction. It
    // stands for any external store that takes keyed upserts, so fencing does not cover it.
    using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrap }).Build())
    {
        try
        {
            await admin.CreateTopicsAsync([new TopicSpecification
            {
                Name = sinkTopic, NumPartitions = 1, ReplicationFactor = 1,
                Configs = new Dictionary<string, string> { ["cleanup.policy"] = "compact" },
            }]);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
        }
    }
    using var sinkProducer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
    {
        BootstrapServers = bootstrap, EnableIdempotence = true, Acks = Acks.All, LingerMs = 0,
    }).Build();

    // ── the standby: everything built, nothing started ─────────────────────────────────
    KafkaLeadership leadership = null!;
    SharedTable<int, Product> products = null!;
    Circuit circuit = null!;
    AcknowledgedOutput<Enriched> enrichedOutput = null!;
    AcknowledgedOutput<KeyedValue<int, long>> totalsOutput = null!;
    AcknowledgedOutput<KeyedValue<int, long>> countsOutput = null!;
    KeyedInputStream<int, Order>? ordersInput = null;
    InputStream<Event>? eventsInput = null;
    Task? build = null;

    async Task BuildAsync()
    {
        // On a deployment's first start this waits until some node — possibly this one —
        // is the writer and has created the table.
        var tableLog = await log.OpenTableLogAsync<int, Product>(ProductsTable);
        products = await SharedTable<int, Product>.FollowAsync(ProductsTable, p => p.Id, tableLog);
        var storage = ChangelogStorage.Follow(log.State);
        leadership.Track(products, tableLog);
        leadership.Track(storage);

        var builder = new CircuitBuilder(new CircuitOptions
        {
            Name = Pipeline,
            AutoStep = false,
            Workers = 1,
            Storage = storage.Pipeline(Pipeline),
            CheckpointInterval = CheckpointInterval,
        });
        var orders = builder.KeyedInput<int, Order>(OrdersInput, o => o.Id);
        var enriched = orders.GroupBy(o => o.Product)
            .Join(builder.Table(products), (_, o, p) => new Enriched(o.Id, o.Customer, (long)o.Qty * p.Price));
        enrichedOutput = enriched.ToAcknowledgedOutput();
        totalsOutput = enriched.GroupBy(e => e.Customer).Sum(e => e.Value).ToAcknowledgedOutput();
        var events = builder.Input<Event>(EventsInput);
        countsOutput = events.GroupBy(e => e.Bucket).Count().ToAcknowledgedOutput();
        ordersInput = orders;
        eventsInput = events;
        circuit = builder.Build();

        await storage.WhenCaughtUpAsync();
        Emit("ready");
    }

    // ── the leader: the generator is the source, resumed at the committed offset ───────
    var stopRequested = false;
    var run = Task.CompletedTask;
    var firstOutput = 0;

    async Task RunAsync(long term)
    {
        try
        {
            var handle = circuit.Handle(ordersInput!);
            var eventsHandle = circuit.Handle(eventsInput!);
            var next = (circuit.CommittedOffset(OrdersInput) ?? -1) + 1;
            if (next % Batch != 0)
                throw new InvalidOperationException($"The committed offset {next - 1} is not the end of a step.");
            Emit("leading", new { term, resumedAt = next });

            long? committed = circuit.CommittedStep;
            for (var step = next / Batch; step < totalSteps; step++)
            {
                // Stop only where nothing is uncommitted, so the successor's checkpoints
                // fall on the same steps this node's did.
                if (Volatile.Read(ref stopRequested) && step % CheckpointInterval == 0)
                    return;

                var wrote = false;
                foreach (var (id, row) in ProductsBefore(step))
                {
                    if (row is null)
                        await products.Writer.DeleteAsync(id);
                    else
                        await products.Writer.UpsertAsync(row);
                    wrote = true;
                }
                if (wrote)
                    await products.Writer.CommitAsync();

                for (var offset = step * Batch; offset < (step + 1) * Batch; offset++)
                {
                    var (id, row) = OrderAt(offset);
                    if (row is null)
                        await handle.DeleteAsync(id, offset);
                    else
                        await handle.UpsertAsync(row, offset);
                    await eventsHandle.InsertAsync(EventAt(offset), offset);
                }
                await circuit.StepAsync();
                if (circuit.CommittedStep != committed)
                {
                    committed = circuit.CommittedStep;
                    Emit("progress", new { step, committed = circuit.CommittedOffset(OrdersInput) });
                }
                await Task.Delay(stepDelay);
            }
            Emit("done", new { term, committed = circuit.CommittedOffset(OrdersInput) });
        }
        catch (Exception ex)
        {
            // A fenced leader's commit or delivery fails here. The leadership says so a
            // moment later and the process exits 3; anything else is a bug.
            Emit("fault", new { error = ex.ToString() });
            if (await Task.WhenAny(leadership.WhenFinished, Task.Delay(TimeSpan.FromSeconds(10))) != leadership.WhenFinished)
                Environment.Exit(5);
        }
    }

    IUpsertSink<TKey, TRow> Sink<TKey, TRow>(string name) where TKey : notnull =>
        leadership.Leased(new TopicSink<TKey, TRow>(sinkProducer, sinkTopic, name, leadership, () =>
        {
            if (Interlocked.Exchange(ref firstOutput, 1) == 0)
                Emit("first-output", new { term = leadership.Term });
        }));

    leadership = new KafkaLeadership(log, new KafkaLeadershipOptions
    {
        TakeoverAfter = takeoverAfter,
        ProbeLeader = peerPort > 0 ? LeadershipProbe.Tcp("127.0.0.1", peerPort) : null,
        ProbeTimeout = TimeSpan.FromMilliseconds(Math.Min(1000, takeoverAfter.TotalMilliseconds / 2)),
        LagSampleInterval = TimeSpan.FromSeconds(1),
        PrepareAsync = _ => build!,
        StartLeadingAsync = (term, _) =>
        {
            // The deliveries end with the circuit; a failed one faults it, which the run
            // loop reports.
            circuit.Start();
            var delivery = new SinkDeliveryOptions { MaxRetries = 0 };
            circuit.Handle(enrichedOutput).DeliverToAsync(Sink<int, Enriched>(EnrichedSink), e => e.OrderId, delivery)
                .ContinueWith(static t => t.Exception, TaskScheduler.Default);
            circuit.Handle(totalsOutput).DeliverToAsync(Sink<int, long>(TotalsSink), delivery)
                .ContinueWith(static t => t.Exception, TaskScheduler.Default);
            circuit.Handle(countsOutput).DeliverToAsync(Sink<int, long>(CountsSink), delivery)
                .ContinueWith(static t => t.Exception, TaskScheduler.Default);
            run = Task.Run(() => RunAsync(term));
            return Task.CompletedTask;
        },
        StopLeadingAsync = async _ =>
        {
            Volatile.Write(ref stopRequested, true);
            await run;
            await circuit.DisposeAsync();
        },
    });
    leadership.Changed += status =>
    {
        Emit("role", new
        {
            role = status.Role.ToString(),
            term = status.Term,
            leader = status.LeaderNodeId,
            leaderTerm = status.LeaderTerm,
            takeover = status.Role is NodeRole.Leader ? Timings(status.LastTakeover) : null,
            fault = status.Fault?.Message,
        });

        // A fenced active exits; its supervisor brings it back as a standby.
        if (status.Role is NodeRole.Fenced)
            Environment.Exit(3);
    };

    build = Task.Run(BuildAsync);
    await using var probe = probePort > 0 ? LeadershipProbe.Listen(leadership, new IPEndPoint(IPAddress.Loopback, probePort)) : null;
    await leadership.StartAsync();
    Emit("started", new { pid = Environment.ProcessId });

    while (await Console.In.ReadLineAsync() is { } command)
    {
        switch (command.Trim())
        {
            case "takeover":
                _ = leadership.TakeOverAsync().ContinueWith(t => Emit("fault", new { error = t.Exception!.ToString() }), TaskContinuationOptions.OnlyOnFaulted);
                break;
            case "stepdown":
                await leadership.StepDownAsync();
                Emit("resigned", new { committed = circuit.CommittedOffset(OrdersInput) });
                Environment.Exit(0);
                break;
            case "status":
                var status = leadership.Status;
                var lag = status.Role is NodeRole.Standby ? await leadership.MeasureLagAsync() : null;
                Emit("status", new
                {
                    role = status.Role.ToString(),
                    term = status.Term,
                    leader = status.LeaderNodeId,
                    leaderTerm = status.LeaderTerm,
                    heartbeatAgeMs = status.HeartbeatAge.TotalMilliseconds,
                    holdsLease = status.HoldsLease,
                    stateLag = lag?.StateOffsets,
                    tableLag = lag?.TableVersions,
                    sampledLag = status.Lag?.TotalStateOffsets,
                });
                break;
            case "quit":
                Environment.Exit(0);
                break;
        }
    }

    // The test that launched this process is gone.
    Environment.Exit(0);
}
catch (Exception ex)
{
    Emit("fault", new { error = ex.ToString() });
    Environment.Exit(5);
}

int Number(string name, int fallback) =>
    arguments.TryGetValue(name, out var text) ? int.Parse(text, CultureInfo.InvariantCulture) : fallback;

static object? Timings(TakeoverTimings? timings) => timings is null ? null : new
{
    reason = timings.Reason,
    silenceMs = timings.Silence.TotalMilliseconds,
    fenceMs = timings.Fence.TotalMilliseconds,
    prepareMs = timings.Prepare.TotalMilliseconds,
    tablesMs = timings.Tables.TotalMilliseconds,
    storageMs = timings.Storage.TotalMilliseconds,
    startMs = timings.Start.TotalMilliseconds,
    totalMs = timings.Total.TotalMilliseconds,
};

static Dictionary<string, string> ParseArgs(string[] args)
{
    var parsed = new Dictionary<string, string>(StringComparer.Ordinal);
    for (var i = 0; i + 1 < args.Length; i += 2)
        parsed[args[i]] = args[i + 1];
    return parsed;
}

/// <summary>
/// A keyed upsert sink on a Kafka topic: key <c>sink|key</c>, value the row as JSON, a null
/// value for a delete. Each record carries the leader term, the step sequence and the node
/// in headers, so a reader can apply the conditional upsert the design describes — refuse a
/// row from an older term than the one it holds — and the test can see which node wrote what.
/// </summary>
internal sealed class TopicSink<TKey, TRow>(
    IProducer<byte[], byte[]> producer, string topic, string name, KafkaLeadership leadership, Action written) : IUpsertSink<TKey, TRow>
    where TKey : notnull
{
    private readonly List<Task> _pending = [];

    public async ValueTask WriteAsync(UpsertBatch<TKey, TRow> batch, CancellationToken cancellationToken)
    {
        var headers = new Headers
        {
            { "term", Encoding.UTF8.GetBytes(leadership.Term.ToString(CultureInfo.InvariantCulture)) },
            { "seq", Encoding.UTF8.GetBytes(batch.Sequence.ToString(CultureInfo.InvariantCulture)) },
            { "node", Encoding.UTF8.GetBytes(leadership.NodeId) },
        };
        _pending.Clear();
        foreach (var (key, row) in batch.Upserts)
            _pending.Add(producer.ProduceAsync(topic, new Message<byte[], byte[]>
            {
                Key = Key(key), Value = JsonSerializer.SerializeToUtf8Bytes(row, FailoverScenario.Json), Headers = headers,
            }, cancellationToken));
        foreach (var key in batch.Deletes)
            _pending.Add(producer.ProduceAsync(topic, new Message<byte[], byte[]> { Key = Key(key), Value = null!, Headers = headers }, cancellationToken));
        await Task.WhenAll(_pending);
        if (_pending.Count > 0)
            written();
    }

    private byte[] Key(TKey key) => Encoding.UTF8.GetBytes($"{name}|{JsonSerializer.Serialize(key, FailoverScenario.Json)}");
}
