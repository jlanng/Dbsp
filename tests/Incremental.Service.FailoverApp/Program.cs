// One node of the service's kill -9 failover tests (KafkaServiceProcessTests): a real
// process running a ServiceNode on the Kafka backend, the way a host would wire it —
// AddIncrementalService, UseKafkaBackend, a contract compiled in, a plugin directory.
// Everything else comes from the catalog. The test starts it twice.
//
// It talks to the test over stdout (one JSON event per line) and stdin:
//   status | declare {TableSpec} | deploy {PipelineSpec} | fill <table-prefix> <rows>
//   | takeover | stepdown | quit
// Exit codes are the service's: 0 asked to stop, 3 fenced, 4 resigned, 5 cannot lead;
// 9 for a failure of this program.
using System.Globalization;
using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using Acme.Contracts;
using Incremental.Kafka;
using Incremental.Service;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var arguments = new Dictionary<string, string>(StringComparer.Ordinal);
for (var i = 0; i + 1 < args.Length; i += 2)
    arguments[args[i]] = args[i + 1];
string Argument(string name, string fallback = "") => arguments.GetValueOrDefault(name, fallback);
int Number(string name, int fallback) => int.Parse(Argument(name, fallback.ToString(CultureInfo.InvariantCulture)), CultureInfo.InvariantCulture);

var nodeId = Argument("--node");
var probePort = Number("--probe-port", 0);
var peerPort = Number("--peer-port", 0);
var extraTables = Number("--tables", 0);

var json = new JsonSerializerOptions(JsonSerializerDefaults.Web) { Converters = { new JsonStringEnumConverter() } };
var emitLock = new object();
void Emit(string kind, object? data = null)
{
    var line = new Dictionary<string, object?> { ["e"] = kind, ["node"] = nodeId, ["t"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() };
    if (data is not null)
    {
        foreach (var property in JsonSerializer.SerializeToElement(data, json).EnumerateObject())
            line[property.Name] = property.Value;
    }
    lock (emitLock)
        Console.Out.WriteLine(JsonSerializer.Serialize(line, json));
}

try
{
    var lifetime = new Lifetime();
    var services = new ServiceCollection();
    services.AddLogging(b => b.SetMinimumLevel(LogLevel.Information).AddProvider(new StderrLogs()));
    services.AddSingleton<IHostApplicationLifetime>(lifetime);
    var builder = services.AddIncrementalService(o =>
        {
            o.PluginDirectory = Argument("--plugins") is { Length: > 0 } plugins ? plugins : null;
            o.CacheDirectory = Argument("--cache");
            o.AdvertisedAdminUrl = $"http://{nodeId}.invalid/admin";
        })
        .UseKafkaBackend(new KafkaServiceBackendOptions
        {
            Log = new KafkaLogOptions
            {
                BootstrapServers = Argument("--bootstrap"),
                TopicPrefix = Argument("--prefix"),
                NodeId = nodeId,
                HeartbeatInterval = TimeSpan.FromMilliseconds(Number("--heartbeat-ms", 1000)),
                // 1 on the local broker; the test passes its own through the environment.
                ReplicationFactor = short.TryParse(Environment.GetEnvironmentVariable("INCREMENTAL_KAFKA_REPLICATION"), out var replication) ? replication : (short)1,
                StatePartitions = 3,
                TableRowPartitions = 2,
                // Client settings a test wants to try: "name=value;name=value".
                ClientConfig = (Environment.GetEnvironmentVariable("INCREMENTAL_KAFKA_CLIENT") ?? "")
                    .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                    .Select(pair => pair.Split('=', 2)).ToDictionary(pair => pair[0], pair => pair[1]),
            },
            TakeoverAfter = TimeSpan.FromMilliseconds(Number("--takeover-after-ms", 3000)),
            ProbeListen = probePort > 0 ? new IPEndPoint(IPAddress.Loopback, probePort) : null,
            PeerProbe = peerPort > 0 ? ("127.0.0.1", peerPort) : null,
        })
        .AddContracts(typeof(Instrument).Assembly);

    // Tables that exist only to be promoted: the takeover's table stage is measured on them.
    for (var i = 0; i < extraTables; i++)
        builder.AddTable(Tables.Instruments, name: $"ref-{i:D2}");

    await using var provider = services.BuildServiceProvider();
    var node = provider.GetRequiredService<ServiceNode>();
    node.Backend.Leadership.Changed += status =>
        Emit("role", new { role = status.Role.ToString(), term = status.Term, leader = status.LeaderNodeId, leaderTerm = status.LeaderTerm });
    await node.StartAsync(default);
    Emit("started", new { role = node.Status.Leadership.Role.ToString() });

    object Status()
    {
        var status = node.Status;
        var kafka = status.Leadership.Kafka;
        return new
        {
            role = status.Leadership.Role.ToString(),
            term = status.Leadership.Term,
            leader = status.Leadership.LeaderNodeId,
            leaderTerm = status.Leadership.LeaderTerm,
            holdsLease = kafka?.HoldsLease ?? false,
            fault = kafka?.Fault,
            health = status.Health.Level.ToString(),
            reasons = status.Health.Reasons,
            warm = status.Standby?.TakeoverWouldBeWarm ?? false,
            notWarm = status.Standby?.NotWarmBecause ?? [],
            stateLag = status.Standby?.StateLagRecords ?? 0,
            pluginsPending = status.Standby?.PluginsPending ?? 0,
            catalog = status.CatalogVersion,
            peer = status.Peer,
            takeover = kafka?.LastTakeover is { } t
                ? new
                {
                    reason = t.Reason,
                    silenceMs = t.Silence.TotalMilliseconds,
                    fenceMs = t.Fence.TotalMilliseconds,
                    prepareMs = t.Prepare.TotalMilliseconds,
                    storageMs = t.Storage.TotalMilliseconds,
                    startMs = t.Start.TotalMilliseconds,
                    totalMs = t.Total.TotalMilliseconds,
                    catalogMs = status.LastTakeover?.Catalog.TotalMilliseconds,
                    tablesMs = status.LastTakeover?.Tables.TotalMilliseconds,
                    tableCount = status.LastTakeover?.TableCount,
                    pipelinesMs = status.LastTakeover?.Pipelines.TotalMilliseconds,
                    pipelineCount = status.LastTakeover?.PipelineCount,
                }
                : null,
            pipelines = node.Pipelines.Pipelines.Select(p => new
            {
                id = p.Id,
                state = p.State.ToString(),
                version = p.Version,
                read = p.Sources.Sum(s => s.Stats.Records),
                restarts = p.Restarts,
                fault = p.Faults.LastOrDefault()?.Message,
            }),
            tables = node.Tables.Tables.Select(t =>
            {
                var stats = node.Tables.Stats(t.Name);
                return new { name = t.Name, state = t.State.ToString(), rows = stats.Rows, version = stats.Version, following = stats.Following, lag = stats.LagVersions };
            }),
            plugins = node.Plugins.Plugins.Select(p => new { name = p.Name, version = p.Version, state = p.LoadState }),
        };
    }

    async Task RunCommandAsync(string line)
    {
        var space = line.IndexOf(' ');
        var command = space < 0 ? line : line[..space];
        var rest = space < 0 ? "" : line[(space + 1)..];
        try
        {
            switch (command)
            {
                case "status":
                    Emit("status", Status());
                    break;
                case "declare":
                    var table = JsonSerializer.Deserialize<TableSpec>(rest, json)!;
                    await node.Tables.DeclareAsync(table, default, "test");
                    Emit("declared", new { table = table.Name });
                    break;
                case "deploy":
                    var spec = JsonSerializer.Deserialize<PipelineSpec>(rest, json)!;
                    var deployed = await node.Pipelines.DeployAsync(spec, new DeployOptions(Caller: "test"), default);
                    Emit("deployed", new { pipeline = deployed.Spec.Id, generation = deployed.Spec.Generation, version = deployed.Spec.Version });
                    break;
                case "fill":
                    // fill <table-prefix> <rows>: a version of that many rows in each table.
                    var parts = rest.Split(' ');
                    var rows = int.Parse(parts[1], CultureInfo.InvariantCulture);
                    var filled = 0;
                    foreach (var info in node.Tables.Tables.Where(t => t.Name.StartsWith(parts[0], StringComparison.Ordinal)))
                    {
                        await node.Tables.Get<int, Instrument>(info.Name).Writer.ReplaceAllAsync(
                            Enumerable.Range(1, rows).Select(i => new Instrument(i, "SYM" + i, "ISIN" + i, 1)));
                        filled++;
                    }
                    Emit("filled", new { tables = filled, rows });
                    break;
                case "takeover":
                    await node.TakeOverAsync(new NodeActionOptions(Force: rest == "force", Caller: "test"), default);
                    Emit("tookover");
                    break;
                case "stepdown":
                    await node.StepDownAsync(new NodeActionOptions(Force: rest == "force", Caller: "test"), default);
                    Emit("steppeddown");
                    break;
                case "quit":
                    lifetime.StopApplication();
                    break;
            }
        }
        catch (Exception ex)
        {
            Emit("error", new { command, type = ex.GetType().Name, message = ex.Message, code = (ex as ServiceException)?.Code });
        }
    }

    _ = Task.Run(async () =>
    {
        while (await Console.In.ReadLineAsync() is { } line)
            _ = RunCommandAsync(line.Trim());
        lifetime.StopApplication(); // the test went away
    });

    await lifetime.Stopped;
    Emit("exiting", new { code = Environment.ExitCode, role = node.Status.Leadership.Role.ToString() });
    using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(20));
    await node.StopAsync(limit.Token);
    return Environment.ExitCode;
}
catch (Exception ex)
{
    Emit("crashed", new { type = ex.GetType().Name, message = ex.Message });
    Console.Error.WriteLine(ex);
    return 9;
}

/// <summary>What a generic host would give the node: somewhere to say "stop this process".</summary>
internal sealed class Lifetime : IHostApplicationLifetime
{
    private readonly CancellationTokenSource _started = new();
    private readonly CancellationTokenSource _stopping = new();
    private readonly CancellationTokenSource _stopped = new();
    private readonly TaskCompletionSource _stop = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public CancellationToken ApplicationStarted => _started.Token;
    public CancellationToken ApplicationStopping => _stopping.Token;
    public CancellationToken ApplicationStopped => _stopped.Token;
    public Task Stopped => _stop.Task;

    public void StopApplication() => _stop.TrySetResult();
}

internal sealed class StderrLogs : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName) => new Logger(categoryName);

    public void Dispose()
    {
    }

    private sealed class Logger(string category) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Console.Error.WriteLine($"{DateTime.UtcNow:HH:mm:ss.fff} {logLevel} {category}: {formatter(state, exception)}{(exception is null ? "" : " — " + exception)}");
    }
}
