using System.Globalization;
using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using Incremental.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Scale.Contracts;

namespace Incremental.Service.Scale;

/// <summary>
/// One node of the rig as a real process: a <see cref="ServiceNode"/> on the Kafka backend,
/// wired the way a host would wire it. It talks to the rig over stdout (one JSON event per
/// line) and stdin:
/// <code>
/// status | gc | declare {TableSpec} | deploy {PipelineSpec}
/// | load &lt;from&gt; &lt;count&gt; &lt;batch&gt; &lt;revision&gt;   rows of the parties table, through its writer
/// | churn &lt;rows-per-second&gt; &lt;key-space&gt;          keep changing rows until told 0 or no longer leading
/// | takeover [force] | stepdown [force] | quit
/// </code>
/// Exit codes are the service's: 0 asked to stop, 3 fenced, 4 resigned, 5 cannot lead; 9
/// for a failure of this program.
/// </summary>
internal static class NodeHost
{
    public static async Task<int> RunAsync(Arguments arguments)
    {
        var nodeId = arguments.Text("--node");
        var probePort = arguments.Number("--probe-port", 0);
        var peerPort = arguments.Number("--peer-port", 0);
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
            var level = Enum.Parse<LogLevel>(arguments.Text("--log-level", "Warning"));
            services.AddLogging(b => b.SetMinimumLevel(level).AddProvider(new StderrLogs(level)));
            services.AddSingleton<IHostApplicationLifetime>(lifetime);
            var clientConfig = Security.ClientConfig(arguments);
            services.AddIncrementalService(o =>
                {
                    o.PluginDirectory = arguments.Text("--plugins") is { Length: > 0 } plugins ? plugins : null;
                    o.CacheDirectory = arguments.Text("--cache");
                    o.AdvertisedAdminUrl = $"http://{nodeId}.invalid/admin";
                })
                .UseKafkaBackend(new KafkaServiceBackendOptions
                {
                    Log = new KafkaLogOptions
                    {
                        BootstrapServers = arguments.Text("--bootstrap", "localhost:9092"),
                        TopicPrefix = arguments.Text("--prefix"),
                        NodeId = nodeId,
                        HeartbeatInterval = TimeSpan.FromMilliseconds(arguments.Number("--heartbeat-ms", 1000)),
                        ReplicationFactor = (short)arguments.Number("--replication", 1),
                        StatePartitions = arguments.Number("--state-partitions", 12),
                        TableRowPartitions = arguments.Number("--table-partitions", 4),
                        ClientConfig = clientConfig,
                        ConfigureProducer = Security.Producer(arguments),
                        ConfigureConsumer = Security.Consumer(arguments),
                        ConfigureAdmin = Security.Admin(arguments),
                        Logger = new StderrLogs(level).CreateLogger("Incremental.Kafka"),
                    },
                    TakeoverAfter = TimeSpan.FromMilliseconds(arguments.Number("--takeover-after-ms", 3000)),
                    ProbeListen = probePort > 0 ? new IPEndPoint(IPAddress.Loopback, probePort) : null,
                    PeerProbe = peerPort > 0 ? ("127.0.0.1", peerPort) : null,
                })
                .AddContracts(typeof(Party).Assembly);

            await using var provider = services.BuildServiceProvider();
            var node = provider.GetRequiredService<ServiceNode>();
            node.Backend.Leadership.Changed += status =>
                Emit("role", new { role = status.Role.ToString(), term = status.Term, leader = status.LeaderNodeId, leaderTerm = status.LeaderTerm });
            await node.StartAsync(default);
            Emit("started", new { role = node.Status.Leadership.Role.ToString(), pid = Environment.ProcessId });

            static double Ms(TimeSpan span) => Math.Round(span.TotalMilliseconds, 1);

            object Status()
            {
                var status = node.Status;
                var kafka = status.Leadership.Kafka;
                var service = status.LastTakeover;
                return new
                {
                    role = status.Leadership.Role.ToString(),
                    term = status.Leadership.Term,
                    leader = status.Leadership.LeaderNodeId,
                    health = status.Health.Level.ToString(),
                    reasons = status.Health.Reasons,
                    warm = status.Standby?.TakeoverWouldBeWarm ?? false,
                    notWarm = status.Standby?.NotWarmBecause ?? [],
                    stateLag = status.Standby?.StateLagRecords ?? 0,
                    peerReady = status.Peer?.Ready ?? false,
                    memory = Memory(false),
                    tokens = Interlocked.Read(ref Security.TokensIssued),
                    takeover = kafka?.LastTakeover is { } t
                        ? new
                        {
                            reason = t.Reason,
                            silenceMs = Ms(t.Silence),
                            fenceMs = Ms(t.Fence),
                            prepareMs = Ms(t.Prepare),
                            storageMs = Ms(t.Storage),
                            startMs = Ms(t.Start),
                            totalMs = Ms(t.Total),
                            catalogMs = service is null ? 0 : Ms(service.Catalog),
                            tablesMs = service is null ? 0 : Ms(service.Tables),
                            leaderRecordMs = service is null ? 0 : Ms(service.LeaderRecord),
                            pluginDirectoryMs = service is null ? 0 : Ms(service.PluginDirectory),
                            pipelinesMs = service is null ? 0 : Ms(service.Pipelines),
                            finishMs = service is null ? 0 : Ms(service.Finish + service.TableSeeds + service.PipelineSeeds),
                        }
                        : null,
                    pipelines = node.Pipelines.Pipelines.Select(p => new
                    {
                        id = p.Id,
                        definition = p.Definition,
                        state = p.State.ToString(),
                        read = p.Sources.Sum(s => s.Stats.Records),
                        lag = p.Sources.Sum(s => s.Stats.Lag ?? 0),
                        lagKnown = p.Sources.Count > 0 && p.Sources.All(s => s.Stats.Lag is not null),
                        step = p.CommittedStep ?? 0,
                        restarts = p.Restarts,
                        fault = p.Faults.LastOrDefault()?.Message,
                        start = p.LastStart is { } s
                            ? new { pluginMs = Ms(s.Plugin), buildMs = Ms(s.Build), startMs = Ms(s.Start), edgesMs = Ms(s.Edges), totalMs = Ms(s.Total), warm = s.Warm }
                            : null,
                    }),
                    tables = node.Tables.Tables.Select(t =>
                    {
                        var stats = node.Tables.Stats(t.Name);
                        return new { name = t.Name, state = t.State.ToString(), rows = stats.Rows, version = stats.Version, following = stats.Following, lag = stats.LagVersions };
                    }),
                };
            }

            CancellationTokenSource? churn = null;

            async Task RunCommandAsync(string line)
            {
                var space = line.IndexOf(' ');
                var command = space < 0 ? line : line[..space];
                var rest = space < 0 ? "" : line[(space + 1)..];
                var parts = rest.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                try
                {
                    switch (command)
                    {
                        case "status":
                            Emit("status", Status());
                            break;
                        case "gc":
                            Emit("gc", Memory(true));
                            break;
                        case "declare":
                            var table = JsonSerializer.Deserialize<TableSpec>(rest, json)!;
                            await node.Tables.DeclareAsync(table, default, "rig");
                            Emit("declared", new { table = table.Name });
                            break;
                        case "deploy":
                            var spec = JsonSerializer.Deserialize<PipelineSpec>(rest, json)!;
                            var deployed = await node.Pipelines.DeployAsync(spec, new DeployOptions(Caller: "rig"), default);
                            Emit("deployed", new { pipeline = deployed.Spec.Id });
                            break;
                        case "load":
                        {
                            long from = long.Parse(parts[0], CultureInfo.InvariantCulture), count = long.Parse(parts[1], CultureInfo.InvariantCulture);
                            int batch = int.Parse(parts[2], CultureInfo.InvariantCulture), revision = int.Parse(parts[3], CultureInfo.InvariantCulture);
                            var writer = node.Tables.Get<long, Party>(Reference.Parties.Name).Writer;
                            var clock = System.Diagnostics.Stopwatch.StartNew();
                            for (long id = from, staged = 0; id < from + count; id++)
                            {
                                await writer.UpsertAsync(Party.Make(id, revision));
                                if (++staged % batch == 0 || id == from + count - 1)
                                    await writer.CommitAsync();
                            }
                            Emit("loaded", new { rows = count, ms = clock.ElapsedMilliseconds });
                            break;
                        }
                        case "churn":
                        {
                            churn?.Cancel();
                            var rate = int.Parse(parts[0], CultureInfo.InvariantCulture);
                            if (rate > 0)
                            {
                                var keys = long.Parse(parts[1], CultureInfo.InvariantCulture);
                                var stop = churn = new CancellationTokenSource();
                                _ = Task.Run(async () =>
                                {
                                    var writer = node.Tables.Get<long, Party>(Reference.Parties.Name).Writer;
                                    var random = new Random(7);
                                    var revision = 1;
                                    try
                                    {
                                        while (!stop.IsCancellationRequested && node.IsLeader)
                                        {
                                            for (var i = 0; i < Math.Max(1, rate / 5); i++)
                                                await writer.UpsertAsync(Party.Make(random.NextInt64(keys), revision));
                                            await writer.CommitAsync();
                                            revision++;
                                            await Task.Delay(200);
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        if (node.IsLeader)
                                            Emit("error", new { command = "churn", type = ex.GetType().Name, message = ex.Message });
                                    }
                                });
                            }
                            Emit("churning", new { rate });
                            break;
                        }
                        case "takeover":
                            await node.TakeOverAsync(new NodeActionOptions(Force: rest == "force", Caller: "rig"), default);
                            Emit("tookover");
                            break;
                        case "stepdown":
                            await node.StepDownAsync(new NodeActionOptions(Force: rest == "force", Caller: "rig"), default);
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
                lifetime.StopApplication(); // the rig went away
            });

            await lifetime.Stopped;
            Emit("exiting", new { code = Environment.ExitCode, role = node.Status.Leadership.Role.ToString() });
            using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await node.StopAsync(limit.Token);
            return Environment.ExitCode;
        }
        catch (Exception ex)
        {
            Emit("crashed", new { type = ex.GetType().Name, message = ex.Message });
            Console.Error.WriteLine(ex);
            return 9;
        }
    }

    /// <summary>The process's memory; with <paramref name="collect"/>, after a full blocking
    /// collection, so that the heap figure is what is live and not what is waiting.</summary>
    private static object Memory(bool collect)
    {
        if (collect)
        {
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        }
        var info = GC.GetGCMemoryInfo();
        return new
        {
            heapMb = GC.GetTotalMemory(false) / 1048576,
            committedMb = info.TotalCommittedBytes / 1048576,
            rssMb = Environment.WorkingSet / 1048576,
            pausePercent = info.PauseTimePercentage,
        };
    }
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

internal sealed class StderrLogs(LogLevel minimum) : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName) => new Logger(categoryName, minimum);

    public void Dispose()
    {
    }

    private sealed class Logger(string category, LogLevel minimum) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= minimum;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Console.Error.WriteLine($"{DateTime.UtcNow:HH:mm:ss.fff} {logLevel} {category}: {formatter(state, exception)}{(exception is null ? "" : " — " + exception)}");
    }
}

/// <summary><c>--key value</c> pairs.</summary>
internal sealed class Arguments
{
    private readonly Dictionary<string, string> _values;

    public Arguments(IEnumerable<string> args)
    {
        _values = new Dictionary<string, string>(StringComparer.Ordinal);
        var list = args.ToList();
        for (var i = 0; i < list.Count; i++)
        {
            if (!list[i].StartsWith("--", StringComparison.Ordinal))
                throw new ArgumentException($"'{list[i]}' is not an option; options are --name value.");
            if (i + 1 < list.Count && !list[i + 1].StartsWith("--", StringComparison.Ordinal))
                _values[list[i]] = list[++i];
            else
                _values[list[i]] = "true";
        }
    }

    private Arguments(Dictionary<string, string> values) => _values = values;

    /// <summary>These arguments with one of them set to another value.</summary>
    public Arguments With(string name, string value) => new(new Dictionary<string, string>(_values, StringComparer.Ordinal) { [name] = value });

    public bool Has(string name) => _values.ContainsKey(name);
    public string Text(string name, string fallback = "") => _values.GetValueOrDefault(name, fallback);
    public int Number(string name, int fallback) => _values.TryGetValue(name, out var value) ? int.Parse(value, NumberStyles.AllowThousands, CultureInfo.InvariantCulture) : fallback;
    public long Long(string name, long fallback) => _values.TryGetValue(name, out var value) ? long.Parse(value, NumberStyles.AllowThousands, CultureInfo.InvariantCulture) : fallback;
}

/// <summary>
/// What every Kafka client of a run carries. Nothing on a plaintext broker. With
/// <c>--oauth principal</c>: SASL/OAUTHBEARER, with the token supplied only by a refresh
/// handler set on each client's builder — which is how MSK's IAM tokens arrive too. The
/// client configuration alone names the mechanism and carries no credential, so a client
/// the service made without the connection's <c>ConfigureProducer</c>,
/// <c>ConfigureConsumer</c> or <c>ConfigureAdmin</c> never authenticates, and the run
/// fails. The token is an unsigned JWT (the <c>kafka-sasl</c> compose profile accepts
/// those) that lives <c>--oauth-lifetime-seconds</c>, so a run longer than that also
/// proves the refresh.
/// </summary>
internal static class Security
{
    public static Dictionary<string, string> ClientConfig(Arguments arguments)
    {
        var config = new Dictionary<string, string>(StringComparer.Ordinal);
        if (arguments.Has("--oauth"))
        {
            config["security.protocol"] = "SASL_PLAINTEXT";
            config["sasl.mechanism"] = "OAUTHBEARER";
        }
        return config;
    }

    /// <summary>Tokens handed out by this process, across all its clients.</summary>
    public static long TokensIssued;

    private static void Refresh(Arguments arguments, Confluent.Kafka.IClient client)
    {
        try
        {
            var principal = arguments.Text("--oauth");
            var now = DateTimeOffset.UtcNow;
            var expires = now.AddSeconds(arguments.Number("--oauth-lifetime-seconds", 20));
            static string Part(string json) => Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(json)).TrimEnd('=').Replace('+', '-').Replace('/', '_');
            var token = Part("""{"alg":"none"}""") + "." +
                        Part($$"""{"sub":"{{principal}}","iat":{{now.ToUnixTimeSeconds()}},"exp":{{expires.ToUnixTimeSeconds()}}}""") + ".";
            Confluent.Kafka.ClientExtensions.OAuthBearerSetToken(client, token, expires.ToUnixTimeMilliseconds(), principal);
            Interlocked.Increment(ref TokensIssued);
        }
        catch (Exception ex)
        {
            Confluent.Kafka.ClientExtensions.OAuthBearerSetTokenFailure(client, ex.Message);
        }
    }

    public static Action<Confluent.Kafka.ProducerBuilder<byte[], byte[]>>? Producer(Arguments arguments) =>
        arguments.Has("--oauth") ? builder => builder.SetOAuthBearerTokenRefreshHandler((client, _) => Refresh(arguments, client)) : null;

    public static Action<Confluent.Kafka.ConsumerBuilder<byte[], byte[]>>? Consumer(Arguments arguments) =>
        arguments.Has("--oauth") ? builder => builder.SetOAuthBearerTokenRefreshHandler((client, _) => Refresh(arguments, client)) : null;

    public static Action<Confluent.Kafka.AdminClientBuilder>? Admin(Arguments arguments) =>
        arguments.Has("--oauth") ? builder => builder.SetOAuthBearerTokenRefreshHandler((client, _) => Refresh(arguments, client)) : null;

    /// <summary>The same settings as node arguments, for the processes the rig starts.</summary>
    public static IEnumerable<string> Forward(Arguments arguments) =>
        arguments.Has("--oauth")
            ? ["--oauth", arguments.Text("--oauth"), "--oauth-lifetime-seconds", arguments.Number("--oauth-lifetime-seconds", 20).ToString(CultureInfo.InvariantCulture)]
            : [];
}
