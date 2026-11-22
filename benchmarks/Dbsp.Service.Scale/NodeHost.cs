using System.Globalization;
using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using Dbsp.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Scale.Contracts;

namespace Dbsp.Service.Scale;

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

        GcLog.Started ??= new GcLog();
        try
        {
            var lifetime = new Lifetime();
            var services = new ServiceCollection();
            var level = Enum.Parse<LogLevel>(arguments.Text("--log-level", "Warning"));
            services.AddLogging(b => b.SetMinimumLevel(level).AddProvider(new StderrLogs(level)));
            services.AddSingleton<IHostApplicationLifetime>(lifetime);
            var clientConfig = Security.ClientConfig(arguments);
            foreach (var pair in arguments.Text("--client").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                clientConfig[pair[..pair.IndexOf('=')]] = pair[(pair.IndexOf('=') + 1)..];
            services.AddDbspService(o =>
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
                        Logger = new StderrLogs(level).CreateLogger("Dbsp.Kafka"),
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
                    released = node.MemoryRelease,
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
                        stateEntries = p.StateSize?.Entries ?? 0,
                        stateBytes = p.StateSize?.EstimatedBytes ?? 0,
                        stateHeldTwice = p.StateSize?.EntriesHeldTwice ?? 0,
                        restarts = p.Restarts,
                        fault = p.Faults.LastOrDefault()?.Message,
                        start = p.LastStart is { } s
                            ? new { pluginMs = Ms(s.Plugin), buildMs = Ms(s.Build), startMs = Ms(s.Start), edgesMs = Ms(s.Edges), totalMs = Ms(s.Total), warm = s.Warm }
                            : null,
                    }),
                    tables = node.Tables.Tables.Select(t =>
                    {
                        var stats = node.Tables.Stats(t.Name);
                        return new
                        {
                            name = t.Name, state = t.State.ToString(), rows = stats.Rows, version = stats.Version, following = stats.Following, lag = stats.LagVersions,
                            memoryBytes = stats.Memory?.TotalBytes ?? 0, rowBytes = stats.Memory?.RowBytes ?? 0, indexBytes = stats.Memory?.IndexBytes ?? 0,
                            historyBytes = stats.Memory?.HistoryBytes ?? 0, meanRowBytes = stats.Memory?.MeanRowBytes ?? 0,
                        };
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
                        case "mark":
                            Stalls.Mark();
                            Emit("marked");
                            break;
                        case "age":
                        {
                            // An experiment's question: does the collector give back what a
                            // burst left it holding once some collections have gone by?
                            var count = int.Parse(parts[0], CultureInfo.InvariantCulture);
                            var generation = parts.Length > 1 ? int.Parse(parts[1], CultureInfo.InvariantCulture) : 0;
                            var background = parts.Length > 2 && parts[2] == "bg";
                            var before = GC.GetGCMemoryInfo().TotalCommittedBytes / 1048576;
                            var pauseBefore = GC.GetTotalPauseDuration();
                            for (var i = 0; i < count; i++)
                            {
                                GC.Collect(generation, GCCollectionMode.Forced, blocking: !background);
                                await Task.Delay(background ? 3000 : 100);
                            }
                            var soon = GC.GetGCMemoryInfo().TotalCommittedBytes / 1048576;
                            await Task.Delay(5000);
                            Emit("aged", new
                            {
                                count,
                                generation,
                                background,
                                committedBeforeMb = before,
                                committedAfterMb = soon,
                                committedFiveSecondsLaterMb = GC.GetGCMemoryInfo().TotalCommittedBytes / 1048576,
                                rssMb = Environment.WorkingSet / 1048576,
                                pauseMs = (GC.GetTotalPauseDuration() - pauseBefore).TotalMilliseconds,
                            });
                            break;
                        }
                        case "trim":
                        {
                            // An experiment's question: how much of what the C allocator
                            // holds is free memory it has not given back?
                            var before = Malloc.Info();
                            var rssBefore = Environment.WorkingSet / 1048576;
                            var clock = System.Diagnostics.Stopwatch.StartNew();
                            Malloc.malloc_trim(0);
                            var ms = clock.Elapsed.TotalMilliseconds;
                            var after = Malloc.Info();
                            Emit("trimmed", new
                            {
                                ms,
                                rssBeforeMb = rssBefore,
                                rssAfterMb = Environment.WorkingSet / 1048576,
                                arenaMb = (long)before.arena / 1048576,
                                mmapMb = (long)before.hblkhd / 1048576,
                                inUseMb = (long)before.uordblks / 1048576,
                                freeMb = (long)before.fordblks / 1048576,
                                freeAfterMb = (long)after.fordblks / 1048576,
                                arenaAfterMb = (long)after.arena / 1048576,
                            });
                            break;
                        }
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
                            // What the load cost the node in stops: the stall meter from
                            // here, and the collections that ran meanwhile.
                            Stalls.Mark();
                            var gen2Before = GC.CollectionCount(2);
                            var pauseBefore = GC.GetTotalPauseDuration();
                            var blockingBefore = GC.GetGCMemoryInfo(GCKind.FullBlocking).Index;
                            var clock = System.Diagnostics.Stopwatch.StartNew();
                            for (long id = from, staged = 0; id < from + count; id++)
                            {
                                await writer.UpsertAsync(Party.Make(id, revision));
                                if (++staged % batch == 0 || id == from + count - 1)
                                    await writer.CommitAsync();
                            }
                            var blocking = GC.GetGCMemoryInfo(GCKind.FullBlocking);
                            Emit("loaded", new
                            {
                                rows = count,
                                ms = clock.ElapsedMilliseconds,
                                longestStallMs = Stalls.SinceMark,
                                gen2 = GC.CollectionCount(2) - gen2Before,
                                pauseMs = Math.Round((GC.GetTotalPauseDuration() - pauseBefore).TotalMilliseconds),
                                // The last full blocking collection, if one ran during the load.
                                lastFullBlockingPauseMs = blocking.Index > blockingBefore && blocking.PauseDurations.Length > 0
                                    ? Math.Round(blocking.PauseDurations[0].TotalMilliseconds) : 0,
                                paced = new { steps = writer.Pacing.Steps, skipped = writer.Pacing.Skipped, ms = Math.Round(writer.Pacing.Collecting.TotalMilliseconds) },
                            });
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
    /// collection, so that the heap figure is what is live and not what is waiting. The
    /// figures are the service's own (<see cref="ProcessMemory"/>) with the generations
    /// beside them.</summary>
    private static object Memory(bool collect)
    {
        if (collect)
        {
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        }
        var info = GC.GetGCMemoryInfo();
        var blocking = GC.GetGCMemoryInfo(GCKind.FullBlocking);
        var memory = ProcessMemory.Read();
        static long Mb(long bytes) => bytes / 1048576;
        var generations = info.GenerationInfo;
        return new
        {
            pid = Environment.ProcessId,
            heapMb = Mb(memory.GcHeapBytes),
            liveMb = Mb(memory.GcLiveBytes),
            fragmentedMb = Mb(memory.GcFragmentedBytes),
            committedMb = Mb(memory.GcCommittedBytes),
            rssMb = Mb(memory.WorkingSetBytes),
            otherMb = memory.OtherBytes is { } other ? Mb(other) : -1,
            pausePercent = memory.GcPausePercent,
            pauseRecentPercent = memory.GcPauseRecentPercent ?? -1,
            pauseMs = (long)GC.GetTotalPauseDuration().TotalMilliseconds,
            uptimeMs = (long)(DateTime.UtcNow - System.Diagnostics.Process.GetCurrentProcess().StartTime.ToUniversalTime()).TotalMilliseconds,
            allocatedMb = Mb(GC.GetTotalAllocatedBytes()),
            gen0 = GC.CollectionCount(0),
            gen1 = GC.CollectionCount(1),
            gen2 = GC.CollectionCount(2),
            // After the last collection: gen0, gen1, gen2, large objects, pinned objects.
            generationsMb = generations.ToArray().Select(g => Mb(g.SizeAfterBytes)).ToArray(),
            gc = memory.Gc?.ToString(),
            // The last full blocking collection — the one kind that stops a node for as
            // long as its heap is large: which collection it was, and how long it paused.
            fullBlockingIndex = blocking.Index,
            fullBlockingPauseMs = blocking.PauseDurations.Length > 0 ? Math.Round(blocking.PauseDurations[0].TotalMilliseconds) : 0,
            // The longest any thread of this process was held up, whatever held it — a
            // timer thread that asks every 5 ms — since the process started, and since
            // the `mark` command was last sent.
            longestStallMs = Stalls.Longest,
            longestStallSinceMarkMs = Stalls.SinceMark,
        };
    }
}

/// <summary>How long the process stood still: a thread that sleeps 5 ms at a time and
/// remembers the longest it overslept. A collection that stops the world shows here as
/// the node's other threads felt it.</summary>
internal static class Stalls
{
    private static long _longestTicks;
    private static long _sinceMarkTicks;

    static Stalls()
    {
        new Thread(() =>
        {
            var last = System.Diagnostics.Stopwatch.GetTimestamp();
            while (true)
            {
                Thread.Sleep(5);
                var now = System.Diagnostics.Stopwatch.GetTimestamp();
                var gap = now - last;
                last = now;
                Raise(ref _longestTicks, gap);
                Raise(ref _sinceMarkTicks, gap);
            }
        }) { IsBackground = true, Name = "stall meter" }.Start();
    }

    private static void Raise(ref long target, long value)
    {
        long seen;
        while (value > (seen = Interlocked.Read(ref target)) && Interlocked.CompareExchange(ref target, value, seen) != seen)
        {
        }
    }

    private static double Ms(long ticks) => Math.Max(0, Math.Round(System.Diagnostics.Stopwatch.GetElapsedTime(0, ticks).TotalMilliseconds - 5, 1));

    public static double Longest => Ms(Interlocked.Read(ref _longestTicks));
    public static double SinceMark => Ms(Interlocked.Read(ref _sinceMarkTicks));
    public static void Mark() => Interlocked.Exchange(ref _sinceMarkTicks, 0);
}

/// <summary>glibc's allocator, which is the Kafka client's.</summary>
internal static class Malloc
{
    [System.Runtime.InteropServices.StructLayout(System.Runtime.InteropServices.LayoutKind.Sequential)]
    public struct Mallinfo2
    {
        public nuint arena, ordblks, smblks, hblks, hblkhd, usmblks, fsmblks, uordblks, fordblks, keepcost;
    }

    [System.Runtime.InteropServices.DllImport("libc")]
    public static extern int malloc_trim(nuint pad);

    [System.Runtime.InteropServices.DllImport("libc")]
    private static extern Mallinfo2 mallinfo2();

    public static Mallinfo2 Info() => mallinfo2();
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

/// <summary>Every collection of the oldest generation, and every pause longer than
/// 200 ms, written to the node's stderr with the runtime's reason and kind: what a
/// stall in the figures was. Kinds: 0 blocking, 1 background, 2 foreground (a young
/// collection inside a background one).</summary>
internal sealed class GcLog : System.Diagnostics.Tracing.EventListener
{
    private readonly Dictionary<string, long> _large = [];
    private long _largeTicks;
    private int _lastDepth = -1, _lastReason = -1, _lastType = -1;
    private uint _lastCount;

    public static GcLog? Started;

    protected override void OnEventSourceCreated(System.Diagnostics.Tracing.EventSource source)
    {
        if (source.Name == "Microsoft-Windows-DotNETRuntime")
            EnableEvents(source, Environment.GetEnvironmentVariable("SCALE_GC_EVERY") == "1" ? System.Diagnostics.Tracing.EventLevel.Verbose : System.Diagnostics.Tracing.EventLevel.Informational, (System.Diagnostics.Tracing.EventKeywords)0x1);
    }

    protected override void OnEventWritten(System.Diagnostics.Tracing.EventWrittenEventArgs e)
    {
        switch (e.EventName)
        {
            case "GCAllocationTick_V4" or "GCAllocationTick_V3":
            {
                var kind = Convert.ToInt32(e.Payload![1], CultureInfo.InvariantCulture);
                if (kind == 1)
                {
                    var name = e.Payload[e.PayloadNames!.IndexOf("TypeName")]?.ToString() ?? "?";
                    var size = e.PayloadNames.IndexOf("ObjectSize") is >= 0 and var i ? Convert.ToInt64(e.Payload[i], CultureInfo.InvariantCulture) : 0;
                    lock (_large)
                    {
                        _large[name] = _large.GetValueOrDefault(name) + 1;
                        if (++_largeTicks % 500 == 0)
                            Console.Error.WriteLine("loh " + string.Join(", ", _large.OrderByDescending(p => p.Value).Take(8).Select(p => $"{p.Key}={p.Value}")) + $" last={size}");
                    }
                }
                break;
            }
            case "GCStart_V2":
                _lastCount = Convert.ToUInt32(e.Payload![0], CultureInfo.InvariantCulture);
                _lastDepth = Convert.ToInt32(e.Payload[1], CultureInfo.InvariantCulture);
                _lastReason = Convert.ToInt32(e.Payload[2], CultureInfo.InvariantCulture);
                _lastType = Convert.ToInt32(e.Payload[3], CultureInfo.InvariantCulture);
                if (_lastDepth == 2)
                    Console.Error.WriteLine($"gc {DateTime.UtcNow:HH:mm:ss.fff} #{_lastCount} gen2 reason={_lastReason} type={_lastType} heap={GC.GetTotalMemory(false) / 1048576} MB");
                break;
            case "GCHeapStats_V2":
            {
                // The end of a collection: its pauses, as the runtime counts them.
                var info = GC.GetGCMemoryInfo(GCKind.Any);
                if (Environment.GetEnvironmentVariable("SCALE_GC_EVERY") == "1")
                {
                    var g = info.GenerationInfo;
                    Console.Error.WriteLine($"gcend {DateTime.UtcNow:HH:mm:ss.fff} #{info.Index} gen{info.Generation} conc={info.Concurrent} pause={(info.PauseDurations.Length > 0 ? info.PauseDurations[0].TotalMilliseconds : 0):F0} heap={info.HeapSizeBytes >> 20} committed={info.TotalCommittedBytes >> 20} frag={info.FragmentedBytes >> 20} g0={g[0].SizeAfterBytes >> 20}/{g[0].SizeBeforeBytes >> 20} g1={g[1].SizeAfterBytes >> 20}/{g[1].SizeBeforeBytes >> 20} g2={g[2].SizeAfterBytes >> 20}/{g[2].SizeBeforeBytes >> 20} loh={g[3].SizeAfterBytes >> 20}/{g[3].SizeBeforeBytes >> 20} promoted={info.PromotedBytes >> 20} load={info.MemoryLoadBytes >> 20}/{info.HighMemoryLoadThresholdBytes >> 20} total={info.TotalAvailableMemoryBytes >> 20}");
                }
                foreach (var pause in info.PauseDurations)
                    if (pause.TotalMilliseconds > 200)
                        Console.Error.WriteLine($"gc {DateTime.UtcNow:HH:mm:ss.fff} pause {pause.TotalMilliseconds:F0} ms in #{info.Index} gen{info.Generation} concurrent={info.Concurrent} compacted={info.Compacted} heap={info.HeapSizeBytes / 1048576} MB load={info.MemoryLoadBytes / 1048576} MB of {info.HighMemoryLoadThresholdBytes / 1048576}");
                break;
            }
        }
    }
}
