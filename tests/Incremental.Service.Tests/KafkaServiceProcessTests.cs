using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Acme.Contracts;
using static Incremental.Service.Tests.SupervisorTests;

namespace Incremental.Service.Tests;

/// <summary>
/// Failover between REAL operating-system processes: tests/Incremental.Service.FailoverApp,
/// a ServiceNode on the Kafka backend, launched twice as <c>dotnet …FailoverApp.dll</c>.
/// One leads and one stands by, and the test does to the leader what production does —
/// <c>kill -9</c> in the middle of a stream, a takeover from under it, a step-down. The
/// pipelines come from a plugin assembly only the first leader was ever given; the table is
/// loaded from a topic; sources and sinks are topics. Each run ends by reading the sink
/// topics and comparing them with a model computed from what was produced.
/// </summary>
[Collection(KafkaServiceCollection.Name)]
public class KafkaServiceProcessTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(90);
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { Converters = { new JsonStringEnumConverter() } };

    // ── a node: one process, its events, its stdin ─────────────────────────────────────

    private sealed class AppNode : IDisposable
    {
        private readonly Process _process;
        private readonly List<JsonElement> _events = [];
        private readonly StringBuilder _stderr = new();
        private TaskCompletionSource _arrived = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <param name="supervised">The process is the node's supervisor
        /// (<see cref="NodeSupervisor"/>): the node is its child, started again whenever it
        /// ends fenced, resigned or killed. The events are then those of every incarnation
        /// in turn, so ask <see cref="StatusAsync"/> for what is true now.</param>
        /// <param name="appDll">Another build's failover app, for the tests that run two
        /// builds against each other; null is this build's.</param>
        public AppNode(
            KafkaTestEnv env, string id, int probePort, int peerPort, bool plugins = false, int tables = 0, bool supervised = false,
            string? appDll = null)
        {
            Id = id;
            Supervised = supervised;
            var dll = appDll ?? AppDllPath();
            var info = new ProcessStartInfo("dotnet")
            {
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                WorkingDirectory = Path.GetDirectoryName(dll)!,
            };
            foreach (var argument in new[]
            {
                dll,
                "--bootstrap", KafkaTestEnv.Bootstrap,
                "--prefix", env.Prefix,
                "--node", id,
                "--probe-port", probePort.ToString(CultureInfo.InvariantCulture),
                "--peer-port", peerPort.ToString(CultureInfo.InvariantCulture),
                "--tables", tables.ToString(CultureInfo.InvariantCulture),
                "--supervise", supervised ? "1" : "0",
                "--cache", Path.Combine(Path.GetTempPath(), "incremental-service-tests", env.Prefix + "-" + id + "-" + Guid.NewGuid().ToString("N")[..6]),

                // The plugin's package directory is given to one node only: the other must
                // get the plugin through Kafka or not at all.
                "--plugins", plugins ? Path.GetDirectoryName(TestService.PluginDirectory("Acme.Pipelines"))! : "",
            })
                info.ArgumentList.Add(argument);
            _process = Process.Start(info) ?? throw new InvalidOperationException("Failed to start the service failover app.");
            _process.OutputDataReceived += (_, e) =>
            {
                if (e.Data is not { Length: > 0 } line || line[0] != '{')
                    return;
                TaskCompletionSource arrived;
                lock (_events)
                {
                    _events.Add(JsonDocument.Parse(line).RootElement);
                    arrived = _arrived;
                    _arrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                }
                arrived.TrySetResult();
            };
            // INCREMENTAL_NODE_LOGS=<directory> keeps every node's stderr in a file there,
            // the Kafka client's debug lines included; those are too many to hold here.
            var logs = Environment.GetEnvironmentVariable("INCREMENTAL_NODE_LOGS") is { Length: > 0 } directory
                ? new StreamWriter(Path.Combine(Directory.CreateDirectory(directory).FullName, $"{env.Prefix}-{id}-{_process.Id}.log")) { AutoFlush = true }
                : null;
            _process.ErrorDataReceived += (_, e) =>
            {
                lock (_stderr)
                {
                    if (e.Data is null)
                    {
                        logs?.Dispose();
                        return;
                    }
                    logs?.WriteLine(e.Data);
                    if (!e.Data.Contains(" Debug ", StringComparison.Ordinal))
                        _stderr.AppendLine(e.Data);
                }
            };
            _process.BeginOutputReadLine();
            _process.BeginErrorReadLine();
        }

        public string Id { get; }

        public bool Supervised { get; }

        public bool HasExited => _process.HasExited;

        /// <summary>How many times the node's process has started: 1 until its supervisor
        /// has had to start it again.</summary>
        public int Starts => Events("started").Count;

        /// <summary><c>kill -9</c> of the node itself, leaving its supervisor to notice.</summary>
        public void KillNode()
        {
            var pid = Events("started")[^1].GetProperty("pid").GetInt32();
            using var node = Process.GetProcessById(pid);
            node.Kill();
        }

        /// <summary>What the supervisor said, and the exit codes it saw.</summary>
        public List<int> SupervisedExits()
        {
            lock (_stderr)
                return [.. System.Text.RegularExpressions.Regex.Matches(_stderr.ToString(), @"node supervisor: process \d+ ended with exit code (\d+)")
                    .Select(m => int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture))];
        }

        /// <summary>The longest the node's log writer said it had been waiting for the
        /// brokers to take a transaction's records, in seconds; 0 if it never said so.</summary>
        public int LongestWaitForBrokers()
        {
            lock (_stderr)
            {
                var longest = 0;
                foreach (System.Text.RegularExpressions.Match match in System.Text.RegularExpressions.Regex.Matches(
                             _stderr.ToString(), @"has been waiting (\d+) s for the brokers"))
                    longest = Math.Max(longest, int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture));
                return longest;
            }
        }

        public List<JsonElement> Events(string kind)
        {
            lock (_events)
                return _events.Where(e => e.GetProperty("e").GetString() == kind).ToList();
        }

        /// <summary>The first event of <paramref name="kind"/> that satisfies
        /// <paramref name="predicate"/>, waiting for it if it has not arrived. An
        /// <c>error</c> event meanwhile fails the wait: a refused command never answers.</summary>
        public async Task<JsonElement> WaitAsync(string kind, Func<JsonElement, bool>? predicate = null, TimeSpan? timeout = null)
        {
            var clock = Stopwatch.StartNew();
            var limit = timeout ?? Patience;
            var errorsBefore = Events("error").Count;
            while (true)
            {
                Task arrived;
                lock (_events)
                {
                    foreach (var e in _events)
                    {
                        if (e.GetProperty("e").GetString() == kind && (predicate is null || predicate(e)))
                            return e;
                    }
                    arrived = _arrived.Task;
                }
                if (kind != "error" && Events("error").Count > errorsBefore)
                    Assert.Fail($"Node '{Id}' reported an error while '{kind}' was awaited.{Environment.NewLine}{Dump()}");
                if (clock.Elapsed > limit)
                    Assert.Fail($"Node '{Id}' did not report '{kind}' within {limit}.{Environment.NewLine}{Dump()}");
                if (_process.HasExited)
                {
                    await Task.Delay(200); // let the last lines arrive, then look once more
                    lock (_events)
                    {
                        if (!_events.Any(e => e.GetProperty("e").GetString() == kind && (predicate is null || predicate(e))))
                            Assert.Fail($"Node '{Id}' exited with code {_process.ExitCode} before reporting '{kind}'.{Environment.NewLine}{Dump()}");
                    }
                    continue;
                }
                await Task.WhenAny(arrived, Task.Delay(200));
            }
        }

        public Task<JsonElement> WaitForRoleAsync(string role, TimeSpan? timeout = null) =>
            WaitAsync("role", e => e.GetProperty("role").GetString() == role, timeout);

        public async Task<JsonElement> StatusAsync()
        {
            var asked = Events("status").Count;
            Send("status");
            var clock = Stopwatch.StartNew();
            var sent = Stopwatch.StartNew();
            while (Events("status").Count == asked)
            {
                if (clock.Elapsed > Patience)
                    Assert.Fail($"Node '{Id}' did not answer a status request.{Environment.NewLine}{Dump()}");

                // A supervised node that ended with the request unread, or read it before
                // it could answer: its next incarnation is asked.
                if (Supervised && sent.Elapsed > TimeSpan.FromSeconds(2))
                {
                    Send("status");
                    sent.Restart();
                }
                await Task.Delay(20);
            }
            return Events("status")[^1];
        }

        /// <summary>Polls the status until it satisfies <paramref name="condition"/>.</summary>
        public async Task<JsonElement> StatusWhenAsync(Func<JsonElement, bool> condition, string what)
        {
            var clock = Stopwatch.StartNew();
            while (true)
            {
                var status = await StatusAsync();
                if (condition(status))
                    return status;
                if (clock.Elapsed > Patience)
                    Assert.Fail($"Node '{Id}': timed out waiting for {what}. Last status: {status.GetRawText()}{Environment.NewLine}{Dump()}");
                await Task.Delay(100);
            }
        }

        public void Send(string command)
        {
            try
            {
                _process.StandardInput.WriteLine(command);
                _process.StandardInput.Flush();
            }
            catch (IOException ex)
            {
                Assert.Fail($"Node '{Id}' could not be sent '{command}' ({ex.Message}); it has {(_process.HasExited ? "exited with code " + _process.ExitCode : "not exited")}.{Environment.NewLine}{Dump()}");
            }
        }

        /// <summary>Sends a command and waits for the event that answers it.</summary>
        public async Task<JsonElement> RunAsync(string command, string answer)
        {
            var before = Events(answer).Count;
            Send(command);
            return await WaitAsync(answer, e => Events(answer).IndexOf(e) >= before);
        }

        /// <summary><c>kill -9</c>.</summary>
        public void Kill() => _process.Kill();

        public async Task<int> ExitCodeAsync()
        {
            using var limit = new CancellationTokenSource(Patience);
            try
            {
                await _process.WaitForExitAsync(limit.Token);
            }
            catch (OperationCanceledException)
            {
                Assert.Fail($"Node '{Id}' did not exit.{Environment.NewLine}{Dump()}");
            }
            return _process.ExitCode;
        }

        public string Dump()
        {
            lock (_events)
            {
                lock (_stderr)
                {
                    var errors = _stderr.ToString().Split(Environment.NewLine);
                    return $"--- node {Id} (pid {_process.Id}) last events ---{Environment.NewLine}" +
                        string.Join(Environment.NewLine, _events.TakeLast(15).Select(e => e.GetRawText())) +
                        $"{Environment.NewLine}--- stderr (last lines) ---{Environment.NewLine}{string.Join(Environment.NewLine, errors.TakeLast(60))}";
                }
            }
        }

        public void Dispose()
        {
            try
            {
                if (!_process.HasExited)
                    _process.Kill(entireProcessTree: true);
            }
            catch (InvalidOperationException)
            {
            }
            _process.Dispose();
        }

        private static string AppDllPath()
        {
            // tests/Incremental.Service.Tests/bin/{Config}/{Tfm} → tests/Incremental.Service.FailoverApp/bin/{Config}/{Tfm}
            var baseDir = AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar);
            var tfm = Path.GetFileName(baseDir);
            var config = Path.GetFileName(Path.GetDirectoryName(baseDir)!);
            var dll = Path.GetFullPath(Path.Combine(
                baseDir, "..", "..", "..", "..", "Incremental.Service.FailoverApp", "bin", config, tfm, "Incremental.Service.FailoverApp.dll"));
            Assert.True(File.Exists(dll), $"Service failover app not found at {dll} — build tests/Incremental.Service.FailoverApp.");
            return dll;
        }
    }

    /// <summary>
    /// The rolling upgrade with the build from before format levels as the older node: a
    /// real process of that build (<c>INCREMENTAL_OLDER_BUILD</c>, the path of its
    /// <c>Incremental.Service.FailoverApp.dll</c>; commit 9de8da9, the last before state
    /// manifest format 8) and real processes of this one take turns leading one
    /// deployment. What the pinned nodes of <see cref="RollingUpgradeTests"/> stand for
    /// is here the thing itself: the older build reads everything this build writes
    /// until both nodes are this build, and leads again on it.
    /// </summary>
    [Fact]
    public async Task The_build_from_before_format_levels_and_this_one_take_turns_until_both_are_this_one()
    {
        if (Environment.GetEnvironmentVariable("INCREMENTAL_OLDER_BUILD") is not { Length: > 0 } older)
        {
            Assert.Skip(
                "INCREMENTAL_OLDER_BUILD is not set: build tests/Incremental.Service.FailoverApp at commit 9de8da9 in a worktree and give the path of its Incremental.Service.FailoverApp.dll.");
            return;
        }
        Assert.True(File.Exists(older), $"INCREMENTAL_OLDER_BUILD names {older}, which is not there.");
        await using var env = KafkaTestEnv.Create();
        var tradesTopic = await env.CreateTopicAsync("trades", partitions: 2);
        var instrumentsTopic = await env.CreateTopicAsync("instruments", compacted: true);
        var enrichedTopic = await env.CreateTopicAsync("enriched", compacted: true);
        var countsTopic = await env.CreateTopicAsync("counts", compacted: true);
        var ports = KafkaTestEnv.FreePorts(2);
        var instruments = Instruments(10);
        var trades = new List<Trade>();
        foreach (var instrument in instruments)
            env.ProduceJson(instrumentsTopic, instrument.Id, instrument);

        async Task FeedAsync(string what)
        {
            var more = Trades(trades.Count, 400);
            trades.AddRange(more);
            foreach (var trade in more)
                env.ProduceJson(tradesTopic, trade.Id, trade);
            var changed = trades.Count / 400 % 10;
            instruments[changed] = instruments[changed] with { Symbol = "S" + trades.Count };
            env.ProduceJson(instrumentsTopic, instruments[changed].Id, instruments[changed]);
            await env.FlushAsync();
            await KafkaTestEnv.TopicEqualsAsync(enrichedTopic, Enrich.Model(trades, instruments), $"the enriched sink, {what}");
            await KafkaTestEnv.TopicEqualsAsync(
                countsTopic, trades.GroupBy(t => t.Trader).ToDictionary(g => g.Key, g => $"v1:{g.Key}={g.Count()}"), $"the counts sink, {what}");
        }

        AppNode Node(string id, bool thisBuild) =>
            new(env, id, ports[id == "a" ? 0 : 1], ports[id == "a" ? 1 : 0], plugins: true, appDll: thisBuild ? null : older);
        static long Level(JsonElement status) =>
            status.GetProperty("counters").TryGetProperty("formatLevelWritten", out var level) ? level.GetInt64() : -1;
        Task<JsonElement> WarmAsync(AppNode standby, string leader) => standby.StatusWhenAsync(
            s => s.GetProperty("warm").GetBoolean() && State(s, "main") == "Built" && State(s, "counts") == "Built"
                && s.GetProperty("stateLag").GetInt64() == 0 && s.GetProperty("leader").GetString() == leader,
            $"the standby to be warm behind '{leader}'");
        async Task TakeOverAsync(AppNode standby, AppNode leader)
        {
            await standby.RunAsync("takeover", "tookover");
            Assert.Equal(3, await leader.ExitCodeAsync());
            leader.Dispose();
            await standby.StatusWhenAsync(s => State(s, "main") == "Running" && State(s, "counts") == "Running", "both pipelines to run on the new leader");
        }

        // Both nodes are the older build.
        var a = Node("a", thisBuild: false);
        await a.WaitForRoleAsync("Leader");
        await a.RunAsync("declare " + JsonSerializer.Serialize(new TableSpec
        {
            Name = "instruments",
            Definition = typeof(InstrumentsTable).FullName!,
            Loader = new TableLoaderSpec("kafka", "log", TestService.Json($$"""{ "topic": "{{instrumentsTopic}}" }""")),
        }, Json), "declared");
        await a.RunAsync("deploy " + JsonSerializer.Serialize(new PipelineSpec
        {
            Id = "main", Plugin = "acme.pipelines", Definition = "trade-enrichment",
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Binding(tradesTopic) },
            Sinks = new Dictionary<string, JsonElement> { ["enriched"] = Binding(enrichedTopic) },
        }, Json), "deployed");
        await a.RunAsync("deploy " + JsonSerializer.Serialize(new PipelineSpec
        {
            Id = "counts", Plugin = "acme.pipelines", Definition = "trader-counts",
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Binding(tradesTopic) },
            Sinks = new Dictionary<string, JsonElement> { ["counts"] = Binding(countsTopic) },
        }, Json), "deployed");
        var b = Node("b", thisBuild: false);
        await WarmAsync(b, "a");
        await FeedAsync("both nodes the older build");

        // 1. The standby is upgraded: this build follows what the older one writes.
        b.Kill();
        b.Dispose();
        b = Node("b", thisBuild: true);
        var followed = await WarmAsync(b, "a");
        Assert.Equal(1, Level(followed)); // the older leader announces no level; what it wrote is the base level
        await FeedAsync("the standby upgraded");
        await WarmAsync(b, "a");

        // 2. Takeover. This build leads at the base level, and the older build — back as
        // the standby — reads all of it: the catalog, the table, the state.
        await TakeOverAsync(b, a);
        await FeedAsync("this build leading alone");
        a = Node("a", thisBuild: false);
        await WarmAsync(a, "b");
        await FeedAsync("this build leading, the older build standing by");
        await WarmAsync(a, "b");
        await Task.Delay(TimeSpan.FromSeconds(3)); // three probes of a standby that says nothing of levels
        Assert.Equal(1, Level(await b.StatusAsync()));

        // The older build leads again, on state this build wrote.
        await TakeOverAsync(a, b);
        await FeedAsync("the older build leading again");
        b = Node("b", thisBuild: true);
        await WarmAsync(b, "a");
        await TakeOverAsync(b, a);
        await FeedAsync("this build leading again");

        // 3. The old leader is upgraded: the leader hears a standby that reads its level
        // and raises what it writes.
        a = Node("a", thisBuild: true);
        var raised = await b.StatusWhenAsync(s => Level(s) == Incremental.Storage.FormatLevel.Current, "the leader to raise the format level");
        await FeedAsync("the level raised");
        await WarmAsync(a, "b");

        // 4. Leadership moves at the new level.
        await TakeOverAsync(a, b);
        Assert.Equal(Incremental.Storage.FormatLevel.Current, Level(await a.StatusAsync()));
        await FeedAsync("after the takeover at the new level");
        Measured($"rolling upgrade from the build before format levels: {trades.Count} trades through 4 takeovers between the two builds, sinks equal to the model at each of 8 stages; level raised to {Level(raised)} only with both nodes this build");

        // 5. What the level protects against, shown on the build that has no levels: the
        // older build started now follows the logs and calls itself a warm standby — it
        // cannot know — and when it takes over, it leads with every pipeline stopped on
        // a checkpoint it refuses. Nothing is lost by that: the state is as it was, and
        // this build, taking over again, goes on from it at the level it finds there,
        // though the older leader in between announced none.
        b = Node("b", thisBuild: false);
        await WarmAsync(b, "a");
        await b.RunAsync("takeover", "tookover");
        Assert.Equal(3, await a.ExitCodeAsync());
        a.Dispose();
        var stopped = await b.StatusWhenAsync(s => State(s, "main") == "NeedsReset" && State(s, "counts") == "NeedsReset", "the older build to refuse both checkpoints");
        Assert.Contains("format=8", stopped.GetProperty("pipelines")[0].GetProperty("fault").GetString());
        a = Node("a", thisBuild: true);
        await a.StatusWhenAsync(s => s.GetProperty("role").GetString() == "Standby" && s.GetProperty("leader").GetString() == "b", "this build to stand by behind the older one");
        await a.RunAsync("takeover force", "tookover");
        Assert.Equal(3, await b.ExitCodeAsync());
        b.Dispose();
        var back = await a.StatusWhenAsync(s => State(s, "main") == "Running" && State(s, "counts") == "Running", "both pipelines to run again on this build");
        Assert.Equal(Incremental.Storage.FormatLevel.Current, Level(back));
        await FeedAsync("after the older build led for a moment at the new level");
        Measured("rolling upgrade: the older build started after the raise called itself warm, took over and stopped both pipelines as NeedsReset (format=8 refused); this build took over again and the sinks equal the model");
        a.Dispose();
    }

    private static long T(JsonElement e) => e.GetProperty("t").GetInt64();

    private static long Now => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static string State(JsonElement status, string pipeline) =>
        status.GetProperty("pipelines").EnumerateArray().FirstOrDefault(p => p.GetProperty("id").GetString() == pipeline) is { ValueKind: JsonValueKind.Object } found
            ? found.GetProperty("state").GetString()!
            : "(absent)";

    private static JsonElement Binding(string topic, string extra = "") =>
        TestService.Json($$"""{ "connector": "kafka", "connection": "log", "topic": "{{topic}}"{{extra}} }""");

    private void Measured(string line)
    {
        output.WriteLine(line);
        lock (typeof(KafkaServiceProcessTests))
            File.AppendAllText(Path.Combine(AppContext.BaseDirectory, "measurements.log"), $"{DateTime.UtcNow:O} {line}{Environment.NewLine}");
    }

    /// <summary>One run's outcome as a line of JSON in <c>broker-runs.jsonl</c> beside the
    /// test assembly: what a script that repeats these tests tabulates.</summary>
    private static void Result(object result)
    {
        lock (typeof(KafkaServiceProcessTests))
            File.AppendAllText(Path.Combine(AppContext.BaseDirectory, "broker-runs.jsonl"), JsonSerializer.Serialize(result, Json) + Environment.NewLine);
    }

    [Fact]
    public async Task Killing_the_leader_process_mid_stream_leaves_the_standby_leading_with_every_pipeline_and_a_correct_sink()
    {
        const int extraTables = 20, extraRows = 500;
        await using var env = KafkaTestEnv.Create();
        var tradesTopic = await env.CreateTopicAsync("trades", partitions: 2);
        var instrumentsTopic = await env.CreateTopicAsync("instruments", compacted: true);
        var enrichedTopic = await env.CreateTopicAsync("enriched", compacted: true);
        var countsTopic = await env.CreateTopicAsync("counts", compacted: true);
        var ports = KafkaTestEnv.FreePorts(2);

        var instruments = Instruments(10);
        var trades = Trades(0, 9000);
        foreach (var instrument in instruments)
            env.ProduceJson(instrumentsTopic, instrument.Id, instrument);
        foreach (var trade in trades.Take(2000))
            env.ProduceJson(tradesTopic, trade.Id, trade);
        await env.FlushAsync();

        // A leads a deployment nobody led: after TakeoverAfter of silence, by itself.
        var startedA = Now;
        using var a = new AppNode(env, "a", ports[0], ports[1], plugins: true, tables: extraTables);
        var firstLead = await a.WaitForRoleAsync("Leader");
        Measured($"service first lead on an empty deployment, {extraTables} table seeds and a plugin import: {T(firstLead) - startedA} ms from process start");
        await a.RunAsync("declare " + JsonSerializer.Serialize(new TableSpec
        {
            Name = "instruments",
            Definition = typeof(InstrumentsTable).FullName!,
            Loader = new TableLoaderSpec("kafka", "log", TestService.Json($$"""{ "topic": "{{instrumentsTopic}}" }""")),
        }, Json), "declared");
        Assert.Equal(extraTables, (await a.RunAsync($"fill ref- {extraRows}", "filled")).GetProperty("tables").GetInt32());
        await a.RunAsync("deploy " + JsonSerializer.Serialize(new PipelineSpec
        {
            Id = "main",
            Plugin = "acme.pipelines",
            Definition = "trade-enrichment",
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Binding(tradesTopic) },
            Sinks = new Dictionary<string, JsonElement> { ["enriched"] = Binding(enrichedTopic) },
        }, Json), "deployed");
        await a.StatusWhenAsync(s => State(s, "main") == "Running", "the first pipeline to run on the leader");

        // B stands by. It was never given the plugin: it has only the broker.
        var startedB = Now;
        using var b = new AppNode(env, "b", ports[1], ports[0], tables: extraTables);
        var standing = await b.StatusWhenAsync(
            s => s.GetProperty("warm").GetBoolean() && State(s, "main") == "Built" && s.GetProperty("leader").GetString() == "a",
            "the standby to be warm");
        Measured($"service standby warm (catalog, {extraTables + 1} tables, state, plugin files): {T(standing) - startedB} ms from process start");
        Assert.True(T(standing) - startedB < 25_000,
            $"the standby took {T(standing) - startedB} ms to open and follow {extraTables + 1} tables; each table's open should cost two short reads, not two seconds");
        Assert.Equal("Standby", standing.GetProperty("role").GetString());
        Assert.Equal(extraTables + 1, standing.GetProperty("tables").GetArrayLength());
        Assert.All(standing.GetProperty("tables").EnumerateArray(), t => Assert.True(t.GetProperty("following").GetBoolean()));
        Assert.Equal(0, standing.GetProperty("pluginsPending").GetInt32());
        await a.StatusWhenAsync(
            s => s.GetProperty("peer") is { ValueKind: JsonValueKind.Object } peer && peer.GetProperty("ready").GetBoolean() && s.GetProperty("health").GetString() == "Healthy",
            "the leader to see a ready standby");

        // The stream runs on through all of what follows.
        var producing = Task.Run(async () =>
        {
            foreach (var chunk in trades.Skip(2000).Take(5000).Chunk(100))
            {
                foreach (var trade in chunk)
                    env.ProduceJson(tradesTopic, trade.Id, trade);
                await Task.Delay(40);
            }
            await env.FlushAsync();
        });
        await a.StatusWhenAsync(
            s => s.GetProperty("pipelines").EnumerateArray().Single(p => p.GetProperty("id").GetString() == "main").GetProperty("read").GetInt64() > 2300,
            "the leader to be reading the stream");

        // A second pipeline, deployed just before the end: the moment its deploy returns,
        // the leader dies. No shutdown, no resignation, nothing flushed.
        await a.RunAsync("deploy " + JsonSerializer.Serialize(new PipelineSpec
        {
            Id = "late",
            Plugin = "acme.pipelines",
            Definition = "trader-counts",
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Binding(tradesTopic) },
            Sinks = new Dictionary<string, JsonElement> { ["counts"] = Binding(countsTopic) },
        }, Json), "deployed");
        Assert.False(producing.IsCompleted, "the stream ended before the kill; the test proved less than it says");
        a.Kill();
        var killedAt = Now;

        var promoting = await b.WaitForRoleAsync("Promoting");
        var leading = await b.WaitForRoleAsync("Leader");
        var detection = T(promoting) - killedAt;
        var failover = T(leading) - killedAt;
        Assert.True(failover < 20_000, $"the standby led {failover} ms after the kill; the bound is generous at 20 s");

        // The world moves on under the new leader: the rest of the stream, a renamed instrument.
        await producing;
        instruments[3] = instruments[3] with { Symbol = "RENAMED", Multiplier = 5 };
        env.ProduceJson(instrumentsTopic, instruments[3].Id, instruments[3]);
        foreach (var trade in trades.Skip(7000))
            env.ProduceJson(tradesTopic, trade.Id, trade);
        await env.FlushAsync();

        var after = await b.StatusWhenAsync(
            s => State(s, "main") == "Running" && State(s, "late") == "Running", "both pipelines to run on the new leader");
        Assert.Equal("loaded", after.GetProperty("plugins").EnumerateArray().Single(p => p.GetProperty("name").GetString() == "acme.pipelines").GetProperty("state").GetString());

        // No trade lost, none stale, the rename applied to trades from before the kill.
        await KafkaTestEnv.TopicEqualsAsync(enrichedTopic, Enrich.Model(trades, instruments), "the enriched sink after the failover");
        await KafkaTestEnv.TopicEqualsAsync(
            countsTopic, trades.GroupBy(t => t.Trader).ToDictionary(g => g.Key, g => $"v1:{g.Key}={g.Count()}"),
            "the sink of the pipeline deployed just before the kill");

        // The new leader resumed from committed offsets; it did not start the topic again.
        var final = await b.StatusAsync();
        var read = final.GetProperty("pipelines").EnumerateArray().Single(p => p.GetProperty("id").GetString() == "main").GetProperty("read").GetInt64();
        Assert.True(read < trades.Count, $"the new leader read {read} of {trades.Count} trades for 'main'; it should have resumed");

        var takeover = final.GetProperty("takeover");
        double Ms(string name) => takeover.GetProperty(name).GetDouble();
        Assert.Equal(extraTables + 1, takeover.GetProperty("tableCount").GetInt32());
        Assert.Equal(2, takeover.GetProperty("pipelineCount").GetInt32());
        Measured(
            $"service kill -9 failover, {extraTables + 1} tables ({extraRows} rows each), 2 plugin pipelines: detection {detection} ms; " +
            $"takeover {Ms("totalMs"):F0} ms = fence {Ms("fenceMs"):F0} + catalog {Ms("catalogMs"):F0} + tables {Ms("tablesMs"):F0} + " +
            $"storage {Ms("storageMs"):F0} + start {Ms("startMs"):F0} (pipelines {Ms("pipelinesMs"):F0}); kill to Leader {failover} ms");

        // Under a tenth of a second a table, which is what a wait of the client's per
        // table costs when one creeps into the stage: tables are promoted one short read
        // after another. 109 to 190 ms for the 21 on a runner and here; 2,164 ms the one
        // time each table's topic was new to the reader and cost a client made and
        // disposed for it (KafkaLogTests has that one by count, without a clock).
        var tableCount = extraTables + 1;
        Assert.True(Ms("tablesMs") < tableCount * 100,
            $"the table stage took {Ms("tablesMs"):F0} ms for {tableCount} tables; under 100 ms a table is the bound");

        // The dead node comes back the only way it can: as the standby.
        using var again = new AppNode(env, "a", ports[0], ports[1], tables: extraTables);
        var back = await again.StatusWhenAsync(
            s => s.GetProperty("warm").GetBoolean() && s.GetProperty("leader").GetString() == "b" && State(s, "late") == "Built",
            "the restarted node to stand by");
        Assert.Equal("Standby", back.GetProperty("role").GetString());
        Assert.Equal("Built", State(back, "main"));
    }

    // ── a cluster of brokers, one of them stopped ──────────────────────────────────────
    //
    // These run only against a broker cluster whose containers the test may stop and
    // start: the kafka3 compose profile, with
    //   INCREMENTAL_KAFKA_BOOTSTRAP=localhost:9192,localhost:9292,localhost:9392
    //   INCREMENTAL_KAFKA_REPLICATION=3
    //   INCREMENTAL_KAFKA_BROKERS=incremental-kafka3-1-1,incremental-kafka3-2-1,incremental-kafka3-3-1
    // Everywhere else they are skipped.

    private static string[] Brokers =>
        (Environment.GetEnvironmentVariable("INCREMENTAL_KAFKA_BROKERS") ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    /// <summary>Skips unless the broker cluster is there to be stopped, and starts every
    /// broker first: a test that failed between a stop and a start must not leave the
    /// next one a cluster of two.</summary>
    private static async Task RequireBrokerClusterAsync()
    {
        Assert.SkipUnless(
            Brokers.Length >= 3 && KafkaTestEnv.Replication >= 3,
            "Needs a three-broker cluster whose containers can be stopped: the kafka3 compose profile, with INCREMENTAL_KAFKA_BROKERS naming them and INCREMENTAL_KAFKA_REPLICATION=3.");
        foreach (var broker in Brokers)
            await DockerAsync("start", broker);
        await TransactionStateExistsAsync();
        await ClusterWholeAsync("__transaction_state");
    }

    /// <summary>A cluster started a moment ago has no <c>__transaction_state</c>: the
    /// brokers create it when a transactional producer first asks for its coordinator.
    /// <see cref="ClusterWholeAsync"/> waits for that topic's replicas, so on a fresh
    /// cluster something has to ask first — these tests must not depend on another test
    /// having run before them.</summary>
    private static async Task TransactionStateExistsAsync()
    {
        var clock = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                using var producer = new Confluent.Kafka.ProducerBuilder<byte[], byte[]>(new Confluent.Kafka.ProducerConfig
                {
                    BootstrapServers = KafkaTestEnv.Bootstrap,
                    TransactionalId = "broker-tests-" + Guid.NewGuid().ToString("N")[..8],
                }).Build();
                await Task.Run(() => producer.InitTransactions(TimeSpan.FromSeconds(20)));
                return;
            }
            catch (Confluent.Kafka.KafkaException) when (clock.Elapsed < TimeSpan.FromMinutes(2))
            {
                await Task.Delay(500);
            }
        }
    }

    private static async Task DockerAsync(params string[] arguments)
    {
        var info = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var argument in arguments)
            info.ArgumentList.Add(argument);
        using var process = Process.Start(info) ?? throw new InvalidOperationException("docker did not start.");
        var error = process.StandardError.ReadToEndAsync();
        await process.StandardOutput.ReadToEndAsync();
        await process.WaitForExitAsync();
        Assert.True(process.ExitCode == 0, $"docker {string.Join(' ', arguments)} exited {process.ExitCode}: {await error}");
    }

    /// <summary>
    /// Waits until every broker is back and every partition of the deployment's own topics
    /// has all its replicas in sync again — as each broker itself says, asked one at a
    /// time. One broker's word for the cluster is not enough: a broker that was killed is
    /// still listed, and still in every replica set, until the controller has given up on
    /// it (nine seconds), so a cluster asked through a survivor looks whole while a third
    /// of it is down. The first version of these tests believed that, and killed the next
    /// broker while the last one was still coming back.
    /// </summary>
    private static async Task ClusterWholeAsync(string prefix)
    {
        var clock = Stopwatch.StartNew();
        var servers = KafkaTestEnv.Bootstrap.Split(',');
        while (true)
        {
            var whole = 0;
            foreach (var server in servers)
            {
                try
                {
                    // A new client each time: its first answer is the bootstrap broker's.
                    using var admin = new Confluent.Kafka.AdminClientBuilder(new Confluent.Kafka.AdminClientConfig
                    {
                        BootstrapServers = server,
                        SocketConnectionSetupTimeoutMs = 2000,
                    }).Build();
                    var metadata = admin.GetMetadata(TimeSpan.FromSeconds(3));
                    var partitions = metadata.Topics.Where(t => t.Topic.StartsWith(prefix + ".", StringComparison.Ordinal) || t.Topic == "__transaction_state")
                        .SelectMany(t => t.Partitions).ToList();
                    if (metadata.Brokers.Count >= Brokers.Length && partitions.Count > 0
                        && partitions.All(p => p.Leader >= 0 && p.InSyncReplicas.Length == p.Replicas.Length))
                        whole++;
                }
                catch (Confluent.Kafka.KafkaException)
                {
                }
            }
            if (whole == servers.Length)
                return;
            Assert.True(clock.Elapsed < TimeSpan.FromMinutes(3), "the broker cluster did not become whole again within three minutes");
            await Task.Delay(500);
        }
    }

    /// <summary>What a broker is to the deployment at this moment: the controller, the
    /// transaction coordinator of the deployment's writer, the leader of the leader
    /// topic's partition, of state partitions, of the input or of the sink.</summary>
    private static async Task<List<string>> RolesAsync(int broker, string prefix, Deployment d)
    {
        var roles = new List<string>();
        var id = broker + 1;
        var quorum = await DockerOutputAsync(
            "exec", Brokers[(broker + 1) % Brokers.Length], "/opt/kafka/bin/kafka-metadata-quorum.sh", "--bootstrap-server", "localhost:19092", "describe", "--status");
        if (System.Text.RegularExpressions.Regex.Match(quorum, @"LeaderId:\s+(\d+)") is { Success: true } leader && int.Parse(leader.Groups[1].Value, CultureInfo.InvariantCulture) == id)
            roles.Add("controller");
        using var admin = new Confluent.Kafka.AdminClientBuilder(new Confluent.Kafka.AdminClientConfig { BootstrapServers = KafkaTestEnv.Bootstrap }).Build();
        var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
        int Leader(string topic, int partition) => metadata.Topics.Single(t => t.Topic == topic).Partitions.Single(p => p.PartitionId == partition).Leader;

        // The coordinator of a transactional id leads one partition of __transaction_state:
        // Java's String.hashCode of the id, made non-negative, modulo the partition count.
        var hash = 0;
        foreach (var c in prefix + ".writer")
            hash = unchecked(31 * hash + c);
        var transactions = metadata.Topics.Single(t => t.Topic == "__transaction_state").Partitions.Count;
        if (Leader("__transaction_state", (hash & 0x7fffffff) % transactions) == id)
            roles.Add("coordinator");
        if (Leader(prefix + ".leader", 0) == id)
            roles.Add("leader-topic");
        if (metadata.Topics.Single(t => t.Topic == prefix + ".state").Partitions.Any(p => p.Leader == id))
            roles.Add("state");
        if (metadata.Topics.Single(t => t.Topic == d.TradesTopic).Partitions.Any(p => p.Leader == id))
            roles.Add("input");
        if (Leader(d.EnrichedTopic, 0) == id)
            roles.Add("sink");
        return roles;
    }

    private static async Task<string> DockerOutputAsync(params string[] arguments)
    {
        var info = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var argument in arguments)
            info.ArgumentList.Add(argument);
        using var process = Process.Start(info) ?? throw new InvalidOperationException("docker did not start.");
        var error = process.StandardError.ReadToEndAsync();
        var text = await process.StandardOutput.ReadToEndAsync();
        await process.WaitForExitAsync();
        await error;
        return text;
    }

    private sealed record Deployment(AppNode A, AppNode B, string TradesTopic, string InstrumentsTopic, string EnrichedTopic, List<Instrument> Instruments, int[] Ports);

    /// <summary>A leads with a Kafka-loaded table and one plugin pipeline between two
    /// topics; B stands by, warm.</summary>
    private static async Task<Deployment> DeployAsync(KafkaTestEnv env, bool supervised = false)
    {
        var tradesTopic = await env.CreateTopicAsync("trades", partitions: 2);
        var instrumentsTopic = await env.CreateTopicAsync("instruments", compacted: true);
        var enrichedTopic = await env.CreateTopicAsync("enriched", compacted: true);
        var ports = KafkaTestEnv.FreePorts(2);
        var instruments = Instruments(10);
        foreach (var instrument in instruments)
            env.ProduceJson(instrumentsTopic, instrument.Id, instrument);
        await env.FlushAsync();

        var a = new AppNode(env, "a", ports[0], ports[1], plugins: true, supervised: supervised);
        await a.WaitForRoleAsync("Leader");
        await a.RunAsync("declare " + JsonSerializer.Serialize(new TableSpec
        {
            Name = "instruments",
            Definition = typeof(InstrumentsTable).FullName!,
            Loader = new TableLoaderSpec("kafka", "log", TestService.Json($$"""{ "topic": "{{instrumentsTopic}}" }""")),
        }, Json), "declared");
        await a.RunAsync("deploy " + JsonSerializer.Serialize(new PipelineSpec
        {
            Id = "main",
            Plugin = "acme.pipelines",
            Definition = "trade-enrichment",
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Binding(tradesTopic) },
            Sinks = new Dictionary<string, JsonElement> { ["enriched"] = Binding(enrichedTopic) },
        }, Json), "deployed");
        await a.StatusWhenAsync(s => State(s, "main") == "Running", "the pipeline to run on the leader");
        var b = new AppNode(env, "b", ports[1], ports[0], supervised: supervised);
        await b.StatusWhenAsync(
            s => s.GetProperty("warm").GetBoolean() && State(s, "main") == "Built" && s.GetProperty("leader").GetString() == "a", "the standby to be warm");
        await a.StatusWhenAsync(
            s => s.GetProperty("peer") is { ValueKind: JsonValueKind.Object } peer && peer.GetProperty("ready").GetBoolean(), "the leader to see a ready standby");
        return new Deployment(a, b, tradesTopic, instrumentsTopic, enrichedTopic, instruments, ports);
    }

    /// <summary>Trades at a steady rate until cancelled; returns what was produced.</summary>
    private static Task<List<Trade>> ProduceUntilAsync(KafkaTestEnv env, string topic, CancellationToken stop) => Task.Run(async () =>
    {
        var produced = new List<Trade>();
        while (!stop.IsCancellationRequested)
        {
            var chunk = Trades(produced.Count, 20);
            foreach (var trade in chunk)
                env.ProduceJson(topic, trade.Id, trade);
            produced.AddRange(chunk);
            await Task.Delay(100);
        }
        await env.FlushAsync();
        return produced;
    });

    /// <summary>What a reader of the sink saw while the input ran at a steady rate.</summary>
    /// <param name="LongestMs">The longest time without a new committed record: the
    /// service's output stall, whatever caused it (a broker moving its partitions, a
    /// leader waiting on its commit, a takeover).</param>
    /// <param name="At">When that silence ended.</param>
    /// <param name="LongestDelayMs">The longest any trade took from being produced to its
    /// row being readable in the sink. A stall of part of the input — one partition's
    /// leader gone, the other's not — does not silence the sink; it shows here.</param>
    private sealed record SinkTimes(long LongestMs, DateTime At, long LongestDelayMs);

    /// <summary>
    /// A steady input and a reader of the sink, for as long as a broker test does things
    /// to the brokers: twenty trades every hundred milliseconds into the trades topic, and
    /// a consumer of the enriched topic — committed records only — that times what arrives.
    /// </summary>
    private sealed class Load
    {
        private readonly System.Collections.Concurrent.ConcurrentDictionary<long, long> _producedAt = new();
        private readonly CancellationTokenSource _stop = new();
        private readonly Task<List<Trade>> _producing;
        private readonly Task<SinkTimes> _watching;
        private volatile bool _produced;

        public Load(KafkaTestEnv env, Deployment d)
        {
            _producing = Task.Run(async () =>
            {
                var produced = new List<Trade>();
                while (!_stop.IsCancellationRequested)
                {
                    var chunk = Trades(produced.Count, 20);
                    foreach (var trade in chunk)
                    {
                        // Timed from when the broker has the trade, not from when it was
                        // handed to this producer: the input's own wait for a broker is
                        // not the service's.
                        var id = trade.Id;
                        env.Producer.Produce(
                            d.TradesTopic,
                            new Confluent.Kafka.Message<byte[], byte[]> { Key = KafkaTestEnv.Json(trade.Id), Value = KafkaTestEnv.Json(trade) },
                            report =>
                            {
                                if (!report.Error.IsError)
                                    _producedAt[id] = Stopwatch.GetTimestamp();
                            });
                    }
                    produced.AddRange(chunk);
                    await Task.Delay(100);
                }
                await env.FlushAsync();
                _produced = true;
                return produced;
            });
            _watching = Task.Run(() => Watch(d.EnrichedTopic));
        }

        /// <summary>Ends the input and returns it with what the reader saw. The reader
        /// goes on until every trade's row has arrived (or three minutes have passed), so
        /// the longest delay counts the trades that were still on their way.</summary>
        public async Task<(List<Trade> Trades, SinkTimes Times)> StopAsync()
        {
            _stop.Cancel();
            return (await _producing, await _watching);
        }

        private SinkTimes Watch(string topic)
        {
            using var consumer = new Confluent.Kafka.ConsumerBuilder<byte[], byte[]>(new Confluent.Kafka.ConsumerConfig
            {
                BootstrapServers = KafkaTestEnv.Bootstrap,
                GroupId = "service-test-watch",
                EnableAutoCommit = false,
                IsolationLevel = Confluent.Kafka.IsolationLevel.ReadCommitted,
                // The reader must not add a stall of its own to the one it measures: with
                // the client's defaults a fetch in flight to a broker that has stopped
                // answering is waited on for a minute, and the reader — not the service —
                // is silent.
                ReconnectBackoffMaxMs = 500,
                TopicMetadataRefreshFastIntervalMs = 100,
                TopicMetadataRefreshIntervalMs = 1000,
                SocketTimeoutMs = 10_000,
                FetchWaitMaxMs = 100,
            }).Build();
            consumer.Assign(new Confluent.Kafka.TopicPartitionOffset(topic, 0, Confluent.Kafka.Offset.Beginning));
            var last = Stopwatch.GetTimestamp();
            var seen = new HashSet<long>();
            var silence = (LongestMs: 0L, At: DateTime.UtcNow);
            var delay = 0L;
            long? stoppedAt = null;
            while (true)
            {
                if (_stop.IsCancellationRequested && stoppedAt is null)
                {
                    // A silence that had not ended when the input did is one too.
                    stoppedAt = Stopwatch.GetTimestamp();
                    var open = (long)Stopwatch.GetElapsedTime(last).TotalMilliseconds;
                    if (seen.Count > 0 && open > silence.LongestMs)
                        silence = (open, DateTime.UtcNow);
                }
                if (stoppedAt is { } since && ((_produced && seen.Count >= _producedAt.Count && _producedAt.Count > 0) || Stopwatch.GetElapsedTime(since) > TimeSpan.FromMinutes(3)))
                    return new SinkTimes(silence.LongestMs, silence.At, delay);

                Confluent.Kafka.ConsumeResult<byte[], byte[]>? result = null;
                try
                {
                    result = consumer.Consume(TimeSpan.FromMilliseconds(50));
                }
                catch (Confluent.Kafka.ConsumeException)
                {
                    // A partition between leaders: the silence is counted, the error is not news.
                }
                if (result?.Message is null)
                    continue;
                var now = Stopwatch.GetTimestamp();
                if (stoppedAt is null)
                {
                    var gap = (long)Stopwatch.GetElapsedTime(last, now).TotalMilliseconds;
                    if (seen.Count > 0 && gap > silence.LongestMs)
                        silence = (gap, DateTime.UtcNow);
                }
                last = now;

                // A trade's first row only: a step run again by a new leader writes it again.
                var id = JsonSerializer.Deserialize<long>(result.Message.Key);
                if (seen.Add(id) && _producedAt.TryGetValue(id, out var producedAt))
                    delay = Math.Max(delay, (long)Stopwatch.GetElapsedTime(producedAt, now).TotalMilliseconds);
            }
        }
    }

    /// <summary>A rolling restart, as an upgrade does it: each broker is asked to stop,
    /// hands its partitions over, and comes back.</summary>
    [Fact]
    public Task Restarting_every_broker_in_turn_under_a_running_pipeline_loses_nothing_and_leaves_one_leader() =>
        EveryBrokerInTurnAsync(kill: false);

    /// <summary>A broker that dies hands nothing over: its partitions, and the transaction
    /// coordinator if it was that, are leaderless until the controller gives up on it
    /// (<c>broker.session.timeout.ms</c>, nine seconds).</summary>
    [Fact]
    public Task Killing_every_broker_in_turn_under_a_running_pipeline_loses_nothing_and_leaves_one_leader() =>
        EveryBrokerInTurnAsync(kill: true);

    /// <summary>
    /// Whichever broker is the controller, the transaction coordinator or the leader of
    /// the state partition is among them, and each goes in the middle of the service
    /// leader's commits and stays away long enough to be missed (a killed broker for
    /// fifteen seconds, which is past the nine the controller gives it). Required: nothing
    /// lost, one node leading with the pipeline running and the other standing by. While
    /// a dead broker's partitions and its coordinator move, the service's leader commits
    /// nothing; that silence, as a reader of the sink sees it, is measured.
    /// </summary>
    private async Task EveryBrokerInTurnAsync(bool kill)
    {
        await RequireBrokerClusterAsync();
        await using var env = KafkaTestEnv.Create();
        var d = await DeployAsync(env, supervised: true);
        using var a = d.A;
        using var b = d.B;
        var load = new Load(env, d);
        var how = kill ? "killed" : "shut down";
        foreach (var broker in Brokers)
        {
            await Task.Delay(2000);
            var stopped = Stopwatch.StartNew();
            if (kill)
                await DockerAsync("kill", broker);
            else
                await DockerAsync("stop", "-t", "30", broker);
            await Task.Delay(kill ? 15000 : 5000);
            await DockerAsync("start", broker);
            await ClusterWholeAsync(env.Prefix);
            Measured($"kafka3: broker {broker} {how} and back, cluster whole again after {stopped.ElapsedMilliseconds} ms");
        }
        await Task.Delay(2000);
        var (trades, stall) = await load.StopAsync();
        await ConcludeAsync(kill ? "kill-in-turn" : "rolling", env, d, trades, stall, $"every broker was {how} in turn", []);
    }

    /// <summary>
    /// One broker is lost under load — which one, and how, is the run's: killed
    /// (<c>kill -9</c>: connections are refused at once), paused (frozen where it stands:
    /// connections stay open and nothing answers, which is what a host or a zone that
    /// drops off the network looks like) or stopped. It stays away for twenty seconds.
    /// The loss of one broker of three must not move the leadership and must not silence
    /// the sink for longer than the brokers themselves take to give the broker up.
    /// <para><c>INCREMENTAL_KAFKA_BROKER</c> (0, 1 or 2) and <c>INCREMENTAL_KAFKA_FAULT</c>
    /// (<c>kill</c>, <c>pause</c>, <c>stop</c>) choose; otherwise a random broker is killed.</para>
    /// </summary>
    [Fact]
    public async Task Losing_one_broker_of_three_under_a_running_pipeline_moves_no_leadership_and_loses_nothing()
    {
        await RequireBrokerClusterAsync();
        await using var env = KafkaTestEnv.Create();
        var d = await DeployAsync(env, supervised: true);
        using var a = d.A;
        using var b = d.B;
        var index = int.TryParse(Environment.GetEnvironmentVariable("INCREMENTAL_KAFKA_BROKER"), out var chosen) ? chosen % Brokers.Length : Random.Shared.Next(Brokers.Length);
        var fault = Environment.GetEnvironmentVariable("INCREMENTAL_KAFKA_FAULT") is { Length: > 0 } named ? named : "kill";
        var away = TimeSpan.FromSeconds(int.TryParse(Environment.GetEnvironmentVariable("INCREMENTAL_KAFKA_AWAY_SECONDS"), out var seconds) ? seconds : 20);
        var broker = Brokers[index];

        var load = new Load(env, d);
        await Task.Delay(3000);
        var roles = await RolesAsync(index, env.Prefix, d);
        // What the loss costs the standby in processor time, second by second: a node
        // that only follows has little to do, and one whose Kafka client connects again
        // without a pause to a broker that refuses it takes more than a processor for as
        // long as the broker is still listed as a leader (Open for James 75).
        var standby = (await a.StatusAsync()).GetProperty("role").GetString() == "Leader" ? b : a;
        var busiest = 0.0;
        var busySeconds = 0;
        try
        {
            await DockerAsync(fault switch { "pause" => ["pause", broker], "stop" => ["stop", "-t", "30", broker], _ => ["kill", broker] });
            var gone = Stopwatch.StartNew();
            var (lastCpu, lastAt) = ((await standby.StatusAsync()).GetProperty("cpuMs").GetDouble(), gone.Elapsed.TotalMilliseconds);
            while (gone.Elapsed < away)
            {
                await Task.Delay(1000);
                var (cpu, at) = ((await standby.StatusAsync()).GetProperty("cpuMs").GetDouble(), gone.Elapsed.TotalMilliseconds);
                var used = (cpu - lastCpu) / Math.Max(1, at - lastAt);
                busiest = Math.Max(busiest, used);
                if (used > 0.9)
                    busySeconds++;
                (lastCpu, lastAt) = (cpu, at);
            }
        }
        finally
        {
            await DockerAsync(fault == "pause" ? ["unpause", broker] : ["start", broker]);
        }
        Measured($"kafka3 {fault}-one: the standby ({standby.Id}) used at most {busiest:F2} of a processor in any second the broker was away, and more than 0.9 in {busySeconds} of them");
        await ClusterWholeAsync(env.Prefix);
        await Task.Delay(3000);
        var (trades, stall) = await load.StopAsync();
        var moved = await ConcludeAsync(
            fault + "-one", env, d, trades, stall, $"broker {index + 1} ({(roles.Count == 0 ? "no role" : string.Join(", ", roles))}) was lost by {fault} for {away.TotalSeconds:F0} s",
            new() { ["broker"] = index + 1, ["roles"] = roles, ["awayS"] = away.TotalSeconds, ["standbyBusiestSecond"] = Math.Round(busiest, 2), ["standbyBusySeconds"] = busySeconds });
        Assert.False(moved, "the loss of one broker of three moved the leadership");

        // It was eight to eleven such seconds, at 1.2 to 1.4 processors each, while the
        // admin client connected on demand.
        Assert.True(busySeconds <= 2, $"the standby used more than 0.9 of a processor in {busySeconds} of the seconds the broker was away (at most {busiest:F2})");

        // The bound is the takeover ceiling, not the 9 to 15 s this takes almost always:
        // in one kill of thirty the two brokers that were left spent the whole twenty
        // seconds waiting on connections to the dead one before a transaction could
        // begin again (durability-and-standby.md, As built: broker failures).
        Assert.True(stall.LongestMs < 30_000, $"the sink was silent for {stall.LongestMs} ms over the loss of one broker");
        Assert.True(stall.LongestDelayMs < 30_000, $"a trade took {stall.LongestDelayMs} ms to reach the sink over the loss of one broker");
    }

    /// <summary>
    /// The end of a broker test: the sink equals the model, exactly one node leads with
    /// the pipeline running and the other stands by behind it, warm. Writes the run's line
    /// to <c>measurements.log</c> and its record to <c>broker-runs.jsonl</c>. Returns
    /// whether the leadership moved.
    /// </summary>
    private async Task<bool> ConcludeAsync(
        string scenario, KafkaTestEnv env, Deployment d, List<Trade> trades, SinkTimes stall, string what, Dictionary<string, object?> extra)
    {
        var (a, b) = (d.A, d.B);

        // The sink first: it is complete only once a leader that can commit has caught up.
        await SinkEqualsAsync(d, trades, a, b, $"the enriched sink after {what}", TimeSpan.FromMinutes(3));
        var (leader, final, other) = await OneLeaderAsync(a, b);
        var term = final.GetProperty("term").GetInt64();
        var moved = term != 1;
        var exits = new Dictionary<string, List<int>> { ["a"] = a.SupervisedExits(), ["b"] = b.SupervisedExits() };
        string Counter(AppNode node, JsonElement status, string name) =>
            status.TryGetProperty("counters", out var counters) && counters.TryGetProperty(name, out var value) ? value.GetInt64().ToString(CultureInfo.InvariantCulture) : "?";
        Measured(
            $"kafka3 {scenario}: {trades.Count} trades while {what}; node {leader.Id} leads at term {term}" +
            (moved ? $" (takeover reason: {final.GetProperty("takeover").GetProperty("reason").GetString()})" : ", nobody took over") +
            $", node {other.Id} stands by; exits seen by the supervisors: a [{string.Join(",", exits["a"])}] b [{string.Join(",", exits["b"])}]; " +
            $"the pipeline restarted {final.GetProperty("pipelines").EnumerateArray().Single().GetProperty("restarts").GetInt32()} time(s) on the leader; " +
            $"the sink's longest silence was {stall.LongestMs} ms (ending {stall.At:HH:mm:ss.fff}) and the longest a trade took to reach it {stall.LongestDelayMs} ms; " +
            $"the first leader's longest wait for the brokers to take a transaction was {a.LongestWaitForBrokers()} s");
        var result = new Dictionary<string, object?>
        {
            ["scenario"] = scenario,
            ["prefix"] = env.Prefix,
            ["trades"] = trades.Count,
            ["sinkEqualsModel"] = true,
            ["oneLeader"] = true,
            ["leader"] = leader.Id,
            ["term"] = term,
            ["moved"] = moved,
            ["reason"] = moved ? final.GetProperty("takeover").GetProperty("reason").GetString() : null,
            ["exitsA"] = exits["a"],
            ["exitsB"] = exits["b"],
            ["sinkStallMs"] = stall.LongestMs,
            ["longestDelayMs"] = stall.LongestDelayMs,
            ["writerWaitS"] = a.LongestWaitForBrokers(),
            ["abandoned"] = Counter(leader, final, "takeoversAbandoned"),
            ["client"] = Environment.GetEnvironmentVariable("INCREMENTAL_KAFKA_CLIENT") ?? "",
        };
        foreach (var (key, value) in extra)
            result[key] = value;
        Result(result);
        return moved;
    }

    /// <summary>Waits for the state every broker test must end in: one node the leader
    /// with the pipeline running, the other its standby, warm. Supervised nodes answer
    /// through whichever incarnation is running.</summary>
    private static async Task<(AppNode Leader, JsonElement Status, AppNode Other)> OneLeaderAsync(AppNode a, AppNode b)
    {
        var clock = Stopwatch.StartNew();
        while (true)
        {
            var statuses = new[] { (Node: a, Status: await a.StatusAsync()), (Node: b, Status: await b.StatusAsync()) };
            var leading = statuses.Where(s => s.Status.GetProperty("role").GetString() == "Leader" && State(s.Status, "main") == "Running").ToList();
            var standing = statuses.Where(s => s.Status.GetProperty("role").GetString() == "Standby" && s.Status.GetProperty("warm").GetBoolean()).ToList();
            if (leading.Count == 1 && standing.Count == 1 && standing[0].Status.GetProperty("leader").GetString() == leading[0].Node.Id)
                return (leading[0].Node, leading[0].Status, standing[0].Node);
            Assert.True(clock.Elapsed < TimeSpan.FromMinutes(3),
                $"the nodes did not settle as one leader and one warm standby: a is {statuses[0].Status.GetProperty("role").GetString()}, b is {statuses[1].Status.GetProperty("role").GetString()}." +
                $"{Environment.NewLine}{a.Dump()}{Environment.NewLine}{b.Dump()}");
            await Task.Delay(500);
        }
    }

    private static async Task SinkEqualsAsync(Deployment d, List<Trade> trades, AppNode a, AppNode b, string what, TimeSpan? patience = null)
    {
        var clock = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                await KafkaTestEnv.TopicEqualsAsync(d.EnrichedTopic, Enrich.Model(trades, d.Instruments), what);
                return;
            }
            catch (Exception ex) when (patience is null || clock.Elapsed > patience)
            {
                Assert.Fail($"{ex.Message}{Environment.NewLine}{a.Dump()}{Environment.NewLine}{b.Dump()}");
            }
            catch (Exception)
            {
                // Not yet: a takeover may still be on its way.
            }
        }
    }

    [Fact]
    public async Task A_takeover_with_one_broker_down_completes_and_the_broker_comes_back_under_the_new_leader()
    {
        await RequireBrokerClusterAsync();
        await using var env = KafkaTestEnv.Create();
        var d = await DeployAsync(env);
        using var a = d.A;
        using var b = d.B;
        using var stop = new CancellationTokenSource();
        var producing = ProduceUntilAsync(env, d.TradesTopic, stop.Token);
        await Task.Delay(2000);

        // A broker goes, and before the cluster has settled the leader is killed. How the
        // broker goes is INCREMENTAL_KAFKA_FAULT's to say, and the two are different
        // things. "stop" (the default): the broker is shut down a second and a half
        // before the kill — it hands its partitions over and tells the controller, so the
        // cluster the standby takes over on already knows. "kill": the broker and the
        // leader die in the same moment, as a zone does — the standby takes over on a
        // cluster that will go on believing in the dead broker for its session timeout,
        // and its takeover waits that out.
        // INCREMENTAL_KAFKA_BROKER_AFTER_MS turns the order round: the leader is killed
        // first and the broker that many milliseconds later — at about 3,000, in the
        // middle of the standby's takeover, whose reads then begin as a partition loses
        // its leader.
        var fault = Environment.GetEnvironmentVariable("INCREMENTAL_KAFKA_FAULT") is "kill" ? "kill" : "stop";
        var index = int.TryParse(Environment.GetEnvironmentVariable("INCREMENTAL_KAFKA_BROKER"), out var chosen) ? chosen % Brokers.Length : 1;
        var brokerAfter = int.TryParse(Environment.GetEnvironmentVariable("INCREMENTAL_KAFKA_BROKER_AFTER_MS"), out var afterMs) ? afterMs : -1;
        var roles = await RolesAsync(index, env.Prefix, d);
        long killedAt;
        if (brokerAfter >= 0)
        {
            fault = "kill";
            a.Kill();
            killedAt = Now;
            await Task.Delay(brokerAfter);
            await DockerAsync("kill", Brokers[index]);
        }
        else
        {
            if (fault == "kill")
                await DockerAsync("kill", Brokers[index]);
            else
            {
                await DockerAsync("stop", "-t", "1", Brokers[index]);
                await Task.Delay(1500);
            }
            a.Kill();
            killedAt = Now;
        }
        var leading = await b.WaitForRoleAsync("Leader");
        var running = await b.StatusWhenAsync(s => State(s, "main") == "Running", "the pipeline to run on the new leader");
        var takeover = running.GetProperty("takeover");
        Measured(
            $"kafka3: kill -9 with broker {index + 1} of three ({(roles.Count == 0 ? "no role" : string.Join(", ", roles))}) " +
            $"{(brokerAfter >= 0 ? $"killed {brokerAfter} ms after it" : fault == "kill" ? "killed in the same moment" : "shut down 1.5 s before")}: kill to Leader {T(leading) - killedAt} ms " +
            $"(silence {takeover.GetProperty("silenceMs").GetDouble():F0} ms, fence {takeover.GetProperty("fenceMs").GetDouble():F0} ms, " +
            $"tables {takeover.GetProperty("tablesMs").GetDouble():F0} ms, takeover {takeover.GetProperty("totalMs").GetDouble():F0} ms)");
        Result(new Dictionary<string, object?>
        {
            ["scenario"] = "takeover-broker-down", ["prefix"] = env.Prefix, ["fault"] = fault, ["broker"] = index + 1, ["roles"] = roles, ["killToLeaderMs"] = T(leading) - killedAt,
            ["brokerAfterMs"] = brokerAfter >= 0 ? brokerAfter : null,
            ["takeover"] = takeover.Clone(),
        });

        // The broker returns while the new leader commits.
        await DockerAsync("start", Brokers[index]);
        await ClusterWholeAsync(env.Prefix);
        await Task.Delay(2000);
        stop.Cancel();
        var trades = await producing;
        await KafkaTestEnv.TopicEqualsAsync(d.EnrichedTopic, Enrich.Model(trades, d.Instruments), "the enriched sink after a takeover with a broker down");
        var status = await b.StatusAsync();
        Assert.Equal("Leader", status.GetProperty("role").GetString());
        Assert.Equal(2, status.GetProperty("term").GetInt64());

        // And the killed node comes back as its standby.
        using var again = new AppNode(env, "a", d.Ports[0], d.Ports[1]);
        await again.StatusWhenAsync(
            s => s.GetProperty("warm").GetBoolean() && s.GetProperty("leader").GetString() == "b" && State(s, "main") == "Built", "the restarted node to stand by");
    }

    [Fact]
    public async Task Seventy_seconds_without_a_quorum_of_brokers_ends_with_one_leader_and_nothing_lost()
    {
        await RequireBrokerClusterAsync();
        await using var env = KafkaTestEnv.Create();
        var d = await DeployAsync(env, supervised: true);
        using var a = d.A;
        using var b = d.B;
        var load = new Load(env, d);
        await Task.Delay(2000);

        // Two of three brokers: no controller quorum, no partition with two replicas in
        // sync. Nothing can be written or elected — for longer than the heartbeat
        // interval and the takeover ceiling put together, and well under the writer's
        // transaction timeout (KafkaLogOptions.TransactionTimeout, five minutes): the
        // leader's open transaction outlives the outage, and so must the leader.
        // INCREMENTAL_KAFKA_OUTAGE_SECONDS changes how long the brokers are away, and
        // INCREMENTAL_KAFKA_TRANSACTION_TIMEOUT_SECONDS the nodes' transaction timeout:
        // 60 with the default 70 s outage is an outage past the timeout, which ends the
        // leader's writer more often than not (the node exits 3, the standby leads; what
        // decides is in durability-and-standby.md, A leader does not outlive its
        // transaction timeout) and is required only to lose nothing.
        // INCREMENTAL_KAFKA_FAULT=kill takes the two away in one command and without a
        // word to the controller; otherwise they are stopped one after the other, a
        // second each to shut down in (which the second of them may not manage).
        // INCREMENTAL_KAFKA_FAULT=pause freezes them: they keep their connections and
        // answer nothing, as a zone cut off from the network does, and nothing refuses
        // the nodes' requests — they wait out their timeouts.
        var seconds = int.TryParse(Environment.GetEnvironmentVariable("INCREMENTAL_KAFKA_OUTAGE_SECONDS"), out var asked) ? asked : 70;
        var fault = Environment.GetEnvironmentVariable("INCREMENTAL_KAFKA_FAULT") is { } named && named is "kill" or "pause" ? named : "stop";
        var transactionTimeout = double.TryParse(
            Environment.GetEnvironmentVariable("INCREMENTAL_KAFKA_TRANSACTION_TIMEOUT_SECONDS"), CultureInfo.InvariantCulture, out var given)
            ? TimeSpan.FromSeconds(given)
            : new Incremental.Kafka.KafkaLogOptions { BootstrapServers = "unused" }.TransactionTimeout;
        var outage = Stopwatch.StartNew();
        if (fault is "kill" or "pause")
            await DockerAsync(fault, Brokers[0], Brokers[1]);
        else
        {
            await DockerAsync("stop", "-t", "1", Brokers[0]);
            await DockerAsync("stop", "-t", "1", Brokers[1]);
        }
        // Near the end of the outage the standby is asked for its status, as an operator
        // would ask: it must answer in seconds with the brokers gone. (It once took
        // thirty: a following table's lag was asked of a log whose broker was dead.)
        await Task.Delay(TimeSpan.FromSeconds(Math.Max(1, seconds - 8)));
        var answering = Stopwatch.StartNew();
        await b.StatusAsync();
        var statusMs = answering.ElapsedMilliseconds;
        if (outage.Elapsed < TimeSpan.FromSeconds(seconds))
            await Task.Delay(TimeSpan.FromSeconds(seconds) - outage.Elapsed);

        // The brokers first, in one command, and the time taken from before and after
        // it: the outage is as long as it was asked to be, whatever the counter costs
        // to read.
        var downFor = outage.ElapsedMilliseconds;
        await DockerAsync(fault == "pause" ? "unpause" : "start", Brokers[0], Brokers[1]);
        var startedAfter = outage.ElapsedMilliseconds;
        var held = (await b.StatusAsync()).GetProperty("counters").GetProperty("takeoversHeld").GetInt64();
        Measured(
            $"kafka3: two of three brokers {(fault == "kill" ? "killed" : fault == "pause" ? "frozen" : "stopped")} and down for {downFor} ms (started again by {startedAfter} ms); " +
            $"the standby held its takeover back {held} time(s) at the ceiling and answered a status request in {statusMs} ms during the outage");
        Assert.True(statusMs < 10_000, $"the standby took {statusMs} ms to answer a status request with two brokers down");
        await ClusterWholeAsync(env.Prefix);
        var back = Stopwatch.StartNew();

        // What is required: in the end one node leads and runs the pipeline, the other
        // stands by, and nothing is lost. A node that ends is started again by its
        // supervisor; which had to, and how, is what the measurement records.
        await Task.Delay(2000);
        var (trades, stall) = await load.StopAsync();
        var moved = await ConcludeAsync(
            "no-quorum", env, d, trades, stall, $"two of three brokers were down for {seconds} s",
            new()
            {
                ["takeoversHeld"] = held, ["wholeToSettledMs"] = null, ["outageMs"] = outage.ElapsedMilliseconds, ["outageSeconds"] = seconds,
                ["downForMs"] = downFor, ["fault"] = fault, ["statusDuringOutageMs"] = statusMs,
                ["transactionTimeoutS"] = transactionTimeout.TotalSeconds,
            });
        Measured($"kafka3 no-quorum: one leader, a warm standby and a complete sink {back.ElapsedMilliseconds} ms after the cluster was whole");

        // An outage the leader's transaction outlives — with half a minute for the
        // brokers to commit again, which took them 15 to 17 s in every run here — moves
        // nothing: the same node leads under the same term and no process ended. (With
        // a 60 s timeout this outage ended the leader in ten runs of fourteen.)
        if (TimeSpan.FromSeconds(seconds + 30) < transactionTimeout)
        {
            Assert.False(moved, $"the leadership moved over an outage of {seconds} s, which is under the transaction timeout of {transactionTimeout}");
            Assert.True(
                a.SupervisedExits().Count == 0 && b.SupervisedExits().Count == 0,
                $"a node process ended over an outage under the transaction timeout: a [{string.Join(",", a.SupervisedExits())}] b [{string.Join(",", b.SupervisedExits())}]");
        }
    }

    /// <summary>
    /// One broker of three away for a long time — an availability zone, not a restart —
    /// and in the middle of it the service's leader dies. The standby takes over on two
    /// brokers, the dead node's supervisor brings it back as the standby, on two brokers,
    /// and when the third returns nothing has been lost. The time away is
    /// <c>INCREMENTAL_KAFKA_ZONE_MINUTES</c> (default 1; the measured run used 10).
    /// </summary>
    [Fact]
    public async Task One_broker_gone_for_minutes_with_the_leader_killed_in_the_middle_loses_nothing()
    {
        await RequireBrokerClusterAsync();
        await using var env = KafkaTestEnv.Create();
        var d = await DeployAsync(env, supervised: true);
        using var a = d.A;
        using var b = d.B;
        var minutes = double.TryParse(Environment.GetEnvironmentVariable("INCREMENTAL_KAFKA_ZONE_MINUTES"), CultureInfo.InvariantCulture, out var asked) ? asked : 1;
        var index = Random.Shared.Next(Brokers.Length);
        var load = new Load(env, d);
        await Task.Delay(2000);
        var roles = await RolesAsync(index, env.Prefix, d);

        var away = Stopwatch.StartNew();
        await DockerAsync("kill", Brokers[index]);
        long killToLeader;
        try
        {
            await Task.Delay(TimeSpan.FromMinutes(minutes / 2));

            // The leader's process dies. Its supervisor starts it again at once; by then
            // the standby is on its way.
            var before = (await a.StatusAsync()).GetProperty("term").GetInt64();
            Assert.Equal("Leader", (await a.StatusAsync()).GetProperty("role").GetString());
            a.KillNode();
            var killed = Stopwatch.StartNew();
            await b.StatusWhenAsync(s => s.GetProperty("role").GetString() == "Leader" && State(s, "main") == "Running", "the standby to lead on two brokers");
            killToLeader = killed.ElapsedMilliseconds;
            await a.StatusWhenAsync(
                s => s.GetProperty("role").GetString() == "Standby" && s.GetProperty("warm").GetBoolean() && s.GetProperty("leader").GetString() == "b",
                "the killed node to be back as a warm standby, on two brokers");
            Assert.Equal(2, a.Starts);
            Assert.Equal(before + 1, (await b.StatusAsync()).GetProperty("term").GetInt64());
            await Task.Delay(TimeSpan.FromMinutes(minutes / 2));
        }
        finally
        {
            await DockerAsync("start", Brokers[index]);
        }
        Measured($"kafka3: broker {index + 1} ({string.Join(", ", roles)}) away for {away.Elapsed.TotalSeconds:F0} s; the leader was killed half way and its standby led with the pipeline running {killToLeader} ms later");
        await ClusterWholeAsync(env.Prefix);
        await Task.Delay(2000);
        var (trades, stall) = await load.StopAsync();
        var moved = await ConcludeAsync(
            "zone", env, d, trades, stall, $"broker {index + 1} was away for {away.Elapsed.TotalSeconds:F0} s and the leader was killed half way",
            new() { ["broker"] = index + 1, ["roles"] = roles, ["awayS"] = away.Elapsed.TotalSeconds, ["killToLeaderMs"] = killToLeader });
        Assert.True(moved);
        var end = await b.StatusAsync();
        Assert.Equal("Leader", end.GetProperty("role").GetString());
        Assert.Equal(2, end.GetProperty("term").GetInt64());
    }

    /// <summary>
    /// The supervisor contract (<see cref="NodeSupervisor"/>), on any broker: a node that
    /// ends because it lost the leadership — fenced, exit 3; resigned, exit 4 — or because
    /// it was killed is started again and stands by, so two nodes stay two nodes whatever
    /// happens to the leadership.
    /// </summary>
    [Fact]
    public async Task A_supervised_node_that_is_fenced_resigns_or_is_killed_comes_back_as_the_standby_by_itself()
    {
        await using var env = KafkaTestEnv.Create();
        var ports = KafkaTestEnv.FreePorts(2);
        using var a = new AppNode(env, "a", ports[0], ports[1], supervised: true);
        await a.StatusWhenAsync(s => s.GetProperty("role").GetString() == "Leader", "a to lead");
        using var b = new AppNode(env, "b", ports[1], ports[0], supervised: true);
        await b.StatusWhenAsync(s => s.GetProperty("warm").GetBoolean() && s.GetProperty("leader").GetString() == "a", "b to stand by");

        Task Standing(AppNode node, string leader, string what) => node.StatusWhenAsync(
            s => s.GetProperty("role").GetString() == "Standby" && s.GetProperty("warm").GetBoolean() && s.GetProperty("leader").GetString() == leader, what);
        Task Leading(AppNode node, long term, string what) => node.StatusWhenAsync(
            s => s.GetProperty("role").GetString() == "Leader" && s.GetProperty("term").GetInt64() == term, what);

        // Fenced: b takes over from under a. a's process ends with 3 and is started again.
        await b.RunAsync("takeover force", "tookover");
        await Standing(a, "b", "the fenced node to be back as the standby");
        Assert.Equal([3], a.SupervisedExits());
        Assert.Equal(2, a.Starts);

        // Resigned: b steps down to its ready standby and ends with 4; a leads; b is back.
        await b.StatusWhenAsync(
            s => s.GetProperty("peer") is { ValueKind: JsonValueKind.Object } peer && peer.GetProperty("ready").GetBoolean(), "the leader to see a ready standby");
        b.Send("stepdown");
        await Leading(a, 3, "a to lead after the step-down");
        await Standing(b, "a", "the resigned node to be back as the standby");
        Assert.Equal([4], b.SupervisedExits());

        // Killed: no exit code of its own; the supervisor starts it again all the same.
        a.KillNode();
        await Leading(b, 4, "b to lead after the kill");
        await Standing(a, "b", "the killed node to be back as the standby");
        Assert.Equal(2, a.SupervisedExits().Count);
        Assert.Equal(3, a.Starts);
        Assert.False(a.HasExited);
        Assert.False(b.HasExited);
    }

    [Fact]
    public async Task A_fenced_process_exits_with_3_and_a_resigned_one_with_4_and_each_time_the_other_leads()
    {
        await using var env = KafkaTestEnv.Create();
        var ports = KafkaTestEnv.FreePorts(2);
        using var a = new AppNode(env, "a", ports[0], ports[1]);
        await a.WaitForRoleAsync("Leader");
        using var b = new AppNode(env, "b", ports[1], ports[0]);
        await b.StatusWhenAsync(s => s.GetProperty("warm").GetBoolean() && s.GetProperty("leader").GetString() == "a", "the standby to be warm");

        // A takeover from under a healthy leader: fenced is final, and the process says so.
        await b.RunAsync("takeover", "tookover");
        Assert.Equal(3, await a.ExitCodeAsync());
        var status = await b.StatusAsync();
        Assert.Equal("Leader", status.GetProperty("role").GetString());
        Assert.Equal("manual", status.GetProperty("takeover").GetProperty("reason").GetString());

        // With nobody standing by, the leader will not step down unless forced.
        b.Send("stepdown");
        var refused = await b.WaitAsync("error", e => e.GetProperty("command").GetString() == "stepdown");
        Assert.Equal("not-ready", refused.GetProperty("code").GetString());

        // The old leader's process comes back as the standby, and the leader hands over to it.
        using var again = new AppNode(env, "a", ports[0], ports[1]);
        await again.StatusWhenAsync(s => s.GetProperty("warm").GetBoolean() && s.GetProperty("leader").GetString() == "b", "the restarted node to be warm");
        await b.StatusWhenAsync(
            s => s.GetProperty("peer") is { ValueKind: JsonValueKind.Object } peer && peer.GetProperty("ready").GetBoolean(), "the leader to see a ready standby");
        var asked = Now;
        b.Send("stepdown");
        Assert.Equal(4, await b.ExitCodeAsync());
        var leading = await again.WaitForRoleAsync("Leader");
        var handed = await again.StatusAsync();
        Assert.Equal("resigned", handed.GetProperty("takeover").GetProperty("reason").GetString());
        Measured($"service step-down: resigned to Leader in {T(leading) - asked} ms (no silence waited out; TakeoverAfter is 3000 ms)");
        Assert.True(T(leading) - asked < 2500, "the hand-over waited out the takeover timeout instead of reading the resignation");
    }
}
