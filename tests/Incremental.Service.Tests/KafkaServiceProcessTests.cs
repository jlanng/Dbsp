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

        public AppNode(KafkaTestEnv env, string id, int probePort, int peerPort, bool plugins = false, int tables = 0)
        {
            Id = id;
            var dll = AppDllPath();
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
            _process.ErrorDataReceived += (_, e) =>
            {
                lock (_stderr)
                    _stderr.AppendLine(e.Data);
            };
            _process.BeginOutputReadLine();
            _process.BeginErrorReadLine();
        }

        public string Id { get; }

        public bool HasExited => _process.HasExited;

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
            while (Events("status").Count == asked)
            {
                if (clock.Elapsed > Patience)
                    Assert.Fail($"Node '{Id}' did not answer a status request.{Environment.NewLine}{Dump()}");
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
        Assert.True(Ms("tablesMs") < 2000, $"the table stage took {Ms("tablesMs"):F0} ms for {extraTables + 1} tables; the target is under 2 s");

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
        await ClusterWholeAsync("__transaction_state");
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

    /// <summary>Waits until every broker is back and every partition of the deployment's
    /// own topics has all its replicas in sync again.</summary>
    private static async Task ClusterWholeAsync(string prefix)
    {
        var clock = Stopwatch.StartNew();
        using var admin = new Confluent.Kafka.AdminClientBuilder(new Confluent.Kafka.AdminClientConfig { BootstrapServers = KafkaTestEnv.Bootstrap }).Build();
        while (true)
        {
            try
            {
                var metadata = admin.GetMetadata(TimeSpan.FromSeconds(5));
                var partitions = metadata.Topics.Where(t => t.Topic.StartsWith(prefix + ".", StringComparison.Ordinal) || t.Topic == "__transaction_state")
                    .SelectMany(t => t.Partitions).ToList();
                if (metadata.Brokers.Count >= Brokers.Length && partitions.Count > 0
                    && partitions.All(p => p.Leader >= 0 && p.InSyncReplicas.Length == p.Replicas.Length))
                    return;
            }
            catch (Confluent.Kafka.KafkaException)
            {
            }
            Assert.True(clock.Elapsed < TimeSpan.FromMinutes(3), "the broker cluster did not become whole again within three minutes");
            await Task.Delay(500);
        }
    }

    private sealed record Deployment(AppNode A, AppNode B, string TradesTopic, string InstrumentsTopic, string EnrichedTopic, List<Instrument> Instruments, int[] Ports);

    /// <summary>A leads with a Kafka-loaded table and one plugin pipeline between two
    /// topics; B stands by, warm.</summary>
    private static async Task<Deployment> DeployAsync(KafkaTestEnv env)
    {
        var tradesTopic = await env.CreateTopicAsync("trades", partitions: 2);
        var instrumentsTopic = await env.CreateTopicAsync("instruments", compacted: true);
        var enrichedTopic = await env.CreateTopicAsync("enriched", compacted: true);
        var ports = KafkaTestEnv.FreePorts(2);
        var instruments = Instruments(10);
        foreach (var instrument in instruments)
            env.ProduceJson(instrumentsTopic, instrument.Id, instrument);
        await env.FlushAsync();

        var a = new AppNode(env, "a", ports[0], ports[1], plugins: true);
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
        var b = new AppNode(env, "b", ports[1], ports[0]);
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
    /// leader's commits. Required: nothing lost, and one node leading with the pipeline
    /// running. While a broker's partitions and its coordinator move, the service's leader
    /// can commit nothing, and if that silence reaches the takeover ceiling (30 s) the
    /// standby takes over, as it should: whether it did is recorded, not asserted — on
    /// three brokers on one machine it happens in about one run in five.
    /// </summary>
    private async Task EveryBrokerInTurnAsync(bool kill)
    {
        await RequireBrokerClusterAsync();
        await using var env = KafkaTestEnv.Create();
        var d = await DeployAsync(env);
        using var a = d.A;
        using var b = d.B;
        using var stop = new CancellationTokenSource();
        var producing = ProduceUntilAsync(env, d.TradesTopic, stop.Token);
        var how = kill ? "killed" : "shut down";
        foreach (var broker in Brokers)
        {
            await Task.Delay(2000);
            var stopped = Stopwatch.StartNew();
            if (kill)
                await DockerAsync("kill", broker);
            else
                await DockerAsync("stop", "-t", "30", broker);
            await Task.Delay(kill ? 8000 : 5000);
            await DockerAsync("start", broker);
            await ClusterWholeAsync(env.Prefix);
            Measured($"kafka3: broker {broker} {how} and back, cluster whole again after {stopped.ElapsedMilliseconds} ms");
        }
        await Task.Delay(2000);
        stop.Cancel();
        var trades = await producing;

        // The sink first: it is complete only once a leader that can commit has caught up,
        // which is after a takeover if one is still to come (a leader stalled on its
        // brokers still answers as the leader until the standby's ceiling is reached).
        await SinkEqualsAsync(d, trades, a, b, $"the enriched sink after every broker was {how} in turn", TimeSpan.FromMinutes(3));

        var leading = new List<(AppNode Node, JsonElement Status)>();
        var clock = Stopwatch.StartNew();
        while (leading.Count != 1 || State(leading[0].Status, "main") != "Running")
        {
            Assert.True(clock.Elapsed < Patience, $"no single leader ran the pipeline after the brokers were back.{Environment.NewLine}{a.Dump()}{Environment.NewLine}{b.Dump()}");
            await Task.Delay(500);
            leading.Clear();
            foreach (var node in new[] { a, b })
            {
                if (node.HasExited)
                    continue;
                try
                {
                    var status = await node.StatusAsync();
                    if (status.GetProperty("role").GetString() == "Leader")
                        leading.Add((node, status));
                }
                catch (Exception) when (node.HasExited)
                {
                }
            }
        }
        var (leader, final) = leading[0];
        var other = ReferenceEquals(leader, a) ? b : a;
        var moved = final.GetProperty("term").GetInt64() != 1;
        Measured(
            $"kafka3: {trades.Count} trades through three brokers {how} in turn; node {leader.Id} leads at term {final.GetProperty("term").GetInt64()}" +
            (moved
                ? $" (takeover reason: {final.GetProperty("takeover").GetProperty("reason").GetString()}), the other node " +
                  (other.HasExited ? $"exited with code {await other.ExitCodeAsync()}" : "is " + (await other.StatusAsync()).GetProperty("role").GetString())
                : ", nobody took over") +
            $"; the pipeline restarted {final.GetProperty("pipelines").EnumerateArray().Single().GetProperty("restarts").GetInt32()} time(s) on the leader; " +
            $"the first leader's longest wait for the brokers to take a transaction was {a.LongestWaitForBrokers()} s");
        if (!moved)
            await b.StatusWhenAsync(
                s => s.GetProperty("warm").GetBoolean() || s.GetProperty("role").GetString() != "Standby", "the standby to be warm again");
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

        // A broker goes, and before the cluster has settled the leader is killed.
        await DockerAsync("stop", "-t", "1", Brokers[1]);
        await Task.Delay(1500);
        a.Kill();
        var killedAt = Now;
        var leading = await b.WaitForRoleAsync("Leader");
        Measured($"kafka3: kill -9 with one of three brokers down: kill to Leader {T(leading) - killedAt} ms");
        await b.StatusWhenAsync(s => State(s, "main") == "Running", "the pipeline to run on the new leader");

        // The broker returns while the new leader commits.
        await DockerAsync("start", Brokers[1]);
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
        var d = await DeployAsync(env);
        using var a = d.A;
        using var b = d.B;
        using var stop = new CancellationTokenSource();
        var producing = ProduceUntilAsync(env, d.TradesTopic, stop.Token);
        await Task.Delay(2000);

        // Two of three brokers: no controller quorum, no partition with two replicas in
        // sync. Nothing can be written or elected — for longer than the transaction
        // timeout (60 s), the heartbeat interval and the takeover ceiling put together.
        var outage = Stopwatch.StartNew();
        await DockerAsync("stop", "-t", "1", Brokers[0]);
        await DockerAsync("stop", "-t", "1", Brokers[1]);
        await Task.Delay(TimeSpan.FromSeconds(70));
        await DockerAsync("start", Brokers[0]);
        await DockerAsync("start", Brokers[1]);
        Measured($"kafka3: two of three brokers down for {outage.ElapsedMilliseconds} ms");
        await ClusterWholeAsync(env.Prefix);
        var back = Stopwatch.StartNew();

        // What is required: in the end one node leads and runs the pipeline, and nothing
        // is lost. A node that exits is started again, as the process supervisor it runs
        // under would: which nodes had to end, and how, is what the measurement records.
        // A leader that cannot commit still answers as the leader until its standby's
        // takeover fences it, so "one leader" is only believed together with a complete
        // sink, which needs a leader that commits.
        await Task.Delay(2000);
        stop.Cancel();
        var trades = await producing;
        var expected = Enrich.Model(trades, d.Instruments);
        var nodes = new Dictionary<string, AppNode> { ["a"] = a, ["b"] = b };
        var started = new List<AppNode>();
        var exits = new List<string>();
        try
        {
            AppNode? leader = null;
            while (leader is null)
            {
                Assert.True(back.Elapsed < TimeSpan.FromMinutes(5),
                    $"no node led with the pipeline running and the sink complete within five minutes of the brokers' return ({string.Join("; ", exits)}).{Environment.NewLine}{nodes["a"].Dump()}{Environment.NewLine}{nodes["b"].Dump()}");
                await Task.Delay(1000);
                var leading = new List<AppNode>();
                foreach (var id in new[] { "a", "b" })
                {
                    var node = nodes[id];
                    if (node.HasExited)
                    {
                        exits.Add($"{id} exited with code {await node.ExitCodeAsync()} {back.ElapsedMilliseconds} ms after the cluster was whole");
                        var again = new AppNode(env, id, id == "a" ? d.Ports[0] : d.Ports[1], id == "a" ? d.Ports[1] : d.Ports[0]);
                        started.Add(again);
                        nodes[id] = again;
                        continue;
                    }
                    try
                    {
                        var status = await node.StatusAsync();
                        if (status.GetProperty("role").GetString() == "Leader" && State(status, "main") == "Running")
                            leading.Add(node);
                    }
                    catch (Exception) when (node.HasExited)
                    {
                        // It ended while it was being asked; the next round restarts it.
                    }
                }
                if (leading.Count != 1)
                    continue;
                var actual = await Task.Run(() => KafkaTestEnv.ReadTable<long, EnrichedTrade>(d.EnrichedTopic));
                if (actual.Count == expected.Count && expected.All(e => actual.TryGetValue(e.Key, out var row) && Equals(row, e.Value)))
                    leader = leading[0];
            }
            var final = await leader.StatusAsync();
            Measured(
                $"kafka3: after the outage node {leader.Id} leads at term {final.GetProperty("term").GetInt64()} with the pipeline running and the sink complete " +
                $"({trades.Count} trades), {back.ElapsedMilliseconds} ms after the cluster was whole; " +
                (exits.Count == 0 ? "no node ended" : string.Join("; ", exits)) +
                $"; the pipeline restarted {final.GetProperty("pipelines").EnumerateArray().Single().GetProperty("restarts").GetInt32()} time(s) on the leader");
        }
        finally
        {
            foreach (var node in started)
                node.Dispose();
        }
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
