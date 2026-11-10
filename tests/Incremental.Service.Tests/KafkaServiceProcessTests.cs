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
            _process.StandardInput.WriteLine(command);
            _process.StandardInput.Flush();
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
            s => s.GetProperty("warm").GetBoolean() && State(s, "main") == "Unbuilt" && s.GetProperty("leader").GetString() == "a",
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
            s => s.GetProperty("warm").GetBoolean() && s.GetProperty("leader").GetString() == "b", "the restarted node to stand by");
        Assert.Equal("Standby", back.GetProperty("role").GetString());
        Assert.Equal("Unbuilt", State(back, "late"));
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
