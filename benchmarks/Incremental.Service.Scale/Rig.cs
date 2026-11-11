using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Scale.Contracts;

namespace Incremental.Service.Scale;

/// <summary>
/// The run: two node processes on one broker, a reference table of <c>--rows</c> wide rows
/// shared by <c>--pipelines</c> pipelines, <c>--joins</c> and <c>--aggregates</c> of them
/// holding state for <c>--state-keys</c> orders, a steady input, and then what production
/// does to it — a cold standby, <c>kill -9</c> of the leader, a manual takeover, and one
/// node started from nothing. Every number is printed as it is measured and written to
/// <c>--out</c> as JSON.
/// </summary>
internal sealed class Rig(Arguments arguments)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { Converters = { new JsonStringEnumConverter() } };
    private static readonly TimeSpan Patience = TimeSpan.FromMinutes(30);

    private readonly string _bootstrap = arguments.Text("--bootstrap", "localhost:9092");
    private readonly string _prefix = arguments.Text("--prefix", "scale" + Guid.NewGuid().ToString("N")[..10]);
    private readonly long _rows = arguments.Long("--rows", 1_000_000);
    private readonly long _stateKeys = arguments.Long("--state-keys", 1_000_000);
    private readonly int _pipelines = arguments.Number("--pipelines", 20);
    private readonly int _joins = arguments.Number("--joins", 2);
    private readonly int _aggregates = arguments.Number("--aggregates", 2);
    private readonly int _rate = arguments.Number("--rate", 2000);
    private readonly int _churn = arguments.Number("--churn", 500);
    private readonly int _steadySeconds = arguments.Number("--steady-seconds", 30);
    private readonly int _replication = arguments.Number("--replication", 1);
    private readonly int _takeoverAfterMs = arguments.Number("--takeover-after-ms", 3000);
    private readonly string _container = arguments.Text("--broker-container");
    private readonly string _brokerData = arguments.Text("--broker-data", "/tmp/kafka-logs");
    private readonly SortedDictionary<string, object?> _results = new(StringComparer.Ordinal);
    private readonly List<string> _summary = [];
    private readonly string _work = Path.Combine(Path.GetTempPath(), "incremental-scale", Guid.NewGuid().ToString("N")[..8]);
    private readonly List<NodeProcess> _nodes = [];

    // The rig's own clients (it creates topics, produces the input, reads the sinks) may
    // be another principal than the nodes', so that the nodes' permissions are exactly
    // the ones granted to theirs.
    private readonly Arguments _own = arguments.Has("--oauth-rig") ? arguments.With("--oauth", arguments.Text("--oauth-rig")) : arguments;

    private static long Now => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private string OrdersTopic => _prefix + ".orders";
    private string FeedTopic => _prefix + ".in.parties";
    private string SinkTopic(string pipeline) => $"{_prefix}.out.{pipeline}";

    private T Client<T>(T config) where T : ClientConfig
    {
        config.BootstrapServers = _bootstrap;
        foreach (var (key, value) in Security.ClientConfig(_own))
            config.Set(key, value);
        return config;
    }

    private IAdminClient Admin()
    {
        var builder = new AdminClientBuilder(Client(new AdminClientConfig()));
        Security.Admin(_own)?.Invoke(builder);
        return builder.Build();
    }

    private void Note(string key, object? value, string? unit = null)
    {
        _results[key] = value;
        var line = $"{key}: {value}{(unit is null ? "" : " " + unit)}";
        _summary.Add(line);
        Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] {line}");
    }

    private static void Say(string line) => Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss}] {line}");

    private NodeProcess Start(string id, int probePort, int peerPort, bool plugins, string? cache = null)
    {
        var args = new List<string>
        {
            "node",
            "--bootstrap", _bootstrap,
            "--prefix", _prefix,
            "--node", id,
            "--probe-port", probePort.ToString(CultureInfo.InvariantCulture),
            "--peer-port", peerPort.ToString(CultureInfo.InvariantCulture),
            "--replication", _replication.ToString(CultureInfo.InvariantCulture),
            "--takeover-after-ms", _takeoverAfterMs.ToString(CultureInfo.InvariantCulture),
            "--cache", cache ?? Path.Combine(_work, "cache-" + id),
            "--plugins", plugins ? Path.Combine(AppContext.BaseDirectory, "plugins") : "",
            "--log-level", arguments.Text("--log-level", "Warning"),
            "--supervise",
        };
        args.AddRange(Security.Forward(arguments));
        var node = new NodeProcess(id, args, Path.Combine(_work, $"{id}-{_nodes.Count}.stderr.log"));
        _nodes.Add(node);
        return node;
    }

    public async Task<int> RunAsync()
    {
        Directory.CreateDirectory(_work);
        Say($"prefix {_prefix}; work directory {_work}");
        Note("config", $"rows={_rows} stateKeys={_stateKeys} pipelines={_pipelines} (joins={_joins}, aggregates={_aggregates}) " +
                       $"rate={_rate}/s churn={_churn} rows/s replication={_replication} takeoverAfter={_takeoverAfterMs} ms bootstrap={_bootstrap}");
        var ports = FreePorts(3);
        var producerBuilder = new ProducerBuilder<byte[], byte[]>(Client(new ProducerConfig { LingerMs = 5, CompressionType = CompressionType.Lz4 }));
        Security.Producer(_own)?.Invoke(producerBuilder);
        using var producer = producerBuilder.Build();
        SinkWatch? watch = null;
        var steady = new CancellationTokenSource();
        Task? steadyTask = null;
        try
        {
            var pipelines = Pipelines();
            await CreateTopicsAsync(pipelines);

            // ── A: the first node leads an empty deployment ────────────────────────────
            var startedA = Now;
            var a = Start("a", ports[0], ports[1], plugins: true);
            var lead = await a.WaitForRoleAsync("Leader");
            Note("first-lead.from-process-start", T(lead) - startedA, "ms");

            // ── B: the table ───────────────────────────────────────────────────────────
            await a.RunAsync("declare " + JsonSerializer.Serialize(
                new TableSpec { Name = Reference.Parties.Name, Definition = typeof(PartiesTable).FullName! }, Json), "declared");
            var loadClock = Stopwatch.StartNew();
            await a.RunAsync($"load 0 {_rows} {arguments.Number("--load-batch", 20_000)} 0", "loaded");
            Note("table.load", loadClock.ElapsedMilliseconds, "ms");
            Note("table.load.rows-per-second", (long)(_rows / loadClock.Elapsed.TotalSeconds));
            var afterLoad = await a.GcAsync();
            Note("leader.heap-after-load", afterLoad.Heap, "MB");
            Note("leader.heap-per-million-rows", Math.Round(afterLoad.Heap / (_rows / 1e6)), "MB");
            if (BrokerBytes($"{_prefix}.table.") is { } tableBytes)
            {
                Note("broker.table-bytes", tableBytes / 1048576, "MB");
                Note("broker.table-bytes-per-row", tableBytes / _rows, "bytes");
            }

            // A second, small table filled the other way: by the kafka loader from a topic,
            // so that a run also covers the loader's consumer and its resumption.
            const int fed = 2000;
            for (long id = 0; id < fed; id++)
                producer.Produce(FeedTopic, new Message<byte[], byte[]>
                {
                    Key = JsonSerializer.SerializeToUtf8Bytes(id, JsonSerializerOptions.Web),
                    Value = JsonSerializer.SerializeToUtf8Bytes(Party.Make(id, 0), JsonSerializerOptions.Web),
                });
            producer.Flush(TimeSpan.FromSeconds(30));
            await a.RunAsync("declare " + JsonSerializer.Serialize(new TableSpec
            {
                Name = "parties-feed",
                Definition = typeof(PartiesTable).FullName!,
                Loader = new TableLoaderSpec("kafka", "log", JsonDocument.Parse($$"""{ "topic": "{{FeedTopic}}" }""").RootElement.Clone()),
            }, Json), "declared");
            await a.StatusWhenAsync(s => Rows(s, "parties-feed") == fed, "the kafka loader to fill its table");

            // ── C: the pipelines, with nothing to read yet ─────────────────────────────
            var deployClock = Stopwatch.StartNew();
            foreach (var (id, definition) in pipelines)
                await a.RunAsync("deploy " + JsonSerializer.Serialize(Spec(id, definition), Json), "deployed");
            await a.StatusWhenAsync(s => AllRunning(s, pipelines.Count), "every pipeline to run");
            Note("pipelines.deploy-all", deployClock.ElapsedMilliseconds, "ms");
            var afterDeploy = await a.GcAsync();
            Note("leader.heap-after-deploy", afterDeploy.Heap, "MB");
            Note("leader.heap-added-by-pipelines", afterDeploy.Heap - afterLoad.Heap, "MB");

            // ── D: the state ───────────────────────────────────────────────────────────
            var heavy = pipelines.Where(p => p.Definition != "order-lookup").Select(p => p.Id).ToHashSet();
            var fillClock = Stopwatch.StartNew();
            for (long id = 0; id < _stateKeys; id++)
            {
                Produce(producer, Order.Make(id, _rows, 0));
                if (id % 100_000 == 99_999)
                    producer.Flush(TimeSpan.FromSeconds(60));
            }
            producer.Flush(TimeSpan.FromSeconds(60));
            await a.StatusWhenAsync(
                s => s.GetProperty("pipelines").EnumerateArray()
                    .Where(p => heavy.Contains(p.GetProperty("id").GetString()!))
                    .All(p => p.GetProperty("read").GetInt64() >= _stateKeys && p.GetProperty("lagKnown").GetBoolean() && p.GetProperty("lag").GetInt64() == 0),
                "the stateful pipelines to have read every order", pollMs: 500);
            Note("state.fill", fillClock.ElapsedMilliseconds, "ms");
            Note("state.fill.orders-per-second-per-pipeline", (long)(_stateKeys / fillClock.Elapsed.TotalSeconds));
            var afterFill = await a.GcAsync();
            Note("leader.heap-after-state", afterFill.Heap, "MB");
            Note("leader.heap-added-by-state", afterFill.Heap - afterDeploy.Heap, "MB");
            if (BrokerBytes($"{_prefix}.state-") is { } stateBytes)
                Note("broker.state-bytes", stateBytes / 1048576, "MB");

            watch = new SinkWatch(
                Client(new ConsumerConfig { GroupId = _prefix + ".rig", EnableAutoCommit = false }), Security.Consumer(_own),
                pipelines.Select(p => SinkTopic(p.Id)).ToList());
            steadyTask = Task.Run(() => SteadyAsync(producer, steady.Token));
            if (arguments.Has("--single"))
            {
                // One node only: a size this machine cannot hold twice. No standby, no
                // takeover; the node is killed and one is started from nothing.
                Note("leader.rss", (await a.GcAsync()).Rss, "MB");
                await a.RunAsync($"churn {_churn} {_rows}", "churning");
                await Task.Delay(TimeSpan.FromSeconds(_steadySeconds));
                await ColdStartAsync(a, ports[2], watch, pipelines);
                steady.Cancel();
                await steadyTask;
                return 0;
            }

            // ── E: a cold standby ──────────────────────────────────────────────────────
            var startedB = Now;
            var b = Start("b", ports[1], ports[0], plugins: false);
            var warm = await b.StatusWhenAsync(s => s.GetProperty("warm").GetBoolean() && s.GetProperty("leader").GetString() == "a", "the standby to be warm", pollMs: 250);
            Note("standby.cold-to-warm", T(warm) - startedB, "ms");
            if (Rows(warm, "parties-feed") != fed || Rows(warm, Reference.Parties.Name) != _rows)
                throw new InvalidOperationException($"The warm standby holds {Rows(warm, Reference.Parties.Name)} and {Rows(warm, "parties-feed")} rows; the leader holds {_rows} and {fed}.");
            var standbyMemory = await b.GcAsync();
            Note("standby.heap-warm", standbyMemory.Heap, "MB");
            Note("standby.rss-warm", standbyMemory.Rss, "MB");
            Note("leader.rss", (await a.GcAsync()).Rss, "MB");
            await a.StatusWhenAsync(s => s.GetProperty("peerReady").GetBoolean(), "the leader to see a ready standby");

            // ── F: steady state ────────────────────────────────────────────────────────
            await a.RunAsync($"churn {_churn} {_rows}", "churning");
            var lags = new List<long>();
            var tableLags = new List<long>();
            var steadyClock = Stopwatch.StartNew();
            while (steadyClock.Elapsed < TimeSpan.FromSeconds(_steadySeconds))
            {
                await Task.Delay(1000);
                RequireMemory();
                var status = await b.StatusAsync();
                lags.Add(status.GetProperty("stateLag").GetInt64());
                tableLags.Add(status.GetProperty("tables").EnumerateArray().Sum(t => t.TryGetProperty("lag", out var lag) && lag.ValueKind == JsonValueKind.Number ? lag.GetInt64() : 0));
            }
            lags.Sort();
            tableLags.Sort();
            Note("standby.state-lag.median", lags[lags.Count / 2], "records");
            Note("standby.state-lag.max", lags[^1], "records");
            Note("standby.table-lag.median", tableLags[tableLags.Count / 2], "versions");
            Note("standby.table-lag.max", tableLags[^1], "versions");
            var leaderSteady = await a.StatusAsync();
            if (arguments.Has("--oauth"))
            {
                // Each of these is one client of the node being handed a token by its
                // refresh handler: more than one per client means the refresh works.
                Note("oauth.tokens-issued.leader", leaderSteady.GetProperty("tokens").GetInt64());
                Note("oauth.tokens-issued.standby", (await b.StatusAsync()).GetProperty("tokens").GetInt64());
            }
            Note("leader.gc-pause-percent", leaderSteady.GetProperty("memory").GetProperty("pausePercent").GetDouble());
            Note("leader.input-lag.max-per-pipeline", leaderSteady.GetProperty("pipelines").EnumerateArray().Max(p => p.GetProperty("lag").GetInt64()), "records");

            // ── G: kill -9 ─────────────────────────────────────────────────────────────
            a.Kill();
            var killedAt = Now;
            watch.Arm(killedAt + 20); // nothing the dead process produced is stamped later than this
            await TakeoverAsync("kill", b, killedAt, watch, pipelines);

            // ── H: the dead node comes back, cold, as the standby ──────────────────────
            // Nobody starts it: its supervisor does, as soon as it sees the process gone.
            await b.RunAsync($"churn {_churn} {_rows}", "churning");
            var restartedA = await a.StartedAfterAsync(killedAt);
            Note("supervisor.kill-to-process-started", restartedA - killedAt, "ms");
            var back = await a.StatusWhenAsync(s => s.GetProperty("warm").GetBoolean() && s.GetProperty("leader").GetString() == "b", "the restarted node to stand by", pollMs: 250);
            Note("standby.cold-to-warm.with-state", T(back) - restartedA, "ms");
            await b.StatusWhenAsync(s => s.GetProperty("peerReady").GetBoolean(), "the leader to see a ready standby");
            await Task.Delay(3000);

            // ── I: a manual takeover from under a healthy leader ───────────────────────
            var askedAt = Now;
            watch.Arm(askedAt);
            a.Send("takeover");
            await TakeoverAsync("manual", a, askedAt, watch, pipelines);
            Note("manual.old-leader-exit-code", await b.SupervisedExitAfterAsync(askedAt));

            // Fenced is final for the process, not for the node: its supervisor starts it
            // again and it stands by behind the node that replaced it.
            var bBack = await b.StatusWhenAsync(s => s.GetProperty("warm").GetBoolean() && s.GetProperty("leader").GetString() == "a", "the fenced node to stand by again", pollMs: 250);
            Note("manual.fenced-node-warm-standby-again", T(bBack) - askedAt, "ms");

            await ColdStartAsync(a, ports[2], watch, pipelines);
            steady.Cancel();
            await steadyTask;
            return 0;
        }
        catch (Exception ex)
        {
            Say("FAILED: " + ex.Message);
            foreach (var node in _nodes)
                Say(node.Dump());
            _results["failed"] = ex.Message;
            return 1;
        }
        finally
        {
            steady.Cancel();
            watch?.Dispose();
            foreach (var node in _nodes)
                node.Dispose();
            if (arguments.Text("--out") is { Length: > 0 } path)
                File.WriteAllText(path, JsonSerializer.Serialize(_results, new JsonSerializerOptions(Json) { WriteIndented = true }));
            if (!arguments.Has("--keep-topics"))
                await DeleteTopicsAsync();
            Console.WriteLine();
            Console.WriteLine("── summary ──");
            foreach (var line in _summary)
                Console.WriteLine(line);
        }
    }

    /// <summary>J: the last node is killed and one node is started from nothing — no
    /// cache, no plugin directory, nobody to follow: everything comes from the broker.</summary>
    private async Task ColdStartAsync(NodeProcess last, int port, SinkWatch watch, List<(string Id, string Definition)> pipelines)
    {
        // Every node and its supervisor: nothing is to come back by itself here.
        foreach (var node in _nodes)
            node.KillWithSupervisor();
        await Task.Delay(500);
        var startedC = Now;
        watch.Arm(startedC);
        var c = Start("c", port, 0, plugins: false);
        await TakeoverAsync("cold-start", c, startedC, watch, pipelines);
        var coldMemory = await c.GcAsync();
        Note("cold-start.heap", coldMemory.Heap, "MB");
        Note("cold-start.rss", coldMemory.Rss, "MB");
        c.Send("quit");
        Note("cold-start.exit-code-on-quit", await c.ExitCodeAsync());
    }

    /// <summary>One takeover, from the moment <paramref name="from"/> (the kill, the
    /// command, the process start) to the first record in every pipeline's sink.</summary>
    private async Task TakeoverAsync(string name, NodeProcess node, long from, SinkWatch watch, List<(string Id, string Definition)> pipelines)
    {
        var promoting = await node.WaitAsync("role", e => e.GetProperty("role").GetString() == "Promoting" && T(e) >= from - 50);
        var leading = await node.WaitAsync("role", e => e.GetProperty("role").GetString() == "Leader" && T(e) >= from - 50);
        Note($"{name}.detection", T(promoting) - from, "ms");
        Note($"{name}.to-leader", T(leading) - from, "ms");
        var status = await node.StatusWhenAsync(s => AllRunning(s, pipelines.Count), "every pipeline to run on the new leader", pollMs: 100);
        var first = await watch.FirstWritesAsync(from, Patience);
        var times = first.Values.Order().ToList();
        Note($"{name}.first-sink-write.first-pipeline", times[0], "ms");
        Note($"{name}.first-sink-write.median-pipeline", times[times.Count / 2], "ms");
        Note($"{name}.first-sink-write.last-pipeline", times[^1], "ms");
        Note($"{name}.first-sink-write.last-stateful-pipeline",
            pipelines.Where(p => p.Definition != "order-lookup").Max(p => first[SinkTopic(p.Id)]), "ms");

        // The same seen from a reader of the sinks: the longest silence in each. For a
        // takeover from under a live leader this is the measure — the old leader writes
        // until it is fenced, so "the first write after the command" is its own.
        await Task.Delay(2000);
        var gaps = watch.Gaps().Values.Order().ToList();
        Note($"{name}.sink-silence.median-pipeline", gaps[gaps.Count / 2], "ms");
        Note($"{name}.sink-silence.longest-pipeline", gaps[^1], "ms");

        var takeover = status.GetProperty("takeover");
        double Ms(string stage) => takeover.GetProperty(stage).GetDouble();
        Note($"{name}.stages", $"fence {Ms("fenceMs"):F0} + waiting for tables being restored {Math.Max(0, Ms("prepareMs") - Ms("catalogMs") - Ms("tablesMs")):F0} + catalog {Ms("catalogMs"):F0} + tables {Ms("tablesMs"):F0} + storage {Ms("storageMs"):F0} + " +
                               $"leader record {Ms("leaderRecordMs"):F0} + plugin directory {Ms("pluginDirectoryMs"):F0} + pipelines {Ms("pipelinesMs"):F0} + " +
                               $"finish {Ms("finishMs"):F0} = {Ms("totalMs"):F0} ms (reason {takeover.GetProperty("reason").GetString()})");
        foreach (var stage in new[] { "fenceMs", "prepareMs", "catalogMs", "tablesMs", "storageMs", "leaderRecordMs", "pluginDirectoryMs", "pipelinesMs", "finishMs", "totalMs" })
            _results[$"{name}.stage.{stage}"] = Ms(stage);
        foreach (var group in status.GetProperty("pipelines").EnumerateArray().GroupBy(p => p.GetProperty("definition").GetString()!))
        {
            double Max(string part) => group.Max(p => p.GetProperty("start").ValueKind == JsonValueKind.Object ? p.GetProperty("start").GetProperty(part).GetDouble() : 0);
            var warm = group.Count(p => p.GetProperty("start").ValueKind == JsonValueKind.Object && p.GetProperty("start").GetProperty("warm").GetBoolean());
            Note($"{name}.pipeline-start.{group.Key}",
                $"{warm} of {group.Count()} built while standing by; slowest: plugin {Max("pluginMs"):F0} + build on state {Max("buildMs"):F0} + circuit start {Max("startMs"):F0} + edges {Max("edgesMs"):F0}; total {Max("totalMs"):F0} ms");
            foreach (var part in new[] { "pluginMs", "buildMs", "startMs", "edgesMs", "totalMs" })
                _results[$"{name}.pipeline-start.{group.Key}.{part}"] = Max(part);
        }
    }

    /// <summary>Stops the run rather than push the machine into swap.</summary>
    internal static void RequireMemory()
    {
        if (!File.Exists("/proc/meminfo"))
            return;
        var line = File.ReadLines("/proc/meminfo").FirstOrDefault(l => l.StartsWith("MemAvailable:", StringComparison.Ordinal));
        if (line is not null && long.Parse(line.Split(' ', StringSplitOptions.RemoveEmptyEntries)[1], CultureInfo.InvariantCulture) < 1_500_000)
            throw new InvalidOperationException("Less than 1.5 GB of memory is available on this machine: the run is too large for it.");
    }

    private static long Rows(JsonElement status, string table) =>
        status.GetProperty("tables").EnumerateArray().FirstOrDefault(t => t.GetProperty("name").GetString() == table) is { ValueKind: JsonValueKind.Object } found
            ? found.GetProperty("rows").GetInt64()
            : -1;

    private static long T(JsonElement e) => e.GetProperty("t").GetInt64();

    private static bool AllRunning(JsonElement status, int count)
    {
        var pipelines = status.GetProperty("pipelines");
        return pipelines.GetArrayLength() == count && pipelines.EnumerateArray().All(p => p.GetProperty("state").GetString() == "Running");
    }

    private List<(string Id, string Definition)> Pipelines()
    {
        var list = new List<(string, string)>();
        for (var i = 0; i < _pipelines; i++)
        {
            var definition = i < _joins ? "order-enrichment" : i < _joins + _aggregates ? "party-exposure" : "order-lookup";
            list.Add(($"{definition}-{i:D2}", definition));
        }
        return list;
    }

    private PipelineSpec Spec(string id, string definition)
    {
        JsonElement Binding(string topic, string extra = "") =>
            JsonDocument.Parse($$"""{ "connector": "kafka", "connection": "log", "topic": "{{topic}}"{{extra}} }""").RootElement.Clone();
        var (input, sink) = definition switch
        {
            "order-enrichment" => (Binding(OrdersTopic), "enriched"),
            "party-exposure" => (Binding(OrdersTopic), "exposure"),
            // The stateless pipelines start at the end: they are there to share the table.
            _ => (Binding(OrdersTopic, """, "startFrom": "latest" """), "tagged"),
        };
        return new PipelineSpec
        {
            Id = id,
            Plugin = "scale.pipelines",
            Definition = definition,
            Inputs = new Dictionary<string, JsonElement> { ["orders"] = input },
            Sinks = new Dictionary<string, JsonElement> { [sink] = Binding(SinkTopic(id)) },
        };
    }

    private void Produce(IProducer<byte[], byte[]> producer, Order order)
    {
        var message = new Message<byte[], byte[]>
        {
            Key = JsonSerializer.SerializeToUtf8Bytes(order.Id, JsonSerializerOptions.Web),
            Value = JsonSerializer.SerializeToUtf8Bytes(order, JsonSerializerOptions.Web),
        };
        while (true)
        {
            try
            {
                producer.Produce(OrdersTopic, message);
                return;
            }
            catch (ProduceException<byte[], byte[]> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
            {
                producer.Poll(TimeSpan.FromMilliseconds(50));
            }
        }
    }

    /// <summary>Orders at <c>--rate</c> a second, each a new revision of an order the
    /// stateful pipelines already hold, until cancelled — through every kill and takeover.</summary>
    private async Task SteadyAsync(IProducer<byte[], byte[]> producer, CancellationToken stop)
    {
        var random = new Random(11);
        var revision = 1;
        var clock = Stopwatch.StartNew();
        long sent = 0;
        while (!stop.IsCancellationRequested)
        {
            var due = (long)(clock.Elapsed.TotalSeconds * _rate);
            for (; sent < due; sent++)
                Produce(producer, Order.Make(random.NextInt64(Math.Max(1, _stateKeys)), _rows, revision));
            revision++;
            await Task.Delay(20);
        }
        producer.Flush(TimeSpan.FromSeconds(30));
    }

    private async Task CreateTopicsAsync(List<(string Id, string Definition)> pipelines)
    {
        using var admin = Admin();
        var topics = new List<TopicSpecification>
        {
            new() { Name = OrdersTopic, NumPartitions = 4, ReplicationFactor = (short)_replication },
            new()
            {
                Name = FeedTopic, NumPartitions = 2, ReplicationFactor = (short)_replication,
                Configs = new Dictionary<string, string> { ["cleanup.policy"] = "compact" },
            },
        };
        foreach (var (id, definition) in pipelines)
            topics.Add(new TopicSpecification
            {
                Name = SinkTopic(id),
                NumPartitions = 1,
                ReplicationFactor = (short)_replication,
                Configs = definition == "order-lookup" ? [] : new Dictionary<string, string> { ["cleanup.policy"] = "compact" },
            });
        await admin.CreateTopicsAsync(topics);
    }

    private async Task DeleteTopicsAsync()
    {
        try
        {
            using var admin = Admin();
            var topics = admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics
                .Select(t => t.Topic).Where(t => t.StartsWith(_prefix + ".", StringComparison.Ordinal)).ToList();
            if (topics.Count > 0)
                await admin.DeleteTopicsAsync(topics);
            Say($"deleted {topics.Count} topics under {_prefix}");
        }
        catch (Exception ex)
        {
            Say("topics not deleted: " + ex.Message);
        }
    }

    /// <summary>Bytes on one broker's disk under topics starting with
    /// <paramref name="topicPrefix"/>, asked of the container; null without
    /// <c>--broker-container</c>.</summary>
    private long? BrokerBytes(string topicPrefix)
    {
        if (_container.Length == 0)
            return null;
        try
        {
            var info = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true };
            foreach (var argument in new[] { "exec", _container, "sh", "-c", $"du -sk {_brokerData}/{topicPrefix}* 2>/dev/null | awk '{{s+=$1}} END {{print s*1024}}'" })
                info.ArgumentList.Add(argument);
            using var process = Process.Start(info)!;
            var output = process.StandardOutput.ReadToEnd();
            process.WaitForExit();
            return long.TryParse(output.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var bytes) ? bytes
                : double.TryParse(output.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var approx) ? (long)approx : null;
        }
        catch (Exception ex)
        {
            Say("broker bytes not read: " + ex.Message);
            return null;
        }
    }

    private static int[] FreePorts(int count)
    {
        var listeners = Enumerable.Range(0, count).Select(_ => new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0)).ToArray();
        foreach (var listener in listeners)
            listener.Start();
        var ports = listeners.Select(l => ((System.Net.IPEndPoint)l.LocalEndpoint).Port).ToArray();
        foreach (var listener in listeners)
            listener.Stop();
        return ports;
    }

    // ── the sinks, watched ─────────────────────────────────────────────────────────────

    /// <summary>Reads every sink topic from its end and, once armed, remembers when the
    /// first record produced after that moment arrived in each.</summary>
    private sealed class SinkWatch : IDisposable
    {
        private readonly IConsumer<byte[], byte[]> _consumer;
        private readonly List<string> _topics;
        private readonly Thread _thread;
        private readonly object _lock = new();
        private readonly Dictionary<string, long> _first = new(StringComparer.Ordinal);
        private readonly Dictionary<string, long> _last = new(StringComparer.Ordinal);
        private readonly Dictionary<string, long> _gap = new(StringComparer.Ordinal);
        private long _armedAt = long.MaxValue;
        private volatile bool _stop;

        public SinkWatch(ConsumerConfig config, Action<ConsumerBuilder<byte[], byte[]>>? configure, List<string> topics)
        {
            _topics = topics;
            var builder = new ConsumerBuilder<byte[], byte[]>(config);
            configure?.Invoke(builder);
            _consumer = builder.Build();
            _consumer.Assign(topics.Select(t => new TopicPartitionOffset(t, 0, Offset.End)));
            _thread = new Thread(Read) { IsBackground = true, Name = "sink watch" };
            _thread.Start();
        }

        /// <summary>From now on, remember each topic's first record produced at or after
        /// <paramref name="from"/> (Unix milliseconds).</summary>
        public void Arm(long from)
        {
            lock (_lock)
            {
                _first.Clear();
                _gap.Clear();
                _armedAt = from;
            }
        }

        private void Read()
        {
            while (!_stop)
            {
                ConsumeResult<byte[], byte[]>? result;
                try
                {
                    result = _consumer.Consume(TimeSpan.FromMilliseconds(50));
                }
                catch (ConsumeException)
                {
                    continue;
                }
                if (result?.Message is null)
                    continue;
                lock (_lock)
                {
                    // Produced after the moment, not merely delivered after it.
                    var produced = result.Message.Timestamp.UnixTimestampMs;
                    if (produced >= _armedAt)
                        _first.TryAdd(result.Topic, Now);
                    if (_last.TryGetValue(result.Topic, out var before) && produced - before > _gap.GetValueOrDefault(result.Topic))
                        _gap[result.Topic] = produced - before;
                    _last[result.Topic] = Math.Max(produced, _last.GetValueOrDefault(result.Topic));
                }
            }
        }

        /// <summary>Topic → milliseconds from <paramref name="from"/> to its first record.</summary>
        public async Task<Dictionary<string, long>> FirstWritesAsync(long from, TimeSpan limit)
        {
            var clock = Stopwatch.StartNew();
            while (true)
            {
                lock (_lock)
                {
                    if (_first.Count == _topics.Count)
                        return _first.ToDictionary(f => f.Key, f => f.Value - from);
                    if (clock.Elapsed > limit)
                        throw new TimeoutException($"No record arrived in {string.Join(", ", _topics.Except(_first.Keys))} within {limit}.");
                }
                await Task.Delay(20);
            }
        }

        /// <summary>Topic → the longest time between two consecutive records' production
        /// since the watch was armed: how long that sink went without a write.</summary>
        public Dictionary<string, long> Gaps()
        {
            lock (_lock)
                return _topics.ToDictionary(t => t, t => _gap.GetValueOrDefault(t));
        }

        public void Dispose()
        {
            _stop = true;
            _thread.Join();
            _consumer.Close();
            _consumer.Dispose();
        }
    }
}

/// <summary>A node process: its events, its stdin, its stderr in a file.</summary>
internal sealed class NodeProcess : IDisposable
{
    private static readonly TimeSpan Patience = TimeSpan.FromMinutes(30);
    private readonly Process _process;
    private readonly List<JsonElement> _events = [];
    private readonly StreamWriter _stderr;
    private readonly string _stderrPath;

    // What the node's supervisor said on stderr: each process it started and each exit
    // it saw, with the time the line arrived.
    private readonly List<(int Pid, long At)> _starts = [];
    private readonly List<(int Code, long At)> _exits = [];

    public NodeProcess(string id, IEnumerable<string> args, string stderrPath)
    {
        Id = id;
        _stderrPath = stderrPath;
        _stderr = new StreamWriter(stderrPath) { AutoFlush = true };
        var info = new ProcessStartInfo("dotnet")
        {
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            WorkingDirectory = AppContext.BaseDirectory,
        };
        // Twenty one-worker circuits do not need a GC heap per core; the value is hexadecimal.
        info.Environment["DOTNET_GCHeapCount"] = Environment.GetEnvironmentVariable("SCALE_GC_HEAPS") ?? "8";
        // SCALE_GC_SERVER=0: workstation GC, for a size whose server-GC working set this machine cannot hold twice.
        if (Environment.GetEnvironmentVariable("SCALE_GC_SERVER") is { Length: > 0 } server)
            info.Environment["DOTNET_gcServer"] = server;
        info.ArgumentList.Add(Path.Combine(AppContext.BaseDirectory, "Incremental.Service.Scale.dll"));
        foreach (var argument in args)
            info.ArgumentList.Add(argument);
        _process = Process.Start(info) ?? throw new InvalidOperationException("The node process did not start.");
        _process.OutputDataReceived += (_, e) =>
        {
            if (e.Data is not { Length: > 0 } line || line[0] != '{')
                return;
            lock (_events)
                _events.Add(JsonDocument.Parse(line).RootElement);
        };
        _process.ErrorDataReceived += (_, e) =>
        {
            lock (_stderr)
            {
                _stderr.WriteLine(e.Data);
                if (e.Data is not { } line)
                    return;
                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                if (System.Text.RegularExpressions.Regex.Match(line, @"node supervisor: started process (\d+)") is { Success: true } started)
                    _starts.Add((int.Parse(started.Groups[1].Value), now));
                else if (System.Text.RegularExpressions.Regex.Match(line, @"node supervisor: process \d+ ended with exit code (\d+)") is { Success: true } ended)
                    _exits.Add((int.Parse(ended.Groups[1].Value), now));
            }
        };
        _process.BeginOutputReadLine();
        _process.BeginErrorReadLine();
    }

    public string Id { get; }

    private List<JsonElement> Events(string kind)
    {
        lock (_events)
            return _events.Where(e => e.GetProperty("e").GetString() == kind).ToList();
    }

    public async Task<JsonElement> WaitAsync(string kind, Func<JsonElement, bool>? predicate = null)
    {
        var clock = Stopwatch.StartNew();
        var errorsBefore = Events("error").Count;
        while (true)
        {
            lock (_events)
            {
                foreach (var e in _events)
                {
                    if (e.GetProperty("e").GetString() == kind && (predicate is null || predicate(e)))
                        return e;
                }
            }
            if (Events("error").Count > errorsBefore)
                throw new InvalidOperationException($"Node '{Id}' reported an error while '{kind}' was awaited: {Events("error")[^1].GetRawText()}");
            if (Events("crashed").Count > 0)
                throw new InvalidOperationException($"Node '{Id}' crashed: {Events("crashed")[^1].GetRawText()}");
            if (clock.Elapsed > Patience)
                throw new TimeoutException($"Node '{Id}' did not report '{kind}' within {Patience}.");
            if (_process.HasExited)
            {
                await Task.Delay(300);
                lock (_events)
                {
                    if (!_events.Any(e => e.GetProperty("e").GetString() == kind && (predicate is null || predicate(e))))
                        throw new InvalidOperationException($"Node '{Id}' exited with code {_process.ExitCode} before reporting '{kind}'.");
                }
                continue;
            }
            await Task.Delay(10);
        }
    }

    public Task<JsonElement> WaitForRoleAsync(string role) => WaitAsync("role", e => e.GetProperty("role").GetString() == role);

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

    /// <summary>The node's status. Asked again every few seconds until answered: a node
    /// that ended with the question unread is answered for by its next incarnation.</summary>
    public async Task<JsonElement> StatusAsync()
    {
        var before = Events("status").Count;
        while (true)
        {
            Send("status");
            for (var waited = 0; waited < 5000; waited += 10)
            {
                if (Events("status") is { } answers && answers.Count > before)
                    return answers[^1];
                if (Events("crashed").Count > 0)
                    throw new InvalidOperationException($"Node '{Id}' crashed: {Events("crashed")[^1].GetRawText()}");
                if (_process.HasExited)
                    throw new InvalidOperationException($"Node '{Id}' and its supervisor exited with code {_process.ExitCode}.");
                await Task.Delay(10);
            }
        }
    }

    /// <summary>When the supervisor started the node's process, the first time after
    /// <paramref name="after"/> (Unix ms).</summary>
    public async Task<long> StartedAfterAsync(long after)
    {
        var clock = Stopwatch.StartNew();
        while (true)
        {
            lock (_stderr)
            {
                foreach (var (_, at) in _starts)
                {
                    if (at >= after)
                        return at;
                }
            }
            if (clock.Elapsed > TimeSpan.FromMinutes(2))
                throw new TimeoutException($"Node '{Id}' was not started again by its supervisor.");
            await Task.Delay(10);
        }
    }

    /// <summary>The exit code the supervisor saw the node's process end with, the first
    /// time after <paramref name="after"/> (Unix ms).</summary>
    public async Task<int> SupervisedExitAfterAsync(long after)
    {
        var clock = Stopwatch.StartNew();
        while (true)
        {
            lock (_stderr)
            {
                foreach (var (code, at) in _exits)
                {
                    if (at >= after)
                        return code;
                }
            }
            if (clock.Elapsed > TimeSpan.FromMinutes(2))
                throw new TimeoutException($"Node '{Id}' did not end.");
            await Task.Delay(10);
        }
    }

    public async Task<(long Heap, long Rss)> GcAsync()
    {
        var memory = await RunAsync("gc", "gc");
        return (memory.GetProperty("heapMb").GetInt64(), memory.GetProperty("rssMb").GetInt64());
    }

    public async Task<JsonElement> StatusWhenAsync(Func<JsonElement, bool> condition, string what, int pollMs = 200)
    {
        var clock = Stopwatch.StartNew();
        while (true)
        {
            var status = await StatusAsync();
            if (condition(status))
                return status;
            Rig.RequireMemory();
            if (clock.Elapsed > Patience)
                throw new TimeoutException($"Node '{Id}': timed out waiting for {what}. Last status: {status.GetRawText()}");
            await Task.Delay(pollMs);
        }
    }

    /// <summary><c>kill -9</c> of the node's process. Its supervisor, which is the
    /// process this object started, notices and starts it again.</summary>
    public void Kill()
    {
        int pid;
        lock (_stderr)
            pid = _starts.Count > 0 ? _starts[^1].Pid : -1;
        if (pid < 0)
        {
            KillWithSupervisor();
            return;
        }
        try
        {
            using var node = Process.GetProcessById(pid);
            node.Kill();
        }
        catch (ArgumentException)
        {
            // It ended a moment ago.
        }
    }

    /// <summary><c>kill -9</c> of the supervisor and the node: nothing comes back.</summary>
    public void KillWithSupervisor()
    {
        try
        {
            if (!_process.HasExited)
                _process.Kill(entireProcessTree: true);
        }
        catch (InvalidOperationException)
        {
        }
    }

    public async Task<int> ExitCodeAsync()
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        await _process.WaitForExitAsync(limit.Token);
        return _process.ExitCode;
    }

    public string Dump()
    {
        lock (_events)
        {
            var text = new StringBuilder($"--- node {Id} (stderr: {_stderrPath}) last events ---{Environment.NewLine}");
            foreach (var e in _events.Where(e => e.GetProperty("e").GetString() != "status").TakeLast(12))
                text.AppendLine(e.GetRawText());
            try
            {
                lock (_stderr)
                    _stderr.Flush();
                using var stream = new FileStream(_stderrPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                using var reader = new StreamReader(stream);
                foreach (var line in reader.ReadToEnd().Split('\n').TakeLast(25))
                    text.AppendLine(line);
            }
            catch (IOException)
            {
            }
            return text.ToString();
        }
    }

    public void Dispose()
    {
        try
        {
            if (!_process.HasExited)
                _process.Kill(entireProcessTree: true);
            _process.WaitForExit(5000);
        }
        catch (InvalidOperationException)
        {
        }
        _process.Dispose();
        lock (_stderr)
            _stderr.Dispose();
    }
}
