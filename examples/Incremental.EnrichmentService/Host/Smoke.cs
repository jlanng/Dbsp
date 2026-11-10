using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Enrichment.Contracts;
using Incremental.Service;

namespace Incremental.EnrichmentService;

/// <summary>
/// <c>--headless</c>: the example run unattended, as a test. It deploys the pipelines
/// through the admin API, publishes trades, changes reference data, replaces a pipeline
/// keeping its state and another resetting it, removes one, and — with two nodes — moves
/// leadership; after every act it compares what the sinks hold with a model. Any claim that
/// does not hold ends the run with exit code 1.
/// With <c>--kafka</c> the first node is a child process on the local connector stack and
/// the move of leadership is its death by <c>SIGKILL</c>.
/// </summary>
public sealed class Smoke(ExampleOptions options)
{
    private static readonly JsonSerializerOptions SpecJson = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() },
    };

    private readonly WorldShape _shape = new()
    {
        Instruments = 400, Counterparties = 120, TradeKeySpace = 1500, TradeInterval = TimeSpan.FromMilliseconds(250),
    };
    private readonly List<ExampleNode> _nodes = [];
    private readonly TimeSpan _patience = options.Kafka ? TimeSpan.FromSeconds(120) : TimeSpan.FromSeconds(45);
    private World _world = null!;
    private Model _model = null!;
    private Specs _specs = null!;
    private KafkaStack? _kafka;
    private Process? _child;
    private AdminClient _admin = null!;     // the leader's
    private int _claims;

    private sealed class Failed(string claim) : Exception(claim);

    public async Task<int> RunAsync()
    {
        var mode = options.Kafka ? "Kafka backend, two processes" : options.Nodes >= 2 ? "in memory, two nodes" : "in memory, one node";
        Console.WriteLine($"Enrichment service smoke test ({mode})");
        var dataDirectory = Path.Combine(Path.GetTempPath(), "incremental-enrichment-service", "smoke-" + Guid.NewGuid().ToString("N")[..10]);
        var watch = Stopwatch.StartNew();
        try
        {
            await StartAsync(dataDirectory);
            await DeployAsync();
            await TradesAsync();
            await ReferenceDataAsync();
            await VersionsAsync();
            await RemoveAsync();
            if (options.Kafka || options.Nodes >= 2)
                await TakeoverAsync();
            Console.WriteLine();
            Console.WriteLine($"{_claims} claims checked, all hold ({watch.Elapsed.TotalSeconds:N0} s).");
            return 0;
        }
        catch (Failed)
        {
            Console.WriteLine();
            Console.WriteLine($"A claim did NOT hold after {_claims - 1} that did.");
            return 1;
        }
        finally
        {
            await StopAsync(dataDirectory);
        }
    }

    // ── claims ─────────────────────────────────────────────────────────────────────────

    /// <summary>Waits, within the patience, for <paramref name="difference"/> to return
    /// null. Everything checked is eventually true or never: sinks converge on the model.</summary>
    private async Task Claim(string text, Func<Task<string?>> difference)
    {
        _claims++;
        var waited = Stopwatch.StartNew();
        string? last;
        while (true)
        {
            try
            {
                last = await difference();
            }
            catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or InvalidOperationException or KeyNotFoundException)
            {
                last = $"{ex.GetType().Name}: {ex.Message}";
            }
            if (last is null || waited.Elapsed > _patience)
                break;
            await Task.Delay(options.Kafka ? 250 : 20);
        }
        if (last is null)
        {
            Console.WriteLine($"  ok    {text}");
            return;
        }
        Console.WriteLine($"  WRONG {text}");
        Console.WriteLine($"        {last}");
        throw new Failed(text);
    }

    private Task Claim(string text, Func<string?> difference) => Claim(text, () => Task.FromResult(difference()));

    private static string? Unless(bool holds, string otherwise) => holds ? null : otherwise;

    /// <summary>What a sink holds: the view in memory, the folded topic on Kafka.</summary>
    private IReadOnlyDictionary<string, string> Sink(string name) =>
        _kafka is null
            ? _world.Views.Find(name)?.Rows() ?? []
            : _kafka.ReadUpserts(_kafka.SinkTopic(name));

    private Task SinkIs(string text, string name, Func<Dictionary<string, JsonObject>> expected) =>
        Claim(text, () => Task.Run(() => Model.Difference(expected(), Sink(name))));

    private Task EnrichedIs(string text, string id = "trade-enrichment") =>
        SinkIs(text, id, () => _model.Enrichments[id].Expected(_model));

    // ── act 0: a node, its tables ──────────────────────────────────────────────────────

    private async Task StartAsync(string dataDirectory)
    {
        Console.WriteLine();
        if (options.Kafka)
        {
            var prefix = "smoke" + Guid.NewGuid().ToString("N")[..10];
            var ports = FreePorts(4);
            var kafkaOptions = new ExampleOptions
            {
                Kafka = true, Prefix = prefix, Bootstrap = options.Bootstrap, Postgres = options.Postgres,
                PostgresHostForConnect = options.PostgresHostForConnect, Connect = options.Connect,
                SeedPipelines = false, Generate = false, Verbose = options.Verbose, Quiet = !options.Verbose,
            };
            _kafka = new KafkaStack(kafkaOptions);
            Console.WriteLine($"0. The local stack under the prefix '{prefix}': three Postgres tables, a Debezium connector, a trades topic, three sink topics.");
            await _kafka.EnsureAsync(_shape);
            _world = new World(_shape, dataDirectory, _kafka, freshData: true);
            _specs = new Specs(_kafka);

            // Node a is a process of its own, so that it can be killed.
            _child = StartChild(
                "--kafka", "--node", "a", "--prefix", prefix, "--port", ports[0].ToString(), "--probe-port", ports[1].ToString(),
                "--peer-probe-port", ports[3].ToString(), "--data", dataDirectory, "--no-generate", "--no-pipelines", "--no-prepare",
                "--bootstrap", options.Bootstrap, "--postgres", options.Postgres, "--connect", options.Connect,
                "--postgres-host-for-connect", options.PostgresHostForConnect);
            _admin = new AdminClient($"http://127.0.0.1:{ports[0]}");
            await Claim("node a, a process of its own, leads the new deployment and is ready", async () =>
                _child.HasExited ? throw new Failed($"node a exited with code {_child.ExitCode}") : await RoleAsync(_admin) is "Leader" ? null : "not the leader yet");

            // Node b is in this process: the standby, and later the survivor.
            _nodes.Add(await ExampleNode.StartAsync(_world, kafkaOptions, "b", ports[2], null, ports[3], ports[1]));
        }
        else
        {
            Console.WriteLine($"0. {(options.Nodes >= 2 ? "Two nodes" : "One node")} on the in-memory backend, the admin API on a loopback port.");
            _world = new World(_shape, dataDirectory, null, freshData: true);
            _specs = new Specs(null);
            var cluster = new InMemoryServiceCluster();
            var ports = FreePorts(2);
            var nodeOptions = new ExampleOptions { SeedPipelines = false, Generate = false, Verbose = options.Verbose, Quiet = !options.Verbose };
            _nodes.Add(await ExampleNode.StartAsync(_world, nodeOptions, "a", ports[0], cluster, exitOnLeadershipLoss: false));
            _admin = new AdminClient(_nodes[0].Url);
            await Claim("node a leads and is ready", async () => await RoleAsync(_admin) is "Leader" ? null : "not the leader yet");
            if (options.Nodes >= 2)
                _nodes.Add(await ExampleNode.StartAsync(_world, nodeOptions, "b", ports[1], cluster, exitOnLeadershipLoss: false));
        }

        _model = new Model(_shape);
        for (var i = 1; i <= _shape.Instruments; i++)
            _model.Instruments[i] = _shape.Instrument(i);
        for (var i = 1; i <= _shape.Desks; i++)
            _model.Desks[i] = _shape.Desk(i);
        for (var i = 1; i <= _shape.Traders; i++)
            _model.Traders[i] = _shape.Trader(i);
        foreach (var row in _world.Counterparties.Rows.Values)
            _model.Counterparties[row.Id] = row;

        var loaders = options.Kafka ? "three from Postgres through Debezium, one from a file" : "two custom sources and the file loader";
        await Claim(
            $"the tables are loaded ({loaders}): {_shape.Instruments} instruments, {_shape.Counterparties} counterparties, {_shape.Traders} traders, {_shape.Desks} desks",
            async () =>
            {
                foreach (var (table, rows) in new[] { ("instruments", _shape.Instruments), ("counterparties", _shape.Counterparties), ("traders", _shape.Traders), ("desks", _shape.Desks) })
                {
                    var item = (await _admin.GetAsync($"tables/{table}"))["table"]!;
                    if (item["state"]!.GetValue<string>() != "Open" || item["rows"]!.GetValue<long>() != rows)
                        return $"{table} is {item["state"]} with {item["rows"]} rows";
                }
                return null;
            });
    }

    private static async Task<string?> RoleAsync(AdminClient admin)
    {
        var (status, body) = await admin.SendAsync(HttpMethod.Get, "/health/ready");
        return status == 200 ? body?["role"]?.GetValue<string>() : null;
    }

    // ── act 1: deploy ──────────────────────────────────────────────────────────────────

    private async Task DeployAsync()
    {
        Console.WriteLine();
        Console.WriteLine("1. Four pipelines are deployed through the admin API from the stored plugin, version 1.");

        var refused = await _admin.PutAsync("pipelines/trade-enrichment", _specs.TradeEnrichment("trade-enrichment", -1));
        await Claim("a spec whose settings fail validation (minNotional −1) is refused, and nothing is deployed",
            async () => Unless(refused.Status == 400 && (await _admin.SendAsync(HttpMethod.Get, "/admin/api/pipelines/trade-enrichment")).Status == 404,
                $"the deploy answered {refused.Status} {refused.Body?.ToJsonString()}"));

        _model.Enrichments["trade-enrichment"] = new EnrichmentModel(250_000m);
        _model.Enrichments["large-trades"] = new EnrichmentModel(5_000_000m);
        _model.Exposure = new ExposureModel(TimeSpan.FromSeconds(60));
        foreach (var spec in new[]
                 {
                     _specs.TraderDesks(), _specs.TradeEnrichment("trade-enrichment", 250_000m),
                     _specs.TradeEnrichment("large-trades", 5_000_000m), _specs.DeskExposure(),
                 })
        {
            var deployed = await _admin.PutAsync($"pipelines/{spec.Id}", spec);
            if (deployed.Status != 200)
                await Claim($"{spec.Id} deploys", () => $"the deploy answered {deployed.Status} {deployed.Body?.ToJsonString()}");
        }
        await Claim("all four are Running", () => RunningAsync("trader-desks", "trade-enrichment", "large-trades", "desk-exposure"));
        await Claim($"the derived table trader-desks holds traders ⋈ desks: {_shape.Traders} rows, written by a pipeline",
            async () => Model.Difference(_model.ExpectedTraderDesks(), await TableRowsAsync("trader-desks")));
    }

    private async Task<string?> RunningAsync(params string[] ids)
    {
        foreach (var id in ids)
        {
            var pipeline = (await _admin.GetAsync($"pipelines/{id}"))["pipeline"]!;
            if (pipeline["state"]!.GetValue<string>() != "Running")
                return $"{id} is {pipeline["state"]}{(pipeline["lastFault"] is { } fault ? ": " + fault["message"] : "")}";
        }
        return null;
    }

    private async Task<Dictionary<string, string>> TableRowsAsync(string table, string? key = null)
    {
        var page = await _admin.GetAsync($"tables/{table}/rows?" + (key is null ? "limit=200" : "key=" + Uri.EscapeDataString(key)));
        return page["rows"]!.AsArray().ToDictionary(r => r!["key"]!.ToJsonString(), r => r!["row"]!.ToJsonString());
    }

    // ── act 2: trades ──────────────────────────────────────────────────────────────────

    private async Task PublishAsync(long count)
    {
        if (_kafka is not null)
            await _kafka.ProduceTradesAsync(_shape, _model.Trades, count);
        else
            _world.Trades.Publish(count);
        _model.Arrive(count);
    }

    private async Task TradesAsync()
    {
        Console.WriteLine();
        Console.WriteLine($"2. 2,000 trades are published over {_shape.TradeKeySpace:N0} trade ids, so a quarter of them amend an earlier trade.");
        await PublishAsync(2000);
        await EnrichedIs($"trade-enrichment delivered exactly the {_model.Enrichments["trade-enrichment"].Expected(_model).Count:N0} live trades at or above its minimum notional, each joined to three tables");
        await EnrichedIs($"large-trades, the same definition with other settings and its own state, delivered its {_model.Enrichments["large-trades"].Expected(_model).Count:N0}", "large-trades");
        await SinkIs($"desk-exposure delivered {_model.Exposure!.Expected().Count} window totals — every execution counted once, in the minute of its execution time",
            "desk-exposure", _model.Exposure.Expected);
    }

    // ── act 3: reference data changes ──────────────────────────────────────────────────

    private async Task ReferenceDataAsync()
    {
        Console.WriteLine();
        Console.WriteLine("3. Reference data changes under the running pipelines.");

        // A trader moves desk: one row, in the source system.
        var first = _shape.TradeAt(0);
        var trader = _model.Traders[first.TraderId];
        var moved = trader with { DeskId = 1 + trader.DeskId % _shape.Desks };
        var exposureBefore = _model.Exposure!.Expected();
        if (_kafka is not null)
            await _kafka.SqlAsync($"update {_kafka.Table("traders")} set desk_id = {moved.DeskId} where id = {moved.Id}");
        else
            _world.Traders.Upsert(moved);
        _model.Change(moved);
        var desk = _model.Desks[moved.DeskId].Name;
        var theirs = _model.Enrichments["trade-enrichment"].Expected(_model).Values.Count(r => r["trader"]!.GetValue<string>() == moved.Name);
        await Claim($"{moved.Name} moves to {desk}: the change reaches the derived table through the trader-desks pipeline",
            async () => Model.Difference(
                new() { [moved.Id.ToString()] = _model.ExpectedTraderDesks()[moved.Id.ToString()] }, await TableRowsAsync("trader-desks", moved.Id.ToString())));
        await EnrichedIs($"all {theirs} of their delivered trades now say {desk} (a retroactive join)");
        await SinkIs("desk-exposure is unchanged: those executions were done on the old desk (a lookup on arrival)", "desk-exposure", () => exposureBefore);

        // An instrument's multiplier is corrected.
        var instrument = await ChangeInstrumentAsync(first.InstrumentId);
        await EnrichedIs($"{instrument.Symbol}'s multiplier becomes {instrument.Multiplier}: every trade in it is repriced, and the filter applied again");
        await EnrichedIs("large-trades took the same correction", "large-trades");
        if (_kafka is null)
        {
            // The table carries the position of the last change applied to it; that is
            // what a restarted loader, here or on another node, resumes from.
            await Claim("the instruments table has committed the feed's position with its rows", () =>
            {
                var committed = _nodes[0].Node.Tables.Get<int, Instrument>("instruments").CommittedOffsets.GetValueOrDefault(0, -1);
                return Unless(committed == _world.Instruments.Published - 1, $"the table's committed offset is {committed}; the feed is at {_world.Instruments.Published - 1}");
            });
        }

        // A counterparty is renamed, in the file the table is loaded from.
        var number = int.Parse(first.CounterpartyId.AsSpan(2), CultureInfo.InvariantCulture);
        if (number > _shape.Counterparties)
            number = 1;
        var old = _model.Counterparties[WorldShape.CounterpartyId(number)];
        var renamed = _shape.Counterparty(number, revision: 1);
        _world.Counterparties.Replace(renamed);
        _model.Change(renamed);
        await Claim($"{old.Id} is renamed in its file, and the file loader makes the table equal to the file",
            async () => Model.Difference(
                new() { [JsonSerializer.Serialize(renamed.Id)] = (JsonObject)JsonSerializer.SerializeToNode(renamed, JsonSerializerOptions.Web)! },
                await TableRowsAsync("counterparties", renamed.Id)));
        await PublishAsync(500);
        await EnrichedIs("500 more trades: those booked with it since carry the new name, and those delivered before keep the old one (a lookup on arrival)");
        await Claim("…and both names are in the sink", () => Task.Run(() =>
        {
            var names = Sink("trade-enrichment").Values.Select(json => JsonNode.Parse(json)!["counterparty"]!.GetValue<string>()).ToHashSet();
            return Unless(names.Contains(old.Name) && names.Contains(renamed.Name), "one of the two names is absent");
        }));
        await SinkIs("desk-exposure counted the new executions under the desks their traders sit on now", "desk-exposure", _model.Exposure.Expected);
    }

    private async Task<Instrument> ChangeInstrumentAsync(int id)
    {
        if (_kafka is not null)
        {
            var old = _model.Instruments[id];
            var corrected = old with { Multiplier = old.Multiplier >= 2.75m ? 1m : old.Multiplier + 0.25m };
            await _kafka.SqlAsync($"update {_kafka.Table("instruments")} set multiplier = {corrected.Multiplier.ToString(CultureInfo.InvariantCulture)} where id = {id}");
            _model.Change(corrected);
            return corrected;
        }
        // In memory the instrument feed decides what changes next; take its records until
        // one of them really changes a multiplier.
        while (true)
        {
            var next = _shape.InstrumentChangeAt(_world.Instruments.Published);
            var changes = _model.Instruments[next.Id].Multiplier != next.Multiplier;
            _world.Instruments.Publish();
            _model.Change(next);
            if (changes)
                return next;
        }
    }

    // ── act 4: a second version of the plugin ──────────────────────────────────────────

    private async Task VersionsAsync()
    {
        Console.WriteLine();
        Console.WriteLine("4. Version 2 of the plugin is uploaded as a zip and two pipelines are replaced.");
        var package = Path.Combine(AppContext.BaseDirectory, "plugin-packages", "enrichment.pipelines-v2.zip");
        var upload = new ByteArrayContent(await File.ReadAllBytesAsync(package));
        upload.Headers.ContentType = new MediaTypeHeaderValue("application/zip");
        var uploaded = await _admin.SendAsync(HttpMethod.Post, "/admin/api/plugins", upload);
        var v2 = _v2 = uploaded.Body?["plugin"]?["version"]?.GetValue<string>() ?? "";
        await Claim("the upload is stored as a second version beside the first, with the same three definitions",
            () => Unless(uploaded.Status == 201 && uploaded.Body!["plugin"]!["definitions"]!.AsArray().Count == 3, $"the upload answered {uploaded.Status} {uploaded.Body?.ToJsonString()}"));

        // trade-enrichment: version 2 adds a computed column and changes nothing stored.
        var enrichment = _specs.TradeEnrichment("trade-enrichment", 250_000m, v2);
        var undecided = await _admin.PutAsync("pipelines/trade-enrichment", enrichment);
        await Claim("replacing trade-enrichment without saying what happens to its state is refused (409 state-choice-required)",
            () => Unless(undecided.Status == 409 && undecided.Body?["error"]?["code"]?.GetValue<string>() == "state-choice-required",
                $"the replace answered {undecided.Status} {undecided.Body?.ToJsonString()}"));
        var before = _model.Enrichments["trade-enrichment"].Expected(_model).Count;
        var kept = await _admin.PutAsync("pipelines/trade-enrichment?state=keep", enrichment);
        _model.Enrichments["trade-enrichment"].V2 = true;
        await Claim("with state=keep it is replaced and runs version 2",
            async () => Unless(kept.Status == 200, $"the replace answered {kept.Status} {kept.Body?.ToJsonString()}") ?? await VersionAsync("trade-enrichment", v2));
        await PublishAsync(500);
        await EnrichedIs($"500 more trades: rows emitted since carry the new column; of the {before:N0} delivered before, those untouched since are as version 1 left them — nothing was recomputed");
        await Claim("…and it read only the new trades: its state and its input offsets were kept", async () =>
        {
            var read = (await _admin.GetAsync("pipelines/trade-enrichment"))["sources"]!.AsArray().Sum(s => s!["records"]!.GetValue<long>());
            return Unless(read is >= 500 and < 1000, $"the new generation read {read} records");
        });

        // desk-exposure: version 2 groups by region, so version 1's state is not its state.
        var stale = _model.Exposure!.Expected();
        var reset = await _admin.PutAsync("pipelines/desk-exposure?state=reset", _specs.DeskExposure(v2));
        _model.Exposure.ByRegion = true;
        _model.Exposure.Reset(_model);
        await Claim("desk-exposure is replaced with state=reset and runs version 2",
            async () => Unless(reset.Status == 200, $"the replace answered {reset.Status} {reset.Body?.ToJsonString()}") ?? await VersionAsync("desk-exposure", v2));
        if (_kafka is null)
            await SinkIs("it read every execution again and delivered totals per region; the view sink, which can be reset, holds nothing else",
                "desk-exposure", _model.Exposure.Expected);
        else
            await SinkIs($"it read every execution again and delivered totals per region; the topic still holds version 1's {stale.Count} per-desk rows, because a reset does not un-write a Kafka sink",
                "desk-exposure", () => _model.Exposure.Expected().Concat(stale).ToDictionary(r => r.Key, r => r.Value));
        _staleExposure = _kafka is null ? [] : stale;
    }

    private Dictionary<string, JsonObject> _staleExposure = [];
    private string _v2 = "";

    private Dictionary<string, JsonObject> ExpectedExposure() =>
        _model.Exposure!.Expected().Concat(_staleExposure).ToDictionary(r => r.Key, r => r.Value);

    private async Task<string?> VersionAsync(string id, string version)
    {
        var pipeline = (await _admin.GetAsync($"pipelines/{id}"))["pipeline"]!;
        return Unless(pipeline["state"]!.GetValue<string>() == "Running" && pipeline["version"]!.GetValue<string>() == version,
            $"{id} is {pipeline["state"]} on version {pipeline["version"]}");
    }

    // ── act 5: remove ──────────────────────────────────────────────────────────────────

    private async Task RemoveAsync()
    {
        Console.WriteLine();
        Console.WriteLine("5. large-trades is removed while the others run.");
        var unconfirmed = await _admin.SendAsync(HttpMethod.Delete, "/admin/api/pipelines/large-trades");
        await Claim("a removal that is not confirmed is refused (400 confirmation-required)",
            () => Unless(unconfirmed.Status == 400 && unconfirmed.Body?["error"]?["code"]?.GetValue<string>() == "confirmation-required",
                $"the removal answered {unconfirmed.Status} {unconfirmed.Body?.ToJsonString()}"));
        var removed = await _admin.SendAsync(HttpMethod.Delete, "/admin/api/pipelines/large-trades?confirm=large-trades");
        _model.Enrichments.Remove("large-trades");
        await Claim("confirmed, it is gone from the catalog with its state",
            async () => Unless(removed.Status == 200 && (await _admin.SendAsync(HttpMethod.Get, "/admin/api/pipelines/large-trades")).Status == 404,
                $"the removal answered {removed.Status} {removed.Body?.ToJsonString()}"));
        await PublishAsync(500);
        await EnrichedIs("500 more trades: trade-enrichment never stopped");
        await SinkIs("neither did desk-exposure", "desk-exposure", ExpectedExposure);
        await Claim("the audit log has every change made here, newest first", async () =>
        {
            var actions = (await _admin.GetAsync("audit?limit=50"))["items"]!.AsArray().Select(a => a!["action"]!.GetValue<string>()).ToList();
            return Unless(actions.Contains("pipeline.remove") && actions.Contains("plugin.import") && actions.Count(a => a.StartsWith("pipeline.", StringComparison.Ordinal)) >= 7,
                "the audit log holds " + string.Join(", ", actions));
        });
    }

    // ── act 6: leadership moves ────────────────────────────────────────────────────────

    private async Task TakeoverAsync()
    {
        Console.WriteLine();
        var standby = new AdminClient(_nodes[^1].Url);
        Console.WriteLine(_kafka is null
            ? "6. Node b, the standby in the same process, takes over."
            : "6. Node a is killed (SIGKILL); node b, the standby in this process, takes over by itself.");
        await Claim("b is a warm standby: it has the catalog, the tables and version 2 of the plugin — which it was never given, only the catalog names it", async () =>
        {
            var status = (await standby.GetAsync("node"))["status"]!;
            return Unless(status["leadership"]!["role"]!.GetValue<string>() == "Standby" && status["standby"]?["takeoverWouldBeWarm"]?.GetValue<bool>() == true,
                $"b is {status["leadership"]!["role"]}, not warm because {status["standby"]?["notWarmBecause"]?.ToJsonString()}");
        });
        var refused = await standby.PutAsync("pipelines/trade-enrichment?state=keep", _specs.TradeEnrichment("trade-enrichment", 250_000m));
        await Claim("a change sent to the standby is refused, with the leader's address",
            () => Unless(refused.Status is 409 or 421 or 503 && refused.Body?["error"]?["leader"] is not null, $"the standby answered {refused.Status} {refused.Body?.ToJsonString()}"));

        var moved = Stopwatch.StartNew();
        if (_kafka is null)
        {
            var took = await standby.SendAsync(HttpMethod.Post, "/admin/api/node/takeover?confirm=b");
            if (took.Status != 200)
                await Claim("the takeover is accepted", () => $"it answered {took.Status} {took.Body?.ToJsonString()}");
        }
        else
        {
            _child!.Kill(entireProcessTree: true);
            await _child.WaitForExitAsync();
        }
        _admin = standby;
        await Claim("b leads", async () => await RoleAsync(standby) is "Leader" ? null : "b is not the leader");
        Console.WriteLine($"        ({moved.Elapsed.TotalSeconds:N1} s from {(_kafka is null ? "the request" : "the kill")} to b ready as leader)");
        await Claim("its three pipelines are Running, on the versions the catalog names", async () =>
            await RunningAsync("trader-desks") ?? await VersionAsync("trade-enrichment", _v2) ?? await VersionAsync("desk-exposure", _v2));

        await PublishAsync(500);
        await EnrichedIs("500 more trades: trade-enrichment's sink equals the model — state, offsets and tables all carried over");
        await SinkIs("so does desk-exposure's", "desk-exposure", ExpectedExposure);

        // A trader moves after the takeover: the new leader's table loaders carry on too.
        var trader = _model.Traders[_shape.TradeAt(1).TraderId];
        var movedTrader = trader with { DeskId = 1 + trader.DeskId % _shape.Desks };
        if (_kafka is not null)
            await _kafka.SqlAsync($"update {_kafka.Table("traders")} set desk_id = {movedTrader.DeskId} where id = {movedTrader.Id}");
        else
            _world.Traders.Upsert(movedTrader);
        _model.Change(movedTrader);
        await EnrichedIs($"{movedTrader.Name} moves desk: the new leader's loader, its trader-desks pipeline and the join carry the change to the sink");
        await Claim("b resumed both pipelines' inputs where a's last checkpoint stood, rather than reading them again", async () =>
        {
            foreach (var id in new[] { "trade-enrichment", "desk-exposure" })
            {
                var read = (await _admin.GetAsync($"pipelines/{id}"))["sources"]!.AsArray().Sum(s => s!["records"]!.GetValue<long>());
                if (read < 500 || read >= _model.Trades)
                    return $"{id} on b read {read} of {_model.Trades} records";
            }
            return null;
        });
    }

    // ── plumbing ───────────────────────────────────────────────────────────────────────

    private Process StartChild(params string[] arguments)
    {
        // The same program again: through the app host when there is one, else `dotnet <dll>`.
        var self = Environment.ProcessPath!;
        var start = new ProcessStartInfo { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        if (Path.GetFileNameWithoutExtension(self).Equals("dotnet", StringComparison.OrdinalIgnoreCase))
        {
            start.FileName = self;
            start.ArgumentList.Add(Path.Combine(AppContext.BaseDirectory, "Incremental.EnrichmentService.dll"));
        }
        else
            start.FileName = self;
        foreach (var argument in arguments)
            start.ArgumentList.Add(argument);
        var child = Process.Start(start)!;
        void Echo(string? line)
        {
            if (line is not null && options.Verbose)
                Console.WriteLine("        a| " + line);
        }
        child.OutputDataReceived += (_, e) => Echo(e.Data);
        child.ErrorDataReceived += (_, e) => Echo(e.Data);
        child.BeginOutputReadLine();
        child.BeginErrorReadLine();
        return child;
    }

    private static int[] FreePorts(int count)
    {
        var listeners = Enumerable.Range(0, count).Select(_ => new TcpListener(IPAddress.Loopback, 0)).ToArray();
        foreach (var listener in listeners)
            listener.Start();
        var ports = listeners.Select(l => ((IPEndPoint)l.LocalEndpoint).Port).ToArray();
        foreach (var listener in listeners)
            listener.Stop();
        return ports;
    }

    private async Task StopAsync(string dataDirectory)
    {
        foreach (var node in Enumerable.Reverse(_nodes))
        {
            try
            {
                await node.DisposeAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  (stopping node {node.Node.NodeId}: {ex.Message})");
            }
        }
        if (_child is { HasExited: false })
        {
            _child.Kill(entireProcessTree: true);
            await _child.WaitForExitAsync();
        }
        if (_kafka is not null)
        {
            await _kafka.CleanUpAsync();
            await _kafka.DisposeAsync();
        }
        try
        {
            Directory.Delete(dataDirectory, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    /// <summary>The admin API, as any client uses it: JSON in and out, the header every
    /// mutation must carry, and errors of one shape.</summary>
    private sealed class AdminClient(string url)
    {
        private readonly HttpClient _http = new() { BaseAddress = new Uri(url), Timeout = TimeSpan.FromSeconds(60) };

        public async Task<(int Status, JsonNode? Body)> SendAsync(HttpMethod method, string path, HttpContent? content = null)
        {
            using var request = new HttpRequestMessage(method, path) { Content = content };
            if (method != HttpMethod.Get)
                request.Headers.Add("X-Incremental-Admin", "1");
            using var response = await _http.SendAsync(request);
            var text = await response.Content.ReadAsStringAsync();
            JsonNode? body = null;
            try
            {
                body = text.Length == 0 ? null : JsonNode.Parse(text);
            }
            catch (JsonException)
            {
            }
            return ((int)response.StatusCode, body);
        }

        public async Task<JsonNode> GetAsync(string path)
        {
            var (status, body) = await SendAsync(HttpMethod.Get, "/admin/api/" + path);
            return status == 200 && body is not null ? body : throw new InvalidOperationException($"GET {path} answered {status} {body?.ToJsonString()}");
        }

        public Task<(int Status, JsonNode? Body)> PutAsync(string path, PipelineSpec spec) =>
            SendAsync(HttpMethod.Put, "/admin/api/" + path, new StringContent(JsonSerializer.Serialize(spec, SpecJson), Encoding.UTF8, "application/json"));
    }
}
