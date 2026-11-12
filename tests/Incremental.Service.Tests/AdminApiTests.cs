using System.IO.Compression;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Security.Claims;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.RegularExpressions;
using Incremental.Service.Admin;
using Incremental.Service.AdminHost;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Incremental.Service.Tests;

/// <summary>A response of the admin API: its status and its JSON, if it had any.</summary>
internal sealed record Answer(HttpStatusCode Status, JsonElement Body, HttpResponseMessage Response)
{
    public int Code => (int)Status;
    public string? ErrorCode => Body.ValueKind == JsonValueKind.Object && Body.TryGetProperty("error", out var e) ? e.GetProperty("code").GetString() : null;
    public JsonElement this[string name] => Body.GetProperty(name);

    /// <summary>The one shape every refusal has.</summary>
    public void IsError(int status, string code)
    {
        Assert.Equal(status, Code);
        Assert.Equal(JsonValueKind.Object, Body.ValueKind);
        var error = Body.GetProperty("error");
        Assert.Equal(code, error.GetProperty("code").GetString());
        Assert.False(string.IsNullOrWhiteSpace(error.GetProperty("message").GetString()));
        Assert.Single(Body.EnumerateObject());
    }
}

/// <summary>An admin host on a free loopback port — real Kestrel, because the loopback
/// rule reads the connection's remote address — and a client for its API.</summary>
internal sealed class AdminFixture : IAsyncDisposable
{
    public required DemoHost Host { get; init; }
    public required HttpClient Http { get; init; }

    public DemoNode Node => Host.Nodes[0];
    public string Api => Node.Url + "/admin/api/";

    public static async Task<AdminFixture> StartAsync(Action<DemoOptions>? configure = null)
    {
        await TestService.WarmedUpAsync();
        var options = new DemoOptions { Port = 0, Rate = 0, Instruments = 300, ImportPlugins = false, LogLevel = LogLevel.None };
        configure?.Invoke(options);
        var host = await DemoHost.StartAsync(options);
        return new AdminFixture { Host = host, Http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) } };
    }

    public async Task<Answer> SendAsync(
        HttpMethod method, string path, object? body = null, Action<HttpRequestMessage>? configure = null, bool header = true, string? baseUrl = null)
    {
        var request = new HttpRequestMessage(method, (baseUrl ?? Api) + path);
        if (method != HttpMethod.Get && header)
            request.Headers.Add("X-Incremental-Admin", "1");
        if (body is HttpContent content)
            request.Content = content;
        else if (body is not null)
            request.Content = new StringContent(body as string ?? JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");
        configure?.Invoke(request);
        var response = await Http.SendAsync(request);
        var text = await response.Content.ReadAsStringAsync();
        JsonElement json = default;
        if (text.Length > 0 && response.Content.Headers.ContentType?.MediaType == "application/json")
            json = JsonDocument.Parse(text).RootElement.Clone();
        return new Answer(response.StatusCode, json, response);
    }

    public Task<Answer> Get(string path, Action<HttpRequestMessage>? configure = null) => SendAsync(HttpMethod.Get, path, null, configure);
    public Task<Answer> Post(string path, object? body = null) => SendAsync(HttpMethod.Post, path, body);

    public async Task PipelineIs(string id, string state) =>
        await TestService.Eventually(() => Node.Node.Pipelines.Get(id).State.ToString() == state, $"pipeline {id} {state} (it is {Node.Node.Pipelines.Get(id).State})");

    public async ValueTask DisposeAsync()
    {
        Http.Dispose();
        await Host.DisposeAsync();
    }
}

/// <summary>Tests that only read share one host; a test that changes something starts its own.</summary>
public sealed class SharedAdminHost : IAsyncLifetime
{
    internal AdminFixture Fixture { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        Fixture = await AdminFixture.StartAsync(o => o.Admin = a => a.MaxRows = 5);
        // A dead letter and a failed pipeline, as the UI's host has them.
        var random = new Random(3);
        Fixture.Host.Memory.KeyedLog<long, Order>(DemoHost.LenientFeed).Upsert(DemoHost.Order(1, random, 300, negative: true));
        Fixture.Host.Memory.KeyedLog<long, Order>(DemoHost.LenientFeed).Upsert(DemoHost.Order(2, random, 300, negative: true));
        for (var i = 0; i < 40; i++)
            Fixture.Host.Memory.KeyedLog<long, Order>(DemoHost.Orders).Upsert(DemoHost.Order(i, random, 300));
        Fixture.Host.Memory.KeyedLog<long, Order>(DemoHost.CheckedFeed).Upsert(DemoHost.Order(1, random, 300, negative: true));
        await TestService.Eventually(() => Fixture.Node.Node.Pipelines.Get("lenient-orders").DeadLetters == 2, "two dead letters");
        await TestService.Eventually(() => Fixture.Node.Node.Pipelines.Get("checked-orders").State == PipelineState.Failed, "the checked pipeline failed");
        await TestService.Eventually(() => Fixture.Node.Node.Tables.Stats("positions").Rows > 0, "positions");
    }

    public async ValueTask DisposeAsync() => await Fixture.DisposeAsync();
}

public class AdminApiTests(SharedAdminHost shared) : IClassFixture<SharedAdminHost>
{
    private AdminFixture Admin => shared.Fixture;

    // ── bounds, paging, the error shape ────────────────────────────────────────────────

    [Fact]
    public async Task Every_list_is_bounded_by_MaxRows_and_paged()
    {
        // MaxRows is 5 on this host; eight pipelines are deployed.
        var first = await Admin.Get("pipelines?limit=1000");
        Assert.Equal(200, first.Code);
        Assert.Equal(5, first["items"].GetArrayLength());
        Assert.Equal(8, first["total"].GetInt32());
        Assert.Equal(5, first["limit"].GetInt32());
        Assert.Equal(5, first["next"].GetInt32());
        var second = await Admin.Get("pipelines?skip=5&limit=1000");
        Assert.Equal(3, second["items"].GetArrayLength());
        Assert.False(second.Body.TryGetProperty("next", out _));
        var ids = first["items"].EnumerateArray().Concat(second["items"].EnumerateArray()).Select(p => p.GetProperty("id").GetString()).ToList();
        Assert.Equal(8, ids.Distinct().Count());

        Assert.Equal(2, (await Admin.Get("pipelines?limit=2"))["items"].GetArrayLength());
        Assert.Equal(1, (await Admin.Get("pipelines?state=Stopped"))["total"].GetInt32());
        Assert.Equal(2, (await Admin.Get("pipelines?q=enrich"))["total"].GetInt32());

        foreach (var list in new[] { "tables", "plugins" })
        {
            var page = await Admin.Get(list + "?limit=1");
            Assert.Equal(1, page["items"].GetArrayLength());
            Assert.True(page["total"].GetInt32() >= 2);
            Assert.Equal(1, page["next"].GetInt32());
        }

        // Rings page from the newest end, and never return more than MaxRows either.
        var events = await Admin.Get("events/recent?limit=1000");
        Assert.Equal(5, events["items"].GetArrayLength());
        Assert.True(events["hasOlder"].GetBoolean());
        var older = await Admin.Get($"events/recent?limit=3&before={events["oldest"].GetInt64()}");
        Assert.All(older["items"].EnumerateArray(), e => Assert.True(e.GetProperty("sequence").GetInt64() < events["oldest"].GetInt64()));
        var audit = await Admin.Get("audit?limit=1000");
        Assert.Equal(5, audit["items"].GetArrayLength());
        Assert.True(audit["total"].GetInt32() >= 11);
        var letters = await Admin.Get("pipelines/lenient-orders/dead-letters?limit=1");
        Assert.Equal(1, letters["items"].GetArrayLength());
        Assert.True(letters["hasOlder"].GetBoolean());
        Assert.Equal(2, letters["items"][0].GetProperty("sequence").GetInt64());
        var olderLetters = await Admin.Get("pipelines/lenient-orders/dead-letters?limit=1&before=2");
        Assert.Equal(1, olderLetters["items"][0].GetProperty("sequence").GetInt64());
        Assert.False(olderLetters["hasOlder"].GetBoolean());
    }

    [Fact]
    public async Task Row_reads_are_bounded_and_say_what_they_scanned()
    {
        var page = await Admin.Get("tables/instruments/rows?limit=1000");
        Assert.Equal(5, page["rows"].GetArrayLength());
        Assert.Equal(300, page["tableRows"].GetInt64());
        Assert.True(page["more"].GetBoolean());
        Assert.False(page["truncated"].GetBoolean());
        Assert.True(page["version"].GetInt64() >= 1);
        var next = await Admin.Get("tables/instruments/rows?limit=5&skip=5");
        Assert.Empty(page["rows"].EnumerateArray().Select(r => r.GetProperty("key").GetInt32())
            .Intersect(next["rows"].EnumerateArray().Select(r => r.GetProperty("key").GetInt32())));

        // A page says where the next one starts; following that reaches the last row.
        Assert.Equal(5, page["next"].GetInt64());
        var seen = new HashSet<int>();
        for (long? from = 0; from is not null;)
        {
            var read = await Admin.Get($"tables/instruments/rows?limit=5&from={from}");
            foreach (var row in read["rows"].EnumerateArray())
                Assert.True(seen.Add(row.GetProperty("key").GetInt32()));
            from = read.Body.TryGetProperty("next", out var cursor) && cursor.ValueKind == JsonValueKind.Number ? cursor.GetInt64() : null;
            Assert.Equal(from is not null, read["more"].GetBoolean());
        }
        Assert.Equal(300, seen.Count);

        var byKey = await Admin.Get("tables/instruments/rows?key=42");
        Assert.Equal("SYM00042", byKey["rows"][0].GetProperty("row").GetProperty("symbol").GetString());
        Assert.Equal(0, (await Admin.Get("tables/instruments/rows?key=9999"))["rows"].GetArrayLength());
        var byIndex = await Admin.Get("tables/instruments/rows?index=isin&key=GB0000000042");
        Assert.Equal(42, byIndex["rows"][0].GetProperty("key").GetInt32());
        var byCountry = await Admin.Get("tables/counterparties/rows?index=country&key=GB&limit=3");
        Assert.Equal(3, byCountry["rows"].GetArrayLength());
        Assert.True(byCountry["more"].GetBoolean());

        // The rows under an index key are paged with skip, to the last of them.
        var under = byCountry["scanned"].GetInt32();
        Assert.True(under > 3, $"{under} counterparties in GB");
        var second = await Admin.Get($"tables/counterparties/rows?index=country&key=GB&limit=3&skip={byCountry["next"].GetInt64()}");
        Assert.Empty(byCountry["rows"].EnumerateArray().Select(r => r.GetProperty("key").GetRawText())
            .Intersect(second["rows"].EnumerateArray().Select(r => r.GetProperty("key").GetRawText())));
        var tail = await Admin.Get($"tables/counterparties/rows?index=country&key=GB&limit=5&skip={under - 1}");
        Assert.Equal(1, tail["rows"].GetArrayLength());
        Assert.False(tail["more"].GetBoolean());
        Assert.All(byCountry["rows"].EnumerateArray(), r => Assert.Equal("GB", r.GetProperty("row").GetProperty("country").GetString()));

        (await Admin.Get("tables/instruments/rows?index=nope&key=1")).IsError(400, "invalid");
        (await Admin.Get("tables/instruments/rows?key=not-a-number")).IsError(400, "invalid");
        (await Admin.Get("tables/nope/rows")).IsError(404, "not-found");
    }

    [Fact]
    public async Task A_scan_stops_at_MaxScannedRows_and_says_it_was_cut()
    {
        await using var admin = await AdminFixture.StartAsync(o => o.Admin = a => a.MaxScannedRows = 50);
        var page = await admin.Get("tables/instruments/rows?contains=SYM00299&limit=10");
        Assert.Equal(50, page["scanned"].GetInt64());
        Assert.True(page["truncated"].GetBoolean());
        Assert.True(page["rows"].GetArrayLength() <= 1);

        // It says where it stopped, and a read from there carries on: the row is found
        // however far into the table it is, six bounded reads later at most.
        var found = page["rows"].GetArrayLength();
        var reads = 1;
        while (page.Body.TryGetProperty("next", out var next) && next.ValueKind == JsonValueKind.Number)
        {
            page = await admin.Get($"tables/instruments/rows?contains=SYM00299&limit=10&from={next.GetInt64()}");
            Assert.True(page["scanned"].GetInt64() <= 50);
            found += page["rows"].GetArrayLength();
            reads++;
        }
        Assert.Equal(1, found);
        Assert.Equal(6, reads);

        // Browsing is not searching: rows passed over are not looked at, so the last
        // rows of a table larger than the bound are a page like any other.
        var last = await admin.Get("tables/instruments/rows?limit=10&skip=295");
        Assert.Equal(5, last["rows"].GetArrayLength());
        Assert.False(last["truncated"].GetBoolean());
        Assert.False(last["more"].GetBoolean());
    }

    [Fact]
    public async Task Every_refusal_has_one_shape()
    {
        (await Admin.Get("nope")).IsError(404, "not-found");
        (await Admin.Get("pipelines/nope")).IsError(404, "not-found");
        (await Admin.Get("pipelines/nope/graph")).IsError(404, "not-found");
        (await Admin.Get("pipelines/nope/metrics")).IsError(404, "not-found");
        (await Admin.Get("pipelines/nope/dead-letters")).IsError(404, "not-found");
        (await Admin.Get("tables/nope")).IsError(404, "not-found");
        (await Admin.Get("plugins/nope")).IsError(404, "not-found");
        (await Admin.Get("pipelines?limit=0")).IsError(400, "invalid-parameter");
        (await Admin.Get("pipelines?limit=abc")).IsError(400, "invalid-parameter");
        (await Admin.Get("pipelines?skip=-1")).IsError(400, "invalid-parameter");
        (await Admin.Get("audit?before=x")).IsError(400, "invalid-parameter");
        (await Admin.Post("pipelines/nope/stop")).IsError(404, "not-found");
        (await Admin.SendAsync(HttpMethod.Delete, "nope/nope")).IsError(404, "not-found");
        (await Admin.SendAsync(HttpMethod.Put, "pipelines/x", "{ not json")).IsError(400, "invalid-json");
        (await Admin.SendAsync(HttpMethod.Put, "pipelines/x", """{ "plugin": "p" }""")).IsError(400, "invalid");
        (await Admin.SendAsync(HttpMethod.Put, "pipelines/x", """{ "id": "y", "plugin": "p", "definition": "d" }""")).IsError(400, "invalid");
        (await Admin.SendAsync(HttpMethod.Put, "pipelines/x", new StringContent("x", Encoding.UTF8, "text/plain"))).IsError(415, "unsupported-media-type");
        (await Admin.SendAsync(HttpMethod.Put, "pipelines/enrich-orders?state=sideways", """{ "plugin": "p", "definition": "d" }""")).IsError(400, "invalid-parameter");
    }

    // ── what the read endpoints carry ──────────────────────────────────────────────────

    [Fact]
    public async Task The_node_tables_pipelines_and_plugins_are_described()
    {
        var info = await Admin.Get("info");
        Assert.Equal(1, info["apiVersion"].GetInt32());
        Assert.False(info["readOnly"].GetBoolean());
        Assert.True(info["exposeRowData"].GetBoolean());
        Assert.Equal(5, info["maxRows"].GetInt32());

        var node = await Admin.Get("node");
        Assert.Equal("Leader", node["status"].GetProperty("leadership").GetProperty("role").GetString());
        Assert.Equal("node-a", node["status"].GetProperty("nodeId").GetString());
        Assert.True(node["leader"].GetProperty("isSelf").GetBoolean());
        Assert.Equal(JsonValueKind.Number, node["status"].GetProperty("lastTakeover").GetProperty("catalog").ValueKind); // durations are milliseconds
        Assert.True(node["contracts"].TryGetProperty("Acme.Contracts", out _));
        Assert.Equal(8, node["status"].GetProperty("pipelines").GetInt32());

        var table = await Admin.Get("tables/counterparties");
        Assert.Equal(DemoHost.CounterpartyCount, table["table"].GetProperty("rows").GetInt64());
        Assert.Equal("file", table["table"].GetProperty("loader").GetProperty("kind").GetString());
        Assert.Equal(1, table["table"].GetProperty("loader").GetProperty("unreadable").GetInt64());
        Assert.Equal("country", table["table"].GetProperty("indexes")[0].GetProperty("name").GetString());
        Assert.Equal("String", table["table"].GetProperty("indexes")[0].GetProperty("keyType").GetString());
        Assert.Equal("counterparties.jsonl:19", table["unreadable"][0].GetProperty("position").GetString());
        Assert.Contains(table["pipelines"].EnumerateArray(), p => p.GetProperty("id").GetString() == "enrich-orders" && p.GetProperty("reads").GetBoolean());
        Assert.Equal("pipeline:positions", (await Admin.Get("tables/positions"))["table"].GetProperty("writer").GetString());

        var pipeline = await Admin.Get("pipelines/enrich-orders");
        Assert.Equal("Running", pipeline["pipeline"].GetProperty("state").GetString());
        Assert.Equal("enrich-orders", pipeline["spec"].GetProperty("id").GetString());
        Assert.Equal("00:10:00", pipeline["spec"].GetProperty("restart").GetProperty("within").GetString()); // a spec is returned in the form it is accepted in
        Assert.True(pipeline["hasGraph"].GetBoolean());
        Assert.Equal(["Starting", "Running"], pipeline["history"].EnumerateArray().Select(h => h.GetProperty("state").GetString()));
        var instruments = pipeline["tables"].EnumerateArray().Single(t => t.GetProperty("name").GetString() == "instruments");
        Assert.Equal(instruments.GetProperty("currentVersion").GetInt64(), instruments.GetProperty("committedVersion").GetInt64() + instruments.GetProperty("versionsBehind").GetInt64());
        await TestService.Eventually(() => Admin.Get("pipelines/enrich-orders").Result["sources"][0].GetProperty("records").GetInt64() == 40, "40 records read");
        await TestService.Eventually(() => Admin.Get("pipelines/enrich-orders").Result["sinks"][0].GetProperty("upserts").GetInt64() == 40, "40 rows delivered");

        var failed = await Admin.Get("pipelines/checked-orders");
        Assert.Equal("InvalidOperationException", failed["faults"][0].GetProperty("exceptionType").GetString());
        Assert.Contains("negative quantity", failed["faults"][0].GetProperty("message").GetString());
        Assert.Contains("CheckedOrders", failed["faults"][0].GetProperty("stack").GetString());
        // A fault names the spec's generation — the header's and the audit's number — and
        // which of the circuits built for it on this node it was.
        Assert.All(failed["faults"].EnumerateArray(), f => Assert.Equal(failed["pipeline"].GetProperty("generation").GetInt64(), f.GetProperty("generation").GetInt64()));
        var runs = failed["faults"].EnumerateArray().Select(f => f.GetProperty("run").GetInt64()).ToList();
        Assert.True(runs.Count > 1 && runs.Distinct().Count() == runs.Count && runs.All(r => r >= 1), string.Join(",", runs));

        var metrics = await Admin.Get("pipelines/enrich-orders/metrics");
        await TestService.Eventually(() => Admin.Get("pipelines/enrich-orders/metrics").Result["seconds"].GetArrayLength() > 0, "a metrics sample");
        Assert.True(metrics["seconds"].GetArrayLength() <= 60);
        Assert.True((await Admin.Get("node/metrics"))["seconds"].GetArrayLength() <= 60);

        var plugins = await Admin.Get("plugins/" + DemoHost.Plugin);
        var version = plugins["versions"][0].GetProperty("plugin");
        Assert.Equal(8, version.GetProperty("pipelines").GetArrayLength());
        var enrich = version.GetProperty("definitions").EnumerateArray().Single(d => d.GetProperty("name").GetString() == "enrich-orders");
        Assert.Contains(enrich.GetProperty("settings").EnumerateArray(), s => s.GetProperty("name").GetString() == "label" && s.GetProperty("stateNeutral").GetBoolean());

        var letters = await Admin.Get("pipelines/lenient-orders/dead-letters");
        Assert.Equal(2, letters["total"].GetInt64());
        Assert.Contains("negative quantity", letters["items"][0].GetProperty("message").GetString());
        Assert.Contains("\"quantity\":-1", letters["items"][0].GetProperty("record").GetString());
        Assert.Equal("n2.select", letters["items"][0].GetProperty("operator").GetString());
    }

    [Fact]
    public async Task The_node_says_what_its_process_holds_and_where_its_takeover_went()
    {
        var node = await Admin.Get("node");
        var memory = node["memory"];
        Assert.True(memory.GetProperty("workingSetBytes").GetInt64() > 10 << 20);
        Assert.True(memory.GetProperty("gcHeapBytes").GetInt64() > 1 << 20);
        Assert.True(memory.GetProperty("gcCommittedBytes").GetInt64() > 0);
        Assert.InRange(memory.GetProperty("gcPausePercent").GetDouble(), 0, 100);
        Assert.True(node["processShared"].GetBoolean()); // in memory, a peer is this process

        // The stages of StartLeading are measured on one clock and leave nothing over.
        var take = node["status"].GetProperty("lastTakeover");
        double Stage(string name) => take.GetProperty(name).GetDouble();
        var stages = new[] { "leaderRecord", "pluginDirectory", "tableSeeds", "pipelines", "pipelineSeeds", "finish" };
        Assert.All(stages, s => Assert.True(Stage(s) >= 0, s));
        Assert.InRange(stages.Sum(Stage), Stage("startLeading") - 0.05, Stage("startLeading") + 0.05);
        // This host deploys its pipelines as seeds: that is where its first lead went.
        Assert.True(Stage("pipelineSeeds") > Stage("finish"));

        // A table's size is an estimate from a sample, and says of how many rows.
        var size = (await Admin.Get("tables/instruments"))["size"];
        Assert.Equal(256, size.GetProperty("sampledRows").GetInt32());
        Assert.Equal(300, size.GetProperty("rows").GetInt64());
        Assert.InRange(size.GetProperty("meanRowBytes").GetDouble(), 30, 400);
        // The mean of the sample times every row of the table, not times the sample.
        Assert.InRange(size.GetProperty("rowsBytes").GetInt64(), 300 * (size.GetProperty("meanRowBytes").GetDouble() - 0.1), 300 * (size.GetProperty("meanRowBytes").GetDouble() + 0.1));
        Assert.True(size.GetProperty("historyChanges").GetInt64() >= 300); // the in-memory log holds its history here
        Assert.True(size.GetProperty("historyBytes").GetInt64() >= size.GetProperty("rowsBytes").GetInt64());

        // A contract is used by the tables declared from its definitions.
        var contract = (await Admin.Get("plugins?limit=5"))["items"].EnumerateArray().Single(p => p.GetProperty("kind").GetString() == "Contract");
        Assert.Contains("instruments", contract.GetProperty("tables").EnumerateArray().Select(t => t.GetString()));
        Assert.Contains("counterparties", contract.GetProperty("tables").EnumerateArray().Select(t => t.GetString()));
    }

    [Fact]
    public async Task Table_definitions_are_listed_and_a_declare_is_checked_whole_at_its_dry_run()
    {
        var definitions = new List<JsonElement>();
        for (int? skip = 0; skip is not null;)
        {
            var page = await Admin.Get($"table-definitions?skip={skip}&limit=5");
            definitions.AddRange(page["items"].EnumerateArray());
            skip = page.Body.TryGetProperty("next", out var next) ? next.GetInt32() : null;
        }
        var instruments = definitions.Single(d => d.GetProperty("defaultName").GetString() == "instruments");
        Assert.Equal("Int32", instruments.GetProperty("keyType").GetString());
        Assert.Equal("Instrument", instruments.GetProperty("rowType").GetString());
        Assert.Equal("Acme.Contracts", instruments.GetProperty("contract").GetString());
        Assert.Equal(["instruments"], instruments.GetProperty("declaredAs").EnumerateArray().Select(t => t.GetString()));
        var definition = instruments.GetProperty("definition").GetString()!;

        var tables = Admin.Node.Node.Tables.Tables.Count;
        var plan = await Admin.Post("tables?dryRun=true", new { name = "instruments-eu", definition, loader = new { kind = "file", connection = "refdata", config = new { path = "eu.jsonl" } }, onUnreadable = "Skip" });
        Assert.Equal(200, plan.Code);
        var steps = plan["plan"].GetProperty("steps").EnumerateArray().Select(x => x.GetString()!).ToList();
        Assert.Contains("Int32 → Instrument", steps[0]);
        Assert.Contains("file loader on connection 'refdata'", steps[^1]);
        Assert.Contains("skipped", steps[^1]);
        Assert.Contains("no loader", (await Admin.Post("tables?dryRun=true", new { name = "instruments-eu", definition }))["plan"].GetProperty("steps")[2].GetString());

        // Everything the declare itself would refuse, the dry run refuses.
        (await Admin.Post("tables?dryRun=true", new { name = "instruments", definition })).IsError(409, "conflict");
        (await Admin.Post("tables?dryRun=true", new { name = "Not Valid", definition })).IsError(400, "invalid");
        (await Admin.Post("tables?dryRun=true", new { name = "svc.mine", definition })).IsError(400, "invalid");
        (await Admin.Post("tables?dryRun=true", new { name = "x", definition = "Acme.Contracts.NoSuchTable" })).IsError(400, "invalid");
        (await Admin.Post("tables?dryRun=true", new { name = "x", definition, contract = "Other.Contracts" })).IsError(400, "invalid");
        (await Admin.Post("tables?dryRun=true", new { name = "x", definition, loader = new { kind = "carrier-pigeon" } })).IsError(400, "invalid");
        (await Admin.Post("tables?dryRun=true", new { name = "x", definition, loader = new { kind = "file", connection = "nope", config = new { path = "a" } } })).IsError(400, "invalid");
        (await Admin.Post("tables?dryRun=true", new { name = "x", definition, loader = new { kind = "file", connection = "refdata" } })).IsError(400, "invalid");
        (await Admin.Post("tables?dryRun=true", new { name = "x", definition, loader = new { kind = "kafka", connection = "nope", config = new { topic = "t" } } })).IsError(400, "invalid");
        // …and the same goes for the declare, as a refusal rather than a failure.
        (await Admin.Post("tables", new { name = "x", definition = "Acme.Contracts.NoSuchTable" })).IsError(400, "invalid");
        Assert.Equal(tables, Admin.Node.Node.Tables.Tables.Count);
    }

    [Fact]
    public async Task The_audit_names_who_the_host_names_then_who_the_caller_says_it_is_then_local()
    {
        await using var admin = await AdminFixture.StartAsync();
        var definition = Admin.Node.Node.Tables.Spec("instruments").Definition;
        async Task<string> DeclaredBy(AdminFixture fixture, string table, Action<HttpRequestMessage>? configure)
        {
            var answer = await fixture.SendAsync(HttpMethod.Post, "tables", new { name = table, definition }, configure);
            Assert.Equal(201, answer.Code);
            return fixture.Node.Node.Catalog.Audit().Single(a => a.Action == "table.declare" && a.Target == table).Caller;
        }

        Assert.Equal("local", await DeclaredBy(admin, "t-anon", null));
        Assert.Equal("ana (self-declared)", await DeclaredBy(admin, "t-ana", r => r.Headers.Add("X-Incremental-Operator", " ana ")));
        // A header carries Latin-1, so the UI sends other names percent-encoded.
        Assert.Equal("José (self-declared)", await DeclaredBy(admin, "t-jose", r => r.Headers.Add("X-Incremental-Operator", "Jos%C3%A9")));
        Assert.Equal(new string('x', 64) + " (self-declared)", await DeclaredBy(admin, "t-long", r => r.Headers.Add("X-Incremental-Operator", new string('x', 500))));
        var who = await admin.Get("whoami", r => r.Headers.Add("X-Incremental-Operator", "ana"));
        Assert.Equal("ana (self-declared)", who["caller"].GetString());
        Assert.False(who["verified"].GetBoolean());
        Assert.Equal("X-Incremental-Operator", who["operatorHeader"].GetString());
        Assert.Equal("local", (await admin.Get("whoami"))["caller"].GetString());

        // A host that knows its callers names them, and then nobody names themselves.
        await using var gated = await AdminFixture.StartAsync(o => o.Admin = a =>
        {
            a.CallerName = context => context.Request.Headers["X-Gateway-User"].FirstOrDefault();
            a.AcceptOperatorHeader = false;
        });
        Assert.Equal("op-7", await DeclaredBy(gated, "t-gated", r => { r.Headers.Add("X-Gateway-User", "op-7"); r.Headers.Add("X-Incremental-Operator", "mallory"); }));
        Assert.Equal("local", await DeclaredBy(gated, "t-unnamed", r => r.Headers.Add("X-Incremental-Operator", "mallory")));
        var named = await gated.Get("whoami", r => r.Headers.Add("X-Gateway-User", "op-7"));
        Assert.True(named["verified"].GetBoolean());
        Assert.Equal("op-7", named["caller"].GetString());
        Assert.False(named.Body.TryGetProperty("operatorHeader", out _));
    }

    // ── discovery: what a deploy needs to know ─────────────────────────────────────────

    private async Task<string> PluginVersionAsync() =>
        (await Admin.Get("plugins"))["items"].EnumerateArray()
            .First(p => p.GetProperty("name").GetString() == DemoHost.Plugin).GetProperty("version").GetString()!;

    [Fact]
    public async Task A_definition_is_described_with_its_settings_slots_and_tables()
    {
        var version = await PluginVersionAsync();
        var risk = await Admin.Get($"plugins/{DemoHost.Plugin}/{version}/definitions/desk-risk");
        Assert.Equal(200, risk.Code);
        Assert.Equal("desk-risk", risk["definition"].GetString());
        Assert.Equal(version, risk["version"].GetString());
        Assert.Empty(risk["notes"].EnumerateArray());

        // Settings: type, default, range.
        var settings = risk["settings"].EnumerateArray().ToDictionary(s => s.GetProperty("name").GetString()!);
        Assert.Equal("array", settings["desks"].GetProperty("jsonType").GetString());
        Assert.Equal(["rates", "credit", "fx", "equities"], settings["desks"].GetProperty("default").EnumerateArray().Select(d => d.GetString()));
        Assert.Equal("number", settings["top"].GetProperty("jsonType").GetString());
        Assert.Equal(5, settings["top"].GetProperty("default").GetInt32());
        Assert.Equal((1, 100), (settings["top"].GetProperty("minimum").GetInt32(), settings["top"].GetProperty("maximum").GetInt32()));
        Assert.False(settings["top"].GetProperty("required").GetBoolean());
        Assert.Contains("largest orders", settings["top"].GetProperty("description").GetString());

        // Slots: name, kind, record type and key type — the real names, not placeholders.
        var input = Assert.Single(risk["inputs"].EnumerateArray());
        Assert.Equal(("orders", "KeyedInput", "Order", "Int64"),
            (input.GetProperty("name").GetString(), input.GetProperty("kind").GetString(), input.GetProperty("type").GetString(), input.GetProperty("keyType").GetString()));
        Assert.Equal(["by-country", "by-symbol", "by-tier", "largest"], risk["sinks"].EnumerateArray().Select(s => s.GetProperty("name").GetString()).Order());
        var largest = risk["sinks"].EnumerateArray().First(s => s.GetProperty("name").GetString() == "largest");
        Assert.Equal(("UpsertSink", "EnrichedOrder", "Int64"),
            (largest.GetProperty("kind").GetString(), largest.GetProperty("type").GetString(), largest.GetProperty("keyType").GetString()));

        // Tables: the slot, the table it resolves to, how it is used, and its types.
        var tables = risk["tables"].EnumerateArray().ToDictionary(t => t.GetProperty("slot").GetString()!);
        Assert.Equal(["counterparties", "instruments"], tables.Keys.Order());
        Assert.Equal(("read", "Int32", "Instrument", "Acme.Contracts.InstrumentsTable"),
            (tables["instruments"].GetProperty("access").GetString(), tables["instruments"].GetProperty("keyType").GetString(),
                tables["instruments"].GetProperty("rowType").GetString(), tables["instruments"].GetProperty("definition").GetString()));

        // The slots are those of the settings given: here one desk instead of four changes none.
        var one = await Admin.Get($"plugins/{DemoHost.Plugin}/{version}/definitions/desk-risk?settings=" + Uri.EscapeDataString("""{ "desks": ["fx"], "top": 3 }"""));
        Assert.Equal(4, one["sinks"].GetArrayLength());
        (await Admin.Get($"plugins/{DemoHost.Plugin}/{version}/definitions/desk-risk?settings=" + Uri.EscapeDataString("""{ "top": 500 }"""))).IsError(400, "invalid");
        (await Admin.Get($"plugins/{DemoHost.Plugin}/{version}/definitions/desk-risk?settings=nope")).IsError(400, "invalid-parameter");

        // A derived table is a table slot that is written; a definition with no settings has none.
        var positions = await Admin.Get($"plugins/{DemoHost.Plugin}/{version}/definitions/positions");
        Assert.Empty(positions["settings"].EnumerateArray());
        Assert.Equal("write", Assert.Single(positions["tables"].EnumerateArray()).GetProperty("access").GetString());
        Assert.Empty(positions["sinks"].EnumerateArray());

        (await Admin.Get($"plugins/{DemoHost.Plugin}/{version}/definitions/no-such")).IsError(400, "invalid");
        (await Admin.Get($"plugins/{DemoHost.Plugin}/0000000000000000/definitions/desk-risk")).IsError(400, "invalid");
    }

    [Fact]
    public async Task A_spec_template_has_the_real_slot_names_and_passes_the_dry_run_as_it_is()
    {
        var version = await PluginVersionAsync();
        var template = await Admin.Get($"plugins/{DemoHost.Plugin}/{version}/definitions/desk-risk/template?id=my-risk");
        Assert.Equal(200, template.Code);
        Assert.True(template["valid"].GetBoolean(), template.Body.ToString());
        Assert.Empty(template["problems"].EnumerateArray());
        var spec = template["spec"];
        Assert.Equal(("my-risk", DemoHost.Plugin, version, "desk-risk"),
            (spec.GetProperty("id").GetString(), spec.GetProperty("plugin").GetString(), spec.GetProperty("version").GetString(), spec.GetProperty("definition").GetString()));
        Assert.Equal(5, spec.GetProperty("settings").GetProperty("top").GetInt32());
        Assert.Equal(4, spec.GetProperty("settings").GetProperty("desks").GetArrayLength());
        Assert.Equal("memory", spec.GetProperty("inputs").GetProperty("orders").GetProperty("connector").GetString());
        Assert.Equal("my-risk/orders", spec.GetProperty("inputs").GetProperty("orders").GetProperty("name").GetString());
        Assert.Equal(["by-country", "by-symbol", "by-tier", "largest"], spec.GetProperty("sinks").EnumerateObject().Select(s => s.Name).Order());
        Assert.DoesNotContain("<", spec.ToString());        // no placeholder anywhere
        Assert.Equal(["files", "kafka", "memory"], template["connectorsForSlot"].GetProperty("orders").EnumerateArray().Select(c => c.GetString()));

        // Sent back unchanged, the dry run of a deploy accepts it.
        var dry = await Admin.SendAsync(HttpMethod.Put, "pipelines/my-risk?dryRun=true", spec.GetRawText());
        Assert.Equal(200, dry.Code);
        Assert.Equal("pipeline.deploy", dry["plan"].GetProperty("action").GetString());
        Assert.Equal("orders", dry["shape"].GetProperty("inputs")[0].GetProperty("name").GetString());

        // The id defaults to the definition's name; a bad one is refused.
        Assert.Equal("positions", (await Admin.Get($"plugins/{DemoHost.Plugin}/{version}/definitions/positions/template"))["spec"].GetProperty("id").GetString());
        (await Admin.Get($"plugins/{DemoHost.Plugin}/{version}/definitions/positions/template?id=Not%20Valid")).IsError(400, "invalid");
    }

    [Fact]
    public async Task Connectors_connections_and_table_sources_are_listed_with_their_config_and_no_secrets()
    {
        var connectors = await Admin.Get("connectors");
        Assert.Equal(3, connectors["total"].GetInt32());
        var byName = connectors["items"].EnumerateArray().ToDictionary(c => c.GetProperty("name").GetString()!);
        Assert.Equal(["AppendInput", "KeyedInput", "UpsertSink", "AppendSink"], byName["memory"].GetProperty("kinds").EnumerateArray().Select(k => k.GetString()));
        Assert.True(byName["memory"].GetProperty("resettable").GetBoolean());
        Assert.True(byName["memory"].GetProperty("config").GetProperty("open").GetBoolean());
        var kafka = byName["kafka"].GetProperty("config").GetProperty("properties").EnumerateArray().ToDictionary(p => p.GetProperty("name").GetString()!);
        Assert.True(kafka["topic"].GetProperty("required").GetBoolean());
        Assert.True(kafka["connection"].GetProperty("required").GetBoolean());
        Assert.Empty(kafka["connection"].GetProperty("values").EnumerateArray());            // this host registered no Kafka connection
        Assert.Equal(["earliest", "latest"], kafka["startFrom"].GetProperty("values").EnumerateArray().Select(v => v.GetString()));
        Assert.Equal("earliest", kafka["startFrom"].GetProperty("default").GetString());
        Assert.False(byName["kafka"].GetProperty("resettable").GetBoolean());

        // Names and kinds, and nothing of where a connection points.
        var connections = await Admin.Get("connections");
        var store = Assert.Single(connections["items"].EnumerateArray());
        Assert.Equal(("refdata", "object-store"), (store.GetProperty("name").GetString(), store.GetProperty("kind").GetString()));
        Assert.Equal(2, store.EnumerateObject().Count());
        Assert.DoesNotContain("incremental-admin-host", connections.Body.ToString());        // the store's directory

        var sources = (await Admin.Get("table-sources"))["items"].EnumerateArray().ToDictionary(s => s.GetProperty("kind").GetString()!);
        Assert.Equal(["debezium", "file", "kafka"], sources.Keys.Order());
        Assert.Equal("object-store", sources["file"].GetProperty("connection").GetString());
        Assert.True(sources["file"].GetProperty("builtIn").GetBoolean());
        Assert.Contains(sources["file"].GetProperty("config").GetProperty("properties").EnumerateArray(),
            p => p.GetProperty("name").GetString() == "path" && p.GetProperty("required").GetBoolean());
        Assert.Equal("kafka", sources["debezium"].GetProperty("connection").GetString());

        // Lists like every other: paged and clamped.
        var page = await Admin.Get("connectors?limit=1&skip=1");
        Assert.Equal((1, 3, "kafka"), (page["items"].GetArrayLength(), page["total"].GetInt32(), page["items"][0].GetProperty("name").GetString()));
    }

    [Fact]
    public async Task The_graph_is_the_circuit_with_its_slots_bound_and_dead_letters_counted()
    {
        var graph = await Admin.Get("pipelines/desk-risk/graph");
        Assert.Equal("circuit", graph["source"].GetString());
        var nodes = graph["nodes"].EnumerateArray().ToList();
        Assert.True(nodes.Count >= 30, $"{nodes.Count} nodes");
        var ids = nodes.Select(n => n.GetProperty("id").GetString()).ToHashSet();
        Assert.Equal(nodes.Count, ids.Count);
        Assert.All(nodes, n => Assert.All(n.GetProperty("inputs").EnumerateArray(), i => Assert.Contains(i.GetString(), ids)));
        var input = nodes.Single(n => n.GetProperty("role").GetString() == "input");
        Assert.Equal("orders", input.GetProperty("label").GetString());
        Assert.Equal("memory: orders", input.GetProperty("detail").GetString());
        Assert.Equal(["counterparties", "instruments"], nodes.Where(n => n.GetProperty("role").GetString() == "table").Select(n => n.GetProperty("label").GetString()).Order());
        Assert.Equal(["by-country", "by-symbol", "by-tier", "largest"], nodes.Where(n => n.GetProperty("role").GetString() == "sink").Select(n => n.GetProperty("label").GetString()).Order());
        Assert.Contains(nodes, n => n.GetProperty("role").GetString() == "exchange");
        Assert.Contains(nodes, n => n.TryGetProperty("name", out var name) && name.GetString() == "priced-rates");
        // A lookup is drawn to the table it probes, though the table is no input of it;
        // a join consumes the table's changes and is reached through its inputs.
        var tableIds = nodes.Where(n => n.GetProperty("role").GetString() == "table").ToDictionary(n => n.GetProperty("id").GetString()!, n => n.GetProperty("label").GetString());
        static bool IsLookup(JsonElement n) => n.TryGetProperty("kind", out var kind) && kind.GetString()!.StartsWith("tableLookup");
        var lookups = nodes.Where(IsLookup).ToList();
        Assert.NotEmpty(lookups);
        Assert.All(lookups, n =>
        {
            var probed = Assert.Single(n.GetProperty("probes").EnumerateArray()).GetString()!;
            Assert.Contains(probed, tableIds.Keys);
            Assert.DoesNotContain(probed, n.GetProperty("inputs").EnumerateArray().Select(i => i.GetString()));
        });
        Assert.All(nodes.Where(n => !IsLookup(n)), n => Assert.False(n.TryGetProperty("probes", out _)));

        var keyed = await Admin.Get("pipelines/trader-activity/graph");
        Assert.Equal(["counts", "quantities"], keyed["nodes"].EnumerateArray().Where(n => n.GetProperty("role").GetString() == "sink").Select(n => n.GetProperty("label").GetString()).Order());
        var writer = await Admin.Get("pipelines/positions/graph");
        Assert.Contains(writer["nodes"].EnumerateArray(), n => n.GetProperty("role").GetString() == "table-out" && n.GetProperty("label").GetString() == "positions");
        var lenient = await Admin.Get("pipelines/lenient-orders/graph");
        Assert.Equal(2, lenient["deadLetters"].GetProperty("n2.select").GetInt32());
        Assert.Contains(lenient["nodes"].EnumerateArray(), n => n.TryGetProperty("stateName", out var s) && s.GetString() == "n2.select");
    }

    // ── the security switches ──────────────────────────────────────────────────────────

    [Fact]
    public async Task Without_a_policy_only_plain_loopback_requests_are_answered_and_health_is_outside_the_rule()
    {
        (await Admin.Get("node", r => r.Headers.Add("X-Forwarded-For", "203.0.113.9"))).IsError(403, "forwarded");
        (await Admin.Get("node", r => r.Headers.Add("Forwarded", "for=203.0.113.9"))).IsError(403, "forwarded");
        (await Admin.Get("node", r => r.Headers.Add("X-Forwarded-Host", "admin.example"))).IsError(403, "forwarded");
        (await Admin.Get("node", r => r.Headers.Add("X-Real-IP", "203.0.113.9"))).IsError(403, "forwarded");
        (await Admin.Get("node", r => r.Headers.Add("Via", "1.1 proxy.example"))).IsError(403, "forwarded");
        (await Admin.Get("node", r => r.Headers.Add("X-Forwarded-Proto", "https"))).IsError(403, "forwarded");

        // A proxy that adds no header at all can still give itself away: nginx speaks
        // HTTP/1.0 to its upstream unless it is told otherwise, and nothing else does.
        (await Admin.Get("node", r =>
        {
            r.Version = HttpVersion.Version10;
            r.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        })).IsError(403, "forwarded");
        Assert.Equal(200, (await Admin.Get("node", r =>
        {
            r.Version = HttpVersion.Version11;
            r.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        })).Code);
        (await Admin.Get("node", r => r.Headers.Host = "admin.example")).IsError(403, "host-not-allowed");
        Assert.Equal(200, (await Admin.Get("node", r => r.Headers.Host = "localhost:1")).Code);
        // The UI's own files are under the same rule.
        Assert.Equal(HttpStatusCode.Forbidden, (await Admin.Get("", r => { r.RequestUri = new Uri(Admin.Node.Admin); r.Headers.Host = "admin.example"; })).Status);

        var health = Admin.Node.Url + "/health/";
        foreach (var path in new[] { "live", "ready", "status" })
        {
            var answer = await Admin.SendAsync(HttpMethod.Get, path, configure: r => { r.Headers.Add("X-Forwarded-For", "203.0.113.9"); r.Headers.Host = "lb.example"; }, baseUrl: health);
            Assert.Equal(200, answer.Code);
        }
        var status = await Admin.SendAsync(HttpMethod.Get, "status", baseUrl: health);
        Assert.Equal("Degraded", status["level"].GetString());
        Assert.Equal(["pipeline-failed"], status["reasons"].EnumerateArray().Select(r => r.GetString()));
        Assert.Equal("Leader", status["role"].GetString());
        // Codes only: no ids, no messages.
        Assert.DoesNotContain("checked-orders", status.Body.GetRawText());
    }

    [Fact]
    public async Task AllowedHosts_admits_a_host_name_and_nothing_else()
    {
        await using var admin = await AdminFixture.StartAsync(o => o.Admin = a => a.AllowedHosts.Add("admin.internal"));
        Assert.Equal(200, (await admin.Get("info", r => r.Headers.Host = "admin.internal")).Code);
        Assert.Equal(200, (await admin.Get("info", r => r.Headers.Host = "ADMIN.internal:8080")).Code);
        (await admin.Get("info", r => r.Headers.Host = "other.internal")).IsError(403, "host-not-allowed");
        (await admin.Get("info", r => { r.Headers.Host = "admin.internal"; r.Headers.Add("X-Forwarded-For", "10.0.0.1"); })).IsError(403, "forwarded");
    }

    [Fact]
    public async Task Mutations_need_the_header_a_confirmation_and_the_generation_the_caller_saw()
    {
        await using var admin = await AdminFixture.StartAsync();
        var node = admin.Node.Node;

        (await admin.SendAsync(HttpMethod.Post, "pipelines/trader-activity/stop", header: false)).IsError(400, "missing-header");
        Assert.Equal(PipelineState.Running, node.Pipelines.Get("trader-activity").State);

        // A dry run says what would happen and does none of it.
        var plan = await admin.Post("pipelines/trader-activity/stop?dryRun=true");
        Assert.Equal(200, plan.Code);
        Assert.True(plan["plan"].GetProperty("dryRun").GetBoolean());
        Assert.True(plan["plan"].GetProperty("steps").GetArrayLength() > 0);
        Assert.Equal(PipelineState.Running, node.Pipelines.Get("trader-activity").State);

        // The destructive ones must be confirmed with the id.
        (await admin.Post("pipelines/trader-activity/reset")).IsError(400, "confirmation-required");
        (await admin.Post("pipelines/trader-activity/reset?confirm=trader")).IsError(400, "confirmation-required");
        (await admin.SendAsync(HttpMethod.Delete, "pipelines/trader-activity")).IsError(400, "confirmation-required");
        (await admin.SendAsync(HttpMethod.Delete, "tables/positions")).IsError(400, "confirmation-required");
        (await admin.Post("node/stepdown")).IsError(400, "confirmation-required");
        Assert.Equal(1, node.Pipelines.Spec("trader-activity").Generation);
        Assert.Equal(200, (await admin.Post("pipelines/trader-activity/reset?dryRun=true")).Code);

        // If-Match carries the generation; a stale one is 412 and changes nothing.
        (await admin.SendAsync(HttpMethod.Post, "pipelines/trader-activity/stop", configure: r => r.Headers.TryAddWithoutValidation("If-Match", "\"7\""))).IsError(412, "precondition-failed");
        Assert.Equal(PipelineState.Running, node.Pipelines.Get("trader-activity").State);
        Assert.Equal(200, (await admin.SendAsync(HttpMethod.Post, "pipelines/trader-activity/stop", configure: r => r.Headers.TryAddWithoutValidation("If-Match", "\"1\""))).Code);
        await admin.PipelineIs("trader-activity", "Stopped");
        Assert.Equal(200, (await admin.Post("pipelines/trader-activity/start")).Code);
        await admin.PipelineIs("trader-activity", "Running");
        Assert.Equal(200, (await admin.Post("pipelines/trader-activity/restart")).Code);
        Assert.Equal(200, (await admin.Post("pipelines/trader-activity/reset?confirm=trader-activity")).Code);
        await admin.PipelineIs("trader-activity", "Running");

        // Replace: a change that can touch state must say keep or reset.
        var spec = (await admin.Get("pipelines/enrich-orders"))["spec"];
        var edited = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(spec.GetRawText())!;
        edited["settings"] = DemoHost.Json("""{ "minNotional": 5, "label": "all orders" }""");
        var refused = await admin.SendAsync(HttpMethod.Put, "pipelines/enrich-orders", edited);
        Assert.Equal(409, refused.Code);
        Assert.Equal("state-choice-required", refused.ErrorCode);
        Assert.NotEmpty(refused.Body.GetProperty("error").GetProperty("details").EnumerateArray());
        var dry = await admin.SendAsync(HttpMethod.Put, "pipelines/enrich-orders?state=reset&dryRun=true", edited);
        Assert.Equal(200, dry.Code);
        Assert.Equal(1, node.Pipelines.Spec("enrich-orders").Generation);
        var replaced = await admin.SendAsync(HttpMethod.Put, "pipelines/enrich-orders?state=reset", edited);
        Assert.Equal(200, replaced.Code);
        Assert.True(replaced["spec"].GetProperty("generation").GetInt64() >= 2);
        Assert.Equal(node.Pipelines.Spec("enrich-orders").Generation, replaced["spec"].GetProperty("generation").GetInt64());
        Assert.Equal("pipeline.replace", replaced["plan"].GetProperty("action").GetString());
        // A neutral setting alone needs no choice.
        edited["settings"] = DemoHost.Json("""{ "minNotional": 5, "label": "renamed" }""");
        Assert.Equal(200, (await admin.SendAsync(HttpMethod.Put, "pipelines/enrich-orders", edited)).Code);

        // Validate runs the definition and changes nothing.
        var validated = await admin.Post("pipelines/validate", DemoHost.Spec("brand-new", "trader-activity", DemoHost.Orders, ["counts", "quantities"]));
        Assert.Equal(200, validated.Code);
        Assert.Equal(2, validated["shape"].GetProperty("sinks").GetArrayLength());
        Assert.True(validated["graph"].GetProperty("nodes").GetArrayLength() > 3);
        (await admin.Get("pipelines/brand-new")).IsError(404, "not-found");
        var unbound = await admin.Post("pipelines/validate", DemoHost.Spec("brand-new", "trader-activity", DemoHost.Orders, ["counts"]));
        unbound.IsError(400, "invalid");

        // Deploy and remove through the API; the audit says who and what.
        var deployed = await admin.SendAsync(HttpMethod.Put, "pipelines/brand-new", DemoHost.Spec("brand-new", "trader-activity", DemoHost.Orders, ["counts", "quantities"]));
        Assert.Equal(200, deployed.Code);
        await admin.PipelineIs("brand-new", "Running");
        Assert.Equal(200, (await admin.SendAsync(HttpMethod.Delete, "pipelines/brand-new?confirm=brand-new")).Code);
        (await admin.Get("pipelines/brand-new")).IsError(404, "not-found");
        var audit = (await admin.Get("audit?limit=50"))["items"].EnumerateArray().ToList();
        var removal = audit.Last(a => a.GetProperty("action").GetString() == "pipeline.remove");
        Assert.Equal("brand-new", removal.GetProperty("target").GetString());
        Assert.Equal("local", removal.GetProperty("caller").GetString());
        Assert.NotEmpty(removal.GetProperty("plan").EnumerateArray());

        // Tables: a removal is refused while pipelines name the table; a loader restarts.
        (await admin.SendAsync(HttpMethod.Delete, "tables/positions?confirm=positions")).IsError(409, "conflict");
        Assert.Equal(200, (await admin.Post("tables/counterparties/loader/restart?confirm=counterparties")).Code);
        Assert.Equal(200, (await admin.Post("tables/counterparties/reopen?dryRun=true")).Code);
        (await admin.Post("tables/instruments/loader/restart?confirm=instruments")).IsError(409, "conflict");

        // The node: a leader cannot take over, and a lone node does not step down unforced.
        (await admin.Post("node/takeover?confirm=node-a")).IsError(409, "conflict");
        (await admin.Post("node/stepdown?dryRun=true")).IsError(409, "not-ready");
        Assert.Equal(200, (await admin.Post("node/stepdown?dryRun=true&force=true")).Code);
        Assert.True(node.IsLeader);
    }

    [Fact]
    public async Task ReadOnly_refuses_every_mutation_and_still_shows_everything()
    {
        await using var admin = await AdminFixture.StartAsync(o => o.ReadOnly = true);
        Assert.True((await admin.Get("info"))["readOnly"].GetBoolean());
        Assert.False((await admin.Get("info"))["allowPluginUpload"].GetBoolean());
        Assert.Equal(200, (await admin.Get("pipelines")).Code);
        Assert.Equal(200, (await admin.Get("tables/instruments/rows")).Code);

        var spec = DemoHost.Spec("ro", "trader-activity", DemoHost.Orders, ["counts", "quantities"]);
        (await admin.Post("pipelines/trader-activity/stop")).IsError(403, "read-only");
        (await admin.Post("pipelines/trader-activity/stop?dryRun=true")).IsError(403, "read-only");
        (await admin.Post("pipelines/trader-activity/reset?confirm=trader-activity")).IsError(403, "read-only");
        (await admin.SendAsync(HttpMethod.Delete, "pipelines/trader-activity?confirm=trader-activity")).IsError(403, "read-only");
        (await admin.SendAsync(HttpMethod.Put, "pipelines/ro", spec)).IsError(403, "read-only");
        (await admin.Post("pipelines/validate", spec)).IsError(403, "read-only");
        (await admin.SendAsync(HttpMethod.Delete, "tables/positions?confirm=positions")).IsError(403, "read-only");
        (await admin.Post("tables", new { name = "x", definition = "y" })).IsError(403, "read-only");
        (await admin.Post("tables/counterparties/loader/restart?confirm=counterparties")).IsError(403, "read-only");
        (await admin.Post("tables/counterparties/reopen?confirm=counterparties")).IsError(403, "read-only");
        (await admin.Post("plugins", new ByteArrayContent([1]) { Headers = { ContentType = new MediaTypeHeaderValue("application/zip") } })).IsError(403, "read-only");
        (await admin.Post("plugins/rescan")).IsError(403, "read-only");
        (await admin.SendAsync(HttpMethod.Delete, "plugins/a/b?confirm=a/b")).IsError(403, "read-only");
        (await admin.Post("node/stepdown?confirm=node-a&force=true")).IsError(403, "read-only");
        (await admin.Post("node/takeover?confirm=node-a")).IsError(403, "read-only");

        Assert.Equal(PipelineState.Running, admin.Node.Node.Pipelines.Get("trader-activity").State);
        Assert.Equal(1, admin.Node.Node.Pipelines.Spec("trader-activity").Generation);
        Assert.True(admin.Node.Node.IsLeader);
    }

    [Fact]
    public async Task With_row_data_off_rows_are_refused_and_messages_records_and_stacks_are_withheld()
    {
        await using var admin = await AdminFixture.StartAsync(o => o.ExposeRowData = false);
        var random = new Random(5);
        admin.Host.Memory.KeyedLog<long, Order>(DemoHost.LenientFeed).Upsert(DemoHost.Order(1, random, 300, negative: true));
        admin.Host.Memory.KeyedLog<long, Order>(DemoHost.CheckedFeed).Upsert(DemoHost.Order(1, random, 300, negative: true));
        await TestService.Eventually(() => admin.Node.Node.Pipelines.Get("lenient-orders").DeadLetters == 1, "a dead letter");
        await TestService.Eventually(() => admin.Node.Node.Pipelines.Get("checked-orders").Faults.Count > 0, "a fault");

        Assert.False((await admin.Get("info"))["exposeRowData"].GetBoolean());
        (await admin.Get("tables/instruments/rows")).IsError(403, "row-data-hidden");
        (await admin.Get("tables/instruments/rows?key=1")).IsError(403, "row-data-hidden");

        var letters = await admin.Get("pipelines/lenient-orders/dead-letters");
        var letter = letters["items"][0];
        Assert.False(letters["rowDataExposed"].GetBoolean());
        Assert.Equal("InvalidOperationException", letter.GetProperty("exceptionType").GetString());
        Assert.Equal("n2.select", letter.GetProperty("operator").GetString());
        Assert.False(letter.TryGetProperty("message", out _));
        Assert.False(letter.TryGetProperty("record", out _));

        var failed = await admin.Get("pipelines/checked-orders");
        Assert.Equal("InvalidOperationException", failed["faults"][0].GetProperty("exceptionType").GetString());
        Assert.False(failed["faults"][0].TryGetProperty("message", out _));
        Assert.False(failed["faults"][0].TryGetProperty("stack", out _));
        Assert.False(failed["pipeline"].GetProperty("lastFault").TryGetProperty("message", out _));
        var listed = (await admin.Get("pipelines?q=checked-orders"))["items"][0];
        Assert.False(listed.GetProperty("lastFault").TryGetProperty("message", out _));

        var table = await admin.Get("tables/counterparties");
        Assert.Equal("JsonException", table["unreadable"][0].GetProperty("exceptionType").GetString());
        Assert.False(table["unreadable"][0].TryGetProperty("message", out _));
        Assert.False(table["unreadable"][0].TryGetProperty("record", out _));

        // A definition's description quotes what its Build threw — which can quote the
        // settings it was asked with — only when messages may be shown.
        var version = (await admin.Get("plugins"))["items"].EnumerateArray()
            .First(p => p.GetProperty("name").GetString() == DemoHost.Plugin).GetProperty("version").GetString();
        var badDesk = Uri.EscapeDataString("""{ "desks": ["X <b>"] }""");
        var described = await admin.Get($"plugins/{DemoHost.Plugin}/{version}/definitions/desk-risk?settings={badDesk}");
        Assert.Equal(200, described.Code);
        var note = Assert.Single(described["notes"].EnumerateArray()).GetString()!;
        Assert.StartsWith("The slots may be incomplete, because the definition threw ", note);
        Assert.DoesNotContain("X <b>", described.Body.ToString());
        Assert.DoesNotContain(":", note);

        // Nothing anywhere in what the API returns quotes the offending value.
        await TestService.Eventually(() => admin.Node.Node.Pipelines.Get("checked-orders").State == PipelineState.Failed, "the checked pipeline failed");
        Assert.Contains((await admin.Get("events/recent?limit=200"))["items"].EnumerateArray(),
            e => e.GetProperty("subject").GetString() == "checked-orders" && e.GetProperty("message").GetString() == "Failed");

        // What is kept of a reason is everything before the exception's own text — the
        // whole of the service's part, a length of time included.
        var why = (await admin.Get("pipelines/checked-orders"))["history"].EnumerateArray()
            .Last(t => t.GetProperty("state").GetString() == "Failed").GetProperty("why").GetString();
        Assert.Matches(@"^\d+ faults within \d+ (ms|s|min|h|d)( \d+ (s|min|h))?; the last$", why);
        foreach (var path in new[] { "pipelines", "pipelines/checked-orders", "pipelines/lenient-orders/dead-letters", "tables/counterparties", "tables", "events/recent?limit=200", "audit?limit=200" })
        {
            var text = (await admin.Get(path)).Body.GetRawText();
            Assert.DoesNotContain("negative quantity", text);
            Assert.DoesNotContain("CP-BROKEN", text);
        }
    }

    private sealed class HeaderAuthentication(IOptionsMonitor<AuthenticationSchemeOptions> options, ILoggerFactory logger, UrlEncoder encoder)
        : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
    {
        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            if (Request.Headers["X-Test-User"].FirstOrDefault() is not { Length: > 0 } user)
                return Task.FromResult(AuthenticateResult.NoResult());
            var parts = user.Split(':');
            var identity = new ClaimsIdentity(parts.Skip(1).Select(r => new Claim(ClaimTypes.Role, r)).Prepend(new Claim(ClaimTypes.Name, parts[0])), "test");
            return Task.FromResult(AuthenticateResult.Success(new AuthenticationTicket(new ClaimsPrincipal(identity), "test")));
        }
    }

    [Fact]
    public async Task The_authorization_policies_are_honoured_and_lift_the_loopback_rule()
    {
        await using var admin = await AdminFixture.StartAsync(o =>
        {
            o.Builder = b =>
            {
                b.Services.AddAuthentication("test").AddScheme<AuthenticationSchemeOptions, HeaderAuthentication>("test", null);
                b.Services.AddAuthorization(a =>
                {
                    a.AddPolicy("viewers", p => p.RequireAuthenticatedUser());
                    a.AddPolicy("operators", p => p.RequireRole("operator"));
                    a.AddPolicy("owners", p => p.RequireRole("owner"));
                });
            };
            o.Admin = a =>
            {
                a.AuthorizationPolicy = "viewers";
                a.PipelinePolicy = "operators";
                a.NodePolicy = "owners";
            };
        });
        Action<HttpRequestMessage> As(string user) => r => r.Headers.Add("X-Test-User", user);

        Assert.Equal(401, (await admin.Get("node")).Code);
        Assert.Equal(401, (await admin.Get("pipelines")).Code);
        Assert.Equal(HttpStatusCode.Unauthorized, (await admin.Get("", r => r.RequestUri = new Uri(admin.Node.Admin))).Status);
        Assert.Equal(401, (await admin.Post("pipelines/trader-activity/stop")).Code);
        Assert.Equal(200, (await admin.Get("node", As("vera"))).Code);

        // A policy is the host saying who may come in: forwarded requests and other hosts are then its business.
        Assert.Equal(200, (await admin.Get("node", r => { As("vera")(r); r.Headers.Add("X-Forwarded-For", "203.0.113.9"); r.Headers.Host = "admin.example"; })).Code);

        // The area policies sit on top of the general one.
        Assert.Equal(403, (await admin.SendAsync(HttpMethod.Post, "pipelines/trader-activity/stop", configure: As("vera"))).Code);
        Assert.Equal(PipelineState.Running, admin.Node.Node.Pipelines.Get("trader-activity").State);
        Assert.Equal(200, (await admin.SendAsync(HttpMethod.Post, "pipelines/trader-activity/stop", configure: As("olga:operator"))).Code);
        await admin.PipelineIs("trader-activity", "Stopped");
        Assert.Equal(403, (await admin.SendAsync(HttpMethod.Post, "node/stepdown?dryRun=true&force=true", configure: As("olga:operator"))).Code);
        Assert.Equal(200, (await admin.SendAsync(HttpMethod.Post, "node/stepdown?dryRun=true&force=true", configure: As("ann:owner"))).Code);
        // An area with no policy of its own falls back to the general one.
        Assert.Equal(200, (await admin.SendAsync(HttpMethod.Post, "tables/counterparties/loader/restart?dryRun=true", configure: As("vera"))).Code);

        // The audit records the authenticated name.
        var audit = (await admin.Get("audit?limit=5", As("vera")))["items"].EnumerateArray().Last();
        Assert.Equal("pipeline.stop", audit.GetProperty("action").GetString());
        Assert.Equal("olga", audit.GetProperty("caller").GetString());

        // Health is still outside all of it.
        Assert.Equal(200, (await admin.SendAsync(HttpMethod.Get, "ready", baseUrl: admin.Node.Url + "/health/")).Code);
    }

    // ── plugin upload ──────────────────────────────────────────────────────────────────

    private static ByteArrayContent Zip(Action<ZipArchive> fill, string type = "application/zip")
    {
        using var buffer = new MemoryStream();
        using (var archive = new ZipArchive(buffer, ZipArchiveMode.Create, leaveOpen: true))
            fill(archive);
        return new ByteArrayContent(buffer.ToArray()) { Headers = { ContentType = new MediaTypeHeaderValue(type) } };
    }

    /// <summary>A body sent chunked: no length is declared for it.</summary>
    private sealed class Unsized : HttpContent
    {
        private readonly byte[] _bytes;

        public Unsized(byte[] bytes)
        {
            _bytes = bytes;
            Headers.ContentType = new MediaTypeHeaderValue("application/zip");
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
        {
            try
            {
                for (var sent = 0; sent < _bytes.Length; sent += 8192)
                    await stream.WriteAsync(_bytes.AsMemory(sent, Math.Min(8192, _bytes.Length - sent)));
            }
            catch (Exception ex) when (ex is IOException or HttpRequestException)
            {
                // The server answered and stopped reading before the body ended.
            }
        }

        protected override bool TryComputeLength(out long length)
        {
            length = 0;
            return false;
        }
    }

    private static void Add(ZipArchive archive, string name, byte[] bytes)
    {
        using var stream = archive.CreateEntry(name).Open();
        stream.Write(bytes);
    }

    [Fact]
    public async Task An_upload_is_validated_before_anything_is_stored()
    {
        await using var off = await AdminFixture.StartAsync(o => o.AllowPluginUpload = false);
        (await off.Post("plugins", Zip(a => Add(a, "x.dll", [1])))).IsError(403, "upload-disabled");

        await using var admin = await AdminFixture.StartAsync(o => o.Admin = a => a.MaxUploadBytes = 200_000);
        var package = TestService.PluginDirectory("Acme.Pipelines");
        var before = (await admin.Get("plugins"))["total"].GetInt32();

        (await admin.Post("plugins", new StringContent("{}", Encoding.UTF8, "application/json"))).IsError(415, "unsupported-media-type");
        (await admin.Post("plugins", new ByteArrayContent([]) { Headers = { ContentType = new MediaTypeHeaderValue("application/zip") } })).IsError(400, "invalid");
        (await admin.Post("plugins", new ByteArrayContent(Encoding.UTF8.GetBytes("this is not a zip")) { Headers = { ContentType = new MediaTypeHeaderValue("application/zip") } })).IsError(400, "invalid-zip");
        (await admin.Post("plugins", new ByteArrayContent(new byte[300_000]) { Headers = { ContentType = new MediaTypeHeaderValue("application/zip") } })).IsError(413, "too-large");
        // The same without a Content-Length to refuse it by: counted as it arrives.
        (await admin.Post("plugins", new Unsized(new byte[300_000]))).IsError(413, "too-large");
        // A name that would leave the directory it is unpacked into.
        (await admin.Post("plugins", Zip(a => Add(a, "../evil.dll", [1, 2, 3])))).IsError(400, "invalid");
        // A zip with no assembly in it.
        Assert.Equal(400, (await admin.Post("plugins", Zip(a => Add(a, "readme.txt", [1])))).Code);
        // Bytes that are named like an assembly and are not one.
        var garbage = await admin.Post("plugins", Zip(a => Add(a, "Garbage.dll", new byte[512])));
        Assert.InRange(garbage.Code, 400, 499);
        Assert.NotNull(garbage.ErrorCode);
        Assert.Equal(before, (await admin.Get("plugins"))["total"].GetInt32());

        var good = await admin.Post("plugins", Zip(a =>
        {
            foreach (var file in Directory.EnumerateFiles(package))
                Add(a, Path.GetFileName(file), File.ReadAllBytes(file));
        }));
        Assert.Equal(201, good.Code);
        Assert.Equal("acme.pipelines", good["plugin"].GetProperty("name").GetString());
        Assert.Equal(["trade-enrichment", "trader-counts"], good["plugin"].GetProperty("definitions").EnumerateArray().Select(d => d.GetProperty("name").GetString()));
        var version = good["plugin"].GetProperty("version").GetString()!;
        var stored = await admin.Get("plugins/acme.pipelines");
        Assert.Equal(version, stored["versions"][0].GetProperty("plugin").GetProperty("version").GetString());
        Assert.True(stored["versions"][0].GetProperty("files").GetArrayLength() >= 2);
        Assert.Equal("plugin.import", (await admin.Get("audit?limit=1"))["items"][0].GetProperty("action").GetString());

        // Deleting it needs the name and the version, and says what it will do first.
        (await admin.SendAsync(HttpMethod.Delete, $"plugins/acme.pipelines/{version}")).IsError(400, "confirmation-required");
        Assert.Equal(200, (await admin.SendAsync(HttpMethod.Delete, $"plugins/acme.pipelines/{version}?dryRun=true")).Code);
        Assert.Equal(200, (await admin.SendAsync(HttpMethod.Delete, $"plugins/acme.pipelines/{version}?confirm=acme.pipelines/{version}")).Code);
        (await admin.Get("plugins/acme.pipelines")).IsError(404, "not-found");
        // A built-in plugin is not something the API deletes.
        var builtIn = (await admin.Get("plugins/" + DemoHost.Plugin))["versions"][0].GetProperty("plugin").GetProperty("version").GetString();
        (await admin.SendAsync(HttpMethod.Delete, $"plugins/{DemoHost.Plugin}/{builtIn}?confirm={DemoHost.Plugin}/{builtIn}")).IsError(404, "not-found");
    }

    // ── the event feed ─────────────────────────────────────────────────────────────────

    private static async Task<List<(string Kind, string? Id, JsonElement Data)>> ReadEventsAsync(
        Stream stream, Func<List<(string Kind, string? Id, JsonElement Data)>, bool> until, TimeSpan timeout)
    {
        var events = new List<(string, string?, JsonElement)>();
        using var cancel = new CancellationTokenSource(timeout);
        using var reader = new StreamReader(stream);
        string? kind = null, id = null;
        while (!until(events))
        {
            var line = await reader.ReadLineAsync(cancel.Token);
            if (line is null)
                break;
            if (line.StartsWith("event: "))
                kind = line[7..];
            else if (line.StartsWith("id: "))
                id = line[4..];
            else if (line.StartsWith("data: "))
            {
                events.Add((kind ?? "message", id, JsonDocument.Parse(line[6..]).RootElement.Clone()));
                (kind, id) = (null, null);
            }
        }
        return events;
    }

    [Fact]
    public async Task The_feed_sends_ticks_and_events_and_replays_from_the_last_id()
    {
        await using var admin = await AdminFixture.StartAsync();
        using var response = await admin.Http.GetAsync(admin.Api + "events", HttpCompletionOption.ResponseHeadersRead);
        Assert.Equal("text/event-stream", response.Content.Headers.ContentType!.MediaType);
        var reading = ReadEventsAsync(await response.Content.ReadAsStreamAsync(),
            e => e.Any(x => x.Kind == "metrics") && e.Any(x => x.Kind == "pipeline" && x.Data.GetProperty("message").GetString() == "Stopped"), TimeSpan.FromSeconds(20));
        await TestService.Eventually(() => admin.Get("node").Result["events"].GetProperty("clients").GetInt32() == 1, "the feed is open");
        Assert.Equal(200, (await admin.Post("pipelines/trader-activity/stop")).Code);
        var events = await reading;

        Assert.Equal("hello", events[0].Kind);
        Assert.Equal("node-a", events[0].Data.GetProperty("nodeId").GetString());
        var tick = events.First(e => e.Kind == "metrics").Data;
        Assert.Equal("Leader", tick.GetProperty("node").GetProperty("role").GetString());
        Assert.True(tick.GetProperty("pipelines").TryGetProperty("enrich-orders", out _));
        var stopped = events.First(e => e.Kind == "pipeline" && e.Data.GetProperty("message").GetString() == "Stopped");
        Assert.Equal("trader-activity", stopped.Data.GetProperty("subject").GetString());
        Assert.Equal(stopped.Data.GetProperty("sequence").GetInt64().ToString(), stopped.Id);

        // A client that reconnects says what it last saw and is sent what it missed, once.
        var last = long.Parse(stopped.Id!);
        Assert.Equal(200, (await admin.Post("pipelines/trader-activity/start")).Code);
        await admin.PipelineIs("trader-activity", "Running");
        var request = new HttpRequestMessage(HttpMethod.Get, admin.Api + "events");
        request.Headers.Add("Last-Event-ID", last.ToString());
        using var again = await admin.Http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
        var replayed = await ReadEventsAsync(await again.Content.ReadAsStreamAsync(), e => e.Any(x => x.Kind == "metrics"), TimeSpan.FromSeconds(20));
        var ids = replayed.Where(e => e.Id is not null).Select(e => long.Parse(e.Id!)).ToList();
        Assert.NotEmpty(ids);
        Assert.All(ids, i => Assert.True(i > last));
        Assert.Equal(ids.Order(), ids);
        Assert.Equal(ids.Distinct().Count(), ids.Count);
        Assert.Contains(replayed, e => e.Kind == "pipeline" && e.Data.GetProperty("message").GetString() == "Running");
    }

    [Fact]
    public async Task A_client_that_does_not_read_costs_a_bounded_queue_and_is_dropped()
    {
        await using var admin = await AdminFixture.StartAsync(o => o.Admin = a =>
        {
            a.EventQueueLength = 16;
            a.EventWriteTimeout = TimeSpan.FromSeconds(2);
            a.MaxEventClients = 2;
        });
        var hub = admin.Node.App.Services.GetRequiredService<AdminEventHub>();
        var uri = new Uri(admin.Api + "events");

        // A raw socket that asks for the feed and never reads a byte of it.
        using var slow = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { ReceiveBufferSize = 4096 };
        await slow.ConnectAsync(IPAddress.Loopback, uri.Port);
        await slow.SendAsync(Encoding.ASCII.GetBytes($"GET {uri.AbsolutePath} HTTP/1.1\r\nHost: 127.0.0.1:{uri.Port}\r\nAccept: text/event-stream\r\n\r\n"));
        // And one that reads.
        using var response = await admin.Http.GetAsync(uri, HttpCompletionOption.ResponseHeadersRead);
        var stream = await response.Content.ReadAsStreamAsync();
        await TestService.Eventually(() => hub.Clients == 2, "both clients connected");

        // A third is refused: the number of streams is bounded too.
        (await admin.Get("events")).IsError(503, "too-many-clients");

        // The reader reads throughout; frames are published until the one that never reads
        // has filled every buffer between the server and itself, and a write to it has
        // then waited out the timeout.
        var done = false;
        var reading = ReadEventsAsync(stream, _ => Volatile.Read(ref done), TimeSpan.FromSeconds(60));
        var payload = JsonSerializer.Serialize(new { filler = new string('x', 16_000) });
        var deepest = 0;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(40);
        while (hub.Aborted == 0)
        {
            Assert.True(DateTime.UtcNow < deadline, $"the stalled client was never aborted (clients {hub.Clients}, dropped {hub.Dropped}, deepest {deepest})");
            for (var i = 0; i < 400; i++)
            {
                hub.Publish("filler", payload);
                deepest = Math.Max(deepest, hub.DeepestQueue);
            }
            await Task.Delay(50);
        }
        Assert.InRange(deepest, 1, 16);
        Assert.True(hub.Dropped > 0, "frames were dropped");
        Assert.Equal(1, hub.Aborted);

        // The reader was told it lagged, and carried on.
        hub.Publish("filler", payload);
        Volatile.Write(ref done, true);
        hub.Publish("filler", payload);
        var read = await reading;
        Assert.Contains(read, e => e.Kind == "filler");
        Assert.True(read.First(e => e.Kind == "lagged").Data.GetProperty("dropped").GetInt64() > 0);
        await TestService.Eventually(() => hub.Clients == 1, "only the reader remains");
        Assert.True((await admin.Get("node"))["events"].GetProperty("dropped").GetInt64() > 0);
    }

    [Fact]
    public async Task The_feed_ends_when_the_application_stops()
    {
        var admin = await AdminFixture.StartAsync();
        using var response = await admin.Http.GetAsync(admin.Api + "events", HttpCompletionOption.ResponseHeadersRead);
        var reading = ReadEventsAsync(await response.Content.ReadAsStreamAsync(), _ => false, TimeSpan.FromSeconds(30));
        await TestService.Eventually(() => admin.Node.App.Services.GetRequiredService<AdminEventHub>().Clients == 1, "the feed is open");
        var clock = System.Diagnostics.Stopwatch.StartNew();
        await admin.DisposeAsync();
        // An open event stream must not hold the server's shutdown until its timeout.
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(15), $"stopping took {clock.Elapsed}");
        try
        {
            await reading.WaitAsync(TimeSpan.FromSeconds(10));
        }
        catch (Exception ex) when (ex is IOException or HttpRequestException)
        {
            // The stream ended with the connection: equally an end.
        }
    }

    // ── two nodes ──────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task A_standby_serves_reads_and_sends_mutations_to_the_leader_until_it_takes_over()
    {
        await using var admin = await AdminFixture.StartAsync(o => { o.Nodes = 2; o.Port = 0; o.Service = s => s.AdvertisedAdminUrl = "http://leader.example/admin/"; });
        var standby = admin.Host.Nodes[1].Url + "/admin/api/";
        await TestService.Eventually(() => admin.Host.Nodes[1].Node.Status.Standby is { TakeoverWouldBeWarm: true }, "the standby is warm");

        var node = await admin.SendAsync(HttpMethod.Get, "node", baseUrl: standby);
        Assert.Equal("Standby", node["status"].GetProperty("leadership").GetProperty("role").GetString());
        Assert.Equal("node-a", node["leader"].GetProperty("nodeId").GetString());
        Assert.False(node["leader"].GetProperty("isSelf").GetBoolean());
        Assert.True(node["status"].GetProperty("standby").GetProperty("takeoverWouldBeWarm").GetBoolean());
        Assert.Equal(4, node["stateLag"].GetArrayLength());
        Assert.Equal("node-a", node["status"].GetProperty("peer").GetProperty("nodeId").GetString());
        Assert.True(node["peerExpected"].GetBoolean());

        // Rows come from the standby's own copy, with how far behind it is.
        var rows = await admin.SendAsync(HttpMethod.Get, "tables/instruments/rows?key=7", baseUrl: standby);
        Assert.Equal(7, rows["rows"][0].GetProperty("key").GetInt32());
        Assert.Equal(0, rows["lagVersions"].GetInt64());
        var tables = await admin.SendAsync(HttpMethod.Get, "tables", baseUrl: standby);
        Assert.All(tables["items"].EnumerateArray(), t => Assert.True(t.GetProperty("following").GetBoolean()));
        // It has built the circuit on the state it follows, so it has the circuit's graph.
        await TestService.Eventually(() => admin.Host.Nodes[1].Node.Pipelines.Get("enrich-orders").State == PipelineState.Built, "the standby builds the circuit");
        var pipeline = await admin.SendAsync(HttpMethod.Get, "pipelines/enrich-orders", baseUrl: standby);
        Assert.Equal("Built", pipeline["pipeline"].GetProperty("state").GetString());
        Assert.True(pipeline["hasGraph"].GetBoolean());
        var graph = await admin.SendAsync(HttpMethod.Get, "pipelines/enrich-orders/graph", baseUrl: standby);
        Assert.Equal("circuit", graph["source"].GetString());

        // What there is to deploy with is the host's, so a standby lists it; describing a
        // definition runs plugin code, which only the leader does.
        var connectors = await admin.SendAsync(HttpMethod.Get, "connectors", baseUrl: standby);
        Assert.Equal(["files", "kafka", "memory"], connectors["items"].EnumerateArray().Select(c => c.GetProperty("name").GetString()));
        var version = (await admin.Get("plugins"))["items"].EnumerateArray().First(p => p.GetProperty("name").GetString() == DemoHost.Plugin).GetProperty("version").GetString();
        (await admin.SendAsync(HttpMethod.Get, $"plugins/{DemoHost.Plugin}/{version}/definitions/enrich-orders", baseUrl: standby)).IsError(409, "not-leader");

        // A mutation there is refused with where to send it.
        var refused = await admin.SendAsync(HttpMethod.Post, "pipelines/trader-activity/stop", baseUrl: standby);
        refused.IsError(409, "not-leader");
        Assert.Equal("node-a", refused.Body.GetProperty("error").GetProperty("leader").GetProperty("nodeId").GetString());
        Assert.Equal("http://leader.example/admin/", refused.Body.GetProperty("error").GetProperty("leader").GetProperty("adminUrl").GetString());
        (await admin.SendAsync(HttpMethod.Post, "node/stepdown?confirm=node-b", baseUrl: standby)).IsError(409, "conflict");

        // Except takeover, which is asked of the standby — once it has built what it can,
        // so that what its plan counts holds still.
        await TestService.Eventually(() => admin.Host.Nodes[1].Node.Status.Standby is { PipelinesToBuild: 0 }, "the standby's builds done");
        var plan = await admin.SendAsync(HttpMethod.Post, "node/takeover?dryRun=true", baseUrl: standby);
        var steps = plan["plan"].GetProperty("steps").EnumerateArray().Select(x => x.GetString()!).ToList();
        Assert.Equal(3, steps.Count);
        // A warm standby starts what it has built; the plan says so rather than "builds".
        // It counts what this standby has built and what it would still have to build.
        var standbyNode = admin.Host.Nodes[1].Node;
        var running = standbyNode.Pipelines.Pipelines.Where(p => p.Desired == DesiredState.Running).ToList();
        var built = running.Count(p => p.State == PipelineState.Built);
        Assert.InRange(built, 1, running.Count - 1);         // the host's pipelines include one that is built at takeover
        Assert.Equal(
            $"it starts the {built} pipeline(s) it has built while standing by, and builds and starts {running.Count - built} more from their committed state",
            steps[2]);
        Assert.Equal(200, (await admin.SendAsync(HttpMethod.Post, "node/takeover?confirm=node-b", baseUrl: standby)).Code);
        await TestService.Eventually(() => admin.Host.Nodes[1].Node.IsLeader, "node-b leads");
        var after = await admin.Get("node");
        Assert.Equal("Fenced", after["status"].GetProperty("leadership").GetProperty("role").GetString());

        // The node that was replaced says who replaced it, and where that node is: from
        // its probe of the other node, since the catalog it holds is the one it wrote
        // and still names itself.
        for (var attempt = 0; attempt < 100 && !(after.Body.TryGetProperty("leader", out var known) && known.ValueKind == JsonValueKind.Object); attempt++)
        {
            await Task.Delay(100);
            after = await admin.Get("node");
        }
        Assert.Equal("node-b", after["leader"].GetProperty("nodeId").GetString());
        Assert.False(after["leader"].GetProperty("isSelf").GetBoolean());
        Assert.Equal("http://leader.example/admin/", after["leader"].GetProperty("adminUrl").GetString());
        Assert.Equal("Leader", after["status"].GetProperty("peer").GetProperty("role").GetString());
        var sentOn = await admin.Post("pipelines/trader-activity/stop");
        sentOn.IsError(409, "not-leader");
        Assert.Equal("node-b", sentOn.Body.GetProperty("error").GetProperty("leader").GetProperty("nodeId").GetString());
        Assert.Equal("Unhealthy", after["status"].GetProperty("health").GetProperty("level").GetString());
        Assert.Equal(503, (await admin.SendAsync(HttpMethod.Get, "ready", baseUrl: admin.Node.Url + "/health/")).Code);
        Assert.Equal(200, (await admin.SendAsync(HttpMethod.Get, "ready", baseUrl: admin.Host.Nodes[1].Url + "/health/")).Code);
        Assert.Equal(200, (await admin.SendAsync(HttpMethod.Post, "pipelines/trader-activity/stop", baseUrl: standby)).Code);
    }

    // ── the UI: every file it needs is served, and every route it calls exists ─────────

    [Fact]
    public async Task The_UI_is_served_whole_with_a_policy_that_lets_it_load_nothing_else()
    {
        var root = new Uri(Admin.Node.Admin);
        using var handler = new HttpClientHandler { AllowAutoRedirect = false };
        using var http = new HttpClient(handler);

        // Without the slash the page's relative links would resolve one level up.
        var bare = await http.GetAsync(Admin.Node.Url + "/admin");
        Assert.Equal(HttpStatusCode.Redirect, bare.StatusCode);
        Assert.Equal("/admin/", bare.Headers.Location!.OriginalString);

        var page = await http.GetAsync(root);
        Assert.Equal(HttpStatusCode.OK, page.StatusCode);
        Assert.Equal("text/html", page.Content.Headers.ContentType!.MediaType);
        var policy = string.Join(" ", page.Headers.GetValues("Content-Security-Policy"));
        Assert.Contains("default-src 'none'", policy);
        Assert.Contains("script-src 'self'", policy);
        Assert.DoesNotContain("unsafe-inline", policy);
        var html = await page.Content.ReadAsStringAsync();

        // No inline script or handler, and nothing fetched from anywhere else.
        Assert.DoesNotMatch(new Regex("<script(?![^>]*\\bsrc=)", RegexOptions.IgnoreCase), html);
        Assert.DoesNotMatch(new Regex("\\son[a-z]+=", RegexOptions.IgnoreCase), html);
        Assert.DoesNotMatch(new Regex("(src|href)=\"(https?:)?//", RegexOptions.IgnoreCase), html);

        var assets = Regex.Matches(html, "(?:src|href)=\"([^\"#:]+)\"").Select(m => m.Groups[1].Value).Distinct().ToList();
        Assert.Contains("app.css", assets);
        Assert.Contains("app.js", assets);
        Assert.True(assets.Count >= 4, string.Join(", ", assets));
        var scripts = new List<string>();
        foreach (var asset in assets)
        {
            var file = await http.GetAsync(new Uri(root, asset));
            Assert.True(file.IsSuccessStatusCode, $"{asset}: {(int)file.StatusCode}");
            var type = file.Content.Headers.ContentType!.MediaType;
            Assert.Equal(asset.EndsWith(".css") ? "text/css" : "text/javascript", type);
            var text = await file.Content.ReadAsStringAsync();
            Assert.True(text.Length > 500, asset);
            // Neither a stylesheet nor a script reaches outside this origin.
            Assert.DoesNotMatch(new Regex("https?://(?!www\\.w3\\.org/)", RegexOptions.IgnoreCase), text);
            Assert.DoesNotContain("@import", text);
            if (asset.EndsWith(".js"))
                scripts.Add(text);

            var conditional = new HttpRequestMessage(HttpMethod.Get, new Uri(root, asset));
            conditional.Headers.TryAddWithoutValidation("If-None-Match", file.Headers.ETag!.Tag);
            Assert.Equal(HttpStatusCode.NotModified, (await http.SendAsync(conditional)).StatusCode);
        }
        Assert.Equal(HttpStatusCode.NotFound, (await http.GetAsync(new Uri(root, "nope.js"))).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await http.GetAsync(new Uri(root, "..%2Fappsettings.json"))).StatusCode);

        // Every API path the scripts build is a route the server maps, with that method.
        var endpoints = Admin.Node.App.Services.GetRequiredService<IEnumerable<EndpointDataSource>>()
            .SelectMany(s => s.Endpoints).OfType<RouteEndpoint>()
            .Where(e => e.RoutePattern.RawText!.StartsWith("/admin/api/") && !e.RoutePattern.RawText.Contains("{**"))
            .Select(e => (
                Pattern: new Regex("^" + Regex.Replace(Regex.Escape(e.RoutePattern.RawText!["/admin/api/".Length..]).Replace("\\{", "{"), "{[^}]+}", "[^/?]+") + "$"),
                Methods: e.Metadata.GetMetadata<HttpMethodMetadata>()?.HttpMethods ?? []))
            .ToList();
        Assert.True(endpoints.Count >= 25, $"{endpoints.Count} API routes");

        var calls = new List<(string Path, string Method)>();
        foreach (var script in scripts)
        {
            // api('…') and all('…') with a literal or template first argument — each call
            // on its own, also where several stand in one statement — and, where
            // the path is built first — the `path` variables of the action dialogs.
            foreach (Match call in Regex.Matches(script, "\\b(?:api|all)\\(\\s*(['`])((?:(?!\\1).)*)\\1((?:(?!\\b(?:api|all)\\()[^;])*)"))
            {
                var method = Regex.Match(call.Groups[3].Value, "method:\\s*'([A-Z]+)'") is { Success: true } m ? m.Groups[1].Value : "GET";
                // 'pipelines/' + enc(id): a path with one more segment.
                var more = Regex.IsMatch(call.Groups[3].Value, "^\\s*\\+\\s*(enc|encodeURIComponent)\\(");
                calls.Add((call.Groups[2].Value + (more ? "X" : ""), method));
            }
            foreach (Match built in Regex.Matches(script, "path:\\s*`([^`]+)`[^}]*?method:\\s*'([A-Z]+)'|method:\\s*'([A-Z]+)',\\s*path:\\s*`([^`]+)`"))
                calls.Add(built.Groups[1].Success ? (built.Groups[1].Value, built.Groups[2].Value) : (built.Groups[4].Value, built.Groups[3].Value));
        }
        calls.Add(("events", "GET")); // the EventSource
        var shapes = calls
            .Select(c => (Path: Regex.Replace(c.Path, "\\$\\{[^}]*\\}", "X").Split('?')[0].TrimEnd('/'), c.Method))
            .Where(c => c.Path.Length > 0 && !c.Path.StartsWith("X"))
            .Distinct().ToList();
        Assert.True(shapes.Count >= 15, $"only {shapes.Count} API calls were found in the scripts: {string.Join(", ", shapes)}");
        foreach (var (path, method) in shapes)
            Assert.True(endpoints.Any(e => e.Pattern.IsMatch(path) && e.Methods.Contains(method)), $"the UI calls {method} {path}, which the API does not map");
        // The check has teeth: a path the server does not map is not matched.
        Assert.DoesNotContain(endpoints, e => e.Pattern.IsMatch("pipelines/X/graphs"));

        // The forms are built from these: a UI that stopped asking would be hand-written JSON again.
        foreach (var needed in new[] { "connectors", "connections", "table-definitions", "table-sources", "whoami", "plugins/X/X/definitions/X", "plugins/X/X/definitions/X/template", "plugins/X/diff" })
            Assert.Contains((needed, "GET"), shapes);
        Assert.Contains(("pipelines/validate", "POST"), shapes);
        Assert.Contains(("tables", "POST"), shapes);

        // And the read routes among them answer.
        var pluginVersion = await PluginVersionAsync();
        foreach (var path in shapes.Where(s => s.Method == "GET" && s.Path != "events").Select(s => s.Path).Distinct())
        {
            var concrete = path.StartsWith("pipelines/X") ? path.Replace("pipelines/X", "pipelines/enrich-orders")
                : path.StartsWith("tables/X") ? path.Replace("tables/X", "tables/instruments")
                : path.StartsWith("plugins/X/X/definitions/X") ? path.Replace("plugins/X/X/definitions/X", $"plugins/{DemoHost.Plugin}/{pluginVersion}/definitions/desk-risk")
                : path == "plugins/X/diff" ? $"plugins/{DemoHost.Plugin}/diff?from={pluginVersion}&to={pluginVersion}"
                : path.StartsWith("plugins/X") ? path.Replace("plugins/X", "plugins/" + DemoHost.Plugin)
                : path;
            Assert.DoesNotContain("X", concrete.Split('?')[0].Split('/'));
            var answer = await Admin.Get(concrete);
            Assert.True(answer.Code == 200, $"GET {concrete}: {answer.Code} {answer.ErrorCode}");
        }
    }
}
