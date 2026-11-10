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

        var byKey = await Admin.Get("tables/instruments/rows?key=42");
        Assert.Equal("SYM00042", byKey["rows"][0].GetProperty("row").GetProperty("symbol").GetString());
        Assert.Equal(0, (await Admin.Get("tables/instruments/rows?key=9999"))["rows"].GetArrayLength());
        var byIndex = await Admin.Get("tables/instruments/rows?index=isin&key=GB0000000042");
        Assert.Equal(42, byIndex["rows"][0].GetProperty("key").GetInt32());
        var byCountry = await Admin.Get("tables/counterparties/rows?index=country&key=GB&limit=3");
        Assert.Equal(3, byCountry["rows"].GetArrayLength());
        Assert.True(byCountry["more"].GetBoolean());
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

        // Nothing anywhere in what the API returns quotes the offending value.
        await TestService.Eventually(() => admin.Node.Node.Pipelines.Get("checked-orders").State == PipelineState.Failed, "the checked pipeline failed");
        Assert.Contains((await admin.Get("events/recent?limit=200"))["items"].EnumerateArray(),
            e => e.GetProperty("subject").GetString() == "checked-orders" && e.GetProperty("message").GetString() == "Failed");
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
        // It has run no definition, so the graph is the declared edges.
        var pipeline = await admin.SendAsync(HttpMethod.Get, "pipelines/enrich-orders", baseUrl: standby);
        Assert.Equal("Unbuilt", pipeline["pipeline"].GetProperty("state").GetString());
        Assert.False(pipeline["hasGraph"].GetBoolean());
        var graph = await admin.SendAsync(HttpMethod.Get, "pipelines/enrich-orders/graph", baseUrl: standby);
        Assert.Equal("declared", graph["source"].GetString());
        Assert.Equal(["circuit", "input", "sink", "table", "table"], graph["nodes"].EnumerateArray().Select(n => n.GetProperty("role").GetString()).Order());

        // A mutation there is refused with where to send it.
        var refused = await admin.SendAsync(HttpMethod.Post, "pipelines/trader-activity/stop", baseUrl: standby);
        refused.IsError(409, "not-leader");
        Assert.Equal("node-a", refused.Body.GetProperty("error").GetProperty("leader").GetProperty("nodeId").GetString());
        Assert.Equal("http://leader.example/admin/", refused.Body.GetProperty("error").GetProperty("leader").GetProperty("adminUrl").GetString());
        (await admin.SendAsync(HttpMethod.Post, "node/stepdown?confirm=node-b", baseUrl: standby)).IsError(409, "conflict");

        // Except takeover, which is asked of the standby.
        var plan = await admin.SendAsync(HttpMethod.Post, "node/takeover?dryRun=true", baseUrl: standby);
        Assert.Equal(2, plan["plan"].GetProperty("steps").GetArrayLength());
        Assert.Equal(200, (await admin.SendAsync(HttpMethod.Post, "node/takeover?confirm=node-b", baseUrl: standby)).Code);
        await TestService.Eventually(() => admin.Host.Nodes[1].Node.IsLeader, "node-b leads");
        var after = await admin.Get("node");
        Assert.Equal("Fenced", after["status"].GetProperty("leadership").GetProperty("role").GetString());
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
            // api('…') and all('…') with a literal or template first argument, and — where
            // the path is built first — the `path` variables of the action dialogs.
            foreach (Match call in Regex.Matches(script, "\\b(?:api|all)\\(\\s*(['`])((?:(?!\\1).)*)\\1([^;]*)"))
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

        // And the read routes among them answer.
        foreach (var path in shapes.Where(s => s.Method == "GET" && s.Path != "events").Select(s => s.Path).Distinct())
        {
            var concrete = path.StartsWith("pipelines/X") ? path.Replace("pipelines/X", "pipelines/enrich-orders")
                : path.StartsWith("tables/X") ? path.Replace("tables/X", "tables/instruments")
                : path.StartsWith("plugins/X") ? path.Replace("plugins/X", "plugins/" + DemoHost.Plugin)
                : path;
            Assert.DoesNotContain("X", concrete.Split('/'));
            var answer = await Admin.Get(concrete);
            Assert.True(answer.Code == 200, $"GET {concrete}: {answer.Code} {answer.ErrorCode}");
        }
    }
}
