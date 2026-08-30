using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Incremental.Showcase;
using Incremental.Storage;
using Incremental.Storage.Redis;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

// Child mode: this binary relaunched as cluster process 1. No UI, no web host.
if (args.Contains("--cluster-child"))
    return await ClusterAct.RunChildAsync(args);

var useRedis = args.Contains("--redis");
var withCluster = args.Contains("--cluster");
// The CI smoke path: run every tab's triggers unattended, print them, exit 0 (or 1 if a
// verified act did not verify). Explicit rather than inferred from redirected stdin — a
// server that turns into a batch job under nohup or systemd would be a trap.
var headless = args.Contains("--headless");
var port = Arg("--port") is { } p ? int.Parse(p) : 5170;
// >1 by default so every other tab really exercises the exchange path. Tab 5 measures what
// that costs and can relaunch the showcase at another count.
var workers = Arg("--workers") is { } w ? Math.Clamp(int.Parse(w), 1, 32) : 2;

var notices = new NoticeLog(40);
using var metrics = new IncrementalMetrics();
using var loggerFactory = new NoticeLoggerProvider(notices);

IStorageProvider storage;
ConnectionMultiplexer? redis = null;
string storageLabel;
if (useRedis)
{
    redis = await ConnectionMultiplexer.ConnectAsync("localhost:6379");
    storage = new RedisStorageProvider(redis, new RedisStorageOptions { KeyPrefix = $"showcase:{Guid.NewGuid():N}" });
    storageLabel = "redis checkpoints";
}
else
{
    // ICheckpointStorageProvider in memory: same checkpoint semantics, no Docker needed.
    storage = new InMemoryCheckpointStorageProvider();
    storageLabel = "in-memory checkpoints";
}

var show = new Show(storage, storageLabel, withCluster, notices, metrics, loggerFactory, workers);
var json = new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
};

if (headless)
    try { return await Headless.RunAsync(show, withCluster); }
    finally { redis?.Dispose(); }

// ── the web host ───────────────────────────────────────────────────────────────
// Microsoft.NET.Sdk.Web resolves ASP.NET Core from the shared framework, so this example
// still has no NuGet dependency beyond the library itself. No args are passed to the
// builder: the showcase's own flags (--redis, --cluster) are not configuration switches.

var builder = WebApplication.CreateBuilder();
builder.Logging.ClearProviders();                      // circuit logs go to the notice feed
builder.Logging.SetMinimumLevel(LogLevel.Warning);
builder.WebHost.UseUrls($"http://localhost:{port}");
var app = builder.Build();
app.UseDefaultFiles();
app.UseStaticFiles();

var hub = new Hub();

app.MapGet("/api/tabs", () => Results.Json(new
{
    tabs = show.Tabs.Select(t => new { t.Id, t.Title, t.Explainer, t.Code }),
    cluster = withCluster,
}, json));

// One SSE stream per browser. Every client gets its own single-slot channel that drops
// stale frames rather than blocking the driver — safe because each frame is a whole
// snapshot, so a dropped one is simply superseded by the next.
//
// The stream must also end on ApplicationStopping, not just RequestAborted: an SSE
// response never completes on its own, and Kestrel's graceful shutdown waits for in-flight
// requests — so without this, Ctrl-C hangs for as long as a browser is attached.
app.MapGet("/api/stream", async (HttpContext ctx, IHostApplicationLifetime lifetime) =>
{
    ctx.Response.Headers.ContentType = "text/event-stream";
    ctx.Response.Headers.CacheControl = "no-cache";
    ctx.Response.Headers["X-Accel-Buffering"] = "no";
    using var linked = CancellationTokenSource.CreateLinkedTokenSource(
        ctx.RequestAborted, lifetime.ApplicationStopping);
    var client = hub.Subscribe();
    try
    {
        await foreach (var frame in client.Reader.ReadAllAsync(linked.Token))
        {
            await ctx.Response.WriteAsync($"data: {frame}\n\n", linked.Token);
            await ctx.Response.Body.FlushAsync(linked.Token);
        }
    }
    catch (OperationCanceledException)
    {
        // The browser navigated away or reloaded, or the showcase is shutting down.
    }
    finally
    {
        hub.Unsubscribe(client);
    }
});

// Triggers never touch the circuit directly: they queue onto the driver loop and wait
// their turn, so the circuit is still stepped from exactly one thread.
app.MapPost("/api/trigger/{id}", async (string id) =>
{
    if (!show.TryGetBeat(id, out var beat)) return Results.NotFound();
    try
    {
        await show.FireAsync(beat);
        return Results.Ok();
    }
    catch (InvalidOperationException ex)
    {
        return Results.BadRequest(ex.Message);
    }
});

// Relaunch the showcase into a different topology. Worker count changes how state is
// sharded, so it cannot be swapped under a running circuit — the process restarts instead.
// Kestrel has released the port by the time RunAsync returns, so the successor binds the
// same one and the browser's EventSource reconnects to it on its own.
string[]? relaunchArgs = null;
app.MapPost("/api/relaunch", (int? workers, bool? cluster, IHostApplicationLifetime lifetime) =>
{
    var next = new List<string>
    {
        "--port", port.ToString(),
        "--no-open",                       // the browser that asked for this is already open
        "--workers", Math.Clamp(workers ?? show.CurrentWorkers, 1, 32).ToString(),
    };
    if (useRedis) next.Add("--redis");
    if (cluster ?? withCluster) next.Add("--cluster");
    relaunchArgs = [.. next];
    notices.Push(NoticeLevel.Info, $"relaunching: {string.Join(' ', next)}");
    lifetime.StopApplication();
    return Results.Ok();
});

app.MapPost("/api/run/{state}", async (string state) =>
{
    switch (state)
    {
        case "play": await show.SetRunningAsync(true); return Results.Ok();
        case "pause": await show.SetRunningAsync(false); return Results.Ok();
        case "step": await show.SetRunningAsync(false); await show.StepOnceAsync(); return Results.Ok();
        default: return Results.NotFound();
    }
});

var stopping = new CancellationTokenSource();
var driver = Task.Run(async () =>
{
    try
    {
        await show.RunAsync(snapshot => hub.Publish(JsonSerializer.Serialize(snapshot, json)), stopping.Token);
    }
    catch (Exception ex)
    {
        notices.Push(NoticeLevel.Bad, $"driver stopped: {ex.Message}");
        Console.Error.WriteLine(ex);
    }
});

var url = $"http://localhost:{port}";
Console.WriteLine($"Incremental showcase — a marketplace payments desk");
Console.WriteLine($"  {url}   ({storageLabel}, {workers} worker{(workers == 1 ? "" : "s")}{(withCluster ? ", cluster armed" : "")})");
Console.WriteLine("  Ctrl-C to stop.");
if (!args.Contains("--no-open")) OpenBrowser(url);

try
{
    await app.RunAsync();
}
finally
{
    await stopping.CancelAsync();
    await driver;
    await show.DisposeAsync();
    redis?.Dispose();
}

if (relaunchArgs is not null)
{
    Console.WriteLine($"  relaunching: {string.Join(' ', relaunchArgs)}");
    Process.Start(Child.Spawn(relaunchArgs));
}
return 0;

string? Arg(string name)
{
    var i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
}

static void OpenBrowser(string url)
{
    try
    {
        var opener = OperatingSystem.IsWindows() ? "explorer" : OperatingSystem.IsMacOS() ? "open" : "xdg-open";
        Process.Start(new ProcessStartInfo(opener, url) { UseShellExecute = false, RedirectStandardError = true });
    }
    catch
    {
        // Headless box, no desktop, no opener on PATH — the URL is printed above.
    }
}

/// <summary>Fans one snapshot out to every connected browser.</summary>
sealed class Hub
{
    readonly ConcurrentDictionary<Channel<string>, byte> clients = new();

    public Channel<string> Subscribe()
    {
        var client = Channel.CreateBounded<string>(
            new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropOldest, SingleReader = true });
        clients[client] = 0;
        return client;
    }

    public void Unsubscribe(Channel<string> client)
    {
        clients.TryRemove(client, out _);
        client.Writer.TryComplete();
    }

    public void Publish(string frame)
    {
        foreach (var client in clients.Keys) client.Writer.TryWrite(frame);
    }
}
