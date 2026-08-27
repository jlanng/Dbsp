using System.Diagnostics;
using Spectre.Console;
using Spectre.Console.Rendering;
using StackExchange.Redis;
using Incremental;
using Incremental.Storage;
using Incremental.Storage.Redis;

// Live log analytics over a simulated clickstream, rendered as an in-place TUI dashboard:
//
//   page views ──▶ 5s tumbling windows ──▶ count per (window, page) ──▶ top 3 pages per window
//
// Every stage is incremental: as views stream in, the counts stream retracts stale totals,
// and the TopK stage re-ranks each window's leaderboard from those deltas alone. The
// watermark bounds state: events more than 2s late are dropped, and closed windows'
// operator state is evicted, so the pipeline runs in constant memory.
//
// Run with --redis to keep all operator state in Redis (start it with `docker compose up -d`).

IStorageProvider storage = new InMemoryStorageProvider();
ConnectionMultiplexer? redis = null;
var storageLabel = "in-memory";
if (args.Contains("--redis"))
{
    redis = await ConnectionMultiplexer.ConnectAsync("localhost:6379");
    storage = new RedisStorageProvider(redis, new RedisStorageOptions
    {
        KeyPrefix = $"loganalytics:{Guid.NewGuid():N}",
    });
    storageLabel = "redis";
}

var builder = new CircuitBuilder(new CircuitOptions { Workers = 2, Storage = storage });

var windowSize = TimeSpan.FromSeconds(5);
var views = builder.Input<PageView>("views");

var viewsPerPage = views
    .WithWatermark(v => v.At, allowedLateness: TimeSpan.FromSeconds(2))
    .TumblingWindow(windowSize, v => v.At)
    .IndexBy(w => (Window: w.WindowStart, Page: w.Value.Page))
    .WithEviction(key => key.Window + windowSize)
    .Count();

viewsPerPage
    .IndexBy(count => count.Key.Window)
    .WithEviction(window => window + windowSize)
    .TopK(3, count => count.Value)
    .Output("top-pages");

await using var circuit = builder.Build();
circuit.Start();

var input = circuit.Input<PageView>("views");
var topPages = circuit.Output<KeyedValue<DateTimeOffset, KeyedValue<(DateTimeOffset Window, string Page), long>>>("top-pages");

AnsiConsole.Write(new Panel(new Rows(
        new Markup("A simulated clickstream flows through an incremental (DBSP-style) circuit:"),
        new Markup("[bold]  page views ─▶ 5s windows ─▶ count per (window, page) ─▶ top 3 per window[/]"),
        new Text(""),
        new Markup("[green]▪[/] Nothing below is ever recomputed. Each update is a [bold]delta[/]: when a count changes, the stale total is retracted and the new one asserted, and the TopK stage re-ranks each window's leaderboard from those deltas alone."),
        new Markup("[green]▪[/] The top (newest) window's bars grow live; older windows freeze once the [bold]watermark[/] (max event time seen − 2s allowed lateness, shown in the header) passes their end. Their operator state is then evicted — the pipeline could run forever in constant memory."),
        new Markup("[green]▪[/] Two worker threads process the stream in parallel; the [bold]IndexBy[/] stages hash-shard records by key (DBSP's exchange operator), so each worker owns its slice of state.")))
    .Header("[bold] what you're watching [/]")
    .Padding(1, 0)
    .BorderColor(Color.Grey));

var runFor = TimeSpan.FromSeconds(20);
var stopwatch = Stopwatch.StartNew();
long eventsSent = 0;

// Renderer: integrate the output deltas and redraw the dashboard in place.
var renderer = Task.Run(async () =>
{
    var integrated = new ZSet<KeyedValue<DateTimeOffset, KeyedValue<(DateTimeOffset Window, string Page), long>>>();
    await AnsiConsole.Live(new Markup("[grey]waiting for data…[/]"))
        .AutoClear(false)
        .StartAsync(async ctx =>
        {
            var lastRefresh = 0L;
            await foreach (var delta in topPages.ReadAllAsync())
            {
                integrated.AddAll(delta);
                if (stopwatch.ElapsedMilliseconds - lastRefresh < 100)
                    continue;
                lastRefresh = stopwatch.ElapsedMilliseconds;
                ctx.UpdateTarget(BuildDashboard(integrated));
            }
            ctx.UpdateTarget(BuildDashboard(integrated));
        });
});

// Simulate a skewed clickstream in real time.
string[] pages = ["/home", "/pricing", "/docs", "/blog", "/about", "/careers", "/status"];
var rng = new Random(42);
while (stopwatch.Elapsed < runFor)
{
    var page = pages[(int)(pages.Length * rng.NextDouble() * rng.NextDouble())];
    await input.InsertAsync(new PageView(page, DateTimeOffset.UtcNow));
    Interlocked.Increment(ref eventsSent);
    await Task.Delay(TimeSpan.FromMilliseconds(8));
}

input.Complete();
await renderer;
await circuit.DisposeAsync();
redis?.Dispose();

IRenderable BuildDashboard(ZSet<KeyedValue<DateTimeOffset, KeyedValue<(DateTimeOffset Window, string Page), long>>> integrated)
{
    var renderables = new List<IRenderable>
    {
        new Markup(
            $"[bold]live page views[/] [grey]({storageLabel} state, 2 workers)[/]   " +
            $"[grey]elapsed[/] {stopwatch.Elapsed:mm\\:ss}   " +
            $"[grey]events[/] {Interlocked.Read(ref eventsSent):N0}   " +
            $"[grey]watermark[/] {(circuit.CurrentWatermark is { } wm ? wm.ToString("HH:mm:ss") : "—")}"),
    };

    var colors = new[] { Color.Green, Color.Yellow, Color.Orange1 };
    var windows = integrated
        .GroupBy(entry => entry.Value.Key)
        .OrderByDescending(group => group.Key)
        .Take(3);

    foreach (var group in windows)
    {
        var chart = new BarChart()
            .Width(64)
            .Label($"[bold]{group.Key:HH:mm:ss} – {group.Key + windowSize:HH:mm:ss}[/]")
            .LeftAlignLabel();
        var rank = 0;
        foreach (var entry in group.OrderByDescending(e => e.Value.Value.Value))
            chart.AddItem(entry.Value.Value.Key.Page, entry.Value.Value.Value, colors[Math.Min(rank++, colors.Length - 1)]);
        renderables.Add(new Padder(chart, new Padding(0, 1, 0, 0)));
    }

    return new Rows(renderables);
}

record PageView(string Page, DateTimeOffset At);
