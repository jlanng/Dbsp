using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Spectre.Console;
using Spectre.Console.Rendering;
using StackExchange.Redis;
using Dbsp;
using Dbsp.Hosting;
using Dbsp.Storage.Redis;

// Live log analytics over a simulated clickstream, rendered as an in-place TUI dashboard:
//
//   page views ──▶ 5s tumbling windows ──▶ count per (window, page) ──▶ top 3 pages per window
//
// Every stage is incremental: as views stream in, the counts stream retracts stale totals,
// and the TopK stage re-ranks each window's leaderboard from those deltas alone. The
// watermark bounds state: events more than 2s late are dropped, and closed windows'
// operator state is evicted, so the pipeline runs in constant memory.
//
// This is the hosted example: a worker service on the generic host, with the circuit
// registered through Dbsp.Hosting. The other console examples build and run their
// circuit inline, which is the right shape for a script; this is the shape for a service.
//
// Run with --redis to keep all operator state in Redis (start it with `docker compose up -d`).
// Options bind from configuration, so `--Dbsp:PageAnalytics:Workers=4` works too
// (keep the GC heap count — <CircuitWorkers> in the csproj — in step if you change it).

var useRedis = args.Contains("--redis");
var builder = Host.CreateApplicationBuilder(args.Where(a => a != "--redis").ToArray());
builder.Logging.SetMinimumLevel(LogLevel.Warning);     // the dashboard owns the console

builder.Services.AddSingleton(new RunInfo(useRedis ? "redis" : "in-memory"));

// Registration order is shutdown order, reversed. The dashboard goes first so it stops
// last — after the circuit has drained — and its final frame shows the final state. The
// clickstream goes last so it stops first: a producer quits sending before the circuit
// completes its inputs.
builder.Services.AddHostedService<Dashboard>();

var analytics = builder.Services
    .AddDbsp<PageAnalytics>()
    .Configure(o => o.Workers = 2)
    .BindConfiguration("Dbsp:PageAnalytics");
if (useRedis)
{
    builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect("localhost:6379"));
    analytics.Configure((o, services) => o.Storage = new RedisStorageProvider(
        services.GetRequiredService<IConnectionMultiplexer>(),
        new RedisStorageOptions { KeyPrefix = $"loganalytics:{Guid.NewGuid():N}" }));
}

builder.Services.AddHostedService<Clickstream>();

AnsiConsole.Write(new Panel(new Rows(
        new Markup("A simulated clickstream flows through an incremental (DBSP-style) circuit:"),
        new Markup("[bold]  page views ─▶ 5s windows ─▶ count per (window, page) ─▶ top 3 per window[/]"),
        new Text(""),
        new Markup("[green]▪[/] Nothing below is ever recomputed. Each update is a [bold]delta[/]: when a count changes, the stale total is retracted and the new one asserted, and the TopK stage re-ranks each window's leaderboard from those deltas alone."),
        new Markup("[green]▪[/] The top (newest) window's bars grow live; older windows freeze once the [bold]watermark[/] (max event time seen − 2s allowed lateness, shown in the header) passes their end. Their operator state is then evicted — the pipeline could run forever in constant memory."),
        new Markup("[green]▪[/] Two worker threads process the stream in parallel; the [bold]GroupBy[/] stages hash-shard records by key (DBSP's exchange operator), so each worker owns its slice of state.")))
    .Header("[bold] what you're watching [/]")
    .Padding(1, 0)
    .BorderColor(Color.Grey));

await builder.Build().RunAsync();

/// <summary>The dataflow, as a definition class: the constructor declares it, the members
/// are what the rest of the app redeems for handles through <c>CircuitHost&lt;PageAnalytics&gt;</c>.</summary>
sealed class PageAnalytics
{
    public static readonly TimeSpan WindowSize = TimeSpan.FromSeconds(5);

    public InputStream<PageView> Views { get; }
    public Output<WindowedValue<string, long>> TopPages { get; }

    public PageAnalytics(CircuitBuilder builder)
    {
        Views = builder.Input<PageView>("views");
        TopPages = Views
            .WithEventTime(v => v.At, lateness: TimeSpan.FromSeconds(2))
            .GroupBy(v => v.Page)
            .TumblingWindow(WindowSize)
            .Count()                    // live count per (window, page); closed windows evicted
            .TopK(3, r => r.Value)      // top 3 pages within each window
            .ToOutput();
    }
}

/// <summary>What the producer and the dashboard share.</summary>
sealed class RunInfo(string storageLabel)
{
    public string StorageLabel { get; } = storageLabel;
    public Stopwatch Elapsed { get; } = Stopwatch.StartNew();
    public long EventsSent;
}

/// <summary>Simulates a skewed clickstream in real time for twenty seconds, then stops the
/// application — which drains the circuit.</summary>
sealed class Clickstream(CircuitHost<PageAnalytics> analytics, RunInfo run, IHostApplicationLifetime lifetime)
    : BackgroundService
{
    static readonly TimeSpan RunFor = TimeSpan.FromSeconds(20);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var input = analytics.Handle(d => d.Views);
        string[] pages = ["/home", "/pricing", "/docs", "/blog", "/about", "/careers", "/status"];
        var rng = new Random(42);
        try
        {
            while (run.Elapsed.Elapsed < RunFor)
            {
                var page = pages[(int)(pages.Length * rng.NextDouble() * rng.NextDouble())];
                await input.InsertAsync(new PageView(page, DateTimeOffset.UtcNow), stoppingToken);
                Interlocked.Increment(ref run.EventsSent);
                await Task.Delay(TimeSpan.FromMilliseconds(8), stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Ctrl-C: stop sending; the host drains what was sent.
        }
        lifetime.StopApplication();
    }
}

/// <summary>Redraws the dashboard in place from the host's shared in-memory view of the
/// output — the view integrates the changefeed, so a frame is just a snapshot read.</summary>
sealed class Dashboard(CircuitHost<PageAnalytics> analytics, RunInfo run) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var view = analytics.View(d => d.TopPages);
        return AnsiConsole.Live(new Markup("[grey]waiting for data…[/]"))
            .AutoClear(false)
            .StartAsync(async ctx =>
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    ctx.UpdateTarget(BuildDashboard(view.Current()));
                    try { await Task.Delay(100, stoppingToken); }
                    catch (OperationCanceledException) { }
                }
                // Stopped last, so the circuit has drained: this frame is the final state.
                ctx.UpdateTarget(BuildDashboard(view.Current()));
            });
    }

    IRenderable BuildDashboard(IReadOnlyList<WeightedValue<WindowedValue<string, long>>> integrated)
    {
        var renderables = new List<IRenderable>
        {
            new Markup(
                $"[bold]live page views[/] [grey]({run.StorageLabel} state, {analytics.Circuit.Name})[/]   " +
                $"[grey]elapsed[/] {run.Elapsed.Elapsed:mm\\:ss}   " +
                $"[grey]events[/] {Interlocked.Read(ref run.EventsSent):N0}   " +
                $"[grey]watermark[/] {(analytics.Circuit.CurrentWatermark is { } wm ? wm.ToString("HH:mm:ss") : "—")}"),
        };

        var colors = new[] { Color.Green, Color.Yellow, Color.Orange1 };
        var windows = integrated
            .GroupBy(entry => entry.Value.WindowStart)
            .OrderByDescending(group => group.Key)
            .Take(3);

        foreach (var group in windows)
        {
            var chart = new BarChart()
                .Width(64)
                .Label($"[bold]{group.Key:HH:mm:ss} – {group.Key + PageAnalytics.WindowSize:HH:mm:ss}[/]")
                .LeftAlignLabel();
            var rank = 0;
            foreach (var entry in group.OrderByDescending(e => e.Value.Value))
                chart.AddItem(entry.Value.Key, entry.Value.Value, colors[Math.Min(rank++, colors.Length - 1)]);
            renderables.Add(new Padder(chart, new Padding(0, 1, 0, 0)));
        }

        return new Rows(renderables);
    }
}

record PageView(string Page, DateTimeOffset At);
