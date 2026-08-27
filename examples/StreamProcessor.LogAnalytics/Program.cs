using StackExchange.Redis;
using StreamProcessor;
using StreamProcessor.Storage;
using StreamProcessor.Storage.Redis;

// Live log analytics over a simulated clickstream:
//
//   page views ──▶ 5s tumbling windows ──▶ count per (window, page) ──▶ top 3 pages per window
//
// Every stage is incremental: as views stream in, the counts stream retracts stale totals,
// and the TopK stage re-ranks each window's leaderboard from those deltas alone.
//
// Run with --redis to keep all operator state in Redis (start it with `docker compose up -d`).

IStorageProvider storage = new InMemoryStorageProvider();
ConnectionMultiplexer? redis = null;
if (args.Contains("--redis"))
{
    redis = await ConnectionMultiplexer.ConnectAsync("localhost:6379");
    storage = new RedisStorageProvider(redis, new RedisStorageOptions
    {
        KeyPrefix = $"loganalytics:{Guid.NewGuid():N}",
    });
    Console.WriteLine("Operator state: Redis (localhost:6379)");
}
else
{
    Console.WriteLine("Operator state: in-memory (pass --redis to use Redis)");
}

var builder = new CircuitBuilder(new CircuitOptions { Workers = 2, Storage = storage });

var windowSize = TimeSpan.FromSeconds(5);
var views = builder.Input<PageView>("views");

// The watermark bounds state: events more than 2s late are dropped, and once the watermark
// passes a window's end, both the per-page counts and the leaderboard state for that window
// are evicted — this pipeline can run forever in constant memory.
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

// Integrate the output deltas and reprint a window's leaderboard whenever it changes.
var renderer = Task.Run(async () =>
{
    var integrated = new ZSet<KeyedValue<DateTimeOffset, KeyedValue<(DateTimeOffset Window, string Page), long>>>();
    await foreach (var delta in topPages.ReadAllAsync())
    {
        integrated.AddAll(delta);
        foreach (var window in delta.Select(entry => entry.Value.Key).Distinct().Order())
        {
            var board = integrated
                .Where(entry => entry.Value.Key == window)
                .OrderByDescending(entry => entry.Value.Value.Value)
                .Select((entry, rank) => $"{rank + 1}. {entry.Value.Value.Key.Page} ({entry.Value.Value.Value})");
            Console.WriteLine($"{window:HH:mm:ss} | {string.Join("   ", board)}");
        }
    }
});

// Simulate a skewed clickstream in real time for ~12 seconds.
string[] pages = ["/home", "/pricing", "/docs", "/blog", "/about", "/careers", "/status"];
var rng = new Random(42);
var until = DateTimeOffset.UtcNow.AddSeconds(12);
while (DateTimeOffset.UtcNow < until)
{
    var page = pages[(int)(pages.Length * rng.NextDouble() * rng.NextDouble())];
    await input.InsertAsync(new PageView(page, DateTimeOffset.UtcNow));
    await Task.Delay(TimeSpan.FromMilliseconds(10));
}

input.Complete();
await renderer;
await circuit.DisposeAsync();
redis?.Dispose();

record PageView(string Page, DateTimeOffset At);
