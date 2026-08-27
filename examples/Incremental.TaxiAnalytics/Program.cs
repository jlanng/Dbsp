using System.Diagnostics;
using Parquet;
using Spectre.Console;
using Spectre.Console.Rendering;
using Incremental;

// The point of incremental computation, demonstrated on genuine big data.
//
// Millions of real NYC yellow-taxi trips flow through a join + three aggregations; then
// single-row changes stream in continuously and every result stays up to date for
// microseconds per change — while recomputing the same answers from the raw rows costs a
// full pass over all of them, every time anything changes.
//
//   trips ──▶ join zones ──▶ trips & revenue per borough
//                        └─▶ count per zone ──▶ top 5 zones per borough
//
// Usage: [--months N]   load N months of 2024 data (default 4, ≈12.7M rows; ≈50MB download each)
//        [--synthetic]  skip the download, generate similar data locally

var months = 4;
var synthetic = false;
for (var i = 0; i < args.Length; i++)
{
    if (args[i] == "--months" && i + 1 < args.Length) months = Math.Clamp(int.Parse(args[++i]), 1, 12);
    else if (args[i] == "--synthetic") synthetic = true;
}

AnsiConsole.Write(new Panel(new Rows(
        new Markup("Millions of real NYC taxi trips are pushed through an incremental circuit — a zone join feeding per-borough trip counts, revenue sums, and a top-5-zones ranking. Then single-row changes stream in and you watch what each one costs."),
        new Text(""),
        new Markup("[green]▪[/] [bold]1 · bulk load[/] — every row becomes a +1-weighted delta; the circuit builds all four views in one pass."),
        new Markup("[green]▪[/] [bold]2 · naive baseline[/] — the same views computed from the raw rows, timed. That's the cost of [italic]every[/] refresh without incrementality."),
        new Markup("[green]▪[/] [bold]3 · continuous churn[/] — single trips inserted and cancelled until you stop it, each timed against the naive baseline, with running totals of the time saved."),
        new Text(""),
        new Markup("[grey]Data: NYC TLC yellow-taxi trip records (public parquet files, cached in ./data), read with the Parquet.Net library — only the two columns used (pickup zone, total amount) are decoded.[/]")))
    .Header("[bold] incremental computation vs. big data [/]")
    .Padding(1, 0)
    .BorderColor(Color.Grey));

// ── acquire data ────────────────────────────────────────────────────────────────

var zoneRows = new List<ZoneInfo>();
var tripZones = new List<int>(4_000_000);
var tripTotals = new List<double>(4_000_000);

if (!synthetic)
{
    try
    {
        await LoadRealDataAsync();
    }
    catch (Exception ex)
    {
        AnsiConsole.MarkupLine($"[yellow]download failed ({Markup.Escape(ex.Message)}); using synthetic data[/]");
        synthetic = true;
    }
}
if (synthetic)
    GenerateSyntheticData();

var totalRows = tripZones.Count;
AnsiConsole.MarkupLine($"[grey]dataset:[/] {totalRows:N0} trips, {zoneRows.Count} zones{(synthetic ? " [grey](synthetic)[/]" : "")}");
AnsiConsole.WriteLine();

// ── build the circuit ───────────────────────────────────────────────────────────

var builder = new CircuitBuilder(new CircuitOptions
{
    AutoStep = false, // manual stepping: deterministic batches and precise timing
    Workers = 4,
    InputChannelCapacity = 1_000_000,
    MaxStepItemsPerInput = 1_000_000,
});

var trips = builder.Input<Trip>("trips");
// Zones are master data: a keyed input, so a corrected zone row would be one UpsertAsync.
var zones = builder.KeyedInput<int, ZoneInfo>("zones", z => z.Id);

var enriched = trips
    .IndexBy(t => t.Zone)
    .Join(zones.IndexBy(z => z.Id), (_, trip, zone) => (zone.Borough, zone.Name, trip.Total));

var byBorough = enriched.IndexBy(e => e.Borough);
byBorough.Count().Output("trips-per-borough");
byBorough.Sum(e => e.Total).Output("revenue-per-borough");

enriched.IndexBy(e => (e.Borough, e.Name))
    .Count()
    .IndexBy(c => c.Key.Borough)
    .TopK(5, c => c.Value)
    .Output("top-zones");

await using var circuit = builder.Build();
circuit.Start();

var tripsIn = circuit.Input<Trip>("trips");
var zonesIn = circuit.KeyedInput<int, ZoneInfo>("zones");
var countsOut = circuit.Output<KeyedValue<string, long>>("trips-per-borough");
var revenueOut = circuit.Output<KeyedValue<string, double>>("revenue-per-borough");
var topZonesOut = circuit.Output<KeyedValue<string, KeyedValue<(string Borough, string Name), long>>>("top-zones");

var countsView = new ZSet<KeyedValue<string, long>>();
var revenueView = new ZSet<KeyedValue<string, double>>();
var topZonesView = new ZSet<KeyedValue<string, KeyedValue<(string Borough, string Name), long>>>();

long DrainOutputs()
{
    long deltaRows = 0;
    while (countsOut.Reader.TryRead(out var d)) { countsView.AddAll(d); deltaRows += d.DistinctCount; }
    while (revenueOut.Reader.TryRead(out var d)) { revenueView.AddAll(d); deltaRows += d.DistinctCount; }
    while (topZonesOut.Reader.TryRead(out var d)) { topZonesView.AddAll(d); deltaRows += d.DistinctCount; }
    return deltaRows;
}

// ── 1 · bulk load ───────────────────────────────────────────────────────────────

AnsiConsole.Write(new Rule("[bold]1 · bulk load[/]").LeftJustified());
AnsiConsole.MarkupLine(
    "[grey]Trips are inserted as +1-weighted deltas and the circuit is stepped every 250k rows. " +
    "Each step pushes only that batch through join and aggregates — the join probes its integrated " +
    "state, and COUNT/SUM fold the batch into one accumulator per borough.[/]");

foreach (var zone in zoneRows)
    await zonesIn.UpsertAsync(zone);
await circuit.StepAsync();
DrainOutputs();

var distinctEntries = new HashSet<(int, long)>();
var loadTimer = Stopwatch.StartNew();
await AnsiConsole.Progress()
    .Columns(new TaskDescriptionColumn(), new ProgressBarColumn(), new PercentageColumn(), new RemainingTimeColumn())
    .StartAsync(async ctx =>
    {
        var task = ctx.AddTask("ingesting trips", maxValue: totalRows);
        const int BatchSize = 250_000;
        var pending = 0;
        for (var i = 0; i < totalRows; i++)
        {
            var trip = new Trip(tripZones[i], tripTotals[i]);
            distinctEntries.Add((trip.Zone, (long)Math.Round(trip.Total * 100)));
            await tripsIn.InsertAsync(trip);
            if (++pending == BatchSize)
            {
                await circuit.StepAsync();
                DrainOutputs();
                task.Value = i + 1;
                pending = 0;
            }
        }
        if (pending > 0)
        {
            await circuit.StepAsync();
            DrainOutputs();
        }
        task.Value = totalRows;
    });
loadTimer.Stop();

AnsiConsole.MarkupLine(
    $"[grey]loaded[/] {totalRows:N0} rows in {loadTimer.Elapsed.TotalSeconds:F1}s " +
    $"([bold]{totalRows / loadTimer.Elapsed.TotalSeconds:N0} rows/s[/], 4 workers) — " +
    $"Z-set weights compressed them to {distinctEntries.Count:N0} distinct weighted entries");
AnsiConsole.WriteLine();

RenderResults();

// ── 2 · the naive baseline ──────────────────────────────────────────────────────

AnsiConsole.Write(new Rule("[bold]2 · the naive baseline[/]").LeftJustified());
AnsiConsole.MarkupLine(
    "[grey]The same four views computed the way a batch job or dashboard query would: " +
    "one pass over every raw row, rebuilding the dictionaries from scratch. Without " +
    "incrementality this cost is paid on every refresh, however small the change.[/]");

var naiveDuration = TimeSpan.MaxValue;
for (var run = 0; run < 2; run++)
{
    var d = TimeNaiveRecompute();
    if (d < naiveDuration) naiveDuration = d;
}
AnsiConsole.MarkupLine($"  full recompute over {totalRows:N0} rows: [bold]{naiveDuration.TotalMilliseconds:N1} ms[/]");
AnsiConsole.WriteLine();

// ── 3 · continuous churn ────────────────────────────────────────────────────────

// The bulk load and the naive baseline leave a lot of garbage (and pending collection
// work) behind; take the GC hit now rather than inside the first timed update.
GC.Collect();
GC.WaitForPendingFinalizers();
GC.Collect();

// Warm the full update path (join match, aggregate retract/assert, TopK, publishing) so
// the first visible timings measure work, not first-call effects. Each warm-up trip is
// immediately retracted, so the net effect on every view is zero.
// 15 cycles (30 steps) so tiered compilation promotes the hot step path before timing.
var warmupTrip = new Trip(zoneRows[0].Id, 0.01);
for (var i = 0; i < 15; i++)
{
    await tripsIn.SendAsync(warmupTrip, +1);
    await circuit.StepAsync();
    await tripsIn.SendAsync(warmupTrip, -1);
    await circuit.StepAsync();
}
DrainOutputs();

var updateCount = 0L;
var updateTicks = 0L;

var interactive = !Console.IsInputRedirected;
AnsiConsole.Write(new Rule("[bold]3 · now change one row at a time[/]").LeftJustified());
AnsiConsole.MarkupLine(
    $"[grey]Single trips now stream in (1 in 4 cancels an earlier one) and every view stays " +
    $"current. Each change makes the join emit one delta, the affected borough's COUNT and " +
    $"SUM fold it into their accumulators, and TopK re-rank one small group — a handful of " +
    $"delta rows and microseconds of work, whatever the dataset size. " +
    $"{(interactive ? "[bold]Press any key to stop.[/]" : "Non-interactive input: stopping after 30 updates.")}[/]");

var churnRng = new Random(7);
var insertedPool = new List<Trip>();
var lastDescription = "warming up…";
var lastMicros = 0.0;
var maxChurnUpdates = interactive ? long.MaxValue : 30;
var churnApplied = 0L;

await AnsiConsole.Live(BuildLiveView()).AutoClear(false).StartAsync(async ctx =>
{
    // Let the initial frame finish rendering, then run one untimed dry-run update: the
    // first step after an idle gap pays extra for waking parked cores and cold caches,
    // and that cost belongs here rather than in the first displayed reading.
    await Task.Delay(200);
    for (var i = 0; i < 8; i++)
    {
        // Shaped exactly like the timed churn (random zones touch the big boroughs'
        // state), but inserted-then-retracted so the net effect on every view is zero.
        var dryRun = new Trip(zoneRows[churnRng.Next(zoneRows.Count)].Id, Math.Round(5 + churnRng.NextDouble() * 145, 2));
        await tripsIn.SendAsync(dryRun, +1);
        await circuit.StepAsync();
        await tripsIn.SendAsync(dryRun, -1);
        await circuit.StepAsync();
    }
    DrainOutputs();
    // Match the churn cadence before the first timed update: a step fired immediately
    // after the hot dry-run burst measures ~4× slower than one following the same idle
    // gap the real updates get (scheduler state after a burst differs from steady state).
    await Task.Delay(250);

    while (churnApplied < maxChurnUpdates)
    {
        if (interactive && Console.KeyAvailable)
        {
            Console.ReadKey(intercept: true);
            break;
        }

        Trip trip;
        long weight;
        if (insertedPool.Count > 0 && churnRng.NextDouble() < 0.25)
        {
            var index = churnRng.Next(insertedPool.Count);
            trip = insertedPool[index];
            insertedPool.RemoveAt(index);
            weight = -1;
            lastDescription = $"cancel {trip.Total:C} trip at {ZoneName(trip.Zone)}";
        }
        else
        {
            var zone = zoneRows[churnRng.Next(zoneRows.Count)];
            var amount = churnRng.NextDouble() < 0.05
                ? Math.Round(500 + churnRng.NextDouble() * 4500, 2) // the occasional surge fare
                : Math.Round(5 + churnRng.NextDouble() * 145, 2);
            trip = new Trip(zone.Id, amount);
            insertedPool.Add(trip);
            weight = +1;
            lastDescription = $"insert {amount:C} trip at {zone.Name}";
        }

        lastMicros = await ApplyUpdateAsync(trip, weight);
        churnApplied++;
        ctx.UpdateTarget(BuildLiveView());
        await Task.Delay(250);
    }
});

// ── the punchline ───────────────────────────────────────────────────────────────

var avgIncremental = TimeSpan.FromTicks(updateTicks / Math.Max(1, updateCount));
var speedup = naiveDuration.TotalMicroseconds / avgIncremental.TotalMicroseconds;
var naiveEquivalent = naiveDuration * updateCount;
var actualSpent = TimeSpan.FromTicks(updateTicks);

AnsiConsole.Write(new Panel(new Rows(
        new Markup($"bulk load                [bold]{totalRows / loadTimer.Elapsed.TotalSeconds,12:N0}[/] rows/s"),
        new Markup($"one-row update (circuit) [bold]{avgIncremental.TotalMicroseconds,12:N0}[/] µs   avg over {updateCount} updates"),
        new Markup($"one-row update (naive)   [bold]{naiveDuration.TotalMilliseconds,12:N1}[/] ms   full pass over {totalRows:N0} raw rows"),
        new Text(""),
        new Markup($"[bold green]incremental is {speedup:N0}× cheaper per update[/] — keeping these views current through " +
                   $"{updateCount} changes cost {actualSpent.TotalMilliseconds:N0} ms instead of {naiveEquivalent.TotalSeconds:N1} s, " +
                   "and the gap grows with the data.")))
    .Header("[bold] the punchline [/]")
    .Padding(1, 0)
    .BorderColor(Color.Green));

await circuit.DisposeAsync();
return;

// ────────────────────────────────────────────────────────────────────────────────

async Task<double> ApplyUpdateAsync(Trip trip, long weight)
{
    await tripsIn.SendAsync(trip, weight);
    var timer = Stopwatch.StartNew();
    await circuit.StepAsync();
    timer.Stop();
    DrainOutputs();
    updateCount++;
    updateTicks += timer.Elapsed.Ticks;
    return timer.Elapsed.TotalMicroseconds;
}

IRenderable BuildLiveView()
{
    var avgMicros = updateCount == 0 ? 0 : TimeSpan.FromTicks(updateTicks / updateCount).TotalMicroseconds;
    var savedSoFar = naiveDuration * churnApplied - TimeSpan.FromTicks(updateTicks);

    // Fixed-width slots keep every number anchored in place; the variable-length
    // description gets its own line so nothing shifts around it.
    var table = new Table().Border(TableBorder.Rounded);
    table.AddColumn(new TableColumn("borough").Width(14));
    table.AddColumn(new TableColumn("trips").RightAligned().Width(10));
    table.AddColumn(new TableColumn("revenue").RightAligned().Width(12));
    table.AddColumn(new TableColumn("busiest zone").Width(36));

    var busiest = topZonesView
        .GroupBy(e => e.Value.Key)
        .ToDictionary(
            g => g.Key,
            g => g.OrderByDescending(e => e.Value.Value.Value)
                .Select(e => $"{e.Value.Value.Key.Name} ({e.Value.Value.Value:N0})")
                .FirstOrDefault(""));

    foreach (var entry in countsView.OrderByDescending(e => e.Value.Value))
    {
        var borough = entry.Value.Key;
        var revenue = revenueView.SingleOrDefault(e => e.Value.Key == borough).Value.Value;
        table.AddRow(
            Markup.Escape(Truncate(borough, 14)),
            $"{entry.Value.Value:N0}",
            $"{revenue:C0}",
            Markup.Escape(Truncate(busiest.GetValueOrDefault(borough, ""), 36)));
    }

    return new Rows(
        new Markup(
            $"[grey]updates[/] [bold]{churnApplied,7:N0}[/]   [grey]step[/] [bold green]{lastMicros,8:N0} µs[/] " +
            $"[bold]({(lastMicros > 0 ? naiveDuration.TotalMicroseconds / lastMicros : 0),6:N0}× vs naive)[/]   " +
            $"[grey]avg[/] {avgMicros,8:N0} µs   [grey]saved:[/] [bold]{Math.Max(0, savedSoFar.TotalSeconds),7:N1} s[/]"),
        new Markup($"[grey]last:[/] {Markup.Escape(lastDescription)}"),
        table);
}

static string Truncate(string value, int width) =>
    value.Length <= width ? value : value[..(width - 1)] + "…";

string ZoneName(int zoneId) => zoneRows.FirstOrDefault(z => z.Id == zoneId)?.Name ?? $"zone {zoneId}";

TimeSpan TimeNaiveRecompute()
{
    var lookup = zoneRows.ToDictionary(z => z.Id);
    var timer = Stopwatch.StartNew();
    var counts = new Dictionary<string, long>();
    var revenue = new Dictionary<string, double>();
    var zoneCounts = new Dictionary<(string, string), long>();
    for (var i = 0; i < tripZones.Count; i++)
    {
        if (!lookup.TryGetValue(tripZones[i], out var zone))
            continue;
        counts[zone.Borough] = counts.GetValueOrDefault(zone.Borough) + 1;
        revenue[zone.Borough] = revenue.GetValueOrDefault(zone.Borough) + tripTotals[i];
        var key = (zone.Borough, zone.Name);
        zoneCounts[key] = zoneCounts.GetValueOrDefault(key) + 1;
    }
    _ = zoneCounts.GroupBy(kv => kv.Key.Item1)
        .ToDictionary(g => g.Key, g => g.OrderByDescending(kv => kv.Value).Take(5).ToList());
    timer.Stop();
    return timer.Elapsed;
}

void RenderResults()
{
    var table = new Table().Border(TableBorder.Rounded).Title("[bold]per borough[/]");
    table.AddColumn("borough");
    table.AddColumn(new TableColumn("trips").RightAligned());
    table.AddColumn(new TableColumn("revenue").RightAligned());
    table.AddColumn("top zones");

    var topByBorough = topZonesView
        .GroupBy(e => e.Value.Key)
        .ToDictionary(
            g => g.Key,
            g => string.Join(", ", g.OrderByDescending(e => e.Value.Value.Value)
                .Take(5)
                .Select(e => $"{e.Value.Value.Key.Name} ({e.Value.Value.Value:N0})")));

    foreach (var entry in countsView.OrderByDescending(e => e.Value.Value))
    {
        var borough = entry.Value.Key;
        var rev = revenueView.SingleOrDefault(e => e.Value.Key == borough).Value.Value;
        table.AddRow(
            Markup.Escape(borough),
            $"{entry.Value.Value:N0}",
            $"{rev:C0}",
            Markup.Escape(topByBorough.GetValueOrDefault(borough, "")));
    }
    AnsiConsole.Write(table);
}

async Task LoadRealDataAsync()
{
    var dataDir = Path.Combine(Environment.CurrentDirectory, "data");
    Directory.CreateDirectory(dataDir);
    using var http = new HttpClient();

    var zonesPath = Path.Combine(dataDir, "taxi_zone_lookup.csv");
    await DownloadAsync(http, "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv", zonesPath);
    foreach (var line in File.ReadLines(zonesPath).Skip(1))
    {
        var fields = SplitCsv(line);
        if (fields.Length >= 3 && int.TryParse(fields[0], out var id))
            zoneRows.Add(new ZoneInfo(id, fields[1], fields[2]));
    }

    for (var m = 1; m <= months; m++)
    {
        var name = $"yellow_tripdata_2024-{m:D2}.parquet";
        var path = Path.Combine(dataDir, name);
        await DownloadAsync(http, $"https://d37ci6vzurychx.cloudfront.net/trip-data/{name}", path);
        await ReadTripsParquetAsync(path);
    }
}

async Task DownloadAsync(HttpClient http, string url, string path)
{
    if (File.Exists(path))
        return;
    await AnsiConsole.Status().StartAsync($"downloading {Path.GetFileName(path)}…", async _ =>
    {
        var tmp = path + ".tmp";
        using (var response = await http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead))
        {
            response.EnsureSuccessStatusCode();
            await using var target = File.Create(tmp);
            await response.Content.CopyToAsync(target);
        }
        File.Move(tmp, path, overwrite: true);
    });
}

// Parquet ingestion (via the Parquet.Net library): open the file, walk its row groups, and
// decode only the two columns this demo uses — columnar storage means the other ~17 columns
// are never even read off disk.
async Task ReadTripsParquetAsync(string path)
{
    using var reader = await ParquetReader.CreateAsync(path);
    var zoneField = reader.Schema.DataFields.First(f => f.Name == "PULocationID");
    var totalField = reader.Schema.DataFields.First(f => f.Name == "total_amount");
    for (var g = 0; g < reader.RowGroupCount; g++)
    {
        using var rowGroup = reader.OpenRowGroupReader(g);
        var zoneData = (await rowGroup.ReadColumnAsync(zoneField)).Data;
        var totalData = (await rowGroup.ReadColumnAsync(totalField)).Data;
        var count = zoneData.Length;
        for (var i = 0; i < count; i++)
        {
            tripZones.Add(ToInt(zoneData.GetValue(i)));
            tripTotals.Add(ToDouble(totalData.GetValue(i)));
        }
    }

    static int ToInt(object? v) => v switch { int i => i, long l => (int)l, short s => s, null => 0, _ => Convert.ToInt32(v) };
    static double ToDouble(object? v) => v switch { double d => d, float f => f, decimal m => (double)m, null => 0, _ => Convert.ToDouble(v) };
}

void GenerateSyntheticData()
{
    string[] boroughs = ["Manhattan", "Queens", "Brooklyn", "Bronx", "Staten Island", "EWR"];
    for (var id = 1; id <= 265; id++)
        zoneRows.Add(new ZoneInfo(id, boroughs[id % boroughs.Length], id == 1 ? "Newark Airport" : id == 132 ? "JFK Airport" : $"Zone {id}"));

    var rng = new Random(42);
    for (var i = 0; i < 3_000_000; i++)
    {
        tripZones.Add((int)(265 * rng.NextDouble() * rng.NextDouble()) + 1);
        tripTotals.Add(Math.Round(5 + rng.NextDouble() * rng.NextDouble() * 200, 2));
    }
}

static string[] SplitCsv(string line)
{
    var fields = new List<string>();
    var current = new System.Text.StringBuilder();
    var inQuotes = false;
    foreach (var c in line)
    {
        if (c == '"')
            inQuotes = !inQuotes;
        else if (c == ',' && !inQuotes)
        {
            fields.Add(current.ToString());
            current.Clear();
        }
        else
            current.Append(c);
    }
    fields.Add(current.ToString());
    return [.. fields];
}

record ZoneInfo(int Id, string Borough, string Name);

readonly record struct Trip(int Zone, double Total);
