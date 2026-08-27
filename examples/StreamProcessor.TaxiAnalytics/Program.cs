using System.Diagnostics;
using Parquet;
using Spectre.Console;
using StreamProcessor;

// The point of incremental computation, demonstrated on genuine big data.
//
// Millions of real NYC yellow-taxi trips flow through a join + three aggregations. Then a
// SINGLE trip is inserted (and another cancelled) and the circuit updates every result in
// microseconds — while recomputing the same answers from the raw rows takes a full pass
// over all of them, every time anything changes.
//
//   trips ──▶ join zones ──▶ trips & revenue per borough
//                        └─▶ count per zone ──▶ top 5 zones per borough
//
// Usage: [--months N]   load N months of 2024 data (default 1, ≈3M rows / ≈50MB each)
//        [--synthetic]  skip the download, generate similar data locally

var months = 1;
var synthetic = false;
for (var i = 0; i < args.Length; i++)
{
    if (args[i] == "--months" && i + 1 < args.Length) months = Math.Clamp(int.Parse(args[++i]), 1, 12);
    else if (args[i] == "--synthetic") synthetic = true;
}

AnsiConsole.Write(new Panel(new Rows(
        new Markup("Millions of real NYC taxi trips are pushed through an incremental circuit — a zone join feeding per-borough trip counts, revenue sums, and a top-5-zones ranking."),
        new Markup("Then [bold]one trip is inserted and one cancelled[/]. Watch the cost: the circuit touches only what changed, while a from-scratch recompute has to scan every raw row again."),
        new Markup("[grey]COUNT/SUM use linear aggregates: their state is one accumulator per borough — not the rows — so update cost is independent of data volume.[/]")))
    .Header("[bold] incremental computation vs. big data [/]")
    .Padding(1, 0)
    .BorderColor(Color.Grey));

// ── 1. Acquire data ─────────────────────────────────────────────────────────────

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

// ── 2. Build the circuit ────────────────────────────────────────────────────────

var builder = new CircuitBuilder(new CircuitOptions
{
    AutoStep = false, // manual stepping: deterministic batches and precise timing
    Workers = 4,
    InputChannelCapacity = 1_000_000,
    MaxStepItemsPerInput = 1_000_000,
});

var trips = builder.Input<Trip>("trips");
var zones = builder.Input<ZoneInfo>("zones");

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
var zonesIn = circuit.Input<ZoneInfo>("zones");
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

// ── 3. Bulk load ────────────────────────────────────────────────────────────────

await zonesIn.SendAsync(zoneRows.Select(z => new WeightedValue<ZoneInfo>(z, 1)));
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

// ── 4. The naive baseline: what every update costs without incrementality ───────

var naiveDuration = TimeSpan.MaxValue;
for (var run = 0; run < 2; run++)
{
    var d = TimeNaiveRecompute();
    if (d < naiveDuration) naiveDuration = d;
}

// ── 5. Single-row updates through the circuit ───────────────────────────────────

// Warm the step path so timings measure work, not first-call effects.
for (var i = 0; i < 3; i++)
    await circuit.StepAsync();

AnsiConsole.Write(new Rule("[bold]now change one row[/]").LeftJustified());

var jfk = zoneRows.First(z => z.Name.Contains("JFK")).Id;
var updateTimings = new List<TimeSpan>
{
    await ApplyUpdateAsync("insert a $10,000 trip at JFK", new Trip(jfk, 10_000.00), +1),
    await ApplyUpdateAsync("the ride is cancelled (retract it)", new Trip(jfk, 10_000.00), -1),
    await ApplyUpdateAsync("insert a $25.50 trip at Newark Airport", new Trip(1, 25.50), +1),
};

// ── 6. The comparison ───────────────────────────────────────────────────────────

var avgIncremental = TimeSpan.FromTicks((long)updateTimings.Average(t => t.Ticks));
var speedup = naiveDuration.TotalMicroseconds / avgIncremental.TotalMicroseconds;

AnsiConsole.Write(new Panel(new Rows(
        new Markup($"bulk load               [bold]{totalRows / loadTimer.Elapsed.TotalSeconds,12:N0}[/] rows/s"),
        new Markup($"one-row update (circuit) [bold]{avgIncremental.TotalMicroseconds,11:N0}[/] µs   touches only the affected borough"),
        new Markup($"one-row update (naive)   [bold]{naiveDuration.TotalMilliseconds,11:N1}[/] ms   full pass over {totalRows:N0} raw rows"),
        new Text(""),
        new Markup($"[bold green]incremental is {speedup:N0}× cheaper per update[/] — and the gap grows with the data.")))
    .Header("[bold] the punchline [/]")
    .Padding(1, 0)
    .BorderColor(Color.Green));

await circuit.DisposeAsync();
return;

// ────────────────────────────────────────────────────────────────────────────────

async Task<TimeSpan> ApplyUpdateAsync(string title, Trip trip, long weight)
{
    await tripsIn.SendAsync(trip, weight);
    var timer = Stopwatch.StartNew();
    await circuit.StepAsync();
    timer.Stop();
    var deltaRows = DrainOutputs();

    var borough = zoneRows.First(z => z.Id == trip.Zone).Borough;
    var tripsNow = countsView.Single(e => e.Value.Key == borough).Value.Value;
    var revenueNow = revenueView.Single(e => e.Value.Key == borough).Value.Value;
    AnsiConsole.MarkupLine(
        $"  [bold]{Markup.Escape(title)}[/] → step took [bold green]{timer.Elapsed.TotalMicroseconds:N0} µs[/], " +
        $"{deltaRows} delta rows emitted; {borough} is now {tripsNow:N0} trips / {revenueNow:C0}");
    return timer.Elapsed;
}

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
