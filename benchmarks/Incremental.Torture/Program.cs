using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using Incremental;
using Incremental.Showcase;
using Incremental.Storage;
using Incremental.Torture;

// ── The torture rig ─────────────────────────────────────────────────────────────
// Runs the showcase pipeline — the densest graph in the repo: three joins, an anti-join,
// a full outer join, linear and non-linear aggregates, distinct, top-k, tumbling/hopping/
// session windows and a recursive transitive closure — over a large live dataset at a
// configurable order rate, on N workers, and reports what it cost.
//
// Everything here is measurement scaffolding; the circuit under test is exactly the one the
// showcase demo runs.

var options = Options.Parse(args);

if (options.Processes > 1)
{
    await ClusterRun.RunAsync(options);
    return;
}

// Records drained and steps run come from the library's own meter rather than from the
// harness's bookkeeping: it is the number the circuit actually processed.
long recordsDrained = 0, stepsRun = 0;
using var meter = new MeterListener();
meter.InstrumentPublished = (instrument, listener) =>
{
    if (instrument.Meter.Name == "Incremental" &&
        instrument.Name is "incremental.circuit.records" or "incremental.circuit.steps")
        listener.EnableMeasurementEvents(instrument);
};
meter.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
{
    if (instrument.Name == "incremental.circuit.records")
        Interlocked.Add(ref recordsDrained, value);
    else
        Interlocked.Increment(ref stepsRun);
});
meter.Start();

var feed = new Feed(options);
var builder = new CircuitBuilder(new CircuitOptions
{
    Name = "torture",
    Workers = options.Workers,
    AutoStep = true,
    Storage = new InMemoryStorageProvider(),
    InputChannelCapacity = options.InputCapacity,
    MaxStepItemsPerInput = options.StepBatch,
    OutputChannelCapacity = 1024,
    PointToPointExchange = options.PointToPoint,
});
var defs = ShowcasePipeline.Declare(builder);
var circuit = builder.Build();
circuit.Start();

// Every output needs a consumer or its bounded channel becomes the bottleneck. The rig
// discards: this measures the circuit, not a downstream materializer.
var drains = new List<Task>();
void Drain<T>(Output<T> output) where T : notnull
{
    var handle = circuit.Handle(output);
    drains.Add(Task.Run(async () =>
    {
        try
        {
            await foreach (var _ in handle.Changes()) { }
        }
        catch (Exception ex) when (ex is CircuitFaultedException or OperationCanceledException)
        {
        }
    }));
}

Drain(defs.CategoryStats); Drain(defs.CategoryMin); Drain(defs.CategoryMax); Drain(defs.TopOrders);
Drain(defs.MerchantOrders); Drain(defs.MerchantRevenue); Drain(defs.MerchantBuyers);
Drain(defs.MerchantItems); Drain(defs.RegionRevenue); Drain(defs.Compliance); Drain(defs.Recon);
Drain(defs.EnrichedFeed); Drain(defs.LiveLeaders); Drain(defs.FinalLeaders);
Drain(defs.AcceptanceRate); Drain(defs.Sessions); Drain(defs.RingSizes); Drain(defs.RingExposure);
Drain(defs.RingMembership);

var web = circuit.Handle(defs.WebOrders);
var mobile = circuit.Handle(defs.MobileOrders);
var payments = circuit.Handle(defs.Payments);
var merchants = circuit.Handle(defs.Merchants);
var customers = circuit.Handle(defs.Customers);
var feeRates = circuit.Handle(defs.FeeRates);
var edges = circuit.Handle(defs.Edges);

var seedTimer = Stopwatch.StartNew();
foreach (var m in feed.Merchants) await merchants.UpsertAsync(m);
foreach (var c in feed.Customers) await customers.UpsertAsync(c);
foreach (var f in feed.FeeRates) await feeRates.UpsertAsync(f);
foreach (var e in feed.Edges) await edges.UpsertAsync(e);

long recordsSent = feed.Merchants.Length + feed.Customers.Length + feed.FeeRates.Length + feed.Edges.Length;

// The producer runs on its own thread for the whole run: seeding the live population, the
// warmup, and the measured window are one continuous stream, so nothing about the steady
// state changes at the measurement boundary.
long producerAllocated = 0, producedOrders = 0;
var stopProducer = new CancellationTokenSource();
var producer = new Thread(() => ProduceAsync().GetAwaiter().GetResult())
{
    IsBackground = true,
    Name = "torture-producer",
};

async Task ProduceAsync()
{
    var started = Stopwatch.GetTimestamp();
    long orders = 0;
    var startAllocated = GC.GetAllocatedBytesForCurrentThread();
    try
    {
        while (!stopProducer.IsCancellationRequested)
        {
            var unit = feed.NextUnit();
            await (unit.Web ? web : mobile).InsertAsync(unit.Insert).ConfigureAwait(false);
            if (unit.InsertPayment is { } inserted)
                await payments.InsertAsync(inserted).ConfigureAwait(false);
            if (unit.Retract is { } retract)
                await (retract.Channel == "web" ? web : mobile).RemoveAsync(retract).ConfigureAwait(false);
            if (unit.RetractPayment is { } retractPayment)
                await payments.RemoveAsync(retractPayment).ConfigureAwait(false);

            orders++;
            // Paced runs hold the target order rate; unpaced ones let backpressure decide.
            // Seeding the live population is never paced — it is setup, not measurement.
            if (options.Rate > 0 && orders > options.LiveOrders)
            {
                var due = (double)(orders - options.LiveOrders) / options.Rate;
                var elapsed = Stopwatch.GetElapsedTime(started).TotalSeconds;
                if (elapsed < due)
                    await Task.Delay(TimeSpan.FromSeconds(Math.Min(0.005, due - elapsed))).ConfigureAwait(false);
            }
            if (orders == options.LiveOrders)
                started = Stopwatch.GetTimestamp();   // pacing clock starts once the ring is full
        }
    }
    catch (Exception ex) when (ex is System.Threading.Channels.ChannelClosedException
                                  or OperationCanceledException or CircuitFaultedException)
    {
    }
    finally
    {
        Volatile.Write(ref producerAllocated, GC.GetAllocatedBytesForCurrentThread() - startAllocated);
        Volatile.Write(ref producedOrders, orders);
    }
}

producer.Start();

// Wait for the live population to be in the circuit before anything is measured.
while (Volatile.Read(ref recordsDrained) < recordsSent + (long)options.LiveOrders * 2)
    await Task.Delay(20);
var seedElapsed = seedTimer.Elapsed;

Console.Error.WriteLine(
    $"seeded {options.LiveOrders:N0} live orders, {options.Merchants:N0} merchants, {options.Customers:N0} customers " +
    $"in {seedElapsed.TotalSeconds:F1}s; working set {Environment.WorkingSet / (1024 * 1024):N0} MB");

await Task.Delay(TimeSpan.FromSeconds(options.WarmupSeconds));

// ── measured window ────────────────────────────────────────────────────────────
using var allocations = options.AllocationTypes ? new AllocationProfiler() : null;
allocations?.Begin();
using var gcProfile = new GcProfiler();
gcProfile.Begin();
var pause0 = GC.GetTotalPauseDuration();
var cpu0 = ThreadCpu.Take();
var records0 = Volatile.Read(ref recordsDrained);
var steps0 = Volatile.Read(ref stepsRun);
var alloc0 = GC.GetTotalAllocatedBytes(precise: false);
var gc0 = (GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2));
var wall = Stopwatch.StartNew();

await Task.Delay(TimeSpan.FromSeconds(options.Seconds));

wall.Stop();
var cpu1 = ThreadCpu.Take();
var records = Volatile.Read(ref recordsDrained) - records0;
var steps = Volatile.Read(ref stepsRun) - steps0;
var allocated = GC.GetTotalAllocatedBytes(precise: false) - alloc0;
var gc1 = (GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2));
var gcTime = gcProfile.End();
var pause = GC.GetTotalPauseDuration() - pause0;
var workingSet = Environment.WorkingSet;

var seconds = wall.Elapsed.TotalSeconds;
var workerCpu = cpu1.WorkerSeconds - cpu0.WorkerSeconds;
var processCpu = cpu1.ProcessSeconds - cpu0.ProcessSeconds;
var gcCpu = cpu1.GcSeconds - cpu0.GcSeconds;
var producerCpu = cpu1.ProducerSeconds - cpu0.ProducerSeconds;
var saturation = workerCpu / (seconds * options.Workers);

stopProducer.Cancel();
producer.Join(TimeSpan.FromSeconds(5));

Console.WriteLine();
Console.WriteLine($"workers {options.Workers}   rate {(options.Rate == 0 ? "unpaced" : options.Rate.ToString("N0") + " ord/s")}" +
                  $"   live orders {options.LiveOrders:N0}   window {seconds:F1}s");
Console.WriteLine($"  throughput      {records / seconds:N0} records/s   ({records / seconds / 3.8:N0} orders/s equivalent)");
Console.WriteLine($"  steps           {steps / seconds:N0}/s   {records / (double)Math.Max(1, steps):N0} records/step   " +
                  $"{seconds / Math.Max(1, steps) * 1e6:N0} us/step");
Console.WriteLine($"  worker CPU      {workerCpu:F2}s over {seconds:F1}s x {options.Workers} = {saturation:P0} saturated");
Console.WriteLine($"  process CPU     {processCpu:F2}s ({processCpu / seconds:F2} cores)   " +
                  $"= workers {workerCpu:F1}s + GC {gcCpu:F1}s + producer {producerCpu:F1}s + other {processCpu - workerCpu - gcCpu - producerCpu:F1}s");
Console.WriteLine($"  CPU per record  {processCpu / records * 1e6:F2} us   (workers {workerCpu / records * 1e6:F2}, " +
                  $"GC {gcCpu / records * 1e6:F2}, rest {(processCpu - workerCpu - gcCpu) / records * 1e6:F2})");
Console.WriteLine($"  allocated       {allocated / (double)records:N0} B/record   {allocated / seconds / (1024 * 1024):N0} MB/s" +
                  $"   (producer share {Volatile.Read(ref producerAllocated) / (double)Math.Max(1, Volatile.Read(ref producedOrders)):N0} B/order)");
Console.WriteLine($"  GC              gen0 {gc1.Item1 - gc0.Item1}   gen1 {gc1.Item2 - gc0.Item2}   gen2 {gc1.Item3 - gc0.Item3}" +
                  $"   working set {workingSet / (1024 * 1024):N0} MB");
Console.WriteLine($"  GC time         blocking gen0 {gcTime.Gen0} / gen1 {gcTime.Gen1} / gen2 {gcTime.Gen2Blocking} " +
                  $"(EE pause {pause.TotalSeconds:F2}s total)   background gen2 {gcTime.Background}x {gcTime.BackgroundSeconds:F2}s concurrent");
Console.WriteLine($"  gen2 triggers   {gcProfile.Gen2ReasonSummary()}");
Console.WriteLine($"  GC CPU          blocking collections (server threads) {cpu1.GcServerSeconds - cpu0.GcServerSeconds:F2}s + " +
                  $"background gen2 (BGC threads) {cpu1.GcBgcSeconds - cpu0.GcBgcSeconds:F2}s");

if (allocations is not null)
{
    Console.WriteLine();
    Console.WriteLine("  allocation by type (byte-weighted sample):");
    var sampled = allocations.End().ToList();
    var totalSampled = sampled.Sum(e => e.Bytes);
    foreach (var (type, bytes) in sampled.Take(28))
        Console.WriteLine($"    {bytes * 100.0 / totalSampled,5:F1}%  {bytes / (double)records,8:N0} B/rec  {type}");
}

if (options.Json)
    Console.WriteLine("JSON " + string.Create(CultureInfo.InvariantCulture,
        $$"""{"workers":{{options.Workers}},"rate":{{options.Rate}},"recordsPerSec":{{records / seconds:F0}},"usPerStep":{{seconds / Math.Max(1, steps) * 1e6:F1}},"saturation":{{saturation:F4}},"cpuPerRecordUs":{{processCpu / records * 1e6:F3}},"workerCpuPerRecordUs":{{workerCpu / records * 1e6:F3}},"bytesPerRecord":{{allocated / (double)records:F1}},"workingSetMB":{{workingSet / (1024 * 1024)}}}"""));

web.Complete(); mobile.Complete(); payments.Complete();
merchants.Complete(); customers.Complete(); feeRates.Complete(); edges.Complete();
await circuit.DisposeAsync();
await Task.WhenAll(drains).WaitAsync(TimeSpan.FromSeconds(10));
