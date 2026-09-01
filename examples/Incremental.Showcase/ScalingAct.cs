using System.Diagnostics;
using System.Diagnostics.Metrics;
using Incremental;
using Microsoft.Extensions.Logging;

namespace Incremental.Showcase;

/// <summary>
/// Tab 5, in-process half: the same declaration run at several worker counts, measured.
/// Sharding is not free — every GroupBy inserts an exchange, and an exchange costs a
/// barrier per step whether or not there is enough work to amortize it. This act measures
/// where that trade flips instead of asserting it.
/// </summary>
public static class ScalingAct
{
    /// <summary>Per-step latency at the showcase's live batch size, and bulk throughput,
    /// for one topology. The barrier percentages are the share of aggregate worker time
    /// spent waiting at exchange barriers rather than computing — measured separately for
    /// the live-batch and bulk sections, because barrier cost amortizes with batch size.</summary>
    public record Sample(
        string Label, int Workers, int Processes, ExchangeMode Exchange,
        double LatencyP50, double LatencyP99, double BulkRowsPerSec,
        double BarrierPctLive, double BarrierPctBulk, bool Highlight);

    // The showcase's enrichment shape, minus the parts that would dominate the timing:
    // two exchanges (group by merchant to join, group by category to aggregate).
    static (CircuitBuilder Builder, InputStream<Order> Orders, KeyedInputStream<int, Merchant> Merchants,
        Output<KeyedValue<string, FeeStats>> Stats) Declare(CircuitOptions options)
    {
        var builder = new CircuitBuilder(options);
        var orders = builder.Input<Order>("orders");
        var merchants = builder.KeyedInput<int, Merchant>("merchants", m => m.Id);
        var stats = orders
            .GroupBy(o => o.MerchantId)
            .Join(merchants.GroupBy(m => m.Id),
                (_, o, m) => new EnrichedOrder(o.Id, o.MerchantId, m.Name, m.Category, o.CustomerId,
                    "guest", o.Amount, Math.Round(o.Amount * 0.02m, 2)))
            .GroupBy(e => e.Category)
            .AggregateLinear(new FeeTakeAggregate())
            .ToOutput(bootstrap: false);
        return (builder, orders, merchants, stats);
    }

    public const int LiveBatch = 8;         // ~ what one showcase tick feeds
    public const int LatencySteps = 200;
    // Must stay under CircuitOptions.InputChannelCapacity (8192): a producer that fills the
    // bounded input channel before anything steps blocks forever — that is backpressure
    // working as designed, and it is easy to walk into when hand-feeding a bulk batch.
    public const int BulkBatch = 8_000;
    public const int BulkSteps = 10;

    /// <summary>Every in-process topology, measured back to back on this machine — the
    /// multi-worker ones in every exchange mode, so the table shows what the barrier itself
    /// costs against point-to-point publication and what streaming adds on top. The row
    /// matching the running topology is highlighted.</summary>
    public static async Task<ScalingView> MeasureAllAsync(Topology current)
    {
        var samples = new List<Sample>
        {
            await MeasureAsync("1 worker · no exchange", 1, current.Workers == 1, ExchangeMode.Barrier),
        };
        foreach (var workers in new[] { 2, 4 })
            foreach (var mode in new[] { ExchangeMode.Barrier, ExchangeMode.PointToPoint, ExchangeMode.Streaming })
                samples.Add(await MeasureAsync($"{workers} workers · {Topology.ExchangeLabel(mode)}", workers,
                    workers == current.Workers && mode == current.Exchange, mode));
        return new ScalingView([.. samples], Environment.ProcessorCount, LiveBatch, BulkBatch,
            current.Workers, current.Exchange, DebugBuild);
    }

    // `dotnet run` builds Debug by default, and Debug flatters the single-worker case: the
    // per-row work that sharding parallelizes is exactly what the optimizer speeds up, so
    // the throughput spread collapses. Say which build produced the numbers.
    public static bool DebugBuild =>
#if DEBUG
        true;
#else
        false;
#endif

    public static async Task<Sample> MeasureAsync(string label, int workers, bool highlight, ExchangeMode mode)
    {
        var rng = new Random(11);
        var merchants = Enumerable.Range(0, 24)
            .Select(i => new Merchant(100 + i, $"m{i}", $"cat{i % 6}", 1 + i % 3)).ToList();

        Order NextOrder(long id) => new(id, 1 + rng.Next(80), 100 + (int)(rng.NextDouble() * 24),
            "web", 1, Math.Round(5m + (decimal)rng.NextDouble() * 120m, 2), DateTimeOffset.UtcNow);

        var options = new CircuitOptions
        {
            Name = $"scaling-w{workers}-{Topology.ExchangeArg(mode)}",
            Workers = workers,
            AutoStep = false,
            PointToPointExchange = mode != ExchangeMode.Barrier,
            FragmentStreaming = mode == ExchangeMode.Streaming,
        };

        // The library reports cumulative per-worker barrier wait through its meter; polling
        // the gauge before and after a measured section and dividing the delta by
        // workers × step time gives the share of worker time spent synchronizing.
        var barrierSeconds = 0.0;
        using var meters = new MeterListener();
        meters.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "Incremental" && instrument.Name == "incremental.circuit.barrier_wait")
                l.EnableMeasurementEvents(instrument);
        };
        meters.SetMeasurementEventCallback<double>((_, value, tags, _) =>
        {
            foreach (var tag in tags)
                if (tag.Key == "circuit" && Equals(tag.Value, options.Name))
                {
                    barrierSeconds += value;
                    break;
                }
        });
        meters.Start();
        double BarrierWait()
        {
            barrierSeconds = 0;
            meters.RecordObservableInstruments();
            return barrierSeconds;
        }

        var (builder, ordersDef, merchantsDef, statsDef) = Declare(options);
        await using var circuit = builder.Build();
        circuit.Start();
        var orders = circuit.Handle(ordersDef);
        var merchantInput = circuit.Handle(merchantsDef);
        var stats = circuit.Handle(statsDef);

        foreach (var m in merchants) await merchantInput.UpsertAsync(m);
        await circuit.StepAsync();

        long id = 1;
        void Drain() { while (stats.Reader.TryRead(out _)) { } }

        // Warm up: first steps pay JIT and first-touch allocation, and would swamp a p50.
        for (var i = 0; i < 60; i++)
        {
            for (var j = 0; j < LiveBatch; j++) await orders.InsertAsync(NextOrder(id++));
            await circuit.StepAsync();
            Drain();
        }

        var micros = new double[LatencySteps];
        var barrierBefore = BarrierWait();
        for (var i = 0; i < LatencySteps; i++)
        {
            for (var j = 0; j < LiveBatch; j++) await orders.InsertAsync(NextOrder(id++));
            var timer = Stopwatch.StartNew();
            await circuit.StepAsync();
            timer.Stop();
            micros[i] = timer.Elapsed.TotalMicroseconds;
            Drain();
        }
        // Barrier wait accrues only inside steps (idle workers park on their Go semaphore,
        // not the barrier), so the summed step time is the right denominator.
        var barrierPctLive = 100 * (BarrierWait() - barrierBefore) / (workers * micros.Sum() / 1e6);
        Array.Sort(micros);

        barrierBefore = BarrierWait();
        var bulkTimer = Stopwatch.StartNew();
        var bulkStepTimer = new Stopwatch();
        for (var i = 0; i < BulkSteps; i++)
        {
            for (var j = 0; j < BulkBatch; j++) await orders.InsertAsync(NextOrder(id++));
            bulkStepTimer.Start();
            await circuit.StepAsync();
            bulkStepTimer.Stop();
            Drain();
        }
        bulkTimer.Stop();
        var barrierPctBulk = 100 * (BarrierWait() - barrierBefore)
            / (workers * bulkStepTimer.Elapsed.TotalSeconds);

        orders.Complete();
        merchantInput.Complete();
        await circuit.DisposeAsync();

        return new Sample(label, workers, 1, mode,
            micros[micros.Length / 2], micros[(int)(micros.Length * 0.99)],
            BulkBatch * BulkSteps / bulkTimer.Elapsed.TotalSeconds,
            barrierPctLive, barrierPctBulk, highlight);
    }

    /// <summary>The full showcase pipeline (136 nodes, 19 outputs, checkpointing on) at one
    /// worker count, fed the director's own traffic — this is the step time the header
    /// reports, not a micro-benchmark.</summary>
    public static async Task<(double P50, double P99)> MeasureShowcaseAsync(
        int workers, Incremental.Storage.IStorageProvider storage, ILoggerFactory loggerFactory)
    {
        var director = new Director(DateTimeOffset.UtcNow);
        var circuit = new ShowcaseCircuit($"measure-w{workers}", storage, loggerFactory, workers);
        try
        {
            await circuit.ReplayMasterDataAsync(director);
            await circuit.StepTimedAsync();

            var micros = new List<double>();
            for (var tick = 0; tick < 260; tick++)
            {
                director.AdvanceClock();
                await circuit.FeedAsync(director.EmitOrders(tick), director.EmitPayments(tick));
                var (step, _) = await circuit.StepTimedAsync();
                if (tick >= 60) micros.Add(step);       // discard warm-up
            }
            micros.Sort();
            return (micros[micros.Count / 2], micros[(int)(micros.Count * 0.99)]);
        }
        finally
        {
            await circuit.ShutdownAsync();
        }
    }
}
