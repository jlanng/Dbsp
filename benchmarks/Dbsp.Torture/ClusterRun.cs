using System.Diagnostics;
using System.Diagnostics.Metrics;
using Dbsp.Cluster;
using Dbsp.Showcase;
using Dbsp.Storage;

namespace Dbsp.Torture;

/// <summary>
/// Cluster mode: N co-hosted "processes" wired by <see cref="InMemoryExchangeTransport"/> and
/// stepped in lockstep. The transport is in-process on purpose — it removes the socket and
/// leaves exactly the thing worth measuring on the cross-process path, which is what every
/// exchange does to get a shard from one process to another: serialize it, frame it,
/// deserialize it. A TCP mesh adds the copy to and from the socket, not a different shape.
/// </summary>
internal static class ClusterRun
{
    public static async Task RunAsync(Options options)
    {
        long bytesSent = 0, framesSent = 0, recordsDrained = 0;
        using var meter = new MeterListener();
        meter.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "Dbsp" &&
                instrument.Name is "dbsp.cluster.bytes_sent" or "dbsp.cluster.frames_sent"
                    or "dbsp.circuit.records")
                listener.EnableMeasurementEvents(instrument);
        };
        meter.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            switch (instrument.Name)
            {
                case "dbsp.cluster.bytes_sent": Interlocked.Add(ref bytesSent, value); break;
                case "dbsp.cluster.frames_sent": Interlocked.Add(ref framesSent, value); break;
                default: Interlocked.Add(ref recordsDrained, value); break;
            }
        });
        meter.Start();

        var processes = options.Processes;
        var transports = InMemoryExchangeTransport.CreateMesh(processes);
        var circuits = new Circuit[processes];
        var defs = new ClusterDefs[processes];
        var orders = new InputHandle<Order>[processes];
        var merchants = new KeyedInputHandle<int, Merchant>[processes];
        var feeRates = new KeyedInputHandle<int, FeeRate>[processes];
        var drains = new List<Task>();

        for (var p = 0; p < processes; p++)
        {
            var builder = new CircuitBuilder(new CircuitOptions
            {
                Name = $"torture-p{p}",
                Workers = options.Workers,
                AutoStep = false,                    // clustered circuits step in lockstep
                Storage = new InMemoryStorageProvider(),
                InputChannelCapacity = options.InputCapacity,
                MaxStepItemsPerInput = options.StepBatch,
                OutputChannelCapacity = 1024,
                Cluster = new ClusterOptions { Transport = transports[p] },
            });
            defs[p] = ClusterPipeline.Declare(builder);
            circuits[p] = builder.Build();
            circuits[p].Start();

            orders[p] = circuits[p].Handle(defs[p].Orders);
            merchants[p] = circuits[p].Handle(defs[p].Merchants);
            feeRates[p] = circuits[p].Handle(defs[p].FeeRates);
            Drain(circuits[p], defs[p].CategoryStats);
            Drain(circuits[p], defs[p].MerchantOrders);
            Drain(circuits[p], defs[p].MerchantRevenue);
            Drain(circuits[p], defs[p].CategoryMax);
            Drain(circuits[p], defs[p].TopOrders);
            Drain(circuits[p], defs[p].EnrichedFeed);
        }

        void Drain<T>(Circuit circuit, Output<T> output) where T : notnull
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

        // Master data is fed once, by process 0. The exchange routes each row to whichever
        // global worker owns its key regardless of which process fed it, so feeding it in
        // every process would duplicate it.
        var feed = new Feed(options);
        foreach (var m in feed.Merchants) await merchants[0].UpsertAsync(m);
        foreach (var f in feed.FeeRates) await feeRates[0].UpsertAsync(f);
        await StepAllAsync();

        // Order traffic is split across the processes, as a partitioned source would be.
        var perStep = options.StepBatch;
        var stopwatch = new Stopwatch();
        var warmupUntil = TimeSpan.FromSeconds(options.WarmupSeconds);
        var runUntil = warmupUntil + TimeSpan.FromSeconds(options.Seconds);

        long records0 = 0, bytes0 = 0, frames0 = 0;
        var cpu0 = default(ThreadCpu.Snapshot);
        var measuring = false;
        var measureStart = 0L;

        stopwatch.Start();
        while (stopwatch.Elapsed < runUntil)
        {
            for (var i = 0; i < perStep; i++)
            {
                var unit = feed.NextUnit();
                var p = i % processes;
                await orders[p].InsertAsync(unit.Insert);
                if (unit.Retract is { } retract)
                    await orders[p].RemoveAsync(retract);
            }
            await StepAllAsync();

            if (!measuring && stopwatch.Elapsed >= warmupUntil)
            {
                (records0, bytes0, frames0) = (Volatile.Read(ref recordsDrained), Volatile.Read(ref bytesSent), Volatile.Read(ref framesSent));
                cpu0 = ThreadCpu.Take();
                measureStart = Stopwatch.GetTimestamp();
                measuring = true;
            }
        }

        var elapsed = Stopwatch.GetElapsedTime(measureStart).TotalSeconds;
        var cpu1 = ThreadCpu.Take();
        var records = Volatile.Read(ref recordsDrained) - records0;
        var bytes = Volatile.Read(ref bytesSent) - bytes0;
        var frames = Volatile.Read(ref framesSent) - frames0;
        var processCpu = cpu1.ProcessSeconds - cpu0.ProcessSeconds;
        var workerCpu = cpu1.WorkerSeconds - cpu0.WorkerSeconds;
        var gcCpu = cpu1.GcSeconds - cpu0.GcSeconds;

        Console.WriteLine();
        Console.WriteLine($"cluster {processes} processes x {options.Workers} workers   window {elapsed:F1}s");
        Console.WriteLine($"  throughput      {records / elapsed:N0} records/s");
        Console.WriteLine($"  CPU per record  {processCpu / records * 1e6:F2} us   " +
                          $"(workers {workerCpu / records * 1e6:F2}, GC {gcCpu / records * 1e6:F2})");
        Console.WriteLine($"  exchange wire   {bytes / (double)records:N0} B/record   " +
                          $"{bytes / elapsed / (1024 * 1024):N1} MB/s over {frames / elapsed:N0} frames/s " +
                          $"({bytes / (double)Math.Max(1, frames):N0} B/frame)");

        foreach (var handle in orders) handle.Complete();
        foreach (var handle in merchants) handle.Complete();
        foreach (var handle in feeRates) handle.Complete();
        foreach (var circuit in circuits) await circuit.DisposeAsync();
        await Task.WhenAll(drains).WaitAsync(TimeSpan.FromSeconds(10));

        // Every process must run every step, and a step blocks at its first exchange until
        // all of them do — so they are launched together and awaited together.
        Task StepAllAsync()
        {
            var steps = new Task[processes];
            for (var p = 0; p < processes; p++)
                steps[p] = circuits[p].StepAsync();
            return Task.WhenAll(steps);
        }
    }
}
