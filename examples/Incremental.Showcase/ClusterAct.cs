using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using Incremental;
using Incremental.Cluster;

namespace Incremental.Showcase;

/// <summary>
/// Act 5: the same pipeline on two real OS processes, exchanging shards over TCP.
/// This binary relaunches itself as process 1 (<c>--cluster-child</c>); both processes
/// build the identical circuit and call StepAsync in lockstep — the cluster contract.
/// `Recurse` is rejected in clustered circuits, so this act runs its own small pipeline.
/// </summary>
public static class ClusterAct
{
    public const int Steps = 4;
    public const int OrderCount = 1200;

    public record ShardRow(int MerchantId, decimal Revenue, long Weight);

    static (CircuitBuilder Builder, InputStream<Order> Orders, Output<KeyedValue<int, decimal>> Revenue)
        Declare(CircuitOptions options)
    {
        // Identical on every process; only CircuitOptions.Cluster differs (its process index).
        var builder = new CircuitBuilder(options);
        var orders = builder.Input<Order>("orders");
        var revenue = orders.GroupBy(o => o.MerchantId).Sum(o => o.Amount).ToOutput(bootstrap: false);
        return (builder, orders, revenue);
    }

    static CircuitOptions Options(int processIndex, string[] endpoints) => new()
    {
        Name = $"showcase-cluster-p{processIndex}",
        Workers = 2,
        AutoStep = false,                  // clustered circuits step explicitly, in lockstep
        ExchangeTimeout = TimeSpan.FromSeconds(30),
        Cluster = new ClusterOptions
        {
            Transport = new TcpExchangeTransport(processIndex, endpoints),
        },
    };

    /// <summary>Deterministic order set; both the feed and the expected totals come from here.</summary>
    public static List<Order> DemoOrders()
    {
        var rng = new Random(7);
        var orders = new List<Order>(OrderCount);
        var at = DateTimeOffset.UtcNow;
        for (var i = 0; i < OrderCount; i++)
        {
            var merchant = 100 + (int)(rng.NextDouble() * rng.NextDouble() * 24);
            orders.Add(new Order(9_000_000 + i, 1 + rng.Next(80), merchant, "web", 1,
                Math.Round(5m + (decimal)rng.NextDouble() * 120m, 2), at));
        }
        return orders;
    }

    // ── the child process (no UI; writes its shard as JSON and exits) ──────────

    public static async Task<int> RunChildAsync(string[] args)
    {
        var arguments = new Dictionary<string, string>();
        for (var i = 0; i + 1 < args.Length; i++)
            if (args[i].StartsWith("--", StringComparison.Ordinal))
                arguments[args[i]] = args[i + 1];
        var index = int.Parse(arguments["--index"]);
        var endpoints = arguments["--endpoints"].Split(',');
        var resultPath = arguments["--result"];

        var (builder, _, revenueDef) = Declare(Options(index, endpoints));
        await using var circuit = builder.Build();
        circuit.Start();
        var revenue = circuit.Handle(revenueDef);

        var shard = new ZSet<KeyedValue<int, decimal>>();
        for (var step = 0; step < Steps; step++)
        {
            await circuit.StepAsync();     // lockstep with process 0; feeds nothing itself
            while (revenue.Reader.TryRead(out var batch)) shard.AddAll(batch);
        }
        await circuit.DisposeAsync();
        while (revenue.Reader.TryRead(out var batch)) shard.AddAll(batch);

        var rows = shard.Select(e => new ShardRow(e.Value.Key, e.Value.Value, e.Weight)).ToList();
        await File.WriteAllTextAsync(resultPath, JsonSerializer.Serialize(rows));
        return 0;
    }

    // ── the parent (process 0): spawn, feed, lockstep, merge, verify ───────────

    /// <summary>Runs the two-process verification and returns the report (the caller shows
    /// it inside the cluster tab — no direct console writes).</summary>
    public static async Task<ClusterView> RunParentAsync(NoticeLog notices, IncrementalMetrics metrics)
    {
        var endpoints = new[] { $"127.0.0.1:{FreePort()}", $"127.0.0.1:{FreePort()}" };
        var resultPath = Path.Combine(Path.GetTempPath(), $"showcase-cluster-{Environment.ProcessId}.json");

        var start = Child.Spawn(
            ["--cluster-child", "--index", "1", "--endpoints", string.Join(',', endpoints), "--result", resultPath]);
        start.RedirectStandardOutput = true;
        start.RedirectStandardError = true;
        var child = Process.Start(start)
            ?? throw new InvalidOperationException("failed to spawn the peer process");

        var localShard = new ZSet<KeyedValue<int, decimal>>();
        List<ShardRow> remoteShard;
        var orders = DemoOrders();
        var stepMicros = new double[Steps];
        try
        {
            var (builder, ordersDef, revenueDef) = Declare(Options(0, endpoints));
            await using var circuit = builder.Build();
            circuit.Start();               // TCP mesh comes up here; peers retry until connected
            var input = circuit.Handle(ordersDef);
            var revenue = circuit.Handle(revenueDef);

            foreach (var order in orders)
                await input.InsertAsync(order);
            for (var step = 0; step < Steps; step++)
            {
                var timer = Stopwatch.StartNew();
                await circuit.StepAsync();
                timer.Stop();
                stepMicros[step] = timer.Elapsed.TotalMicroseconds;
                while (revenue.Reader.TryRead(out var batch)) localShard.AddAll(batch);
            }
            await circuit.DisposeAsync();
            while (revenue.Reader.TryRead(out var batch)) localShard.AddAll(batch);

            if (!child.WaitForExit(30_000))
            {
                child.Kill(entireProcessTree: true);
                throw new TimeoutException("peer process did not exit within 30 s");
            }
            if (child.ExitCode != 0)
                throw new InvalidOperationException(
                    $"peer process exited {child.ExitCode}: {await child.StandardError.ReadToEndAsync()}");
            remoteShard = JsonSerializer.Deserialize<List<ShardRow>>(await File.ReadAllTextAsync(resultPath))!;
        }
        finally
        {
            if (!child.HasExited) child.Kill(entireProcessTree: true);
            if (File.Exists(resultPath)) File.Delete(resultPath);
        }

        // Every exchange routed by StableHash, so each merchant's rows landed on exactly
        // one global worker: the two shards are disjoint and their union is the answer.
        var expected = orders.GroupBy(o => o.MerchantId)
            .ToDictionary(g => g.Key, g => g.Sum(o => o.Amount));
        var merged = new Dictionary<int, decimal>();
        var overlap = 0;
        foreach (var (entry, weight) in localShard)
            if (weight > 0) merged[entry.Key] = entry.Value;
        foreach (var row in remoteShard.Where(r => r.Weight > 0))
        {
            if (merged.ContainsKey(row.MerchantId)) overlap++;
            merged[row.MerchantId] = row.Revenue;
        }
        var correct = overlap == 0 && merged.Count == expected.Count &&
            expected.All(kv => merged.TryGetValue(kv.Key, out var v) && v == kv.Value);

        var localRows = localShard.Count(e => e.Weight > 0);
        var remoteRows = remoteShard.Count(r => r.Weight > 0);
        metrics.Poll();
        notices.Push(correct ? NoticeLevel.Good : NoticeLevel.Bad, correct
            ? "cluster: two-process TCP run verified against single-process reference"
            : "cluster: verification FAILED");
        return new ClusterView(
            [
                new ClusterProcessRow("p0 (this process, fed all orders)", "global 0–1", $"{localRows:N0}",
                    $"{metrics.Counter("showcase-cluster-p0", "incremental.cluster.frames_sent"):N0}↑ / {metrics.Counter("showcase-cluster-p0", "incremental.cluster.frames_received"):N0}↓"),
                new ClusterProcessRow("p1 (spawned peer)", "global 2–3", $"{remoteRows:N0}", "reported by peer"),
            ],
            orders.Count, Steps, stepMicros.Average() / 1000, correct, localRows, remoteRows);
    }

    static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
