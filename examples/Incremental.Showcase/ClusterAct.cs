using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using Incremental;
using Incremental.Cluster;

namespace Incremental.Showcase;

/// <summary>
/// Act 5: one declaration on several real OS processes (the topology's node count, each
/// with its worker count), exchanging shards over TCP. This binary relaunches itself as
/// processes 1…N−1 (<c>--cluster-child</c>); every process builds the identical circuit and
/// calls StepAsync in lockstep — the cluster contract. `Recurse` is rejected in clustered
/// circuits, so this act runs its own small pipeline.
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

    static CircuitOptions Options(int processIndex, string[] endpoints, int workers) => new()
    {
        Name = $"showcase-cluster-p{processIndex}",
        Workers = workers,
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
        var workers = int.Parse(arguments["--workers"]);

        var (builder, _, revenueDef) = Declare(Options(index, endpoints, workers));
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

    /// <summary>Runs the <paramref name="nodes"/>-process verification (this process is node
    /// 0) and returns the report (the caller shows it inside the cluster tab — no direct
    /// console writes).</summary>
    public static async Task<ClusterView> RunParentAsync(NoticeLog notices, IncrementalMetrics metrics, int nodes, int workers)
    {
        if (nodes < 2)
            throw new InvalidOperationException("the cluster act needs at least two nodes");
        var endpoints = Enumerable.Range(0, nodes).Select(_ => $"127.0.0.1:{FreePort()}").ToArray();
        var resultPaths = Enumerable.Range(1, nodes - 1)
            .Select(i => Path.Combine(Path.GetTempPath(), $"showcase-cluster-{Environment.ProcessId}-p{i}.json"))
            .ToArray();

        var children = new List<Process>();
        for (var i = 1; i < nodes; i++)
        {
            var start = Child.Spawn(
                ["--cluster-child", "--index", i.ToString(), "--endpoints", string.Join(',', endpoints),
                 "--result", resultPaths[i - 1], "--workers", workers.ToString()],
                workers);
            start.RedirectStandardOutput = true;
            start.RedirectStandardError = true;
            children.Add(Process.Start(start)
                ?? throw new InvalidOperationException($"failed to spawn peer process {i}"));
        }

        var localShard = new ZSet<KeyedValue<int, decimal>>();
        var remoteShards = new List<List<ShardRow>>();
        var orders = DemoOrders();
        var stepMicros = new double[Steps];
        try
        {
            var (builder, ordersDef, revenueDef) = Declare(Options(0, endpoints, workers));
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

            for (var i = 0; i < children.Count; i++)
            {
                var child = children[i];
                if (!child.WaitForExit(30_000))
                {
                    child.Kill(entireProcessTree: true);
                    throw new TimeoutException($"peer process {i + 1} did not exit within 30 s");
                }
                if (child.ExitCode != 0)
                    throw new InvalidOperationException(
                        $"peer process {i + 1} exited {child.ExitCode}: {await child.StandardError.ReadToEndAsync()}");
                remoteShards.Add(JsonSerializer.Deserialize<List<ShardRow>>(await File.ReadAllTextAsync(resultPaths[i]))!);
            }
        }
        finally
        {
            foreach (var child in children)
                if (!child.HasExited) child.Kill(entireProcessTree: true);
            foreach (var path in resultPaths)
                if (File.Exists(path)) File.Delete(path);
        }

        // Every exchange routed by StableHash, so each merchant's rows landed on exactly
        // one global worker: the shards are pairwise disjoint and their union is the answer.
        var expected = orders.GroupBy(o => o.MerchantId)
            .ToDictionary(g => g.Key, g => g.Sum(o => o.Amount));
        var merged = new Dictionary<int, decimal>();
        var overlap = 0;
        foreach (var (entry, weight) in localShard)
            if (weight > 0) merged[entry.Key] = entry.Value;
        foreach (var shard in remoteShards)
            foreach (var row in shard.Where(r => r.Weight > 0))
            {
                if (merged.ContainsKey(row.MerchantId)) overlap++;
                merged[row.MerchantId] = row.Revenue;
            }
        var correct = overlap == 0 && merged.Count == expected.Count &&
            expected.All(kv => merged.TryGetValue(kv.Key, out var v) && v == kv.Value);

        var localRows = localShard.Count(e => e.Weight > 0);
        var remoteRows = remoteShards.Sum(shard => shard.Count(r => r.Weight > 0));
        metrics.Poll();
        notices.Push(correct ? NoticeLevel.Good : NoticeLevel.Bad, correct
            ? $"cluster: {nodes}-process TCP run verified against single-process reference"
            : "cluster: verification FAILED");
        string Range(int node) => workers == 1 ? $"global {node * workers}" : $"global {node * workers}–{(node + 1) * workers - 1}";
        var rows = new List<ClusterProcessRow>
        {
            new("p0 (this process, fed all orders)", Range(0), $"{localRows:N0}",
                $"{metrics.Counter("showcase-cluster-p0", "incremental.cluster.frames_sent"):N0}↑ / {metrics.Counter("showcase-cluster-p0", "incremental.cluster.frames_received"):N0}↓"),
        };
        for (var i = 1; i < nodes; i++)
            rows.Add(new($"p{i} (spawned peer)", Range(i), $"{remoteShards[i - 1].Count(r => r.Weight > 0):N0}", "reported by peer"));
        return new ClusterView([.. rows], nodes, workers,
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
