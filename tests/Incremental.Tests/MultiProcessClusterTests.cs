using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using Incremental.ClusterHost;
using StackExchange.Redis;

namespace Incremental.Tests;

/// <summary>
/// Clustered circuits across two REAL operating-system processes (Incremental.ClusterHost,
/// launched over loopback TCP) — unlike <see cref="ClusterTests"/>, whose "processes" share
/// this test process. Real processes have different randomized string-hash seeds, so these
/// tests fail against any exchange routing that isn't process-stable (the bug
/// <see cref="StableHash"/> fixed) and guard the wire protocol end to end. Scenario data
/// and pipeline shape are source-shared with the host via <see cref="ClusterScenario"/>.
/// Tests share this class so xunit never runs them concurrently.
/// </summary>
public class MultiProcessClusterTests
{
    private static readonly TimeSpan HostTimeout = TimeSpan.FromSeconds(60);

    private sealed record ShardRow(string Key, string Amount, long Weight);

    private sealed record HostResult(int Pid, long? CommittedStepAtStart, long? CommittedStep, ShardRow[] Shard);

    [Fact]
    public async Task String_keyed_pipeline_agrees_across_two_real_processes()
    {
        var results = await RunHostPairAsync(scenario: "revenue");

        Assert.NotEqual(results[0].Pid, results[1].Pid); // genuinely separate OS processes
        // Both processes' workers must own some regions — that's what makes the routing real.
        Assert.NotEmpty(results[0].Shard);
        Assert.NotEmpty(results[1].Shard);

        AssertSameZSet(await ReferenceRevenue(), Merge(results));
    }

    [SkippableFact]
    public async Task Checkpoint_restart_across_real_processes_is_exactly_once()
    {
        using var redis = ConnectOrSkip();
        var prefix = $"mp-test:{Guid.NewGuid():N}";
        try
        {
            var load = await RunHostPairAsync(scenario: "revenue", phase: "load", redisPrefix: prefix);
            Assert.NotNull(load[0].CommittedStep);
            Assert.Equal(load[0].CommittedStep, load[1].CommittedStep); // lockstep commit boundary

            // Fresh processes — and fresh string-hash seeds — resume the committed state.
            var resume = await RunHostPairAsync(scenario: "revenue", phase: "resume", redisPrefix: prefix);
            Assert.Equal(load[0].CommittedStep, resume[0].CommittedStepAtStart);
            Assert.Equal(load[0].CommittedStep, resume[1].CommittedStepAtStart);

            var pids = new[] { load[0].Pid, load[1].Pid, resume[0].Pid, resume[1].Pid };
            Assert.Equal(4, pids.Distinct().Count());

            // Phase-1 deltas + phase-2 deltas must integrate to exactly the reference: any
            // restore duplication or loss corrupts phase 2's retractions of phase-1 orders.
            AssertSameZSet(await ReferenceRevenue(), Merge([.. load, .. resume]));
        }
        finally
        {
            foreach (var endpoint in redis.GetEndPoints())
            {
                var server = redis.GetServer(endpoint);
                foreach (var key in server.Keys(pattern: $"{prefix}*").ToArray())
                    await redis.GetDatabase().KeyDeleteAsync(key);
            }
        }
    }

    private static async Task<HostResult[]> RunHostPairAsync(
        string scenario, string? phase = null, string? redisPrefix = null)
    {
        var hostDll = HostDllPath();
        var ports = FreePorts(2);
        var endpoints = $"127.0.0.1:{ports[0]},127.0.0.1:{ports[1]}";
        var resultPaths = new[] { Path.GetTempFileName(), Path.GetTempFileName() };
        try
        {
            var children = Enumerable.Range(0, 2).Select(index =>
            {
                var info = new ProcessStartInfo("dotnet")
                {
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    WorkingDirectory = Path.GetDirectoryName(hostDll)!,
                };
                info.ArgumentList.Add(hostDll);
                foreach (var arg in new[]
                {
                    "--index", index.ToString(CultureInfo.InvariantCulture),
                    "--endpoints", endpoints,
                    "--scenario", scenario,
                    "--result", resultPaths[index],
                })
                    info.ArgumentList.Add(arg);
                if (phase is not null)
                {
                    info.ArgumentList.Add("--phase");
                    info.ArgumentList.Add(phase);
                }
                if (redisPrefix is not null)
                {
                    info.ArgumentList.Add("--redis-prefix");
                    info.ArgumentList.Add(redisPrefix);
                }
                return Process.Start(info) ?? throw new InvalidOperationException("Failed to start cluster host.");
            }).ToArray();

            using var timeout = new CancellationTokenSource(HostTimeout);
            try
            {
                foreach (var child in children)
                    await child.WaitForExitAsync(timeout.Token);
            }
            catch (OperationCanceledException)
            {
                foreach (var child in children.Where(c => !c.HasExited))
                    child.Kill(entireProcessTree: true);
                Assert.Fail($"Cluster hosts did not finish within {HostTimeout}. " +
                    $"stderr[0]: {await children[0].StandardError.ReadToEndAsync()} " +
                    $"stderr[1]: {await children[1].StandardError.ReadToEndAsync()}");
            }

            foreach (var (child, index) in children.Select((c, i) => (c, i)))
                Assert.True(child.ExitCode == 0,
                    $"Cluster host {index} exited with {child.ExitCode}: {await child.StandardError.ReadToEndAsync()}");

            return [.. resultPaths.Select(path => JsonSerializer.Deserialize<HostResult>(File.ReadAllText(path))!)];
        }
        finally
        {
            foreach (var path in resultPaths)
                File.Delete(path);
        }
    }

    /// <summary>The same churn integrated by a single-process circuit built from the same
    /// shared pipeline definition.</summary>
    private static async Task<ZSet<KeyedValue<string, decimal>>> ReferenceRevenue()
    {
        await using var single = ClusterScenario.RevenuePipeline(new CircuitOptions { AutoStep = false }).Build();
        single.Start();
        var orders = single.Input<(int Id, int CustomerId, decimal Amount)>("orders");
        var customers = single.Input<(int Id, string Region)>("customers");
        foreach (var customer in ClusterScenario.Customers)
            await customers.InsertAsync(customer);
        foreach (var (order, weight) in ClusterScenario.Orders)
            await orders.SendAsync(order, weight);
        await single.StepAsync().WaitAsync(HostTimeout);

        var integrated = new ZSet<KeyedValue<string, decimal>>();
        var output = single.Output<KeyedValue<string, decimal>>("revenue");
        while (output.Reader.TryRead(out var delta))
            integrated.AddAll(delta);
        return integrated;
    }

    private static ZSet<KeyedValue<string, decimal>> Merge(HostResult[] results)
    {
        var merged = new ZSet<KeyedValue<string, decimal>>();
        foreach (var row in results.SelectMany(r => r.Shard))
            merged.Add(
                new KeyedValue<string, decimal>(row.Key, decimal.Parse(row.Amount, CultureInfo.InvariantCulture)),
                row.Weight);
        return merged;
    }

    private static void AssertSameZSet<T>(ZSet<T> expected, ZSet<T> actual) where T : notnull
    {
        Assert.Equal(expected.DistinctCount, actual.DistinctCount);
        foreach (var (value, weight) in expected)
            Assert.Equal(weight, actual.WeightOf(value));
    }

    private static string HostDllPath()
    {
        // tests/Incremental.Tests/bin/{Config}/{Tfm} → tests/Incremental.ClusterHost/bin/{Config}/{Tfm}
        var baseDir = AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar);
        var tfm = Path.GetFileName(baseDir);
        var config = Path.GetFileName(Path.GetDirectoryName(baseDir)!);
        var dll = Path.GetFullPath(Path.Combine(
            baseDir, "..", "..", "..", "..", "Incremental.ClusterHost", "bin", config, tfm, "Incremental.ClusterHost.dll"));
        Assert.True(File.Exists(dll), $"Cluster host executable not found at {dll} — build tests/Incremental.ClusterHost.");
        return dll;
    }

    private static ConnectionMultiplexer ConnectOrSkip()
    {
        try
        {
            return ConnectionMultiplexer.Connect("localhost:6379,connectTimeout=1000,abortConnect=true");
        }
        catch (RedisConnectionException)
        {
            throw new SkipException("Redis is not reachable on localhost:6379 — run `docker compose up -d`.");
        }
    }

    private static int[] FreePorts(int count)
    {
        var listeners = Enumerable.Range(0, count)
            .Select(_ =>
            {
                var listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start();
                return listener;
            })
            .ToArray();
        var ports = listeners.Select(l => ((IPEndPoint)l.LocalEndpoint).Port).ToArray();
        foreach (var listener in listeners)
            listener.Stop();
        return ports;
    }
}
