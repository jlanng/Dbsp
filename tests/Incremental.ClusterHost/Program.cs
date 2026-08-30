// Child process for the multi-process cluster tests: one real OS process of a two-process
// clustered circuit. The parent test launches two of these over loopback TCP, waits for
// both, and merges the result files. Kept deliberately thin — all scenario data and the
// pipeline shape live in ClusterScenario, source-shared with the test assembly.
using System.Globalization;
using System.Text.Json;
using Incremental;
using Incremental.Cluster;
using Incremental.ClusterHost;
using Incremental.Storage.Redis;
using StackExchange.Redis;

var arguments = ParseArgs(args);
var processIndex = int.Parse(arguments["--index"], CultureInfo.InvariantCulture);
var endpoints = arguments["--endpoints"].Split(',');
var scenario = arguments["--scenario"];
var resultPath = arguments["--result"];
var phase = arguments.GetValueOrDefault("--phase");
var redisPrefix = arguments.GetValueOrDefault("--redis-prefix");

using var redis = redisPrefix is null
    ? null
    : await ConnectionMultiplexer.ConnectAsync("localhost:6379,abortConnect=true");

var options = new CircuitOptions
{
    Name = $"cluster-host-{processIndex}",
    Workers = 2,
    AutoStep = false,
    // A wedged sibling must fail this process loudly, not hang the test run.
    ExchangeTimeout = TimeSpan.FromSeconds(30),
    Cluster = new ClusterOptions
    {
        Transport = new TcpExchangeTransport(processIndex, endpoints),
    },
    Storage = redis is null
        ? new Incremental.Storage.InMemoryStorageProvider()
        : new RedisStorageProvider(redis, new RedisStorageOptions { KeyPrefix = redisPrefix! }),
};

if (scenario != "revenue")
    throw new ArgumentException($"Unknown scenario '{scenario}'.");

var pipeline = ClusterScenario.RevenuePipeline(options);
await using var circuit = pipeline.Builder.Build();
circuit.Start();
var committedStepAtStart = circuit.CommittedStep;

var orders = circuit.Handle(pipeline.Orders);
var customers = circuit.Handle(pipeline.Customers);
var revenue = circuit.Handle(pipeline.Revenue);

// Both processes execute the identical StepAsync sequence — the lockstep contract. Which
// order range runs, and whether the reference-data step happens, depends only on the phase.
var (firstOrder, lastOrder) = phase switch
{
    "load" => (0, ClusterScenario.LoadPhaseOrders),
    "resume" => (ClusterScenario.LoadPhaseOrders, ClusterScenario.Orders.Length),
    null => (0, ClusterScenario.Orders.Length),
    _ => throw new ArgumentException($"Unknown phase '{phase}'."),
};

if (phase != "resume")
{
    if (processIndex == 0)
        foreach (var customer in ClusterScenario.Customers)
            await customers.InsertAsync(customer);
    await circuit.StepAsync();
}

for (var i = firstOrder; i < lastOrder; i++)
{
    var (order, weight) = ClusterScenario.Orders[i];
    if (ClusterScenario.FeederOf(i) == processIndex)
        await orders.SendAsync(order, weight);
    await circuit.StepAsync();
}

var committedStep = circuit.CommittedStep;

// This process's output handle carries the deltas its own workers produced; the parent
// merges both processes' shards into the full stream. Disposing first completes the output
// (clustered circuits never step implicitly on dispose), so the changefeed yields exactly
// the batches the explicit steps buffered, then ends.
await circuit.DisposeAsync();
var shard = new ZSet<KeyedValue<string, decimal>>();
await foreach (var delta in revenue.Changes())
    shard.AddAll(delta);

var result = new
{
    Pid = Environment.ProcessId,
    CommittedStepAtStart = committedStepAtStart,
    CommittedStep = committedStep,
    Shard = shard
        .Select(e => new
        {
            e.Value.Key,
            Amount = e.Value.Value.ToString(CultureInfo.InvariantCulture),
            e.Weight,
        })
        .OrderBy(r => r.Key, StringComparer.Ordinal)
        .ToArray(),
};
await File.WriteAllTextAsync(resultPath, JsonSerializer.Serialize(result));
return 0;

static Dictionary<string, string> ParseArgs(string[] args)
{
    var parsed = new Dictionary<string, string>();
    for (var i = 0; i + 1 < args.Length; i += 2)
    {
        if (!args[i].StartsWith("--", StringComparison.Ordinal))
            throw new ArgumentException($"Expected an option at '{args[i]}'.");
        parsed[args[i]] = args[i + 1];
    }
    return parsed;
}
