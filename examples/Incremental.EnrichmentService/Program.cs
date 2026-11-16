using Incremental.EnrichmentService;
using Incremental.Service;

// The enrichment service example: a host for Incremental.Service with the admin UI, a
// contract assembly, a pipeline plugin in two versions, and the adapters a host writes.
//
//   dotnet run --project examples/Incremental.EnrichmentService                 one node, in memory, http://127.0.0.1:5180/admin/
//   dotnet run --project examples/Incremental.EnrichmentService -- --nodes 2    leader and standby in one process (5180, 5181)
//   dotnet run --project examples/Incremental.EnrichmentService -- --kafka --node a     on the local connector stack;
//   dotnet run --project examples/Incremental.EnrichmentService -- --kafka --node b     run both, then kill a
//   dotnet run --project examples/Incremental.EnrichmentService -- --headless [--nodes 2 | --kafka]     the smoke test
//
//   dotnet run --project examples/Incremental.EnrichmentService -- --kafka --clean      remove what the Kafka mode made
//
// A Kafka node runs under a supervisor of its own (NodeSupervisor): when the node ends
// because it lost the leadership — or is killed — it is started again and comes back as
// the standby. --no-supervise runs the node alone, for a platform that restarts it.
//
// --help prints every option; an option the program does not have ends it with exit code 2.
// Other flags: --port N, --rate N (trades per second), --poison N (one trade in N has no
// quantity, which one definition refuses; 0 for none), --prefix name (Kafka topics and
// Postgres tables), --no-generate, --no-pipelines, --verbose, --otlp (export the meters
// over OTLP; the endpoint is OTEL_EXPORTER_OTLP_ENDPOINT). README.md beside this file
// walks through all of it.
if (ExampleOptions.AsksForHelp(args))
{
    Console.Out.Write(ExampleOptions.Usage);
    return 0;
}
ExampleOptions options;
try
{
    options = ExampleOptions.Parse(args);
}
catch (Exception ex) when (ex is ArgumentException or FormatException)
{
    Console.Error.WriteLine(ex.Message);
    return 2;
}

if (options.Headless)
    return await new Smoke(options).RunAsync();

// <snippet:supervisor>
// A node that has led and no longer does ends its process (exit code 3 fenced, 4 resigned,
// 5 cannot lead), and something must start it again: then it is the standby. Under
// systemd, Kubernetes or ECS that is the platform's restart policy. From a terminal there
// is nobody, so the program supervises itself: this process only starts the node, as a
// child, and starts it again each time it ends.
if (options.Kafka && options.Supervise && !args.Contains("--clean") && !NodeSupervisor.IsSupervised)
    return await NodeSupervisor.RunAsync(args);
// </snippet:supervisor>

if (args.Contains("--clean"))
{
    // Everything the Kafka mode made under this prefix: the connector and its replication
    // slot, the Postgres tables, every topic — the service's own included.
    await using var stack = new KafkaStack(options);
    await stack.CleanUpAsync();
    var data = options.DataDirectory ?? Path.Combine(Path.GetTempPath(), "incremental-enrichment-service", options.Prefix);
    if (Directory.Exists(data))
        Directory.Delete(data, recursive: true);        // the counterparties file and the nodes' plugin caches
    Console.WriteLine($"Removed the connector, the Postgres tables and the topics of the prefix '{options.Prefix}'.");
    return 0;
}

var dataDirectory = options.DataDirectory
    ?? Path.Combine(Path.GetTempPath(), "incremental-enrichment-service", options.Kafka ? options.Prefix : "memory-" + Environment.ProcessId);
// Event time moves at the pace trades are generated, so a one-minute window takes a minute.
var shape = new WorldShape
{
    TradeInterval = TimeSpan.FromSeconds(1.0 / Math.Max(1, options.Rate)), PoisonEvery = Math.Max(0, options.Poison),
    TradeKeySpace = options.TradeIds > 0 ? options.TradeIds : long.MaxValue,
};

KafkaStack? kafka = null;
var nodes = new List<ExampleNode>();
var supervisors = new List<Task>();
using var stop = new CancellationTokenSource();
try
{
    World world;
    if (options.Kafka)
    {
        kafka = new KafkaStack(options);
        if (options.PrepareStack)
        {
            Console.WriteLine($"Preparing the local stack under the prefix '{options.Prefix}': Postgres tables, the Debezium connector, topics…");
            await kafka.EnsureAsync(shape);
        }
        // The counterparties file is shared by both processes; whichever starts first writes it.
        world = new World(shape, dataDirectory, kafka, freshData: false);
        nodes.Add(await ExampleNode.StartAsync(world, options, options.Node, options.Port, null, options.ProbePort, options.PeerProbePort));
    }
    else
    {
        // The in-memory backend keeps nothing between runs, so neither does the world.
        world = new World(shape, dataDirectory, null, freshData: true);
        var cluster = new InMemoryServiceCluster();
        for (var i = 0; i < Math.Clamp(options.Nodes, 1, 2); i++)
        {
            // One after the other: the first leads before the second starts.
            var node = await ExampleNode.StartAsync(world, options, i == 0 ? "a" : "b", options.Port + i, cluster, exitOnLeadershipLoss: options.Nodes < 2);
            nodes.Add(node);
            while (i == 0 && !node.Node.IsLeader)
                await Task.Delay(20);
        }
        if (nodes.Count == 2)
        {
            // <snippet:in-process-supervisor>
            // Two nodes in one process: a node that lost the leadership cannot end the
            // process, so it is started again in it, on the same cluster, port and name, and
            // comes back as the standby — the contract NodeSupervisor keeps for a process.
            for (var i = 0; i < nodes.Count; i++)
            {
                var index = i;
                ExampleNode? running = nodes[i];
                supervisors.Add(NodeSupervisor.RunInProcessAsync(
                    _ => Interlocked.Exchange(ref running, null) is { } first
                        ? Task.FromResult(first)
                        : ExampleNode.StartAsync(world, options, index == 0 ? "a" : "b", options.Port + index, cluster, exitOnLeadershipLoss: false),
                    n => n.Node,
                    n => nodes[index] = n,
                    stop: stop.Token));
            }
            // </snippet:in-process-supervisor>
        }
    }

    foreach (var node in nodes)
        Console.WriteLine($"node {node.Node.NodeId}: {node.AdminUrl}");
    Console.WriteLine(kafka is null
        ? $"sinks:  {nodes[0].Url}/views"
        : $"sinks:  the topics {kafka.SinkTopic("trade-enrichment")}, {kafka.SinkTopic("large-trades")}, {kafka.SinkTopic("desk-exposure")} and {kafka.SinkTopic("execution-quality")}");
    Console.WriteLine($"files:  {world.LakeDirectory} (alerts/ and, in memory, quotes/ and dead-letters/)");
    Console.WriteLine("Ctrl+C to stop.");

    var generator = options.Generate ? new Generator(world, nodes, options.Rate).RunAsync(stop.Token) : Task.CompletedTask;
    var interrupted = new TaskCompletionSource();
    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true;
        interrupted.TrySetResult();
    };
    using var terminated = System.Runtime.InteropServices.PosixSignalRegistration.Create(
        System.Runtime.InteropServices.PosixSignal.SIGTERM, context =>
        {
            context.Cancel = true;
            interrupted.TrySetResult();
        });
    // A node that loses leadership stops its host (exit code 3 fenced, 4 resigned, 5 cannot
    // lead); a process supervisor starts it again and it comes back as the standby. Two
    // nodes in memory are supervised in this process instead, and only a stop request ends it.
    await (supervisors.Count > 0 ? interrupted.Task : Task.WhenAny(nodes.Select(n => n.App.WaitForShutdownAsync()).Append(interrupted.Task)));
    stop.Cancel();
    await generator;
    await Task.WhenAll(supervisors);
}
finally
{
    foreach (var node in Enumerable.Reverse(nodes))
        await node.DisposeAsync();
    if (kafka is not null)
        await kafka.DisposeAsync();
    if (!options.Kafka && options.DataDirectory is null)
    {
        try
        {
            Directory.Delete(dataDirectory, recursive: true);
        }
        catch (IOException)
        {
        }
    }
}
return Environment.ExitCode;
