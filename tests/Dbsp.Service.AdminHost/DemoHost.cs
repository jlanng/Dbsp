using System.Text;
using System.Text.Json;
using Acme.Contracts;
using Dbsp.Service.Admin;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using AcmeTables = Acme.Contracts.Tables;

namespace Dbsp.Service.AdminHost;

public sealed class DemoOptions
{
    /// <summary>The first node's port; the second node, if any, gets the next. 0: any free port.</summary>
    public int Port { get; set; } = 5180;
    public int Nodes { get; set; } = 1;

    /// <summary>Orders generated per second across the feeds; 0 generates nothing, and the
    /// poison records are then the caller's to append.</summary>
    public int Rate { get; set; } = 60;
    public int Instruments { get; set; } = 5000;
    public bool ReadOnly { get; set; }
    public bool ExposeRowData { get; set; } = true;
    public bool AllowPluginUpload { get; set; } = true;

    /// <summary>Whether the stored test plugin (Acme.Pipelines) is imported at first lead.</summary>
    public bool ImportPlugins { get; set; } = true;

    /// <summary>Serve the UI from the package's wwwroot in this checkout, not the embedded copy.</summary>
    public string? UiDirectory { get; set; }
    public Action<ServiceAdminOptions>? Admin { get; set; }
    public Action<ServiceOptions>? Service { get; set; }
    public Action<WebApplicationBuilder>? Builder { get; set; }
    public Action<WebApplication>? App { get; set; }
    public LogLevel LogLevel { get; set; } = LogLevel.Warning;

    /// <summary>With two nodes: a node that loses the leadership is started again in this
    /// process, on the same port, and comes back as the standby
    /// (<see cref="NodeSupervisor.RunInProcessAsync{THost}"/>). The AdminHost program sets
    /// it; tests that look at a fenced node leave it off.</summary>
    public bool SuperviseNodes { get; set; }
}

public sealed class DemoNode : IAsyncDisposable
{
    public required WebApplication App { get; init; }
    public required ServiceNode Node { get; init; }
    public required string Url { get; init; }

    /// <summary>The admin UI's address: <c>{Url}/admin/</c>.</summary>
    public string Admin => Url + "/admin/";

    public async ValueTask DisposeAsync()
    {
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            await App.StopAsync(timeout.Token);
        }
        catch (OperationCanceledException)
        {
        }
        await App.DisposeAsync();
    }
}

/// <summary>
/// One or two service nodes in this process on the in-memory backend, each with its own
/// web server and the admin UI at <c>/admin</c>: three shared tables (one written by the
/// host, one loaded from a file with an unreadable line in it, one derived by a pipeline),
/// eight pipelines — one that faults until it is Failed, one that dead-letters, one left
/// stopped, one wide enough to test the topology view — and a generator feeding them.
/// </summary>
public sealed class DemoHost : IAsyncDisposable
{
    public const string Plugin = "dbsp.service.adminhost";
    public const string Orders = "orders";
    public const string CheckedFeed = "orders-checked";
    public const string LenientFeed = "orders-lenient";

    private readonly CancellationTokenSource _stop = new();
    private readonly string _work;
    private Task _generator = Task.CompletedTask;
    private readonly List<Task> _supervisors = [];

    private DemoHost(string work) => _work = work;

    public List<DemoNode> Nodes { get; } = [];
    // The feeds are memory logs, which by default keep every record so that a test can
    // replay them; a host left running keeps the newest 100,000 of each instead.
    public MemoryConnector Memory { get; } = new() { RetainLast = 100_000 };
    public DemoOptions Options { get; private set; } = new();

    public DemoNode Leader => Nodes.First(n => n.Node.IsLeader);

    public static readonly string[] Desks = ["rates", "credit", "fx", "equities"];
    private static readonly string[] Traders = ["ada", "bo", "cy", "dee", "eli", "fay", "gus", "hal", "ivy", "jo", "kit", "lou"];
    private static readonly string[] Countries = ["GB", "US", "DE", "FR", "JP", "SG", "CH", "NL"];
    private static readonly string[] Tiers = ["gold", "silver", "bronze"];
    public const int CounterpartyCount = 240;

    public static JsonElement Json(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private static JsonElement Memory_(string name) => Json($$"""{ "connector": "memory", "name": "{{name}}" }""");

    public static PipelineSpec Spec(
        string id, string definition, string feed, string[] sinks, string? settings = null,
        DesiredState desired = DesiredState.Running, RecordErrorPolicy errors = RecordErrorPolicy.Fault, int workers = 1) => new()
    {
        Id = id,
        Plugin = Plugin,
        Definition = definition,
        Desired = desired,
        Settings = settings is null ? null : Json(settings),
        Circuit = new PipelineCircuitOptions { Workers = workers, ErrorPolicy = errors },
        Inputs = feed.Length == 0 ? new Dictionary<string, JsonElement>() : new Dictionary<string, JsonElement> { ["orders"] = Memory_(feed) },
        Sinks = sinks.ToDictionary(s => s, s => Memory_($"{id}/{s}")),
        Restart = new RestartPolicy { MaxRestarts = 3, Within = TimeSpan.FromMinutes(10), InitialBackoff = TimeSpan.FromSeconds(1), MaxBackoff = TimeSpan.FromSeconds(4) },
    };

    public static async Task<DemoHost> StartAsync(DemoOptions? options = null)
    {
        options ??= new DemoOptions();
        var work = Path.Combine(Path.GetTempPath(), "dbsp-admin-host", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(work);
        var host = new DemoHost(work) { Options = options };
        try
        {
            host.WriteReferenceData();
            var cluster = new InMemoryServiceCluster();
            for (var n = 0; n < Math.Max(1, options.Nodes); n++)
            {
                // One after the other: the first leads before the second starts.
                var node = await host.StartNodeAsync(cluster, n, options);
                host.Nodes.Add(node);
                if (n == 0)
                    await host.LoadInstrumentsAsync(node.Node, options.Instruments);
            }
            if (options.SuperviseNodes && host.Nodes.Count > 1)
            {
                for (var n = 0; n < host.Nodes.Count; n++)
                {
                    var index = n;
                    DemoNode? running = host.Nodes[n];
                    var port = new Uri(running.Url).Port;
                    host._supervisors.Add(NodeSupervisor.RunInProcessAsync(
                        _ => Interlocked.Exchange(ref running, null) is { } first ? Task.FromResult(first) : host.StartNodeAsync(cluster, index, options, port),
                        node => node.Node,
                        node =>
                        {
                            lock (host.Nodes)
                                host.Nodes[index] = node;
                        },
                        stop: host._stop.Token));
                }
            }
            if (options.Rate > 0)
                host._generator = Task.Run(() => host.GenerateAsync(options.Rate, host._stop.Token));
            return host;
        }
        catch
        {
            await host.DisposeAsync();
            throw;
        }
    }

    private void WriteReferenceData()
    {
        var store = Path.Combine(_work, "refdata");
        Directory.CreateDirectory(store);
        var lines = new StringBuilder();
        for (var i = 0; i < CounterpartyCount; i++)
        {
            lines.AppendLine(JsonSerializer.Serialize(
                new Counterparty($"CP{i:D4}", $"Counterparty {i:D4} {(i % 3 == 0 ? "Holdings" : i % 3 == 1 ? "Partners" : "Capital")}", Countries[i % Countries.Length], Tiers[i % Tiers.Length]),
                new JsonSerializerOptions(JsonSerializerDefaults.Web)));
            if (i == 17)
                lines.AppendLine("""{"id":"CP-BROKEN","name":"A line that is not finished","country":""");
        }
        File.WriteAllText(Path.Combine(store, "counterparties.jsonl"), lines.ToString());

        if (!Options.ImportPlugins)
            return;
        // The stored plugin: the test plugin project's build output, as a package directory.
        var output = new DirectoryInfo(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar));
        var built = Path.Combine(
            output.Parent!.Parent!.Parent!.Parent!.FullName, "Dbsp.Service.TestPlugins", "Acme.Pipelines", "bin", output.Parent!.Name, output.Name);
        if (Directory.Exists(built))
            CopyDirectory(built, Path.Combine(_work, "plugins", "acme.pipelines"));
    }

    /// <summary>Where the test plugin's build put its package, or null when it was not built.</summary>
    public string? PluginPackage => Directory.Exists(Path.Combine(_work, "plugins", "acme.pipelines")) ? Path.Combine(_work, "plugins", "acme.pipelines") : null;

    private static void CopyDirectory(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (var file in Directory.EnumerateFiles(from))
            File.Copy(file, Path.Combine(to, Path.GetFileName(file)), overwrite: true);
    }

    private async Task<DemoNode> StartNodeAsync(InMemoryServiceCluster cluster, int index, DemoOptions options, int? port = null)
    {
        var nodeId = index == 0 ? "node-a" : $"node-{(char)('a' + index)}";
        var builder = WebApplication.CreateBuilder();
        builder.Logging.SetMinimumLevel(options.LogLevel);
        if (options.LogLevel == LogLevel.None)
            builder.Logging.ClearProviders();
        builder.WebHost.UseUrls($"http://127.0.0.1:{port ?? (options.Port == 0 ? 0 : options.Port + index)}");
        builder.Services.AddSingleton(Memory);
        var advertised = options.Port == 0 ? null : $"http://127.0.0.1:{port ?? options.Port + index}/admin/";
        var service = builder.Services.AddDbspService(o =>
            {
                o.CacheDirectory = Path.Combine(_work, "cache-" + nodeId);
                o.PluginDirectory = index == 0 && options.ImportPlugins ? Path.Combine(_work, "plugins") : null;
                o.AdvertisedAdminUrl = advertised;
                o.TrimInterval = TimeSpan.FromSeconds(5);
                o.FollowerRetention = TimeSpan.FromSeconds(30);
                // Two nodes in one process: a resigned or fenced node must not stop the other's host.
                o.ExitOnLeadershipLoss = options.Nodes < 2;
                options.Service?.Invoke(o);
            })
            .UseInMemoryBackend(cluster, nodeId)
            .AddObjectStore("refdata", _ => new LocalObjectStore(Path.Combine(_work, "refdata")))
            .AddContracts(typeof(Instrument).Assembly)
            .AddPlugin(typeof(DemoHost).Assembly)
            .AddTable(AcmeTables.Instruments)
            .AddTable(AcmeTables.Counterparties,
                new TableLoaderSpec("file", "refdata", Json("""{ "path": "counterparties.jsonl", "pollInterval": "00:00:02" }""")),
                onUnreadable: UnreadablePolicy.Skip)
            .AddTable(AcmeTables.Positions);

        foreach (var spec in Seeds())
            service.AddPipeline(spec);

        builder.Services.AddDbspAdmin(o =>
        {
            o.ReadOnly = options.ReadOnly;
            o.ExposeRowData = options.ExposeRowData;
            o.AllowPluginUpload = options.AllowPluginUpload;
            o.AllowPluginRescan = true;
            o.UiDirectory = options.UiDirectory;
            options.Admin?.Invoke(o);
        });
        options.Builder?.Invoke(builder);

        var app = builder.Build();
        options.App?.Invoke(app);
        app.MapDbspAdmin("/admin");
        app.MapDbspHealth("/health");
        app.MapGet("/", () => Results.Redirect("/admin/"));
        await app.StartAsync();
        var url = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First().TrimEnd('/');
        return new DemoNode { App = app, Node = app.Services.GetRequiredService<ServiceNode>(), Url = url };
    }

    public static IEnumerable<PipelineSpec> Seeds()
    {
        yield return Spec("enrich-orders", "enrich-orders", Orders, ["enriched"], """{ "minNotional": 0, "label": "all orders" }""", workers: 2);
        yield return Spec("trader-activity", "trader-activity", Orders, ["counts", "quantities"]);
        yield return Spec("positions", "positions", Orders, []);
        yield return Spec("position-report", "position-report", "", ["report"]);
        yield return Spec("desk-risk", "desk-risk", Orders, ["by-country", "by-tier", "by-symbol", "largest"], """{ "desks": ["rates", "credit"], "top": 5 }""", workers: 2);
        yield return Spec("checked-orders", "checked-orders", CheckedFeed, ["counts"]);
        yield return Spec("lenient-orders", "checked-orders", LenientFeed, ["counts"], errors: RecordErrorPolicy.DeadLetter);
        yield return Spec("large-orders", "enrich-orders", Orders, ["enriched"], """{ "minNotional": 250000 }""", DesiredState.Stopped);
    }

    public static Instrument Instrument(int id, int revision = 0) =>
        new(id, $"SYM{id:D5}", $"GB{id:D10}", 1 + (id + revision) % 7 * 0.25m);

    private async Task LoadInstrumentsAsync(ServiceNode node, int count)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (!node.IsLeader || node.Tables.Tables.All(t => t.Name != "instruments" || t.State != TableState.Open))
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("The first node did not lead and open its tables within 30 s.");
            await Task.Delay(20);
        }
        await node.Tables.Get<int, Instrument>("instruments").Writer.ReplaceAllAsync(Enumerable.Range(1, count).Select(i => Instrument(i)));
    }

    public static Order Order(long sequence, Random random, int instruments, bool negative = false) => new(
        sequence % 20_000, 1 + random.Next(instruments), $"CP{random.Next(CounterpartyCount + 8):D4}",
        negative ? -1 : 1 + random.Next(500), 10 + random.Next(990), Traders[random.Next(Traders.Length)], Desks[random.Next(Desks.Length)]);

    /// <summary>Orders into the three feeds, an instrument revised now and then, and — into
    /// the two checked feeds — an order with a negative quantity: once into the feed of the
    /// pipeline that faults on it, every few seconds into the feed of the one that drops it.</summary>
    private async Task GenerateAsync(int rate, CancellationToken stop)
    {
        var random = new Random(7);
        var orders = Memory.KeyedLog<long, Order>(Orders);
        var checkedOrders = Memory.KeyedLog<long, Order>(CheckedFeed);
        var lenient = Memory.KeyedLog<long, Order>(LenientFeed);
        long sequence = 0;
        var tick = 0;
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
        try
        {
            while (await timer.WaitForNextTickAsync(stop))
            {
                tick++;
                // A feed that breathes: a slow wave over the base rate.
                var perTick = Math.Max(1, (int)(rate / 10.0 * (1 + 0.5 * Math.Sin(tick / 40.0))));
                for (var i = 0; i < perTick; i++)
                    orders.Upsert(Order(sequence++, random, Options.Instruments));
                checkedOrders.Upsert(Order(sequence, random, Options.Instruments, negative: tick == 150));
                lenient.Upsert(Order(sequence, random, Options.Instruments, negative: tick % 70 == 35));

                DemoNode[] nodes;
                lock (Nodes)
                    nodes = [.. Nodes];
                if (tick % 20 == 0 && nodes.FirstOrDefault(n => n.Node.IsLeader) is { } leader)
                {
                    try
                    {
                        var writer = leader.Node.Tables.Get<int, Instrument>("instruments").Writer;
                        for (var i = 0; i < 5; i++)
                        {
                            var id = 1 + random.Next(Options.Instruments);
                            await writer.UpsertAsync(Instrument(id, tick));
                        }
                        await writer.CommitAsync(stop);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        // Between leaders, or the table is being reopened: the next tick tries again.
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        try
        {
            await _generator;
            await Task.WhenAll(_supervisors);
        }
        catch
        {
        }
        foreach (var node in Enumerable.Reverse(Nodes))
        {
            try
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
                await node.App.StopAsync(timeout.Token);
            }
            catch
            {
            }
            await node.App.DisposeAsync();
        }
        try
        {
            Directory.Delete(_work, recursive: true);
        }
        catch (IOException)
        {
        }
    }
}
