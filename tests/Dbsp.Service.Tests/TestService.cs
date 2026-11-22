using System.Text.Json;
using Acme.Contracts;
using Dbsp.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Dbsp.Service.Tests;

/// <summary>One service node built the way a host builds it, through DI, with fast
/// intervals. The memory connector is passed in when several nodes share the outside
/// world (the same input logs and sinks), as a standby and its leader do.</summary>
internal sealed class TestService : IAsyncDisposable
{
    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    public required ServiceProvider Services { get; init; }
    public required ServiceNode Node { get; init; }
    public required MemoryConnector Memory { get; init; }
    public required TestLog Log { get; init; }

    public IPipelineSupervisor Pipelines => Node.Pipelines;
    public ITableRegistry Tables => Node.Tables;
    public IPluginCatalog Plugins => Node.Plugins;

    // The first node of the process is started alone, before the test classes run theirs in
    // parallel. On this runtime (10.0.14, Linux) several threads taking the same first
    // pass through a node's startup — JIT and type loading of the same generic code at
    // once — crashed the process with "Internal CLR error (0x80131506)" in about one run
    // in five; the crashing thread was in native runtime code and no test had got further
    // than ServiceNode.PrepareAsync. Started once alone first, it does not.
    private static readonly Lazy<Task> WarmUp = new(async () =>
    {
        await using var service = await StartCoreAsync(null, "warm-up", null, null, null);
        await service.DeclareInstrumentsAsync([new Instrument(1, "S", "I", 1)]);
        await service.DeployAsync(Spec("warm-up", "enrich", "warm-up-in", "warm-up-out"));
        service.Memory.Log<Trade>("warm-up-in").Append(new Trade(1, 1, 1, "t"));
        var sink = service.Memory.UpsertSink<long, EnrichedTrade>("warm-up-out");
        await Eventually(() => sink.Rows.Count == 1, "the warm-up pipeline's output");
    });

    /// <summary>For tests that build their nodes another way (the admin host): the same
    /// warm-up first.</summary>
    public static Task WarmedUpAsync() => WarmUp.Value;

    public static async Task<TestService> StartAsync(
        InMemoryServiceCluster? cluster = null, string nodeId = "a", MemoryConnector? memory = null,
        Action<ServiceOptions>? options = null, Action<DbspServiceBuilder>? configure = null)
    {
        await WarmUp.Value;
        return await StartCoreAsync(cluster, nodeId, memory, options, configure);
    }

    private static async Task<TestService> StartCoreAsync(
        InMemoryServiceCluster? cluster, string nodeId, MemoryConnector? memory,
        Action<ServiceOptions>? options, Action<DbspServiceBuilder>? configure)
    {
        memory ??= new MemoryConnector();
        var log = new TestLog();
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Information).AddProvider(log));
        services.AddSingleton(memory);
        var builder = services.AddDbspService(o =>
            {
                o.CatalogPollInterval = TimeSpan.FromMilliseconds(20);
                o.BlockedRetryInterval = TimeSpan.FromMilliseconds(50);
                o.FollowerRetention = TimeSpan.Zero;
                o.CacheDirectory = Path.Combine(Path.GetTempPath(), "dbsp-service-tests", Guid.NewGuid().ToString("N"));
                o.Restart = FastRestart;
                options?.Invoke(o);
            })
            .UseInMemoryBackend(cluster ?? new InMemoryServiceCluster(), nodeId)
            .AddContracts(typeof(Instrument).Assembly)
            .AddPlugin(typeof(TestService).Assembly);
        configure?.Invoke(builder);
        var provider = services.BuildServiceProvider();
        var node = provider.GetRequiredService<ServiceNode>();
        await node.StartAsync(default).WaitAsync(Timeout);
        return new TestService { Services = provider, Node = node, Memory = memory, Log = log };
    }

    public static RestartPolicy FastRestart => new()
    {
        MaxRestarts = 3,
        Within = TimeSpan.FromMinutes(1),
        InitialBackoff = TimeSpan.FromMilliseconds(40),
        MaxBackoff = TimeSpan.FromMilliseconds(200),
    };

    /// <summary>The plugin compiled into the test host: this assembly.</summary>
    public const string BuiltIn = "dbsp.service.tests";

    public static JsonElement Json(string json) => JsonDocument.Parse(json).RootElement.Clone();

    public static JsonElement MemoryBinding(string name) => Json($$"""{ "connector": "memory", "name": "{{name}}" }""");

    public static PipelineSpec Spec(
        string id, string definition, string? input = null, string? sink = null, string sinkSlot = "out", string? settings = null,
        string plugin = BuiltIn, string version = "") => new()
    {
        Id = id,
        Plugin = plugin,
        Version = version,
        Definition = definition,
        Settings = settings is null ? null : Json(settings),
        Inputs = input is null ? new Dictionary<string, JsonElement>() : new Dictionary<string, JsonElement> { ["trades"] = MemoryBinding(input) },
        Sinks = sink is null ? new Dictionary<string, JsonElement>() : new Dictionary<string, JsonElement> { [sinkSlot] = MemoryBinding(sink) },
    };

    public Task<DeployResult> DeployAsync(PipelineSpec spec, StateChoice state = StateChoice.Unspecified) =>
        Pipelines.DeployAsync(spec, new DeployOptions(state), default).WaitAsync(Timeout);

    public async Task<SharedTable<int, Instrument>> DeclareInstrumentsAsync(IEnumerable<Instrument>? rows = null, string name = "instruments")
    {
        await Tables.DeclareAsync(new TableSpec { Name = name, Definition = typeof(InstrumentsTable).FullName! }, default);
        var table = Tables.Get<int, Instrument>(name);
        if (rows is not null)
            await table.Writer.ReplaceAllAsync(rows);
        return table;
    }

    public static async Task Eventually(Func<bool> condition, string what, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? Timeout);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                Assert.Fail($"Timed out waiting for: {what}");
            await Task.Delay(10);
        }
    }

    /// <summary>
    /// Waits until every running pipeline of this node has committed what it has read and
    /// no sink of it is inside a write. A step's output is in the sink before the step is
    /// committed (commit after delivery), so "the sink has it" says nothing yet about the
    /// checkpoint: a test that has seen the output and goes on to something that reads
    /// the checkpoint — a takeover or a restart that must resume and not replay, a
    /// replace that keeps state, a figure taken at a commit — waits for this in between.
    /// (Found with the sink's acknowledgement held back half a second after its rows
    /// were visible: eight tests failed that had gone on at once.)
    /// </summary>
    public Task Settled() => Eventually(
        () => Pipelines.Pipelines.Where(p => p.State == PipelineState.Running).All(p => IsSettled(Pipelines.Get(p.Id))),
        "every running pipeline to have committed what it has read");

    private static bool IsSettled(PipelineStatus status) =>
        status.Sinks.All(sink => sink.WriteInFlightSince is null)
        && status.Sources.All(source =>
            source.Stats.Lag is null or 0
            && (source.Stats.Positions ?? new Dictionary<int, long>()).All(at =>
                at.Value < 0 || status.CommittedOffsets.GetValueOrDefault($"{source.Slot}#p{at.Key}", -1) >= at.Value));

    public Task Running(string id) => Eventually(() => Pipelines.Get(id).State == PipelineState.Running, $"pipeline {id} running (it is {Pipelines.Get(id).State})");

    public async ValueTask DisposeAsync()
    {
        await Node.DisposeAsync().AsTask().WaitAsync(Timeout);
        await Services.DisposeAsync();
    }

    /// <summary>Where a test plugin project's build put its package.</summary>
    public static string PluginDirectory(string project)
    {
        // …/tests/Dbsp.Service.Tests/bin/{Configuration}/net10.0/
        var output = new DirectoryInfo(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar));
        var configuration = output.Parent!.Name;
        var tests = output.Parent!.Parent!.Parent!.Parent!.FullName;
        var directory = Path.Combine(tests, "Dbsp.Service.TestPlugins", project, "bin", configuration, output.Name);
        Assert.True(Directory.Exists(directory), $"Plugin project output not found: {directory}");
        return directory;
    }
}

/// <summary>Keeps what the service logged, so a failing test can show it.</summary>
internal sealed class TestLog : ILoggerProvider
{
    private readonly List<string> _lines = [];

    public IReadOnlyList<string> Lines
    {
        get
        {
            lock (_lines)
                return [.. _lines];
        }
    }

    public ILogger CreateLogger(string categoryName) => new Logger(this, categoryName);

    public void Dispose()
    {
    }

    private sealed class Logger(TestLog owner, string category) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (owner._lines)
                owner._lines.Add($"{logLevel} {category}: {formatter(state, exception)}{(exception is null ? "" : " — " + exception.GetType().Name + ": " + exception.Message)}");
        }
    }
}
