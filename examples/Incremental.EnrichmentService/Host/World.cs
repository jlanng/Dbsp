using System.Text.Json;
using Enrichment.Contracts;
using Incremental.Service;

namespace Incremental.EnrichmentService;

/// <summary>The command line, parsed.</summary>
public sealed class ExampleOptions
{
    public bool Kafka { get; init; }
    public bool Headless { get; init; }
    public int Nodes { get; init; } = 1;

    /// <summary><c>a</c> or <c>b</c>: which of the two Kafka nodes this process is.</summary>
    public string Node { get; init; } = "a";
    public int Port { get; init; } = 5180;

    /// <summary>Kafka: where this node answers its peer's probe, and where its peer answers.</summary>
    public int ProbePort { get; init; } = 5190;
    public int PeerProbePort { get; init; } = 5191;
    public int Rate { get; init; } = 50;

    /// <summary>One generated trade in this many has no quantity, which one definition
    /// refuses: it is what fills the dead-letter page. 0: none.</summary>
    public int Poison { get; init; } = 200;

    /// <summary>The Kafka topic prefix of this deployment, and the prefix of its Postgres tables.</summary>
    public string Prefix { get; init; } = "enrich";
    public string Bootstrap { get; init; } = "localhost:9092";
    public string Postgres { get; init; } = "Host=localhost;Port=5432;Username=incremental;Password=incremental;Database=incremental";

    /// <summary>The Postgres host as Kafka Connect reaches it (inside the compose network).</summary>
    public string PostgresHostForConnect { get; init; } = "postgres";
    public string Connect { get; init; } = "http://localhost:8083";
    public string? DataDirectory { get; init; }

    /// <summary>Seed the example's pipelines at first lead. The smoke test deploys its own.</summary>
    public bool SeedPipelines { get; init; } = true;
    public bool Generate { get; init; } = true;

    /// <summary>Kafka: create the Postgres tables, the connector and the topics if they are
    /// missing. The smoke test's child node is told not to: its parent already has.</summary>
    public bool PrepareStack { get; init; } = true;
    public bool Verbose { get; init; }

    /// <summary>Export the meters over OTLP. Where to is the exporter's own configuration:
    /// <c>OTEL_EXPORTER_OTLP_ENDPOINT</c> and its companions.</summary>
    public bool Otlp { get; init; }

    /// <summary>No log output at all: the smoke test's nodes, whose story its claims tell.</summary>
    public bool Quiet { get; init; }

    public static ExampleOptions Parse(string[] args)
    {
        string? Value(string name)
        {
            var i = Array.IndexOf(args, name);
            return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
        }
        var defaults = new ExampleOptions();
        var node = Value("--node") ?? "a";
        if (node is not ("a" or "b"))
            throw new ArgumentException("--node is a or b.");
        var headless = args.Contains("--headless");
        return new ExampleOptions
        {
            Kafka = args.Contains("--kafka"),
            Headless = headless,
            Nodes = int.Parse(Value("--nodes") ?? "1"),
            Node = node,
            Port = int.Parse(Value("--port") ?? (node == "b" ? "5181" : "5180")),
            ProbePort = int.Parse(Value("--probe-port") ?? (node == "b" ? "5191" : "5190")),
            PeerProbePort = int.Parse(Value("--peer-probe-port") ?? (node == "b" ? "5190" : "5191")),
            Rate = int.Parse(Value("--rate") ?? "50"),
            Poison = int.Parse(Value("--poison") ?? "200"),
            Prefix = Value("--prefix") ?? defaults.Prefix,
            Bootstrap = Value("--bootstrap") ?? defaults.Bootstrap,
            Postgres = Value("--postgres") ?? defaults.Postgres,
            PostgresHostForConnect = Value("--postgres-host-for-connect") ?? defaults.PostgresHostForConnect,
            Connect = Value("--connect") ?? defaults.Connect,
            DataDirectory = Value("--data"),
            SeedPipelines = !headless && !args.Contains("--no-pipelines"),
            Generate = !headless && !args.Contains("--no-generate"),
            PrepareStack = !args.Contains("--no-prepare"),
            Verbose = args.Contains("--verbose"),
            Otlp = args.Contains("--otlp"),
        };
    }
}

/// <summary>
/// What stands outside the service in this example: the feeds and directories the adapters
/// read, the file the counterparties come from, and the views the sinks write. One per
/// process; two in-process nodes share it, as two real nodes share the systems around them.
/// </summary>
public sealed class World
{
    public World(WorldShape shape, string dataDirectory, KafkaStack? kafka, bool freshData)
    {
        Shape = shape;
        DataDirectory = dataDirectory;
        Kafka = kafka;
        Trades = new PositionedFeed<Trade>(shape.TradeAt);
        Instruments = new PositionedFeed<Instrument>(shape.InstrumentChangeAt);
        Traders = new LiveDirectory<int, Trader>(t => t.Id);
        Desks = new LiveDirectory<int, Desk>(d => d.Id);
        for (var i = 1; i <= shape.Desks; i++)
            Desks.Upsert(shape.Desk(i));
        for (var i = 1; i <= shape.Traders; i++)
            Traders.Upsert(shape.Trader(i));
        Instruments.Publish(shape.Instruments);                      // the feed's first records: every instrument once
        var refdata = Path.Combine(dataDirectory, "refdata");
        Counterparties = new CounterpartyFile(refdata, shape, overwrite: freshData);
        Sanctions = new WatchlistFile(refdata, "sanctions.jsonl", shape.Sanctions, overwrite: freshData);
        CreditWatch = new WatchlistFile(refdata, "credit-watch.jsonl", shape.CreditWatch, overwrite: freshData);
        LakeDirectory = Path.Combine(dataDirectory, "lake");
        Directory.CreateDirectory(LakeDirectory);
        Lake = new LocalObjectStore(LakeDirectory);
    }

    public WorldShape Shape { get; }
    public string DataDirectory { get; }
    public KafkaStack? Kafka { get; }
    public PositionedFeed<Trade> Trades { get; }
    public PositionedFeed<Instrument> Instruments { get; }
    public LiveDirectory<int, Trader> Traders { get; }
    public LiveDirectory<int, Desk> Desks { get; }
    public CounterpartyFile Counterparties { get; }
    public WatchlistFile Sanctions { get; }
    public WatchlistFile CreditWatch { get; }
    public Views Views { get; } = new();

    /// <summary>A directory that stands in for a bucket: the quote files land under
    /// <c>quotes/</c> (in memory), and the alert pipelines write under <c>alerts/</c> and
    /// <c>dead-letters/</c>. The nodes reach it as the object store <c>lake</c>.</summary>
    public string LakeDirectory { get; }
    public LocalObjectStore Lake { get; }

    // ── the quote feed ─────────────────────────────────────────────────────────────────

    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);
    private long _quoteIntervals = -1;

    /// <summary>How many intervals of quotes have been published; read from the topic
    /// first, in the Kafka mode, by a process that has just started generating.</summary>
    public long QuoteIntervals => Interlocked.Read(ref _quoteIntervals) is >= 0 and var known ? known : 0;

    /// <summary>Another process may have published since: ask again next time.</summary>
    public void ForgetQuotePosition() => Interlocked.Exchange(ref _quoteIntervals, -1);

    /// <summary>
    /// Publishes every interval of quotes that starts at or before <paramref name="time"/>
    /// and is not out yet. In memory an interval is one JSON-lines file under
    /// <c>quotes/</c>, written whole and moved into place, which the <c>files</c>
    /// connector reads in name order; on Kafka it is that many records on the quotes topic.
    /// </summary>
    public async Task PublishQuotesThroughAsync(DateTimeOffset time)
    {
        var next = Interlocked.Read(ref _quoteIntervals);
        if (next < 0)
            next = Kafka is null ? 0 : await Task.Run(() => Kafka.QuotesPublished() / Shape.QuotesPerInterval);
        for (; WorldShape.Epoch + Shape.QuoteInterval * next <= time; next++)
        {
            var first = next * Shape.QuotesPerInterval;
            var quotes = Enumerable.Range(0, Shape.QuotesPerInterval).Select(i => Shape.QuoteAt(first + i));
            if (Kafka is not null)
                await Kafka.ProduceQuotesAsync(quotes);
            else
            {
                await using (var file = await Lake.OpenWriteAsync($"quotes/{next:D8}.jsonl", default))
                    await file.WriteAsync(System.Text.Encoding.UTF8.GetBytes(string.Concat(quotes.Select(q => JsonSerializer.Serialize(q, Web) + "\n"))));
                // The source's position outlives the files it names: old ones can go.
                if (next >= KeptQuoteFiles)
                    await Lake.DeleteAsync($"quotes/{next - KeptQuoteFiles:D8}.jsonl", default);
            }
            Interlocked.Exchange(ref _quoteIntervals, next + 1);
        }
        Interlocked.CompareExchange(ref _quoteIntervals, next, -1);
    }

    public const int KeptQuoteFiles = 30;
}

/// <summary>The example's table and pipeline specs. A spec is data: it names a definition,
/// its settings, and what each of the definition's slots is bound to — and the bindings are
/// the only thing that differs between the in-memory mode and Kafka.</summary>
public sealed class Specs(KafkaStack? kafka, TimeSpan? fileSpacing = null)
{
    // The files sinks write one object per step; left alone that is several small objects
    // a second, so they are told to keep two seconds between objects. The smoke test,
    // which waits on what they write, passes zero.
    private readonly string _spacing = (fileSpacing ?? TimeSpan.FromSeconds(2)).ToString("c");

    public const string Plugin = "enrichment.pipelines";

    public static JsonElement Json(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private JsonElement Trades => kafka is null
        ? Json("""{ "connector": "trade-feed" }""")
        : Json($$"""{ "connector": "kafka", "connection": "log", "topic": "{{kafka.TradesTopic}}" }""");

    private JsonElement Sink(string name) => kafka is null
        ? Json($$"""{ "connector": "view", "name": "{{name}}" }""")
        : Json($$"""{ "connector": "kafka", "connection": "log", "topic": "{{kafka.SinkTopic(name)}}" }""");

    private JsonElement Quotes => kafka is null
        ? Json("""{ "connector": "files", "store": "lake", "prefix": "quotes/", "pollInterval": "00:00:00.25" }""")
        : Json($$"""{ "connector": "kafka", "connection": "log", "topic": "{{kafka.QuotesTopic}}" }""");

    private JsonElement Files(string prefix) =>
        Json($$"""{ "connector": "files", "store": "lake", "prefix": "{{prefix}}", "minInterval": "{{_spacing}}" }""");

    // <snippet:specs>
    public PipelineSpec TraderDesks(string version = "") => new()
    {
        Id = "trader-desks",
        Plugin = Plugin,
        Version = version,                  // may be empty while the plugin has one stored version
        Definition = "trader-desks",
    };

    public PipelineSpec TradeEnrichment(string id, decimal minNotional, string version = "") => new()
    {
        Id = id,
        Plugin = Plugin,
        Version = version,
        Definition = "trade-enrichment",
        Settings = Json($$"""{ "minNotional": {{minNotional.ToString(System.Globalization.CultureInfo.InvariantCulture)}}, "label": "{{id}}" }"""),
        Inputs = new Dictionary<string, JsonElement> { ["trades"] = Trades },
        Sinks = new Dictionary<string, JsonElement> { ["enriched"] = Sink(id) },
    };

    public PipelineSpec DeskExposure(string version = "") => new()
    {
        Id = "desk-exposure",
        Plugin = Plugin,
        Version = version,
        Definition = "desk-exposure",
        Settings = Json("""{ "windowSeconds": 60, "latenessSeconds": 5 }"""),
        Inputs = new Dictionary<string, JsonElement> { ["executions"] = Trades },
        Sinks = new Dictionary<string, JsonElement> { ["exposure"] = Sink("desk-exposure") },
    };
    // </snippet:specs>

    // <snippet:spec-two-inputs>
    public PipelineSpec ExecutionQuality(string version = "") => new()
    {
        Id = "execution-quality",
        Plugin = Plugin,
        Version = version,
        Definition = "execution-quality",
        Settings = Json("""{ "quoteSeconds": 60, "windowSeconds": 300, "hopSeconds": 60, "latenessSeconds": 5 }"""),
        // Two inputs, each bound on its own: the trade feed, and the quote files (or topic).
        Inputs = new Dictionary<string, JsonElement> { ["executions"] = Trades, ["quotes"] = Quotes },
        Sinks = new Dictionary<string, JsonElement> { ["quality"] = Sink("execution-quality") },
    };
    // </snippet:spec-two-inputs>

    // <snippet:spec-alerts>
    public PipelineSpec WatchlistAlerts(string id, string watchlist, decimal minNotional, string severity, string version = "") => new()
    {
        Id = id,
        Plugin = Plugin,
        Version = version,
        Definition = "watchlist-alerts",
        Settings = Json($$"""{ "minNotional": {{minNotional.ToString(System.Globalization.CultureInfo.InvariantCulture)}}, "severity": "{{severity}}" }"""),
        // A record the definition throws on is dropped and reported, not fatal…
        Circuit = new PipelineCircuitOptions { ErrorPolicy = Incremental.RecordErrorPolicy.DeadLetter },
        // …and kept: any append sink connector will do. Files in memory, a topic on Kafka.
        DeadLetters = kafka is null
            ? Files($"dead-letters/{id}/")
            : Json($$"""{ "connector": "kafka", "connection": "log", "topic": "{{kafka.DeadLetterTopic(id)}}" }"""),
        // The definition's table slot "watchlist", bound to one of the two declared watchlists.
        Tables = new Dictionary<string, string> { ["watchlist"] = watchlist },
        Inputs = new Dictionary<string, JsonElement> { ["executions"] = Trades },
        Sinks = new Dictionary<string, JsonElement> { ["alerts"] = Files($"alerts/{id}/") },
    };
    // </snippet:spec-alerts>

    public PipelineSpec SanctionsAlerts(string version = "") => WatchlistAlerts("sanctions-alerts", "sanctions", 0, "block", version);

    public PipelineSpec CreditAlerts(string version = "") => WatchlistAlerts("credit-alerts", "credit-watch", 1_000_000, "review", version);

    public IEnumerable<PipelineSpec> Seeds()
    {
        yield return TraderDesks();
        yield return TradeEnrichment("trade-enrichment", 0);
        yield return TradeEnrichment("large-trades", 20_000_000);
        yield return DeskExposure();
        yield return ExecutionQuality();
        yield return SanctionsAlerts();
        yield return CreditAlerts();
    }

    /// <summary>The tables, with what loads each. A table with no loader is written by a
    /// pipeline (<c>ToTable</c>).</summary>
    public void AddTables(IncrementalServiceBuilder service)
    {
        TableLoaderSpec File(string path) => new("file", "refdata", Json($$"""{ "path": "{{path}}", "pollInterval": "00:00:01" }"""));
        var counterparties = File(CounterpartyFile.FileName);
        // One definition, declared twice under two names: a spec's "tables" picks one.
        service
            .AddTable(Reference.Watchlist, File("sanctions.jsonl"), name: "sanctions")
            .AddTable(Reference.Watchlist, File("credit-watch.jsonl"), name: "credit-watch");
        if (kafka is null)
        {
            // <snippet:tables>
            service
                .AddTable(Reference.Instruments, new TableLoaderSpec("instrument-feed"))        // a custom source, with positions
                .AddTable(Reference.Traders, new TableLoaderSpec("directory"))                  // a custom source, snapshot + changes
                .AddTable(Reference.Desks, new TableLoaderSpec("directory"))
                .AddTable(Reference.Counterparties, counterparties)                             // the built-in file loader
                .AddTable(Reference.TraderDesks);                                               // no loader: a pipeline writes it
            // </snippet:tables>
        }
        else
        {
            TableLoaderSpec Cdc(string table) => new("debezium", "log", Json($$"""{ "topic": "{{kafka.ChangeTopic(table)}}" }"""));
            service
                .AddTable(Reference.Instruments, Cdc("instruments"))                            // Postgres → Debezium → the table
                .AddTable(Reference.Traders, Cdc("traders"))
                .AddTable(Reference.Desks, Cdc("desks"))
                .AddTable(Reference.Counterparties, counterparties)
                .AddTable(Reference.TraderDesks);
        }
    }
}
