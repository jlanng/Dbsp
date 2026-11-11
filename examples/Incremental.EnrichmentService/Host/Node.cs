using System.Net;
using Confluent.Kafka;
using Enrichment.Contracts;
using Incremental.Kafka;
using Incremental.Service;
using Incremental.Service.Admin;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;

namespace Incremental.EnrichmentService;

/// <summary>One service node with its web server: the admin UI at <c>/admin/</c>, health
/// at <c>/health</c>, and the example's views at <c>/views</c>.</summary>
public sealed class ExampleNode : IAsyncDisposable
{
    private ExampleNode(WebApplication app, string url)
    {
        App = app;
        Url = url;
        Node = app.Services.GetRequiredService<ServiceNode>();
    }

    public WebApplication App { get; }
    public ServiceNode Node { get; }
    public string Url { get; }
    public string AdminUrl => Url + "/admin/";

    /// <param name="nodeId">The node's name in logs, events and the leader record.</param>
    /// <param name="cluster">The in-memory cluster two in-process nodes share; null on Kafka.</param>
    /// <param name="probePort">Kafka only: where this node answers its peer's "who are you" probe.</param>
    /// <param name="peerProbePort">Kafka only: where the other node answers.</param>
    public static async Task<ExampleNode> StartAsync(
        World world, ExampleOptions options, string nodeId, int port,
        InMemoryServiceCluster? cluster = null, int probePort = 0, int peerProbePort = 0, bool exitOnLeadershipLoss = true)
    {
        var specs = new Specs(world.Kafka);
        var builder = WebApplication.CreateBuilder();
        builder.Logging.SetMinimumLevel(options.Verbose ? LogLevel.Information : LogLevel.Warning);
        if (options.Quiet)
            builder.Logging.ClearProviders();
        builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
        // A node drains its pipelines when the host stops; give it longer than the default 30 s
        // only if your sinks are slow. Here the default is plenty.

        // What the adapters read and write. In a real host these are clients of real systems.
        builder.Services.AddSingleton(world);

        // Two nodes in one process share the process's meters: one of them exports.
        if (options.Otlp && (cluster is null || nodeId == "a"))
        {
            // <snippet:monitoring>
            // The libraries publish two System.Diagnostics.Metrics meters and reference no
            // exporter; the host chooses one. OTLP here, to whatever collector the standard
            // variables name (OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_PROTOCOL,
            // OTEL_METRIC_EXPORT_INTERVAL).
            builder.Services.AddOpenTelemetry()
                .ConfigureResource(resource => resource.AddService("enrichment-service", serviceInstanceId: nodeId))
                .WithMetrics(metrics => metrics
                    .AddMeter(ServiceMeter.Name)        // "Incremental.Service": pipelines, tables, role, readiness, takeovers
                    .AddMeter("Incremental")            // the core: steps, records, durations and queues per circuit
                    .AddOtlpExporter());
            // </snippet:monitoring>
        }

        // <snippet:host>
        var service = builder.Services.AddIncrementalService(o =>
            {
                o.PluginDirectory = Path.Combine(AppContext.BaseDirectory, "plugins");     // imported at first lead
                o.CacheDirectory = Path.Combine(world.DataDirectory, "cache-" + nodeId);    // one per node
                o.AdvertisedAdminUrl = $"http://127.0.0.1:{port}/admin/";                   // where a standby's UI sends you
                o.ExitOnLeadershipLoss = exitOnLeadershipLoss;
            })
            .AddContracts(typeof(Instrument).Assembly)                                      // row types and table definitions
            .AddObjectStore("refdata", _ => new LocalObjectStore(Path.Combine(world.DataDirectory, "refdata")))
            .AddObjectStore("lake", _ => world.Lake)                                        // quote files in, alerts and dead letters out
            .AddTableSource("instrument-feed", _ => new InstrumentFeedSourceFactory(world.Instruments))
            .AddTableSource("directory", _ => new DirectorySourceFactory(world.Traders, world.Desks))
            .AddSourceConnector("trade-feed", _ => new TradeFeedConnector(world.Trades))
            .AddSinkConnector("view", _ => new ViewConnector(world.Views));
        // </snippet:host>

        if (world.Kafka is { } kafka)
        {
            // <snippet:kafka-backend>
            // Two processes are configured alike except for the node id and the probe ports.
            // The log's cluster is also the Kafka connection "log", which specs may name.
            service.UseKafkaBackend(new KafkaServiceBackendOptions
            {
                Log = new KafkaLogOptions
                {
                    BootstrapServers = kafka.Bootstrap,
                    TopicPrefix = kafka.Prefix,
                    NodeId = nodeId,
                    ReplicationFactor = 1,              // the local stack has one broker; 3 in production
                    StatePartitions = 3,
                },
                TakeoverAfter = TimeSpan.FromSeconds(3),                                    // silence before a standby takes over
                ProbeListen = new IPEndPoint(IPAddress.Loopback, probePort),
                PeerProbe = ("127.0.0.1", peerProbePort),
            });
            // </snippet:kafka-backend>
        }
        else if (cluster is not null)
            service.UseInMemoryBackend(cluster, nodeId);
        else
            service.UseInMemoryBackend(nodeId: nodeId);

        // Seeds: written to the catalog once, by the first leader that finds them not yet
        // applied. The catalog records that, so after it these lines do nothing — also
        // for a seeded table or pipeline that an operator has since removed.
        specs.AddTables(service);
        if (options.SeedPipelines)
        {
            foreach (var spec in specs.Seeds())
                service.AddPipeline(spec);
        }

        // <snippet:admin>
        builder.Services.AddIncrementalAdmin(o =>
        {
            // With no authorization policy the admin API answers loopback requests only.
            o.ExposeRowData = true;             // off by default: rows and exception messages are tenant data
            o.AllowPluginUpload = true;         // off by default: an upload is code that will run in this process
            o.AllowPluginRescan = true;
        });

        var app = builder.Build();
        app.MapIncrementalAdmin("/admin");      // the UI at /admin/, the API at /admin/api
        app.MapIncrementalHealth("/health");    // /health/live, /health/ready, /health/status — outside the loopback rule
        // </snippet:admin>
        app.MapGet("/", () => Results.Redirect("/admin/"));
        app.MapGet("/views", () => Results.Json(world.Views.Names.Select(n => new { name = n, rows = world.Views.Find(n)!.Count })));
        app.MapGet("/views/{name}", (string name, int? limit) => world.Views.Find(name) is { } view
            ? Results.Text(
                "[" + string.Join(",\n", view.Page(Math.Clamp(limit ?? 100, 1, 1000)).Select(r => $"{{\"key\":{r.Key},\"row\":{r.Value}}}")) + "]",
                "application/json")
            : Results.NotFound());

        await app.StartAsync();
        return new ExampleNode(app, $"http://127.0.0.1:{port}");
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await App.StopAsync(limit.Token);
        }
        catch (OperationCanceledException)
        {
        }
        await App.DisposeAsync();
    }
}

/// <summary>
/// Not run by the example: how the same host is pointed at Amazon MSK with IAM
/// authentication. Client settings and the token callback stay in the host; a spec only
/// ever names a connection. See docs/enrichment-service.md, "MSK".
/// </summary>
public static class MskConfiguration
{
    // <snippet:msk>
    /// <param name="token">Returns a signed IAM token and its expiry — from the
    /// AWS.MSK.Auth package's <c>AWSMSKAuthTokenGenerator</c>, which this repository does not reference.</param>
    public static void UseMsk(
        IncrementalServiceBuilder service, string bootstrapServers, string nodeId, Func<(string Token, long ExpiryMs)> token)
    {
        void Refresh(IClient client, string _)
        {
            try
            {
                var (value, expiryMs) = token();
                client.OAuthBearerSetToken(value, expiryMs, "incremental-service");
            }
            catch (Exception ex)
            {
                client.OAuthBearerSetTokenFailure(ex.Message);
            }
        }

        var log = new KafkaLogOptions
        {
            BootstrapServers = bootstrapServers,        // the cluster's IAM bootstrap string (port 9098)
            TopicPrefix = "enrichment",
            NodeId = nodeId,
            ReplicationFactor = 3,
            // librdkafka settings, applied to every client the log makes.
            ClientConfig = new Dictionary<string, string>
            {
                ["security.protocol"] = "SASL_SSL",
                ["sasl.mechanism"] = "OAUTHBEARER",
            },
            // What a dictionary cannot carry: the token refresh, on every client. The admin
            // client is one of its own (it creates and describes the topics) and needs it too.
            ConfigureProducer = producer => producer.SetOAuthBearerTokenRefreshHandler(Refresh),
            ConfigureConsumer = consumer => consumer.SetOAuthBearerTokenRefreshHandler(Refresh),
            ConfigureAdmin = admin => admin.SetOAuthBearerTokenRefreshHandler(Refresh),
        };
        service.UseKafkaBackend(new KafkaServiceBackendOptions { Log = log });      // also the connection "log"

        // A second cluster that pipelines may read and write, restricted to its own topics.
        service.AddKafkaConnection("market-data", _ =>
        {
            var connection = new KafkaConnection
            {
                BootstrapServers = "b-1.marketdata.example:9098",
                ConfigureProducer = producer => producer.SetOAuthBearerTokenRefreshHandler(Refresh),
                ConfigureConsumer = consumer => consumer.SetOAuthBearerTokenRefreshHandler(Refresh),
            };
            connection.ClientConfig["security.protocol"] = "SASL_SSL";
            connection.ClientConfig["sasl.mechanism"] = "OAUTHBEARER";
            connection.AllowedTopicPrefixes.Add("md.");
            return connection;
        });
    }
    // </snippet:msk>
}
