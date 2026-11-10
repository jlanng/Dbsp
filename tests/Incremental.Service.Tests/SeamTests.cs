using System.Text.Json;
using Acme.Contracts;
using Microsoft.Extensions.DependencyInjection;
using static Incremental.Service.Tests.TestService;

namespace Incremental.Service.Tests;

/// <summary>What the admin API reads of a node that S1 and S2 did not have: the metrics
/// rings, rows of a table by scan, key and index, the circuit's graph, a pipeline's
/// history, a redacted spec, and dead letters paged from the newest.</summary>
public class SeamTests
{
    private static List<Instrument> Instruments(int count) =>
        Enumerable.Range(1, count).Select(i => new Instrument(i, $"S{i}", $"ISIN{i % 10}", 1 + i % 3)).ToList();

    [Fact]
    public async Task Metrics_count_what_a_pipeline_took_in_and_delivered_and_forget_a_removed_pipeline()
    {
        await using var service = await StartAsync();
        await service.DeclareInstrumentsAsync(Instruments(20));
        await service.DeployAsync(Spec("m-enrich", "enrich", "m-in", "m-out"));
        await service.DeployAsync(Spec("m-idle", "counts", "m-idle-in", "m-idle-out"));
        await service.Running("m-enrich");
        var log = service.Memory.Log<Trade>("m-in");
        for (var i = 1; i <= 500; i++)
            log.Append(new Trade(i, 1 + i % 20, 1, "t"));
        var sink = service.Memory.UpsertSink<long, EnrichedTrade>("m-out");
        await Eventually(() => sink.Rows.Count == 500, "the output");

        var metrics = service.Node.Metrics;
        await Eventually(() => metrics.Pipeline("m-enrich").Seconds.Sum(s => s.RecordsOut) == 500, "500 records out in the rings");
        var samples = metrics.Pipeline("m-enrich").Seconds;
        Assert.Equal(500, samples.Sum(s => s.RecordsIn));
        Assert.True(samples.Sum(s => s.Steps) > 0);
        Assert.True(samples.Sum(s => s.Commits) > 0);
        Assert.True(samples.Sum(s => s.StepSeconds) > 0);
        Assert.True(samples.Max(s => s.StepMaxSeconds) <= samples.Sum(s => s.StepSeconds));
        Assert.NotNull(samples[^1].CheckpointAgeSeconds);
        Assert.InRange(samples.Count, 1, 60);

        // The other pipeline's numbers are its own; the node's are the sum.
        Assert.Equal(0, metrics.Pipeline("m-idle").Seconds.Sum(s => s.RecordsIn));
        Assert.Equal(500, metrics.Node.Seconds.Sum(s => s.RecordsIn));
        Assert.Contains("m-enrich", metrics.Latest.Keys);
        Assert.Contains("", metrics.Latest.Keys);

        // A ring never holds more than its sixty slots.
        for (var i = 0; i < 130; i++)
            metrics.Sample(DateTimeOffset.UtcNow.AddSeconds(i + 1));
        Assert.Equal(60, metrics.Pipeline("m-enrich").Seconds.Count);
        Assert.InRange(metrics.Pipeline("m-enrich").Minutes.Count, 2, 60);
        Assert.Equal(500, metrics.Pipeline("m-enrich").Minutes.Sum(s => s.RecordsIn));

        await service.Pipelines.RemoveAsync("m-enrich", new ActionOptions(), default);
        metrics.Sample(DateTimeOffset.UtcNow.AddSeconds(200));
        Assert.Empty(metrics.Pipeline("m-enrich").Seconds);
        Assert.DoesNotContain("m-enrich", metrics.Latest.Keys);
    }

    [Fact]
    public async Task Rows_are_read_by_scan_by_key_and_by_index_within_the_bounds_asked_for()
    {
        await using var service = await StartAsync();
        var table = await service.DeclareInstrumentsAsync(Instruments(100));

        var first = service.Tables.ReadRows("instruments", new TableRowQuery(Limit: 30));
        Assert.Equal(30, first.Rows.Count);
        Assert.True(first.More);
        Assert.False(first.Truncated);
        Assert.Equal(100, first.TableRows);
        Assert.Equal(table.LatestVersion, first.Version);

        // Pages of one scan cover the table once.
        var keys = new HashSet<int>();
        for (var skip = 0; ; skip += 30)
        {
            var page = service.Tables.ReadRows("instruments", new TableRowQuery(Limit: 30, Skip: skip));
            foreach (var row in page.Rows)
                Assert.True(keys.Add(row.Key.GetInt32()));
            if (!page.More)
                break;
        }
        Assert.Equal(100, keys.Count);

        // The scan stops at its bound and says so.
        var bounded = service.Tables.ReadRows("instruments", new TableRowQuery(Limit: 50, MaxScanned: 20));
        Assert.Equal(20, bounded.Rows.Count);
        Assert.Equal(20, bounded.Scanned);
        Assert.True(bounded.Truncated);
        var filtered = service.Tables.ReadRows("instruments", new TableRowQuery(Limit: 50, Contains: "\"symbol\":\"s77\""));
        Assert.Equal(77, Assert.Single(filtered.Rows).Key.GetInt32());
        Assert.Equal(100, filtered.Scanned);

        var byKey = service.Tables.ReadRows("instruments", new TableRowQuery(Key: "42"));
        Assert.Equal("S42", Assert.Single(byKey.Rows).Row.GetProperty("symbol").GetString());
        Assert.Empty(service.Tables.ReadRows("instruments", new TableRowQuery(Key: "4200")).Rows);
        Assert.Equal("invalid", Assert.Throws<ServiceException>(() => service.Tables.ReadRows("instruments", new TableRowQuery(Key: "forty-two"))).Code);

        // An index key that is a string may be given bare or as JSON.
        var byIndex = service.Tables.ReadRows("instruments", new TableRowQuery(Limit: 4, Index: "isin", Key: "ISIN7"));
        Assert.Equal(4, byIndex.Rows.Count);
        Assert.True(byIndex.More);
        Assert.Equal(10, byIndex.Scanned);
        Assert.All(byIndex.Rows, r => Assert.Equal("ISIN7", r.Row.GetProperty("isin").GetString()));
        Assert.Equal(10, service.Tables.ReadRows("instruments", new TableRowQuery(Index: "isin", Key: "\"ISIN7\"")).Rows.Count);
        Assert.Equal("invalid", Assert.Throws<ServiceException>(() => service.Tables.ReadRows("instruments", new TableRowQuery(Index: "nope", Key: "x"))).Code);
        Assert.Equal(404, Assert.Throws<ServiceException>(() => service.Tables.ReadRows("nope", new TableRowQuery())).Status);

        var stats = await service.Tables.StatsAsync("instruments");
        Assert.Equal("String", Assert.Single(stats.IndexKeyTypes!).Value);
        Assert.Equal("instruments", service.Tables.Spec("instruments").Name);
    }

    [Fact]
    public async Task A_pipeline_has_a_graph_a_history_and_dead_letters_paged_from_the_newest()
    {
        await using var service = await StartAsync();
        await service.DeclareInstrumentsAsync(Instruments(5));
        var spec = Spec("g-enrich", "enrich", "g-in", "g-out");

        var described = await service.Pipelines.DescribeAsync(spec, default);
        Assert.NotEqual("", described.Spec.Version);
        var graph = described.Graph;
        Assert.Contains(graph.Circuit.Nodes, n => n.Kind == "input[trades]");
        Assert.Contains(graph.Circuit.Nodes, n => n.Kind == "table[instruments]");
        Assert.Contains(graph.Circuit.Nodes, n => n.Name == "joined");
        var output = Assert.Single(graph.Circuit.Outputs);
        Assert.Equal("out", graph.SinkSlots[output.Name]);
        // Each kind of sink slot names its output: keyed rows, and an append sink.
        var counts = (await service.Pipelines.DescribeAsync(Spec("g-counts", "counts", "g-in", "g-counts-out"), default)).Graph;
        Assert.Equal("out", counts.SinkSlots[Assert.Single(counts.Circuit.Outputs).Name]);
        var appends = (await service.Pipelines.DescribeAsync(Spec("g-wedge", "wedge", "g-in", "g-wedge-out"), default)).Graph;
        Assert.Equal("out", appends.SinkSlots[Assert.Single(appends.Circuit.Outputs).Name]);
        Assert.Equal("ServiceException", (await Assert.ThrowsAsync<ServiceException>(() => service.Pipelines.DescribeAsync(spec with { Definition = "nope" }, default))).GetType().Name);

        await service.DeployAsync(spec);
        await service.Running("g-enrich");
        Assert.Equal(graph.Circuit.Nodes.Count, service.Pipelines.Graph("g-enrich")!.Circuit.Nodes.Count);
        var history = service.Pipelines.Get("g-enrich").History!;
        Assert.Equal([PipelineState.Starting, PipelineState.Running], history.Select(h => h.State));
        await service.Pipelines.StopAsync("g-enrich", new ActionOptions(), default);
        Assert.Equal(PipelineState.Stopped, service.Pipelines.Get("g-enrich").History![^1].State);

        // Dead letters: forwards from a sequence, and backwards from the newest.
        await service.DeployAsync(Spec("g-lenient", "fragile", "g-bad", "g-bad-out") with { Circuit = new PipelineCircuitOptions { ErrorPolicy = RecordErrorPolicy.DeadLetter } });
        await service.Running("g-lenient");
        var bad = service.Memory.Log<Trade>("g-bad");
        for (var i = 1; i <= 7; i++)
            bad.Append(new Trade(i, 1, -1, "t"));
        await Eventually(() => service.Pipelines.Get("g-lenient").DeadLetters == 7, "seven dead letters");
        Assert.Equal([5L, 6L, 7L], service.Pipelines.DeadLettersBefore("g-lenient", limit: 3).Select(l => l.Sequence));
        Assert.Equal([2L, 3L, 4L], service.Pipelines.DeadLettersBefore("g-lenient", 5, 3).Select(l => l.Sequence));
        Assert.Equal([1L], service.Pipelines.DeadLettersBefore("g-lenient", 2, 3).Select(l => l.Sequence));
        Assert.Equal([3L, 4L], service.Pipelines.DeadLetters("g-lenient", 2, 2).Select(l => l.Sequence));
        // The operator a dead letter names is a node of the graph, by its state name.
        var letter = service.Pipelines.DeadLetters("g-lenient")[0];
        Assert.Contains(service.Pipelines.Graph("g-lenient")!.Circuit.Nodes, n => n.StateName == letter.Operator);
    }

    private sealed class SecretiveConnector : IUpsertSinkConnector
    {
        public ConnectorDescription Describe(JsonElement config) => new("secretive", ["password", "token"]);

        public IUpsertSink<TKey, TRow> Create<TKey, TRow>(SinkContext context) where TKey : notnull => new InMemoryUpsertSink<TKey, TRow>();
    }

    [Fact]
    public async Task A_spec_is_shown_with_the_properties_its_connector_calls_secret_blanked()
    {
        await using var service = await StartAsync(configure: b => b.AddSinkConnector("secretive", _ => new SecretiveConnector()));
        await service.DeclareInstrumentsAsync(Instruments(3));
        var spec = Spec("r-enrich", "enrich", "r-in") with
        {
            Sinks = new Dictionary<string, JsonElement> { ["out"] = Json("""{ "connector": "secretive", "host": "db", "password": "hunter2", "token": "t0k" }""") },
        };
        await service.DeployAsync(spec);

        var shown = service.Pipelines.RedactedSpec("r-enrich").Sinks["out"];
        Assert.Equal("db", shown.GetProperty("host").GetString());
        Assert.Equal("***", shown.GetProperty("password").GetString());
        Assert.Equal("***", shown.GetProperty("token").GetString());
        Assert.DoesNotContain("hunter2", JsonSerializer.Serialize(service.Pipelines.RedactedSpec("r-enrich")));
        // What is stored, and what a config without secrets shows, is untouched.
        Assert.Equal("hunter2", service.Pipelines.Spec("r-enrich").Sinks["out"].GetProperty("password").GetString());
        Assert.Equal("r-in", service.Pipelines.RedactedSpec("r-enrich").Inputs["trades"].GetProperty("name").GetString());
    }

    [Fact]
    public async Task A_standby_has_no_graph_and_counts_nothing_for_circuits_the_leader_runs()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        await using var leader = await StartAsync(cluster, "a", memory);
        await leader.DeclareInstrumentsAsync(Instruments(5));
        await leader.DeployAsync(Spec("s-enrich", "enrich", "s-in", "s-out"));
        await using var standby = await StartAsync(cluster, "b", memory);
        await Eventually(() => standby.Pipelines.Pipelines.Any(p => p.Id == "s-enrich"), "the standby sees the pipeline");
        Assert.Null(standby.Pipelines.Graph("s-enrich"));

        var log = memory.Log<Trade>("s-in");
        for (var i = 1; i <= 50; i++)
            log.Append(new Trade(i, 1, 1, "t"));
        await Eventually(() => leader.Node.Metrics.Pipeline("s-enrich").Seconds.Sum(s => s.RecordsIn) == 50, "the leader's count");
        standby.Node.Metrics.Sample(DateTimeOffset.UtcNow.AddSeconds(1));
        Assert.Equal(0, standby.Node.Metrics.Pipeline("s-enrich").Seconds.Sum(s => s.RecordsIn + s.Steps));
        // Rows are served by a standby from its own copy.
        await Eventually(() => standby.Tables.Tables.Any(t => t.Name == "instruments" && t.State == TableState.Open), "the standby follows the table");
        Assert.Equal(5, standby.Tables.ReadRows("instruments", new TableRowQuery()).Rows.Count);
    }
}
