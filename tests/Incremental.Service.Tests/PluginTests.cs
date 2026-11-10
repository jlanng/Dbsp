using System.IO.Compression;
using Acme.Contracts;
using static Incremental.Service.Tests.SupervisorTests;
using static Incremental.Service.Tests.TestService;

namespace Incremental.Service.Tests;

/// <summary>Real plugin assemblies: compiled by their own projects, never referenced by
/// this one, imported from their output directories and loaded at runtime.</summary>
public class PluginTests
{
    private const string Plugin = "acme.pipelines";

    private static bool Same<TKey, TRow>(IReadOnlyDictionary<TKey, TRow> actual, IReadOnlyDictionary<TKey, TRow> expected) where TKey : notnull =>
        actual.Count == expected.Count && expected.All(e => actual.TryGetValue(e.Key, out var row) && Equals(row, e.Value));

    private static PipelineSpec TraderCounts(string id, string version, string input, string sink) =>
        Spec(id, "trader-counts", input, sink, sinkSlot: "counts", plugin: Plugin, version: version);

    private static Dictionary<string, string> CountLines(string version, IEnumerable<Trade> trades) =>
        trades.Select(t => t.Trader + "#" + t.Id).Distinct().GroupBy(x => x[..x.IndexOf('#')])
            .ToDictionary(g => g.Key, g => $"{version}:{g.Key}={g.Count()}");

    private static async Task CollectedAsync(Func<bool> condition, string what)
    {
        for (var attempt = 0; attempt < 200 && !condition(); attempt++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            await Task.Delay(25);
        }
        Assert.True(condition(), what);
    }

    [Fact]
    public async Task Import_reads_a_plugin_package_and_is_idempotent()
    {
        await using var service = await StartAsync();
        var v1 = await service.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines"), default, "tester");

        Assert.Equal(Plugin, v1.Name);
        Assert.Equal(PluginKind.Pipeline, v1.Kind);
        Assert.Equal("Acme.Pipelines.dll", v1.MainAssembly);
        Assert.Matches("^[0-9a-f]{16}$", v1.Version);
        Assert.Equal(["Acme.Contracts.dll", "Acme.Pipelines.deps.json", "Acme.Pipelines.dll"], v1.Files.Select(f => f.Path));
        Assert.Equal(["trade-enrichment", "trader-counts"], v1.Definitions.Select(d => d.Name));

        // The settings schema a form is built from.
        var enrichment = v1.Definitions[0];
        Assert.Equal("Trades joined to instruments.", enrichment.Description);
        Assert.Equal(
            [("label", "string", true), ("minQuantity", "number", false), ("mode", "string", false)],
            enrichment.Settings.OrderBy(s => s.Name).Select(s => (s.Name, s.JsonType, s.StateNeutral)));
        Assert.Equal("Trades below this quantity are dropped.", enrichment.Settings.Single(s => s.Name == "minQuantity").Description);
        Assert.Empty(v1.Definitions[1].Settings);

        // The same bytes are the same version; nothing is written the second time.
        var version = service.Node.Catalog.Version;
        Assert.Equal(v1.Version, (await service.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines"), default)).Version);
        Assert.Equal(version, service.Node.Catalog.Version);

        // The other build of the same sources is another version of the same plugin.
        var v2 = await service.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines.V2"), default);
        Assert.Equal(Plugin, v2.Name);
        Assert.NotEqual(v1.Version, v2.Version);
        Assert.Equal(v1.Fingerprint, v2.Fingerprint); // the two versions' data shapes are identical
        Assert.Equal(2, service.Plugins.Plugins.Count(p => p.Name == Plugin));
        Assert.Contains(service.Node.Catalog.Audit(), a => a is { Action: "plugin.import", Caller: "tester" });

        // A zip of the same files is the same version, so nothing new is stored.
        using var zip = new MemoryStream();
        ZipFile.CreateFromDirectory(PluginDirectory("Acme.Pipelines"), zip);
        zip.Position = 0;
        Assert.Equal(v1.Version, (await service.Plugins.ImportZipAsync(zip, default)).Version);

        // A spec that names no version is ambiguous now.
        var ambiguous = await Assert.ThrowsAsync<ServiceException>(() => service.DeployAsync(TraderCounts("p", "", "in", "out")));
        Assert.Contains("has 2 versions", ambiguous.Message);

        // Delete: refused while a pipeline names the version.
        await service.DeployAsync(TraderCounts("p", v1.Version, "in", "out"));
        await Assert.ThrowsAsync<ServiceException>(() => service.Plugins.DeleteAsync(Plugin, v1.Version, default));
        await service.Plugins.DeleteAsync(Plugin, v2.Version, default);
        Assert.Single(service.Plugins.Plugins, p => p.Name == Plugin);
        Assert.True(await service.Node.Backend.Blobs.ExistsAsync(v1.Files[0].Sha256, default)); // shared with v1, so kept
    }

    [Fact]
    public async Task Hostile_packages_are_refused()
    {
        await using var service = await StartAsync(options: o =>
        {
            o.MaxPluginFiles = 4;
            o.MaxPluginBytes = 50_000;
        });

        static MemoryStream Zip(params (string Name, byte[] Content)[] entries)
        {
            var stream = new MemoryStream();
            using (var archive = new ZipArchive(stream, ZipArchiveMode.Create, leaveOpen: true))
                foreach (var (name, content) in entries)
                    using (var entry = archive.CreateEntry(name, CompressionLevel.Optimal).Open())
                        entry.Write(content);
            stream.Position = 0;
            return stream;
        }

        async Task<string> Refused(MemoryStream zip) =>
            (await Assert.ThrowsAsync<ServiceException>(() => service.Plugins.ImportZipAsync(zip, default))).Message;

        Assert.Contains("not a safe relative name", await Refused(Zip(("../escape.dll", [1]))));
        Assert.Contains("not a safe relative name", await Refused(Zip(("a b.dll", [1]))));
        Assert.Contains("at most 4", await Refused(Zip(("a", [1]), ("b", [1]), ("c", [1]), ("d", [1]), ("e", [1]))));

        // A small zip that inflates past the cap: counted while inflating.
        var bomb = Zip(("big.dll", new byte[5_000_000]));
        Assert.True(bomb.Length < 50_000);
        Assert.Contains("inflates to more than", await Refused(bomb));

        Assert.Contains("not a .NET assembly", await Refused(Zip(("Fake.dll", [1, 2, 3]))));
        Assert.Contains("main assembly cannot be told", await Refused(Zip(("readme.txt", [1]))));
        Assert.DoesNotContain(service.Node.Catalog.Read().Keys, k => k.StartsWith("plugin/"));
    }

    [Fact]
    public async Task Two_versions_of_one_plugin_run_side_by_side_and_one_unloads_after_its_last_use()
    {
        await using var service = await StartAsync();
        var v1 = await service.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines"), default);
        var v2 = await service.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines.V2"), default);
        await CollectedAsync(() => service.Plugins.LiveContexts == 0, "the import's throwaway contexts are collected");

        var trades = Trades(0, 120);
        service.Memory.Log<Trade>("in").AppendRange(trades);
        await service.DeployAsync(TraderCounts("old", v1.Version, "in", "old-out"));
        await service.DeployAsync(TraderCounts("new", v2.Version, "in", "new-out"));
        var oldOut = service.Memory.UpsertSink<string, string>("old-out");
        var newOut = service.Memory.UpsertSink<string, string>("new-out");

        // Each pipeline runs its own version's code: the same class name, two behaviours.
        await Eventually(() => Same(oldOut.Rows, CountLines("v1", trades)), "the v1 pipeline's output");
        await Eventually(() => Same(newOut.Rows, CountLines("v2", trades)), "the v2 pipeline's output");
        Assert.Equal(2, service.Plugins.LiveContexts);
        Assert.All(service.Plugins.Plugins.Where(p => p.Name == Plugin), p => Assert.Equal(("loaded", 1), (p.LoadState, p.Users)));

        // Replace the v1 pipeline with v2, keeping its state. The version is a change the
        // service will not make silently.
        var refused = await Assert.ThrowsAsync<ServiceException>(() => service.DeployAsync(TraderCounts("old", v2.Version, "in", "old-out")));
        Assert.Equal("state-choice-required", refused.Code);
        Assert.Contains(refused.Details, d => d.StartsWith("version:") && d.Contains("identical"));
        await service.DeployAsync(TraderCounts("old", v2.Version, "in", "old-out"), StateChoice.Keep);
        await service.Running("old");

        // The state crossed the versions: v2's types read what v1's types wrote (the
        // distinct set of a private record), so the counts continue rather than restart,
        // and the source resumes rather than replays.
        var more = Trades(120, 30);
        service.Memory.Log<Trade>("in").AppendRange(more);
        await Eventually(() => Same(oldOut.Rows, CountLines("v2", trades.Concat(more))), "the replaced pipeline continuing on v1's state");
        Assert.Equal(30, service.Pipelines.Get("old").Sources.Single().Stats.Records);

        // Nothing uses v1 any more: its load context is unloaded and collected.
        await CollectedAsync(() => service.Plugins.LiveContexts == 1, $"v1's load context is collected (alive: {service.Plugins.LiveContexts})");
        Assert.Equal("stored", service.Plugins.Plugins.Single(p => p.Version == v1.Version).LoadState);
        Assert.Equal(("loaded", 2), service.Plugins.Plugins.Where(p => p.Version == v2.Version).Select(p => (p.LoadState, p.Users)).Single());

        // And with every pipeline of the plugin removed, v2 goes too.
        await service.Pipelines.RemoveAsync("old", new ActionOptions(), default);
        await service.Pipelines.RemoveAsync("new", new ActionOptions(), default);
        await CollectedAsync(() => service.Plugins.LiveContexts == 0, $"v2's load context is collected (alive: {service.Plugins.LiveContexts})");
    }

    [Fact]
    public async Task Two_plugins_agree_on_a_shared_row_type_and_a_contract_arrives_through_the_catalog()
    {
        var files = Directory.CreateTempSubdirectory("incremental-service-files").FullName;
        await File.WriteAllLinesAsync(Path.Combine(files, "desks.jsonl"),
            Enumerable.Range(0, 5).Select(i => $$"""{ "trader": "trader{{i}}", "name": "desk-{{i}}" }"""));
        await using var service = await StartAsync(configure: b => b.AddObjectStore("refdata", _ => new LocalObjectStore(files)));

        // The Instrument type is the host's: both plugins ship their own copy of
        // Acme.Contracts.dll and both get the host's instead.
        var instruments = Instruments(10);
        var table = await service.DeclareInstrumentsAsync(instruments);

        // Acme.RefData is a contract the host has never seen: it is stored like a plugin
        // and loaded into the process's contract context.
        var refData = await service.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.RefData"), default);
        Assert.Equal(PluginKind.Contract, refData.Kind);
        Assert.Equal(["Acme.RefData.DesksTable"], refData.TableDefinitions);
        Assert.Equal("loaded", service.Plugins.Plugins.Single(p => p.Name == "acme.refdata").LoadState);
        await service.Tables.DeclareAsync(new TableSpec
        {
            Name = "desks", Definition = "Acme.RefData.DesksTable", Contract = "Acme.RefData",
            Loader = new TableLoaderSpec("file", "refdata", Json("""{ "path": "desks.jsonl" }""")),
        }, default);
        await Eventually(() => service.Tables.Stats("desks").Rows == 5, "the desks table loading");
        Assert.Equal("Desk", service.Tables.Stats("desks").RowType);

        var pipelines = await service.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines"), default);
        var desks = await service.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Desks"), default);
        Assert.Equal(["desk-notional"], desks.Definitions.Select(d => d.Name));

        var trades = Trades(0, 100);
        service.Memory.Log<Trade>("in").AppendRange(trades);
        await service.DeployAsync(Spec("enrichment", "trade-enrichment", "in", "enriched", sinkSlot: "enriched", plugin: Plugin, version: pipelines.Version));
        await service.DeployAsync(Spec("desks", "desk-notional", "in", "lines", sinkSlot: "lines", plugin: "acme.desks"));

        // One table, two plugins, each in its own load context, one row type.
        var enriched = service.Memory.UpsertSink<long, EnrichedTrade>("enriched");
        var lines = service.Memory.UpsertSink<long, string>("lines");
        var byId = instruments.ToDictionary(i => i.Id);
        await Eventually(() => Same(enriched.Rows, Enrich.Model(trades, instruments)), "the first plugin's join");
        await Eventually(
            () => Same(lines.Rows, trades.ToDictionary(t => t.Id, t => $"{t.Id}|desk-{t.Id % 5}|{t.Quantity * byId[t.InstrumentId].Multiplier:0.##}")),
            "the second plugin's join and lookup");
        await Eventually(
            () => service.Tables.Stats("instruments").Readers.Select(r => r.Pipeline).Order().SequenceEqual(["desks", "enrichment"]),
            "both plugins' pipelines committing a version of the one table");

        // The row the first plugin delivered is the table's own object graph.
        using (var snapshot = table.Snapshot())
        {
            Assert.True(snapshot.TryGet(trades[0].InstrumentId, out var instrument));
            Assert.Same(instrument.Symbol, enriched.Rows[trades[0].Id].Symbol);
        }

        // A change to the shared table reaches both.
        instruments[2] = instruments[2] with { Multiplier = 10 };
        await table.Writer.UpsertAsync(instruments[2]);
        await table.Writer.CommitAsync();
        byId = instruments.ToDictionary(i => i.Id);
        await Eventually(() => Same(enriched.Rows, Enrich.Model(trades, instruments)), "the first plugin's join after the change");
        await Eventually(
            () => Same(lines.Rows, trades.ToDictionary(t => t.Id, t => $"{t.Id}|desk-{t.Id % 5}|{t.Quantity * byId[t.InstrumentId].Multiplier:0.##}")),
            "the second plugin's join after the change");
    }

    [Fact]
    public async Task Settings_bind_strictly_and_plugins_survive_a_service_restart()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        var instruments = Instruments(10);
        var trades = Trades(0, 80);
        memory.Log<Trade>("in").AppendRange(trades);
        string version;
        PipelineSpec Enrichment(string settings) =>
            Spec("enrichment", "trade-enrichment", "in", "enriched", sinkSlot: "enriched", settings: settings, plugin: Plugin);

        await using (var first = await StartAsync(cluster, "a", memory))
        {
            await first.DeclareInstrumentsAsync(instruments);
            version = (await first.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines"), default)).Version;

            // camelCase, unknown members refused, string enums, then DataAnnotations.
            async Task<ServiceException> Refused(string settings) =>
                await Assert.ThrowsAsync<ServiceException>(() => first.DeployAsync(Enrichment(settings)));
            Assert.Contains("minQuantitty", (await Refused("""{ "minQuantitty": 3 }""")).Message);
            Assert.Contains("mode", (await Refused("""{ "mode": "Lenient" }""")).Message);
            Assert.Contains((await Refused("""{ "minQuantity": -1 }""")).Details, d => d.Contains("minQuantity"));

            var deployed = await first.DeployAsync(Enrichment("""{ "minQuantity": 3, "mode": "Strict" }"""));
            Assert.Equal(version, deployed.Spec.Version); // resolved: the plugin has one version
            var enriched = memory.UpsertSink<long, EnrichedTrade>("enriched");
            await Eventually(() => Same(enriched.Rows, Enrich.Model(trades, instruments, minQuantity: 3)), "the plugin pipeline's output");
        }

        // A new node on the same storage: the plugin comes from the catalog and the blob
        // store into this node's own cache directory — never from the build output again.
        await using var second = await StartAsync(cluster, "a", memory);
        await second.Running("enrichment");
        Assert.Equal(("loaded", 1), second.Plugins.Plugins.Where(p => p.Name == Plugin).Select(p => (p.LoadState, p.Users)).Single());
        var later = Trades(80, 40);
        memory.Log<Trade>("in").AppendRange(later);
        var output = memory.UpsertSink<long, EnrichedTrade>("enriched");
        await Eventually(() => Same(output.Rows, Enrich.Model(trades.Concat(later), instruments, minQuantity: 3)), "the plugin pipeline after the restart");
        Assert.Equal(40, second.Pipelines.Get("enrichment").Sources.Single().Stats.Records);
    }

    [Fact]
    public async Task The_context_cap_refuses_another_version_and_fails_nothing_else()
    {
        await using var service = await StartAsync(options: o => o.MaxLoadedPluginVersions = 1);
        var v1 = await service.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines"), default);
        var v2 = await service.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines.V2"), default);
        await CollectedAsync(() => service.Plugins.LiveContexts == 0, "the import's throwaway contexts are collected");

        await service.DeployAsync(TraderCounts("one", v1.Version, "in", "one-out"));
        await service.Running("one");
        var refused = await Assert.ThrowsAsync<ServiceException>(() => service.DeployAsync(TraderCounts("two", v2.Version, "in", "two-out")));
        Assert.Equal("too-many-plugin-versions", refused.Code);
        Assert.DoesNotContain(service.Pipelines.Pipelines, p => p.Id == "two");
        Assert.Equal(PipelineState.Running, service.Pipelines.Get("one").State);

        // A second pipeline of the loaded version needs no new context.
        await service.DeployAsync(TraderCounts("three", v1.Version, "in", "three-out"));
        await service.Running("three");
        Assert.Equal(1, service.Plugins.LiveContexts);
    }
}
