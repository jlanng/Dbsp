using System.Reflection;
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
        Assert.Contains(refused.Details, d => d == $"version: {v1.Version} → {v2.Version}");

        // The evidence is about the state and nothing else: the two versions differ in a
        // constant and in a sink row's member, neither of which any store holds.
        var evidence = Assert.Single(refused.Details, d => d.StartsWith("state shapes:"));
        Assert.StartsWith("state shapes: unchanged", evidence);
        Assert.Contains("Acme.Pipelines.Seen", evidence);
        Assert.DoesNotContain(refused.Details, d => d.Contains("keep is not possible"));
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
        Assert.Contains(desks.Files, f => f.Path == "K4os.Compression.LZ4.dll");

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

    // ── packaging ──────────────────────────────────────────────────────────────────────

    /// <summary>A copy of a plugin project's output as a package directory of the test's own.</summary>
    private static string PackageCopy(string project, Action<string>? change = null)
    {
        var directory = Directory.CreateTempSubdirectory("incremental-service-package").FullName;
        foreach (var file in Directory.EnumerateFiles(PluginDirectory(project)))
            File.Copy(file, Path.Combine(directory, Path.GetFileName(file)));
        change?.Invoke(directory);
        return directory;
    }

    /// <summary>The main assembly moved into a folder under a name that is not the
    /// assembly's, with a <c>plugin.json</c> that says where it is.</summary>
    private static string Relocated(string project, string assembly, string to)
        => PackageCopy(project, directory =>
        {
            Directory.CreateDirectory(Path.GetDirectoryName(Path.Combine(directory, to))!);
            File.Move(Path.Combine(directory, assembly + ".dll"), Path.Combine(directory, to));
            File.Delete(Path.Combine(directory, assembly + ".deps.json"));
            File.WriteAllText(Path.Combine(directory, "plugin.json"), $$"""{ "assembly": "{{to}}" }""");
        });

    [Fact]
    public async Task A_main_assembly_named_and_placed_as_the_package_likes_is_loaded_from_where_it_is()
    {
        await using var service = await StartAsync();

        // A pipeline plugin: lib/main.dll holds the assembly Acme.Pipelines.
        var plugin = await service.Plugins.ImportDirectoryAsync(Relocated("Acme.Pipelines", "Acme.Pipelines", "lib/main.dll"), default);
        Assert.Equal(("acme.pipelines", "lib/main.dll", "Acme.Pipelines"), (plugin.Name, plugin.MainAssembly, plugin.AssemblyName));
        var trades = Trades(0, 40);
        service.Memory.Log<Trade>("in").AppendRange(trades);
        await service.DeployAsync(TraderCounts("p", plugin.Version, "in", "out"));
        await Eventually(() => Same(service.Memory.UpsertSink<string, string>("out").Rows, CountLines("v1", trades)), "the relocated plugin's output");

        // A contract: c/venues-main.dll holds the assembly Acme.Venues. No other test
        // imports it, so if it is loaded in this process it was loaded from that file.
        var contract = await service.Plugins.ImportDirectoryAsync(Relocated("Acme.Venues", "Acme.Venues", "c/venues-main.dll"), default);
        Assert.Equal((PluginKind.Contract, "c/venues-main.dll", "Acme.Venues"), (contract.Kind, contract.MainAssembly, contract.AssemblyName));
        Assert.Equal("loaded", service.Plugins.Plugins.Single(p => p.Name == "acme.venues").LoadState);
        await service.Tables.DeclareAsync(new TableSpec { Name = "venues", Definition = "Acme.Venues.VenuesTable", Contract = "Acme.Venues" }, default);
        Assert.Equal("Venue", service.Tables.Stats("venues").RowType);

        // A manifest stored before the assembly's name was recorded: the file's name is it.
        Assert.Equal("Acme.Old", new PluginManifest("acme.old", "v", PluginKind.Contract, "Acme.Old.dll", [], [], [], "f").MainAssemblyName);
    }

    [Fact]
    public async Task A_plugins_private_package_dependency_is_in_its_package_and_nothing_of_the_hosts_is()
    {
        await using var service = await StartAsync();
        var desks = await service.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Desks"), default);

        // The NuGet package the plugin references is copied beside it; Incremental.Service
        // and what it brings — its assemblies, the Kafka client's native libraries — are not.
        Assert.Contains(desks.Files, f => f.Path == "K4os.Compression.LZ4.dll");
        Assert.DoesNotContain(desks.Files, f =>
            f.Path.StartsWith("runtimes/", StringComparison.Ordinal) || f.Path.StartsWith("Incremental", StringComparison.Ordinal)
            || f.Path.StartsWith("Confluent", StringComparison.Ordinal) || f.Path.Contains("rdkafka", StringComparison.OrdinalIgnoreCase));
        Assert.True(desks.Bytes < 2_000_000, $"the package is {desks.Bytes} bytes");

        // And it is loaded from there: the definition calls into it while it describes
        // its circuit (Two_plugins_agree… runs the pipeline).
        await service.DeclareInstrumentsAsync();
        await service.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.RefData"), default);
        await service.Tables.DeclareAsync(new TableSpec { Name = "desks", Definition = "Acme.RefData.DesksTable", Contract = "Acme.RefData" }, default);
        var shape = await service.Pipelines.ValidateAsync(Spec("d", "desk-notional", "in", "lines", sinkSlot: "lines", plugin: "acme.desks"), default);
        Assert.Equal(["desks", "instruments"], shape.TablesRead.Order());
    }

    [Fact]
    public void A_reference_goes_to_the_host_when_its_types_cross_the_boundary_or_the_hosts_version_will_do()
    {
        static AssemblyName Named(string name, int major) => new(name) { Version = new Version(major, 0, 0, 0) };

        // Types that cross between host and plugin are the host's, whatever the plugin ships.
        Assert.True(HostAssemblies.Provides(Named("System.Text.Json", 99), pluginShipsIt: true));
        Assert.True(HostAssemblies.Provides(Named("Incremental.Service", 99), pluginShipsIt: true));
        Assert.True(HostAssemblies.Provides(Named("Microsoft.Extensions.Logging.Abstractions", 99), pluginShipsIt: true));

        // Anything else the host carries is shared while the host's version satisfies the
        // reference; a plugin built against a newer one, and shipping it, gets its own.
        var carried = typeof(Xunit.Assert).Assembly.GetName();
        Assert.False(HostAssemblies.AlwaysShared(carried.Name!));
        Assert.True(HostAssemblies.Provides(Named(carried.Name!, carried.Version!.Major), pluginShipsIt: true));
        Assert.True(HostAssemblies.Provides(Named(carried.Name!, carried.Version.Major + 1), pluginShipsIt: false));
        Assert.False(HostAssemblies.Provides(Named(carried.Name!, carried.Version.Major + 1), pluginShipsIt: true));

        // A plugin's own assembly that happens to be named Incremental.* is the plugin's.
        Assert.False(HostAssemblies.Provides(Named("Incremental.Not.The.Hosts", 1), pluginShipsIt: true));
        Assert.False(HostAssemblies.Provides(Named("K4os.Compression.LZ4", 1), pluginShipsIt: true));
    }

    private sealed class ForwardOnly(Stream inner) : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => inner.ReadAsync(buffer, cancellationToken);
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    [Fact]
    public async Task A_zip_from_a_stream_that_cannot_seek_is_spooled_to_disk_and_bounded_before_it_is_read()
    {
        var cache = Path.Combine(Path.GetTempPath(), "incremental-service-tests", Guid.NewGuid().ToString("N"));
        await using var service = await StartAsync(options: o =>
        {
            o.CacheDirectory = cache;
            o.MaxPluginBytes = 400_000;
        });
        using var zip = new MemoryStream();
        ZipFile.CreateFromDirectory(PluginDirectory("Acme.Pipelines"), zip);

        // A network stream, as a host would hand an upload on: forward only.
        zip.Position = 0;
        var imported = await service.Plugins.ImportZipAsync(new ForwardOnly(zip), default);
        Assert.Equal(Plugin, imported.Name);
        zip.Position = 0;
        Assert.Equal(imported.Version, (await service.Plugins.ImportZipAsync(zip, default)).Version);

        // One larger than a plugin may be is refused as it arrives, not after it has all
        // been taken in: the stream is read no further than the bound.
        var endless = new Endless();
        var refused = await Assert.ThrowsAsync<ServiceException>(() => service.Plugins.ImportZipAsync(endless, default));
        Assert.Contains("larger than 400000 bytes", refused.Message);
        Assert.InRange(endless.Served, 400_000, 400_000 + 200_000);

        // Nothing of either import is left in the import directory.
        var import = Path.Combine(cache, "import");
        Assert.True(!Directory.Exists(import) || !Directory.EnumerateFileSystemEntries(import).Any());
    }

    private sealed class Endless : Stream
    {
        public long Served;
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override int Read(byte[] buffer, int offset, int count)
        {
            Served += count;
            return count;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Served += buffer.Length;
            return new ValueTask<int>(buffer.Length);
        }

        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    [Fact]
    public async Task A_contract_cannot_be_deleted_from_under_the_tables_it_defines_and_a_newer_version_waits_for_a_restart()
    {
        await using var service = await StartAsync();
        var first = await service.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.RefData"), default);
        await service.Tables.DeclareAsync(new TableSpec { Name = "desks", Definition = "Acme.RefData.DesksTable", Contract = "Acme.RefData" }, default);

        // No pipeline names a contract, but a table does: a node that starts without it
        // could not create the table.
        var refused = await Assert.ThrowsAsync<ServiceException>(() => service.Plugins.DeleteAsync(first.Name, first.Version, default));
        Assert.Equal("conflict", refused.Code);
        Assert.Contains("defines table(s) desks", refused.Message);
        Assert.NotNull(service.Plugins.Manifest(first.Name, first.Version));
        Assert.True(await service.Node.Backend.Blobs.ExistsAsync(first.Files[0].Sha256, default));

        // Another version of the same contract: the same assembly and one more file.
        var second = await service.Plugins.ImportDirectoryAsync(
            PackageCopy("Acme.RefData", directory => File.WriteAllText(Path.Combine(directory, "notes.txt"), "a second version")), default);
        Assert.NotEqual(first.Version, second.Version);
        var states = service.Plugins.Plugins.Where(p => p.Name == first.Name).ToDictionary(p => p.Version, p => p.LoadState);
        Assert.Equal("loaded", states[first.Version]);
        Assert.Equal("pending", states[second.Version]);

        // What decides which of them a restarted process loads is recorded with each: the
        // order they were imported in, read back from the catalog.
        var stored = service.Node.Catalog.Read().Where(e => e.Key.StartsWith("plugin/acme.refdata/", StringComparison.Ordinal))
            .Select(e => System.Text.Json.JsonSerializer.Deserialize<PluginManifest>(e.Value, ServiceJson.Options)!).ToList();
        Assert.True(stored.Single(m => m.Version == second.Version).Sequence > stored.Single(m => m.Version == first.Version).Sequence);
        Assert.Equal(second.Version, PluginCatalog.ContractLoadOrder(stored)[0].Version);
        Assert.Equal(second.Version, PluginCatalog.ContractLoadOrder(stored.AsEnumerable().Reverse())[0].Version);

        // With a second version that defines the table too, the first can go; the last cannot.
        await service.Plugins.DeleteAsync(first.Name, first.Version, default);
        await Assert.ThrowsAsync<ServiceException>(() => service.Plugins.DeleteAsync(second.Name, second.Version, default));
        await service.Tables.RemoveAsync("desks", new ActionOptions(), default);
        await service.Plugins.DeleteAsync(second.Name, second.Version, default);
        Assert.DoesNotContain(service.Plugins.Plugins, p => p.Name == first.Name);
    }

    [Fact]
    public async Task Importing_a_version_again_puts_back_a_blob_the_store_has_lost()
    {
        await using var service = await StartAsync();
        var plugin = await service.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines"), default);
        var lost = plugin.Files.Single(f => f.Path == "Acme.Pipelines.dll");
        await service.Node.Backend.Blobs.DeleteAsync(lost.Sha256, default);
        Assert.False(await service.Node.Backend.Blobs.ExistsAsync(lost.Sha256, default));

        var version = service.Node.Catalog.Version;
        Assert.Equal(plugin.Version, (await service.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines"), default)).Version);
        Assert.True(await service.Node.Backend.Blobs.ExistsAsync(lost.Sha256, default));
        Assert.Equal(version, service.Node.Catalog.Version);
        await service.DeployAsync(TraderCounts("p", plugin.Version, "in", "out"));
        await service.Running("p");
    }

    [Fact]
    public async Task A_constructor_that_throws_is_reported_by_its_own_exception()
    {
        await using var service = await StartAsync();
        var refused = await Assert.ThrowsAsync<ServiceException>(
            () => service.Pipelines.ValidateAsync(Spec("b", "broken-constructor", "in", "out"), default));
        Assert.Contains("NotSupportedException: the constructor's own message", refused.Message);
        Assert.DoesNotContain("TargetInvocationException", refused.Message);
    }

    [Fact]
    public async Task The_cache_directory_holds_what_is_stored_and_a_default_one_goes_with_its_process()
    {
        var cache = Path.Combine(Path.GetTempPath(), "incremental-service-tests", Guid.NewGuid().ToString("N"));
        string cached;
        await using (var service = await StartAsync(options: o => o.CacheDirectory = cache))
        {
            var plugin = await service.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines"), default);
            await service.DeployAsync(TraderCounts("p", plugin.Version, "in", "out"));
            await service.Running("p");
            cached = Path.Combine(cache, "plugins", plugin.Name, plugin.Version);
            Assert.True(File.Exists(Path.Combine(cached, "Acme.Pipelines.dll")));

            // A version nothing uses any more and that is deleted leaves the disk too.
            await service.Pipelines.RemoveAsync("p", new ActionOptions(), default);
            await service.Plugins.DeleteAsync(plugin.Name, plugin.Version, default);
            Assert.False(Directory.Exists(cached));

            // And what a version left behind without being deleted here — another node
            // deleted it — is pruned against the catalog.
            Directory.CreateDirectory(Path.Combine(cache, "plugins", "gone.plugin", "0123456789abcdef"));
            File.WriteAllText(Path.Combine(cache, "plugins", "gone.plugin", "0123456789abcdef", "Gone.dll"), "x");
            await ((PluginCatalog)service.Plugins).PruneCacheAsync(null, default);
            Assert.False(Directory.Exists(Path.Combine(cache, "plugins", "gone.plugin")));
        }
        Assert.True(Directory.Exists(cache)); // a configured directory is the host's to keep

        // The default location is this process's: what a process that no longer runs
        // left there goes when a node starts, and this node's goes when it is disposed.
        var root = Path.Combine(Path.GetTempPath(), "incremental-service");
        var dead = Path.Combine(root, "2147480000", "node");
        Directory.CreateDirectory(Path.Combine(dead, "plugins", "p", "v"));
        File.WriteAllText(Path.Combine(dead, "plugins", "p", "v", "P.dll"), "x");
        var nodeId = "cache-" + Guid.NewGuid().ToString("N")[..8];
        var own = Path.Combine(root, Environment.ProcessId.ToString(), nodeId);
        await using (var service = await StartAsync(nodeId: nodeId, options: o => o.CacheDirectory = null))
        {
            Assert.False(Directory.Exists(Path.Combine(root, "2147480000")));
            var plugin = await service.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines"), default);
            await service.DeployAsync(TraderCounts("p", plugin.Version, "in", "out"));
            await service.Running("p");
            Assert.True(File.Exists(Path.Combine(own, "plugins", plugin.Name, plugin.Version, "Acme.Pipelines.dll")));
        }
        Assert.False(Directory.Exists(own));
    }

    [Fact]
    public async Task A_context_that_took_its_own_copy_of_an_assembly_is_given_to_no_new_use_once_a_contract_of_that_name_arrives()
    {
        await using var service = await StartAsync();
        var catalog = (PluginCatalog)service.Plugins;
        var desks = await service.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Desks"), default);
        await CollectedAsync(() => service.Plugins.LiveContexts == 0, "the import's throwaway context is collected");

        // The first use resolves a name to a file of the plugin's own, as a plugin does
        // with a contract it ships when the contract has not been imported yet.
        var held = await catalog.AcquireAsync("acme.desks", desks.Version, default);
        TakePrivateCopy(held);
        Assert.Equal(1, service.Plugins.LiveContexts);

        // A contract of that name is loaded: the context that holds a private copy is
        // retired, and the next use of the version gets a context of its own.
        catalog.SupersedePrivateCopies("K4os.Compression.LZ4");
        var fresh = await catalog.AcquireAsync("acme.desks", desks.Version, default);
        Assert.Equal(2, service.Plugins.LiveContexts);
        Assert.False(SameType(held, fresh));
        Assert.Contains(service.Log.Lines, l => l.Contains("holds its own copy"));

        // The retired one unloads with its last use, the fresh one with its own.
        held.Dispose();
        await CollectedAsync(() => service.Plugins.LiveContexts == 1, $"the retired context is collected (alive: {service.Plugins.LiveContexts})");
        fresh.Dispose();
        await CollectedAsync(() => service.Plugins.LiveContexts == 0, $"the fresh context is collected (alive: {service.Plugins.LiveContexts})");

        // Kept out of line so that no local of the test's frame holds the plugin's types.
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
        static void TakePrivateCopy(PluginLease lease) =>
            System.Runtime.Loader.AssemblyLoadContext.GetLoadContext(lease.Definition("desk-notional").Type.Assembly)!
                .LoadFromAssemblyName(new AssemblyName("K4os.Compression.LZ4"));

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
        static bool SameType(PluginLease one, PluginLease other) =>
            ReferenceEquals(one.Definition("desk-notional").Type, other.Definition("desk-notional").Type);
    }

    [Fact]
    public void The_fingerprint_covers_enum_members_and_anonymous_types_and_says_so_of_no_more()
    {
        static string Of(Action<System.Reflection.Emit.ModuleBuilder> define)
        {
            var assembly = System.Reflection.Emit.AssemblyBuilder.DefineDynamicAssembly(
                new System.Reflection.AssemblyName("Shapes"), System.Reflection.Emit.AssemblyBuilderAccess.RunAndCollect);
            define(assembly.DefineDynamicModule("Shapes"));
            return PluginCatalog.Fingerprint(assembly);
        }

        static void Side(System.Reflection.Emit.ModuleBuilder module, params (string Name, int Value)[] members)
        {
            var side = module.DefineEnum("Shapes.Side", System.Reflection.TypeAttributes.Public, typeof(int));
            foreach (var (name, value) in members)
                side.DefineLiteral(name, value);
            side.CreateType();
        }

        static void Key(System.Reflection.Emit.ModuleBuilder module, params string[] properties)
        {
            // What the compiler makes of `new { t.Trader, t.Id }`: a generated class, one
            // read-only property per member.
            var key = module.DefineType("<>f__AnonymousType0", System.Reflection.TypeAttributes.NotPublic | System.Reflection.TypeAttributes.Sealed);
            key.SetCustomAttribute(new System.Reflection.Emit.CustomAttributeBuilder(
                typeof(System.Runtime.CompilerServices.CompilerGeneratedAttribute).GetConstructor(Type.EmptyTypes)!, []));
            foreach (var name in properties)
            {
                var getter = key.DefineMethod("get_" + name, System.Reflection.MethodAttributes.Public | System.Reflection.MethodAttributes.SpecialName, typeof(string), Type.EmptyTypes);
                var il = getter.GetILGenerator();
                il.Emit(System.Reflection.Emit.OpCodes.Ldnull);
                il.Emit(System.Reflection.Emit.OpCodes.Ret);
                key.DefineProperty(name, System.Reflection.PropertyAttributes.None, typeof(string), null).SetGetMethod(getter);
            }
            key.CreateType();
        }

        // State stores an enum as its number: reordering the members changes what a kept store means.
        Assert.Equal(Of(m => Side(m, ("Buy", 0), ("Sell", 1))), Of(m => Side(m, ("Buy", 0), ("Sell", 1))));
        Assert.NotEqual(Of(m => Side(m, ("Buy", 0), ("Sell", 1))), Of(m => Side(m, ("Sell", 0), ("Buy", 1))));
        Assert.NotEqual(Of(m => Side(m, ("Buy", 0), ("Sell", 1))), Of(m => Side(m, ("Buy", 0), ("Sell", 1), ("Short", 2))));

        // The natural key of a Distinct or a GroupBy is an anonymous type.
        Assert.Equal(Of(m => Key(m, "Trader", "Id")), Of(m => Key(m, "Trader", "Id")));
        Assert.NotEqual(Of(m => Key(m, "Trader", "Id")), Of(m => Key(m, "Trader", "Desk")));
    }
}
