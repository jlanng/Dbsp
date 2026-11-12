using System.Text.Json;
using Acme.Contracts;
using static Incremental.Service.Tests.SupervisorTests;
using static Incremental.Service.Tests.TestService;

namespace Incremental.Service.Tests;

public class TableTests
{
    private static bool Same<TKey, TRow>(IReadOnlyDictionary<TKey, TRow> actual, IReadOnlyDictionary<TKey, TRow> expected) where TKey : notnull =>
        actual.Count == expected.Count && expected.All(e => actual.TryGetValue(e.Key, out var row) && Equals(row, e.Value));

    [Fact]
    public async Task A_lagging_pipeline_holds_the_retention_floor()
    {
        await using var service = await StartAsync();
        var instruments = Instruments(10);
        var table = await service.DeclareInstrumentsAsync(instruments);

        await service.DeployAsync(Spec("reader", "enrich", "in", "out"));
        var trades = Trades(0, 50);
        service.Memory.Log<Trade>("in").AppendRange(trades);
        var output = service.Memory.UpsertSink<long, EnrichedTrade>("out");
        await Eventually(() => Same(output.Rows, Enrich.Model(trades, instruments)), "the reader's first output");

        // A pipeline that has never committed holds nothing; this one has, and stops.
        await service.Pipelines.StopAsync("reader", new ActionOptions(), default);
        var committed = service.Pipelines.Get("reader").CommittedOffsets["instruments#t"];
        Assert.Equal(table.LatestVersion, committed);

        // The table moves on by forty versions while the reader is down.
        for (var v = 1; v <= 40; v++)
        {
            instruments[0] = instruments[0] with { Symbol = "RENAMED" + v };
            await table.Writer.UpsertAsync(instruments[0]);
            await table.Writer.CommitAsync();
        }

        // With no follower margin (FollowerRetention is zero here) the stopped reader's
        // checkpoint is all that keeps this history.
        await service.Node.TrimAsync();
        var stats = service.Tables.Stats("instruments");
        Assert.Equal(committed + 1, stats.OldestRetainedVersion);
        Assert.Equal("reader", stats.FloorHeldBy);
        var reader = Assert.Single(stats.Readers);
        Assert.Equal(("reader", committed, 40L), (reader.Pipeline, reader.CommittedVersion, reader.VersionsBehind));
        Assert.Equal(service.Node.Backend.RetainedHistoryVersions - 40, reader.VersionsUntilCutLoose);

        // So it can resume: it reads the forty versions it missed and corrects its output.
        await service.Pipelines.StartAsync("reader", new ActionOptions(), default);
        await service.Running("reader");
        await Eventually(() => Same(output.Rows, Enrich.Model(trades, instruments)), "the reader's output after catching up");
        Assert.Equal(PipelineState.Running, service.Pipelines.Get("reader").State);

        // Once it has committed the latest version the floor follows it up...
        await Eventually(() => service.Pipelines.Get("reader").CommittedOffsets["instruments#t"] == table.LatestVersion, "the reader committing the latest version");
        await service.Node.TrimAsync();
        Assert.Equal(table.LatestVersion + 1, service.Tables.Stats("instruments").OldestRetainedVersion);

        // ...and a removed pipeline holds nothing at all.
        await service.Pipelines.StopAsync("reader", new ActionOptions(), default);
        await table.Writer.UpsertAsync(instruments[1] with { Symbol = "LATER" });
        await table.Writer.CommitAsync();
        await service.Node.TrimAsync();
        Assert.Equal(table.LatestVersion, service.Tables.Stats("instruments").OldestRetainedVersion);
        await service.Pipelines.RemoveAsync("reader", new ActionOptions(), default);
        await service.Node.TrimAsync();
        Assert.Equal(table.LatestVersion + 1, service.Tables.Stats("instruments").OldestRetainedVersion);
        Assert.Null(service.Tables.Stats("instruments").FloorHeldBy);
    }

    [Fact]
    public async Task The_follower_margin_keeps_history_no_reader_holds()
    {
        // Ten minutes of margin: nothing is old enough to trim on the followers' account.
        await using var service = await StartAsync(options: o => o.FollowerRetention = TimeSpan.FromMinutes(10));
        var table = await service.DeclareInstrumentsAsync(Instruments(3));
        var oldest = service.Tables.Stats("instruments").OldestRetainedVersion;
        for (var v = 0; v < 20; v++)
        {
            await table.Writer.UpsertAsync(new Instrument(1, "S" + v, "ISIN1", 1));
            await table.Writer.CommitAsync();
            await service.Node.TrimAsync();
        }
        Assert.Equal(oldest, service.Tables.Stats("instruments").OldestRetainedVersion);
    }

    [Fact]
    public async Task A_pipeline_the_history_cap_passes_is_cut_loose_and_needs_a_reset()
    {
        var cluster = new InMemoryServiceCluster(new InMemoryBackendOptions { RetainedHistoryVersions = 8 });
        var memory = new MemoryConnector();
        var instruments = Instruments(5);
        var trades = Trades(0, 20, instruments: 5);
        await using (var first = await StartAsync(cluster, "a", memory))
        {
            var table = await first.DeclareInstrumentsAsync(instruments);
            await first.DeployAsync(Spec("reader", "enrich", "in", "out"));
            memory.Log<Trade>("in").AppendRange(trades);
            var output = memory.UpsertSink<long, EnrichedTrade>("out");
            await Eventually(() => Same(output.Rows, Enrich.Model(trades, instruments)), "the reader's first output");

            await Eventually(() => first.Pipelines.Get("reader").CommittedOffsets.ContainsKey("instruments#t"), "the reader's first commit");

            // The plan for the stop says what stopping costs.
            var plan = await first.Pipelines.StopAsync("reader", new ActionOptions(), default);
            Assert.Contains(plan.Steps, step => step.Contains("'instruments'") && step.Contains("must restart within 8 versions"));

            for (var v = 1; v <= 30; v++)
            {
                instruments[0] = instruments[0] with { Symbol = "R" + v };
                await table.Writer.UpsertAsync(instruments[0]);
                await table.Writer.CommitAsync();
            }
            Assert.Equal(0, first.Tables.Stats("instruments").Readers.Single().VersionsUntilCutLoose);
        }

        // A table object keeps a tail of recent versions of its own, so the cap bites when
        // the table is opened from its log again: a restart, or a takeover.
        await using var service = await StartAsync(cluster, "a", memory);
        Assert.Equal(PipelineState.Stopped, service.Pipelines.Get("reader").State);
        await service.Pipelines.StartAsync("reader", new ActionOptions(), default);
        await Eventually(() => service.Pipelines.Get("reader").State == PipelineState.NeedsReset, $"the reader needing a reset (it is {service.Pipelines.Get("reader").State})");
        Assert.Contains("pipeline-needsreset", service.Node.Health.Reasons);
        await Task.Delay(300); // it is not retried: only a reset mends it
        Assert.Equal(PipelineState.NeedsReset, service.Pipelines.Get("reader").State);
        Assert.Single(service.Pipelines.Get("reader").Faults);

        await service.Pipelines.ResetAsync("reader", new ActionOptions(), default);
        await service.Running("reader");
        var fresh = memory.UpsertSink<long, EnrichedTrade>("out");
        await Eventually(() => Same(fresh.Rows, Enrich.Model(trades, instruments)), "the reset reader's output");
    }

    [Fact]
    public async Task The_file_loader_snapshots_resyncs_and_handles_unreadable_records()
    {
        var directory = Directory.CreateTempSubdirectory("incremental-service-files").FullName;
        await using var service = await StartAsync(configure: b => b.AddObjectStore("refdata", _ => new LocalObjectStore(directory)));
        static string Line(Instrument i) => JsonSerializer.Serialize(i, JsonSerializerOptions.Web);
        var path = Path.Combine(directory, "instruments.jsonl");
        var loader = new TableLoaderSpec("file", "refdata", Json("""{ "path": "instruments.jsonl", "pollInterval": "00:00:00.03" }"""));

        var rows = Instruments(4);
        await File.WriteAllLinesAsync(path, rows.Select(Line));
        await service.Tables.DeclareAsync(new TableSpec { Name = "instruments", Definition = typeof(InstrumentsTable).FullName!, Loader = loader }, default);
        var table = service.Tables.Get<int, Instrument>("instruments");
        await Eventually(() => table.Count == 4, "the first snapshot");
        Assert.Equal("loader:file", service.Tables.Stats("instruments").Writer);
        Assert.Equal(["isin"], service.Tables.Stats("instruments").Indexes);

        // The object changes: a row altered, a row gone, a row added — and the table equals it.
        rows = [rows[0] with { Symbol = "CHANGED" }, rows[2], new Instrument(9, "NEW", "ISIN9", 2), rows[3]];
        await File.WriteAllTextAsync(path, "[" + string.Join(",", rows.Select(Line)) + "]"); // one JSON array this time
        await Eventually(() =>
        {
            using var snapshot = table.Snapshot();
            return snapshot.Rows().Select(r => r.Value).OrderBy(r => r.Id).SequenceEqual(rows.OrderBy(r => r.Id));
        }, "the table following the changed file");

        // An unreadable line under the default policy faults the loader at that line,
        // restarts it under the restart policy and ends Failed naming the position.
        await File.WriteAllLinesAsync(path, [Line(rows[0]), "{ not json", Line(rows[1])]);
        await Eventually(() => service.Tables.Stats("instruments").State == TableState.Failed, "the table failing on the unreadable line");
        var failed = service.Tables.Stats("instruments");
        Assert.Contains("instruments.jsonl:2", failed.Fault);
        Assert.Equal(4, failed.Loader!.Restarts);
        Assert.Contains(service.Tables.Unreadable("instruments"), u => u.Position == "instruments.jsonl:2" && u.Record == "{ not json");
        Assert.Contains("table-faulted", service.Node.Health.Reasons);
        Assert.Equal(4, table.Count); // readers keep the last good version

        // The same file under skip: the line is counted and passed over.
        await service.Tables.DeclareAsync(new TableSpec
        {
            Name = "instruments-lenient", Definition = typeof(InstrumentsTable).FullName!, Loader = loader, OnUnreadable = UnreadablePolicy.Skip,
        }, default);
        var lenient = service.Tables.Get<int, Instrument>("instruments-lenient");
        await Eventually(() => lenient.Count == 2, "the lenient table loading around the bad line");
        Assert.Equal(1, service.Tables.Stats("instruments-lenient").Loader!.Unreadable);
        Assert.Equal(TableState.Open, service.Tables.Stats("instruments-lenient").State);

        // The operator fixes the source and restarts the loader.
        await File.WriteAllLinesAsync(path, rows.Take(3).Select(Line));
        await service.Tables.RestartLoaderAsync("instruments", new ActionOptions(), default);
        var reopened = service.Tables.Get<int, Instrument>("instruments");
        await Eventually(() => reopened.Count == 3 && service.Tables.Stats("instruments").State == TableState.Open, "the mended table");

        // A path outside the store's root is refused by the store.
        await Assert.ThrowsAsync<ArgumentException>(async () => await new LocalObjectStore(directory).StatAsync("../outside", default));
        await Assert.ThrowsAsync<ArgumentException>(async () => await new LocalObjectStore(directory).StatAsync("/etc/passwd", default));
    }

    /// <summary>A custom table source with stable positions: it stamps each row with its
    /// offset and resumes from the table's committed offsets.</summary>
    private sealed class SequenceFeed : ITableSourceFactory
    {
        public readonly List<Instrument> Rows = [];
        public readonly List<long> ResumedFrom = [];
        public int Applied;

        public ITableSource<TKey, TRow> Create<TKey, TRow>(TableLoaderSpec loader)
            where TKey : notnull
            where TRow : class =>
            new Source(this) as ITableSource<TKey, TRow>
            ?? throw new InvalidOperationException($"the sequence feed carries instruments, not {typeof(TRow).Name}");

        private sealed class Source(SequenceFeed feed) : ITableSource<int, Instrument>
        {
            public async Task RunAsync(TableSourceContext<int, Instrument> context, CancellationToken cancellationToken)
            {
                var position = context.Table.CommittedOffsets.GetValueOrDefault(0, -1);
                lock (feed.ResumedFrom)
                    feed.ResumedFrom.Add(position);
                while (true)
                {
                    List<(long Offset, Instrument Row)> next;
                    lock (feed.Rows)
                        next = feed.Rows.Skip((int)position + 1).Select((row, i) => (position + 1 + i, row)).ToList();
                    foreach (var (offset, row) in next)
                    {
                        await context.Writer.UpsertAsync(row, 0, offset);
                        Interlocked.Increment(ref feed.Applied);
                        position = offset;
                    }
                    if (next.Count > 0)
                        await context.Writer.CommitAsync(cancellationToken);
                    context.Report(TableSourceState.Streaming, $"0:{position}");
                    await Task.Delay(10, cancellationToken);
                }
            }
        }
    }

    [Fact]
    public async Task A_custom_table_source_resumes_from_the_tables_committed_offsets()
    {
        var feed = new SequenceFeed();
        await using var service = await StartAsync(configure: b => b.AddTableSource("sequence", _ => feed));
        lock (feed.Rows)
            feed.Rows.AddRange(Instruments(5));
        await service.Tables.DeclareAsync(new TableSpec
        {
            Name = "instruments", Definition = typeof(InstrumentsTable).FullName!, Loader = new TableLoaderSpec("sequence"),
        }, default);
        var table = service.Tables.Get<int, Instrument>("instruments");
        await Eventually(() => table.Count == 5, "the first five rows");
        await Eventually(() => service.Tables.Stats("instruments").Loader is { State: TableSourceState.Streaming, Position: "0:4" }, "the loader reporting its position");

        await service.Tables.RestartLoaderAsync("instruments", new ActionOptions(), default);
        lock (feed.Rows)
            feed.Rows.AddRange(Instruments(7).Skip(5));
        await Eventually(() => table.Count == 7, "the rows after the restart");
        lock (feed.ResumedFrom)
            Assert.Equal([-1L, 4L], feed.ResumedFrom);
        Assert.Equal(7, feed.Applied); // nothing was applied twice

        // A factory asked for a table it does not serve refuses the declare — the dry run
        // too — before a log is created or the catalog names the table.
        var counterparties = new TableSpec { Name = "parties", Definition = typeof(CounterpartiesTable).FullName!, Loader = new TableLoaderSpec("sequence") };
        var planned = Assert.Throws<ServiceException>(() => service.Tables.PlanDeclare(counterparties));
        Assert.Equal("invalid", planned.Code);
        Assert.Contains("The sequence loader cannot load table 'parties' (String → Counterparty)", planned.Message);
        Assert.Contains("the sequence feed carries instruments", planned.Message);
        var declared = await Assert.ThrowsAsync<ServiceException>(() => service.Tables.DeclareAsync(counterparties, default));
        Assert.Equal(planned.Message, declared.Message);
        Assert.DoesNotContain(service.Tables.Tables, t => t.Name == "parties");
        Assert.Null(service.Node.Catalog.Get("table/parties"));
        Assert.Contains("start its sequence loader", string.Join(" ", service.Tables.PlanDeclare(counterparties with
        {
            Name = "more-instruments", Definition = typeof(InstrumentsTable).FullName!,
        }).Steps));

        // An unknown loader kind is refused at the declaration.
        var refused = await Assert.ThrowsAsync<ServiceException>(() => service.Tables.DeclareAsync(new TableSpec
        {
            Name = "other", Definition = typeof(InstrumentsTable).FullName!, Loader = new TableLoaderSpec("nope"),
        }, default));
        Assert.Contains("sequence", refused.Message);
    }

    [Fact]
    public async Task A_writer_fault_is_the_tables_failure_and_the_node_keeps_leading()
    {
        var feed = new SequenceFeed();
        await using var service = await StartAsync(configure: b => b.AddTableSource("sequence", _ => feed));
        var instruments = Instruments(3);
        lock (feed.Rows)
            feed.Rows.AddRange(instruments);
        await service.Tables.DeclareAsync(new TableSpec
        {
            Name = "poisonable", Definition = typeof(PoisonableInstrumentsTable).FullName!, Loader = new TableLoaderSpec("sequence"),
        }, default);
        var table = service.Tables.Get<int, Instrument>("poisonable");
        await Eventually(() => table.Count == 3 && service.Tables.Stats("poisonable").Loader!.Position == "0:2", "the good rows");

        // A pipeline joined to that table, and one that has nothing to do with it.
        await service.DeployAsync(Spec("reader", "enrich", "in", "out") with { Tables = new Dictionary<string, string> { ["instruments"] = "poisonable" } });
        await service.DeployAsync(Spec("bystander", "counts", "in", "counts"));
        var trades = Trades(0, 30, instruments: 3);
        service.Memory.Log<Trade>("in").AppendRange(trades);
        var output = service.Memory.UpsertSink<long, EnrichedTrade>("out");
        var counts = service.Memory.UpsertSink<string, long>("counts");
        await Eventually(() => Same(output.Rows, Enrich.Model(trades, instruments)), "the reader's output");

        // A row whose index selector throws faults the writer for good. The cause is
        // deterministic, so every reopen from the log meets it again at the same position.
        lock (feed.Rows)
            feed.Rows.Add(new Instrument(4, "POISON", "ISIN4", 1));
        await Eventually(() => service.Tables.Stats("poisonable").State == TableState.Failed, "the table failing");
        var failed = service.Tables.Stats("poisonable");
        Assert.Contains("0:2", failed.Fault);
        Assert.Contains(service.Node.RecentEvents(), e => e is { Kind: "table", Subject: "poisonable" } && e.Message.Contains("writer faulted"));
        Assert.Contains(service.Node.RecentEvents(), e => e is { Kind: "table", Subject: "poisonable", Message: "reopened from its log" });

        // That is the table's failure. The node leads, and says what is wrong.
        Assert.True(service.Node.IsLeader);
        Assert.Contains("table-faulted", service.Node.Health.Reasons);
        Assert.Equal(HealthLevel.Degraded, service.Node.Health.Level);

        // Readers keep the last good version, on the instance the last reopen made: the
        // pipeline that held the old one was rebuilt, and still answers correctly.
        Assert.Equal(3, service.Tables.Stats("poisonable").Rows);
        Assert.NotSame(table, service.Tables.Get<int, Instrument>("poisonable"));
        var more = Trades(30, 30, instruments: 3);
        service.Memory.Log<Trade>("in").AppendRange(more);
        await Eventually(() => service.Pipelines.Get("reader").State == PipelineState.Running, "the reader running on the reopened table");
        await Eventually(() => Same(output.Rows, Enrich.Model(trades.Concat(more), instruments)), "the reader's output after the table failed");
        await Eventually(() => Same(counts.Rows, Counts.Model(trades.Concat(more))), "the bystander's output");
        Assert.Equal(0, service.Pipelines.Get("bystander").Restarts);
        Assert.Empty(service.Pipelines.Get("bystander").Faults);
    }

    [Fact]
    public async Task Reopening_a_table_rebuilds_the_pipelines_that_held_the_old_instance()
    {
        await using var service = await StartAsync();
        var instruments = Instruments(3);
        var table = await service.DeclareInstrumentsAsync(instruments);
        await service.DeployAsync(Spec("reader", "enrich", "in", "out"));
        var trades = Trades(0, 30, instruments: 3);
        service.Memory.Log<Trade>("in").AppendRange(trades);
        var output = service.Memory.UpsertSink<long, EnrichedTrade>("out");
        await Eventually(() => Same(output.Rows, Enrich.Model(trades, instruments)), "the reader's output");

        await service.Tables.ReopenAsync("instruments", default);
        var reopened = service.Tables.Get<int, Instrument>("instruments");
        Assert.NotSame(table, reopened);
        Assert.Equal(3, reopened.Count);

        // The old instance will never see this version; a circuit still built on it would
        // never correct its output.
        instruments[0] = instruments[0] with { Symbol = "AFTER" };
        await reopened.Writer.UpsertAsync(instruments[0]);
        await reopened.Writer.CommitAsync();
        await Eventually(() => Same(output.Rows, Enrich.Model(trades, instruments)), "the reader following the reopened table");
        Assert.Equal(0, service.Pipelines.Get("reader").Restarts); // a rebuild, not a fault
    }

    [Fact]
    public async Task A_derived_table_has_one_writing_pipeline_and_any_number_of_readers()
    {
        await using var service = await StartAsync();
        await service.Tables.DeclareAsync(new TableSpec { Name = "positions", Definition = typeof(PositionsTable).FullName! }, default);
        Assert.Equal("none", service.Tables.Stats("positions").Writer);

        var writer = await service.DeployAsync(Spec("positions", "positions", "in"));
        Assert.Equal(["positions"], writer.Spec.Writes);
        await service.DeployAsync(Spec("report", "position-report", null, "out"));
        var trades = Trades(0, 200);
        service.Memory.Log<Trade>("in").AppendRange(trades);

        var expected = trades.GroupBy(t => t.InstrumentId).ToDictionary(g => g.Key, g => new Position(g.Key, g.Sum(t => t.Quantity)));
        var output = service.Memory.UpsertSink<int, Position>("out");
        await Eventually(() => Same(output.Rows, expected), "the report reading the derived table");
        Assert.Equal("pipeline:positions", service.Tables.Stats("positions").Writer);
        await Eventually(
            () => service.Tables.Stats("positions").Readers.Select(r => r.Pipeline).Order().SequenceEqual(["positions", "report"]),
            "the writer and the reader committing a version of the derived table");

        // One writer: a second pipeline writing it is refused, and so is removing the table.
        var second = await Assert.ThrowsAsync<ServiceException>(() => service.DeployAsync(Spec("positions-2", "positions", "in2")));
        Assert.Contains("already written by pipeline 'positions'", second.Message);
        var named = await Assert.ThrowsAsync<ServiceException>(() => service.Tables.RemoveAsync("positions", new ActionOptions(), default));
        Assert.Contains("positions, report", named.Message);

        // The writer restarts on its state and the table still equals it.
        await service.Pipelines.RestartAsync("positions", new ActionOptions(), default);
        var more = Trades(200, 50);
        service.Memory.Log<Trade>("in").AppendRange(more);
        expected = trades.Concat(more).GroupBy(t => t.InstrumentId).ToDictionary(g => g.Key, g => new Position(g.Key, g.Sum(t => t.Quantity)));
        await Eventually(() => Same(output.Rows, expected), "the report after the writer restarted");

        // With both pipelines gone the table can be removed, and its rows are still in its log.
        await service.Pipelines.RemoveAsync("report", new ActionOptions(), default);
        var plan = await service.Pipelines.RemoveAsync("positions", new ActionOptions(), default);
        Assert.Contains(plan.Steps, s => s.Contains("derived table 'positions' keeps its rows"));
        await service.Tables.RemoveAsync("positions", new ActionOptions(), default);
        Assert.DoesNotContain(service.Tables.Tables, t => t.Name == "positions");
        await service.Tables.DeclareAsync(new TableSpec { Name = "positions", Definition = typeof(PositionsTable).FullName! }, default);
        Assert.Equal(expected.Count, service.Tables.Get<int, Position>("positions").Count);
    }

    [Fact]
    public async Task A_declared_index_is_read_by_its_definition_and_table_types_are_checked()
    {
        await using var service = await StartAsync();
        var instruments = Instruments(20);
        var table = await service.DeclareInstrumentsAsync(instruments);
        await service.DeployAsync(Spec("lookup", "by-isin", null, "out") with
        {
            Inputs = new Dictionary<string, JsonElement> { ["requests"] = MemoryBinding("requests") },
        });
        var requests = service.Memory.KeyedLog<string, IsinRequest>("requests");
        requests.Upsert(new IsinRequest("ISIN7"));
        requests.Upsert(new IsinRequest("ISIN12"));
        requests.Upsert(new IsinRequest("ISIN-UNKNOWN"));
        var output = service.Memory.UpsertSink<string, IsinAnswer>("out");
        await Eventually(() => output.Rows.Count == 2, "two lookups answered");
        Assert.Equal(new IsinAnswer("ISIN7", "SYM7"), output.Rows["ISIN7"]);
        Assert.Equal(new IsinAnswer("ISIN12", "SYM12"), output.Rows["ISIN12"]);

        // A keyed input's delete retracts its answer.
        requests.Delete("ISIN7");
        await Eventually(() => output.Rows.Count == 1, "the deleted request's answer retracted");

        // The table is the one object every caller gets; other types are refused by name.
        Assert.Same(table, service.Tables.Get<int, Instrument>("instruments"));
        var wrong = Assert.Throws<InvalidOperationException>(() => service.Tables.Get<int, Position>("instruments"));
        Assert.Contains("Acme.Contracts.Instrument", wrong.Message);
        Assert.Contains("Acme.Contracts.Position", wrong.Message);
        Assert.Throws<TableNotOpenException>(() => service.Tables.Get<int, Instrument>("undeclared"));

        // Reserved and invalid names.
        await Assert.ThrowsAsync<ServiceException>(() => service.Tables.DeclareAsync(new TableSpec { Name = "svc.mine", Definition = typeof(InstrumentsTable).FullName! }, default));
        await Assert.ThrowsAsync<ServiceException>(() => service.Tables.DeclareAsync(new TableSpec { Name = "instruments", Definition = typeof(InstrumentsTable).FullName! }, default));
    }

    // ── an input that is not there ─────────────────────────────────────────────────────

    [Fact]
    public async Task A_table_whose_file_does_not_exist_says_so_at_the_dry_run_waits_visibly_and_degrades_the_node_until_it_arrives()
    {
        var directory = Directory.CreateTempSubdirectory("incremental-service-files").FullName;
        await using var service = await StartAsync(
            options: o => o.MissingInputGrace = TimeSpan.FromMilliseconds(400),
            configure: b => b.AddObjectStore("refdata", _ => new LocalObjectStore(directory)));
        var spec = new TableSpec
        {
            Name = "instruments", Definition = typeof(InstrumentsTable).FullName!,
            Loader = new TableLoaderSpec("file", "refdata", Json("""{ "path": "nope.jsonl", "pollInterval": "00:00:00.03" }""")),
        };

        // The dry run goes ahead and says what it saw.
        var plan = await service.Tables.PlanDeclareAsync(spec, default);
        var warning = Assert.Single(plan.Warnings);
        Assert.Contains("object nope.jsonl is not in store refdata", warning);
        Assert.Contains("waits until it is there", warning);

        await service.Tables.DeclareAsync(spec, default);
        await Eventually(() => service.Tables.Stats("instruments").Loader is { State: TableSourceState.Waiting }, "the loader waiting");
        var waiting = service.Tables.Stats("instruments");
        Assert.Equal(TableState.Open, waiting.State);
        Assert.Equal(0, waiting.Rows);
        Assert.Equal("object nope.jsonl in store refdata", waiting.Loader!.WaitingFor);
        Assert.NotNull(waiting.Loader.WaitingSince);
        Assert.Null(waiting.Loader.Fault);
        Assert.Equal(0, waiting.Loader.Restarts);

        // Not at once: a file may be on its way. After the grace period it is an event,
        // once, and a reason the node is Degraded.
        await Eventually(() => service.Node.Health.Reasons.Contains("loader-waiting"), "the node saying so");
        Assert.Equal(HealthLevel.Degraded, service.Node.Health.Level);
        await Eventually(() => service.Node.RecentEvents().Any(e => e.Kind == "table" && e.Message == "loader waiting for object nope.jsonl in store refdata"), "the event");
        await Task.Delay(200);
        Assert.Single(service.Node.RecentEvents(), e => e.Kind == "table" && e.Message.StartsWith("loader waiting for"));

        // The file arrives: loaded, no longer waiting, healthy.
        await File.WriteAllLinesAsync(Path.Combine(directory, "nope.jsonl"), Instruments(3).Select(i => JsonSerializer.Serialize(i, JsonSerializerOptions.Web)));
        await Eventually(() => service.Tables.Stats("instruments") is { Rows: 3, Loader.State: TableSourceState.Idle }, "the table loaded");
        Assert.Null(service.Tables.Stats("instruments").Loader!.WaitingFor);
        await Eventually(() => service.Node.Health.Level == HealthLevel.Healthy, "the node healthy again");
        Assert.Contains(service.Node.RecentEvents(), e => e.Kind == "table" && e.Message.StartsWith("loader no longer waiting"));
        Assert.Empty((await service.Tables.PlanDeclareAsync(spec with { Name = "again" }, default)).Warnings);

        // An object that goes away after a load leaves its rows, and is not "waiting".
        File.Delete(Path.Combine(directory, "nope.jsonl"));
        await Task.Delay(150);
        Assert.True(service.Tables.Stats("instruments") is { Rows: 3, Loader.State: TableSourceState.Idle });
    }

    [Fact]
    public async Task A_loaded_table_whose_file_has_since_been_removed_is_not_waiting_after_a_takeover_or_a_restart()
    {
        // The upstream job delivers a file and takes it away again. The table has its
        // rows; that the loader of a node which took over, or was restarted, has loaded
        // nothing itself does not make the table one that is waiting for its input.
        var directory = Directory.CreateTempSubdirectory("incremental-service-files").FullName;
        var cluster = new InMemoryServiceCluster();
        Action<ServiceOptions> options = o => o.MissingInputGrace = TimeSpan.FromMilliseconds(200);
        Action<IncrementalServiceBuilder> configure = b => b.AddObjectStore("refdata", _ => new LocalObjectStore(directory));
        var spec = new TableSpec
        {
            Name = "instruments", Definition = typeof(InstrumentsTable).FullName!,
            Loader = new TableLoaderSpec("file", "refdata", Json("""{ "path": "delivered.jsonl", "pollInterval": "00:00:00.03" }""")),
        };
        await File.WriteAllLinesAsync(Path.Combine(directory, "delivered.jsonl"), Instruments(5).Select(i => JsonSerializer.Serialize(i, JsonSerializerOptions.Web)));

        await using (var a = await StartAsync(cluster, "a", options: options, configure: configure))
        await using (var b = await StartAsync(cluster, "b", options: options, configure: configure))
        {
            await a.Tables.DeclareAsync(spec, default);
            await Eventually(() => a.Tables.Stats("instruments") is { Rows: 5, Loader.State: TableSourceState.Idle }, "the table loaded on a");
            await Eventually(() => b.Tables.Tables.Any(t => t.Name == "instruments") && b.Tables.Stats("instruments") is { Rows: 5 }, "the table followed by b");
            File.Delete(Path.Combine(directory, "delivered.jsonl"));

            await b.Node.TakeOverAsync(new NodeActionOptions(), default).WaitAsync(TestService.Timeout);
            await Eventually(() => b.Tables.Stats("instruments").Loader is { State: TableSourceState.Idle }, "b's loader running, with nothing to load");
            await Task.Delay(500);      // well past the grace period
            var taken = b.Tables.Stats("instruments");
            Assert.Equal(5, taken.Rows);
            Assert.Equal(TableSourceState.Idle, taken.Loader!.State);
            Assert.Null(taken.Loader.WaitingFor);
            Assert.DoesNotContain("loader-waiting", b.Node.Health.Reasons);
            Assert.DoesNotContain(b.Node.RecentEvents(), e => e.Kind == "table" && e.Message.StartsWith("loader waiting for"));
        }

        // A node from nothing restores the table with its rows; the file is still gone.
        await using var c = await StartAsync(cluster, "c", options: options, configure: configure);
        await Eventually(() => c.Node.Status.Leadership.Role == Incremental.Kafka.NodeRole.Leader, "c leading");
        await Eventually(() => c.Tables.Tables.Any(t => t.Name == "instruments") && c.Tables.Stats("instruments") is { Rows: 5, Loader.State: TableSourceState.Idle }, "the table restored on c, its loader idle");
        await Task.Delay(500);
        Assert.Null(c.Tables.Stats("instruments").Loader!.WaitingFor);
        Assert.DoesNotContain("loader-waiting", c.Node.Health.Reasons);

        // A new file is still loaded: the table is replaced by it.
        await File.WriteAllLinesAsync(Path.Combine(directory, "delivered.jsonl"), Instruments(2).Select(i => JsonSerializer.Serialize(i, JsonSerializerOptions.Web)));
        await Eventually(() => c.Tables.Stats("instruments") is { Rows: 2 }, "the new file loaded");
    }
}
