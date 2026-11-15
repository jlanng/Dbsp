using Acme.Contracts;
using static Incremental.Service.Tests.TestService;

namespace Incremental.Service.Tests;

public class SupervisorTests
{
    internal static List<Instrument> Instruments(int count) =>
        Enumerable.Range(1, count).Select(i => new Instrument(i, "SYM" + i, "ISIN" + i, 1 + i % 3)).ToList();

    internal static List<Trade> Trades(int from, int count, int instruments = 10) =>
        Enumerable.Range(from, count).Select(i => new Trade(i, i % instruments + 1, 1 + i % 7, "trader" + i % 5)).ToList();

    private static bool Same<TKey, TRow>(IReadOnlyDictionary<TKey, TRow> actual, IReadOnlyDictionary<TKey, TRow> expected) where TKey : notnull =>
        actual.Count == expected.Count && expected.All(e => actual.TryGetValue(e.Key, out var row) && Equals(row, e.Value));

    [Fact]
    public async Task A_pipeline_is_deployed_replaced_and_removed_while_another_keeps_producing()
    {
        await using var service = await StartAsync();
        var instruments = Instruments(10);
        await service.DeclareInstrumentsAsync(instruments);

        // "steady" runs throughout, fed by a producer that never pauses.
        await service.DeployAsync(Spec("steady", "counts", "steady-in", "steady-out"));
        var steadyIn = service.Memory.Log<Trade>("steady-in");
        var steadyOut = service.Memory.UpsertSink<string, long>("steady-out");
        var produced = new List<Trade>();
        using var stop = new CancellationTokenSource();
        var producer = Task.Run(async () =>
        {
            for (var id = 0; !stop.IsCancellationRequested; id++)
            {
                var trade = new Trade(id, 1, 1, "trader" + id % 5);
                lock (produced)
                    produced.Add(trade);
                steadyIn.Append(trade);
                if (id % 50 == 0)
                    await Task.Delay(1);
            }
        });

        async Task SteadyIsCorrectAndMoving()
        {
            // Correct as of some prefix of what was produced, and advancing.
            var seen = steadyOut.Batches;
            await Eventually(() => steadyOut.Batches > seen, "the steady pipeline delivering more batches");
            var status = service.Pipelines.Get("steady");
            Assert.Equal(PipelineState.Running, status.State);
            Assert.Equal(0, status.Restarts);
            Assert.Empty(status.Faults);
            var delivered = steadyOut.Rows.Values.Sum();
            long producedSoFar;
            lock (produced)
                producedSoFar = produced.Count;
            Assert.InRange(delivered, 1, producedSoFar);
        }

        // Deploy another pipeline beside it.
        var otherIn = service.Memory.Log<Trade>("other-in");
        var otherTrades = Trades(0, 300);
        otherIn.AppendRange(otherTrades);
        var deployed = await service.DeployAsync(Spec("other", "enrich", "other-in", "other-out", settings: """{ "minQuantity": 0 }"""));
        Assert.Equal(1, deployed.Spec.Generation);
        Assert.Equal(["instruments"], deployed.Spec.Reads);
        var otherOut = service.Memory.UpsertSink<long, EnrichedTrade>("other-out");
        await Eventually(() => Same(otherOut.Rows, Enrich.Model(otherTrades, instruments)), "the first generation's output");
        await SteadyIsCorrectAndMoving();

        // Replace it: a setting that feeds a filter cannot be changed without a choice.
        var tightened = Spec("other", "enrich", "other-in", "other-out", settings: """{ "minQuantity": 4 }""");
        var refused = await Assert.ThrowsAsync<ServiceException>(() => service.DeployAsync(tightened));
        Assert.Equal("state-choice-required", refused.Code);
        Assert.Contains("setting: minQuantity", refused.Details);
        Assert.Equal(1, service.Pipelines.Spec("other").Generation);
        await SteadyIsCorrectAndMoving();

        // With reset: the memory sink is cleared by its connector and refilled by the new run.
        var replaced = await service.DeployAsync(tightened, StateChoice.Reset);
        Assert.True(replaced.Spec.Generation > 1);
        await service.Running("other");
        // The timeline names each generation by what happened to it: the one that ran is
        // what stopped, though the new spec was in place by then.
        var timeline = service.Pipelines.Get("other").History!.Select(t => (t.State, t.Generation)).ToList();
        Assert.Contains((PipelineState.Stopping, 1L), timeline);
        Assert.DoesNotContain(timeline, t => t.State == PipelineState.Stopping && t.Generation == replaced.Spec.Generation);
        Assert.Equal((PipelineState.Running, replaced.Spec.Generation), timeline[^1]);
        var otherOut2 = service.Memory.UpsertSink<long, EnrichedTrade>("other-out");
        Assert.NotSame(otherOut, otherOut2);
        await Eventually(() => Same(otherOut2.Rows, Enrich.Model(otherTrades, instruments, minQuantity: 4)), "the replaced generation's output");
        await SteadyIsCorrectAndMoving();

        // A state-neutral setting changes without being asked, and keeps the state: the
        // source resumes rather than replays.
        var labelled = Spec("other", "enrich", "other-in", "other-out", settings: """{ "minQuantity": 4, "label": "x" }""");
        await service.DeployAsync(labelled);
        await service.Running("other");
        otherIn.AppendRange(Trades(300, 50));
        await Eventually(() => Same(otherOut2.Rows, Enrich.Model(Trades(0, 350), instruments, minQuantity: 4)), "output after the neutral change");
        Assert.Equal(50, service.Pipelines.Get("other").Sources.Single().Stats.Records);
        await SteadyIsCorrectAndMoving();

        // Remove it.
        var plan = await service.Pipelines.RemoveAsync("other", new ActionOptions(), default);
        Assert.Contains(plan.Steps, step => step.Contains("instruments"));
        Assert.DoesNotContain(service.Pipelines.Pipelines, p => p.Id == "other");
        Assert.Null(service.Node.Catalog.Get("pipeline/other"));
        Assert.Empty(service.Tables.Stats("instruments").Readers);
        await SteadyIsCorrectAndMoving();

        // And the steady pipeline's final output is exactly its model: nothing was lost or
        // repeated while its neighbour came and went.
        stop.Cancel();
        await producer;
        await Eventually(() => Same(steadyOut.Rows, Counts.Model(produced)), "the steady pipeline's final output");
        Assert.Equal(0, service.Pipelines.Get("steady").Restarts);
        Assert.Equal(produced.Count, service.Pipelines.Get("steady").Sources.Single().Stats.Records);
    }

    [Fact]
    public async Task A_faulting_pipeline_backs_off_then_fails_without_disturbing_others()
    {
        await using var service = await StartAsync();
        var states = new List<string>();
        service.Node.Events += e =>
        {
            if (e is { Kind: "pipeline", Subject: "fragile" })
                lock (states)
                    states.Add(e.Message.Split(':')[0]);
        };

        await service.DeployAsync(Spec("steady", "counts", "steady-in", "steady-out"));
        await service.DeployAsync(Spec("fragile", "fragile", "fragile-in", "fragile-out") with
        {
            Restart = FastRestart with { MaxRestarts = 2 },
        });
        var steadyIn = service.Memory.Log<Trade>("steady-in");
        var steadyOut = service.Memory.UpsertSink<string, long>("steady-out");
        var fragileIn = service.Memory.Log<Trade>("fragile-in");
        var fragileOut = service.Memory.UpsertSink<string, long>("fragile-out");

        fragileIn.AppendRange(Trades(0, 20));
        steadyIn.AppendRange(Trades(0, 20));
        await Eventually(() => Same(fragileOut.Rows, Counts.Model(Trades(0, 20))), "the fragile pipeline's healthy output");

        // The poison record is never committed, so every restart reads it again.
        fragileIn.Append(new Trade(999, 1, -1, "trader0"));
        await Eventually(() => service.Pipelines.Get("fragile").State == PipelineState.Failed, "the fragile pipeline failing");

        var failed = service.Pipelines.Get("fragile");
        Assert.Equal(3, failed.Faults.Count);
        Assert.Equal(2, failed.Restarts);
        Assert.All(failed.Faults, f => Assert.Contains("negative quantity on trade 999", f.Message));
        lock (states)
        {
            Assert.Equal(2, states.Count(s => s == "Backoff"));
            Assert.Equal("Failed", states[^1]);
        }
        Assert.Contains("pipeline-failed", service.Node.Health.Reasons);
        Assert.Equal(HealthLevel.Degraded, service.Node.Health.Level);

        // Failed is terminal until an operator acts: nothing restarts it by itself.
        await Task.Delay(400);
        Assert.Equal(PipelineState.Failed, service.Pipelines.Get("fragile").State);
        Assert.Equal(3, service.Pipelines.Get("fragile").Faults.Count);

        // The other pipeline never noticed.
        steadyIn.AppendRange(Trades(20, 20));
        await Eventually(() => Same(steadyOut.Rows, Counts.Model(Trades(0, 40))), "the steady pipeline's output");
        var steady = service.Pipelines.Get("steady");
        Assert.Equal(PipelineState.Running, steady.State);
        Assert.Equal(0, steady.Restarts);
        Assert.Equal(40, steady.Sources.Single().Stats.Records);

        // What it had committed before the poison is still what its sink holds.
        Assert.True(Same(fragileOut.Rows, Counts.Model(Trades(0, 20))));
    }

    [Fact]
    public async Task Dead_letters_are_drained_so_a_dead_letter_circuit_keeps_moving()
    {
        await using var service = await StartAsync(options: o => o.DeadLetterBuffer = 32);
        await service.DeployAsync(Spec("fragile", "fragile", "in", "out") with
        {
            Circuit = new PipelineCircuitOptions { ErrorPolicy = RecordErrorPolicy.DeadLetter },
        });
        var input = service.Memory.Log<Trade>("in");
        var output = service.Memory.UpsertSink<string, long>("out");

        // Far more than the circuit's dead-letter capacity (1024): unread, it would stall.
        const int poison = 3000;
        input.AppendRange(Enumerable.Range(0, poison).Select(i => new Trade(i, 1, -1, "bad")));
        input.AppendRange(Trades(poison, 50));
        await Eventually(() => Same(output.Rows, Counts.Model(Trades(poison, 50))), "good records flowing past the dropped ones");

        // The reader runs beside the delivery, so its count trails the sink by a moment.
        await Eventually(() => service.Pipelines.Get("fragile").DeadLetters == poison, "every dropped record counted");
        var status = service.Pipelines.Get("fragile");
        Assert.Equal(PipelineState.Running, status.State);
        var ring = service.Pipelines.DeadLetters("fragile");
        Assert.Equal(32, ring.Count);
        Assert.Equal(poison, ring[^1].Sequence);
        Assert.Equal("InvalidOperationException", ring[^1].ExceptionType);
        Assert.Contains("\"id\":2999", ring[^1].Record);
        Assert.Single(service.Pipelines.DeadLetters("fragile", afterSequence: poison - 1));
    }

    [Fact]
    public async Task A_faulted_source_faults_the_pipeline_and_the_restart_resumes_without_loss()
    {
        await using var service = await StartAsync();
        await service.DeployAsync(Spec("counts", "counts", "in", "out"));
        var input = service.Memory.Log<Trade>("in");
        var output = service.Memory.UpsertSink<string, long>("out");
        input.AppendRange(Trades(0, 100));
        await Eventually(() => Same(output.Rows, Counts.Model(Trades(0, 100))), "the first hundred");

        input.FailReaders(new IOException("connection lost"));
        await Eventually(() => service.Pipelines.Get("counts").Faults.Count > 0, "the source fault reaching the pipeline");
        Assert.Contains("connection lost", service.Pipelines.Get("counts").Faults[0].Message);
        input.Mend();
        input.AppendRange(Trades(100, 100));
        await service.Running("counts");
        await Eventually(() => Same(output.Rows, Counts.Model(Trades(0, 200))), "all two hundred after the restart");

        // Resumed from the committed offset: the restarted source read only the second hundred.
        Assert.Equal(100, service.Pipelines.Get("counts").Sources.Single().Stats.Records);
    }

    [Fact]
    public async Task Stop_drains_and_commits_and_start_resumes()
    {
        await using var service = await StartAsync();
        await service.DeployAsync(Spec("counts", "counts", "in", "out"));
        var input = service.Memory.Log<Trade>("in");
        var output = service.Memory.UpsertSink<string, long>("out");
        input.AppendRange(Trades(0, 500));
        await Eventually(() => output.Rows.Values.Sum() == 500, "five hundred counted");

        var generation = service.Pipelines.Spec("counts").Generation;
        await service.Pipelines.StopAsync("counts", new ActionOptions(ExpectedGeneration: generation), default);
        var stopped = service.Pipelines.Get("counts");
        Assert.Equal(PipelineState.Stopped, stopped.State);
        Assert.Equal(DesiredState.Stopped, stopped.Desired);
        Assert.Equal(499, stopped.CommittedOffsets["trades#p0"]);

        // The generation it was based on has moved: a stale mutation is refused.
        var stale = await Assert.ThrowsAsync<ServiceException>(
            () => service.Pipelines.StartAsync("counts", new ActionOptions(ExpectedGeneration: generation), default));
        Assert.Equal(412, stale.Status);

        input.AppendRange(Trades(500, 100));
        await service.Pipelines.StartAsync("counts", new ActionOptions(), default);
        await Eventually(() => Same(output.Rows, Counts.Model(Trades(0, 600))), "six hundred after the restart");
        Assert.Equal(100, service.Pipelines.Get("counts").Sources.Single().Stats.Records);
    }

    /// <summary>An upsert sink that takes its time, so a backlog stands between the
    /// source and the sink when the pipeline is stopped.</summary>
    private sealed class SlowSinkConnector(MemoryConnector memory, TimeSpan delay) : IUpsertSinkConnector
    {
        public ConnectorDescription Describe(System.Text.Json.JsonElement config) =>
            config.TryGetProperty("name", out _) ? new("slow memory") : throw new ArgumentException("the slow connector needs a name");

        public IUpsertSink<TKey, TRow> Create<TKey, TRow>(SinkContext context) where TKey : notnull =>
            new Slow<TKey, TRow>(memory.UpsertSink<TKey, TRow>(context.Config.GetProperty("name").GetString()!), delay);

        private sealed class Slow<TKey, TRow>(IUpsertSink<TKey, TRow> inner, TimeSpan delay) : IUpsertSink<TKey, TRow> where TKey : notnull
        {
            public async ValueTask WriteAsync(UpsertBatch<TKey, TRow> batch, CancellationToken cancellationToken)
            {
                await Task.Delay(delay, cancellationToken);
                await inner.WriteAsync(batch, cancellationToken);
            }
        }
    }

    [Fact]
    public async Task Stop_drains_what_the_circuit_has_taken_in_while_the_sink_still_reads()
    {
        var memory = new MemoryConnector();
        await using var service = await StartAsync(memory: memory,
            configure: b => b.AddSinkConnector("slow", _ => new SlowSinkConnector(memory, TimeSpan.FromMilliseconds(15))));
        await service.DeployAsync(Spec("counts", "counts", "in", "out") with
        {
            Sinks = new Dictionary<string, System.Text.Json.JsonElement> { ["out"] = Json("""{ "connector": "slow", "name": "out" }""") },
            Circuit = new PipelineCircuitOptions { MaxStepItemsPerInput = 10 },
        });
        var output = memory.UpsertSink<string, long>("out");
        memory.Log<Trade>("in").AppendRange(Trades(0, 300));

        // The source has handed everything to the circuit; the slow sink has seen little of it.
        await Eventually(() => service.Pipelines.Get("counts").Sources.Single().Stats.Records == 300, "the source reading all three hundred");
        Assert.True(output.Rows.Values.Sum() < 300, "the sink kept up; the test needs a backlog");

        // Stop order: sources, then the circuit — which drains while the delivery still
        // reads, and commits only what the sink acknowledged — then the delivery.
        await service.Pipelines.StopAsync("counts", new ActionOptions(), default);
        Assert.Equal(PipelineState.Stopped, service.Pipelines.Get("counts").State);
        Assert.True(Same(output.Rows, Counts.Model(Trades(0, 300))));
        Assert.Equal(299, service.Pipelines.Get("counts").CommittedOffsets["trades#p0"]);

        // A kind the connector does not implement is refused at the dry run, both named.
        var mismatch = await Assert.ThrowsAsync<ServiceException>(() => service.DeployAsync(Spec("p", "wedge", "in2", "o") with
        {
            Sinks = new Dictionary<string, System.Text.Json.JsonElement> { ["out"] = Json("""{ "connector": "slow", "name": "o" }""") },
        }));
        Assert.Contains(mismatch.Details, d => d.Contains("AppendSink") && d.Contains("'slow'"));
    }

    [Fact]
    public async Task Reset_starts_a_new_incarnation_and_replays_the_source()
    {
        await using var service = await StartAsync();
        await service.DeployAsync(Spec("counts", "counts", "in", "out"));
        var input = service.Memory.Log<Trade>("in");
        input.AppendRange(Trades(0, 200));
        var before = service.Memory.UpsertSink<string, long>("out");
        await Eventually(() => before.Rows.Values.Sum() == 200, "two hundred counted");

        var dry = await service.Pipelines.ResetAsync("counts", new ActionOptions(DryRun: true), default);
        Assert.True(dry.DryRun);
        Assert.Contains(dry.Steps, step => step.Contains("cleared by its connector"));
        Assert.Same(before, service.Memory.UpsertSink<string, long>("out"));

        await service.Pipelines.ResetAsync("counts", new ActionOptions(Caller: "tester"), default);
        await service.Running("counts");
        var after = service.Memory.UpsertSink<string, long>("out");
        Assert.NotSame(before, after);
        await Eventually(() => Same(after.Rows, Counts.Model(Trades(0, 200))), "the replayed output");
        Assert.Equal(200, service.Pipelines.Get("counts").Sources.Single().Stats.Records);
        Assert.Null(service.Pipelines.Spec("counts").Pending);
        Assert.Contains(service.Node.Catalog.Audit(), a => a is { Action: "pipeline.reset", Caller: "tester", Target: "counts" });
    }

    [Fact]
    public async Task A_spec_that_does_not_bind_is_refused_before_anything_is_touched()
    {
        await using var service = await StartAsync();
        await service.DeclareInstrumentsAsync(Instruments(3));

        async Task<ServiceException> Refused(PipelineSpec spec) =>
            await Assert.ThrowsAsync<ServiceException>(() => service.DeployAsync(spec));

        // Settings: a misspelt member, a value out of range, a definition that takes none.
        Assert.Contains("minQuantitty", (await Refused(Spec("p", "enrich", "in", "out", settings: """{ "minQuantitty": 1 }"""))).Message);
        var range = await Refused(Spec("p", "enrich", "in", "out", settings: """{ "minQuantity": -5 }"""));
        Assert.Contains(range.Details, d => d.Contains("minQuantity"));
        Assert.Contains("takes no settings", (await Refused(Spec("p", "counts", "in", "out", settings: """{ "x": 1 }"""))).Message);

        // Slots: unbound, bound to nothing the definition declares, bound to an unknown
        // connector, bound to a connector of the wrong kind.
        Assert.Contains((await Refused(Spec("p", "counts", "in"))).Details, d => d.Contains("sink slot 'out' is not bound"));
        var extra = Spec("p", "counts", "in", "out") with { Sinks = new Dictionary<string, System.Text.Json.JsonElement> { ["out"] = MemoryBinding("o"), ["more"] = MemoryBinding("m") } };
        Assert.Contains((await Refused(extra)).Details, d => d.Contains("'more'"));
        var unknown = Spec("p", "counts", "in", "out") with { Inputs = new Dictionary<string, System.Text.Json.JsonElement> { ["trades"] = Json("""{ "connector": "nope" }""") } };
        Assert.Contains((await Refused(unknown)).Details, d => d.Contains("no connector named 'nope'"));

        // Definitions, plugins, tables, ids.
        Assert.Contains("no pipeline definition 'missing'", (await Refused(Spec("p", "missing", "in", "out"))).Message);
        Assert.Contains("No plugin named", (await Refused(Spec("p", "counts", "in", "out", plugin: "nope"))).Message);
        Assert.Contains("positions", (await Refused(Spec("p", "position-report", null, "out"))).Message);
        Assert.Equal(400, (await Refused(Spec("Not_Valid", "counts", "in", "out"))).Status);

        Assert.Empty(service.Pipelines.Pipelines);
        Assert.DoesNotContain(service.Node.Catalog.Read().Keys, k => k.StartsWith("pipeline/"));

        // A dry run of a good spec touches nothing either.
        var dry = await service.Pipelines.DeployAsync(Spec("p", "enrich", "in", "out"), new DeployOptions(DryRun: true), default);
        Assert.True(dry.Plan.DryRun);
        Assert.Equal(["trades"], dry.Shape.Inputs.Select(i => i.Name));
        Assert.Equal(SlotKind.UpsertSink, dry.Shape.Sinks.Single().Kind);
        Assert.Equal(["instruments"], dry.Shape.TablesRead);
        Assert.Empty(service.Pipelines.Pipelines);
        var validated = await service.Pipelines.ValidateAsync(Spec("q", "enrich", "in", "out"), default);
        Assert.Equal(System.Text.Json.JsonSerializer.Serialize(dry.Shape), System.Text.Json.JsonSerializer.Serialize(validated));
    }

    [Fact]
    public async Task A_wedged_circuit_is_abandoned_within_the_bound_and_blocks_nothing_else()
    {
        Wedge.Gate.Reset();
        try
        {
            await using var service = await StartAsync(options: o =>
            {
                o.StopTimeout = TimeSpan.FromMilliseconds(300);
                o.AbandonTimeout = TimeSpan.FromMilliseconds(300);
            });
            await service.DeployAsync(Spec("wedge", "wedge", "wedge-in", "wedge-out"));
            await service.DeployAsync(Spec("counts", "counts", "in", "out"));
            service.Memory.Log<Trade>("wedge-in").AppendRange(Trades(0, 5));
            await Task.Delay(100); // the delegate is now inside a step

            var started = DateTime.UtcNow;
            await service.Pipelines.StopAsync("wedge", new ActionOptions(), default);
            Assert.InRange(DateTime.UtcNow - started, TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(10));
            Assert.Equal(PipelineState.Wedged, service.Pipelines.Get("wedge").State);
            Assert.Contains("pipeline-wedged", service.Node.Health.Reasons);

            // No new generation is built for a wedged id on this node...
            await service.Pipelines.StartAsync("wedge", new ActionOptions(), default);
            Assert.Equal(PipelineState.Wedged, service.Pipelines.Get("wedge").State);

            // ...and its neighbour is untouched.
            service.Memory.Log<Trade>("in").AppendRange(Trades(0, 30));
            var output = service.Memory.UpsertSink<string, long>("out");
            await Eventually(() => Same(output.Rows, Counts.Model(Trades(0, 30))), "the neighbour's output");
            Wedge.Gate.Set();
        }
        finally
        {
            Wedge.Gate.Set();
        }
    }

    [Fact]
    public async Task Concurrent_actions_are_whole_catalog_versions_each_with_its_audit_entry()
    {
        await using var service = await StartAsync(options: o => o.AuditEntries = 6);
        var before = service.Node.Catalog.Version;
        await Task.WhenAll(Enumerable.Range(0, 10).Select(i => service.DeployAsync(Spec($"p{i}", "counts", $"in{i}", $"out{i}"))));

        // Ten deploys: ten versions, each holding one spec and the audit entry describing it.
        Assert.Equal(before + 10, service.Node.Catalog.Version);
        var audit = service.Node.Catalog.Audit();
        Assert.Equal(6, audit.Count); // the newest AuditEntries are kept
        Assert.All(audit, a => Assert.Equal("pipeline.deploy", a.Action));
        Assert.Equal(audit.Select(a => a.Sequence), audit.Select(a => a.Sequence).Order());
        Assert.Equal(10, service.Pipelines.Pipelines.Count);

        // A value over the cap is refused before it is staged, and the writer survives it.
        var huge = Spec("big", "counts", "in", "out") with
        {
            Sinks = new Dictionary<string, System.Text.Json.JsonElement>
            {
                ["out"] = Json("{ \"connector\": \"memory\", \"name\": \"big-out\", \"note\": \"" + new string('x', 300_000) + "\" }"),
            },
        };
        var refused = await Assert.ThrowsAsync<ServiceException>(() => service.DeployAsync(huge));
        Assert.Equal(("too-large", 413), (refused.Code, refused.Status));
        Assert.DoesNotContain(service.Pipelines.Pipelines, p => p.Id == "big");
        await service.DeployAsync(Spec("after", "counts", "in-after", "out-after"));
        await service.Running("after");
    }
}
