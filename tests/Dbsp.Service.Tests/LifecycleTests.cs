using System.Text.Json;
using Acme.Contracts;
using Dbsp.Kafka;
using static Dbsp.Service.Tests.TestService;

namespace Dbsp.Service.Tests;

/// <summary>A connector whose calls a test decides. It hands the work on to the memory
/// connector, so a slot bound to it reads the same logs and writes the same sinks.</summary>
internal sealed class ProbeConnector(MemoryConnector memory) : IAppendSourceConnector, IUpsertSinkConnector, IResettableSinkConnector
{
    /// <summary>Awaited inside <c>StartAsync</c>, with the source's token.</summary>
    public Func<CancellationToken, Task>? OnStart;

    /// <summary>Awaited inside <c>ResetAsync</c>, given which call this is (from 1).</summary>
    public Func<int, Task>? OnReset;

    /// <summary>Run inside a sink's <c>Create</c>.</summary>
    public Action? OnCreate;

    public int Resets;
    public volatile string? LastPassword;

    public ConnectorDescription Describe(JsonElement config) => new("a probe", ["password"]);

    public static JsonElement Binding(string name, string? password = null) =>
        Json(password is null
            ? $$"""{ "connector": "probe", "name": "{{name}}" }"""
            : $$"""{ "connector": "probe", "name": "{{name}}", "password": "{{password}}" }""");

    public async Task<IRunningSource> StartAsync<T>(SourceContext<T> context, CancellationToken cancellationToken) where T : notnull
    {
        if (OnStart is { } hook)
            await hook(cancellationToken);
        return await ((IAppendSourceConnector)memory).StartAsync(context, cancellationToken);
    }

    public IUpsertSink<TKey, TRow> Create<TKey, TRow>(SinkContext context) where TKey : notnull
    {
        LastPassword = context.Config.TryGetProperty("password", out var password) ? password.GetString() : null;
        OnCreate?.Invoke();
        return ((IUpsertSinkConnector)memory).Create<TKey, TRow>(context);
    }

    public async Task ResetAsync(SinkContext context, CancellationToken cancellationToken)
    {
        var call = Interlocked.Increment(ref Resets);
        if (OnReset is { } hook)
            await hook(call);
        await ((IResettableSinkConnector)memory).ResetAsync(context, cancellationToken);
    }
}

/// <summary>
/// What happens to a pipeline when two things happen to it at once, or when something is
/// interrupted half way: actions racing each other and a removal, a reset or removal cut
/// short, a takeover that finds one pending, a node that stops leading while a generation
/// starts, a connector that does not answer.
/// </summary>
public class LifecycleTests
{
    private static bool Same<TKey, TRow>(IReadOnlyDictionary<TKey, TRow> actual, IReadOnlyDictionary<TKey, TRow> expected) where TKey : notnull =>
        actual.Count == expected.Count && expected.All(e => actual.TryGetValue(e.Key, out var row) && Equals(row, e.Value));

    private static async Task<(TestService Service, ProbeConnector Probe)> StartWithProbeAsync(
        InMemoryServiceCluster? cluster = null, string nodeId = "a", MemoryConnector? memory = null, Action<ServiceOptions>? options = null)
    {
        memory ??= new MemoryConnector();
        var probe = new ProbeConnector(memory);
        var service = await StartAsync(cluster, nodeId, memory, options, b => b.AddSinkConnector("probe", _ => probe));
        return (service, probe);
    }

    /// <summary>The counts pipeline with its sink — and, if asked, its input — on the probe.</summary>
    private static PipelineSpec Probed(string id, string input, string sink, bool probeInput = false, string? password = null) =>
        Spec(id, "counts", input, sink) with
        {
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = probeInput ? ProbeConnector.Binding(input) : MemoryBinding(input) },
            Sinks = new Dictionary<string, JsonElement> { ["out"] = ProbeConnector.Binding(sink, password) },
        };

    /// <summary>Fails the test's claim that a pipeline never runs with an act pending: every
    /// time the pipeline is reported Running, its spec must have none.</summary>
    private static List<string> WatchForRunningWhilePending(TestService service, string id)
    {
        var seen = new List<string>();
        service.Node.Events += e =>
        {
            if (e.Kind != "pipeline" || e.Subject != id || !e.Message.StartsWith("Running", StringComparison.Ordinal))
                return;
            try
            {
                if (service.Pipelines.Spec(id).Pending is { } pending)
                    lock (seen)
                        seen.Add(pending);
            }
            catch (ServiceException)
            {
            }
        };
        return seen;
    }

    [Fact]
    public async Task A_dry_run_racing_the_first_deploy_of_an_id_leaves_no_pipeline_outside_the_supervisor()
    {
        await using var service = await StartAsync();
        for (var round = 0; round < 200; round++)
        {
            var id = $"race-{round}";
            var spec = Spec(id, "counts", id + "-in", id + "-out") with { Desired = DesiredState.Stopped };
            var go = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var dry = Task.Run(async () =>
            {
                await go.Task;
                return await service.Pipelines.DeployAsync(spec, new DeployOptions(DryRun: true), default);
            });
            var real = Task.Run(async () =>
            {
                await go.Task;
                return await service.Pipelines.DeployAsync(spec, new DeployOptions(), default);
            });
            go.SetResult();
            await Task.WhenAll(dry, real).WaitAsync(TestService.Timeout);

            // The deployed pipeline is the one the supervisor holds: it can be read, and
            // a later deploy of the id is a replacement, not a second first deploy.
            Assert.NotNull(service.Node.Catalog.Get("pipeline/" + id));
            Assert.Equal(1, service.Pipelines.Get(id).Generation);
            var again = await service.Pipelines.DeployAsync(spec, new DeployOptions(DryRun: true), default);
            Assert.Equal("pipeline.replace", again.Plan.Action);
        }
        Assert.Equal(200, service.Pipelines.Pipelines.Count);
    }

    [Fact]
    public async Task An_action_queued_behind_a_removal_finds_the_pipeline_gone_and_writes_nothing()
    {
        await using var service = await StartAsync();
        for (var round = 0; round < 60; round++)
        {
            var id = $"rm-{round}";
            await service.DeployAsync(Spec(id, "counts", id + "-in", id + "-out") with { Desired = round % 3 == 0 ? DesiredState.Running : DesiredState.Stopped });
            var go = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var remove = Task.Run(async () =>
            {
                await go.Task;
                await service.Pipelines.RemoveAsync(id, new ActionOptions(), default);
            });
            var other = Task.Run(async () =>
            {
                await go.Task;
                try
                {
                    // No expected generation: nothing but the supervisor stands between
                    // this call and a pipeline that is being removed.
                    _ = (round % 3) switch
                    {
                        0 => await service.Pipelines.StopAsync(id, new ActionOptions(), default),
                        1 => await service.Pipelines.StartAsync(id, new ActionOptions(), default),
                        _ => await service.Pipelines.ResetAsync(id, new ActionOptions(), default),
                    };
                }
                catch (ServiceException ex) when (ex.Code is "not-found" or "conflict")
                {
                }
            });
            go.SetResult();
            await Task.WhenAll(remove, other).WaitAsync(TestService.Timeout);

            Assert.DoesNotContain(service.Pipelines.Pipelines, p => p.Id == id);
            Assert.Null(service.Node.Catalog.Get("pipeline/" + id));
        }
        Assert.DoesNotContain(service.Node.Catalog.Read().Keys, k => k.StartsWith("pipeline/", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_reset_whose_caller_goes_away_after_the_intent_is_carried_through()
    {
        await using var service = await StartAsync();
        var trades = SupervisorTests.Trades(0, 80);
        service.Memory.Log<Trade>("in").AppendRange(trades);
        await service.DeployAsync(Spec("p", "counts", "in", "out"));
        await Eventually(() => Same(service.Memory.UpsertSink<string, long>("out").Rows, Counts.Model(trades)), "the first run's output");

        // The caller's token is cancelled the moment the old generation starts to stop:
        // after the reset was recorded, before it is finished.
        using var cancel = new CancellationTokenSource();
        service.Node.Events += e =>
        {
            if (e is { Kind: "pipeline", Subject: "p" } && e.Message.StartsWith("Stopping", StringComparison.Ordinal))
                cancel.Cancel();
        };
        await service.Pipelines.ResetAsync("p", new ActionOptions(), cancel.Token).WaitAsync(TestService.Timeout);
        Assert.True(cancel.IsCancellationRequested);

        await service.Running("p");
        Assert.Null(service.Pipelines.Spec("p").Pending);
        Assert.Null(service.Node.Catalog.Get<PipelineSpec>("pipeline/p")!.Pending);
        await Eventually(() => Same(service.Memory.UpsertSink<string, long>("out").Rows, Counts.Model(trades)), "the reset run's replayed output");
        Assert.Equal(80, service.Pipelines.Get("p").Sources.Single().Stats.Records);
    }

    [Fact]
    public async Task A_reset_that_fails_part_way_is_retried_until_it_is_finished_and_the_pipeline_never_runs_around_it()
    {
        var (service, probe) = await StartWithProbeAsync(options: o => o.Restart = FastRestart with { MaxRestarts = 100 });
        await using var _ = service;
        var trades = SupervisorTests.Trades(0, 60);
        service.Memory.Log<Trade>("in").AppendRange(trades);
        await service.DeployAsync(Probed("p", "in", "out"));
        await Eventually(() => Same(service.Memory.UpsertSink<string, long>("out").Rows, Counts.Model(trades)), "the first run's output");
        var ranPending = WatchForRunningWhilePending(service, "p");

        var reachable = false;
        probe.OnReset = _ => Volatile.Read(ref reachable) ? Task.CompletedTask : throw new IOException("the sink's store is unreachable");

        // The reset is accepted: its intent is recorded and it will be finished.
        var plan = await service.Pipelines.ResetAsync("p", new ActionOptions(), default).WaitAsync(TestService.Timeout);
        Assert.Equal("pipeline.reset", plan.Action);
        await Eventually(() => probe.Resets >= 3, "the reset being tried again by itself");
        var waiting = service.Pipelines.Get("p");
        Assert.Contains(waiting.State, new[] { PipelineState.Backoff, PipelineState.Stopping });
        Assert.Equal("reset", service.Pipelines.Spec("p").Pending);
        Assert.Contains(waiting.History!, t => t.State == PipelineState.Backoff && t.Why!.StartsWith("reset not finished: IOException", StringComparison.Ordinal));

        // Neither a start nor a restart goes round the pending reset, and a deploy says why.
        await service.Pipelines.StartAsync("p", new ActionOptions(), default).WaitAsync(TestService.Timeout);
        await service.Pipelines.RestartAsync("p", new ActionOptions(), default).WaitAsync(TestService.Timeout);
        Assert.NotEqual(PipelineState.Running, service.Pipelines.Get("p").State);
        Assert.Equal("reset", service.Pipelines.Spec("p").Pending);
        var refused = await Assert.ThrowsAsync<ServiceException>(() => service.DeployAsync(Probed("p", "in", "out")));
        Assert.Contains("unfinished reset", refused.Message);

        Volatile.Write(ref reachable, true);
        await service.Running("p");
        Assert.Null(service.Pipelines.Spec("p").Pending);
        await Eventually(() => Same(service.Memory.UpsertSink<string, long>("out").Rows, Counts.Model(trades)), "the reset run's replayed output");
        Assert.Equal(60, service.Pipelines.Get("p").Sources.Single().Stats.Records);
        Assert.Empty(ranPending);
    }

    [Fact]
    public async Task A_pending_reset_that_fails_at_takeover_is_that_pipelines_failure_and_is_retried_not_skipped()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        var trades = SupervisorTests.Trades(0, 90);
        memory.Log<Trade>("in").AppendRange(trades);

        var (a, probeA) = await StartWithProbeAsync(cluster, "a", memory);
        await using var __ = a;
        await a.DeployAsync(Probed("p", "in", "out"));
        await a.DeployAsync(Spec("other", "counts", "in", "other-out"));
        await Eventually(() => Same(memory.UpsertSink<string, long>("out").Rows, Counts.Model(trades)), "the first leader's output");

        // The leader records the reset and cannot finish it.
        probeA.OnReset = _ => throw new IOException("the sink's store is unreachable");
        await a.Pipelines.ResetAsync("p", new ActionOptions(), default).WaitAsync(TestService.Timeout);
        Assert.Equal("reset", a.Pipelines.Spec("p").Pending);

        // Its successor fails the first attempt too — with a cancellation of the
        // connector's own, as an HTTP client reports a timeout — and then succeeds.
        var (b, probeB) = await StartWithProbeAsync(cluster, "b", memory);
        await using var ___ = b;
        probeB.OnReset = call => call == 1 ? throw new TaskCanceledException("the request timed out") : Task.CompletedTask;
        var ranPending = WatchForRunningWhilePending(b, "p");
        await Eventually(() => b.Node.Status.Standby is { CatalogVersionApplied: > 0 }, "the standby following the catalog");
        await b.Node.TakeOverAsync(new NodeActionOptions(Force: true), default).WaitAsync(TestService.Timeout);

        // The takeover is not the pipeline's to fail: the node leads and the other pipeline runs.
        Assert.Equal(NodeRole.Leader, b.Node.Status.Leadership.Role);
        await b.Running("other");

        // And the reset is finished before the pipeline runs: its state is new, so the
        // source replays into a sink the connector cleared.
        await b.Running("p");
        Assert.True(probeB.Resets >= 2, $"the reset was tried {probeB.Resets} time(s) on the new leader");
        Assert.Null(b.Pipelines.Spec("p").Pending);
        Assert.Contains(b.Pipelines.Get("p").History!, t => t.State == PipelineState.Backoff && t.Why!.StartsWith("reset not finished: TaskCanceledException", StringComparison.Ordinal));
        await Eventually(() => Same(memory.UpsertSink<string, long>("out").Rows, Counts.Model(trades)), "the reset run's replayed output");
        Assert.Equal(90, b.Pipelines.Get("p").Sources.Single().Stats.Records);
        Assert.Empty(ranPending);
    }

    [Fact]
    public async Task A_generation_still_starting_when_its_node_is_fenced_is_ended_not_published()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        var (a, probeA) = await StartWithProbeAsync(cluster, "a", memory);
        await using var __ = a;
        var (b, _) = await StartWithProbeAsync(cluster, "b", memory);
        await using var ___ = b;

        // The sink's connector does not come back from Create until the test lets it.
        using var release = new ManualResetEventSlim();
        using var entered = new ManualResetEventSlim();
        probeA.OnCreate = () =>
        {
            entered.Set();
            release.Wait(TestService.Timeout);
        };
        var deploy = Task.Run(() => a.Pipelines.DeployAsync(Probed("p", "in", "out"), new DeployOptions(), default));
        Assert.True(entered.Wait(TestService.Timeout), "the deploy reaching the sink's connector");
        Assert.Equal(PipelineState.Starting, a.Pipelines.Get("p").State);

        await Eventually(() => b.Node.Status.Standby is { CatalogVersionApplied: > 0 }, "the standby following the catalog");
        await b.Node.TakeOverAsync(new NodeActionOptions(Force: true), default).WaitAsync(TestService.Timeout);
        await Eventually(() => a.Node.Status.Leadership.Role == NodeRole.Fenced, "the old leader being fenced");
        await Task.Delay(300);
        probeA.OnCreate = null;
        release.Set();
        try
        {
            await deploy.WaitAsync(TestService.Timeout);
        }
        catch (ServiceException)
        {
        }

        // On the fenced node the generation that was starting is torn down, never Running.
        await Eventually(() => a.Pipelines.Get("p").State == PipelineState.Stopped, $"the fenced node's pipeline ending (it is {a.Pipelines.Get("p").State})");
        await Task.Delay(300);
        var status = a.Pipelines.Get("p");
        Assert.Equal(PipelineState.Stopped, status.State);
        Assert.DoesNotContain(status.History!, t => t.State == PipelineState.Running);
        Assert.Empty(status.Sources);
        await b.Running("p");
    }

    [Fact]
    public async Task A_source_that_never_starts_is_given_up_on_within_its_bound_and_holds_up_no_takeover()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        void Fast(ServiceOptions o) => o.ConnectorTimeout = TimeSpan.FromMilliseconds(300);
        var (a, probeA) = await StartWithProbeAsync(cluster, "a", memory, Fast);
        await using var __ = a;
        var trades = SupervisorTests.Trades(0, 40);
        memory.Log<Trade>("good-in").AppendRange(trades);
        await a.DeployAsync(Spec("good", "counts", "good-in", "good-out"));

        // A connector that connects with no deadline of its own and ignores its token.
        var never = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        probeA.OnStart = _ => never.Task;
        var clock = System.Diagnostics.Stopwatch.StartNew();
        await a.Pipelines.DeployAsync(Probed("stuck", "stuck-in", "stuck-out", probeInput: true), new DeployOptions(), default).WaitAsync(TestService.Timeout);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(10), $"the deploy took {clock.Elapsed}");
        var stuck = a.Pipelines.Get("stuck");
        Assert.NotEqual(PipelineState.Running, stuck.State);
        Assert.Contains(stuck.Faults, f => f.ExceptionType == nameof(TimeoutException) && f.Message.Contains("did not start within 300 ms"));

        // Its queue is free: the pipeline can be stopped.
        await a.Pipelines.StopAsync("stuck", new ActionOptions(), default).WaitAsync(TestService.Timeout);
        await a.Pipelines.StartAsync("stuck", new ActionOptions(), default).WaitAsync(TestService.Timeout);

        // A takeover waits for every pipeline's start, and is not held by this one.
        var (b, probeB) = await StartWithProbeAsync(cluster, "b", memory, Fast);
        await using var ___ = b;
        probeB.OnStart = _ => never.Task;
        await Eventually(() => b.Node.Status.Standby is { CatalogVersionApplied: > 0 }, "the standby following the catalog");
        clock.Restart();
        await b.Node.TakeOverAsync(new NodeActionOptions(Force: true), default).WaitAsync(TestService.Timeout);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(10), $"the takeover took {clock.Elapsed}");
        Assert.Equal(NodeRole.Leader, b.Node.Status.Leadership.Role);
        await b.Running("good");
        await Eventually(() => Same(memory.UpsertSink<string, long>("good-out").Rows, Counts.Model(trades)), "the good pipeline's output on the new leader");
        Assert.NotEqual(PipelineState.Running, b.Pipelines.Get("stuck").State);
    }

    [Fact]
    public async Task A_catalog_write_cancelled_as_it_is_made_is_recorded_and_applied_or_neither()
    {
        await using var service = await StartAsync();
        await service.DeployAsync(Spec("p", "counts", "in", "out") with { Desired = DesiredState.Stopped });
        var cancelled = 0;
        for (var round = 0; round < 60; round++)
        {
            var before = service.Pipelines.Spec("p").Desired;

            // The action queues on the catalog's write lock; the lock is released and
            // the caller's token cancelled back to back.
            var held = await service.Node.Catalog.BeginAsync(default);
            using var cancel = new CancellationTokenSource();
            var action = before == DesiredState.Stopped
                ? service.Pipelines.StartAsync("p", new ActionOptions(), cancel.Token)
                : service.Pipelines.StopAsync("p", new ActionOptions(), cancel.Token);
            await Eventually(() => service.Pipelines.Get("p").Operation is not null, "the action holding the pipeline's queue");
            await Task.Delay(round % 5);
            held.Dispose();
            cancel.Cancel();
            var threw = false;
            try
            {
                await action.WaitAsync(TestService.Timeout);
            }
            catch (OperationCanceledException)
            {
                threw = true;
                cancelled++;
            }

            // Any later commit publishes whatever a cancelled write left behind.
            using (var write = await service.Node.Catalog.BeginAsync(default))
            {
                write.Audit("a", 0, "test", "test.flush", "p", null, null);
                await write.CommitAsync(default);
            }
            var recorded = service.Node.Catalog.Get<PipelineSpec>("pipeline/p")!.Desired;
            Assert.Equal(service.Pipelines.Spec("p").Desired, recorded);
            if (threw)
                Assert.Equal(before, recorded);
        }
        Assert.True(cancelled > 0, "no round cancelled its action: the test did not reach the window");
    }

    [Fact]
    public async Task Removing_a_table_and_deploying_a_pipeline_that_reads_it_cannot_both_succeed()
    {
        await using var service = await StartAsync();
        for (var round = 0; round < 150; round++)
        {
            await service.DeclareInstrumentsAsync();
            var id = $"tr-{round}";
            var go = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var remove = Task.Run(async () =>
            {
                await go.Task;
                try
                {
                    await service.Tables.RemoveAsync("instruments", new ActionOptions(), default);
                    return true;
                }
                catch (ServiceException)
                {
                    return false;
                }
            });
            var deploy = Task.Run(async () =>
            {
                await go.Task;
                await Task.Yield();
                try
                {
                    await service.Pipelines.DeployAsync(Spec(id, "enrich", id + "-in", id + "-out") with { Desired = DesiredState.Stopped }, new DeployOptions(), default);
                    return true;
                }
                catch (ServiceException)
                {
                    return false;
                }
            });
            go.SetResult();
            var outcome = await Task.WhenAll(remove, deploy).WaitAsync(TestService.Timeout);

            Assert.False(outcome[0] && outcome[1], $"round {round}: the table was removed and a pipeline reading it deployed");
            Assert.True(outcome[0] || outcome[1], $"round {round}: neither the removal nor the deploy went through");
            var catalog = service.Node.Catalog.Read();
            Assert.Equal(outcome[1], catalog.ContainsKey("pipeline/" + id));
            Assert.Equal(!outcome[0], catalog.ContainsKey("table/instruments"));

            if (outcome[1])
            {
                await service.Pipelines.RemoveAsync(id, new ActionOptions(), default);
                await service.Tables.RemoveAsync("instruments", new ActionOptions(), default);
            }
        }
    }

    [Fact]
    public async Task A_seed_that_was_removed_stays_removed_and_a_seed_added_later_is_applied()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        void Seeds(DbspServiceBuilder b) => b
            .AddTable(Acme.Contracts.Tables.Instruments)
            .AddPipeline(Spec("seeded", "counts", "trades", "counts"))
            .AddPipeline(Spec("kept", "counts", "trades", "kept-counts"));

        await using (var first = await StartAsync(cluster, "a", memory, configure: Seeds))
        {
            await first.Running("seeded");
            Assert.Equal(new SeedRecord(["instruments"], ["kept", "seeded"]), first.Node.Catalog.Get<SeedRecord>("node/seeds"), SeedComparer.Instance);
            await first.Pipelines.RemoveAsync("seeded", new ActionOptions(), default);
            await first.Tables.RemoveAsync("instruments", new ActionOptions(), default);
        }

        // The next leader finds both keys absent, as it would for seeds never applied.
        await using (var second = await StartAsync(cluster, "a", memory, configure: Seeds))
        {
            Assert.Equal(["kept"], second.Pipelines.Pipelines.Select(p => p.Id));
            Assert.Empty(second.Tables.Tables);
            Assert.Null(second.Node.Catalog.Get("pipeline/seeded"));
        }

        // A seed the host gains later is new to the catalog and is applied, once.
        await using var third = await StartAsync(cluster, "a", memory, configure: b =>
        {
            Seeds(b);
            b.AddPipeline(Spec("late", "counts", "trades", "late-counts"));
        });
        Assert.Equal(["kept", "late"], third.Pipelines.Pipelines.Select(p => p.Id));
        Assert.Empty(third.Tables.Tables);
        Assert.Contains("late", third.Node.Catalog.Get<SeedRecord>("node/seeds")!.Pipelines);
    }

    private sealed class SeedComparer : IEqualityComparer<SeedRecord?>
    {
        public static readonly SeedComparer Instance = new();

        public bool Equals(SeedRecord? x, SeedRecord? y) =>
            x is not null && y is not null && x.Tables.SequenceEqual(y.Tables) && x.Pipelines.SequenceEqual(y.Pipelines);

        public int GetHashCode(SeedRecord? obj) => 0;
    }

    [Fact]
    public async Task A_spec_read_back_with_its_secrets_hidden_can_be_deployed_again_without_storing_the_mark()
    {
        var (service, probe) = await StartWithProbeAsync();
        await using var _ = service;
        await service.DeployAsync(Probed("p", "in", "out", password: "hunter2"));
        await service.Running("p");
        Assert.Equal("hunter2", probe.LastPassword);

        // What an operator's editor is seeded with.
        var shown = service.Pipelines.RedactedSpec("p");
        Assert.Equal("***", shown.Sinks["out"].GetProperty("password").GetString());

        // Sent back unchanged: nothing that touches the state changed, and the stored
        // secret is the one the new generation's sink is configured with.
        var replaced = await service.Pipelines.DeployAsync(shown, new DeployOptions(), default);
        Assert.Equal("pipeline.replace", replaced.Plan.Action);
        await service.Running("p");
        Assert.Equal("hunter2", probe.LastPassword);
        Assert.Equal("hunter2", service.Node.Catalog.Get<PipelineSpec>("pipeline/p")!.Sinks["out"].GetProperty("password").GetString());

        // A new value is a new value.
        await service.Pipelines.DeployAsync(Probed("p", "in", "out", password: "correct horse"), new DeployOptions(StateChoice.Keep), default);
        await service.Running("p");
        Assert.Equal("correct horse", probe.LastPassword);

        // The mark with nothing stored to stand for is refused, not stored.
        var refused = await Assert.ThrowsAsync<ServiceException>(() => service.DeployAsync(Probed("q", "in", "q-out", password: "***")));
        Assert.Equal("invalid", refused.Code);
        Assert.Contains("no value is stored", refused.Message);
        Assert.Null(service.Node.Catalog.Get("pipeline/q"));
    }

    [Theory]
    [InlineData(0.25, "250 ms")]
    [InlineData(1.5, "1.5 s")]
    [InlineData(45, "45 s")]
    [InlineData(600, "10 min")]
    [InlineData(1591.4842111, "26 min 31 s")]
    [InlineData(3900, "1 h 5 min")]
    [InlineData(90000, "1 d 1 h")]
    public void Durations_are_written_for_an_operator_and_hold_no_colon(double seconds, string text)
    {
        Assert.Equal(text, Durations.Text(TimeSpan.FromSeconds(seconds)));
        Assert.DoesNotContain(':', Durations.Text(TimeSpan.FromSeconds(seconds)));
    }

    [Fact]
    public void A_standby_is_behind_on_state_only_if_it_was_behind_at_every_recent_sample()
    {
        var window = new StateLagWindow();
        Assert.False(window.Behind);

        // A follower of a busy leader: a few records behind at one instant, caught up at the next.
        foreach (var lag in new long[] { 0, 4, 0, 7, 2, 0, 3, 3, 0, 1, 0 })
        {
            window.Add(lag);
            Assert.False(window.Behind, $"called behind after a sample of {lag}");
        }

        // A standby that is catching up: behind at every sample, and said to be after three.
        window.Add(900);
        window.Add(700);
        Assert.False(window.Behind);
        window.Add(500);
        Assert.True(window.Behind);
        window.Add(200);
        Assert.True(window.Behind);
        window.Add(0);
        Assert.False(window.Behind);
    }
}
