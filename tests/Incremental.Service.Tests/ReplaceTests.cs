using Acme.Contracts;
using Incremental.Kafka;
using static Incremental.Service.Tests.SupervisorTests;
using static Incremental.Service.Tests.TestService;

namespace Incremental.Service.Tests;

/// <summary>A replace that keeps state: what cannot start is refused before anything
/// stops, and what only shows when the state is read is rolled back.</summary>
public class ReplaceTests
{
    private static bool Same<TKey, TRow>(IReadOnlyDictionary<TKey, TRow> actual, IReadOnlyDictionary<TKey, TRow> expected) where TKey : notnull =>
        actual.Count == expected.Count && expected.All(e => actual.TryGetValue(e.Key, out var row) && Equals(row, e.Value));

    private static PipelineSpec Workers(PipelineSpec spec, int workers) => spec with { Circuit = new PipelineCircuitOptions { Workers = workers } };

    [Fact]
    public async Task Keep_that_the_checkpoint_cannot_follow_is_refused_at_the_dry_run_and_the_call_before_anything_stops()
    {
        await using var service = await StartAsync();
        var input = service.Memory.Log<Trade>("in");
        input.AppendRange(Trades(0, 200));
        var spec = Spec("p", "counts", "in", "out");
        await service.DeployAsync(spec);
        var sink = service.Memory.UpsertSink<string, long>("out");
        await Eventually(() => Same(sink.Rows, Counts.Model(Trades(0, 200))), "the first output");
        var history = service.Pipelines.Get("p").History!.Count;

        // Workers 1 → 2 with the state kept: the checkpoint was committed for one worker.
        var twoWorkers = Workers(spec, 2);
        foreach (var dryRun in new[] { true, false })
        {
            var refused = await Assert.ThrowsAsync<ServiceException>(() =>
                service.Pipelines.DeployAsync(twoWorkers, new DeployOptions(StateChoice.Keep, DryRun: dryRun), default));
            Assert.Equal("state-incompatible", refused.Code);
            Assert.Equal(409, refused.Status);
            Assert.Contains("Nothing was changed", refused.Message);
            Assert.Contains("state=reset", refused.Message);
            Assert.Contains(refused.Details, d => d.Contains("workers=1") && d.Contains("workers=2"));
        }

        // A stateful operator renamed: its committed store has no home.
        var renamed = await Assert.ThrowsAsync<ServiceException>(() => service.DeployAsync(Spec("p", "counts-renamed", "in", "out"), StateChoice.Keep));
        Assert.Equal("state-incompatible", renamed.Code);
        Assert.Contains(renamed.Details, d => d.Contains("no home") && d.Contains("count."));

        // The question itself already says that keep is closed.
        var asked = await Assert.ThrowsAsync<ServiceException>(() => service.DeployAsync(twoWorkers));
        Assert.Equal("state-choice-required", asked.Code);
        Assert.Contains(asked.Details, d => d.StartsWith("keep is not possible") && d.Contains("workers=2"));

        // Nothing stopped, nothing was written: same generation, same circuit, no fault.
        var status = service.Pipelines.Get("p");
        Assert.Equal(PipelineState.Running, status.State);
        Assert.Equal(1, status.Generation);
        Assert.Equal(history, status.History!.Count);
        Assert.Empty(status.Faults);
        Assert.Equal(1, service.Pipelines.Spec("p").Circuit.Workers);
        Assert.DoesNotContain(service.Node.Catalog.Audit(), a => a.Action == "pipeline.replace");
        input.AppendRange(Trades(200, 100));
        await Eventually(() => Same(sink.Rows, Counts.Model(Trades(0, 300))), "the output carrying on");
        Assert.Equal(300, service.Pipelines.Get("p").Sources.Single().Stats.Records);

        // With reset the same change is taken: two workers, from empty state.
        await service.DeployAsync(twoWorkers, StateChoice.Reset);
        await service.Running("p");
        Assert.Equal(2, service.Pipelines.Spec("p").Circuit.Workers);
        await Eventually(() => Same(service.Memory.UpsertSink<string, long>("out").Rows, Counts.Model(Trades(0, 300))), "the reset run's output");
    }

    [Fact]
    public async Task Keep_whose_state_cannot_be_read_is_rolled_back_to_the_previous_spec_and_reported()
    {
        await using var service = await StartAsync();
        var input = service.Memory.Log<Trade>("in");
        input.AppendRange(Trades(0, 200));
        var spec = Spec("p", "counts", "in", "out");
        await service.DeployAsync(spec);
        var sink = service.Memory.UpsertSink<string, long>("out");
        await Eventually(() => Same(sink.Rows, Counts.Model(Trades(0, 200))), "the first output");

        // Same store names, other key type: nothing to see beforehand, so the dry run
        // passes — and says what will happen if the state does not fit.
        var incompatible = Spec("p", "counts-by-instrument", "in", "out-2");
        var dry = await service.Pipelines.DeployAsync(incompatible, new DeployOptions(StateChoice.Keep, DryRun: true), default);
        Assert.Contains(dry.Plan.Steps, s => s.Contains("the spec of generation 1 is put back"));

        var refused = await Assert.ThrowsAsync<ServiceException>(() => service.DeployAsync(incompatible, StateChoice.Keep));
        Assert.Equal("state-incompatible", refused.Code);
        Assert.Contains("rolled back", refused.Message);
        Assert.Contains("generation 3", refused.Message);
        Assert.Contains(refused.Details, d => d.Contains("store 'named.count.") && d.Contains("cannot be read"));

        // The pipeline runs the old spec, on the old state, under a new generation.
        var status = service.Pipelines.Get("p");
        Assert.Equal(PipelineState.Running, status.State);
        Assert.Equal("counts", status.Definition);
        Assert.Equal(3, status.Generation);
        Assert.Equal(0, status.Restarts);
        Assert.False(status.ReplaceUnconfirmed);
        var rolledBack = Assert.IsType<PipelineRollback>(status.RolledBack);
        Assert.Equal((2L, 1L, 3L, "counts-by-instrument"), (rolledBack.From, rolledBack.Restored, rolledBack.Now, rolledBack.Definition));
        Assert.NotEmpty(rolledBack.Problems);
        var fault = Assert.Single(status.Faults);
        Assert.Equal(nameof(CircuitStateMismatchException), fault.ExceptionType);
        Assert.Contains(status.History!, h => h.Why is { } why && why.StartsWith("rolled back to the spec of generation 1"));

        var stored = service.Pipelines.Spec("p");
        Assert.Null(stored.Previous);
        Assert.Equal("counts", stored.Definition);
        Assert.Equal("out", stored.Sinks["out"].GetProperty("name").GetString());

        var audit = service.Node.Catalog.Audit();
        var entry = Assert.Single(audit, a => a.Action == "pipeline.replace.rolled-back");
        Assert.Equal("p", entry.Target);
        Assert.Contains(entry.Plan!, s => s.StartsWith("state: ") && s.Contains("cannot be read"));
        Assert.Contains(service.Node.RecentEvents(), e => e.Subject == "p" && e.Message.StartsWith("rolled back to the spec of generation 1:"));

        // The state was not touched: the counts carry on from where they stood, and the
        // source resumes from its committed offset rather than replaying.
        input.AppendRange(Trades(200, 100));
        await Eventually(() => Same(sink.Rows, Counts.Model(Trades(0, 300))), "the output after the roll-back");
        Assert.Equal(100, service.Pipelines.Get("p").Sources.Single().Stats.Records);
        Assert.Empty(service.Memory.UpsertSink<int, long>("out-2").Rows);

        // A compatible replace afterwards runs, is confirmed, and clears the report.
        await service.DeployAsync(Spec("p", "counts", "in", "out", settings: null) with { Circuit = new PipelineCircuitOptions { CheckpointInterval = 2 } });
        await service.Running("p");
        Assert.Null(service.Pipelines.Get("p").RolledBack);
    }

    [Fact]
    public async Task Replace_that_keeps_state_records_what_it_replaced_until_it_runs()
    {
        await using var service = await StartAsync();
        service.Memory.Log<Trade>("in").AppendRange(Trades(0, 50));
        await service.DeployAsync(Spec("p", "fragile", "in", "out"));
        var sink = service.Memory.UpsertSink<string, long>("out");
        await Eventually(() => Same(sink.Rows, Counts.Model(Trades(0, 50))), "the first output");

        // fragile → counts: same store, same types. It runs, so nothing stays recorded.
        var replaced = await service.DeployAsync(Spec("p", "counts", "in", "out"), StateChoice.Keep);
        Assert.Null(replaced.Spec.Previous);
        Assert.Equal(2, replaced.Spec.Generation);
        await service.Running("p");
        Assert.Null(service.Pipelines.Spec("p").Previous);
        Assert.False(service.Pipelines.Get("p").ReplaceUnconfirmed);
        Assert.Null(ServiceJson.Read<PipelineSpec>(service.Node.Catalog.Get("pipeline/p")!).Previous);
        var audit = service.Node.Catalog.Audit().Where(a => a.Target == "p").Select(a => a.Action).ToList();
        Assert.Equal(["pipeline.deploy", "pipeline.replace", "pipeline.replace.confirmed"], audit);
        service.Memory.Log<Trade>("in").AppendRange(Trades(50, 50));
        await Eventually(() => Same(sink.Rows, Counts.Model(Trades(0, 100))), "the output on the kept state");
    }

    [Fact]
    public async Task Second_replace_that_touches_no_state_keeps_the_spec_to_go_back_to()
    {
        await using var service = await StartAsync();
        var input = service.Memory.Log<Trade>("in");
        input.AppendRange(Trades(0, 200));
        await service.DeployAsync(Spec("p", "counts", "in", "out"));
        var sink = service.Memory.UpsertSink<string, long>("out");
        await Eventually(() => Same(sink.Rows, Counts.Model(Trades(0, 200))), "the first output");

        // A keep that will not fit, left stopped: generation 2 never runs, so the state
        // is still generation 1's and the catalog says so.
        var stopped = Spec("p", "counts-by-instrument", "in", "out-2") with { Desired = DesiredState.Stopped };
        await service.DeployAsync(stopped, StateChoice.Keep);
        Assert.Equal(1, service.Pipelines.Spec("p").Previous?.Generation);

        // The same spec with another sink: nothing that touches state differs from
        // generation 2 — but the state is not generation 2's.
        var third = await service.Pipelines.DeployAsync(
            Spec("p", "counts-by-instrument", "in", "out-3") with { Desired = DesiredState.Stopped }, new DeployOptions(), default);
        Assert.Equal(1, service.Pipelines.Spec("p").Previous?.Generation);
        Assert.Equal("counts", service.Pipelines.Spec("p").Previous?.Definition);
        Assert.Contains(third.Plan.Steps, s => s.Contains("the spec of generation 1 is put back"));
        Assert.Contains(third.Plan.Steps, s => s.Contains("the state is still generation 1's"));
        Assert.True(service.Pipelines.Get("p").ReplaceUnconfirmed);

        await service.Pipelines.StartAsync("p", new ActionOptions(), default);
        await Eventually(() => service.Pipelines.Get("p") is { State: PipelineState.Running, Definition: "counts" }, "the roll-back to generation 1's spec");
        var status = service.Pipelines.Get("p");
        Assert.Equal(1, status.RolledBack?.Restored);
        Assert.Null(service.Pipelines.Spec("p").Previous);
        input.AppendRange(Trades(200, 100));
        await Eventually(() => Same(sink.Rows, Counts.Model(Trades(0, 300))), "the output on the kept state");

        // Going back by hand to the spec that wrote the state leaves nothing to go back to.
        await service.DeployAsync(stopped, StateChoice.Keep);
        Assert.NotNull(service.Pipelines.Spec("p").Previous);
        await service.DeployAsync(Spec("p", "counts", "in", "out") with { Desired = DesiredState.Stopped }, StateChoice.Keep);
        Assert.Null(service.Pipelines.Spec("p").Previous);
    }

    [Fact]
    public async Task Takeover_in_the_middle_of_a_roll_back_ends_with_the_previous_spec_running()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        TestService? standby = null;
        var tookOver = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var a = await StartAsync(cluster, "a", memory, options: o =>
            // The leader has found that the state does not fit and is about to put the
            // old spec back — and loses the log first.
            o.BeforeRollbackCommit = async _ =>
            {
                await standby!.Node.TakeOverAsync(new NodeActionOptions(Force: true), default);
                tookOver.TrySetResult();
            });
        var input = memory.Log<Trade>("in");
        input.AppendRange(Trades(0, 200));
        await a.DeployAsync(Spec("p", "counts", "in", "out"));
        var sink = memory.UpsertSink<string, long>("out");
        await Eventually(() => Same(sink.Rows, Counts.Model(Trades(0, 200))), "the first leader's output");

        await using var b = await StartAsync(cluster, "b", memory);
        standby = b;
        await Eventually(() => b.Node.Status.Standby is { TakeoverWouldBeWarm: true } && b.Pipelines.Pipelines.Count == 1, "the standby being warm");

        // What a's caller is told does not matter here: a is fenced under it.
        try
        {
            await a.DeployAsync(Spec("p", "counts-by-instrument", "in", "out-2"), StateChoice.Keep);
        }
        catch (ServiceException)
        {
        }
        await tookOver.Task.WaitAsync(TestService.Timeout);

        // a never wrote the roll-back; b found the unconfirmed spec, tried it, and put
        // the old one back itself.
        Assert.Equal(NodeRole.Fenced, a.Node.Status.Leadership.Role);
        await b.Running("p");
        var status = b.Pipelines.Get("p");
        Assert.Equal("counts", status.Definition);
        Assert.Equal(3, status.Generation);
        Assert.NotNull(status.RolledBack);
        Assert.Null(b.Pipelines.Spec("p").Previous);
        var entry = Assert.Single(b.Node.Catalog.Audit(), e => e.Action == "pipeline.replace.rolled-back");
        Assert.Equal("b", entry.Node);

        input.AppendRange(Trades(200, 100));
        await Eventually(() => Same(sink.Rows, Counts.Model(Trades(0, 300))), "the new leader's output on the kept state");
        Assert.Empty(memory.UpsertSink<int, long>("out-2").Rows);
    }

    [Fact]
    public async Task State_mismatch_with_no_spec_to_go_back_to_needs_a_reset_and_is_not_retried()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        await using var a = await StartAsync(cluster, "a", memory);
        var input = memory.Log<Trade>("in");
        input.AppendRange(Trades(0, 100));
        var spec = Spec("p", "counts", "in", "out");
        await a.DeployAsync(spec);
        var sink = memory.UpsertSink<string, long>("out");
        await Eventually(() => Same(sink.Rows, Counts.Model(Trades(0, 100))), "the first leader's output");

        // A spec changed behind the service's back: no dry run saw it, nothing was recorded.
        using (var write = await a.Node.Catalog.BeginAsync(default))
        {
            write.Put("pipeline/p", Workers(a.Pipelines.Spec("p"), 2) with { Generation = 2 });
            await write.CommitAsync(default);
        }
        await using var b = await StartAsync(cluster, "b", memory);
        await Eventually(() => b.Pipelines.Pipelines.Count == 1 && b.Pipelines.Spec("p").Circuit.Workers == 2, "the standby seeing the catalog");
        await b.Node.TakeOverAsync(new NodeActionOptions(Force: true), default);

        await Eventually(() => b.Pipelines.Get("p").State == PipelineState.NeedsReset, "the pipeline needing a reset");
        await Task.Delay(400);        // several of the fast restart policy's backoffs
        var status = b.Pipelines.Get("p");
        Assert.Equal(PipelineState.NeedsReset, status.State);
        Assert.Equal(0, status.Restarts);
        Assert.Single(status.Faults);
        var why = status.History!.Last(h => h.State == PipelineState.NeedsReset).Why!;
        Assert.Contains("workers=1", why);
        Assert.Contains("reset the pipeline", why);
        Assert.DoesNotContain("KeyPrefix", why);
        Assert.Single(b.Log.Lines, l => l.Contains("cannot start on its committed state"));

        // The reset it asks for works.
        await b.Pipelines.ResetAsync("p", new ActionOptions(), default);
        await b.Running("p");
        await Eventually(() => Same(memory.UpsertSink<string, long>("out").Rows, Counts.Model(Trades(0, 100))), "the reset run's output");
    }
}
