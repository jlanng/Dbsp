using Acme.Contracts;
using static Incremental.Service.Tests.SupervisorTests;
using static Incremental.Service.Tests.TestService;

namespace Incremental.Service.Tests;

/// <summary>
/// Round five, B3 (Open for James 70). A spec may set the most state entries its pipeline
/// holds (<see cref="PipelineCircuitOptions.MaxStateEntries"/>). Passing it fails that
/// pipeline — with a reason that names the limit, and no retry — and nothing else on the
/// node: its neighbours run on. The count is the one the sampler reads for the status.
/// </summary>
public class StateLimitTests
{
    // In the future, as StateGrowthTests' trades are, so that nothing becomes final while
    // the state fills; the bound on a time ahead is turned off for it.
    private static readonly DateTimeOffset T0 = new(2100, 1, 1, 12, 0, 0, TimeSpan.Zero);

    private static PipelineSpec Capped(string id, string input, string sink, long? limit) =>
        Spec(id, "open-trades", input, sink, settings: """{ "openSeconds": 3600 }""") is var spec
            ? spec with { Circuit = spec.Circuit with { MaxStateEntries = limit } }
            : throw new InvalidOperationException();

    [Fact]
    public async Task A_pipeline_past_its_limit_fails_alone_and_its_neighbours_run_on()
    {
        await using var service = await StartAsync(options: o => o.MaxEventTimeAhead = null);
        await service.DeclareInstrumentsAsync(Instruments(10));
        await service.DeployAsync(Capped("capped", "sl-in-1", "sl-out-1", limit: 250));
        await service.DeployAsync(Capped("neighbour", "sl-in-2", "sl-out-2", limit: null));
        await service.DeployAsync(Capped("roomy", "sl-in-3", "sl-out-3", limit: 1_000_000));
        var inputs = new[] { "sl-in-1", "sl-in-2", "sl-in-3" }.Select(service.Memory.KeyedLog<long, TimedTrade>).ToArray();
        var sinks = new[] { "sl-out-1", "sl-out-2", "sl-out-3" }.Select(service.Memory.UpsertSink<long, EnrichedTrade>).ToArray();

        // Forty trades each: every pipeline holds them, a few entries a trade, under 250.
        for (long id = 0; id < 40; id++)
            foreach (var input in inputs)
                input.Upsert(new TimedTrade(id, (int)(id % 10) + 1, "t", T0.AddSeconds(id)));
        await Eventually(() => sinks.All(s => s.Rows.Count == 40), "forty trades through every pipeline");
        await Eventually(() => service.Pipelines.Get("neighbour").StateSize?.Entries >= 120, "the neighbour's count read");
        Assert.InRange(service.Pipelines.Get("neighbour").StateSize!.Entries, 120, 249);
        await Task.Delay(TimeSpan.FromSeconds(1.5));                                     // a reading of the sampler's at least
        Assert.All(new[] { "capped", "neighbour", "roomy" }, id => Assert.Equal(PipelineState.Running, service.Pipelines.Get(id).State));

        // Forty more: the capped pipeline passes 250 and is failed by the sampler's next reading.
        for (long id = 40; id < 80; id++)
            foreach (var input in inputs)
                input.Upsert(new TimedTrade(id, (int)(id % 10) + 1, "t", T0.AddSeconds(id)));
        await Eventually(() => service.Pipelines.Get("capped").State == PipelineState.Failed, "the capped pipeline failed");
        var capped = service.Pipelines.Get("capped");
        var why = capped.History![^1].Why!;
        Assert.Contains("circuit.maxStateEntries", why);
        Assert.Contains("limit of 250", why);
        var fault = Assert.Single(capped.Faults);
        Assert.Equal(nameof(StateLimitExceededException), fault.ExceptionType);
        Assert.Equal(0, capped.Restarts);

        // The neighbours: running, unrestarted, without a fault, and still delivering.
        await Eventually(() => sinks[1].Rows.Count == 80 && sinks[2].Rows.Count == 80, "the neighbours' trades through");
        for (long id = 80; id < 100; id++)
        {
            inputs[1].Upsert(new TimedTrade(id, 1, "t", T0.AddSeconds(id)));
            inputs[2].Upsert(new TimedTrade(id, 1, "t", T0.AddSeconds(id)));
        }
        await Eventually(() => sinks[1].Rows.Count == 100 && sinks[2].Rows.Count == 100, "the neighbours go on");
        foreach (var id in new[] { "neighbour", "roomy" })
        {
            var status = service.Pipelines.Get(id);
            Assert.Equal(PipelineState.Running, status.State);
            Assert.Equal(0, status.Restarts);
            Assert.Empty(status.Faults);
            Assert.True(status.StateSize!.Entries >= 100);
        }

        // Not retried: a restart would resume the same state and pass the limit at once.
        await Task.Delay(TimeSpan.FromSeconds(2));
        Assert.Equal(PipelineState.Failed, service.Pipelines.Get("capped").State);
        Assert.Single(service.Pipelines.Get("capped").Faults);
        Assert.Equal(80, sinks[0].Rows.Count);
    }

    [Fact]
    public async Task A_limit_raised_by_a_replace_that_keeps_the_state_runs_it_again()
    {
        await using var service = await StartAsync(options: o => o.MaxEventTimeAhead = null);
        await service.DeclareInstrumentsAsync(Instruments(10));
        await service.DeployAsync(Capped("capped", "sl-in", "sl-out", limit: 10));
        var input = service.Memory.KeyedLog<long, TimedTrade>("sl-in");
        for (long id = 0; id < 30; id++)
            input.Upsert(new TimedTrade(id, 1, "t", T0.AddSeconds(id)));
        await Eventually(() => service.Pipelines.Get("capped").State == PipelineState.Failed, "failed past ten entries");

        var stored = service.Pipelines.Get("capped");
        await service.Pipelines.DeployAsync(Capped("capped", "sl-in", "sl-out", limit: 1_000),
            new DeployOptions(StateChoice.Keep, ExpectedGeneration: stored.Generation), default).WaitAsync(TestService.Timeout);
        await Eventually(() => service.Pipelines.Get("capped").State == PipelineState.Running, "running again on its state");
        input.Upsert(new TimedTrade(30, 1, "t", T0.AddSeconds(30)));
        var sink = service.Memory.UpsertSink<long, EnrichedTrade>("sl-out");
        await Eventually(() => sink.Rows.ContainsKey(30), "a trade after the raise");
        Assert.Equal(PipelineState.Running, service.Pipelines.Get("capped").State);
    }

    [Fact]
    public async Task A_limit_below_one_is_refused_at_the_dry_run()
    {
        await using var service = await StartAsync();
        await service.DeclareInstrumentsAsync(Instruments(10));
        var refused = await Assert.ThrowsAsync<ServiceException>(() => service.Pipelines.ValidateAsync(Capped("capped", "sl-in", "sl-out", limit: 0), default));
        Assert.Contains("circuit.maxStateEntries", refused.Message);
    }

    /// <summary>The harness holds a definition to the same limit: a step after which the
    /// state holds more throws, and so does every step after it.</summary>
    [Fact]
    public async Task The_harness_throws_past_the_limit_as_a_node_would_fail_the_pipeline()
    {
        await using var pipeline = await Testing.PipelineTestHarness.For<OpenTrades>()
            .Settings("""{ "openSeconds": 3600 }""")
            .Table(Acme.Contracts.Tables.Instruments, Instruments(10))
            .MaxEventTimeAhead(null)
            .MaxStateEntries(100)
            .StartAsync();
        var trades = pipeline.KeyedInput<long, TimedTrade>("trades");
        for (long id = 0; id < 20; id++)
            await trades.UpsertAsync(new TimedTrade(id, 1, "t", T0.AddSeconds(id)));
        await pipeline.StepAsync();
        Assert.InRange(pipeline.StateSize.Entries, 1, 100);      // a few entries a trade

        for (long id = 20; id < 40; id++)
            await trades.UpsertAsync(new TimedTrade(id, 1, "t", T0.AddSeconds(id)));
        var over = await Assert.ThrowsAsync<StateLimitExceededException>(() => pipeline.StepAsync());
        Assert.Equal(100, over.Limit);
        Assert.Equal(pipeline.StateSize.Entries, over.Entries);
        Assert.Contains("circuit.maxStateEntries", over.Message);
        await Assert.ThrowsAsync<StateLimitExceededException>(() => pipeline.StepAsync());
    }
}
