using Enrichment.Contracts;
using Incremental;
using Incremental.Service.Testing;

namespace Enrichment.Pipelines.Tests;

public class WatchlistAlertsTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 5, 9, 0, 0, TimeSpan.Zero);

    private static Trade Execution(long id, string counterparty, long quantity = 100) => new(id, 1, 7, counterparty, quantity, 10m, T0.AddSeconds(id));

    // <snippet:test-dead-letters>
    [Fact]
    public async Task An_execution_that_cannot_be_sized_is_a_dead_letter_and_the_rest_carry_on()
    {
        await using var pipeline = await PipelineTestHarness.For<WatchlistAlerts>()
            .Settings("""{ "severity": "block" }""")
            .Table(Reference.Watchlist, [new WatchlistEntry("CP0001", "sanctioned")])
            .ErrorPolicy(RecordErrorPolicy.DeadLetter)          // a spec's "circuit": { "errorPolicy": "DeadLetter" }
            .StartAsync();

        await pipeline.InsertAsync("executions",
        [
            Execution(1, "CP0001"),
            Execution(2, "CP0001", quantity: 0),                // what the generator's poison flag produces
            Execution(3, "CP0002"),                             // not on the list
            Execution(4, "CP0001"),
        ]);
        await pipeline.StepAsync();

        Assert.Equal([1L, 4L], pipeline.AppendSink<Alert>("alerts").Records.Select(a => a.TradeId).Order());
        Assert.All(pipeline.AppendSink<Alert>("alerts").Records, a => Assert.Equal(("sanctioned", "block", 1000m), (a.Reason, a.Severity, a.Notional)));

        var dropped = Assert.Single(pipeline.DeadLetters);
        Assert.Equal(Execution(2, "CP0001", quantity: 0), dropped.Record);
        Assert.Contains("has quantity 0", dropped.Exception.Message);
    }

    [Fact]
    public async Task Under_the_default_policy_the_same_execution_stops_the_pipeline()
    {
        await using var pipeline = await PipelineTestHarness.For<WatchlistAlerts>().StartAsync();
        await pipeline.InsertAsync("executions", [Execution(2, "CP0001", quantity: 0)]);
        var fault = await Assert.ThrowsAnyAsync<Exception>(() => pipeline.StepAsync());
        Assert.Contains("has quantity 0", fault.ToString());
    }
    // </snippet:test-dead-letters>

    [Fact]
    public async Task The_watchlist_slot_reads_whichever_table_the_spec_binds_it_to()
    {
        // Two deployments of one definition, as the example seeds them.
        await using var sanctions = await PipelineTestHarness.For<WatchlistAlerts>()
            .Table(Reference.Watchlist, [new WatchlistEntry("CP0001", "sanctioned")], name: "sanctions")
            .BindTable("watchlist", "sanctions")
            .StartAsync();
        await using var credit = await PipelineTestHarness.For<WatchlistAlerts>()
            .Settings("""{ "minNotional": 5000 }""")
            .Table(Reference.Watchlist, [new WatchlistEntry("CP0002", "downgrade review")], name: "credit-watch")
            .BindTable("watchlist", "credit-watch")
            .StartAsync();
        Assert.Equal(["sanctions"], sanctions.Shape.TablesRead);

        var executions = new[] { Execution(1, "CP0001"), Execution(2, "CP0002"), Execution(3, "CP0002", quantity: 900) };
        foreach (var pipeline in new[] { sanctions, credit })
        {
            await pipeline.InsertAsync("executions", executions);
            await pipeline.StepAsync();
        }
        Assert.Equal([1L], sanctions.AppendSink<Alert>("alerts").Records.Select(a => a.TradeId));
        Assert.Equal([3L], credit.AppendSink<Alert>("alerts").Records.Select(a => a.TradeId));     // 2 is below its minimum

        // The lookup is as of arrival: a counterparty listed later raises no alert for the past.
        await sanctions.Table(Reference.Watchlist, "sanctions").UpsertAsync(new WatchlistEntry("CP0002", "sanctioned"));
        await sanctions.InsertAsync("executions", [Execution(4, "CP0002")]);
        await sanctions.StepAsync();
        Assert.Equal([1L, 4L], sanctions.AppendSink<Alert>("alerts").Records.Select(a => a.TradeId));
    }

    [Fact]
    public async Task It_keeps_no_state_at_all()
    {
        await using var pipeline = await PipelineTestHarness.For<WatchlistAlerts>()
            .Table(Reference.Watchlist, [new WatchlistEntry("CP0001", "sanctioned")])
            .StartAsync();
        for (var id = 1; id <= 1000; id++)
            await pipeline.InsertAsync("executions", [Execution(id, id % 2 == 0 ? "CP0001" : "CP0002")]);
        await pipeline.StepAsync();
        Assert.Equal(500, pipeline.AppendSink<Alert>("alerts").Records.Count);
        // Every operator is append-only: the lookup on arrival keeps nothing, so neither
        // does the pipeline.
        Assert.Equal(0, pipeline.StateSize.Entries);
    }
}
