using Enrichment.Contracts;
using Incremental.Service;
using Incremental.Service.Testing;

namespace Enrichment.Pipelines.Tests;

// <snippet:test-trade-enrichment>
public class TradeEnrichmentTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 5, 9, 0, 0, TimeSpan.Zero);

    private static readonly Instrument Abc = new(1, "ABC", "GB0000000001", "Equity", 2m);
    private static readonly Counterparty Acme = new("CP0001", "Acme Holdings", "GB", "AA");
    private static readonly TraderDesk Ada = new(7, "Ada A.", 3, "Macro", "EMEA");

    // The definition, its settings as a spec carries them, and the tables it reads with
    // their first rows. Nothing else exists: no node, no connector, no catalog.
    private static Task<PipelineTestHarness> StartAsync(string settings = """{ "minNotional": 1000 }""") =>
        PipelineTestHarness.For<TradeEnrichment>()
            .Settings(settings)
            .Table(Reference.Instruments, [Abc])
            .Table(Reference.Counterparties, [Acme])
            .Table(Reference.TraderDesks, [Ada])
            .StartAsync();

    private static Trade Trade(long id, long quantity, string counterparty = "CP0001") => new(id, 1, 7, counterparty, quantity, 10m, T0);

    [Fact]
    public async Task A_trade_is_joined_to_its_three_tables_and_filtered_by_notional()
    {
        await using var pipeline = await StartAsync();
        var trades = pipeline.KeyedInput<long, Trade>("trades");
        await trades.UpsertAsync(Trade(1, quantity: 100));      // 100 × 10 × multiplier 2 = 2,000
        await trades.UpsertAsync(Trade(2, quantity: 10));       // 200: below the minimum
        await pipeline.StepAsync();                              // returns when the sinks are written

        var enriched = pipeline.UpsertSink<long, EnrichedTrade>("enriched").Rows;
        Assert.Equal(new EnrichedTrade(1, "ABC", "Acme Holdings", "AA", "Ada A.", "Macro", "EMEA", 2000m), Assert.Single(enriched).Value);
    }

    [Fact]
    public async Task The_instrument_and_the_desk_are_retroactive_and_the_counterparty_is_as_of_arrival()
    {
        await using var pipeline = await StartAsync();
        await pipeline.KeyedInput<long, Trade>("trades").UpsertAsync(Trade(1, quantity: 100));
        await pipeline.StepAsync();

        // Three tables change. Two are joined, one was looked up when the trade arrived.
        await pipeline.Table(Reference.Instruments).UpsertAsync(Abc with { Multiplier = 3m });
        await pipeline.Table(Reference.TraderDesks).UpsertAsync(Ada with { Desk = "Rates", Region = "APAC" });
        await pipeline.Table(Reference.Counterparties).UpsertAsync(Acme with { Name = "Acme (renamed)" });
        await pipeline.StepAsync();

        var row = pipeline.UpsertSink<long, EnrichedTrade>("enriched").Rows[1];
        Assert.Equal(3000m, row.Notional);                      // repriced
        Assert.Equal(("Rates", "APAC"), (row.Desk, row.Region)); // moved
        Assert.Equal("Acme Holdings", row.Counterparty);        // left alone
    }

    [Fact]
    public async Task An_unknown_counterparty_is_marked_and_a_deleted_trade_leaves_the_sink()
    {
        await using var pipeline = await StartAsync();
        var trades = pipeline.KeyedInput<long, Trade>("trades");
        await trades.UpsertAsync(Trade(1, quantity: 100, counterparty: "CP9999"));
        await pipeline.StepAsync();
        var sink = pipeline.UpsertSink<long, EnrichedTrade>("enriched");
        Assert.Equal(("(unknown)", "NR"), (sink.Rows[1].Counterparty, sink.Rows[1].Rating));

        await trades.DeleteAsync(1);
        await pipeline.StepAsync();
        Assert.Empty(sink.Rows);
    }

    [Fact]
    public async Task Settings_an_operator_could_not_deploy_are_refused_here_too()
    {
        var refused = await Assert.ThrowsAsync<ServiceException>(() => StartAsync("""{ "minNotional": -1 }"""));
        Assert.Contains(refused.Details, d => d.Contains("minNotional"));
    }
}
// </snippet:test-trade-enrichment>
