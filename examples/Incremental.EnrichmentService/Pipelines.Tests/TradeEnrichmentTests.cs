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
    public async Task A_trade_is_corrected_while_it_is_open_and_left_alone_once_it_is_final()
    {
        await using var pipeline = await StartAsync("""{ "minNotional": 1000, "openSeconds": 60 }""");
        var trades = pipeline.KeyedInput<long, Trade>("trades");
        await trades.UpsertAsync(Trade(1, quantity: 100));                                   // at T0
        await pipeline.StepAsync();
        await trades.UpsertAsync(Trade(2, quantity: 100) with { ExecutedAt = T0.AddMinutes(2) });
        await pipeline.StepAsync();                                                          // the watermark: T0 + 1 min

        await pipeline.Table(Reference.Instruments).UpsertAsync(Abc with { Multiplier = 3m });
        await pipeline.StepAsync();
        var rows = pipeline.UpsertSink<long, EnrichedTrade>("enriched").Rows;
        Assert.Equal(2000m, rows[1].Notional);                  // final: as it was emitted
        Assert.Equal(3000m, rows[2].Notional);                  // open: repriced

        // An amendment of a final trade is behind the watermark: dropped as late.
        await trades.UpsertAsync(Trade(1, quantity: 500));
        await pipeline.StepAsync();
        Assert.Equal(2000m, rows[1].Notional);
        Assert.Equal(1, pipeline.LateRecords);
    }

    [Fact]
    public async Task A_trade_that_first_arrives_behind_the_watermark_is_dropped_and_counted()
    {
        await using var pipeline = await StartAsync("""{ "minNotional": 0, "openSeconds": 60 }""");
        var trades = pipeline.KeyedInput<long, Trade>("trades");
        await trades.UpsertAsync(Trade(1, quantity: 100));                                   // at T0
        // One trade stamped an hour ahead: the watermark is 59 minutes past T0 from here.
        await trades.UpsertAsync(Trade(2, quantity: 100) with { ExecutedAt = T0.AddHours(1) });
        await pipeline.StepAsync();
        Assert.Equal(0, pipeline.LateRecords);

        // Trades with honest times, never seen before: late by their time alone.
        await trades.UpsertAsync(Trade(3, quantity: 100) with { ExecutedAt = T0.AddMinutes(1) });
        await trades.UpsertAsync(Trade(4, quantity: 100) with { ExecutedAt = T0.AddMinutes(2) });
        await pipeline.StepAsync();
        Assert.Equal([1L, 2L], pipeline.UpsertSink<long, EnrichedTrade>("enriched").Rows.Keys.Order());
        Assert.Equal(2, pipeline.LateRecords);
        Assert.Empty(pipeline.DeadLetters);
    }

    [Fact]
    public async Task The_state_holds_the_open_trades_and_no_more_however_long_the_feed()
    {
        await using var pipeline = await StartAsync("""{ "minNotional": 0, "openSeconds": 60 }""");
        var trades = pipeline.KeyedInput<long, Trade>("trades");
        var entries = new List<long>();
        for (var second = 0; second < 1200; second++)       // a trade a second, each a new id
        {
            await trades.UpsertAsync(Trade(second, quantity: 100) with { ExecutedAt = T0.AddSeconds(second) });
            if (second % 10 == 9)
            {
                await pipeline.StepAsync();
                entries.Add(pipeline.StateSize.Entries);
            }
        }
        // Per open trade: its row in the input, what the lookup emitted for it, its fact in
        // each join, and a deadline in each of the four. Open: the 61 seconds from the
        // watermark (60 behind the newest trade) on, and the step's own 10 until the next
        // step evicts what this one made final.
        Assert.InRange(entries.Max(), 1, 8 * (61 + 10));
        Assert.Equal(entries[^1], entries[^50]);                 // level: it stopped growing
        Assert.Equal(1200, pipeline.UpsertSink<long, EnrichedTrade>("enriched").Rows.Count);
    }

    [Fact]
    public async Task Settings_an_operator_could_not_deploy_are_refused_here_too()
    {
        var refused = await Assert.ThrowsAsync<ServiceException>(() => StartAsync("""{ "minNotional": -1 }"""));
        Assert.Contains(refused.Details, d => d.Contains("minNotional"));
    }
}
// </snippet:test-trade-enrichment>
