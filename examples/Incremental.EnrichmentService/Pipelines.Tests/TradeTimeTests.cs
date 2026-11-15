using Enrichment.Contracts;
using Incremental;
using Incremental.Service.Testing;

namespace Enrichment.Pipelines.Tests;

/// <summary>What <c>trade-enrichment</c> makes of a trade's time at the edges: an id
/// that comes back as another trade, and a trade stamped in the future.</summary>
public class TradeTimeTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 5, 9, 0, 0, TimeSpan.Zero);

    private static PipelineTestHarness.Builder Enrichment(string settings) =>
        PipelineTestHarness.For<TradeEnrichment>()
            .Settings(settings)
            .Table(Reference.Instruments, [new Instrument(1, "ABC", "GB0000000001", "Equity", 2m)])
            .Table(Reference.Counterparties, [new Counterparty("CP0001", "Acme Holdings", "GB", "AA")])
            .Table(Reference.TraderDesks, [new TraderDesk(7, "Ada A.", 3, "Macro", "EMEA")]);

    /// <summary>
    /// Ids come round after forty trades, a trade a second, and a trade stays open for
    /// thirty: an id's next trade is another trade. Every tenth is also amended a moment
    /// later, and every seventh reuse is below the minimum notional. Read one trade a
    /// step, as a pipeline that keeps up reads them, and read as one backlog — after a
    /// restart, a takeover, a pipeline that fell behind — the sink holds the same rows.
    /// Before round five the backlog took an id's two trades for one trade amended.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task An_id_used_again_after_its_trade_is_final_gives_the_same_rows_one_by_one_and_as_a_backlog(int workers)
    {
        var feed = new List<Trade>();
        for (var i = 0; i < 400; i++)
        {
            var quantity = i % 7 == 0 && i >= 40 ? 1 : 100 + i;       // 1 × 10 × 2 = 20: below the minimum
            feed.Add(new Trade(i % 40, 1, 7, "CP0001", quantity, 10m, T0.AddSeconds(i)));
            if (i % 10 == 3)
                feed.Add(feed[^1] with { Quantity = quantity + 5 });  // an amendment, the same second
        }

        async Task<(SortedDictionary<long, EnrichedTrade> Rows, long Late)> ReadAsync(Func<int, bool> endsStep)
        {
            await using var pipeline = await Enrichment("""{ "minNotional": 1000, "openSeconds": 30 }""").Workers(workers).StartAsync();
            var trades = pipeline.KeyedInput<long, Trade>("trades");
            for (var i = 0; i < feed.Count; i++)
            {
                await trades.UpsertAsync(feed[i]);
                if (endsStep(i))
                    await pipeline.StepAsync();
            }
            await pipeline.StepAsync();
            await pipeline.StepAsync();
            return (new(pipeline.UpsertSink<long, EnrichedTrade>("enriched").Rows.ToDictionary()), pipeline.LateRecords);
        }

        var oneByOne = await ReadAsync(_ => true);
        Assert.Equal(40, oneByOne.Rows.Count);
        Assert.Equal(0, oneByOne.Late);
        foreach (var (name, endsStep) in new (string, Func<int, bool>)[] { ("a backlog", _ => false), ("steps of ninety", i => i % 90 == 89), ("steps of seven", i => i % 7 == 6) })
        {
            var (rows, late) = await ReadAsync(endsStep);
            Assert.True(late == 0, name);
            Assert.True(oneByOne.Rows.SequenceEqual(rows), $"{name}: the sink differs from the one read a trade a step");
        }
    }

    private sealed class At(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    /// <summary>
    /// A trade stamped a year ahead. Taken, it would move the watermark to its time:
    /// every trade after it late, every open trade closed. It is a record in error
    /// instead — more than a day (the node's default, <c>circuit.maxEventTimeAhead</c> in
    /// a spec) ahead of the node's clock — and what the pipeline's error policy makes of
    /// one: here a dead letter.
    /// </summary>
    [Fact]
    public async Task A_trade_stamped_far_in_the_future_is_a_dead_letter_and_the_trades_after_it_are_not_late()
    {
        await using var pipeline = await Enrichment("""{ "minNotional": 0, "openSeconds": 60 }""")
            .ErrorPolicy(RecordErrorPolicy.DeadLetter)
            .Clock(new At(T0.AddMinutes(5)))
            .StartAsync();
        var trades = pipeline.KeyedInput<long, Trade>("trades");
        await trades.UpsertAsync(new Trade(1, 1, 7, "CP0001", 100, 10m, T0));
        await trades.UpsertAsync(new Trade(2, 1, 7, "CP0001", 100, 10m, T0.AddDays(365)));
        await trades.UpsertAsync(new Trade(3, 1, 7, "CP0001", 100, 10m, T0.AddMinutes(1)));
        await pipeline.StepAsync();
        await trades.UpsertAsync(new Trade(4, 1, 7, "CP0001", 100, 10m, T0.AddMinutes(2)));
        await pipeline.StepAsync();

        Assert.Equal([1L, 3L, 4L], pipeline.UpsertSink<long, EnrichedTrade>("enriched").Rows.Keys.Order());
        Assert.Equal(0, pipeline.LateRecords);
        Assert.Equal(T0.AddMinutes(1), pipeline.Watermark);
        var letter = Assert.Single(pipeline.DeadLetters);
        Assert.Equal(2, ((Trade)letter.Record!).Id);
        Assert.IsType<EventTimeAheadException>(letter.Exception);

        // A day is the node's default; a pipeline whose time is rightly ahead says so.
        await using var settlement = await Enrichment("""{ "minNotional": 0, "openSeconds": 60 }""")
            .Clock(new At(T0)).MaxEventTimeAhead(TimeSpan.FromDays(400)).StartAsync();
        await settlement.KeyedInput<long, Trade>("trades").UpsertAsync(new Trade(2, 1, 7, "CP0001", 100, 10m, T0.AddDays(365)));
        await settlement.StepAsync();
        Assert.Single(settlement.UpsertSink<long, EnrichedTrade>("enriched").Rows);
    }
}
