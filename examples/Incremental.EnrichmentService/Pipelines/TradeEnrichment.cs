using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using Enrichment.Contracts;
using Incremental;
using Incremental.Service;

namespace Enrichment.Pipelines;

// <snippet:trade-enrichment>
/// <summary>
/// Trades enriched from three shared tables, each join of the kind its column needs: the
/// counterparty is looked up when the trade arrives and then left alone; the instrument
/// and the trader's desk are joined, so a change to either corrects every trade it touches.
/// <para>
/// Every trade is a new key, so nothing would ever close one: a retroactive join keeps
/// every fact it has joined, and a keyed input every row it has had. What bounds them is
/// the trade's own time. A trade is <em>open</em> for <c>OpenSeconds</c> of event time —
/// the watermark runs that far behind the newest trade seen — and while it is open an
/// amendment replaces it and a change of reference data corrects it. Then it is
/// <em>final</em>: an amendment that arrives later is dropped as late, its row in the sink
/// stays as last emitted, and every operator below forgets it (<c>evictWhenFinal</c> on
/// the input, <c>EvictWhenFinal()</c> on each grouping). The state is about
/// <c>OpenSeconds × trades per second</c> trades, whatever the feed's length. Copy this
/// shape for any retroactive join over a stream of facts.
/// </para>
/// </summary>
[Pipeline("trade-enrichment", Description = "Trades with counterparty (on arrival), instrument and desk (retroactive while open), filtered by notional.")]
public sealed class TradeEnrichment : IPipelineDefinition<TradeEnrichment.Settings>
{
    public sealed record Settings
    {
        [Range(0, 1e12), Description("Trades with a notional below this are dropped.")]
        public decimal MinNotional { get; init; }

        [Range(1, 30 * 86_400), Description("How long, in seconds of event time behind the newest trade, a trade stays open to amendments and reference-data corrections. Then it is final and its state goes.")]
        public int OpenSeconds { get; init; } = 3600;

        [StateNeutral, Description("A label for operators; it changes no stored row.")]
        public string? Label { get; init; }
    }

    public void Build(PipelineBuilder p, Settings s)
    {
        var counterparties = p.Table(Reference.Counterparties);
        var instruments = p.Table(Reference.Instruments);
        var traderDesks = p.Table(Reference.TraderDesks);

        var enriched = p.KeyedInput<long, Trade>("trades", t => t.Id)
            // The trade's time, and how long it stays open: the input forgets a trade's row
            // once it is final.
            .WithEventTime(t => t.ExecutedAt, TimeSpan.FromSeconds(s.OpenSeconds), evictWhenFinal: true)
            // Enrich on arrival: who the counterparty was when the trade was booked.
            .GroupBy(t => t.CounterpartyId).EvictWhenFinal()
            .Lookup(counterparties,
                (_, t, c) => new Booked(t, c.Name, c.Rating),
                (_, t) => new Booked(t, "(unknown)", "NR")).Named("booked")
            // Retroactive while open: a corrected multiplier reprices the instrument's open trades.
            .WithEventTime(b => b.Trade.ExecutedAt)
            .GroupBy(b => b.Trade.InstrumentId).EvictWhenFinal()
            .Join(instruments, (_, b, i) => new Priced(b, i.Symbol, b.Trade.Quantity * b.Trade.Price * i.Multiplier)).Named("priced")
            .Where(x => x.Notional >= s.MinNotional)
            // Retroactive while open: a trader who moves desk takes their open trades with them.
            .WithEventTime(x => x.Booked.Trade.ExecutedAt)
            .GroupBy(x => x.Booked.Trade.TraderId).EvictWhenFinal()
            .Join(traderDesks, (_, x, td) => Row(x, td)).Named("with-desk");

        p.UpsertSink("enriched", enriched, e => e.TradeId);
    }

    private static EnrichedTrade Row(Priced x, TraderDesk td) => new(
        x.Booked.Trade.Id, x.Symbol, x.Booked.Counterparty, x.Booked.Rating, td.Trader, td.Desk, td.Region, x.Notional
#if V2
        , x.Notional >= 1_000_000m ? "block" : "regular"     // version 2: one more computed column
#endif
        );
}

// Private to the plugin: every loaded version has its own types of these names. Booked and
// Priced are what the two joins store per trade, so their shape is part of the state.
public sealed record Booked(Trade Trade, string Counterparty, string Rating);

public sealed record Priced(Booked Booked, string Symbol, decimal Notional);

public sealed record EnrichedTrade(
    long TradeId, string Symbol, string Counterparty, string Rating, string Trader, string Desk, string Region, decimal Notional
#if V2
    , string Size
#endif
    );
// </snippet:trade-enrichment>
