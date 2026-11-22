using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using Enrichment.Contracts;
using Dbsp;
using Dbsp.Service;

namespace Enrichment.Pipelines;

// <snippet:watchlist-alerts>
/// <summary>
/// An alert for every execution with a counterparty on a watchlist: an append-only result,
/// so it has an append sink — each alert is delivered once per step that produced it and
/// never taken back. Which watchlist is the spec's business: the definition names the slot
/// <c>watchlist</c>, and a spec's <c>"tables": { "watchlist": "sanctions" }</c> binds it,
/// so the same class is deployed once per list.
/// <para>
/// An execution with no quantity cannot be sized, and <see cref="Sized"/> throws on it.
/// Deployed with <c>"errorPolicy": "DeadLetter"</c> the record is dropped and reported —
/// to the pipeline's dead-letter sink if the spec binds one — and the pipeline carries on;
/// under the default policy the same record stops it.
/// </para>
/// </summary>
[Pipeline("watchlist-alerts", Description = "Executions with a counterparty on a watchlist, as an append-only stream of alerts.")]
public sealed class WatchlistAlerts : IPipelineDefinition<WatchlistAlerts.Settings>
{
    public sealed record Settings
    {
        [Range(0, 1e12), Description("Executions with a notional below this raise no alert.")]
        public decimal MinNotional { get; init; }

        [StateNeutral, Description("What an alert from this deployment is marked with.")]
        public string Severity { get; init; } = "review";
    }

    public void Build(PipelineBuilder p, Settings s)
    {
        var alerts = p.Input<Trade>("executions")
            .Select(Sized)
            .Where(x => x.Notional >= s.MinNotional)
            // On arrival, and append-only: nothing is remembered. An execution whose
            // counterparty is not on the list finds no row and is dropped.
            .LookupAppendOnly(p.Table(Reference.Watchlist), x => x.Trade.CounterpartyId,
                (x, entry) => new Alert(x.Trade.Id, x.Trade.ExecutedAt, entry.CounterpartyId, entry.Reason, x.Notional, s.Severity));

        p.AppendSink("alerts", alerts);
    }

    private static SizedExecution Sized(Trade t) =>
        t.Quantity > 0
            ? new SizedExecution(t, t.Quantity * t.Price)
            : throw new ArgumentException($"Execution {t.Id} at {t.ExecutedAt:O} has quantity {t.Quantity}; an execution has a positive quantity.");
}

public sealed record SizedExecution(Trade Trade, decimal Notional);

public sealed record Alert(long TradeId, DateTimeOffset ExecutedAt, string CounterpartyId, string Reason, decimal Notional, string Severity);
// </snippet:watchlist-alerts>
