using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using Enrichment.Contracts;
using Incremental;
using Incremental.Service;

namespace Enrichment.Pipelines;

// <snippet:execution-quality>
/// <summary>
/// Two inputs joined to each other: every execution meets the reference price quoted for
/// its instrument in the interval it was done in, and the difference — the slippage, in
/// basis points — is summed per asset class over a sliding window.
/// <para>
/// Both inputs carry event time, so the circuit's watermark is the lower of the two: it
/// only moves when both feeds have moved, and nothing is late merely because one source
/// was read ahead of the other. The join's key is (instrument, interval), and
/// <c>EvictWhen</c> closes a key when the watermark passes the interval's end — from then
/// on neither side holds anything for it, which is what keeps the state of a join between
/// two unbounded streams bounded. The watermark and the keys waiting to close are in the
/// checkpoint with the rows they bound, so that holds across a restart and a takeover
/// too: a key open when the leader died still closes on the node that took over.
/// </para>
/// <para>
/// It is a left join: an execution in an instrument that was not quoted in its interval is
/// still counted, as unpriced. Should the quote arrive after the execution — and before the
/// interval closes — the unpriced row is taken back and the priced one put in its place,
/// through the window totals too.
/// </para>
/// </summary>
[Pipeline("execution-quality", Description = "Executions ⋈ reference quotes on (instrument, interval): slippage per asset class in sliding windows.")]
public sealed class ExecutionQuality : IPipelineDefinition<ExecutionQuality.Settings>
{
    public sealed record Settings
    {
        [Range(1, 86_400), Description("The length, in seconds, of the interval a quote is the reference price for.")]
        public int QuoteSeconds { get; init; } = 60;

        [Range(1, 86_400), Description("The sliding window's length in seconds.")]
        public int WindowSeconds { get; init; } = 300;

        [Range(1, 86_400), Description("How often, in seconds, a new window starts.")]
        public int HopSeconds { get; init; } = 60;

        [Range(0, 3600), Description("How late, in seconds of event time, a record of either input may arrive and still count.")]
        public int LatenessSeconds { get; init; } = 5;
    }

    public void Build(PipelineBuilder p, Settings s)
    {
        var interval = TimeSpan.FromSeconds(s.QuoteSeconds);
        var lateness = TimeSpan.FromSeconds(s.LatenessSeconds);
        IntervalKey In(int instrument, DateTimeOffset at) =>
            new(instrument, new DateTimeOffset(at.UtcTicks - at.UtcTicks % interval.Ticks, TimeSpan.Zero));

        // One slot per input; the spec binds each to a connector of its own.
        var executions = p.Input<Trade>("executions")
            // A table lookup before the stream join: the instrument's asset class, as of
            // arrival. Executions only ever arrive, so the lookup remembers nothing. After
            // the join it would have to (the join takes rows back), one entry per
            // execution with nothing to close them: state that grows for ever.
            .LookupAppendOnly(p.Table(Reference.Instruments), t => t.InstrumentId,
                (t, i) => new ClassedTrade(i.AssetClass, t.InstrumentId, t.ExecutedAt, t.Price))
            .WithEventTime(t => t.ExecutedAt, lateness);
        var quotes = p.Input<Quote>("quotes").WithEventTime(q => q.At, lateness);

        var priced = executions
            .GroupBy(t => In(t.InstrumentId, t.ExecutedAt))
            .EvictWhen(key => key.Start + interval)
            .LeftJoin(
                quotes.GroupBy(q => In(q.InstrumentId, q.At)),
                (_, t, q) => new ClassedExecution(t.AssetClass, t.ExecutedAt, Math.Round((t.Price - q.Mid) / q.Mid * 10_000m, 2)),
                (_, t) => new ClassedExecution(t.AssetClass, t.ExecutedAt, null)).Named("priced");

        var quality = priced
            .WithEventTime(x => x.ExecutedAt, lateness)
            .GroupBy(x => x.AssetClass)
            .HoppingWindow(TimeSpan.FromSeconds(s.WindowSeconds), TimeSpan.FromSeconds(s.HopSeconds))
            .AggregateLinear(new QualityTotals()).Named("quality")
            .Rows()
            .Select(w => new Quality(w.Key, w.WindowStart, w.WindowEnd, w.Value.Priced, w.Value.Unpriced, w.Value.SlippageBps));

        p.UpsertSink("quality", quality, q => $"{q.AssetClass}@{q.WindowStart.UtcDateTime:yyyy-MM-ddTHH:mm:ss}Z");
    }
}

/// <summary>The join's key: an instrument and the start of a quote interval.</summary>
public readonly record struct IntervalKey(int InstrumentId, DateTimeOffset Start);

/// <summary>An execution with its instrument's asset class: what the join keeps per key
/// until the key closes.</summary>
public sealed record ClassedTrade(string AssetClass, int InstrumentId, DateTimeOffset ExecutedAt, decimal Price);

public sealed record ClassedExecution(string AssetClass, DateTimeOffset ExecutedAt, decimal? SlippageBps);

/// <summary>What a window holds per asset class. A linear aggregate keeps this and nothing
/// else per window: a row added or taken back changes it by the row's own contribution.</summary>
public readonly record struct Totals(long Priced, long Unpriced, decimal SlippageBps);

public sealed class QualityTotals : ILinearAggregate<ClassedExecution, Totals>
{
    public Totals Zero => default;

    public Totals Map(ClassedExecution x, long weight) =>
        x.SlippageBps is { } bps ? new Totals(weight, 0, bps * weight) : new Totals(0, weight, 0);

    public Totals Combine(Totals a, Totals b) => new(a.Priced + b.Priced, a.Unpriced + b.Unpriced, a.SlippageBps + b.SlippageBps);
}

public sealed record Quality(string AssetClass, DateTimeOffset WindowStart, DateTimeOffset WindowEnd, long Priced, long Unpriced, decimal SlippageBps);
// </snippet:execution-quality>
