using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using Acme.Contracts;
using Dbsp;
using Dbsp.Service;

namespace Acme.Pipelines;

/// <summary>Joins trades to the shared instruments table. V2 (the same source compiled
/// with V2 defined, under the same assembly name) adds a venue to the result.</summary>
[Pipeline("trade-enrichment", Description = "Trades joined to instruments.")]
public sealed class TradeEnrichment : IPipelineDefinition<TradeEnrichment.Options>
{
    public sealed record Options
    {
        [Range(0, 1e9), Description("Trades below this quantity are dropped.")]
        public decimal MinQuantity { get; init; }

        [StateNeutral, Description("A label; it changes no stored row.")]
        public string? Label { get; init; }

        public Mode Mode { get; init; } = Mode.Normal;
    }

    public enum Mode { Normal, Strict }

    public void Build(PipelineBuilder p, Options o)
    {
        var instruments = p.Table(Tables.Instruments);
        var enriched = p.Input<Trade>("trades")
            .Where(t => t.Quantity >= o.MinQuantity)
            .GroupBy(t => t.InstrumentId)
            .Join(instruments, (_, t, i) => new EnrichedTrade(t.Id, i.Symbol, t.Quantity * i.Multiplier, Version.Venue)).Named("joined");
        p.UpsertSink("enriched", enriched, e => e.TradeId);
    }
}

/// <summary>State that must survive a restart and a version change: the running count of
/// trades per trader, held in a private type the two versions each define for themselves.</summary>
[Pipeline("trader-counts")]
public sealed class TraderCounts : IPipelineDefinition
{
    public void Build(PipelineBuilder p)
    {
        var counts = p.Input<Trade>("trades")
            .Select(t => new Seen(t.Trader, t.Id))
            .Distinct().Named("seen")
            .GroupBy(s => s.Trader)
            .Count().Named("count")
            .Select(kv => $"{Version.Name}:{kv.Key}={kv.Value}");
        p.UpsertSink("counts", counts, line => line[(line.IndexOf(':') + 1)..line.IndexOf('=')]);
    }
}

/// <summary>Private to the plugin: each loaded version has its own type of this name.</summary>
public sealed record Seen(string Trader, long TradeId);

/// <summary>A sink row of a type private to the plugin, on both kinds of sink: what a
/// connector's record format is given to write is then a type of the plugin's own load
/// context, and the context unloads only if the connector kept none of it.</summary>
[Pipeline("trade-notes")]
public sealed class TradeNotes : IPipelineDefinition
{
    public void Build(PipelineBuilder p)
    {
        var notes = p.Input<Trade>("trades").Select(t => new Note(t.Id, t.Trader, Version.Name));
        p.UpsertSink("notes", notes, n => n.TradeId);
        p.AppendSink("log", notes);
    }
}

public sealed record Note(long TradeId, string Trader, string Version);

public static class Version
{
#if V2
    public const string Name = "v2";
    public const string? Venue = "XLON";
#else
    public const string Name = "v1";
    public const string? Venue = null;
#endif
}
