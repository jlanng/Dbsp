using Acme.Contracts;
using Acme.RefData;
using Incremental;
using Incremental.Service;

namespace Acme.Desks;

/// <summary>A second plugin. It joins the same instruments table as Acme.Pipelines — the
/// two agree on the Instrument type because both get the host's contract assembly — and
/// looks each trade's desk up in a table whose contract arrived through the catalog.</summary>
[Pipeline("desk-notional")]
public sealed class DeskNotional : IPipelineDefinition
{
    public void Build(PipelineBuilder p)
    {
        // A call into the plugin's private NuGet dependency: the definition cannot be
        // described, let alone run, unless the package holds that assembly.
        if (K4os.Compression.LZ4.LZ4Codec.MaximumOutputSize(16) < 16)
            throw new InvalidOperationException("LZ4 bounds an output below its input.");
        var instruments = p.Table(Tables.Instruments);
        var desks = p.Table(RefTables.Desks);
        var lines = p.Input<Trade>("trades")
            .GroupBy(t => t.InstrumentId)
            .Join(instruments, (_, t, i) => new Priced(t.Id, t.Trader, t.Quantity * i.Multiplier)).Named("priced")
            .Lookup(desks, x => x.Trader, (x, d) => $"{x.TradeId}|{d.Name}|{x.Notional:0.##}");
        p.UpsertSink("lines", lines, line => long.Parse(line[..line.IndexOf('|')]));
    }
}

public sealed record Priced(long TradeId, string Trader, decimal Notional);
