using Incremental.Service;

[assembly: IncrementalContract]

namespace Acme.Contracts;

public sealed record Instrument(int Id, string Symbol, string Isin, decimal Multiplier);

public sealed record Trade(long Id, int InstrumentId, decimal Quantity, string Trader);

public sealed record EnrichedTrade(long TradeId, string Symbol, decimal Notional, string? Venue = null);

public sealed record Position(int InstrumentId, decimal Quantity);

public sealed class InstrumentsTable : TableDefinition<int, Instrument>
{
    public static readonly IndexDefinition<Instrument, string> ByIsin = new("isin", i => i.Isin);

    public override string Name => "instruments";
    public override int KeyOf(Instrument row) => row.Id;
    protected override void Configure(TableBuilder<int, Instrument> table) => table.Index(ByIsin);
}

/// <summary>A derived table: written by a pipeline's ToTable.</summary>
public sealed class PositionsTable : TableDefinition<int, Position>
{
    public override string Name => "positions";
    public override int KeyOf(Position row) => row.InstrumentId;
}

/// <summary>Instruments under an index whose selector throws on one symbol: a row that
/// faults the table's writer, deterministically.</summary>
public sealed class PoisonableInstrumentsTable : TableDefinition<int, Instrument>
{
    public static readonly IndexDefinition<Instrument, string> BySymbol =
        new("symbol", i => i.Symbol == "POISON" ? throw new InvalidOperationException("poison symbol") : i.Symbol);

    public override string Name => "poisonable";
    public override int KeyOf(Instrument row) => row.Id;
    protected override void Configure(TableBuilder<int, Instrument> table) => table.Index(BySymbol);
}

public static class Tables
{
    public static readonly InstrumentsTable Instruments = new();
    public static readonly PositionsTable Positions = new();
}
