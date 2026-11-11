using Incremental.Service;

// <snippet:contract>
// A contract assembly: the row types of shared tables and the tables' definitions. It is
// loaded once per process and never unloaded, so every plugin version sees the same types.
[assembly: IncrementalContract]

namespace Enrichment.Contracts;

// Rows are immutable records: every pipeline in the process is handed the same objects.
public sealed record Instrument(int Id, string Symbol, string Isin, string AssetClass, decimal Multiplier);

public sealed record Counterparty(string Id, string Name, string Country, string Rating);

public sealed record Trader(int Id, string Name, int DeskId);

public sealed record Desk(int Id, string Name, string Region);

/// <summary>A derived row: a trader with their desk, written by the trader-desks pipeline.</summary>
public sealed record TraderDesk(int TraderId, string Trader, int DeskId, string Desk, string Region);

public sealed class InstrumentsTable : TableDefinition<int, Instrument>
{
    // An index is declared once, with its selector, and read as table.By(InstrumentsTable.ByIsin).
    public static readonly IndexDefinition<Instrument, string> ByIsin = new("isin", i => i.Isin);

    public override string Name => "instruments";
    public override int KeyOf(Instrument row) => row.Id;
    protected override void Configure(TableBuilder<int, Instrument> table) => table.Index(ByIsin);
}
// </snippet:contract>

public sealed class CounterpartiesTable : TableDefinition<string, Counterparty>
{
    public static readonly IndexDefinition<Counterparty, string> ByCountry = new("country", c => c.Country);

    public override string Name => "counterparties";
    public override string KeyOf(Counterparty row) => row.Id;
    protected override void Configure(TableBuilder<string, Counterparty> table) => table.Index(ByCountry);
}

public sealed class TradersTable : TableDefinition<int, Trader>
{
    public override string Name => "traders";
    public override int KeyOf(Trader row) => row.Id;
}

public sealed class DesksTable : TableDefinition<int, Desk>
{
    public override string Name => "desks";
    public override int KeyOf(Desk row) => row.Id;
}

public sealed class TraderDesksTable : TableDefinition<int, TraderDesk>
{
    public static readonly IndexDefinition<TraderDesk, string> ByDesk = new("desk", td => td.Desk);

    public override string Name => "trader-desks";
    public override int KeyOf(TraderDesk row) => row.TraderId;
    protected override void Configure(TableBuilder<int, TraderDesk> table) => table.Index(ByDesk);
}

/// <summary>A counterparty someone wants to hear about, and why.</summary>
public sealed record WatchlistEntry(string CounterpartyId, string Reason);

/// <summary>A watchlist's shape. It is declared under more than one name — <c>sanctions</c>,
/// <c>credit-watch</c> — and a pipeline's spec says which of them its slot reads.</summary>
public sealed class WatchlistTable : TableDefinition<string, WatchlistEntry>
{
    public override string Name => "watchlist";
    public override string KeyOf(WatchlistEntry row) => row.CounterpartyId;
}

/// <summary>The tables, for <c>p.Table(Reference.Instruments)</c>.</summary>
public static class Reference
{
    public static readonly InstrumentsTable Instruments = new();
    public static readonly CounterpartiesTable Counterparties = new();
    public static readonly TradersTable Traders = new();
    public static readonly DesksTable Desks = new();
    public static readonly TraderDesksTable TraderDesks = new();
    public static readonly WatchlistTable Watchlist = new();
}

/// <summary>
/// The trade feed's record. It is not a table row, but the host's source adapter and every
/// plugin must agree on it, so it lives here with the other shared types.
/// </summary>
public sealed record Trade(
    long Id, int InstrumentId, int TraderId, string CounterpartyId, long Quantity, decimal Price, DateTimeOffset ExecutedAt);

/// <summary>The reference price feed's record: an instrument's mid price for the interval
/// that starts at <c>At</c>. Not every instrument is quoted in every interval.</summary>
public sealed record Quote(int InstrumentId, DateTimeOffset At, decimal Mid);
