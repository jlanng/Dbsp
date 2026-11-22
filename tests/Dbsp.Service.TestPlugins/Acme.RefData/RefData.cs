using Dbsp.Service;

[assembly: DbspContract]

namespace Acme.RefData;

/// <summary>A contract the test host does not reference: it arrives through the catalog
/// and is loaded into the process's contract context.</summary>
public sealed record Desk(string Trader, string Name);

public sealed class DesksTable : TableDefinition<string, Desk>
{
    public override string Name => "desks";
    public override string KeyOf(Desk row) => row.Trader;
}

public static class RefTables
{
    public static readonly DesksTable Desks = new();
}
