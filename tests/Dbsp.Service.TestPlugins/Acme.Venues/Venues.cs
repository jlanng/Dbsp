using Dbsp.Service;

[assembly: DbspContract]

namespace Acme.Venues;

/// <summary>A contract only one test imports, and only as a package whose main assembly
/// is neither named after the assembly nor at the package's root — so that when it is
/// loaded, it was loaded from where the manifest says the file is.</summary>
public sealed record Venue(string Mic, string Name);

public sealed class VenuesTable : TableDefinition<string, Venue>
{
    public override string Name => "venues";
    public override string KeyOf(Venue row) => row.Mic;
}
