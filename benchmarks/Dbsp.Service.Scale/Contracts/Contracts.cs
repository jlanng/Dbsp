using System.Globalization;
using Dbsp.Service;
using Dbsp.Tables;

[assembly: DbspContract]

namespace Scale.Contracts;

/// <summary>
/// A wide reference row: twenty-one columns, about 520 bytes as JSON. Every string is a
/// string of its own, as it is in a row a loader has just deserialized — a generator that
/// shared literals between rows would flatter the memory numbers.
/// </summary>
public sealed record Party(
    long Id, string Name, string LegalName, string Country, string Region, string Sector, string Rating, string Lei, string Bic,
    string Address1, string Address2, string City, string PostalCode, string AccountManager, string Segment,
    decimal CreditLimit, decimal Exposure, bool Active, DateTimeOffset Onboarded, DateTimeOffset Reviewed, string Notes)
{
    private static readonly string[] Countries = ["GB", "US", "DE", "FR", "JP", "SG", "CH", "NL", "SE", "AU", "CA", "HK"];
    private static readonly string[] Regions = ["EMEA", "AMER", "APAC"];
    private static readonly string[] Sectors = ["Banks", "Insurance", "Asset Management", "Energy", "Technology", "Industrials", "Sovereign"];
    private static readonly string[] Ratings = ["AAA", "AA+", "AA", "A+", "A", "BBB+", "BBB", "BB", "B", "NR"];
    private static readonly DateTimeOffset Epoch = new(2015, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static string Own(string value) => new(value.AsSpan());

    /// <summary>The row of <paramref name="id"/> at <paramref name="revision"/>: the same
    /// arguments always give an equal row, so a model needs no memory.</summary>
    public static Party Make(long id, int revision)
    {
        var n = id.ToString("D9", CultureInfo.InvariantCulture);
        var h = (ulong)id * 0x9E3779B97F4A7C15UL;
        return new Party(
            id,
            "Party " + n,
            "Party " + n + " Holdings International Limited",
            Own(Countries[h % (ulong)Countries.Length]),
            Own(Regions[(h >> 8) % (ulong)Regions.Length]),
            Own(Sectors[(h >> 16) % (ulong)Sectors.Length]),
            Own(Ratings[((h >> 24) + (ulong)revision) % (ulong)Ratings.Length]),
            "5493" + (h % 10_000_000_000_000_000UL).ToString("D16", CultureInfo.InvariantCulture),
            "PTY" + n[..5] + "XX",
            (h % 900 + 1).ToString(CultureInfo.InvariantCulture) + " Reference Data Street, Floor " + (h % 40).ToString(CultureInfo.InvariantCulture),
            "Building " + n[3..],
            "City " + (h % 5000).ToString(CultureInfo.InvariantCulture),
            "PC" + n[2..8],
            "manager." + (h % 2000).ToString(CultureInfo.InvariantCulture) + "@example.test",
            Own(h % 3 == 0 ? "Institutional" : h % 3 == 1 ? "Corporate" : "Retail"),
            1_000_000m + h % 50_000_000,
            (decimal)((h >> 12) % 10_000_000) + revision,
            h % 17 != 0,
            Epoch.AddMinutes(h % 5_000_000),
            Epoch.AddMinutes(h % 5_000_000 + (ulong)revision * 1440),
            "Reviewed at revision " + revision.ToString(CultureInfo.InvariantCulture) + "; no exceptions recorded for " + n + ".");
    }
}

public sealed class PartiesTable : TableDefinition<long, Party>
{
    /// <summary>An alternate key, as reference data usually has one.</summary>
    public static readonly IndexDefinition<Party, string> ByLei = new("lei", p => p.Lei);

    public override string Name => "parties";
    public override long KeyOf(Party row) => row.Id;
    /// <summary>Set in a node's environment by the rig's <c>--no-pacing</c>: the table is
    /// loaded without <see cref="SharedTableOptions.LoadPacing"/>.</summary>
    public const string NoPacingVariable = "SCALE_NO_LOAD_PACING";

    protected override void Configure(TableBuilder<long, Party> table)
    {
        table.Index(ByLei);
        if (Environment.GetEnvironmentVariable(NoPacingVariable) == "1")
            table.Options = new SharedTableOptions { LoadPacing = null };
    }
}

public static class Reference
{
    public static readonly PartiesTable Parties = new();
}

/// <summary>The fact: an order for a party.</summary>
public sealed record Order(long Id, long PartyId, string Symbol, decimal Quantity, decimal Price, DateTimeOffset At)
{
    public static Order Make(long id, long parties, int revision)
    {
        var h = (ulong)id * 0xD6E8FEB86659FD93UL;
        return new Order(
            id, (long)(h % (ulong)parties), "SYM" + (h >> 20) % 4000, 1m + (h >> 8) % 5000 + revision, 10m + (h >> 4) % 9000 / 100m,
            DateTimeOffset.UnixEpoch.AddSeconds(1_700_000_000 + id % 86_400));
    }
}
