using Dbsp.Operators;
using Dbsp.Cluster;

namespace Dbsp.Tests;

/// <summary>
/// The dense exchange codec is what every clustered shuffle rides on, and a value that does
/// not survive the round trip is silent cross-process corruption — so the contract under test
/// is exact equality after encode/decode, for the whole type universe the codec claims, plus
/// an explicit "no codec" answer for the types it does not.
/// </summary>
public class ShardCodecTests
{
    private enum Colour : short { Red = 1, Green = 2 }

    private record Person(string Name, int Age);

    private record Nested(Person Owner, Person? Deputy, string?[] Tags);

    private record struct Point(int X, int Y);

    private record Wide(
        bool Flag, byte B, sbyte Sb, short S, ushort Us, int I, uint Ui, long L, ulong Ul, char C,
        float F, double D, decimal M, string Text, Guid Id, Colour Colour,
        DateTime Timestamp, DateTimeOffset Moment, TimeSpan Duration, DateOnly Day, TimeOnly Clock,
        int? MaybeInt, string? MaybeText);

    private sealed class Opaque
    {
        public Opaque(int hidden) => Hidden = hidden;
        internal int Hidden { get; }
    }

    private static T RoundTrip<T>(T value)
    {
        var writer = new WireWriter();
        WireCodec<T>.Write!(writer, value);
        var reader = new WireReader();
        reader.Reset(writer.ToArray());
        return WireCodec<T>.Read!(reader);
    }

    [Fact]
    public void Every_supported_leaf_and_composite_survives_the_round_trip()
    {
        var value = new Wide(
            true, 200, -100, -30000, 60000, -123456789, 3_000_000_000u, -9_000_000_000L, 18_000_000_000_000_000_000ul,
            'ß', 1.5f, -2.25e100, -12345.6789m, "héllo · wörld", Guid.NewGuid(), Colour.Green,
            new DateTime(2026, 8, 31, 12, 34, 56, DateTimeKind.Utc),
            new DateTimeOffset(2026, 8, 31, 12, 34, 56, TimeSpan.FromMinutes(-330)),
            TimeSpan.FromTicks(-987654321), new DateOnly(2026, 8, 31), new TimeOnly(23, 59, 58),
            null, null);

        Assert.Equal(value, RoundTrip(value));
    }

    [Fact]
    public void Nullable_members_round_trip_with_their_values_present()
    {
        var value = new Wide(
            false, 0, 0, 0, 0, 0, 0, 0, 0, '\0', 0, 0, 0m, "", Guid.Empty, Colour.Red,
            DateTime.UnixEpoch, DateTimeOffset.UnixEpoch, TimeSpan.Zero, DateOnly.MinValue, TimeOnly.MinValue,
            42, "present");

        Assert.Equal(value, RoundTrip(value));
    }

    [Fact]
    public void Nested_records_nulls_and_arrays_round_trip()
    {
        var value = new Nested(new Person("Ada", 36), null, ["a", null, ""]);
        var back = RoundTrip(value);

        Assert.Equal(value.Owner, back.Owner);
        Assert.Null(back.Deputy);
        Assert.Equal(value.Tags, back.Tags);
    }

    [Fact]
    public void An_empty_array_is_distinct_from_a_null_one()
    {
        Assert.Empty(RoundTrip(new Nested(new Person("A", 1), null, [])).Tags);
        Assert.Null(RoundTrip(new Nested(new Person("A", 1), null, null!)).Tags);
    }

    [Fact]
    public void The_types_the_runtime_itself_shuffles_round_trip()
    {
        // These are the shapes the library builds around user records: keyed pairs, window
        // keys, tuple keys. If any of them lost a field, every clustered keyed operator would
        // silently mis-route or mis-window.
        Assert.Equal(
            new KeyedValue<string, Point>("k", new Point(3, -4)),
            RoundTrip(new KeyedValue<string, Point>("k", new Point(3, -4))));

        var window = new WindowKey<int>(
            new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
            new DateTimeOffset(2026, 1, 1, 0, 0, 10, TimeSpan.Zero),
            7);
        Assert.Equal(window, RoundTrip(window));

        Assert.Equal((A: 3, B: 9), RoundTrip((A: 3, B: 9)));

        var windowed = new WindowedValue<int, decimal>(window.WindowStart, window.WindowEnd, 7, 12.34m);
        Assert.Equal(windowed, RoundTrip(windowed));

        // A clustered keyed input ships its resolved commands; a delete carries no row.
        var upsert = new KeyedCommand<int, Point>(5, false, new Point(1, 2));
        Assert.Equal(upsert, RoundTrip(upsert));
        var delete = new KeyedCommand<int, string>(5, true, null);
        Assert.Equal(delete, RoundTrip(delete));
    }

    [Fact]
    public void A_type_that_cannot_be_rebuilt_has_no_dense_codec_and_falls_back()
    {
        // No public member the codec could read, and no constructor it could call with what
        // it can read: the answer must be "no codec", not a wrong one.
        Assert.Null(WireCodec<Opaque>.Write);
        Assert.Null(WireCodec<Opaque>.Read);

        // Dictionaries are enumerable but have no stable indexed order, exactly as for the
        // stable hash.
        Assert.Null(WireCodec<Dictionary<string, int>>.Write);
    }

    [Fact]
    public void Weights_and_shard_counts_use_the_compact_integer_encoding()
    {
        // The encoding's whole point is that the common values are one byte: a Z-set weight is
        // almost always +1 or -1, and a small identifier should not cost eight bytes.
        var writer = new WireWriter();
        writer.WriteInt64(1);
        writer.WriteInt64(-1);
        writer.WriteInt32(63);
        Assert.Equal(3, writer.Length);
    }

    [Fact]
    public void A_truncated_frame_is_rejected_rather_than_decoded()
    {
        var writer = new WireWriter();
        WireCodec<Person>.Write!(writer, new Person("Grace", 45));
        var truncated = writer.ToArray()[..2];

        var reader = new WireReader();
        reader.Reset(truncated);
        Assert.ThrowsAny<Exception>(() => WireCodec<Person>.Read!(reader));
    }
}
