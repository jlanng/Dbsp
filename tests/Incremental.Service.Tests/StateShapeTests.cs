using Incremental.Kafka;
namespace Incremental.Service.Tests;

/// <summary>The evidence for a keep-or-reset choice: only what state depends on is
/// compared, and a difference is named down to the member and what it does to entries.</summary>
public class StateShapeTests
{
    // Two "versions" of the same types: the nested class stands for the assembly version,
    // and the comparison is made on names with it taken out.
    private static class V1
    {
        public sealed record Seen(string Trader, long TradeId);
        public sealed record Booked(long Id, string Counterparty, Rating Rating);
        public enum Rating { A = 0, B = 1 }
        public sealed record SinkRow(long Id, string Text);
    }

    private static class V2
    {
        public sealed record Seen(string Trader, long TradeId, string? Venue);
        public sealed record Booked(long Id, string Counterparty, Rating Rating, decimal Notional);
        public enum Rating { A = 0, B = 2, C = 3 }
        public sealed record SinkRow(long Id, string Text, string Size);
    }

    private static StateShape Shape(params CircuitStateStore[] stores)
    {
        var shape = StateShapes.Describe(stores, new Dictionary<string, (Type, Type)> { ["instruments"] = (typeof(int), typeof(string)) });
        static string Plain(string text) => text.Replace(".V1.", ".").Replace(".V2.", ".");
        return new StateShape(
            shape.Stores.ToDictionary(s => s.Key, s => s.Value with
            {
                Key = s.Value.Key is null ? null : Plain(s.Value.Key),
                Value = Plain(s.Value.Value),
                KeyReach = [.. s.Value.KeyReach.Select(Plain)],
                ValueReach = [.. s.Value.ValueReach.Select(Plain)],
            }),
            shape.Tables,
            shape.Types.ToDictionary(t => Plain(t.Key), t => (IReadOnlyList<string>)[.. t.Value.Select(Plain)]));
    }

    private static CircuitStateStore Weight<T>(string name) => new(name + ".w0", 1, CircuitStateStoreKind.Weight, null, typeof(T));

    private static CircuitStateStore Value<TKey, TValue>(string name) => new(name + ".w0", 2, CircuitStateStoreKind.Value, typeof(TKey), typeof(TValue));

    private static CircuitStateStore Indexed<TKey, TValue>(string name) => new(name + ".w0", 3, CircuitStateStoreKind.IndexedWeight, typeof(TKey), typeof(TValue));

    [Fact]
    public void Shape_holds_the_stores_of_one_worker_and_every_type_they_are_made_of()
    {
        var shape = Shape(
            Weight<V1.Seen>("named.seen.distinct"),
            new CircuitStateStore("named.seen.distinct.w1", 1, CircuitStateStoreKind.Weight, null, typeof(V1.Seen)),
            Value<long, KeyedValue<string, V1.Booked?[]>>("named.booked.rows"),
            Indexed<(int, string), List<long>>("n4.join.left"));

        Assert.Equal(["n4.join.left", "named.booked.rows", "named.seen.distinct"], shape.Stores.Keys.Order(StringComparer.Ordinal));
        var booked = shape.Stores["named.booked.rows"];
        Assert.Equal("Int64", booked.Key);
        Assert.Equal("Incremental.KeyedValue<String, Incremental.Service.Tests.StateShapeTests.Booked[]>", booked.Value);
        Assert.Empty(booked.KeyReach);
        Assert.Contains("Incremental.Service.Tests.StateShapeTests.Booked", booked.ValueReach);
        Assert.Contains("Incremental.Service.Tests.StateShapeTests.Rating", booked.ValueReach);   // through Booked's member

        Assert.Equal(["Counterparty: String", "Id: Int64", "Rating: Incremental.Service.Tests.StateShapeTests.Rating"],
            shape.Types["Incremental.Service.Tests.StateShapeTests.Booked"]);
        Assert.Equal(["A = 0", "B = 1"], shape.Types["Incremental.Service.Tests.StateShapeTests.Rating"]);
        Assert.Equal(["TradeId: Int64", "Trader: String"], shape.Types["Incremental.Service.Tests.StateShapeTests.Seen"]);
        // A tuple and a list are what they are made of; the runtime's own types are leaves.
        Assert.Equal("ValueTuple<Int32, String>", shape.Stores["n4.join.left"].Key);
        Assert.DoesNotContain(shape.Types.Keys, t => t.StartsWith("ValueTuple") || t.StartsWith("List") || t == "String");
        Assert.Equal(("Int32", "String"), shape.Tables["instruments"]);
    }

    [Fact]
    public void A_record_type_is_spelled_out_with_the_types_it_is_made_of()
    {
        // The type itself first, then what it reaches; members in a stable order.
        Assert.Equal(
            [
                "Incremental.Service.Tests.StateShapeTests.V1.Booked { Counterparty: String; Id: Int64; Rating: Incremental.Service.Tests.StateShapeTests.V1.Rating }",
                "Incremental.Service.Tests.StateShapeTests.V1.Rating { A = 0; B = 1 }",
            ],
            StateShapes.Members(typeof(V1.Booked)));
        // A member added in the next version is there to be seen.
        Assert.Contains("Size: String", StateShapes.Members(typeof(V2.SinkRow)).Single());
        Assert.DoesNotContain("Size", StateShapes.Members(typeof(V1.SinkRow)).Single());
        // The runtime's own types have nothing to spell out.
        Assert.Empty(StateShapes.Members(typeof(string)));
        Assert.Empty(StateShapes.Members(typeof((int, string))));
    }

    [Fact]
    public void A_peer_says_what_its_process_holds_in_its_probe_answer()
    {
        var memory = new ProcessMemory(405_900_000, 168_800_000, 281_500_000, 0.13, 4);
        var self = new PeerInfo("b", NodeRole.Standby, 3, false, "state behind; 12 records", "http://127.0.0.1:5181/admin/", memory);
        var detail = PeerDetail.Write(self);
        Assert.DoesNotContain(' ', detail); // one token of the probe line
        Assert.Equal(self, PeerDetail.Read("b", NodeRole.Standby, 3, detail));
        // A node that predates the figures, or sends something else, is still read.
        Assert.Null(PeerDetail.Read("b", NodeRole.Leader, 3, "ready;admin=http%3A%2F%2Fx").Memory);
        Assert.Null(PeerDetail.Read("b", NodeRole.Leader, 3, "ready;mem=1,2,three").Memory);
        Assert.Null(PeerDetail.Read("b", NodeRole.Leader, 3, "ready;mem=1,2,3").Memory);
        Assert.True(PeerDetail.Read("b", NodeRole.Leader, 3, "ready;mem=1,2,3,4.5,6;later=x").Ready);
        Assert.True(ProcessMemory.Read().WorkingSetBytes > 0);
    }

    [Fact]
    public void Same_state_under_another_version_is_unchanged_whatever_else_the_version_changed()
    {
        // SinkRow gained a member between the versions, and no store holds it.
        var old = Shape(Weight<V1.Seen>("named.seen.distinct"), Value<string, long>("named.count.rows"));
        var next = Shape(Weight<V1.Seen>("named.seen.distinct"), Value<string, long>("named.count.rows"));
        Assert.Empty(StateShapes.Differences(old, next));
        var line = StateShapes.Unchanged(next);
        Assert.StartsWith("state shapes: unchanged — the 2 state store(s) and the 1 table(s) it reads or writes are the same", line);
        Assert.Contains("StateShapeTests.Seen", line);
        Assert.DoesNotContain("SinkRow", line);
    }

    [Fact]
    public void Unchanged_line_of_a_definition_with_no_stores_does_not_call_a_table_s_row_a_state_store_s()
    {
        // A stateless lookup over a table: no store, and the only types are the table's.
        var lookup = StateShapes.Describe([], new Dictionary<string, (Type, Type)> { ["watchlist"] = (typeof(string), typeof(V1.Booked)) });
        var line = StateShapes.Unchanged(lookup);
        Assert.DoesNotContain("0 state store", line);
        Assert.StartsWith("state shapes: unchanged — the definition keeps no state stores, before or after; the 1 table(s) it reads or writes hold the same types", line);
        Assert.Contains("Booked", line);

        var nothing = StateShapes.Unchanged(StateShapes.Describe([], new Dictionary<string, (Type, Type)>()));
        Assert.Equal("state shapes: unchanged — the definition keeps no state stores and uses no tables, before or after; what the definition computes from them is not compared", nothing);
    }

    [Fact]
    public void Differences_name_the_store_the_type_the_member_and_what_it_does_to_stored_entries()
    {
        var old = Shape(
            Weight<V1.Seen>("named.seen.distinct"),
            Value<long, V1.Booked>("named.booked.rows"),
            Value<string, long>("n7.gone"),
            Value<string, long>("n8.retyped"));
        var next = Shape(
            Weight<V2.Seen>("named.seen.distinct"),
            Value<long, V2.Booked>("named.booked.rows"),
            Value<string, long>("n9.new"),
            Value<int, long>("n8.retyped"));

        var lines = StateShapes.Differences(old, next);

        Assert.Contains(lines, l => l.StartsWith("store 'n9.new' is new (String → Int64): it starts empty"));
        Assert.Contains(lines, l => l.StartsWith("store 'n7.gone' (String → Int64) has no place in the new circuit"));
        Assert.Contains(lines, l => l.StartsWith("store 'n8.retyped' held String → Int64 and would hold Int32 → Int64"));

        // A weight store's value identifies the entry: a new member changes every key.
        var seen = Assert.Single(lines, l => l.StartsWith("type Incremental.Service.Tests.StateShapeTests.Seen "));
        Assert.Contains("adds Venue: String", seen);
        Assert.Contains("it identifies entries of 'named.seen.distinct'", seen);
        Assert.Contains("not safe", seen);

        // A value store's value does not: old entries read with the new member defaulted.
        var booked = Assert.Single(lines, l => l.StartsWith("type Incremental.Service.Tests.StateShapeTests.Booked "));
        Assert.Contains("adds Notional: Decimal", booked);
        Assert.Contains("it is a value in 'named.booked.rows'", booked);
        Assert.Contains("reads as its default", booked);

        // An enum is stored as its number: a renumbered member is a changed one.
        var rating = Assert.Single(lines, l => l.StartsWith("type Incremental.Service.Tests.StateShapeTests.Rating "));
        Assert.Contains("adds C = 3", rating);
        Assert.Contains("changes B = 1 to 2", rating);
        Assert.Equal(6, lines.Count);
    }
}
