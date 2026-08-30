namespace Incremental.Tests;

/// <summary>
/// The stable hash is what exchange routing and persistent tournament-tree slots are
/// derived from, so its values must never change between processes, machines, or library
/// versions — the fixed-vector tests pin the algorithm itself.
/// </summary>
public class StableHashTests
{
    private enum Color { Red, Green }

    private record Person(string Name, int Age);

    private record WithOptional(int? X, string? Note);

    private record WithDictionary(Dictionary<string, int> Lookup);

    private sealed class NoPublicMembers
    {
        private readonly int _value;
        public NoPublicMembers(int value) => _value = value;
        public override bool Equals(object? obj) => obj is NoPublicMembers o && o._value == _value;
        public override int GetHashCode() => _value;
        internal int Value => _value;
    }

    [Fact]
    public void String_hash_matches_pinned_vectors()
    {
        // Computed once from the FNV-1a + splitmix64 definition; a change here means the
        // algorithm changed, which would orphan persisted tournament-tree state and break
        // rolling restarts of sharded circuits.
        Assert.Equal(0x3D75AEAF693FFBE2ul, StableHash.Of("emea"));
        Assert.Equal(0xF52A15E9A9B5E89Bul, StableHash.Of(""));
    }

    [Fact]
    public void Equal_values_hash_equally_and_unequal_values_differ()
    {
        Assert.Equal(StableHash.Of(5), StableHash.Of(5));
        Assert.NotEqual(StableHash.Of(5), StableHash.Of(6));
        Assert.Equal(StableHash.Of(Color.Green), StableHash.Of(Color.Green));
        Assert.NotEqual(StableHash.Of(Color.Red), StableHash.Of(Color.Green));
        Assert.Equal(StableHash.Of(Guid.Empty), StableHash.Of(Guid.Empty));
    }

    [Fact]
    public void Records_and_tuples_hash_structurally()
    {
        Assert.Equal(StableHash.Of(new Person("ada", 36)), StableHash.Of(new Person("ada", 36)));
        Assert.NotEqual(StableHash.Of(new Person("ada", 36)), StableHash.Of(new Person("ada", 37)));
        Assert.NotEqual(StableHash.Of(new Person("ada", 36)), StableHash.Of(new Person("bob", 36)));

        Assert.Equal(StableHash.Of(("a", 1)), StableHash.Of(("a", 1)));
        Assert.NotEqual(StableHash.Of(("a", 1)), StableHash.Of(("a", 2)));
    }

    [Fact]
    public void Nullable_members_distinguish_null_from_default()
    {
        Assert.Equal(StableHash.Of(new WithOptional(1, null)), StableHash.Of(new WithOptional(1, null)));
        Assert.NotEqual(StableHash.Of(new WithOptional(null, "x")), StableHash.Of(new WithOptional(0, "x")));
        Assert.NotEqual(StableHash.Of(new WithOptional(1, null)), StableHash.Of(new WithOptional(1, "")));
    }

    [Fact]
    public void Sequences_hash_by_content_regardless_of_collection_type()
    {
        Assert.Equal(StableHash.Of(new[] { 1, 2, 3 }), StableHash.Of(new[] { 1, 2, 3 }));
        Assert.NotEqual(StableHash.Of(new[] { 1, 2, 3 }), StableHash.Of(new[] { 1, 2 }));
        Assert.NotEqual(StableHash.Of(new[] { 1, 2 }), StableHash.Of(new[] { 2, 1 }));
        Assert.Equal(StableHash.Of(new[] { "a", "b" }), StableHash.Of(new List<string> { "a", "b" }));
    }

    [Fact]
    public void Values_that_compare_equal_despite_different_representations_hash_equally()
    {
        Assert.Equal(StableHash.Of(1.0m), StableHash.Of(1.00m)); // decimal scale
        Assert.Equal(StableHash.Of(0.0), StableHash.Of(-0.0));   // signed zero
        var ticks = DateTime.UtcNow.Ticks;
        Assert.Equal( // DateTime equality ignores Kind
            StableHash.Of(new DateTime(ticks, DateTimeKind.Utc)),
            StableHash.Of(new DateTime(ticks, DateTimeKind.Local)));
    }

    [Fact]
    public void Unsupported_types_are_rejected_with_the_offending_member_named()
    {
        var e = Assert.Throws<NotSupportedException>(() => StableHash.Of(new WithDictionary([])));
        Assert.Contains("Lookup", e.Message);

        Assert.Throws<NotSupportedException>(() => StableHash.Of(new NoPublicMembers(1)));
    }

    [Fact]
    public void Sharded_IndexBy_rejects_unhashable_key_types_at_definition_time()
    {
        var builder = new CircuitBuilder(new CircuitOptions { Workers = 2 });
        var e = Assert.Throws<InvalidOperationException>(() =>
            builder.Input<int>("numbers").IndexBy(v => new NoPublicMembers(v)));
        Assert.Contains("stableHash", e.Message);

        // A single-worker circuit never exchanges, so the same key type is accepted.
        var singleWorker = new CircuitBuilder(new CircuitOptions { Workers = 1 });
        singleWorker.Input<int>("numbers").IndexBy(v => new NoPublicMembers(v)).Count().Output("counts");
    }

    [Fact]
    public async Task Custom_stable_hash_overload_routes_unhashable_keys()
    {
        var builder = new CircuitBuilder(new CircuitOptions { Workers = 4 });
        builder.Input<int>("numbers")
            .IndexBy(v => new NoPublicMembers(v % 3), k => (uint)k.Value)
            .Count()
            .Output("counts");

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<int>("numbers");
        var output = circuit.Output<KeyedValue<NoPublicMembers, long>>("counts");

        for (var i = 0; i < 300; i++)
            await input.InsertAsync(i);
        input.Complete();

        var integrated = new ZSet<KeyedValue<NoPublicMembers, long>>();
        await foreach (var delta in output.ReadAllAsync())
            integrated.AddAll(delta);

        Assert.Equal(3, integrated.DistinctCount);
        for (var k = 0; k < 3; k++)
            Assert.Equal(1, integrated.WeightOf(new KeyedValue<NoPublicMembers, long>(new NoPublicMembers(k), 100)));
    }
}
