using System.Diagnostics.Metrics;
using Incremental.Operators;
using Incremental.Storage;
using Incremental.Tables;

namespace Incremental.Tests;

/// <summary>
/// The per-record error policy: under <see cref="RecordErrorPolicy.DeadLetter"/> a record a
/// user delegate throws on (or that yields a null key or result) is taken out of the step
/// and reported on <see cref="Circuit.DeadLetters"/>, and the step carries on. The tests
/// that matter most are the ones that retract a dropped record afterwards: a drop is only
/// correct if the retraction is dropped too, and the output still integrates to the model.
/// </summary>
public class RecordErrorPolicyTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Settle = TimeSpan.FromMilliseconds(100);

    private static CircuitBuilder Builder(
        RecordErrorPolicy policy = RecordErrorPolicy.DeadLetter, int workers = 1, IStorageProvider? storage = null,
        string? name = null, int deadLetterCapacity = 1024) =>
        new(new CircuitOptions
        {
            Name = name,
            AutoStep = false,
            Workers = workers,
            ErrorPolicy = policy,
            DeadLetterCapacity = deadLetterCapacity,
            Storage = storage ?? new InMemoryStorageProvider(),
        });

    private sealed class Bad(string message) : Exception(message);

    /// <summary>Adds everything an output has published so far into <paramref name="state"/>.</summary>
    private static void Integrate<T>(OutputHandle<T> output, ZSet<T> state) where T : notnull
    {
        while (output.Reader.TryRead(out var delta))
            state.AddAll(delta);
    }

    private static List<DeadLetter> Letters(Circuit circuit)
    {
        var letters = new List<DeadLetter>();
        while (circuit.DeadLetters.TryRead(out var letter))
            letters.Add(letter);
        return letters;
    }

    private static void AssertState<T>(ZSet<T> actual, params (T Value, long Weight)[] expected) where T : notnull
    {
        var want = new ZSet<T>();
        foreach (var (value, weight) in expected)
            want.Add(value, weight);
        Assert.True(
            want.DistinctCount == actual.DistinctCount && want.All(e => actual.WeightOf(e.Value) == e.Weight),
            $"expected {want} but got {actual}");
    }

    // ---- the default is unchanged ----

    [Fact]
    public async Task Default_policy_still_faults_and_has_no_dead_letter_output()
    {
        var builder = Builder(RecordErrorPolicy.Fault);
        var numbers = builder.Input<int>("numbers");
        var doubled = numbers.Select(n => n == 13 ? throw new Bad("thirteen") : n * 2).ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        Assert.Throws<InvalidOperationException>(() => circuit.DeadLetters);

        await circuit.Handle(numbers).InsertAsync(13);
        await Assert.ThrowsAsync<Bad>(() => circuit.StepAsync()).WaitAsync(Timeout);
        Assert.IsType<Bad>(circuit.Fault);
        _ = doubled;
    }

    // ---- stateless operators ----

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task Select_drops_the_record_and_its_retraction_and_reports_both(int workers)
    {
        var builder = Builder(workers: workers);
        var numbers = builder.Input<int>("numbers");
        var doubledDef = numbers.Select(n => n == 13 ? throw new Bad("thirteen") : n * 2).Named("double").ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);
        var state = new ZSet<int>();

        await input.InsertAsync(1);
        await input.InsertAsync(13);
        await input.InsertAsync(2);
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(doubledDef), state);
        AssertState(state, (2, 1), (4, 1));

        var letter = Assert.Single(Letters(circuit));
        Assert.Equal(13, letter.Record);
        Assert.Equal(1, letter.Weight);
        Assert.Equal("named.double.select", letter.Operator);
        Assert.IsType<Bad>(letter.Exception);
        Assert.Equal(1, letter.Step);
        Assert.Null(circuit.Fault);

        // The retraction of the dropped record is dropped too, and reported with its weight:
        // the dead letters of an operator are themselves a stream of changes.
        await input.RemoveAsync(13);
        await input.RemoveAsync(1);
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(doubledDef), state);
        AssertState(state, (4, 1));

        letter = Assert.Single(Letters(circuit));
        Assert.Equal(13, letter.Record);
        Assert.Equal(-1, letter.Weight);
        Assert.Equal(2, letter.Step);
    }

    [Fact]
    public async Task Select_returning_null_is_a_dead_letter()
    {
        var builder = Builder();
        var words = builder.Input<string>("words");
        var upperDef = words.Select(w => w == "null" ? null! : w.ToUpperInvariant()).ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var state = new ZSet<string>();

        await circuit.Handle(words).InsertAsync("a");
        await circuit.Handle(words).InsertAsync("null");
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(upperDef), state);

        AssertState(state, ("A", 1));
        var letter = Assert.Single(Letters(circuit));
        Assert.Equal("null", letter.Record);
        Assert.Contains("returned null", letter.Exception.Message);
    }

    [Fact]
    public async Task Where_and_MapValues_drop_only_the_record_that_throws()
    {
        var builder = Builder();
        var numbers = builder.Input<int>("numbers");
        var countsDef = numbers
            .Where(n => n == 7 ? throw new Bad("seven") : n > 0)
            .GroupBy(n => n % 2)
            .Count()
            .MapValues(count => count == 3 ? throw new Bad("three") : count * 10)
            .ToOutput(bootstrap: false);
        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);
        var state = new ZSet<KeyedValue<int, long>>();

        foreach (var n in new[] { 1, 3, 7, -5, 2 })
            await input.InsertAsync(n);
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(countsDef), state);
        AssertState(state, (new KeyedValue<int, long>(1, 20), 1), (new KeyedValue<int, long>(0, 10), 1));
        var letter = Assert.Single(Letters(circuit));
        Assert.Equal(7, letter.Record);
        Assert.EndsWith(".where", letter.Operator);

        // The odd count reaches 3: MapValues throws on the new row, and passes the
        // retraction of the old one (20), so the key has no row while its count is 3.
        await input.InsertAsync(5);
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(countsDef), state);
        AssertState(state, (new KeyedValue<int, long>(0, 10), 1));
        letter = Assert.Single(Letters(circuit));
        Assert.Equal(new KeyedValue<int, long>(1, 3), letter.Record);
        Assert.Equal(1, letter.Weight);
        Assert.EndsWith(".mapValues", letter.Operator);

        // …and comes back when the count moves on: the retraction of (1, 3) is dropped as
        // its insertion was.
        await input.InsertAsync(9);
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(countsDef), state);
        AssertState(state, (new KeyedValue<int, long>(0, 10), 1), (new KeyedValue<int, long>(1, 40), 1));
        letter = Assert.Single(Letters(circuit));
        Assert.Equal(new KeyedValue<int, long>(1, 3), letter.Record);
        Assert.Equal(-1, letter.Weight);
    }

    [Fact]
    public async Task SelectMany_that_throws_part_way_emits_none_of_the_records_rows()
    {
        static IEnumerable<int> Expand(int n)
        {
            yield return n;
            yield return n + 1;
            if (n == 10)
                throw new Bad("ten");
            yield return n + 2;
        }

        var builder = Builder();
        var numbers = builder.Input<int>("numbers");
        var expandedDef = numbers.SelectMany(Expand).ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var state = new ZSet<int>();

        await circuit.Handle(numbers).InsertAsync(10);
        await circuit.Handle(numbers).InsertAsync(20);
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(expandedDef), state);

        AssertState(state, (20, 1), (21, 1), (22, 1));
        Assert.Equal(10, Assert.Single(Letters(circuit)).Record);
    }

    // ---- keys ----

    private sealed record Order(int Id, string? Region, long Amount);

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task GroupBy_drops_a_null_key_and_a_throwing_key_selector(int workers)
    {
        var builder = Builder(workers: workers);
        var orders = builder.Input<Order>("orders");
        var countsDef = orders
            .GroupBy(o => o.Region == "boom" ? throw new Bad("boom") : o.Region!)
            .Count()
            .ToOutput(bootstrap: false);
        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(orders);
        var state = new ZSet<KeyedValue<string, long>>();

        await input.InsertAsync(new Order(1, "emea", 1));
        await input.InsertAsync(new Order(2, null, 1));
        await input.InsertAsync(new Order(3, "boom", 1));
        await input.InsertAsync(new Order(4, "emea", 1));
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(countsDef), state);
        AssertState(state, (new KeyedValue<string, long>("emea", 2), 1));

        var letters = Letters(circuit).OrderBy(l => ((Order)l.Record!).Id).ToArray();
        Assert.Equal(2, letters.Length);
        Assert.Equal(new Order(2, null, 1), letters[0].Record);
        Assert.Contains("key selector returned null", letters[0].Exception.Message);
        Assert.Equal(new Order(3, "boom", 1), letters[1].Record);
        Assert.IsType<Bad>(letters[1].Exception);
        Assert.All(letters, l => Assert.EndsWith(".groupBy", l.Operator));

        await input.RemoveAsync(new Order(2, null, 1));
        await input.RemoveAsync(new Order(4, "emea", 1));
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(countsDef), state);
        AssertState(state, (new KeyedValue<string, long>("emea", 1), 1));
        Assert.Equal(-1, Assert.Single(Letters(circuit)).Weight);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Keyed_input_reports_an_upsert_it_cannot_key_and_consumes_its_offset(bool nullKey)
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var builder = Builder(storage: storage);
        var orders = builder.KeyedInput<string, Order>(
            "orders", o => nullKey ? o.Region! : o.Region ?? throw new Bad("no region"));
        var rowsDef = orders.ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(orders);
        var state = new ZSet<Order>();

        await input.UpsertAsync(new Order(1, "emea", 5), offset: 0);
        await input.UpsertAsync(new Order(2, null, 7), offset: 1); // no exception reaches the producer
        await input.UpsertAsync(new Order(3, "apac", 9), offset: 2);
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(rowsDef), state);

        AssertState(state, (new Order(1, "emea", 5), 1), (new Order(3, "apac", 9), 1));
        var letter = Assert.Single(Letters(circuit));
        Assert.Equal(new Order(2, null, 7), letter.Record);
        Assert.Equal(1, letter.Weight);
        Assert.Equal("input[orders]", letter.Operator);
        Assert.True(nullKey ? letter.Exception is InvalidOperationException : letter.Exception is Bad);
        Assert.Equal(2L, circuit.CommittedOffset("orders"));

        // The dropped record's offset is in the checkpoint like any other: when it is the
        // last one consumed, a replay from the committed offset does not bring it back.
        await input.UpsertAsync(new Order(4, null, 1), offset: 3);
        await circuit.StepAsync().WaitAsync(Timeout);
        Assert.Single(Letters(circuit));
        Assert.Equal(3L, circuit.CommittedOffset("orders"));
    }

    [Fact]
    public async Task Keyed_input_under_the_default_policy_still_throws_to_the_producer()
    {
        var builder = Builder(RecordErrorPolicy.Fault);
        var orders = builder.KeyedInput<string, Order>("orders", o => o.Region ?? throw new Bad("no region"));
        _ = orders.ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();

        await Assert.ThrowsAsync<Bad>(async () => await circuit.Handle(orders).UpsertAsync(new Order(2, null, 7)));
        Assert.Null(circuit.Fault);
    }

    // ---- per-operator override ----

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task OnError_overrides_the_circuit_policy_for_one_operator(int workers)
    {
        // Circuit: Fault. The grouping and the first Select dead-letter; the second does not.
        var builder = Builder(RecordErrorPolicy.Fault, workers);
        var numbers = builder.Input<int>("numbers");
        var countsDef = numbers
            .Select(n => n == 13 ? throw new Bad("thirteen") : n).OnError(RecordErrorPolicy.DeadLetter)
            .Select(n => n == 99 ? throw new Bad("ninety-nine") : n)
            .GroupBy(n => n == 5 ? throw new Bad("five") : n % 2).OnError(RecordErrorPolicy.DeadLetter)
            .Count()
            .ToOutput(bootstrap: false);
        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);
        var state = new ZSet<KeyedValue<int, long>>();

        foreach (var n in new[] { 1, 13, 5, 2 })
            await input.InsertAsync(n);
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(countsDef), state);
        AssertState(state, (new KeyedValue<int, long>(1, 1), 1), (new KeyedValue<int, long>(0, 1), 1));
        Assert.Equal([5, 13], Letters(circuit).Select(l => (int)l.Record!).Order());

        await input.InsertAsync(99);
        await Assert.ThrowsAnyAsync<Exception>(() => circuit.StepAsync()).WaitAsync(Timeout);
        Assert.NotNull(circuit.Fault);
    }

    [Fact]
    public async Task OnError_Fault_exempts_an_operator_from_a_dead_lettering_circuit()
    {
        var builder = Builder();
        var numbers = builder.Input<int>("numbers");
        _ = numbers.Select(n => n == 13 ? throw new Bad("thirteen") : n).OnError(RecordErrorPolicy.Fault).ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();

        await circuit.Handle(numbers).InsertAsync(13);
        await Assert.ThrowsAsync<Bad>(() => circuit.StepAsync()).WaitAsync(Timeout);
    }

    // ---- aggregates ----

    /// <summary>The largest value of a group, refusing a group that holds a negative one.</summary>
    private sealed class MaxOfNonNegative : IAggregateFunction<(string Key, int Value), int>
    {
        public int Aggregate(IReadOnlyList<WeightedValue<(string Key, int Value)>> group) =>
            group.Any(row => row.Value.Value < 0) ? throw new Bad("negative in group") : group.Max(row => row.Value.Value);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task Aggregate_that_throws_leaves_the_key_without_a_result_until_the_group_mends(int workers)
    {
        var builder = Builder(workers: workers);
        var numbers = builder.Input<(string Key, int Value)>("numbers");
        var maxDef = numbers.GroupBy(n => n.Key).Aggregate(new MaxOfNonNegative())
            .Named("max").ToOutput(bootstrap: false);
        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);
        var state = new ZSet<KeyedValue<string, int>>();

        async Task Step(params (string Key, int Value, long Weight)[] changes)
        {
            foreach (var (key, value, weight) in changes)
                await input.SendAsync((key, value), weight);
            await circuit.StepAsync().WaitAsync(Timeout);
            Integrate(circuit.Handle(maxDef), state);
        }

        await Step(("a", 3, 1), ("b", 8, 1));
        AssertState(state, (new KeyedValue<string, int>("a", 3), 1), (new KeyedValue<string, int>("b", 8), 1));
        Assert.Empty(Letters(circuit));

        // The poison row arrives with a good one: the group has no result now, so the one
        // it had is retracted. Both rows of the step are reported.
        await Step(("a", -1, 1), ("a", 5, 1));
        AssertState(state, (new KeyedValue<string, int>("b", 8), 1));
        var letters = Letters(circuit);
        Assert.Equal([-1, 5], letters.Select(l => ((KeyedValue<string, (string Key, int Value)>)l.Record!).Value.Value).Order());
        Assert.All(letters, l => Assert.Equal("named.max.aggregate", l.Operator));
        Assert.All(letters, l => Assert.Equal(1, l.Weight));

        // Still broken: another row changes nothing downstream, and is reported.
        await Step(("a", 9, 1));
        AssertState(state, (new KeyedValue<string, int>("b", 8), 1));
        Assert.Single(Letters(circuit));

        // Mended: the group's contents were integrated all along, so the result is over
        // everything it holds, not just what arrived since.
        await Step(("a", -1, -1));
        AssertState(state, (new KeyedValue<string, int>("a", 9), 1), (new KeyedValue<string, int>("b", 8), 1));
        Assert.Empty(Letters(circuit));

        // A group that is broken from its first row and then vanishes never emits.
        await Step(("c", -7, 1));
        await Step(("c", -7, -1));
        AssertState(state, (new KeyedValue<string, int>("a", 9), 1), (new KeyedValue<string, int>("b", 8), 1));
        Assert.Single(Letters(circuit));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task Sum_and_Max_leave_out_a_row_their_selector_throws_for_and_its_retraction(int workers)
    {
        var builder = Builder(workers: workers);
        var orders = builder.Input<Order>("orders");
        var grouped = orders.GroupBy(o => o.Region!);
        var sumDef = grouped.Sum(o => o.Amount < 0 ? throw new Bad("negative") : o.Amount).ToOutput(bootstrap: false);
        var maxDef = grouped.Max(o => o.Amount < 0 ? throw new Bad("negative") : o.Amount).ToOutput(bootstrap: false);
        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(orders);
        var sums = new ZSet<KeyedValue<string, long>>();
        var maxes = new ZSet<KeyedValue<string, long>>();

        async Task Step(params (Order Order, long Weight)[] changes)
        {
            foreach (var (order, weight) in changes)
                await input.SendAsync(order, weight);
            await circuit.StepAsync().WaitAsync(Timeout);
            Integrate(circuit.Handle(sumDef), sums);
            Integrate(circuit.Handle(maxDef), maxes);
        }

        await Step((new Order(1, "emea", 10), 1), (new Order(2, "emea", -4), 1), (new Order(3, "apac", -1), 1));
        AssertState(sums, (new KeyedValue<string, long>("emea", 10), 1));
        AssertState(maxes, (new KeyedValue<string, long>("emea", 10), 1));
        var letters = Letters(circuit);
        Assert.Equal(4, letters.Count); // two rows, each dropped by both aggregates
        Assert.All(letters, l => Assert.True(((KeyedValue<string, Order>)l.Record!).Value.Amount < 0));

        // Retracting the dropped rows changes nothing: no negative count, no phantom group.
        await Step((new Order(2, "emea", -4), -1), (new Order(3, "apac", -1), -1), (new Order(4, "emea", 2), 1));
        AssertState(sums, (new KeyedValue<string, long>("emea", 12), 1));
        AssertState(maxes, (new KeyedValue<string, long>("emea", 10), 1));
        Assert.All(Letters(circuit), l => Assert.Equal(-1, l.Weight));

        await Step((new Order(1, "emea", 10), -1), (new Order(4, "emea", 2), -1));
        AssertState(sums);
        AssertState(maxes);
    }

    // ---- joins ----

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task Join_leaves_out_the_pair_its_selector_throws_for_from_either_side(int workers)
    {
        var builder = Builder(workers: workers);
        var left = builder.Input<(int Key, string Name)>("left");
        var right = builder.Input<(int Key, string Tag)>("right");
        var joinedDef = left.GroupBy(l => l.Key)
            .Join(right.GroupBy(r => r.Key),
                (_, l, r) => l.Name == "bad" && r.Tag == "x" ? throw new Bad("bad-x") : $"{l.Name}-{r.Tag}")
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var state = new ZSet<string>();

        async Task Step(Func<Task> feed)
        {
            await feed();
            await circuit.StepAsync().WaitAsync(Timeout);
            Integrate(circuit.Handle(joinedDef), state);
        }

        await Step(async () =>
        {
            await circuit.Handle(left).InsertAsync((1, "bad"));
            await circuit.Handle(left).InsertAsync((1, "good"));
            await circuit.Handle(right).InsertAsync((1, "x"));
        });
        AssertState(state, ("good-x", 1));
        Assert.Single(Letters(circuit));

        // The same left row pairs with another right row without trouble.
        await Step(async () => await circuit.Handle(right).InsertAsync((1, "y")));
        AssertState(state, ("good-x", 1), ("good-y", 1), ("bad-y", 1));
        Assert.Empty(Letters(circuit));

        // Retracting the right row takes away good-x only; the bad pair is dropped again.
        await Step(async () => await circuit.Handle(right).RemoveAsync((1, "x")));
        AssertState(state, ("good-y", 1), ("bad-y", 1));
        var letter = Assert.Single(Letters(circuit));
        Assert.Equal(new KeyedValue<int, (int Key, string Tag)>(1, (1, "x")), letter.Record);
        Assert.Equal(-1, letter.Weight);
        Assert.EndsWith(".join", letter.Operator);

        // The other direction: the right row is back (reported from the right), and then
        // the left row goes while the right side holds both rows.
        await Step(async () => await circuit.Handle(right).InsertAsync((1, "x")));
        AssertState(state, ("good-x", 1), ("good-y", 1), ("bad-y", 1));
        Assert.Equal(1, Assert.Single(Letters(circuit)).Weight);

        await Step(async () => await circuit.Handle(left).RemoveAsync((1, "bad")));
        AssertState(state, ("good-x", 1), ("good-y", 1));
        letter = Assert.Single(Letters(circuit));
        Assert.Equal(new KeyedValue<int, (int Key, string Name)>(1, (1, "bad")), letter.Record);
        Assert.Equal(-1, letter.Weight);
    }

    private sealed record Instrument(int Id, string Symbol);

    private sealed record Trade(int Id, int InstrumentId);

    private static async Task Commit(SharedTable<int, Instrument> table, params Instrument[] rows)
    {
        foreach (var row in rows)
            await table.Writer.UpsertAsync(row);
        await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task Table_join_leaves_out_a_pair_while_the_row_makes_the_selector_throw(int workers)
    {
        var table = new SharedTable<int, Instrument>("instruments", i => i.Id);
        await Commit(table, new Instrument(1, "AAA"), new Instrument(2, "?"));
        var builder = Builder(workers: workers);
        var instruments = builder.Table(table);
        var trades = builder.Input<Trade>("trades");
        var enrichedDef = trades.GroupBy(t => t.InstrumentId)
            .LeftJoin(instruments,
                (_, trade, instrument) => instrument.Symbol == "?" ? throw new Bad("no symbol") : (trade.Id, instrument.Symbol),
                (_, trade) => (trade.Id, "(none)"))
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var state = new ZSet<(int, string)>();

        async Task Step()
        {
            await circuit.StepAsync().WaitAsync(Timeout);
            Integrate(circuit.Handle(enrichedDef), state);
        }

        await circuit.Handle(trades).InsertAsync(new Trade(10, 1));
        await circuit.Handle(trades).InsertAsync(new Trade(20, 2));
        await Step();
        AssertState(state, ((10, "AAA"), 1));
        Assert.Equal(new KeyedValue<int, Trade>(2, new Trade(20, 2)), Assert.Single(Letters(circuit)).Record);

        // The row is corrected: the join retracts the old pair (dropped again, as it was
        // never emitted) and asserts the new one.
        await Commit(table, new Instrument(2, "BBB"));
        await Step();
        AssertState(state, ((10, "AAA"), 1), ((20, "BBB"), 1));
        Assert.Equal(-1, Assert.Single(Letters(circuit)).Weight);

        // And broken again: the good pair is retracted, the bad one dropped.
        await Commit(table, new Instrument(2, "?"));
        await Step();
        AssertState(state, ((10, "AAA"), 1));
        Assert.Equal(1, Assert.Single(Letters(circuit)).Weight);
    }

    private sealed record SymbolTrade(int Id, string Symbol);

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task Index_join_leaves_out_the_pairs_its_selectors_throw_for(int workers)
    {
        var table = new SharedTable<int, Instrument>("instruments", i => i.Id);
        await Commit(table, new Instrument(1, "AAA"), new Instrument(2, "AAA"));
        var builder = Builder(workers: workers);
        var bySymbol = builder.Table(table).By("bySymbol", i => i.Symbol);
        var trades = builder.Input<SymbolTrade>("trades");
        var enrichedDef = trades.GroupBy(t => t.Symbol)
            .LeftJoin(bySymbol,
                (_, trade, instrument) => instrument.Id == 2 ? throw new Bad("two") : (trade.Id, instrument.Id),
                (_, trade) => trade.Id == 99 ? throw new Bad("ninety-nine") : (trade.Id, 0))
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var state = new ZSet<(int, int)>();

        async Task<List<DeadLetter>> Step()
        {
            await circuit.StepAsync().WaitAsync(Timeout);
            Integrate(circuit.Handle(enrichedDef), state);
            return Letters(circuit);
        }

        // New facts against the table: one of AAA's two rows throws; ZZZ has no row and its
        // unmatched selector throws for trade 99.
        await circuit.Handle(trades).InsertAsync(new SymbolTrade(10, "AAA"));
        await circuit.Handle(trades).InsertAsync(new SymbolTrade(99, "ZZZ"));
        await circuit.Handle(trades).InsertAsync(new SymbolTrade(98, "ZZZ"));
        var letters = await Step();
        AssertState(state, ((10, 1), 1), ((98, 0), 1));
        Assert.Equal([10, 99], letters.Select(l => ((KeyedValue<string, SymbolTrade>)l.Record!).Value.Id).Order());
        Assert.All(letters, l => Assert.Equal(1, l.Weight));

        // Table changes against held facts: the throwing row leaves AAA (its pair is
        // dropped again), and ZZZ gains a row (the unmatched results go, 99's dropped).
        await Commit(table, new Instrument(2, "CCC"), new Instrument(5, "ZZZ"));
        letters = await Step();
        AssertState(state, ((10, 1), 1), ((98, 5), 1), ((99, 5), 1));
        Assert.Equal([10, 99], letters.Select(l => ((KeyedValue<string, SymbolTrade>)l.Record!).Value.Id).Order());
        Assert.All(letters, l => Assert.Equal(-1, l.Weight));

        // And back in, then the fact itself is retracted.
        await Commit(table, new Instrument(2, "AAA"));
        Assert.Equal(1, Assert.Single(await Step()).Weight);
        await circuit.Handle(trades).RemoveAsync(new SymbolTrade(10, "AAA"));
        Assert.Equal(-1, Assert.Single(await Step()).Weight);
        AssertState(state, ((98, 5), 1), ((99, 5), 1));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task Remembered_lookup_remembers_that_it_dropped_a_fact(int workers)
    {
        var table = new SharedTable<int, Instrument>("instruments", i => i.Id);
        await Commit(table, new Instrument(1, "AAA"));
        var builder = Builder(workers: workers);
        var instruments = builder.Table(table);
        var trades = builder.Input<Trade>("trades");

        // Throws the first time it sees trade 20 and never again: not deterministic, which
        // a stateless operator could not survive. The lookup remembers what it emitted.
        var thrown = 0;
        var enrichedDef = trades.Lookup(
                instruments,
                t => t.InstrumentId,
                (trade, instrument) => trade.Id == 20 && Interlocked.Exchange(ref thrown, 1) == 0
                    ? throw new Bad("first time")
                    : (trade.Id, instrument.Symbol))
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var state = new ZSet<(int, string)>();

        async Task Step()
        {
            await circuit.StepAsync().WaitAsync(Timeout);
            Integrate(circuit.Handle(enrichedDef), state);
        }

        await circuit.Handle(trades).InsertAsync(new Trade(10, 1));
        await circuit.Handle(trades).InsertAsync(new Trade(20, 1));
        await Step();
        AssertState(state, ((10, "AAA"), 1));
        var letter = Assert.Single(Letters(circuit));
        Assert.Equal(new KeyedValue<int, Trade>(1, new Trade(20, 1)), letter.Record);
        Assert.EndsWith(".tableLookup", letter.Operator);

        // The retraction finds the fact held as "emitted nothing": no delegate runs, no
        // row is taken away that was never emitted, and nothing more is reported.
        await circuit.Handle(trades).RemoveAsync(new Trade(20, 1));
        await Step();
        AssertState(state, ((10, "AAA"), 1));
        Assert.Empty(Letters(circuit));

        // Asserted afresh, it is enriched afresh.
        await circuit.Handle(trades).InsertAsync(new Trade(20, 1));
        await Step();
        AssertState(state, ((10, "AAA"), 1), ((20, "AAA"), 1));
    }

    [Fact]
    public async Task Append_only_lookup_reports_a_retraction_a_null_key_and_a_throwing_delegate()
    {
        var table = new SharedTable<string, Instrument>("instruments", i => i.Symbol);
        foreach (var row in new[] { new Instrument(1, "AAA"), new Instrument(2, "BBB") })
            await table.Writer.UpsertAsync(row);
        await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);

        var builder = Builder();
        var instruments = builder.Table(table);
        var ticks = builder.Input<(int Id, string? Symbol)>("ticks");
        var enrichedDef = ticks.LookupAppendOnly(
                instruments,
                t => t.Symbol!,
                (tick, instrument) => instrument.Id == 2 ? throw new Bad("two") : (tick.Id, instrument.Id))
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(ticks);
        var state = new ZSet<(int, int)>();

        await input.InsertAsync((1, "AAA"));
        await input.InsertAsync((2, null));
        await input.InsertAsync((3, "BBB"));
        await input.RemoveAsync((4, "AAA"));
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(enrichedDef), state);

        AssertState(state, ((1, 1), 1));
        var letters = Letters(circuit).ToDictionary(l => (((int Id, string? Symbol))l.Record!).Id);
        Assert.Equal([2, 3, 4], letters.Keys.Order());
        Assert.Contains("key selector returned null", letters[2].Exception.Message);
        Assert.IsType<Bad>(letters[3].Exception);
        Assert.Contains("received a retraction", letters[4].Exception.Message);
        Assert.Equal(-1, letters[4].Weight);
        Assert.Null(circuit.Fault);
    }

    [Fact]
    public async Task Append_only_lookup_over_several_rows_drops_the_whole_record_when_one_row_throws()
    {
        var table = new SharedTable<int, Instrument>("instruments", i => i.Id);
        await Commit(table, new Instrument(1, "AAA"), new Instrument(2, "AAA"), new Instrument(3, "BBB"), new Instrument(4, "BBB"));
        var builder = Builder();
        var bySymbol = builder.Table(table).By("bySymbol", i => i.Symbol);
        var ticks = builder.Input<(int Id, string Symbol)>("ticks");
        var enrichedDef = ticks.LookupAppendOnly(
                bySymbol,
                t => t.Symbol,
                (tick, instrument) => instrument.Id == 2 ? throw new Bad("two") : (tick.Id, instrument.Id))
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var state = new ZSet<(int, int)>();

        await circuit.Handle(ticks).InsertAsync((10, "AAA")); // rows 1 and 2: one of them throws
        await circuit.Handle(ticks).InsertAsync((20, "BBB")); // rows 3 and 4
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(enrichedDef), state);

        AssertState(state, ((20, 3), 1), ((20, 4), 1));
        Assert.Equal((10, "AAA"), Assert.Single(Letters(circuit)).Record);
    }

    // ---- event time ----

    [Fact]
    public async Task Event_time_that_cannot_be_read_drops_the_record_and_does_not_move_the_watermark()
    {
        var t0 = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var builder = Builder();
        var events = builder.Input<(int Id, int Minute)>("events");
        var timedDef = events
            .WithEventTime(e => e.Minute < 0 ? throw new Bad("no time") : t0.AddMinutes(e.Minute), TimeSpan.Zero)
            .Select(e => e.Id)
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var state = new ZSet<int>();

        await circuit.Handle(events).InsertAsync((1, 5));
        await circuit.Handle(events).InsertAsync((2, -1));
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(timedDef), state);

        AssertState(state, (1, 1));
        Assert.Equal((2, -1), Assert.Single(Letters(circuit)).Record);
        Assert.Equal(t0.AddMinutes(5), circuit.CurrentWatermark);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task Windowed_groupings_drop_null_keys_at_both_levels(int workers)
    {
        var t0 = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var builder = Builder(workers: workers);
        var views = builder.Input<(string? Page, string? Site, int Second)>("views");
        var perSiteDef = views
            .WithEventTime(v => t0.AddSeconds(v.Second), TimeSpan.Zero)
            .GroupBy(v => v.Page!)                       // a grouping of an event-timed stream
            .TumblingWindow(TimeSpan.FromSeconds(10))
            .Count()
            .GroupBy(row => row.Key.StartsWith("x-") ? null! : row.Key[..1]) // regrouped within the window
            .Count()
            .ToOutput(bootstrap: false);
        await using var circuit = builder.Build();
        circuit.Start();
        var state = new ZSet<WindowedValue<string, long>>();

        foreach (var view in new (string?, string?, int)[] { ("a1", "s", 1), ("a2", "s", 2), (null, "s", 3), ("x-1", "s", 4), ("b1", "s", 5) })
            await circuit.Handle(views).InsertAsync(view);
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(perSiteDef), state);

        AssertState(state,
            (new WindowedValue<string, long>(t0, t0.AddSeconds(10), "a", 2), 1),
            (new WindowedValue<string, long>(t0, t0.AddSeconds(10), "b", 1), 1));
        var letters = Letters(circuit);
        Assert.Equal(2, letters.Count);
        Assert.Contains(letters, l => l.Record is ValueTuple<string?, string?, int> { Item1: null });
        Assert.Contains(letters, l => l.Record is KeyedValue<WindowKey<string>, long> { Key.Key: "x-1" });
        Assert.All(letters, l => Assert.Contains("key selector returned null", l.Exception.Message));
        Assert.Null(circuit.Fault);
    }

    // ---- differential ----

    [Theory]
    [InlineData(1, 1)]
    [InlineData(4, 2)]
    [InlineData(4, 3)]
    public async Task Random_inserts_and_retractions_with_poison_records_match_the_model(int workers, int seed)
    {
        // Poison is a property of the record, so every delegate is deterministic: values
        // divisible by 7 break the Select, by 11 the key selector, by 13 the Sum.
        var builder = Builder(workers: workers);
        var numbers = builder.Input<int>("numbers");
        var sumsDef = numbers
            .Select(n => n % 7 == 0 ? throw new Bad("select") : n)
            .GroupBy(n => n % 11 == 0 ? throw new Bad("key") : n % 5)
            .Sum(n => n % 13 == 0 ? throw new Bad("sum") : (long)n)
            .ToOutput(bootstrap: false);
        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);

        var random = new Random(seed);
        var present = new ZSet<int>();
        var state = new ZSet<KeyedValue<int, long>>();
        var dead = new ZSet<int>();
        for (var step = 0; step < 60; step++)
        {
            for (var i = random.Next(1, 12); i > 0; i--)
            {
                var value = random.Next(1, 120);
                var weight = present.WeightOf(value) > 0 && random.Next(3) == 0 ? -1 : 1;
                present.Add(value, weight);
                await input.SendAsync(value, weight);
            }
            await circuit.StepAsync().WaitAsync(Timeout);
            Integrate(circuit.Handle(sumsDef), state);
            foreach (var letter in Letters(circuit))
                dead.Add((int)(letter.Record is KeyedValue<int, int> keyed ? keyed.Value : letter.Record!), letter.Weight);

            static bool Poison(int n) => n % 7 == 0 || n % 11 == 0 || n % 13 == 0;
            var expected = present.Where(e => !Poison(e.Value))
                .GroupBy(e => e.Value % 5)
                .Select(g => (new KeyedValue<int, long>(g.Key, g.Sum(e => e.Value * e.Weight)), 1L))
                .ToArray();
            AssertState(state, expected);

            // The dead letters integrate to exactly the poison records present now.
            AssertState(dead, present.Where(e => Poison(e.Value)).Select(e => (e.Value, e.Weight)).ToArray());
        }
        Assert.Null(circuit.Fault);
    }

    // ---- the dead-letter output ----

    [Fact]
    public async Task Unread_dead_letters_stall_the_circuit_instead_of_piling_up()
    {
        var builder = Builder(deadLetterCapacity: 2);
        var numbers = builder.Input<int>("numbers");
        _ = numbers.Select(n => n < 0 ? throw new Bad("negative") : n).ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);

        for (var n = -1; n >= -5; n--)
            await input.InsertAsync(n);
        var step = circuit.StepAsync();
        await Task.Delay(Settle);
        Assert.False(step.IsCompleted, "The step finished with more dead letters than the output can hold.");
        Assert.Equal(2, circuit.DeadLetters.Count);

        var read = 0;
        while (read < 5)
        {
            await circuit.DeadLetters.ReadAsync().AsTask().WaitAsync(Timeout);
            read++;
        }
        await step.WaitAsync(Timeout);
        Assert.Null(circuit.Fault);
    }

    [Fact]
    public async Task Dead_letter_output_completes_with_the_circuit()
    {
        var builder = Builder();
        var numbers = builder.Input<int>("numbers");
        _ = numbers.Select(n => n < 0 ? throw new Bad("negative") : n).ToOutput();
        var circuit = builder.Build();
        circuit.Start();
        await circuit.Handle(numbers).InsertAsync(-1);
        await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);

        var letters = new List<DeadLetter>();
        await foreach (var letter in circuit.DeadLetters.ReadAllAsync())
            letters.Add(letter);
        Assert.Equal(-1, Assert.Single(letters).Record);
    }

    [Fact]
    public async Task Auto_stepping_circuit_publishes_dead_letters_too()
    {
        var builder = new CircuitBuilder(new CircuitOptions { ErrorPolicy = RecordErrorPolicy.DeadLetter, Workers = 2 });
        var numbers = builder.Input<int>("numbers");
        var doubledDef = numbers.Select(n => n < 0 ? throw new Bad("negative") : n * 2).ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var doubled = circuit.Handle(doubledDef);

        for (var n = -3; n <= 3; n++)
            await circuit.Handle(numbers).InsertAsync(n);

        var letters = new List<int>();
        while (letters.Count < 3)
            letters.Add((int)(await circuit.DeadLetters.ReadAsync().AsTask().WaitAsync(Timeout)).Record!);
        Assert.Equal([-3, -2, -1], letters.Order());

        var state = new ZSet<int>();
        while (state.DistinctCount < 4)
            state.AddAll(await doubled.Reader.ReadAsync().AsTask().WaitAsync(Timeout));
        AssertState(state, (0, 1), (2, 1), (4, 1), (6, 1));
    }

    [Fact]
    public async Task Dead_letters_are_counted_by_operator()
    {
        const string name = "dead-letter-metric";
        var counts = new Dictionary<string, long>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "Incremental" && instrument.Name == "incremental.circuit.dead_letters")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            string? circuitTag = null, operatorTag = null;
            foreach (var tag in tags)
            {
                if (tag.Key == "circuit")
                    circuitTag = tag.Value as string;
                if (tag.Key == "operator")
                    operatorTag = tag.Value as string;
            }
            if (circuitTag == name)
                lock (counts)
                    counts[operatorTag!] = counts.GetValueOrDefault(operatorTag!) + value;
        });
        listener.Start();

        var builder = Builder(name: name);
        var numbers = builder.Input<int>("numbers");
        _ = numbers
            .Select(n => n < 0 ? throw new Bad("negative") : n).Named("positive")
            .Where(n => n == 0 ? throw new Bad("zero") : true).Named("nonzero")
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        foreach (var n in new[] { -2, -1, 0, 1 })
            await circuit.Handle(numbers).InsertAsync(n);
        await circuit.StepAsync().WaitAsync(Timeout);

        lock (counts)
            Assert.Equal(
                new Dictionary<string, long> { ["named.positive.select"] = 2, ["named.nonzero.where"] = 1 }, counts);
    }

    [Fact]
    public void Options_are_validated()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new CircuitBuilder(new CircuitOptions { DeadLetterCapacity = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new CircuitBuilder(new CircuitOptions { ErrorPolicy = (RecordErrorPolicy)7 }));
        var builder = new CircuitBuilder();
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.Input<int>("n").OnError((RecordErrorPolicy)7));
    }

    // ---- the healthy path ----

    [Fact]
    public void Guarded_loops_allocate_no_more_than_the_default_ones_for_records_that_do_not_throw()
    {
        var sink = new List<DeadLetter>();
        var errors = new RecordErrors("test", sink);
        var delta = new ZSet<int>();
        for (var i = 0; i < 512; i++)
            delta.Add(i);
        var keyed = new ZSet<KeyedValue<int, int>>();
        for (var i = 0; i < 512; i++)
            keyed.Add(new KeyedValue<int, int>(i % 16, i));
        var one = new ZSet<KeyedValue<int, int>>();
        for (var i = 0; i < 16; i++)
            one.Add(new KeyedValue<int, int>(i, i));

        var storage = new InMemoryStorageProvider();
        var nodes = 0;
        OperatorContext Context(RecordErrors? e) => new()
        {
            WorkerId = 0, WorkerCount = 1, Storage = storage, NodeName = $"n{nodes++}", Errors = e,
        };

        // Each operator twice, default and guarded, fed the same deltas; state-keeping ones
        // do the same store work either way, so the difference is the guard's alone.
        (string Name, Func<RecordErrors?, IStepOperator> Make, object?[] Inputs)[] cases =
        [
            ("select", e => new MapOperator<int, int>(n => n + 1, e), [delta]),
            ("where", e => new FilterOperator<int>(n => n % 2 == 0, e), [delta]),
            ("groupBy", e => new MapOperator<int, KeyedValue<int, int>>(
                KeySelectors.Keyed<int, int>(n => n % 16, guarded: e is not null), e), [delta]),
            ("sum", e => new LinearAggregateOperator<int, int, long>(Context(e), LinearAggregates.Sum<int, long>(n => n), null), [keyed]),
            ("max", e => new ExtremumOperator<int, int, int>(Context(e), n => n, +1, null), [keyed]),
            ("aggregate", e => new AggregateOperator<int, int, long>(Context(e), Aggregates.Count<int>(), null), [keyed]),
            ("join", e => new JoinOperator<int, int, int, int>(Context(e), (_, l, r) => l + r, null), [keyed, one]),
        ];

        foreach (var (name, make, inputs) in cases)
        {
            // The least of several trials. Run beside the rest of the suite, one trial now
            // and then shows a few kilobytes that the others do not (seen: 4224 bytes for
            // the guarded join against 0, never when the test runs alone) — most likely
            // unoptimised code still running because the machine is too busy to finish
            // compiling it during the warm-up. A cost the guard really has shows in every trial.
            long Measure(IStepOperator op)
            {
                for (var round = 0; round < 20; round++)
                    op.Step(inputs);
                var least = long.MaxValue;
                for (var trial = 0; trial < 5; trial++)
                {
                    var before = GC.GetAllocatedBytesForCurrentThread();
                    for (var round = 0; round < 10; round++)
                        op.Step(inputs);
                    least = Math.Min(least, GC.GetAllocatedBytesForCurrentThread() - before);
                }
                return least;
            }

            var plain = Measure(make(null));
            var guarded = Measure(make(errors));
            Assert.True(guarded <= plain + 256, $"{name}: guarded steps allocated {guarded} bytes, default ones {plain}");
            if (name is "select" or "where" or "groupBy")
                Assert.True(guarded < 256, $"{name}: guarded steps allocated {guarded} bytes");
        }
        Assert.Empty(sink);
    }

    // ---- review follow-ups ----

    [Fact]
    public async Task MapValues_returning_null_is_a_dead_letter()
    {
        var builder = Builder();
        var words = builder.Input<string>("words");
        var namedDef = words.GroupBy(w => w.Length).Count()
            .MapValues(count => count == 2 ? null! : "n" + count)
            .ToOutput(bootstrap: false);
        await using var circuit = builder.Build();
        circuit.Start();
        var state = new ZSet<KeyedValue<int, string>>();

        foreach (var word in new[] { "a", "bb", "cc" })
            await circuit.Handle(words).InsertAsync(word);
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(namedDef), state);

        AssertState(state, (new KeyedValue<int, string>(1, "n1"), 1));
        var letter = Assert.Single(Letters(circuit));
        Assert.Equal(new KeyedValue<int, long>(2, 2), letter.Record);
        Assert.EndsWith(".mapValues", letter.Operator);
        Assert.Contains("returned null", letter.Exception.Message);
    }

    [Fact]
    public async Task Event_time_Where_and_windowed_MapValues_and_Where_drop_only_the_record_that_throws()
    {
        var t0 = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var builder = Builder();
        var views = builder.Input<(string Page, int Second)>("views");
        var countsDef = views
            .WithEventTime(v => t0.AddSeconds(v.Second), TimeSpan.Zero)
            .Where(v => v.Page == "boom" ? throw new Bad("event-time where") : v.Page != "skip")
            .GroupBy(v => v.Page)
            .TumblingWindow(TimeSpan.FromSeconds(10))
            .Count()
            .MapValues(count => count == 3 ? throw new Bad("windowed mapValues") : count == 4 ? null! : "n" + count)
            .Where(row => row.Value == "n2" ? throw new Bad("windowed where") : row.Key != "hidden")
            .ToOutput(bootstrap: false);
        await using var circuit = builder.Build();
        circuit.Start();
        var state = new ZSet<WindowedValue<string, string>>();

        // one: 1 view; two: 2 (the windowed Where throws); three: 3 (MapValues throws);
        // four: 4 (MapValues returns null); hidden: filtered out; boom: the first Where throws.
        var feed = new List<(string, int)> { ("boom", 1), ("skip", 1), ("hidden", 1), ("one", 1) };
        feed.AddRange(Enumerable.Repeat(("two", 2), 2));
        feed.AddRange(Enumerable.Repeat(("three", 3), 3));
        feed.AddRange(Enumerable.Repeat(("four", 4), 4));
        foreach (var view in feed)
            await circuit.Handle(views).InsertAsync(view);
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(countsDef), state);

        AssertState(state, (new WindowedValue<string, string>(t0, t0.AddSeconds(10), "one", "n1"), 1));
        var letters = Letters(circuit);
        Assert.Equal(4, letters.Count);
        Assert.Contains(letters, l => l.Record is ValueTuple<string, int> { Item1: "boom" }
                                      && l.Operator.EndsWith(".where") && l.Exception.Message == "event-time where");
        Assert.Contains(letters, l => l.Record is KeyedValue<WindowKey<string>, long> { Key.Key: "three", Value: 3 }
                                      && l.Operator.EndsWith(".mapValues") && l.Exception.Message == "windowed mapValues");
        Assert.Contains(letters, l => l.Record is KeyedValue<WindowKey<string>, long> { Key.Key: "four", Value: 4 }
                                      && l.Operator.EndsWith(".mapValues") && l.Exception.Message.Contains("returned null"));
        Assert.Contains(letters, l => l.Record is KeyedValue<WindowKey<string>, string> { Key.Key: "two", Value: "n2" }
                                      && l.Operator.EndsWith(".where") && l.Exception.Message == "windowed where");
        Assert.Null(circuit.Fault);
    }

    [Fact]
    public async Task Aggregate_reports_only_the_rows_of_the_key_that_failed()
    {
        // One worker, so the healthy key and the failing one are in the same operator's step.
        var builder = Builder(workers: 1);
        var numbers = builder.Input<(string Key, int Value)>("numbers");
        var maxDef = numbers.GroupBy(n => n.Key).Aggregate(new MaxOfNonNegative()).ToOutput(bootstrap: false);
        await using var circuit = builder.Build();
        circuit.Start();
        var state = new ZSet<KeyedValue<string, int>>();

        foreach (var row in new[] { ("a", -1), ("a", 4), ("b", 8), ("b", 9), ("c", 2) })
            await circuit.Handle(numbers).InsertAsync(row);
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(maxDef), state);

        AssertState(state, (new KeyedValue<string, int>("b", 9), 1), (new KeyedValue<string, int>("c", 2), 1));
        var letters = Letters(circuit).Select(l => (KeyedValue<string, (string Key, int Value)>)l.Record!).ToList();
        Assert.Equal([-1, 4], letters.Select(l => l.Value.Value).Order());
        Assert.All(letters, l => Assert.Equal("a", l.Key));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task OnError_on_a_left_or_full_join_reaches_every_delegate_of_the_join(int workers)
    {
        var builder = Builder(RecordErrorPolicy.Fault, workers);
        var left = builder.Input<(int Key, string Name)>("left");
        var right = builder.Input<(int Key, string Tag)>("right");
        var leftDef = left.GroupBy(l => l.Key)
            .LeftJoin(right.GroupBy(r => r.Key),
                (_, l, r) => r.Tag == "bad" ? throw new Bad("matched") : $"{l.Name}-{r.Tag}",
                (_, l) => l.Name == "bad" ? throw new Bad("unmatched") : $"{l.Name}-")
            .OnError(RecordErrorPolicy.DeadLetter)
            .ToOutput();
        var fullDef = left.GroupBy(l => l.Key)
            .FullJoin(right.GroupBy(r => r.Key),
                (_, l, r) => r.Tag == "bad" ? throw new Bad("matched") : $"{l.Name}-{r.Tag}",
                (_, l) => l.Name == "bad" ? throw new Bad("leftOnly") : $"{l.Name}-",
                (_, r) => r.Tag == "worse" ? throw new Bad("rightOnly") : $"-{r.Tag}")
            .OnError(RecordErrorPolicy.DeadLetter)
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var leftState = new ZSet<string>();
        var fullState = new ZSet<string>();

        // Key 1: an unmatched left row that throws. Key 2: a match that throws. Key 3: a
        // right row with no left that throws. Key 4: all well.
        await circuit.Handle(left).InsertAsync((1, "bad"));
        await circuit.Handle(left).InsertAsync((2, "two"));
        await circuit.Handle(right).InsertAsync((2, "bad"));
        await circuit.Handle(right).InsertAsync((3, "worse"));
        await circuit.Handle(left).InsertAsync((4, "four"));
        await circuit.Handle(right).InsertAsync((4, "x"));
        await circuit.Handle(right).InsertAsync((5, "five"));
        await circuit.StepAsync().WaitAsync(Timeout);
        Integrate(circuit.Handle(leftDef), leftState);
        Integrate(circuit.Handle(fullDef), fullState);

        Assert.Null(circuit.Fault);
        AssertState(leftState, ("four-x", 1));
        AssertState(fullState, ("four-x", 1), ("-five", 1));
        var reasons = Letters(circuit).Select(l => l.Exception.Message).Order().ToList();
        Assert.Equal(["leftOnly", "matched", "matched", "rightOnly", "unmatched"], reasons);
    }

    [Fact]
    public async Task OnError_on_a_left_join_leaves_the_operators_around_it_on_the_circuits_policy()
    {
        var builder = Builder(RecordErrorPolicy.Fault);
        var left = builder.Input<(int Key, string Name)>("left");
        var right = builder.Input<(int Key, string Tag)>("right");
        var joinedDef = left.GroupBy(l => l.Key)
            .LeftJoin(right.GroupBy(r => r.Key), (_, l, r) => $"{l.Name}-{r.Tag}", (_, l) => $"{l.Name}-")
            .OnError(RecordErrorPolicy.DeadLetter)
            .Select(row => row == "x-" ? throw new Bad("after the join") : row)
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        await circuit.Handle(left).InsertAsync((1, "x"));
        await Assert.ThrowsAsync<Bad>(() => circuit.StepAsync()).WaitAsync(Timeout);
        _ = joinedDef;
    }

    // ---- a record the source could not read (InputHandle.RejectAsync) ----

    [Fact]
    public async Task ARejectedSourceRecord_IsDeadLettered_AndItsOffsetIsConsumed()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        Circuit Build(out InputStream<int> numbers, out KeyedInputStream<int, string> names, out Output<int> output)
        {
            var builder = Builder(storage: storage, name: "rejects");
            numbers = builder.Input<int>("numbers");
            names = builder.KeyedInput<int, string>("names", s => s.Length);
            output = numbers.Concat(names.Select(s => s.Length)).ToOutput();
            return builder.Build();
        }

        var circuit = Build(out var numbers, out var names, out var output);
        circuit.Start();
        var input = circuit.Handle(numbers);
        var keyed = circuit.Handle(names);
        await input.InsertAsync(1, 0, 10);
        await input.RejectAsync("raw-11", new Bad("unreadable"), 0, 11);
        await input.InsertAsync(3, 0, 12);
        await keyed.UpsertAsync("ab", 1, 5);
        await keyed.RejectAsync("raw-6", new Bad("unreadable too"), 1, 6);
        await circuit.StepAsync();

        var state = new ZSet<int>();
        Integrate(circuit.Handle(output), state);
        AssertState(state, (1, 1), (3, 1), (2, 1));
        var letters = Letters(circuit);
        Assert.Equal(2, letters.Count);
        Assert.Equal(("input[numbers]", "raw-11", "unreadable"), (letters[0].Operator, letters[0].Record, letters[0].Exception.Message));
        Assert.Equal(("input[names]", "raw-6", "unreadable too"), (letters[1].Operator, letters[1].Record, letters[1].Exception.Message));

        // The rejected records' offsets are the committed positions: nothing came after them
        // on the keyed input, so only a consumed rejection can have put 6 there.
        Assert.Equal(12, circuit.CommittedOffsets("numbers")[0]);
        Assert.Equal(6, circuit.CommittedOffsets("names")[1]);
        await circuit.DisposeAsync();

        // Replayed after a restart, the rejection is dropped as any duplicate is.
        var restarted = Build(out numbers, out names, out output);
        restarted.Start();
        await restarted.Handle(numbers).RejectAsync("raw-11", new Bad("unreadable"), 0, 11);
        await restarted.Handle(names).RejectAsync("raw-6", new Bad("unreadable too"), 1, 6);
        await restarted.Handle(numbers).InsertAsync(4, 0, 13);
        await restarted.StepAsync();
        Assert.Empty(Letters(restarted));
        await restarted.DisposeAsync();
    }

    [Fact]
    public async Task ARejectedSourceRecord_FaultsTheCircuit_UnderTheDefaultPolicy()
    {
        var builder = Builder(RecordErrorPolicy.Fault);
        var numbers = builder.Input<int>("numbers");
        var names = builder.KeyedInput<int, string>("names", s => s.Length);
        numbers.Concat(names.Select(s => s.Length)).ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        await circuit.Handle(numbers).RejectAsync("raw", new Bad("unreadable"), 2, 7);
        var fault = await Assert.ThrowsAnyAsync<Exception>(() => circuit.StepAsync());
        var cause = fault as InvalidOperationException ?? fault.InnerException as InvalidOperationException;
        Assert.NotNull(cause);
        Assert.Contains("partition 2, offset 7", cause.Message);
        Assert.IsType<Bad>(cause.InnerException);

        var keyedBuilder = Builder(RecordErrorPolicy.Fault);
        var keyedNames = keyedBuilder.KeyedInput<int, string>("names", s => s.Length);
        keyedNames.ToOutput();
        await using var keyedCircuit = keyedBuilder.Build();
        keyedCircuit.Start();
        await keyedCircuit.Handle(keyedNames).RejectAsync("raw", new Bad("unreadable"), 0, 1);
        await Assert.ThrowsAnyAsync<Exception>(() => keyedCircuit.StepAsync());
    }
}
