using Incremental.Storage;
using Incremental.Tables;

namespace Incremental.Benchmarks.Contracts;

/// <summary>
/// One measurable workload for a performance contract: a circuit with preloaded operator
/// state whose size is the scenario's scale, plus a fixed-size delta that
/// <see cref="RunCycleAsync"/> inserts and retracts, leaving state at its baseline.
/// Keyed outputs opt out of bootstrap so the contracts measure the operators themselves,
/// not snapshot maintenance.
/// </summary>
internal interface IDeltaScenario : IAsyncDisposable
{
    Task RunCycleAsync();
}

/// <summary>Shared shape: send an insert batch, step, send its retraction, step.</summary>
internal sealed class CycleScenario<T> : IDeltaScenario where T : notnull
{
    private readonly Rig _rig;
    private readonly InputHandle<T> _input;
    private readonly WeightedValue<T>[] _insert;
    private readonly WeightedValue<T>[] _retract;

    public CycleScenario(Rig rig, InputHandle<T> input, WeightedValue<T>[] insert)
    {
        _rig = rig;
        _input = input;
        _insert = insert;
        _retract = insert.Select(w => w with { Weight = -w.Weight }).ToArray();
    }

    public async Task RunCycleAsync()
    {
        await _input.SendAsync(_insert);
        await _rig.StepAsync();
        await _input.SendAsync(_retract);
        await _rig.StepAsync();
    }

    public ValueTask DisposeAsync() => _rig.DisposeAsync();
}

/// <summary>A table join's two deltas, each undone: new facts in (they probe the table),
/// a batch of table rows changed (the facts held under them are corrected), the rows
/// changed back, the facts out. Rows are built once, so a cycle allocates only what the
/// table and the circuit allocate for it.</summary>
internal sealed class TableJoinScenario(
    Rig rig,
    SharedTable<int, TableJoinScenario.Customer> table,
    InputHandle<(int OrderId, int CustomerId)> orders,
    WeightedValue<(int OrderId, int CustomerId)>[] insert,
    TableJoinScenario.Customer[] changed,
    TableJoinScenario.Customer[] original) : IDeltaScenario
{
    internal sealed record Customer(int Id, int Rank);

    private readonly WeightedValue<(int OrderId, int CustomerId)>[] _retract =
        insert.Select(w => w with { Weight = -w.Weight }).ToArray();

    public async Task RunCycleAsync()
    {
        await orders.SendAsync(insert);
        await rig.StepAsync();
        await CommitAsync(changed);
        await rig.StepAsync();
        await CommitAsync(original);
        await rig.StepAsync();
        await orders.SendAsync(_retract);
        await rig.StepAsync();
    }

    private async Task CommitAsync(Customer[] rows)
    {
        foreach (var row in rows)
            await table.Writer.UpsertAsync(row);
        await table.Writer.CommitAsync();
    }

    public ValueTask DisposeAsync() => rig.DisposeAsync();
}

/// <summary>A table read as a stream: a few rows changed, then changed back, each as its
/// own version and step. Rows are built once, as in <see cref="TableJoinScenario"/>.</summary>
internal sealed class TableChangesScenario(
    Rig rig,
    SharedTable<int, TableJoinScenario.Customer> table,
    TableJoinScenario.Customer[] changed,
    TableJoinScenario.Customer[] original) : IDeltaScenario
{
    public async Task RunCycleAsync()
    {
        await CommitAsync(changed);
        await rig.StepAsync();
        await CommitAsync(original);
        await rig.StepAsync();
    }

    private async Task CommitAsync(TableJoinScenario.Customer[] rows)
    {
        foreach (var row in rows)
            await table.Writer.UpsertAsync(row);
        await table.Writer.CommitAsync();
    }

    public ValueTask DisposeAsync() => rig.DisposeAsync();
}

internal static class Scenarios
{
    private const int DeltaSize = 256;

    /// <summary>Join against a shared table of <paramref name="stateSize"/> rows with as
    /// many facts already held, one per key. Both halves of the rule are in the cycle: the
    /// fact delta probes the table, and the table delta — the same keys at both scales —
    /// corrects the facts held under it. Neither may depend on how much there is of either.</summary>
    public static async Task<IDeltaScenario> TableJoinAsync(int stateSize)
    {
        const int chunk = 8192;
        var table = new SharedTable<int, TableJoinScenario.Customer>("customers", c => c.Id);
        for (var i = 0; i < stateSize; i++)
        {
            await table.Writer.UpsertAsync(new TableJoinScenario.Customer(i, i));
            if ((i + 1) % chunk == 0)
                await table.Writer.CommitAsync();
        }
        await table.Writer.CommitAsync();

        var builder = new CircuitBuilder(Rig.Options());
        var customers = builder.Table(table);
        var orders = builder.Input<(int OrderId, int CustomerId)>("orders");
        var joined = orders.GroupBy(o => o.CustomerId)
            .Join(customers, (_, order, customer) => (order.OrderId, customer.Rank))
            .ToOutput();

        var circuit = builder.Build();
        circuit.Start();
        var rig = new Rig(circuit);
        rig.Watch(joined);

        var ordersIn = circuit.Handle(orders);
        await rig.FeedAsync(ordersIn,
            Enumerable.Range(0, stateSize).Select(i => new WeightedValue<(int, int)>((i, i), 1)));

        var keys = Enumerable.Range(0, DeltaSize).Select(i => i * 37 % stateSize).ToArray();
        var insert = keys
            .Select((key, i) => new WeightedValue<(int, int)>((stateSize + i, key), 1))
            .ToArray();
        var changed = keys.Select(key => new TableJoinScenario.Customer(key, -key - 1)).ToArray();
        var original = keys.Select(key => new TableJoinScenario.Customer(key, key)).ToArray();
        return new TableJoinScenario(rig, table, ordersIn, insert, changed, original);
    }

    private static async Task<SharedTable<int, TableJoinScenario.Customer>> LoadCustomersAsync(int rows)
    {
        // Every eight customers share a rank, so an index on it has small groups at any scale.
        var table = new SharedTable<int, TableJoinScenario.Customer>("customers", c => c.Id);
        await table.Writer.ReplaceAllAsync(Enumerable.Range(0, rows).Select(i => new TableJoinScenario.Customer(i, i / 8)));
        return table;
    }

    /// <summary>Remembered lookup against a shared table of <paramref name="stateSize"/> rows
    /// with as many enriched facts already remembered. The insert half probes the table and
    /// remembers; the retract half reads what was remembered. Neither may depend on the
    /// size of the table or of the memory.</summary>
    public static async Task<IDeltaScenario> TableLookupAsync(int stateSize)
    {
        var table = await LoadCustomersAsync(stateSize);
        var builder = new CircuitBuilder(Rig.Options());
        var customers = builder.Table(table);
        var orders = builder.Input<(int OrderId, int CustomerId)>("orders");
        var enriched = orders
            .Lookup(customers, o => o.CustomerId, (order, customer) => (order.OrderId, customer.Rank))
            .ToOutput();

        var circuit = builder.Build();
        circuit.Start();
        var rig = new Rig(circuit);
        rig.Watch(enriched);

        var ordersIn = circuit.Handle(orders);
        await rig.FeedAsync(ordersIn,
            Enumerable.Range(0, stateSize).Select(i => new WeightedValue<(int, int)>((i, i), 1)));

        var insert = Enumerable.Range(0, DeltaSize)
            .Select(i => new WeightedValue<(int, int)>((stateSize + i, i * 37 % stateSize), 1))
            .ToArray();
        return new CycleScenario<(int OrderId, int CustomerId)>(rig, ordersIn, insert);
    }

    /// <summary>A shared table of <paramref name="stateSize"/> rows read as a stream and
    /// counted per bucket. The circuit's first step is the bootstrap — every row, in one
    /// delta, through every operator — and the cycle is what follows it: a handful of rows
    /// changed and changed back. Its cost may not remember how large that first delta was.</summary>
    public static async Task<IDeltaScenario> TableChangesAsync(int stateSize)
    {
        const int changedRows = 16;
        var table = await LoadCustomersAsync(stateSize);
        var builder = new CircuitBuilder(Rig.Options());
        var perBucket = builder.Table(table).Changes()
            .GroupBy(c => c.Rank % 64)
            .Count()
            .ToOutput(bootstrap: false);

        var circuit = builder.Build();
        circuit.Start();
        var rig = new Rig(circuit);
        rig.Watch(perBucket);
        await rig.StepAsync();

        var keys = Enumerable.Range(0, changedRows).Select(i => i * 37 % stateSize).ToArray();
        var changed = keys.Select(key => new TableJoinScenario.Customer(key, key / 8 + 1)).ToArray();
        var original = keys.Select(key => new TableJoinScenario.Customer(key, key / 8)).ToArray();
        return new TableChangesScenario(rig, table, changed, original);
    }

    /// <summary>A derived table of <paramref name="stateSize"/> rows: a circuit reads one
    /// table as a stream and writes a row for each of its rows into another
    /// (<c>ToTable</c>). The first step is the bootstrap — every row, collapsed and
    /// committed as one version — and the cycle is what follows: a handful of source rows
    /// changed and changed back, each a version of the derived table. Its cost may not
    /// remember the bootstrap, in the circuit, in the port's scratch or in the writer's.</summary>
    public static async Task<IDeltaScenario> DerivedTableAsync(int stateSize)
    {
        const int changedRows = 16;
        var table = await LoadCustomersAsync(stateSize);
        var derived = new SharedTable<int, TableJoinScenario.Customer>("ranked", c => c.Id);
        var builder = new CircuitBuilder(Rig.Options());
        builder.Table(table).Changes()
            .Select(c => new TableJoinScenario.Customer(c.Id, c.Rank + 1))
            .ToTable(derived);

        var circuit = builder.Build();
        circuit.Start();
        var rig = new Rig(circuit);
        await rig.StepAsync();
        if (derived.Count != stateSize)
            throw new InvalidOperationException($"The derived table holds {derived.Count} rows after its bootstrap, not {stateSize}.");

        var keys = Enumerable.Range(0, changedRows).Select(i => i * 37 % stateSize).ToArray();
        var changed = keys.Select(key => new TableJoinScenario.Customer(key, key / 8 + 1)).ToArray();
        var original = keys.Select(key => new TableJoinScenario.Customer(key, key / 8)).ToArray();
        return new TableChangesScenario(rig, table, changed, original);
    }

    /// <summary>Join through a secondary index of a shared table of
    /// <paramref name="stateSize"/> rows, eight to an index key, with a fact held per key.
    /// The same cycle as <see cref="TableJoinAsync"/>: the fact delta probes the index and
    /// then the rows, and the table delta — re-keyed by the index — corrects the facts held
    /// under it. The writer maintaining the index is inside the measurement too.</summary>
    public static async Task<IDeltaScenario> TableIndexJoinAsync(int stateSize)
    {
        var table = await LoadCustomersAsync(stateSize);
        var builder = new CircuitBuilder(Rig.Options());
        var byRank = builder.Table(table).By("rank", c => c.Rank);
        var orders = builder.Input<(int OrderId, int Rank)>("orders");
        var joined = orders.GroupBy(o => o.Rank)
            .Join(byRank, (_, order, customer) => (order.OrderId, customer.Id))
            .ToOutput();

        var circuit = builder.Build();
        circuit.Start();
        var rig = new Rig(circuit);
        rig.Watch(joined);

        var ordersIn = circuit.Handle(orders);
        var ranks = stateSize / 8;
        await rig.FeedAsync(ordersIn,
            Enumerable.Range(0, ranks).Select(i => new WeightedValue<(int, int)>((i, i), 1)));

        // The same ranks at both scales, so the two runs differ in nothing but size: new
        // facts under 32 of them, and the first row of each of 96 moved to the next rank
        // and back, which takes it out of one group and into another.
        const int moved = 96;
        var insert = Enumerable.Range(0, DeltaSize / 8)
            .Select(i => new WeightedValue<(int, int)>((ranks + i, i * 3), 1))
            .ToArray();
        var changed = Enumerable.Range(0, moved).Select(rank => new TableJoinScenario.Customer(rank * 8, rank + 1)).ToArray();
        var original = Enumerable.Range(0, moved).Select(rank => new TableJoinScenario.Customer(rank * 8, rank)).ToArray();
        return new TableJoinScenario(rig, table, ordersIn, insert, changed, original);
    }

    /// <summary>The checkpoint commit of <see cref="ChangelogStorageProvider"/>: a join, a
    /// count and a distinct — one store of each kind, <paramref name="stateSize"/> entries
    /// in each — committing after every step. A commit serializes the entries the step
    /// touched and nothing else, so the cycle must cost the same however much state sits
    /// behind it. The changelog discards what it is given: the log's own cost is not the
    /// provider's, and an in-memory log would grow for the length of the run.</summary>
    public static async Task<IDeltaScenario> ChangelogCommitAsync(int stateSize)
    {
        var storage = await ChangelogStorage.OpenAsync(new DiscardingChangelog());
        var builder = new CircuitBuilder(Rig.Options(storage: storage.Pipeline("contract")));
        var orders = builder.Input<(int OrderId, int CustomerId)>("orders");
        var customers = builder.Input<(int CustomerId, int Rank)>("customers");
        var joined = orders.GroupBy(o => o.CustomerId)
            .Join(customers.GroupBy(c => c.CustomerId), (_, order, customer) => (order.OrderId, customer.Rank))
            .ToOutput();
        var counts = orders.GroupBy(o => o.CustomerId).Count().ToOutput(bootstrap: false);
        var seen = orders.Select(o => o.OrderId).Distinct().ToOutput();

        var circuit = builder.Build();
        circuit.Start();
        var rig = new Rig(circuit);
        rig.Watch(joined);
        rig.Watch(counts);
        rig.Watch(seen);

        var ordersIn = circuit.Handle(orders);
        await rig.FeedAsync(circuit.Handle(customers),
            Enumerable.Range(0, stateSize).Select(i => new WeightedValue<(int, int)>((i, i), 1)));
        await rig.FeedAsync(ordersIn,
            Enumerable.Range(0, stateSize).Select(i => new WeightedValue<(int, int)>((i, i), 1)));

        var insert = Enumerable.Range(0, DeltaSize)
            .Select(i => new WeightedValue<(int, int)>((stateSize + i, i * 37 % stateSize), 1))
            .ToArray();
        return new CycleScenario<(int OrderId, int CustomerId)>(rig, ordersIn, insert);
    }

    /// <summary>A changelog that takes every commit and keeps nothing.</summary>
    private sealed class DiscardingChangelog : IStateChangelog, IStateChangelogWriter, IStateChangelogTransaction
    {
        private readonly System.Buffers.ArrayBufferWriter<byte> _buffer = new();

        public int PartitionCount => 1;
        public long Term => 1;

        public ValueTask<IStateChangelogWriter> AcquireWriterAsync(CancellationToken cancellationToken = default) => new(this);
        public ValueTask<long> EndOffsetAsync(int partition, CancellationToken cancellationToken = default) => new(0L);

        public async IAsyncEnumerable<ChangelogBatch> ReadAsync(
            int partition, long fromOffset,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
            yield break;
        }

        public IStateChangelogTransaction BeginTransaction() => this;

        // The copy a real transaction makes, into a buffer that is then thrown away.
        public void Append(int partition, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            System.Buffers.BuffersExtensions.Write(_buffer, key);
            System.Buffers.BuffersExtensions.Write(_buffer, value);
        }

        public void AppendTombstone(int partition, ReadOnlySpan<byte> key) => System.Buffers.BuffersExtensions.Write(_buffer, key);

        public ValueTask CommitAsync(CancellationToken cancellationToken = default)
        {
            _buffer.ResetWrittenCount();
            return default;
        }

        public void Dispose()
        {
        }

        public ValueTask DisposeAsync() => default;
    }

    /// <summary>Join delta against <paramref name="stateSize"/> rows per side; every delta
    /// order matches exactly one customer, so output work is identical at both scales.</summary>
    public static async Task<IDeltaScenario> JoinAsync(int stateSize)
    {
        var builder = new CircuitBuilder(Rig.Options());
        var orders = builder.Input<(int OrderId, int CustomerId)>("orders");
        var customers = builder.Input<(int CustomerId, int Rank)>("customers");
        var joined = orders.GroupBy(o => o.CustomerId)
            .Join(customers.GroupBy(c => c.CustomerId), (_, order, customer) => (order.OrderId, customer.Rank))
            .ToOutput();

        var circuit = builder.Build();
        circuit.Start();
        var rig = new Rig(circuit);
        rig.Watch(joined);

        var ordersIn = circuit.Handle(orders);
        await rig.FeedAsync(circuit.Handle(customers),
            Enumerable.Range(0, stateSize).Select(i => new WeightedValue<(int, int)>((i, i), 1)));
        await rig.FeedAsync(ordersIn,
            Enumerable.Range(0, stateSize).Select(i => new WeightedValue<(int, int)>((i, i), 1)));

        var insert = Enumerable.Range(0, DeltaSize)
            .Select(i => new WeightedValue<(int, int)>((stateSize + i, i * 37 % stateSize), 1))
            .ToArray();
        return new CycleScenario<(int OrderId, int CustomerId)>(rig, ordersIn, insert);
    }

    /// <summary>Linear SUM delta touching a fixed set of keys among <paramref name="groupCount"/> groups.</summary>
    public static async Task<IDeltaScenario> SumAsync(int groupCount)
    {
        var builder = new CircuitBuilder(Rig.Options());
        var rows = builder.Input<(int Key, long Amount)>("rows");
        var sums = rows
            .GroupBy(r => r.Key)
            .Sum(r => r.Amount)
            .ToOutput(bootstrap: false);

        var circuit = builder.Build();
        circuit.Start();
        var rig = new Rig(circuit);
        rig.Watch(sums);

        var input = circuit.Handle(rows);
        await rig.FeedAsync(input,
            Enumerable.Range(0, groupCount).Select(i => new WeightedValue<(int, long)>((i, i), 1)));

        var insert = Enumerable.Range(0, DeltaSize)
            .Select(i => new WeightedValue<(int, long)>((i * 37 % groupCount, 5), 1))
            .ToArray();
        return new CycleScenario<(int Key, long Amount)>(rig, input, insert);
    }

    /// <summary>Tournament-tree MIN worst case (new minimum in, then deleted) against groups
    /// of <paramref name="groupSize"/> values.</summary>
    public static async Task<IDeltaScenario> MinAsync(int groupSize)
    {
        const int groups = 16;
        var builder = new CircuitBuilder(Rig.Options());
        var rows = builder.Input<(int Key, int Value)>("rows");
        var mins = rows
            .GroupBy(r => r.Key)
            .Min(r => r.Value)
            .ToOutput(bootstrap: false);

        var circuit = builder.Build();
        circuit.Start();
        var rig = new Rig(circuit);
        rig.Watch(mins);

        var input = circuit.Handle(rows);
        await rig.FeedAsync(input,
            from key in Enumerable.Range(0, groups)
            from value in Enumerable.Range(1, groupSize)
            select new WeightedValue<(int, int)>((key, value), 1));

        var insert = Enumerable.Range(0, groups)
            .Select(key => new WeightedValue<(int, int)>((key, 0), 1))
            .ToArray();
        return new CycleScenario<(int Key, int Value)>(rig, input, insert);
    }

    /// <summary>Keyed distinct: new values entering and leaving keys that already hold
    /// <paramref name="valuesPerKey"/> other values.</summary>
    public static async Task<IDeltaScenario> KeyedDistinctAsync(int valuesPerKey)
    {
        const int keys = 16;
        var builder = new CircuitBuilder(Rig.Options());
        var rows = builder.Input<(int Key, int Value)>("rows");
        var distinct = rows.GroupBy(r => r.Key).Distinct().ToOutput(bootstrap: false);

        var circuit = builder.Build();
        circuit.Start();
        var rig = new Rig(circuit);
        rig.Watch(distinct);

        var input = circuit.Handle(rows);
        await rig.FeedAsync(input,
            from key in Enumerable.Range(0, keys)
            from value in Enumerable.Range(0, valuesPerKey)
            select new WeightedValue<(int, int)>((key, value), 1));

        var insert = Enumerable.Range(0, keys)
            .Select(key => new WeightedValue<(int, int)>((key, -1 - key), 1))
            .ToArray();
        return new CycleScenario<(int Key, int Value)>(rig, input, insert);
    }
}
