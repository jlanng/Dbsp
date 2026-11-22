using CsCheck;
using Dbsp.Storage;
using Dbsp.Tables;

namespace Dbsp.Tests;

/// <summary>
/// Derived tables (<c>docs/design/shared-tables.md</c>, <em>Derived tables</em>): a circuit's
/// table-shaped result written into a shared table with <c>ToTable</c>, for other circuits to
/// join. The differential property pins the two-circuit pipeline to the single circuit it
/// replaces; the crash properties check that after a crash at any point the table equals what
/// the restarted circuit's state describes after every replayed step, however the replay is
/// cut; the directed tests cover the guard rails, restarts with and without a log, and the
/// backpressure a lagging reader puts on the writing circuit. "Crash" is abandoning a circuit
/// without disposing it, or faulting it. Every wait is fenced with a timeout.
/// </summary>
public class DerivedTableTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan Settle = TimeSpan.FromMilliseconds(150);

    private sealed record Item(int Id, int Group, int Val);

    private sealed record Grp(int Id, int Val);

    private sealed record Enriched(int Id, int Val, int GroupVal);

    private sealed record GroupCount(int Group, long Count);

    private static Dictionary<TKey, TRow> Rows<TKey, TRow>(SharedTable<TKey, TRow> table)
        where TKey : notnull
        where TRow : class
    {
        using var snapshot = table.Snapshot();
        return table.RowsAt(snapshot.Version).ToDictionary(pair => pair.Key, pair => pair.Value);
    }

    private static void AssertRows<TKey, TRow>(IReadOnlyDictionary<TKey, TRow> expected, SharedTable<TKey, TRow> table, string when)
        where TKey : notnull
        where TRow : class
    {
        var actual = Rows(table);
        Assert.True(
            expected.Count == actual.Count && expected.All(e => actual.TryGetValue(e.Key, out var row) && row.Equals(e.Value)),
            $"{when}: expected [{string.Join(", ", expected.Values.OrderBy(r => r.ToString()))}] " +
            $"but the table holds [{string.Join(", ", actual.Values.OrderBy(r => r.ToString()))}]");
    }

    private static async Task Eventually(Func<bool> done, string what = "the expected state")
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!done())
        {
            Assert.True(DateTime.UtcNow < deadline, $"Timed out waiting for {what}.");
            await Task.Delay(5);
        }
    }

    /// <summary>A plain output integrated as it arrives, readable from the test's thread.</summary>
    private sealed class Integrated<T> where T : notnull
    {
        private readonly ZSet<T> _sum = new();

        public Integrated(OutputHandle<T> output) =>
            _ = Task.Run(async () =>
            {
                await foreach (var delta in output.Reader.ReadAllAsync())
                    lock (_sum)
                        _sum.AddAll(delta);
            });

        public long WeightOf(T value)
        {
            lock (_sum)
                return _sum.WeightOf(value);
        }

        public bool IsEmpty
        {
            get
            {
                lock (_sum)
                    return _sum.IsEmpty;
            }
        }
    }

    private static async Task DisposeBounded(Circuit circuit) =>
        await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);

    private static ZSet<T> Drain<T>(OutputHandle<T> output) where T : notnull
    {
        var drained = new ZSet<T>();
        while (output.Reader.TryRead(out var delta))
            drained.AddAll(delta);
        return drained;
    }

    /// <summary>Items counted per group, written to a table of <see cref="GroupCount"/>.</summary>
    private sealed record Counts(CircuitBuilder Builder, KeyedInputStream<int, Item> Items);

    private static Counts BuildCounts(
        SharedTable<int, GroupCount> table,
        IStorageProvider? storage = null,
        int workers = 1,
        int interval = 1,
        bool autoStep = false,
        string? name = null,
        int inputCapacity = 1024,
        int maxStepItems = 100_000)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            Name = name,
            AutoStep = autoStep,
            Workers = workers,
            Storage = storage ?? new InMemoryStorageProvider(),
            CheckpointInterval = interval,
            InputChannelCapacity = inputCapacity,
            MaxStepItemsPerInput = maxStepItems,
        });
        var items = builder.KeyedInput<int, Item>("items", i => i.Id);
        items.GroupBy(i => i.Group).Count().Named("counts").ToTable(table, (group, count) => new GroupCount(group, count));
        return new Counts(builder, items);
    }

    private static SharedTable<int, GroupCount> NewCounts(SharedTableOptions? options = null) =>
        new("counts", c => c.Group, options);

    private static Dictionary<int, GroupCount> CountsOf(IEnumerable<Item> items) =>
        items.GroupBy(i => i.Group).ToDictionary(g => g.Key, g => new GroupCount(g.Key, g.LongCount()));

    /// <summary>Delegates to an in-memory provider; commits can be made to fail.</summary>
    private sealed class FlakyStorage(InMemoryCheckpointStorageProvider inner) : ICheckpointStorageProvider
    {
        public bool FailCommits { get; set; }
        public int Commits { get; private set; }

        public IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull => inner.CreateWeightStore<T>(name);

        public IIndexedWeightStore<TKey, TValue> CreateIndexedWeightStore<TKey, TValue>(string name)
            where TKey : notnull
            where TValue : notnull => inner.CreateIndexedWeightStore<TKey, TValue>(name);

        public IValueStore<TKey, TValue> CreateValueStore<TKey, TValue>(string name) where TKey : notnull =>
            inner.CreateValueStore<TKey, TValue>(name);

        public IReadOnlyDictionary<string, long> Recover() => inner.Recover();

        public void Commit(IReadOnlyDictionary<string, long> inputOffsets)
        {
            if (FailCommits)
                throw new IOException("commit failed");
            Commits++;
            inner.Commit(inputOffsets);
        }

        public string? CommittedManifest(string scope) => inner.CommittedManifest(scope);

        public void StageManifest(string scope, string manifest) => inner.StageManifest(scope, manifest);
    }

    /// <summary>A table log whose appends can be made to fail or to wait, and whose restore
    /// can be made to report an older head than it holds.</summary>
    private sealed class TestLog<TKey, TRow>(int maxRetainedVersions = 1024) : ITableLog<TKey, TRow>
        where TKey : notnull
        where TRow : class
    {
        private readonly InMemoryTableLog<TKey, TRow> _inner = new(maxRetainedVersions);

        public bool FailAppends { get; set; }
        public TaskCompletionSource? AppendGate { get; set; }
        public TaskCompletionSource AppendStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public long? RestoreAtVersion { get; set; }

        public string Epoch => _inner.Epoch;
        public long OldestRetainedVersion => _inner.OldestRetainedVersion;
        public ValueTask<long> LastVersionAsync(CancellationToken cancellationToken = default) => _inner.LastVersionAsync(cancellationToken);

        public async ValueTask AppendAsync(TableVersion<TKey, TRow> version, CancellationToken cancellationToken = default)
        {
            if (FailAppends)
                throw new IOException("append failed");
            AppendStarted.TrySetResult();
            if (AppendGate is { } gate)
                await gate.Task;
            await _inner.AppendAsync(version, cancellationToken);
        }

        public IAsyncEnumerable<TableVersion<TKey, TRow>> ReadAsync(long fromExclusive, long toInclusive, CancellationToken cancellationToken = default) =>
            _inner.ReadAsync(fromExclusive, toInclusive, cancellationToken);

        public IAsyncEnumerable<TableVersion<TKey, TRow>> FollowAsync(long fromExclusive, CancellationToken cancellationToken = default) =>
            _inner.FollowAsync(fromExclusive, cancellationToken);

        public async ValueTask<TableHead> RestoreAsync(Action<TKey, TRow> row, CancellationToken cancellationToken = default)
        {
            var head = await _inner.RestoreAsync(row, cancellationToken);
            return RestoreAtVersion is { } version ? head with { Version = version } : head;
        }

        public ValueTask TrimBeforeAsync(long version, CancellationToken cancellationToken = default) =>
            _inner.TrimBeforeAsync(version, cancellationToken);
    }

    // ── differential: two circuits through a derived table against the one circuit ──────

    private enum Kind { UpsertItem, DeleteItem, UpsertGroup, DeleteGroup, InsertFact, RetractFact }

    /// <summary>One generated command, and what to do before it: bit 0 steps the writing
    /// circuit, bit 1 the reading one, bit 2 commits the source table, bit 3 brings
    /// everything to rest and compares.</summary>
    private static readonly Gen<List<(int Kind, int Key, int Alt, int Before)>> GenCommands =
        Gen.Select(Gen.Int[0, 5], Gen.Int[0, 3], Gen.Int[0, 2], Gen.Int[0, 15]).List[1, 60];

    /// <summary>Circuit A enriches items with their group's row and writes the result to a
    /// table; circuit B joins facts to that table. The reference is one circuit doing both
    /// joins on streams. A and B step on their own schedules, in any interleaving, and
    /// whenever everything is brought to rest B's integrated output equals the reference's.</summary>
    private static void AssertMatchesSingleCircuit(List<(int Kind, int Key, int Alt, int Before)> commands)
    {
        RunAsync().GetAwaiter().GetResult();
        return;

        async Task RunAsync()
        {
            foreach (var workers in new[] { 1, 4 })
            {
                var groups = new SharedTable<int, Grp>("groups", g => g.Id);
                var enriched = new SharedTable<int, Enriched>("enriched", e => e.Id);

                var builderA = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = workers });
                var itemsDef = builderA.KeyedInput<int, Item>("items", i => i.Id);
                itemsDef.GroupBy(i => i.Group)
                    .Join(builderA.Table(groups), (_, item, grp) => new Enriched(item.Id, item.Val, grp.Val))
                    .ToTable(enriched);

                var builderB = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = workers });
                var factsDef = builderB.Input<(int ItemId, int V)>("facts");
                var viaTableDef = factsDef.GroupBy(f => f.ItemId)
                    .Join(builderB.Table(enriched), (id, f, e) => (id, f.V, e.Val, e.GroupVal))
                    .ToOutput();

                var builderC = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = workers });
                var refItemsDef = builderC.KeyedInput<int, Item>("items", i => i.Id);
                var refGroupsDef = builderC.KeyedInput<int, Grp>("groups", g => g.Id);
                var refFactsDef = builderC.Input<(int ItemId, int V)>("facts");
                var refEnriched = refItemsDef.GroupBy(i => i.Group)
                    .Join(refGroupsDef.GroupBy(g => g.Id), (_, item, grp) => new Enriched(item.Id, item.Val, grp.Val));
                var viaStreamDef = refFactsDef.GroupBy(f => f.ItemId)
                    .Join(refEnriched.GroupBy(e => e.Id), (id, f, e) => (id, f.V, e.Val, e.GroupVal))
                    .ToOutput();

                await using var a = builderA.Build();
                await using var b = builderB.Build();
                await using var c = builderC.Build();
                a.Start();
                b.Start();
                c.Start();
                var items = a.Handle(itemsDef);
                var facts = b.Handle(factsDef);
                var refItems = c.Handle(refItemsDef);
                var refGroups = c.Handle(refGroupsDef);
                var refFacts = c.Handle(refFactsDef);
                var viaTable = new ZSet<(int, int, int, int)>();
                var viaStream = new ZSet<(int, int, int, int)>();
                var compared = 0;

                async Task RestAndCompare()
                {
                    await groups.Writer.CommitAsync();
                    await a.StepAsync().WaitAsync(Timeout);
                    await b.StepAsync().WaitAsync(Timeout);
                    await c.StepAsync().WaitAsync(Timeout);
                    viaTable.AddAll(Drain(b.Handle(viaTableDef)));
                    viaStream.AddAll(Drain(c.Handle(viaStreamDef)));
                    var equal = viaTable.DistinctCount == viaStream.DistinctCount
                        && viaStream.All(e => viaTable.WeightOf(e.Value) == e.Weight);
                    if (!equal)
                        throw new Exception(
                            $"differential mismatch (workers={workers}, comparison {compared}): the single circuit gives " +
                            $"{viaStream} but the pipeline through the derived table gives {viaTable}");
                    compared++;
                }

                for (var n = 0; n < commands.Count; n++)
                {
                    var command = commands[n];
                    if ((command.Before & 8) != 0)
                        await RestAndCompare();
                    if ((command.Before & 4) != 0)
                        await groups.Writer.CommitAsync();
                    if ((command.Before & 1) != 0)
                        await a.StepAsync().WaitAsync(Timeout);
                    if ((command.Before & 2) != 0)
                    {
                        await b.StepAsync().WaitAsync(Timeout);
                        viaTable.AddAll(Drain(b.Handle(viaTableDef)));
                    }

                    switch ((Kind)command.Kind)
                    {
                        case Kind.UpsertItem:
                            var item = new Item(command.Key, command.Alt, n % 4);
                            await items.UpsertAsync(item);
                            await refItems.UpsertAsync(item);
                            break;
                        case Kind.DeleteItem:
                            await items.DeleteAsync(command.Key);
                            await refItems.DeleteAsync(command.Key);
                            break;
                        case Kind.UpsertGroup:
                            var group = new Grp(command.Alt, command.Key);
                            await groups.Writer.UpsertAsync(group);
                            await refGroups.UpsertAsync(group);
                            break;
                        case Kind.DeleteGroup:
                            await groups.Writer.DeleteAsync(command.Alt);
                            await refGroups.DeleteAsync(command.Alt);
                            break;
                        case Kind.InsertFact:
                            await facts.SendAsync((command.Key, command.Alt), 1);
                            await refFacts.SendAsync((command.Key, command.Alt), 1);
                            break;
                        default:
                            await facts.SendAsync((command.Key, command.Alt), -1);
                            await refFacts.SendAsync((command.Key, command.Alt), -1);
                            break;
                    }
                }
                await RestAndCompare();
            }
        }
    }

    [Fact]
    public void A_pipeline_through_a_derived_table_matches_the_single_circuit_it_replaces() =>
        GenCommands.Sample(AssertMatchesSingleCircuit);

    // ── directed: what a derived table is ──────────────────────────────────────────────

    private sealed record Trader(int Id, string Name, int DeskId);

    private sealed record Desk(int Id, string Region);

    private sealed record TraderDesk(int TraderId, string Name, string Region);

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task A_derived_table_bootstraps_from_loaded_sources_and_a_change_two_hops_away_reaches_the_reader(int workers)
    {
        var traders = new SharedTable<int, Trader>("traders", t => t.Id);
        var desks = new SharedTable<int, Desk>("desks", d => d.Id);
        var traderDesks = new SharedTable<int, TraderDesk>("trader-desks", td => td.TraderId);

        // The sources are loaded before the deriving circuit exists.
        await traders.Writer.UpsertAsync(new Trader(1, "ann", 10));
        await traders.Writer.UpsertAsync(new Trader(2, "bob", 20));
        await traders.Writer.CommitAsync();
        await desks.Writer.UpsertAsync(new Desk(10, "emea"));
        await desks.Writer.UpsertAsync(new Desk(20, "apac"));
        await desks.Writer.CommitAsync();

        var deriving = new CircuitBuilder(new CircuitOptions { Workers = workers });
        deriving.Table(traders).Changes()
            .GroupBy(t => t.DeskId)
            .Join(deriving.Table(desks), (_, trader, desk) => new TraderDesk(trader.Id, trader.Name, desk.Region))
            .ToTable(traderDesks);
        await using var derivingCircuit = deriving.Build();
        derivingCircuit.Start();

        await Eventually(() => traderDesks.Count == 2, "the derived table to be bootstrapped");
        AssertRows(
            new Dictionary<int, TraderDesk> { [1] = new(1, "ann", "emea"), [2] = new(2, "bob", "apac") },
            traderDesks, "after the bootstrap");

        // A pipeline added later joins trades to the derived table directly: one hop.
        var pipeline = new CircuitBuilder(new CircuitOptions { Workers = workers });
        var tradesDef = pipeline.Input<(int Id, int TraderId)>("trades");
        var enrichedDef = tradesDef.GroupBy(t => t.TraderId)
            .Join(pipeline.Table(traderDesks), (_, trade, td) => (trade.Id, td.Name, td.Region))
            .ToOutput();
        await using var pipelineCircuit = pipeline.Build();
        pipelineCircuit.Start();
        var view = new Integrated<(int, string, string)>(pipelineCircuit.Handle(enrichedDef));
        await pipelineCircuit.Handle(tradesDef).InsertAsync((100, 1));
        await Eventually(() => view.WeightOf((100, "ann", "emea")) == 1, "the trade to be enriched");

        // The desk moves region: two hops upstream of the trade.
        await desks.Writer.UpsertAsync(new Desk(10, "amer"));
        await desks.Writer.CommitAsync();
        await Eventually(
            () => view.WeightOf((100, "ann", "amer")) == 1 && view.WeightOf((100, "ann", "emea")) == 0,
            "the desk's change to reach the trade");

        // The trader leaves: the derived row goes, and with it the enrichment.
        await traders.Writer.DeleteAsync(1);
        await traders.Writer.CommitAsync();
        await Eventually(() => traderDesks.Count == 1 && view.IsEmpty, "the trader's removal to reach the trade");
    }

    [Fact]
    public async Task Each_step_is_one_version_and_a_step_that_changes_nothing_mints_none()
    {
        var table = NewCounts();
        var pipeline = BuildCounts(table);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var items = circuit.Handle(pipeline.Items);

        await items.UpsertAsync(new Item(1, 0, 0));
        await items.UpsertAsync(new Item(2, 0, 0));
        await items.UpsertAsync(new Item(3, 1, 0));
        await circuit.StepAsync();
        Assert.Equal(1, table.LatestVersion);
        AssertRows(new Dictionary<int, GroupCount> { [0] = new(0, 2), [1] = new(1, 1) }, table, "after the first step");

        // Item 3 moves to group 0 and item 1 to group 1: the counts end where they were.
        await items.UpsertAsync(new Item(3, 0, 0));
        await items.UpsertAsync(new Item(1, 1, 0));
        await circuit.StepAsync();
        Assert.Equal(1, table.LatestVersion);

        await circuit.StepAsync(); // nothing at all
        Assert.Equal(1, table.LatestVersion);

        // One version carrying an update and a delete.
        await items.DeleteAsync(1);
        await items.UpsertAsync(new Item(4, 0, 0));
        await circuit.StepAsync();
        Assert.Equal(2, table.LatestVersion);
        AssertRows(new Dictionary<int, GroupCount> { [0] = new(0, 3) }, table, "after the delete");
    }

    [Fact]
    public async Task The_circuit_is_the_tables_only_writer_until_it_is_disposed()
    {
        var table = NewCounts();
        await table.Writer.UpsertAsync(new GroupCount(9, 9));
        await table.Writer.CommitAsync();

        // Staged by whoever wrote the table before, and never committed: not the circuit's.
        await table.Writer.UpsertAsync(new GroupCount(8, 8));

        var pipeline = BuildCounts(table, name: "counter");
        var circuit = pipeline.Builder.Build();

        var refused = Assert.Throws<InvalidOperationException>(() => { _ = table.Writer.UpsertAsync(new GroupCount(1, 1)); });
        Assert.Contains("circuit 'counter'", refused.Message);
        Assert.Throws<InvalidOperationException>(() => { _ = table.Writer.UpsertAsync(new GroupCount(1, 1), 0, 5); });
        Assert.Throws<InvalidOperationException>(() => { _ = table.Writer.DeleteAsync(9); });
        Assert.Throws<InvalidOperationException>(() => { _ = table.Writer.DeleteAsync(9, 0, 6); });
        await Assert.ThrowsAsync<InvalidOperationException>(() => table.Writer.CommitAsync().AsTask());
        await Assert.ThrowsAsync<InvalidOperationException>(() => table.Writer.ReplaceAllAsync([new GroupCount(1, 1)]).AsTask());

        // What the table held before the circuit is replaced by what the circuit computes.
        circuit.Start();
        await circuit.Handle(pipeline.Items).UpsertAsync(new Item(1, 3, 0));
        await circuit.StepAsync();
        AssertRows(new Dictionary<int, GroupCount> { [3] = new(3, 1) }, table, "after the circuit's first step");

        await DisposeBounded(circuit);
        await table.Writer.UpsertAsync(new GroupCount(7, 7));
        await table.Writer.CommitAsync();
        Assert.Equal(2, table.Count);
    }

    // ── guard rails ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task A_circuit_cannot_write_a_table_it_reads()
    {
        var table = new SharedTable<int, Grp>("groups", g => g.Id);

        var readThenWrite = new CircuitBuilder();
        readThenWrite.Table(table).Changes().Select(g => new Grp(g.Id, g.Val + 1)).ToTable(table);
        var error = Assert.Throws<InvalidOperationException>(() => readThenWrite.Build());
        Assert.Contains("both reads table 'groups' and writes it", error.Message);

        var writeThenRead = new CircuitBuilder();
        var rows = writeThenRead.Input<Grp>("rows");
        rows.ToTable(table);
        rows.GroupBy(g => g.Id).Join(writeThenRead.Table(table), (_, a, b) => (a, b)).ToOutput();
        Assert.Throws<InvalidOperationException>(() => writeThenRead.Build());

        // Neither refusal left the table claimed.
        var fine = new CircuitBuilder();
        fine.Input<Grp>("rows").ToTable(table);
        await DisposeBounded(fine.Build());
    }

    [Fact]
    public async Task A_cycle_through_other_derived_tables_is_rejected_when_the_circuit_closing_it_is_built()
    {
        var one = new SharedTable<int, Grp>("one", g => g.Id);
        var two = new SharedTable<int, Grp>("two", g => g.Id);
        var three = new SharedTable<int, Grp>("three", g => g.Id);

        CircuitBuilder Copy(SharedTable<int, Grp> from, SharedTable<int, Grp> to)
        {
            var builder = new CircuitBuilder();
            builder.Table(from).Changes().ToTable(to);
            return builder;
        }

        var first = Copy(one, two).Build();
        var second = Copy(two, three).Build();
        var error = Assert.Throws<InvalidOperationException>(() => Copy(three, one).Build());
        Assert.Contains("'one' → 'two' → 'three'", error.Message);

        // Once a link is gone the loop is not one.
        await DisposeBounded(second);
        await DisposeBounded(Copy(three, one).Build());
        await DisposeBounded(first);
    }

    [Fact]
    public async Task A_table_has_one_writing_circuit()
    {
        var table = NewCounts();
        var first = BuildCounts(table, name: "first").Builder.Build();

        var error = Assert.Throws<InvalidOperationException>(() => BuildCounts(table, name: "second").Builder.Build());
        Assert.Contains("already written by circuit 'first'", error.Message);

        // Twice from one circuit is refused where it is declared.
        var twice = new CircuitBuilder();
        var rows = twice.Input<GroupCount>("rows");
        rows.ToTable(NewCounts());
        Assert.Throws<ArgumentException>(() => rows.ToTable(NewCounts()));

        await DisposeBounded(first);
        await DisposeBounded(BuildCounts(table, name: "second").Builder.Build());
    }

    [Fact]
    public void Clustered_circuits_and_loop_bodies_cannot_write_tables()
    {
        var table = new SharedTable<int, Grp>("groups", g => g.Id);

        var clustered = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false,
            Cluster = new ClusterOptions { Transport = Dbsp.Cluster.InMemoryExchangeTransport.CreateMesh(2)[0] },
        });
        clustered.Input<Grp>("rows").ToTable(table);
        Assert.Throws<NotSupportedException>(() => clustered.Build());

        var looping = new CircuitBuilder();
        Assert.Throws<NotSupportedException>(() =>
            looping.Input<Grp>("rows").Recurse(body =>
            {
                body.ToTable(table);
                return body;
            }));

        // Neither left the table claimed.
        var fine = new CircuitBuilder();
        fine.Input<Grp>("rows").ToTable(table);
        fine.Build();
    }

    [Fact]
    public async Task A_stream_that_is_not_table_shaped_faults_the_circuit()
    {
        async Task<(Circuit Circuit, InputHandle<Grp> Rows)> Start()
        {
            var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
            var rows = builder.Input<Grp>("rows");
            rows.ToTable(new SharedTable<int, Grp>("groups", g => g.Id));
            var circuit = builder.Build();
            circuit.Start();
            await circuit.StepAsync();
            return (circuit, circuit.Handle(rows));
        }

        // Two rows for one key in one step.
        var (circuit, rows) = await Start();
        await rows.InsertAsync(new Grp(1, 1));
        await rows.InsertAsync(new Grp(1, 2));
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => circuit.StepAsync());
        Assert.Contains("not table-shaped at key '1'", error.Message);
        Assert.Contains("more than one row", error.Message);
        Assert.NotNull(circuit.Fault);

        // The same row twice.
        (circuit, rows) = await Start();
        await rows.SendAsync(new Grp(1, 1), 2);
        error = await Assert.ThrowsAsync<InvalidOperationException>(() => circuit.StepAsync());
        Assert.Contains("more than one row", error.Message);

        // A second row in a later step, the first never retracted: only the table can know.
        (circuit, rows) = await Start();
        await rows.InsertAsync(new Grp(1, 1));
        await circuit.StepAsync();
        await rows.InsertAsync(new Grp(1, 2));
        error = await Assert.ThrowsAsync<InvalidOperationException>(() => circuit.StepAsync());
        Assert.Contains("is still there", error.Message);

        // A retraction of a row the table does not hold.
        (circuit, rows) = await Start();
        await rows.InsertAsync(new Grp(1, 1));
        await circuit.StepAsync();
        await rows.RemoveAsync(new Grp(1, 5));
        error = await Assert.ThrowsAsync<InvalidOperationException>(() => circuit.StepAsync());
        Assert.Contains("but the table holds", error.Message);

        // Two retractions for one key.
        (circuit, rows) = await Start();
        await rows.InsertAsync(new Grp(1, 1));
        await circuit.StepAsync();
        await rows.RemoveAsync(new Grp(1, 1));
        await rows.RemoveAsync(new Grp(1, 2));
        error = await Assert.ThrowsAsync<InvalidOperationException>(() => circuit.StepAsync());
        Assert.Contains("retracted more than one row", error.Message);

        // A replacement is fine: old row out, new row in, one step.
        (circuit, rows) = await Start();
        await rows.InsertAsync(new Grp(1, 1));
        await circuit.StepAsync();
        await rows.RemoveAsync(new Grp(1, 1));
        await rows.InsertAsync(new Grp(1, 2));
        await circuit.StepAsync();
        Assert.Null(circuit.Fault);
        await DisposeBounded(circuit);
    }

    // ── commit after delivery ──────────────────────────────────────────────────────────

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task The_checkpoint_commits_only_after_the_table_version_has(bool autoStep)
    {
        var log = new TestLog<int, GroupCount> { AppendGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };
        var table = await SharedTable<int, GroupCount>.OpenAsync("counts", c => c.Group, log);
        var storage = new FlakyStorage(new InMemoryCheckpointStorageProvider());
        var pipeline = BuildCounts(table, storage, autoStep: autoStep);
        var circuit = pipeline.Builder.Build();
        circuit.Start();
        if (autoStep)
            await Eventually(() => circuit.CommittedStep == 1, "the first, reconciling step"); // nothing to append: it commits
        else
            await circuit.StepAsync();
        var commitsBefore = storage.Commits;

        await circuit.Handle(pipeline.Items).UpsertAsync(new Item(1, 0, 0));
        var step = autoStep ? Task.CompletedTask : circuit.StepAsync();
        await log.AppendStarted.Task.WaitAsync(Timeout);
        await Task.Delay(Settle);

        // The version is with the log and not yet durable: nothing is committed past it.
        Assert.Equal(commitsBefore, storage.Commits);
        Assert.Equal(1, circuit.CommittedStep);
        Assert.Equal(0, circuit.CommittedVersion(table));
        Assert.Equal(0, table.LatestVersion);
        if (!autoStep)
            Assert.False(step.IsCompleted);

        log.AppendGate.SetResult();
        await step.WaitAsync(Timeout);
        await Eventually(() => circuit.CommittedStep == 2, "the checkpoint after the version");
        Assert.Equal(1, table.LatestVersion);
        Assert.Equal(1, circuit.CommittedVersion(table));
        await DisposeBounded(circuit);
    }

    // ── crash and restart ──────────────────────────────────────────────────────────────

    public enum CrashPoint
    {
        /// <summary>The step ran; its table version was never appended.</summary>
        BeforeTableVersion,

        /// <summary>The table version is durable; the checkpoint covering it is not.</summary>
        AfterTableVersionBeforeCheckpoint,

        /// <summary>The step went through whole.</summary>
        AfterCheckpoint,
    }

    private static readonly Gen<(List<(bool Delete, int Id, int Group, bool Cut)> Commands, int CrashAt, int Point, int Interval, bool InProcess, int ReplayCuts)>
        GenCrash = Gen.Select(
            Gen.Select(Gen.Bool, Gen.Int[0, 5], Gen.Int[0, 2], Gen.Bool).List[1, 40],
            Gen.Int[0, 39], Gen.Int[0, 2], Gen.Int[1, 3], Gen.Bool, Gen.Int[0, int.MaxValue]);

    /// <summary>Runs the counting circuit over stamped commands, crashes it at a chosen step
    /// and point, restarts it — against the table reopened from its log, or against the same
    /// table object after a fault — and replays from the committed offset with a different
    /// cut of steps. After every step, before and after the crash, the table must equal the
    /// counts of exactly the commands the circuit has consumed: not merely converge.</summary>
    private static void AssertTableTracksTheCircuitAcrossACrash(
        (List<(bool Delete, int Id, int Group, bool Cut)> Commands, int CrashAt, int Point, int Interval, bool InProcess, int ReplayCuts) sample)
    {
        RunAsync().GetAwaiter().GetResult();
        return;

        async Task RunAsync()
        {
            var (commands, crashAt, pointIndex, interval, inProcess, replayCuts) = sample;
            var point = (CrashPoint)pointIndex;
            crashAt %= commands.Count;
            // A failed append faults the table's writer for good, as a real one does: that
            // crash is only survived by reopening the table from its log.
            if (point == CrashPoint.BeforeTableVersion)
                inProcess = false;

            foreach (var workers in new[] { 1, 4 })
            {
                var log = new TestLog<int, GroupCount>();
                var storage = new FlakyStorage(new InMemoryCheckpointStorageProvider());
                var table = await SharedTable<int, GroupCount>.OpenAsync("counts", c => c.Group, log);
                var context = $"workers={workers}, interval={interval}, crash at {crashAt} {point}, inProcess={inProcess}";

                Dictionary<int, GroupCount> ModelAfter(int consumed)
                {
                    var live = new Dictionary<int, Item>();
                    for (var n = 0; n < consumed; n++)
                    {
                        if (commands[n].Delete)
                            live.Remove(commands[n].Id);
                        else
                            live[commands[n].Id] = new Item(commands[n].Id, commands[n].Group, 0);
                    }
                    return CountsOf(live.Values);
                }

                static async Task Send(KeyedInputHandle<int, Item> items, (bool Delete, int Id, int Group, bool Cut) command, int offset)
                {
                    if (command.Delete)
                        await items.DeleteAsync(command.Id, offset);
                    else
                        await items.UpsertAsync(new Item(command.Id, command.Group, 0), offset);
                }

                // ── the run that crashes ──
                var first = BuildCounts(table, storage, workers, interval);
                var crashed = first.Builder.Build();
                crashed.Start();
                var items = crashed.Handle(first.Items);
                var faulted = false;
                for (var n = 0; n <= crashAt; n++)
                {
                    await Send(items, commands[n], n);
                    if (n < crashAt && !commands[n].Cut)
                        continue;
                    if (n < crashAt)
                    {
                        await crashed.StepAsync().WaitAsync(Timeout);
                        AssertRows(ModelAfter(n + 1), table, $"{context}: before the crash, after command {n}");
                        continue;
                    }

                    log.FailAppends = point == CrashPoint.BeforeTableVersion;
                    storage.FailCommits = point == CrashPoint.AfterTableVersionBeforeCheckpoint;
                    try
                    {
                        await crashed.StepAsync().WaitAsync(Timeout);
                    }
                    catch (IOException)
                    {
                        faulted = true;
                    }
                }

                // A restart in the same process needs the old circuit out of the way, which
                // a crash does by itself: fault it on its next commit.
                storage.FailCommits = true;
                for (var n = 0; inProcess && !faulted; n++)
                {
                    Assert.True(n <= interval, $"{context}: the circuit never reached a commit to fail");
                    try
                    {
                        await crashed.StepAsync().WaitAsync(Timeout);
                    }
                    catch (IOException)
                    {
                        faulted = true;
                    }
                }
                log.FailAppends = storage.FailCommits = false;

                // ── the restart ──
                var reopened = inProcess ? table : await SharedTable<int, GroupCount>.OpenAsync("counts", c => c.Group, log);
                var second = BuildCounts(reopened, storage, workers, interval);
                var resumed = second.Builder.Build();
                resumed.Start();
                items = resumed.Handle(second.Items);
                var from = (int)((resumed.CommittedOffset("items") ?? -1) + 1);
                Assert.True(from <= crashAt + 1, $"{context}: the checkpoint is past what was sent");
                if (point == CrashPoint.AfterCheckpoint && interval == 1 && !inProcess)
                    Assert.Equal(crashAt + 1, from);

                // Before any input is replayed the first step alone must put the table back
                // to what the checkpoint describes.
                await resumed.StepAsync().WaitAsync(Timeout);
                AssertRows(ModelAfter(from), reopened, $"{context}: after the restart's first step (resumed at {from})");

                var cuts = new Random(replayCuts);
                for (var n = from; n < commands.Count; n++)
                {
                    await Send(items, commands[n], n);
                    if (n < commands.Count - 1 && cuts.Next(3) != 0)
                        continue;
                    await resumed.StepAsync().WaitAsync(Timeout);
                    AssertRows(ModelAfter(n + 1), reopened, $"{context}: replaying, after command {n} (resumed at {from})");
                }

                await DisposeBounded(resumed);
                AssertRows(ModelAfter(commands.Count), reopened, $"{context}: at the end");
                if (!faulted)
                {
                    // The abandoned circuit's threads; its table is the old object.
                    try
                    {
                        await DisposeBounded(crashed);
                    }
                    catch (Exception)
                    {
                    }
                }
            }
        }
    }

    [Fact]
    public void After_a_crash_at_any_point_the_table_equals_the_restarted_circuits_state_at_every_step() =>
        GenCrash.Sample(AssertTableTracksTheCircuitAcrossACrash, iter: 60);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_key_written_before_a_crash_and_gone_within_the_replayed_step_does_not_stay_behind(bool inProcess)
    {
        var log = new TestLog<int, GroupCount>();
        var storage = new FlakyStorage(new InMemoryCheckpointStorageProvider());
        var table = await SharedTable<int, GroupCount>.OpenAsync("counts", c => c.Group, log);
        var first = BuildCounts(table, storage);
        var crashed = first.Builder.Build();
        crashed.Start();
        var items = crashed.Handle(first.Items);
        await items.UpsertAsync(new Item(1, 0, 0), 0);
        await crashed.StepAsync();

        // Group 7 appears, reaches the table, and the checkpoint that would cover it fails.
        storage.FailCommits = true;
        await items.UpsertAsync(new Item(2, 7, 0), 1);
        await Assert.ThrowsAsync<IOException>(() => crashed.StepAsync());
        storage.FailCommits = false;
        Assert.True(Rows(table).ContainsKey(7));

        var reopened = inProcess ? table : await SharedTable<int, GroupCount>.OpenAsync("counts", c => c.Group, log);
        var second = BuildCounts(reopened, storage);
        await using var resumed = second.Builder.Build();
        resumed.Start();
        Assert.Equal(0, resumed.CommittedOffset("items"));
        Assert.Equal(1, resumed.CommittedVersion(reopened));
        items = resumed.Handle(second.Items);

        // The replay cuts differently: the upsert that made group 7 and the delete that was
        // to follow it arrive in one step, and cancel inside the circuit. The step's delta
        // says nothing about group 7.
        await items.UpsertAsync(new Item(2, 7, 0), 1);
        await items.DeleteAsync(2, 2);
        await resumed.StepAsync();
        AssertRows(new Dictionary<int, GroupCount> { [0] = new(0, 1) }, reopened, "after the merged replay step");
        Assert.Equal(reopened.LatestVersion, resumed.CommittedVersion(reopened));
    }

    [Fact]
    public async Task A_replay_that_repeats_the_crashed_step_mints_no_version_at_all()
    {
        var log = new TestLog<int, GroupCount>();
        var storage = new FlakyStorage(new InMemoryCheckpointStorageProvider());
        var table = await SharedTable<int, GroupCount>.OpenAsync("counts", c => c.Group, log);
        var first = BuildCounts(table, storage);
        var crashed = first.Builder.Build();
        crashed.Start();
        await crashed.StepAsync();
        storage.FailCommits = true;
        await crashed.Handle(first.Items).UpsertAsync(new Item(1, 4, 0), 0);
        await Assert.ThrowsAsync<IOException>(() => crashed.StepAsync());
        storage.FailCommits = false;
        Assert.Equal(1, table.LatestVersion);

        // Readers of the table see nothing step back: the undo and the redo are one version,
        // and it changes nothing.
        var second = BuildCounts(table, storage);
        await using var resumed = second.Builder.Build();
        resumed.Start();
        await resumed.Handle(second.Items).UpsertAsync(new Item(1, 4, 0), 0);
        await resumed.StepAsync();
        Assert.Equal(1, table.LatestVersion);
        AssertRows(new Dictionary<int, GroupCount> { [4] = new(4, 1) }, table, "after the replay");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task An_auto_stepping_circuit_restarted_with_nothing_to_replay_still_takes_back_what_it_wrote_past_its_checkpoint(int workers)
    {
        var storage = new FlakyStorage(new InMemoryCheckpointStorageProvider());
        var table = NewCounts();
        var first = BuildCounts(table, storage, workers, autoStep: true);
        var crashed = first.Builder.Build();
        crashed.Start();
        await crashed.Handle(first.Items).UpsertAsync(new Item(1, 0, 0));
        await Eventually(() => table.Count == 1 && crashed.CommittedVersion(table) == table.LatestVersion, "the first row to be checkpointed");

        storage.FailCommits = true;
        await crashed.Handle(first.Items).UpsertAsync(new Item(2, 5, 0));
        await Eventually(() => crashed.Fault is not null, "the failed commit to fault the circuit");
        Assert.Equal(2, table.Count); // written, not checkpointed
        storage.FailCommits = false;

        // The source cannot replay (unstamped, and nothing is sent): the circuit's state is
        // the checkpoint's, and so the table must be.
        var second = BuildCounts(table, storage, workers, autoStep: true);
        await using var resumed = second.Builder.Build();
        resumed.Start();
        await Eventually(() => table.Count == 1, "the uncheckpointed row to be taken back");
        AssertRows(new Dictionary<int, GroupCount> { [0] = new(0, 1) }, table, "after the restart");
    }

    [Fact]
    public async Task The_writer_never_waits_for_its_own_checkpoint_and_the_tail_keeps_what_a_restart_must_undo()
    {
        // The tail holds two versions; the circuit checkpoints every six steps. Unlogged,
        // the versions since the checkpoint exist nowhere but the tail.
        var storage = new FlakyStorage(new InMemoryCheckpointStorageProvider());
        var table = NewCounts(new SharedTableOptions { TailVersions = 2 });
        var first = BuildCounts(table, storage, interval: 6);
        var crashed = first.Builder.Build();
        crashed.Start();
        var items = crashed.Handle(first.Items);
        for (var n = 0; n < 6; n++) // step 6 commits: version 6
        {
            await items.UpsertAsync(new Item(n, n, 0), n);
            await crashed.StepAsync().WaitAsync(Timeout);
        }
        Assert.Equal(6, crashed.CommittedVersion(table));
        Assert.True(table.TailCount <= 3, $"the tail holds {table.TailCount} versions after a checkpoint");

        storage.FailCommits = true;
        for (var n = 6; n < 11; n++) // five more versions, none checkpointed
        {
            await items.UpsertAsync(new Item(n, n, 0), n);
            await crashed.StepAsync().WaitAsync(Timeout);
        }
        Assert.Equal(11, table.LatestVersion);
        Assert.True(table.TailCount >= 5, $"the tail holds only {table.TailCount} versions; the restart needs five");
        await items.UpsertAsync(new Item(11, 11, 0), 11);
        await Assert.ThrowsAsync<IOException>(() => crashed.StepAsync().WaitAsync(Timeout));
        storage.FailCommits = false;

        var second = BuildCounts(table, storage, interval: 6);
        await using var resumed = second.Builder.Build();
        resumed.Start();
        await resumed.StepAsync().WaitAsync(Timeout);
        AssertRows(
            Enumerable.Range(0, 6).ToDictionary(n => n, n => new GroupCount(n, 1)), table, "after the restart's first step");
    }

    [Fact]
    public async Task A_resumed_circuit_holds_the_versions_past_its_checkpoint_for_the_restart_after_next()
    {
        // Neither the log nor the tail keeps more than two versions of its own accord.
        var options = new SharedTableOptions { TailVersions = 2 };
        var log = new TestLog<int, GroupCount>(maxRetainedVersions: 2);
        var storage = new FlakyStorage(new InMemoryCheckpointStorageProvider());
        var table = await SharedTable<int, GroupCount>.OpenAsync("counts", c => c.Group, log, options);
        var first = BuildCounts(table, storage, interval: 6);
        var circuit = first.Builder.Build();
        circuit.Start();
        var items = circuit.Handle(first.Items);
        for (var n = 0; n < 6; n++)
        {
            await items.UpsertAsync(new Item(n, n, 0), n);
            await circuit.StepAsync().WaitAsync(Timeout);
        }
        await DisposeBounded(circuit);

        // Reopened, and resumed from the checkpoint at version 6: from here the table must
        // keep what this circuit writes until its next checkpoint, which never comes.
        var reopened = await SharedTable<int, GroupCount>.OpenAsync("counts", c => c.Group, log, options);
        var second = BuildCounts(reopened, storage, interval: 6);
        var crashed = second.Builder.Build();
        crashed.Start();
        Assert.Equal(6, crashed.CommittedVersion(reopened));
        items = crashed.Handle(second.Items);
        storage.FailCommits = true;
        for (var n = 6; n < 11; n++)
        {
            await items.UpsertAsync(new Item(n, n, 0), n);
            await crashed.StepAsync().WaitAsync(Timeout);
        }
        await Assert.ThrowsAsync<IOException>(() => crashed.StepAsync().WaitAsync(Timeout));
        storage.FailCommits = false;
        Assert.Equal(11, reopened.LatestVersion);

        var third = BuildCounts(reopened, storage, interval: 6);
        await using var resumed = third.Builder.Build();
        resumed.Start();
        await resumed.StepAsync().WaitAsync(Timeout);
        AssertRows(
            Enumerable.Range(0, 6).ToDictionary(n => n, n => new GroupCount(n, 1)), reopened, "after the second restart's first step");
    }

    [Fact]
    public async Task A_circuit_that_does_not_resume_leaves_no_older_circuits_hold_on_the_tail()
    {
        var table = NewCounts(new SharedTableOptions { TailVersions = 2 });
        var first = BuildCounts(table, new InMemoryCheckpointStorageProvider());
        var circuit = first.Builder.Build();
        circuit.Start();
        for (var n = 0; n < 3; n++)
        {
            await circuit.Handle(first.Items).UpsertAsync(new Item(n, n, 0));
            await circuit.StepAsync();
        }
        await DisposeBounded(circuit);

        // No checkpoints at all this time: nothing will ever need these versions back.
        var second = BuildCounts(table);
        await using var rebuilt = second.Builder.Build();
        rebuilt.Start();
        for (var n = 0; n < 10; n++)
        {
            await rebuilt.Handle(second.Items).UpsertAsync(new Item(n, n, 0));
            await rebuilt.StepAsync();
        }
        Assert.True(table.TailCount <= 3, $"the tail holds {table.TailCount} versions for a circuit that is gone");
    }

    // ── restart: with a log, without one ───────────────────────────────────────────────

    [Fact]
    public async Task A_logged_table_and_its_circuit_resume_together_after_a_clean_stop()
    {
        var log = new TestLog<int, GroupCount>();
        var storage = new InMemoryCheckpointStorageProvider();
        var table = await SharedTable<int, GroupCount>.OpenAsync("counts", c => c.Group, log);
        var first = BuildCounts(table, storage);
        var circuit = first.Builder.Build();
        circuit.Start();
        await circuit.Handle(first.Items).UpsertAsync(new Item(1, 0, 0));
        await circuit.Handle(first.Items).UpsertAsync(new Item(2, 1, 0));
        await circuit.StepAsync();
        await DisposeBounded(circuit);

        var reopened = await SharedTable<int, GroupCount>.OpenAsync("counts", c => c.Group, log);
        var second = BuildCounts(reopened, storage);
        await using var resumed = second.Builder.Build();
        resumed.Start();
        await resumed.StepAsync();
        Assert.Equal(1, reopened.LatestVersion); // nothing to reconcile, nothing written

        // The circuit's state came back with it: item 1 leaves group 0, and the row goes.
        await resumed.Handle(second.Items).DeleteAsync(1);
        await resumed.StepAsync();
        AssertRows(new Dictionary<int, GroupCount> { [1] = new(1, 1) }, reopened, "after the delete");
    }

    [Fact]
    public async Task An_unlogged_table_is_a_new_table_each_start_so_its_circuit_must_start_from_empty_state_too()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var first = BuildCounts(NewCounts(), storage);
        var circuit = first.Builder.Build();
        circuit.Start();
        await circuit.Handle(first.Items).UpsertAsync(new Item(1, 0, 0));
        await circuit.StepAsync();
        await DisposeBounded(circuit);

        // "The process restarts": the table is recreated, empty, with a new epoch, and the
        // checkpoint survived. Resuming would leave group 0 out of the table for good.
        var recreated = NewCounts();
        var stale = BuildCounts(recreated, storage).Builder.Build();
        var error = Assert.Throws<CircuitStateMismatchException>(() => stale.Start());
        Assert.Contains("recreated", error.Message);
        Assert.Contains("OpenAsync", error.Message);

        // The refused circuit does not keep the table. Against an empty state store the
        // circuit computes the table from the start.
        var fresh = BuildCounts(recreated, new InMemoryCheckpointStorageProvider());
        await using var rebuilt = fresh.Builder.Build();
        rebuilt.Start();
        await rebuilt.Handle(fresh.Items).UpsertAsync(new Item(1, 0, 0));
        await rebuilt.StepAsync();
        AssertRows(new Dictionary<int, GroupCount> { [0] = new(0, 1) }, recreated, "after the rebuild");
        await DisposeBounded(stale);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_circuit_with_no_checkpoint_replaces_whatever_the_table_held(bool autoStep)
    {
        // The table outlives the circuit (same process, or a log); the circuit's state does not.
        var table = NewCounts();
        var first = BuildCounts(table);
        var circuit = first.Builder.Build();
        circuit.Start();
        await circuit.Handle(first.Items).UpsertAsync(new Item(1, 0, 0));
        await circuit.Handle(first.Items).UpsertAsync(new Item(2, 1, 0));
        await circuit.StepAsync();
        await DisposeBounded(circuit);
        Assert.Equal(2, table.Count);

        var second = BuildCounts(table, autoStep: autoStep);
        await using var rebuilt = second.Builder.Build();
        rebuilt.Start();
        if (autoStep)
        {
            // No input at all: the first step is owed to the table regardless.
            await Eventually(() => table.Count == 0, "the stale rows to be removed");
            return;
        }

        // Group 1 is recomputed to the row the table already has; group 0 is not recomputed.
        await rebuilt.Handle(second.Items).UpsertAsync(new Item(2, 1, 0));
        await rebuilt.StepAsync();
        AssertRows(new Dictionary<int, GroupCount> { [1] = new(1, 1) }, table, "after the rebuild's first step");
        Assert.Equal(2, table.LatestVersion); // one version: the delete of group 0
    }

    [Fact]
    public async Task ToTable_added_to_a_circuit_that_already_has_a_checkpoint_is_refused()
    {
        var storage = new InMemoryCheckpointStorageProvider();

        CircuitBuilder Build(SharedTable<int, GroupCount>? table)
        {
            var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
            var counts = builder.KeyedInput<int, Item>("items", i => i.Id).GroupBy(i => i.Group).Count().Named("counts");
            counts.ToOutput(bootstrap: false);
            if (table is not null)
                counts.ToTable(table, (group, count) => new GroupCount(group, count));
            return builder;
        }

        var before = Build(null).Build();
        before.Start();
        await before.StepAsync();
        await DisposeBounded(before);

        var table = NewCounts();
        var after = Build(table).Build();
        var error = Assert.Throws<InvalidOperationException>(() => after.Start());
        Assert.Contains("was taken before it did", error.Message);
        await DisposeBounded(after);
    }

    [Fact]
    public async Task A_circuit_refuses_to_resume_against_a_table_that_is_behind_its_checkpoint_or_has_lost_the_versions_since()
    {
        // Behind: the table was restored from somewhere that never reached the checkpoint.
        var log = new TestLog<int, GroupCount>();
        var storage = new FlakyStorage(new InMemoryCheckpointStorageProvider());
        var table = await SharedTable<int, GroupCount>.OpenAsync("counts", c => c.Group, log);
        var first = BuildCounts(table, storage);
        var circuit = first.Builder.Build();
        circuit.Start();
        for (var n = 0; n < 3; n++)
        {
            await circuit.Handle(first.Items).UpsertAsync(new Item(n, n, 0));
            await circuit.StepAsync();
        }
        await DisposeBounded(circuit);

        log.RestoreAtVersion = 2;
        var behind = await SharedTable<int, GroupCount>.OpenAsync("counts", c => c.Group, log);
        var stale = BuildCounts(behind, storage).Builder.Build();
        var error = Assert.Throws<InvalidOperationException>(() => stale.Start());
        Assert.Contains("has lost history", error.Message);
        await DisposeBounded(stale);

        // Lost: a log that keeps no history, and a version written past the checkpoint.
        var shortLog = new TestLog<int, GroupCount>(maxRetainedVersions: 0);
        storage = new FlakyStorage(new InMemoryCheckpointStorageProvider());
        table = await SharedTable<int, GroupCount>.OpenAsync("counts", c => c.Group, shortLog);
        first = BuildCounts(table, storage);
        circuit = first.Builder.Build();
        circuit.Start();
        await circuit.Handle(first.Items).UpsertAsync(new Item(1, 1, 0));
        await circuit.StepAsync();
        storage.FailCommits = true;
        await circuit.Handle(first.Items).UpsertAsync(new Item(2, 2, 0));
        await Assert.ThrowsAsync<IOException>(() => circuit.StepAsync());
        storage.FailCommits = false;

        var reopened = await SharedTable<int, GroupCount>.OpenAsync("counts", c => c.Group, shortLog);
        var lost = BuildCounts(reopened, storage).Builder.Build();
        error = Assert.Throws<InvalidOperationException>(() => lost.Start());
        Assert.Contains("no longer retained", error.Message);
        await DisposeBounded(lost);
    }

    [Fact]
    public async Task A_faulted_circuit_gives_the_table_up_and_cannot_write_to_it_again()
    {
        var storage = new FlakyStorage(new InMemoryCheckpointStorageProvider());
        var table = NewCounts();
        var first = BuildCounts(table, storage, name: "first");
        var crashed = first.Builder.Build();
        crashed.Start();
        storage.FailCommits = true;
        await Assert.ThrowsAsync<IOException>(() => crashed.StepAsync());
        storage.FailCommits = false;

        // Free for an adapter, or for the circuit's replacement.
        await table.Writer.CommitAsync();
        var second = BuildCounts(table, storage, name: "second").Builder.Build();
        Assert.Throws<InvalidOperationException>(() => { _ = table.Writer.DeleteAsync(1); });
        await DisposeBounded(second);
        await DisposeBounded(crashed);
    }

    [Fact]
    public async Task A_write_in_flight_when_its_circuit_gave_the_table_up_is_refused_at_the_gate()
    {
        var table = NewCounts();
        var claim = TableLineage.Claim("circuit 'gone'", [], [table])[0];
        await table.Writer.EnterAsync(claim, CancellationToken.None);
        table.Writer.Exit();

        TableLineage.Release(table, claim);
        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => table.Writer.EnterAsync(claim, CancellationToken.None).AsTask());
        Assert.Contains("no longer holds the writer", error.Message);

        // The refusal did not keep the gate.
        await table.Writer.UpsertAsync(new GroupCount(1, 1));
        Assert.Equal(1, await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout));
    }

    // ── backpressure ───────────────────────────────────────────────────────────────────

    [Fact]
    public async Task A_reader_that_lags_a_full_tail_stalls_the_writing_circuit_and_its_producers_until_it_steps()
    {
        const int total = 12;
        var table = NewCounts(new SharedTableOptions { TailVersions = 2 });

        // The reader is attached and not stepping; with no log the table has nowhere else
        // to keep the versions it has not read.
        var readerBuilder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var rowsDef = readerBuilder.Table(table).Changes().ToOutput();
        await using var reader = readerBuilder.Build();
        reader.Start();

        // The writer steps once per item, so each item is a version.
        var pipeline = BuildCounts(table, autoStep: true, inputCapacity: 1, maxStepItems: 1);
        var writer = pipeline.Builder.Build();
        writer.Start();
        var items = writer.Handle(pipeline.Items);
        var producer = Task.Run(async () =>
        {
            for (var n = 0; n < total; n++)
                await items.UpsertAsync(new Item(n, n, 0));
        });

        await Eventually(() => table.LatestVersion == 2, "the tail to fill");
        await Task.Delay(Settle * 2);
        Assert.Equal(2, table.LatestVersion);
        Assert.False(producer.IsCompleted, "the producer was not held back");
        Assert.Null(writer.Fault);

        // Each step of the reader lets the writer on by what the tail can hold again.
        var seen = new ZSet<GroupCount>();
        var deadline = DateTime.UtcNow + Timeout;
        while (table.LatestVersion < total)
        {
            Assert.True(DateTime.UtcNow < deadline, $"Timed out with the table at version {table.LatestVersion}.");
            var before = table.LatestVersion;
            await reader.StepAsync().WaitAsync(Timeout);
            seen.AddAll(Drain(reader.Handle(rowsDef)));
            await Eventually(() => table.LatestVersion > before || table.LatestVersion == total, "the writer to move on");
            Assert.True(table.LatestVersion - before <= 3, $"the writer ran {table.LatestVersion - before} versions past a stalled reader");
        }
        await producer.WaitAsync(Timeout);
        await DisposeBounded(writer);

        await reader.StepAsync().WaitAsync(Timeout);
        seen.AddAll(Drain(reader.Handle(rowsDef)));
        Assert.Equal(total, seen.DistinctCount);
        Assert.All(Enumerable.Range(0, total), n => Assert.Equal(1, seen.WeightOf(new GroupCount(n, 1))));
        Assert.Equal(total, table.Count);
    }

    // ── what a circuit leaves behind ───────────────────────────────────────────────────

    [Fact]
    public async Task Direct_writes_after_the_writing_circuit_is_gone_do_not_pile_up_in_the_tail()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var table = NewCounts(new SharedTableOptions { TailVersions = 4 });
        var first = BuildCounts(table, storage);
        var circuit = first.Builder.Build();
        circuit.Start();
        await circuit.Handle(first.Items).UpsertAsync(new Item(1, 1, 0), 0);
        await circuit.StepAsync().WaitAsync(Timeout);
        await DisposeBounded(circuit);
        var checkpointed = table.LatestVersion;

        // Nothing will ever checkpoint past that version again: the tail must not wait for it.
        for (var n = 0; n < 200; n++)
        {
            await table.Writer.UpsertAsync(new GroupCount(100 + n, 1));
            await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
        }
        Assert.Equal(checkpointed + 200, table.LatestVersion);
        Assert.True(table.TailCount <= 5, $"the tail holds {table.TailCount} versions for a circuit that is gone");

        // The circuit's checkpoint is still there, and what it would have to undo is not:
        // resuming is refused, not done wrongly.
        var again = BuildCounts(table, storage).Builder.Build();
        Assert.Contains("no longer retained", Assert.Throws<InvalidOperationException>(again.Start).Message);
        await DisposeBounded(again);
    }

    [Fact]
    public async Task A_circuit_being_built_keeps_its_predecessors_hold_through_a_direct_commit_already_under_way()
    {
        // The hold belongs to whoever holds the writer: only a write with no circuit on
        // the table ends it.
        var storage = new FlakyStorage(new InMemoryCheckpointStorageProvider());
        var table = NewCounts(new SharedTableOptions { TailVersions = 2 });
        var first = BuildCounts(table, storage, interval: 6);
        var crashed = first.Builder.Build();
        crashed.Start();
        var items = crashed.Handle(first.Items);
        for (var n = 0; n < 6; n++)
        {
            await items.UpsertAsync(new Item(n, n, 0), n);
            await crashed.StepAsync().WaitAsync(Timeout);
        }
        storage.FailCommits = true;
        for (var n = 6; n < 11; n++)
        {
            await items.UpsertAsync(new Item(n, n, 0), n);
            await crashed.StepAsync().WaitAsync(Timeout);
        }
        await items.UpsertAsync(new Item(11, 11, 0), 11);
        await Assert.ThrowsAsync<IOException>(() => crashed.StepAsync().WaitAsync(Timeout));
        storage.FailCommits = false;

        // The replacement holds the writer from Build(), and its hold from Start(). A
        // direct commit that got past the ownership check just before then reaches this
        // call afterwards, and must leave the hold alone.
        var second = BuildCounts(table, storage, interval: 6);
        await using var resumed = second.Builder.Build();
        resumed.Start();
        table.EndWriterHoldForDirectWrite();
        await resumed.StepAsync().WaitAsync(Timeout);
        AssertRows(Enumerable.Range(0, 6).ToDictionary(n => n, n => new GroupCount(n, 1)), table, "after the restart's first step");

        items = resumed.Handle(second.Items);
        for (var n = 6; n < 10; n++) // four more versions, none checkpointed
        {
            await items.UpsertAsync(new Item(n, n, 0), n);
            await resumed.StepAsync().WaitAsync(Timeout);
        }
        Assert.True(table.TailCount >= 5, $"the tail holds only {table.TailCount} versions; a restart needs five");
    }

    [Fact]
    public async Task A_step_the_table_rejects_leaves_nothing_staged_for_the_next_writer()
    {
        var table = new SharedTable<int, Grp>("groups", g => g.Id);
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var rows = builder.Input<Grp>("rows");
        rows.ToTable(table);
        var circuit = builder.Build();
        circuit.Start();
        await circuit.Handle(rows).InsertAsync(new Grp(1, 1));
        await circuit.StepAsync().WaitAsync(Timeout);

        // Two good rows and one that is not table-shaped: the step is rejected whole.
        await circuit.Handle(rows).InsertAsync(new Grp(2, 5));
        await circuit.Handle(rows).InsertAsync(new Grp(3, 5));
        await circuit.Handle(rows).InsertAsync(new Grp(1, 2));
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => circuit.StepAsync().WaitAsync(Timeout));
        Assert.Contains("is still there", error.Message);
        AssertRows(new Dictionary<int, Grp> { [1] = new(1, 1) }, table, "after the rejected step");
        await DisposeBounded(circuit);

        await table.Writer.UpsertAsync(new Grp(9, 9));
        await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
        AssertRows(new Dictionary<int, Grp> { [1] = new(1, 1), [9] = new(9, 9) }, table, "after the next direct commit");
    }

    [Fact]
    public async Task An_idle_auto_stepping_circuit_faults_when_its_table_write_fails()
    {
        // No checkpointing and no further input: nothing but the pump is left to notice.
        var log = new TestLog<int, GroupCount>();
        var table = await SharedTable<int, GroupCount>.OpenAsync("counts", c => c.Group, log);
        var pipeline = BuildCounts(table, autoStep: true);
        var circuit = pipeline.Builder.Build();
        circuit.Start();
        await circuit.Handle(pipeline.Items).UpsertAsync(new Item(1, 1, 0));
        await Eventually(() => table.Count == 1, "the first row");

        log.FailAppends = true;
        await circuit.Handle(pipeline.Items).UpsertAsync(new Item(2, 2, 0));
        var fault = await Assert.ThrowsAsync<IOException>(() => circuit.Completion.WaitAsync(Timeout));
        Assert.Same(fault, circuit.Fault);
        Assert.Equal(1, table.Count);
        await DisposeBounded(circuit);
    }

    // ── guard rails ────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task A_build_that_fails_after_claiming_the_table_gives_it_back()
    {
        var table = NewCounts();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, ExchangeTimeout = TimeSpan.Zero });
        builder.KeyedInput<int, Item>("items", i => i.Id)
            .GroupBy(i => i.Group).Count().ToTable(table, (group, count) => new GroupCount(group, count));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.Build());

        await table.Writer.UpsertAsync(new GroupCount(1, 1));
        Assert.Equal(1, await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout));
        await using var next = BuildCounts(table).Builder.Build();
    }

    [Fact]
    public async Task A_start_refused_by_the_table_it_writes_leaves_the_tables_it_reads_unattached()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var source = new SharedTable<int, Grp>("source", g => g.Id, new SharedTableOptions { TailVersions = 2 });

        CircuitBuilder Build(SharedTable<int, Grp> target)
        {
            var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
            builder.Table(source).Changes().ToTable(target);
            return builder;
        }

        var log = new TestLog<int, Grp>();
        var first = Build(await SharedTable<int, Grp>.OpenAsync("copy", g => g.Id, log)).Build();
        first.Start();
        await source.Writer.UpsertAsync(new Grp(1, 1));
        await source.Writer.CommitAsync();
        await first.StepAsync().WaitAsync(Timeout);
        await DisposeBounded(first);

        // The table comes back behind the checkpoint, so the circuit is refused — after it
        // has attached to the table it reads.
        log.RestoreAtVersion = 0;
        var refused = Build(await SharedTable<int, Grp>.OpenAsync("copy", g => g.Id, log)).Build();
        Assert.Contains("lost history", Assert.Throws<InvalidOperationException>(refused.Start).Message);

        // An attached circuit that never steps would stall the source's writer a tail on.
        for (var n = 0; n < 10; n++)
        {
            await source.Writer.UpsertAsync(new Grp(1, 2 + n));
            await source.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
        }
        Assert.True(source.TailCount <= 3, $"the source's tail holds {source.TailCount} versions for a circuit that never started");
        await DisposeBounded(refused);
    }

    [Fact]
    public void A_circuit_cannot_read_one_table_and_write_another_of_the_same_name()
    {
        var read = new SharedTable<int, Grp>("groups", g => g.Id);
        var written = new SharedTable<int, Grp>("groups", g => g.Id);
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        builder.Table(read).Changes().Select(g => new Grp(g.Id, g.Val + 1)).ToTable(written);
        var error = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains("reads one table named 'groups' and writes another", error.Message);

        // Nothing was claimed by the build that failed.
        Assert.Null(((ITableNode)written).WriterClaim);
    }

    [Fact]
    public async Task A_row_whose_table_key_is_null_faults_the_circuit_and_says_which()
    {
        var table = new SharedTable<string, Trader>("traders", t => t.Name);
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var rows = builder.Input<Trader>("rows");
        rows.ToTable(table);
        await using var circuit = builder.Build();
        circuit.Start();
        await circuit.Handle(rows).InsertAsync(new Trader(1, "ann", 1));
        await circuit.Handle(rows).InsertAsync(new Trader(2, null!, 1));
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => circuit.StepAsync().WaitAsync(Timeout));
        Assert.Contains("key selector of table 'traders' returned null", error.Message);
        Assert.Equal(0, table.Count);
    }

    /// <summary>Commits can be held at the door: the caller waits inside the commit.</summary>
    private sealed class GatedStorage(InMemoryCheckpointStorageProvider inner) : ICheckpointStorageProvider
    {
        public volatile TaskCompletionSource? Gate;
        public readonly TaskCompletionSource Entered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull => inner.CreateWeightStore<T>(name);

        public IIndexedWeightStore<TKey, TValue> CreateIndexedWeightStore<TKey, TValue>(string name)
            where TKey : notnull
            where TValue : notnull => inner.CreateIndexedWeightStore<TKey, TValue>(name);

        public IValueStore<TKey, TValue> CreateValueStore<TKey, TValue>(string name) where TKey : notnull =>
            inner.CreateValueStore<TKey, TValue>(name);

        public IReadOnlyDictionary<string, long> Recover() => inner.Recover();

        public void Commit(IReadOnlyDictionary<string, long> inputOffsets)
        {
            if (Gate is { } gate)
            {
                Entered.TrySetResult();
                gate.Task.Wait();
            }
            inner.Commit(inputOffsets);
        }

        public string? CommittedManifest(string scope) => inner.CommittedManifest(scope);

        public void StageManifest(string scope, string manifest) => inner.StageManifest(scope, manifest);
    }

    [Fact]
    public async Task A_disposed_circuits_last_checkpoint_does_not_take_the_tail_hold_from_its_successor()
    {
        // The first circuit has three steps uncommitted when it is disposed. Disposal gives
        // the table up and then commits them; the commit is held while a second circuit —
        // one that keeps no checkpoint, so needs nothing held — takes the table over.
        var storage = new GatedStorage(new InMemoryCheckpointStorageProvider());
        var table = NewCounts(new SharedTableOptions { TailVersions = 2 });
        var first = BuildCounts(table, storage, interval: 100);
        var leaving = first.Builder.Build();
        leaving.Start();
        for (var n = 0; n < 3; n++)
        {
            await leaving.Handle(first.Items).UpsertAsync(new Item(n, n, 0), n);
            await leaving.StepAsync().WaitAsync(Timeout);
        }
        storage.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var disposal = Task.Run(async () => await leaving.DisposeAsync());
        await storage.Entered.Task.WaitAsync(Timeout);

        var second = BuildCounts(table);
        await using var successor = second.Builder.Build();
        successor.Start();
        await successor.StepAsync().WaitAsync(Timeout);
        storage.Gate.SetResult();
        await disposal.WaitAsync(Timeout);

        for (var n = 0; n < 10; n++)
        {
            await successor.Handle(second.Items).UpsertAsync(new Item(n, n, 0));
            await successor.StepAsync().WaitAsync(Timeout);
        }
        Assert.True(table.TailCount <= 3, $"the tail holds {table.TailCount} versions for a checkpoint that is not this circuit's");
    }

    [Fact]
    public async Task A_table_port_does_not_acknowledge_a_step_it_never_wrote_or_a_circuit_that_failed()
    {
        var table = NewCounts();
        var claim = TableLineage.Claim("circuit 'test'", [], [table])[0];
        var port = new DerivedTablePort<int, GroupCount>("out0", 0, table, claim);
        await port.WaitAcknowledgedAsync().WaitAsync(Timeout);

        // Staged — the first step's reconciliation is owed — and not yet flushed.
        port.Open(resuming: false, committedVersion: null, checkpointing: true);
        Assert.True(port.Stage([], 1));
        var unwritten = await Assert.ThrowsAsync<InvalidOperationException>(() => port.WaitAcknowledgedAsync().WaitAsync(Timeout));
        Assert.Contains("never written", unwritten.Message);

        await port.FlushStagedAsync(CancellationToken.None);
        await port.WaitAcknowledgedAsync().WaitAsync(Timeout);

        port.Complete(new IOException("the circuit faulted"));
        await Assert.ThrowsAsync<IOException>(() => port.WaitAcknowledgedAsync().WaitAsync(Timeout));
    }
}
