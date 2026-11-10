using CsCheck;
using Incremental.Tables;

namespace Incremental.Tests;

/// <summary>
/// Secondary indexes of a shared table (<c>docs/design/shared-tables.md</c>, <em>Secondary
/// indexes</em>): rows found by a column other than the key, any number of rows per value,
/// versioned with the rows. The differential properties pin a join through an index to the
/// existing stream join over a keyed input grouped by that column; the directed tests cover
/// declaration and the writer's side; the stress test reads an index under a churning writer.
/// </summary>
public class TableIndexTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);
    private const string None = "(none)";

    private sealed record Listing(int Id, string? Symbol, string Venue);

    private static SharedTable<int, Listing> NewTable(SharedTableOptions? options = null) =>
        new("listings", l => l.Id, options);

    private static async Task<long> Commit(SharedTable<int, Listing> table, params Listing[] rows)
    {
        foreach (var row in rows)
            await table.Writer.UpsertAsync(row);
        return await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
    }

    private static async Task<long> Delete(SharedTable<int, Listing> table, params int[] keys)
    {
        foreach (var key in keys)
            await table.Writer.DeleteAsync(key);
        return await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
    }

    /// <summary>Quotes joined to every listing of their symbol, as (quote id, venue).</summary>
    private sealed record Pipeline(
        CircuitBuilder Builder,
        InputStream<(int Id, string Symbol)> Quotes,
        Output<(int Quote, string Venue)> Listed);

    private static Pipeline Build(SharedTable<int, Listing> table, bool left = false, int workers = 1)
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = workers });
        var bySymbol = builder.Table(table).By("symbol", l => l.Symbol);
        var quotes = builder.Input<(int Id, string Symbol)>("quotes");
        var grouped = quotes.GroupBy(q => q.Symbol);
        var listed = left
            ? grouped.LeftJoin(bySymbol, (_, quote, listing) => (quote.Id, listing.Venue), (_, quote) => (quote.Id, None))
            : grouped.Join(bySymbol, (_, quote, listing) => (quote.Id, listing.Venue));
        return new Pipeline(builder, quotes, listed.ToOutput());
    }

    private static ZSet<T> Drain<T>(OutputHandle<T> output) where T : notnull
    {
        var drained = new ZSet<T>();
        while (output.Reader.TryRead(out var delta))
            drained.AddAll(delta);
        return drained;
    }

    private static void AssertDelta<T>(ZSet<T> actual, params (T Value, long Weight)[] expected) where T : notnull
    {
        var want = new ZSet<T>();
        foreach (var (value, weight) in expected)
            want.Add(value, weight);
        Assert.True(
            want.DistinctCount == actual.DistinctCount && want.All(e => actual.WeightOf(e.Value) == e.Weight),
            $"expected {want} but got {actual}");
    }

    // ── differential: the index join against the stream join on that column ─────────────

    private sealed record Row(int Key, int Group, int Val);

    private enum Kind { InsertFact, RetractFact, UpsertRow, DeleteRow }

    /// <summary>One generated command; for a row, <c>Alt</c> picks both its group and its
    /// payload, so an upsert may move a row between groups, change it in place, or neither.</summary>
    private static readonly Gen<List<(int Kind, int Key, int Alt, bool NewStep, bool Commit)>> GenCommands =
        Gen.Select(Gen.Int[0, 3], Gen.Int[0, 4], Gen.Int[0, 5], Gen.Bool, Gen.Bool).List[1, 60];

    /// <summary>Runs one circuit that joins the same facts twice on a column that is not the
    /// rows' key — through the table's index, and against a keyed input fed the table's
    /// commands and grouped by that column — and asserts after every step that the two
    /// integrated outputs are equal.</summary>
    private static void AssertMatchesStreamJoin(
        List<(int Kind, int Key, int Alt, bool NewStep, bool Commit)> commands, bool left)
    {
        RunAsync().GetAwaiter().GetResult();
        return;

        async Task RunAsync()
        {
            foreach (var workers in new[] { 1, 4 })
            {
                var table = new SharedTable<int, Row>("reference", r => r.Key);
                var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = workers });
                var byGroup = builder.Table(table).By("group", r => r.Group);
                var factsDef = builder.Input<(int Group, int Val)>("facts");
                var rowsDef = builder.KeyedInput<int, Row>("rows", r => r.Key);
                var viaIndexDef = (left
                    ? factsDef.GroupBy(f => f.Group).LeftJoin(byGroup, (g, f, r) => (g, f.Val, r.Key, r.Val), (g, f) => (g, f.Val, -1, -1))
                    : factsDef.GroupBy(f => f.Group).Join(byGroup, (g, f, r) => (g, f.Val, r.Key, r.Val))).ToOutput();
                var rowsByGroup = rowsDef.GroupBy(r => r.Group);
                var viaStreamDef = (left
                    ? factsDef.GroupBy(f => f.Group).LeftJoin(rowsByGroup, (g, f, r) => (g, f.Val, r.Key, r.Val), (g, f) => (g, f.Val, -1, -1))
                    : factsDef.GroupBy(f => f.Group).Join(rowsByGroup, (g, f, r) => (g, f.Val, r.Key, r.Val))).ToOutput();

                await using var circuit = builder.Build();
                circuit.Start();
                var facts = circuit.Handle(factsDef);
                var rows = circuit.Handle(rowsDef);
                var viaIndex = new ZSet<(int, int, int, int)>();
                var viaStream = new ZSet<(int, int, int, int)>();
                var step = 0;

                async Task StepAndCompare()
                {
                    await table.Writer.CommitAsync();
                    await circuit.StepAsync();
                    viaIndex.AddAll(Drain(circuit.Handle(viaIndexDef)));
                    viaStream.AddAll(Drain(circuit.Handle(viaStreamDef)));
                    var equal = viaIndex.DistinctCount == viaStream.DistinctCount
                        && viaStream.All(e => viaIndex.WeightOf(e.Value) == e.Weight);
                    if (!equal)
                        throw new Exception(
                            $"differential mismatch (workers={workers}, left={left}, after step {step}): " +
                            $"the stream join gives {viaStream} but the index join gives {viaIndex}");
                    step++;
                }

                var pending = false;
                foreach (var command in commands)
                {
                    if (command.NewStep && pending)
                    {
                        await StepAndCompare();
                        pending = false;
                    }
                    switch ((Kind)command.Kind)
                    {
                        case Kind.InsertFact:
                            await facts.SendAsync((command.Alt % 3, command.Key), 1);
                            break;
                        case Kind.RetractFact:
                            await facts.SendAsync((command.Alt % 3, command.Key), -1);
                            break;
                        case Kind.UpsertRow:
                            var row = new Row(command.Key, command.Alt % 3, command.Alt / 3);
                            await table.Writer.UpsertAsync(row);
                            await rows.UpsertAsync(row);
                            break;
                        default:
                            await table.Writer.DeleteAsync(command.Key);
                            await rows.DeleteAsync(command.Key);
                            break;
                    }
                    if (command.Commit && (Kind)command.Kind is Kind.UpsertRow or Kind.DeleteRow)
                        await table.Writer.CommitAsync();
                    pending = true;
                }
                await StepAndCompare();
            }
        }
    }

    [Fact]
    public void A_join_through_an_index_matches_the_stream_join_on_a_grouping_of_that_column() =>
        GenCommands.Sample(commands => AssertMatchesStreamJoin(commands, left: false));

    [Fact]
    public void A_left_join_through_an_index_matches_the_stream_left_join_on_a_grouping_of_that_column() =>
        GenCommands.Sample(commands => AssertMatchesStreamJoin(commands, left: true));

    // ── directed: joins ────────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task A_fact_joins_every_row_under_its_index_key_and_follows_rows_between_keys(int workers)
    {
        var table = NewTable();
        await Commit(table, new Listing(1, "ACME", "NYSE"), new Listing(2, "ACME", "LSE"), new Listing(3, "BOLT", "NYSE"));
        var pipeline = Build(table, workers: workers);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var quotes = circuit.Handle(pipeline.Quotes);
        var listed = circuit.Handle(pipeline.Listed);

        await quotes.InsertAsync((1, "ACME"));
        await quotes.InsertAsync((2, "BOLT"));
        await circuit.StepAsync();
        AssertDelta(Drain(listed), ((1, "NYSE"), 1), ((1, "LSE"), 1), ((2, "NYSE"), 1));

        // A row changed in place stays in its group; the facts joined to it are corrected.
        await Commit(table, new Listing(2, "ACME", "LSE-AIM"));
        await circuit.StepAsync();
        AssertDelta(Drain(listed), ((1, "LSE"), -1), ((1, "LSE-AIM"), 1));

        // A row moved to another symbol leaves one key's facts and reaches the other's.
        await Commit(table, new Listing(1, "BOLT", "NASDAQ"));
        await circuit.StepAsync();
        AssertDelta(Drain(listed), ((1, "NYSE"), -1), ((2, "NASDAQ"), 1));

        await Delete(table, 3);
        await circuit.StepAsync();
        AssertDelta(Drain(listed), ((2, "NYSE"), -1));
    }

    [Fact]
    public async Task A_step_reads_an_index_as_of_its_pinned_version_groups_and_rows_alike()
    {
        var table = NewTable();
        await Commit(table, new Listing(1, "ACME", "NYSE"), new Listing(2, "BOLT", "LSE"));

        // Every worker parks mid-step, before anything has probed, until the table has
        // moved on underneath it.
        using var entered = new ManualResetEventSlim();
        using var resume = new ManualResetEventSlim();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = 3 });
        var bySymbol = builder.Table(table).By("symbol", l => l.Symbol);
        var quotesDef = builder.Input<(int Id, string Symbol)>("quotes");
        var parked = quotesDef.Select(quote =>
        {
            entered.Set();
            resume.Wait(Timeout);
            return quote;
        });
        var joinedDef = parked.GroupBy(q => q.Symbol).Join(bySymbol, (_, quote, listing) => (quote.Id, listing.Venue)).ToOutput();
        var lookedUpDef = parked.Lookup(bySymbol, q => q.Symbol, (quote, listing) => (quote.Id, listing.Venue)).ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var quotes = circuit.Handle(quotesDef);
        await quotes.InsertAsync((1, "ACME"));
        await quotes.InsertAsync((2, "BOLT"));

        // Listing 1 moves from ACME's group to BOLT's while the step is in flight. A probe
        // that took the groups from the newer version and the rows from the pinned one would
        // find nothing under ACME and both listings under BOLT.
        var step = circuit.StepAsync();
        Assert.True(entered.Wait(Timeout));
        await Commit(table, new Listing(1, "BOLT", "NASDAQ"));
        resume.Set();
        await step.WaitAsync(Timeout);
        AssertDelta(Drain(circuit.Handle(joinedDef)), ((1, "NYSE"), 1), ((2, "LSE"), 1));
        AssertDelta(Drain(circuit.Handle(lookedUpDef)), ((1, "NYSE"), 1), ((2, "LSE"), 1));

        // The next step pins the new version: the join follows the move, the lookup does not.
        await circuit.StepAsync().WaitAsync(Timeout);
        AssertDelta(Drain(circuit.Handle(joinedDef)), ((1, "NYSE"), -1), ((2, "NASDAQ"), 1));
        AssertDelta(Drain(circuit.Handle(lookedUpDef)));
    }

    [Fact]
    public async Task A_left_join_through_an_index_shows_the_unmatched_row_only_while_the_key_has_no_rows()
    {
        var table = NewTable();
        var pipeline = Build(table, left: true, workers: 2);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var quotes = circuit.Handle(pipeline.Quotes);
        var listed = circuit.Handle(pipeline.Listed);

        await quotes.InsertAsync((1, "ACME"));
        await circuit.StepAsync();
        AssertDelta(Drain(listed), ((1, None), 1));

        // The first row under the key retracts the unmatched row; the second only adds a match.
        await Commit(table, new Listing(1, "ACME", "NYSE"));
        await circuit.StepAsync();
        AssertDelta(Drain(listed), ((1, None), -1), ((1, "NYSE"), 1));
        await Commit(table, new Listing(2, "ACME", "LSE"));
        await circuit.StepAsync();
        AssertDelta(Drain(listed), ((1, "LSE"), 1));

        // Losing one of two rows is not losing them all.
        await Delete(table, 1);
        await circuit.StepAsync();
        AssertDelta(Drain(listed), ((1, "NYSE"), -1));

        // The last row is replaced in the same step: the key never has no rows.
        await table.Writer.DeleteAsync(2);
        await table.Writer.UpsertAsync(new Listing(3, "ACME", "XETRA"));
        await table.Writer.CommitAsync();
        await circuit.StepAsync();
        AssertDelta(Drain(listed), ((1, "LSE"), -1), ((1, "XETRA"), 1));

        // The last row leaves by being re-keyed, which is as good as deleted for this key.
        await Commit(table, new Listing(3, "BOLT", "XETRA"));
        await circuit.StepAsync();
        AssertDelta(Drain(listed), ((1, "XETRA"), -1), ((1, None), 1));
    }

    // ── directed: declaration and the writer's side ────────────────────────────────────

    [Fact]
    public async Task An_index_declared_after_the_table_is_loaded_covers_the_rows_already_there_and_those_that_follow()
    {
        const int rows = 500;
        var table = NewTable();
        for (var id = 0; id < rows; id++)
            await table.Writer.UpsertAsync(new Listing(id, "S" + id % 50, "V" + id));
        await table.Writer.CommitAsync();
        await Delete(table, 0);

        // No index exists until this pipeline asks for one.
        var pipeline = Build(table, workers: 3);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var quotes = circuit.Handle(pipeline.Quotes);
        var listed = circuit.Handle(pipeline.Listed);

        await quotes.InsertAsync((1, "S0"));
        await quotes.InsertAsync((2, "S7"));
        await circuit.StepAsync();
        AssertDelta(
            Drain(listed),
            Enumerable.Range(1, rows / 50 - 1).Select(i => ((1, "V" + i * 50), 1L))
                .Concat(Enumerable.Range(0, rows / 50).Select(i => ((2, "V" + (i * 50 + 7)), 1L)))
                .ToArray());

        await Commit(table, new Listing(rows, "S7", "LATE"));
        await Delete(table, 7);
        await circuit.StepAsync();
        AssertDelta(Drain(listed), ((2, "LATE"), 1), ((2, "V7"), -1));
    }

    [Fact]
    public async Task Circuits_asking_for_the_same_index_name_share_one_index_and_a_different_key_type_is_refused()
    {
        var table = NewTable();
        table.AddIndex("symbol", (Listing l) => l.Symbol);
        var first = table.Index("symbol", (Listing l) => l.Symbol);

        // The second selector is not compared, and not used: the name is the identity.
        var second = table.Index("symbol", (Listing l) => l.Venue);
        Assert.Same(first, second);
        await Commit(table, new Listing(1, "ACME", "NYSE"));
        using (var snapshot = table.Snapshot())
        {
            Assert.Equal([1], first.Read("ACME", snapshot.Version)!);
            Assert.Null(first.Read("NYSE", snapshot.Version));
        }

        var wrongType = Assert.Throws<ArgumentException>(() => table.AddIndex("symbol", (Listing l) => l.Id));
        Assert.Contains("already has an index named 'symbol' with a different key type", wrongType.Message);
    }

    [Fact]
    public async Task A_row_whose_index_key_is_null_is_under_no_key()
    {
        var table = NewTable();
        await Commit(table, new Listing(1, null, "DARK"), new Listing(2, "ACME", "NYSE"));
        var pipeline = Build(table, left: true);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var quotes = circuit.Handle(pipeline.Quotes);
        var listed = circuit.Handle(pipeline.Listed);

        await quotes.InsertAsync((1, "ACME"));
        await circuit.StepAsync();
        AssertDelta(Drain(listed), ((1, "NYSE"), 1));

        // Gaining a symbol puts the row under it; losing it again takes the row out.
        await Commit(table, new Listing(1, "ACME", "DARK"));
        await circuit.StepAsync();
        AssertDelta(Drain(listed), ((1, "DARK"), 1));
        await Commit(table, new Listing(1, null, "DARK"), new Listing(2, null, "NYSE"));
        await circuit.StepAsync();
        AssertDelta(Drain(listed), ((1, "DARK"), -1), ((1, "NYSE"), -1), ((1, None), 1));
    }

    [Fact]
    public async Task An_index_group_keeps_superseded_versions_only_while_a_snapshot_can_read_them()
    {
        var table = NewTable();
        var index = table.Index("symbol", (Listing l) => l.Symbol);
        await Commit(table, new Listing(1, "ACME", "NYSE"));

        var held = table.Snapshot();
        await Commit(table, new Listing(2, "ACME", "LSE"));
        await Commit(table, new Listing(3, "ACME", "XETRA"));
        await Delete(table, 1);

        // The held snapshot still reads the group, and through it the rows, as they were.
        Assert.Equal([1], index.Read("ACME", held.Version)!);
        Assert.True(index.ChainLength("ACME") > 1);
        using (var latest = table.Snapshot())
            Assert.Equal([2, 3], index.Read("ACME", latest.Version)!.Order());

        held.Dispose();
        table.Writer.Trim();
        Assert.Equal(1, index.ChainLength("ACME"));

        // A group that empties goes the way of a deleted row: nothing is left behind.
        await Delete(table, 2, 3);
        table.Writer.Trim();
        Assert.Equal(0, index.ChainLength("ACME"));
    }

    [Fact]
    public async Task A_selector_that_throws_faults_the_writer_before_anything_is_published()
    {
        var log = new InMemoryTableLog<int, Listing>();
        var table = await SharedTable<int, Listing>.OpenAsync("listings", l => l.Id, log);
        table.AddIndex("symbol", (Listing l) => l.Venue == "BAD" ? throw new FormatException("no symbol") : l.Symbol);
        await Commit(table, new Listing(1, "ACME", "NYSE"));

        await table.Writer.UpsertAsync(new Listing(2, "ACME", "BAD"));
        await Assert.ThrowsAsync<FormatException>(() => table.Writer.CommitAsync().AsTask());

        // Neither the table nor its log saw the version, and the writer says why it stopped.
        Assert.Equal(1, table.LatestVersion);
        Assert.Equal(1, (await log.RestoreAsync((_, _) => { })).Version);
        var refused = await Assert.ThrowsAsync<InvalidOperationException>(
            () => table.Writer.UpsertAsync(new Listing(3, "ACME", "LSE")).AsTask());
        Assert.Contains("a secondary index's selector threw", refused.Message);
    }

    // ── concurrency ────────────────────────────────────────────────────────────────────

    /// <summary>Every version rewrites every row, moving it to another index group, and
    /// deletes a rotating few; what the table holds at version <c>v</c> is therefore a pure
    /// function of <c>v</c>. Readers pin a version while the writer churns and check every
    /// group against that function: the exact primary keys under it, each with a row of
    /// exactly that version. A group read from another version than the rows, a group seen
    /// half-updated, or a chain trimmed under a pin all show up as a wrong member.</summary>
    [Fact]
    public async Task Index_readers_always_see_exactly_their_pinned_version_under_a_churning_writer()
    {
        const int keys = 60;
        const int groups = 7;
        const int versions = 1500;
        const int minChecked = 2000;
        // No revocation: a reader descheduled mid-check must find its version still there.
        var table = new SharedTable<int, Stamped>(
            "stamped", r => r.Key, new SharedTableOptions { MaxRetainedVersions = int.MaxValue });
        var index = table.Index("group", (Stamped r) => r.Group);

        static bool Present(int key, long version) => (key + version) % 11 != 0;
        static int GroupOf(int key, long version) => (int)((key + version) % groups);

        using var stop = new CancellationTokenSource();
        var checkedGroups = 0L;
        var readers = Enumerable.Range(0, 4).Select(reader => Task.Run(() =>
        {
            var random = new Random(reader);
            while (!stop.IsCancellationRequested)
            {
                using var snapshot = table.Snapshot();
                var version = snapshot.Version;
                if (version == 0)
                    continue;
                for (var probe = 0; probe < 8; probe++)
                {
                    var group = random.Next(groups);
                    var expected = Enumerable.Range(0, keys)
                        .Where(key => Present(key, version) && GroupOf(key, version) == group)
                        .ToArray();
                    var actual = index.Read(group, version) ?? [];
                    table.ThrowIfRevoked(version);
                    if (!expected.SequenceEqual(actual.Order()))
                        throw new Exception(
                            $"at version {version} group {group} should hold [{string.Join(",", expected)}] " +
                            $"but the index gives [{string.Join(",", actual.Order())}]");
                    foreach (var key in actual)
                        if (!snapshot.TryGet(key, out var row) || row.Version != version || row.Group != group)
                            throw new Exception($"at version {version} group {group} lists key {key}, whose row is {row}");
                    Interlocked.Increment(ref checkedGroups);
                }
            }
        })).ToArray();

        // The writer keeps going until the readers have had a real look, however slowly the
        // thread pool gets round to starting them on a busy machine.
        var deadline = DateTime.UtcNow + Timeout;
        var version = 0L;
        while ((version < versions || Interlocked.Read(ref checkedGroups) < minChecked)
            && !readers.Any(r => r.IsFaulted) && DateTime.UtcNow < deadline)
        {
            version++;
            for (var key = 0; key < keys; key++)
            {
                if (Present(key, version))
                    await table.Writer.UpsertAsync(new Stamped(key, GroupOf(key, version), version));
                else
                    await table.Writer.DeleteAsync(key);
            }
            Assert.Equal(version, await table.Writer.CommitAsync());
        }
        stop.Cancel();
        await Task.WhenAll(readers).WaitAsync(Timeout);
        Assert.True(Interlocked.Read(ref checkedGroups) >= minChecked, "the readers barely ran");

        // Idle again: every group is back to one version.
        table.Writer.Trim();
        Assert.Equal(0, table.PinCount);
        for (var group = 0; group < groups; group++)
            Assert.True(index.ChainLength(group) <= 1, $"group {group} kept {index.ChainLength(group)} versions");
    }

    private sealed record Stamped(int Key, int Group, long Version);

    [Fact]
    public async Task Probing_a_table_or_an_index_allocates_nothing()
    {
        const int rows = 4096;
        var table = new SharedTable<int, Stamped>("stamped", r => r.Key);
        var index = table.Index("group", (Stamped r) => r.Group);
        for (var key = 0; key < rows; key++)
            await table.Writer.UpsertAsync(new Stamped(key, key / 4, 1));
        await table.Writer.CommitAsync();

        var probe = new TableProbe<int, Stamped>(table);
        var byGroup = new TableIndexProbe<int, int, Stamped>(table, index, probe);
        var keys = Enumerable.Range(0, 512).Select(i => i * 7 % (rows / 4)).ToList();
        var found = new List<Stamped>(4 * keys.Count);
        var bounds = new List<int>(keys.Count);

        probe.Version = table.AcquirePin();
        try
        {
            long Measure(TableGroupProbe<int, Stamped> target)
            {
                // Once to size the buffers and settle the JIT, then measured.
                for (var round = 0; round < 3; round++)
                {
                    found.Clear();
                    bounds.Clear();
                    target.GetGroupsInto(keys, found, bounds);
                }
                var before = GC.GetAllocatedBytesForCurrentThread();
                for (var round = 0; round < 50; round++)
                {
                    found.Clear();
                    bounds.Clear();
                    target.GetGroupsInto(keys, found, bounds);
                }
                return GC.GetAllocatedBytesForCurrentThread() - before;
            }

            // The least of a few tries: a collection started by another test's threads
            // while this one measures retires this thread's allocation buffer, and the
            // unused remainder is then counted as allocated. A probe that really allocates
            // does so on every try.
            long Least(TableGroupProbe<int, Stamped> target)
            {
                var least = long.MaxValue;
                for (var attempt = 0; attempt < 5 && least != 0; attempt++)
                    least = Math.Min(least, Measure(target));
                return least;
            }

            Assert.Equal(0, Least(probe));
            Assert.Equal(keys.Count, found.Count);
            Assert.Equal(0, Least(byGroup));
            Assert.Equal(4 * keys.Count, found.Count);
        }
        finally
        {
            table.ReleasePin(probe.Version);
        }
    }

    [Fact]
    public void Joins_through_an_index_are_rejected_inside_a_recursion_and_across_builders()
    {
        var table = NewTable();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var bySymbol = builder.Table(table).By("symbol", l => l.Symbol);
        var symbols = builder.Input<string>("symbols");

        Assert.Throws<NotSupportedException>(() => symbols.Recurse(loop => loop
            .GroupBy(symbol => symbol)
            .Join(bySymbol, (symbol, _, listing) => symbol + listing.Venue)));

        var other = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        Assert.Throws<ArgumentException>(() => other.Input<string>("symbols")
            .GroupBy(symbol => symbol)
            .LeftJoin(bySymbol, (symbol, _, _) => symbol, (symbol, _) => symbol));
    }
}
