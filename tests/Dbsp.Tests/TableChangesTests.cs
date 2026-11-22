using Dbsp.Storage;
using Dbsp.Tables;

namespace Dbsp.Tests;

/// <summary>
/// A shared table read as a stream (<c>TableRef.Changes()</c>): the old row retracted and
/// the new row asserted for every change, beginning — in a circuit with no checkpoint — with
/// every row the table already holds, so that whatever is computed from the stream is about
/// the whole table and not just the part of its history the circuit happened to watch.
/// </summary>
public class TableChangesTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    private sealed record Instrument(int Id, string Sector, int Lot);

    private static SharedTable<int, Instrument> NewTable() => new("instruments", i => i.Id);

    private static async Task<long> Commit(SharedTable<int, Instrument> table, params Instrument[] rows)
    {
        foreach (var row in rows)
            await table.Writer.UpsertAsync(row);
        return await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
    }

    private static async Task<long> Delete(SharedTable<int, Instrument> table, params int[] keys)
    {
        foreach (var key in keys)
            await table.Writer.DeleteAsync(key);
        return await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
    }

    /// <summary>The table's rows as a stream, and the instruments counted per sector.</summary>
    private sealed record Pipeline(
        CircuitBuilder Builder,
        Output<Instrument> Rows,
        Output<KeyedValue<string, long>> PerSector);

    private static Pipeline Build(
        SharedTable<int, Instrument> table, int workers = 1, IStorageProvider? storage = null, bool autoStep = false)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = autoStep,
            Workers = workers,
            Storage = storage ?? new InMemoryStorageProvider(),
        });
        var instruments = builder.Table(table);
        var rows = instruments.Changes();
        var perSector = rows.GroupBy(i => i.Sector).Count().Named("per-sector");
        return new Pipeline(builder, rows.ToOutput(), perSector.ToOutput(bootstrap: false));
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

    private static (KeyedValue<string, long>, long) Count(string sector, long count, long weight) =>
        (new KeyedValue<string, long>(sector, count), weight);

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task A_new_circuit_begins_with_every_row_the_table_holds_and_then_follows_its_changes(int workers)
    {
        const int rows = 300;
        var table = NewTable();
        for (var id = 0; id < rows; id++)
            await table.Writer.UpsertAsync(new Instrument(id, "S" + id % 3, 1));
        await table.Writer.CommitAsync();
        await Commit(table, new Instrument(0, "S0", 2));
        await Delete(table, 1);

        var pipeline = Build(table, workers);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();

        // History is not replayed: the rows as they are now, each once.
        await circuit.StepAsync();
        var bootstrap = Drain(circuit.Handle(pipeline.Rows));
        Assert.Equal(rows - 1, bootstrap.DistinctCount);
        Assert.Equal(1, bootstrap.WeightOf(new Instrument(0, "S0", 2)));
        Assert.Equal(0, bootstrap.WeightOf(new Instrument(1, "S1", 1)));
        Assert.All(bootstrap, entry => Assert.Equal(1, entry.Weight));
        AssertDelta(Drain(circuit.Handle(pipeline.PerSector)), Count("S0", 100, 1), Count("S1", 99, 1), Count("S2", 100, 1));

        // From here on: the old row out, the new row in.
        await Commit(table, new Instrument(2, "S0", 1), new Instrument(rows, "S1", 1));
        await Delete(table, 5);
        await circuit.StepAsync();
        AssertDelta(
            Drain(circuit.Handle(pipeline.Rows)),
            (new Instrument(2, "S2", 1), -1), (new Instrument(2, "S0", 1), 1),
            (new Instrument(rows, "S1", 1), 1),
            (new Instrument(5, "S2", 1), -1));
        AssertDelta(
            Drain(circuit.Handle(pipeline.PerSector)),
            Count("S0", 100, -1), Count("S0", 101, 1),
            Count("S1", 99, -1), Count("S1", 100, 1),
            Count("S2", 100, -1), Count("S2", 98, 1));

        // An idle step emits nothing: the bootstrap happens once.
        await circuit.StepAsync();
        AssertDelta(Drain(circuit.Handle(pipeline.Rows)));
    }

    [Fact]
    public async Task Versions_published_between_start_and_the_first_step_are_in_the_bootstrap_and_not_applied_again()
    {
        var table = NewTable();
        await Commit(table, new Instrument(1, "A", 1), new Instrument(2, "A", 1));
        var pipeline = Build(table, workers: 2);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();

        // The circuit is attached at version 1; these arrive before it first steps.
        await Commit(table, new Instrument(2, "B", 1), new Instrument(3, "B", 1));
        await Delete(table, 1);

        await circuit.StepAsync();
        AssertDelta(Drain(circuit.Handle(pipeline.Rows)), (new Instrument(2, "B", 1), 1), (new Instrument(3, "B", 1), 1));
        AssertDelta(Drain(circuit.Handle(pipeline.PerSector)), Count("B", 2, 1));

        // Nor are they left over for the step after.
        await circuit.StepAsync();
        AssertDelta(Drain(circuit.Handle(pipeline.Rows)));
        await Commit(table, new Instrument(3, "C", 1));
        await circuit.StepAsync();
        AssertDelta(Drain(circuit.Handle(pipeline.Rows)), (new Instrument(3, "B", 1), -1), (new Instrument(3, "C", 1), 1));
    }

    [Fact]
    public async Task An_auto_stepping_circuit_bootstraps_from_a_table_that_is_loaded_and_quiet_and_then_follows_it()
    {
        var table = NewTable();
        await Commit(table, Enumerable.Range(0, 10).Select(id => new Instrument(id, "A", 1)).ToArray());
        var pipeline = Build(table, autoStep: true);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var rows = circuit.Handle(pipeline.Rows);

        // No input record arrives and the table publishes nothing: the rows the circuit is
        // owed are themselves the work that makes it step.
        var bootstrap = await rows.Reader.ReadAsync().AsTask().WaitAsync(Timeout);
        Assert.Equal(10, bootstrap.DistinctCount);

        // A circuit whose only inputs are tables has nothing to complete it; it runs on.
        await Commit(table, new Instrument(10, "B", 1));
        var change = await rows.Reader.ReadAsync().AsTask().WaitAsync(Timeout);
        AssertDelta(change, (new Instrument(10, "B", 1), 1));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_circuit_disposed_before_its_first_step_still_delivers_the_rows_it_was_owed(bool autoStep)
    {
        var table = NewTable();
        await Commit(table, new Instrument(1, "A", 1), new Instrument(2, "B", 1));
        var pipeline = Build(table, autoStep: autoStep);
        var circuit = pipeline.Builder.Build();
        var rows = circuit.Handle(pipeline.Rows);
        circuit.Start();
        await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);

        AssertDelta(Drain(rows), (new Instrument(1, "A", 1), 1), (new Instrument(2, "B", 1), 1));
        Assert.True(rows.Reader.Completion.IsCompleted);
    }

    [Fact]
    public async Task The_bootstrap_step_commits_the_version_it_scanned_so_a_restart_does_not_apply_it_again()
    {
        var table = NewTable();
        var storage = new InMemoryCheckpointStorageProvider();
        await Commit(table, new Instrument(1, "A", 1), new Instrument(2, "A", 1));

        var before = Build(table, storage: storage);
        await using (var first = before.Builder.Build())
        {
            first.Start();

            // Attached at version 1; version 2 arrives before the first step, which scans
            // the rows as of it. That is the version the step has consumed.
            Assert.Equal(2, await Commit(table, new Instrument(2, "B", 1)));
            await first.StepAsync();
            AssertDelta(Drain(first.Handle(before.PerSector)), Count("A", 1, 1), Count("B", 1, 1));
            Assert.Equal(2, first.CommittedVersion(table));
        }

        // Resuming from version 1 instead would move instrument 2 out of a sector the
        // checkpointed counts never had it in.
        var after = Build(table, storage: storage);
        await using var resumed = after.Builder.Build();
        resumed.Start();
        await resumed.StepAsync();
        AssertDelta(Drain(resumed.Handle(after.Rows)));
        AssertDelta(Drain(resumed.Handle(after.PerSector)));
    }

    [Fact]
    public async Task A_bootstrap_scan_the_writer_overtakes_faults_the_step_instead_of_delivering_part_of_the_table()
    {
        // A table large enough that the scan takes a while, and a cap small enough that a
        // writer committing beside it revokes the scan's pin long before it ends.
        const int rows = 200_000;
        var table = new SharedTable<int, Instrument>("instruments", i => i.Id, new SharedTableOptions { MaxRetainedVersions = 1 });
        await table.Writer.ReplaceAllAsync(Enumerable.Range(0, rows).Select(id => new Instrument(id, "A", 0)));

        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var rowsDef = builder.Table(table).Changes().ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();

        using var stop = new CancellationTokenSource();
        var churn = Task.Run(async () =>
        {
            var random = new Random(1);
            try
            {
                for (var lot = 1; !stop.IsCancellationRequested; lot++)
                {
                    await table.Writer.UpsertAsync(new Instrument(random.Next(rows), "A", lot));
                    await table.Writer.CommitAsync(stop.Token);
                }
            }
            catch (OperationCanceledException)
            {
            }
        });

        try
        {
            // Either the scan finished inside the cap and delivered every row, or it did not
            // and the step says so. What it must never do is succeed with rows missing: the
            // circuit's position has moved past them, so they would never arrive.
            await circuit.StepAsync().WaitAsync(Timeout);
            Assert.Equal(rows, Drain(circuit.Handle(rowsDef)).DistinctCount);
        }
        catch (InvalidOperationException revoked)
        {
            Assert.Contains("revoked", revoked.Message);
        }
        finally
        {
            stop.Cancel();
            await churn.WaitAsync(Timeout);
        }
    }

    [Fact]
    public async Task A_restarted_circuit_resumes_from_its_committed_version_and_is_not_bootstrapped_again()
    {
        var table = NewTable();
        var storage = new InMemoryCheckpointStorageProvider();
        await Commit(table, new Instrument(1, "A", 1), new Instrument(2, "A", 1), new Instrument(3, "B", 1));

        var before = Build(table, workers: 2, storage: storage);
        await using (var first = before.Builder.Build())
        {
            first.Start();
            await first.StepAsync();
            AssertDelta(Drain(first.Handle(before.PerSector)), Count("A", 2, 1), Count("B", 1, 1));
            Assert.Equal(1, first.CommittedVersion(table));
        }

        // The table moves on while no circuit is running.
        await Commit(table, new Instrument(2, "B", 1));
        await Commit(table, new Instrument(4, "C", 1));
        await Delete(table, 3);

        var after = Build(table, workers: 2, storage: storage);
        await using var resumed = after.Builder.Build();
        resumed.Start();
        await resumed.StepAsync();

        // Only what it missed. Bootstrapping again on top of the counts in its checkpoint
        // would count every instrument twice.
        AssertDelta(
            Drain(resumed.Handle(after.Rows)),
            (new Instrument(2, "A", 1), -1), (new Instrument(2, "B", 1), 1),
            (new Instrument(4, "C", 1), 1),
            (new Instrument(3, "B", 1), -1));
        AssertDelta(Drain(resumed.Handle(after.PerSector)), Count("A", 2, -1), Count("A", 1, 1), Count("C", 1, 1));
    }

    [Fact]
    public async Task A_circuit_that_faulted_before_its_first_checkpoint_bootstraps_again_from_nothing()
    {
        var table = NewTable();
        var storage = new InMemoryCheckpointStorageProvider();
        await Commit(table, new Instrument(1, "A", 1), new Instrument(2, "B", 1));

        Pipeline Poisoned(bool poison)
        {
            var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
            var rows = builder.Table(table).Changes()
                .Select(i => poison && i.Id == 2 ? throw new InvalidOperationException("boom") : i);
            return new Pipeline(builder, rows.ToOutput(), rows.GroupBy(i => i.Sector).Count().Named("per-sector").ToOutput(bootstrap: false));
        }

        var doomed = Poisoned(poison: true);
        await using (var first = doomed.Builder.Build())
        {
            first.Start();
            await Assert.ThrowsAsync<InvalidOperationException>(() => first.StepAsync());
        }

        // Nothing was committed, so the rebuilt circuit has no position in the table and no
        // state: the rows are owed to it in full.
        var healthy = Poisoned(poison: false);
        await using var second = healthy.Builder.Build();
        second.Start();
        await second.StepAsync();
        AssertDelta(Drain(second.Handle(healthy.PerSector)), Count("A", 1, 1), Count("B", 1, 1));
    }

    [Fact]
    public async Task A_table_read_as_a_stream_can_be_joined_to_another_table_and_both_keep_it_current()
    {
        var instruments = NewTable();
        var sectors = new SharedTable<string, SectorInfo>("sectors", s => s.Code);
        await Commit(instruments, new Instrument(1, "A", 1), new Instrument(2, "B", 1));
        await sectors.Writer.UpsertAsync(new SectorInfo("A", "Alpha"));
        await sectors.Writer.CommitAsync();

        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = 3 });
        var sectorTable = builder.Table(sectors);
        var namedDef = builder.Table(instruments).Changes()
            .GroupBy(i => i.Sector)
            .LeftJoin(sectorTable, (_, instrument, sector) => (instrument.Id, sector.Name), (_, instrument) => (instrument.Id, "?"))
            .ToOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var named = circuit.Handle(namedDef);

        // The instruments were loaded before the circuit existed; the join still has them.
        await circuit.StepAsync();
        AssertDelta(Drain(named), ((1, "Alpha"), 1), ((2, "?"), 1));

        // Either table changing corrects the result.
        await sectors.Writer.UpsertAsync(new SectorInfo("B", "Beta"));
        await sectors.Writer.CommitAsync();
        await Commit(instruments, new Instrument(1, "B", 1));
        await circuit.StepAsync();
        AssertDelta(Drain(named), ((1, "Alpha"), -1), ((1, "Beta"), 1), ((2, "?"), -1), ((2, "Beta"), 1));
    }

    private sealed record SectorInfo(string Code, string Name);

    [Fact]
    public void Asking_for_the_changes_twice_gives_the_same_stream_and_a_recursion_cannot_ask()
    {
        var table = NewTable();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var instruments = builder.Table(table);
        Assert.Equal(instruments.Changes().NodeId, instruments.Changes().NodeId);

        Assert.Throws<NotSupportedException>(() => builder.Input<Instrument>("seed")
            .Recurse(loop => loop.Concat(instruments.Changes())));
    }
}
