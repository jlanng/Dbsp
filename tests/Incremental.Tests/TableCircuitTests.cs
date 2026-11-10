using System.Collections.Concurrent;
using Incremental.Cluster;
using Incremental.Storage;
using Incremental.Tables;

namespace Incremental.Tests;

/// <summary>
/// A shared table attached to a circuit (<c>docs/design/shared-tables.md</c>, <em>How a
/// circuit sees a table</em>): each step pins one version, receives the net changes since
/// the previous step's, and releases the pin when its workers finish; the version commits
/// with the checkpoint and a rebuilt circuit resumes from it. These tests read the staged
/// change delta and the probe directly — the two things the table joins are built from;
/// the joins themselves are in <see cref="TableJoinTests"/>.
/// </summary>
public class TableCircuitTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Settle = TimeSpan.FromMilliseconds(200);

    private sealed record Instrument(int Id, string Symbol, long Stamp = 0);

    private static SharedTable<int, Instrument> NewTable(SharedTableOptions? options = null) =>
        new("instruments", i => i.Id, options);

    private static async Task<long> Commit(SharedTable<int, Instrument> table, params Instrument[] rows)
    {
        foreach (var row in rows)
            await table.Writer.UpsertAsync(row);
        return await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
    }

    /// <summary>A circuit that publishes the table's change delta as it is staged, and — for
    /// each key sent on "lookups" — the symbol the table has for it as of the step's version.</summary>
    private sealed record Pipeline(
        CircuitBuilder Builder,
        TableRef<int, Instrument> Table,
        InputStream<int> Lookups,
        Output<TableChange<int, Instrument>> Changes,
        Output<(int Key, string Symbol)> Probed);

    private static Pipeline Build(
        SharedTable<int, Instrument> table,
        IStorageProvider? storage = null,
        int workers = 1,
        bool autoStep = false,
        int outputCapacity = 64,
        Action<int>? beforeProbe = null)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = autoStep,
            Workers = workers,
            OutputChannelCapacity = outputCapacity,
            Storage = storage ?? new InMemoryStorageProvider(),
        });
        var reference = builder.Table(table);
        var probe = reference.Probe;
        var lookups = builder.Input<int>("lookups");
        var probed = lookups
            .Select(key =>
            {
                beforeProbe?.Invoke(key);
                return (key, probe.TryGet(key, out var row) ? row.Symbol : "(none)");
            })
            .ToOutput();
        var changes = reference.ChangeStream.ToOutput();
        return new Pipeline(builder, reference, lookups, changes, probed);
    }

    /// <summary>Everything buffered on <paramref name="output"/>, as value → weight.</summary>
    private static Dictionary<T, long> Drain<T>(OutputHandle<T> output) where T : notnull
    {
        var drained = new Dictionary<T, long>();
        while (output.Reader.TryRead(out var delta))
            foreach (var (value, weight) in delta)
                drained[value] = drained.GetValueOrDefault(value) + weight;
        return drained;
    }

    private static Dictionary<TableChange<int, Instrument>, long> Changes(params TableChange<int, Instrument>[] changes) =>
        changes.ToDictionary(c => c, _ => 1L);

    // ── coalescing ─────────────────────────────────────────────────────────────────────

    [Fact]
    public void Coalescing_keeps_the_first_old_and_the_last_new_per_key()
    {
        var a1 = new Instrument(1, "a1");
        var a2 = new Instrument(1, "a2");
        var a3 = new Instrument(1, "a3");
        var b1 = new Instrument(2, "b1");
        var c1 = new Instrument(3, "c1");
        var offsets = new Dictionary<int, long>();

        var coalescer = new TableChangeCoalescer<int, Instrument>();
        Assert.True(coalescer.IsEmpty);
        coalescer.Add(new TableVersion<int, Instrument>(5, [new(1, a1, a2), new(2, null, b1), new(3, c1, null)], offsets));
        coalescer.Add(new TableVersion<int, Instrument>(6, [new(1, a2, a3), new(2, b1, null)], offsets));
        coalescer.Add(new TableVersion<int, Instrument>(7, [new(3, null, new Instrument(3, "c1"))], offsets));

        var staged = new[] { new ZSet<TableChange<int, Instrument>>() };
        Assert.Equal(1, coalescer.DrainInto(staged));
        Assert.True(coalescer.IsEmpty);

        // Key 1 moved a1 → a3. Key 2 was inserted and deleted again, and key 3 deleted and
        // put back as an equal row: both end where they started and yield nothing.
        Assert.Equal(1, staged[0].DistinctCount);
        Assert.Equal(1, staged[0].WeightOf(new TableChange<int, Instrument>(1, a1, a3)));
    }

    /// <summary>The property the circuit leans on: for any history and any p ≤ t, applying
    /// the coalesced changes of (p, t] to the table as of p gives the table as of t, with one
    /// change per key, each a real change whose Old is the row as of p.</summary>
    [Fact]
    public async Task Coalesced_changes_take_the_table_from_any_version_to_any_later_one()
    {
        var random = new Random(20261109);
        for (var trial = 0; trial < 40; trial++)
        {
            var log = new InMemoryTableLog<int, Instrument>();
            var table = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, log);

            // states[v] is the table as of version v; a batch that changes nothing mints none.
            var states = new List<Dictionary<int, Instrument>> { new() };
            while (states.Count <= 12)
            {
                var next = new Dictionary<int, Instrument>(states[^1]);
                for (var op = random.Next(1, 6); op > 0; op--)
                {
                    var key = random.Next(6);
                    if (random.Next(3) == 0)
                    {
                        await table.Writer.DeleteAsync(key);
                        next.Remove(key);
                    }
                    else
                    {
                        // Few distinct symbols, so keys often change back to an earlier row.
                        var row = new Instrument(key, "s" + random.Next(3));
                        await table.Writer.UpsertAsync(row);
                        next[key] = row;
                    }
                }
                if (await table.Writer.CommitAsync() == states.Count)
                    states.Add(next);
            }

            for (var p = 0; p < states.Count; p++)
            {
                for (var t = p; t < states.Count; t++)
                {
                    var coalescer = new TableChangeCoalescer<int, Instrument>();
                    await foreach (var version in log.ReadAsync(p, t))
                        coalescer.Add(version);
                    var staged = new[] { new ZSet<TableChange<int, Instrument>>(), new ZSet<TableChange<int, Instrument>>() };
                    var count = coalescer.DrainInto(staged);

                    var applied = new Dictionary<int, Instrument>(states[p]);
                    var seen = new HashSet<int>();
                    foreach (var shard in staged)
                    {
                        foreach (var (change, weight) in shard)
                        {
                            Assert.Equal(1, weight);
                            Assert.True(seen.Add(change.Key), $"key {change.Key} changed twice in ({p}, {t}]");
                            Assert.Equal(states[p].GetValueOrDefault(change.Key), change.Old);
                            Assert.NotEqual(change.Old, change.New);
                            if (change.New is null)
                                applied.Remove(change.Key);
                            else
                                applied[change.Key] = change.New;
                        }
                    }
                    Assert.Equal(count, seen.Count);
                    Assert.Equal(states[t], applied);
                }
            }
        }
    }

    // ── what a step sees ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task A_new_circuit_starts_at_the_tables_current_version_with_no_changes()
    {
        var table = NewTable();
        await Commit(table, new Instrument(1, "AAPL"), new Instrument(2, "MSFT"));
        await Commit(table, new Instrument(1, "AAPL.O"));

        var pipeline = Build(table);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var lookups = circuit.Handle(pipeline.Lookups);

        await lookups.InsertAsync(1);
        await lookups.InsertAsync(2);
        await lookups.InsertAsync(3);
        await circuit.StepAsync();

        // No backfill of the rows that were already there — but they are there to be read.
        Assert.Empty(Drain(circuit.Handle(pipeline.Changes)));
        Assert.Equal(
            new Dictionary<(int, string), long> { [(1, "AAPL.O")] = 1, [(2, "MSFT")] = 1, [(3, "(none)")] = 1 },
            Drain(circuit.Handle(pipeline.Probed)));
    }

    [Fact]
    public async Task Each_step_receives_the_net_changes_since_the_previous_step()
    {
        var table = NewTable();
        var aapl = new Instrument(1, "AAPL");
        var msft = new Instrument(2, "MSFT");
        await Commit(table, aapl, msft);

        var pipeline = Build(table);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var changes = circuit.Handle(pipeline.Changes);

        // One version behind: that version's changes, as they are.
        var goog = new Instrument(3, "GOOG");
        await Commit(table, goog);
        await circuit.StepAsync();
        Assert.Equal(Changes(new TableChange<int, Instrument>(3, null, goog)), Drain(changes));

        // Nothing new: nothing staged.
        await circuit.StepAsync();
        Assert.Empty(Drain(changes));

        // Four versions behind: one change per key, first Old to last New. Key 2 changes
        // and changes back, key 4 comes and goes — neither is a change from where this
        // circuit stood.
        var aapl2 = new Instrument(1, "AAPL.2");
        var aapl3 = new Instrument(1, "AAPL.3");
        await Commit(table, aapl2, new Instrument(2, "MSFT.2"), new Instrument(4, "TMP"));
        await Commit(table, aapl3);
        await Commit(table, new Instrument(2, "MSFT"));
        await table.Writer.DeleteAsync(4);
        await table.Writer.DeleteAsync(3);
        await table.Writer.CommitAsync();
        await circuit.StepAsync();
        Assert.Equal(
            Changes(new TableChange<int, Instrument>(1, aapl, aapl3), new TableChange<int, Instrument>(3, goog, null)),
            Drain(changes));
    }

    [Fact]
    public async Task Changes_are_spread_across_workers_and_none_is_lost()
    {
        var table = NewTable();
        var pipeline = Build(table, workers: 4);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();

        var rows = Enumerable.Range(0, 100).Select(i => new Instrument(i, "S" + i)).ToArray();
        await Commit(table, rows);
        await circuit.StepAsync();

        Assert.Equal(
            Changes(rows.Select(r => new TableChange<int, Instrument>(r.Id, null, r)).ToArray()),
            Drain(circuit.Handle(pipeline.Changes)));
    }

    [Fact]
    public async Task A_step_reads_the_table_as_of_its_pinned_version_on_every_worker()
    {
        var table = NewTable();
        await Commit(table, Enumerable.Range(0, 64).Select(i => new Instrument(i, "v1")).ToArray());

        // Every probing worker parks before its first read, mid-step, until the table has
        // moved on underneath it.
        using var entered = new ManualResetEventSlim();
        using var resume = new ManualResetEventSlim();
        var pipeline = Build(table, workers: 4, beforeProbe: _ =>
        {
            entered.Set();
            resume.Wait(Timeout);
        });
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var lookups = circuit.Handle(pipeline.Lookups);
        for (var key = 0; key < 64; key++)
            await lookups.InsertAsync(key);

        var step = circuit.StepAsync();
        Assert.True(entered.Wait(Timeout));
        await Commit(table, Enumerable.Range(0, 64).Select(i => new Instrument(i, "v2")).ToArray());
        await table.Writer.DeleteAsync(7);
        await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
        resume.Set();
        await step.WaitAsync(Timeout);

        var probed = Drain(circuit.Handle(pipeline.Probed));
        Assert.Equal(64, probed.Count);
        Assert.All(probed.Keys, p => Assert.Equal("v1", p.Symbol));

        // The next step pins the new version.
        await lookups.InsertAsync(7);
        await lookups.InsertAsync(8);
        await circuit.StepAsync();
        Assert.Equal(
            new Dictionary<(int, string), long> { [(7, "(none)")] = 1, [(8, "v2")] = 1 },
            Drain(circuit.Handle(pipeline.Probed)));
    }

    [Fact]
    public async Task An_auto_stepping_circuit_steps_when_the_table_changes()
    {
        var table = NewTable();
        var pipeline = Build(table, autoStep: true);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var changes = circuit.Handle(pipeline.Changes);

        // No input record ever arrives: the new version alone must wake the pump.
        var row = new Instrument(1, "AAPL");
        await Commit(table, row);
        var delta = await changes.Reader.ReadAsync().AsTask().WaitAsync(Timeout);
        Assert.Equal(1, delta.WeightOf(new TableChange<int, Instrument>(1, null, row)));
    }

    // ── the pin's lifetime ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task The_pin_is_released_before_outputs_are_published()
    {
        var table = NewTable();
        var pipeline = Build(table, outputCapacity: 1);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var changes = circuit.Handle(pipeline.Changes);

        await Commit(table, new Instrument(1, "v1"));
        await circuit.StepAsync(); // fills the output channel; nobody is reading it

        await Commit(table, new Instrument(1, "v2"));
        var blocked = circuit.StepAsync();
        await Task.Delay(Settle);
        Assert.False(blocked.IsCompleted); // stuck publishing to the slow consumer…

        // …but holding nothing of the table: no pin, and the superseded row is reclaimable.
        Assert.Equal(0, table.PinCount);
        table.Writer.Trim();
        Assert.Equal(1, table.ChainLength(1));

        Assert.True(changes.Reader.TryRead(out _));
        await blocked.WaitAsync(Timeout);
    }

    [Fact]
    public async Task A_faulted_step_releases_its_pin_and_stops_holding_the_table_back()
    {
        var table = NewTable(new SharedTableOptions { TailVersions = 1 });
        await Commit(table, new Instrument(1, "v1"));
        var pipeline = Build(table, workers: 2, beforeProbe: key =>
        {
            if (key == 13)
                throw new InvalidOperationException("boom 13");
        });
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var lookups = circuit.Handle(pipeline.Lookups);

        await lookups.InsertAsync(13);
        await Assert.ThrowsAsync<InvalidOperationException>(() => circuit.StepAsync().WaitAsync(Timeout));
        Assert.NotNull(circuit.Fault);
        Assert.Equal(0, table.PinCount);

        // With a tail of one version, a circuit still attached at v1 would stall the second
        // of these commits forever; a pin still held would keep every old row alive.
        await Commit(table, new Instrument(1, "v2"));
        await Commit(table, new Instrument(1, "v3"));
        await Commit(table, new Instrument(1, "v4"));
        Assert.Equal(1, table.ChainLength(1));
    }

    [Fact]
    public async Task A_step_wedged_past_the_retention_cap_has_its_pin_revoked_and_faults_on_its_next_read()
    {
        var table = NewTable(new SharedTableOptions { MaxRetainedVersions = 3 });
        await Commit(table, new Instrument(1, "v1"));

        using var entered = new ManualResetEventSlim();
        using var resume = new ManualResetEventSlim();
        var pipeline = Build(table, beforeProbe: _ =>
        {
            entered.Set();
            resume.Wait(Timeout);
        });
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        await circuit.Handle(pipeline.Lookups).InsertAsync(1);

        var step = Task.Run(() => circuit.StepAsync());
        Assert.True(entered.Wait(Timeout));

        // Three versions on, the step's pin (v1) is at the cap and still honoured.
        for (var v = 2; v <= 4; v++)
            await Commit(table, new Instrument(1, "v" + v));
        Assert.Equal(4, table.ChainLength(1));

        // One more and it is revoked: the table stops retaining rows for it.
        await Commit(table, new Instrument(1, "v5"));
        Assert.Equal(0, table.PinCount);
        Assert.Equal(1, table.ChainLength(1));

        resume.Set();
        var fault = await Assert.ThrowsAsync<InvalidOperationException>(() => step.WaitAsync(Timeout));
        Assert.Contains("revoked", fault.Message);
        Assert.Same(fault, circuit.Fault);
    }

    // ── a circuit that falls behind ────────────────────────────────────────────────────

    [Fact]
    public async Task The_writer_waits_for_an_attached_circuit_that_is_a_full_tail_behind()
    {
        var table = NewTable(new SharedTableOptions { TailVersions = 2 });
        var pipeline = Build(table);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        var changes = circuit.Handle(pipeline.Changes);

        var v1 = new Instrument(1, "v1");
        var v3 = new Instrument(1, "v3");
        await Commit(table, v1);
        await Commit(table, new Instrument(1, "v2"));

        // The tail is full of versions the circuit has not read, and there is no log to
        // read them from later: the third commit must not publish.
        await table.Writer.UpsertAsync(v3);
        var waiting = table.Writer.CommitAsync().AsTask();
        await Task.Delay(Settle);
        Assert.False(waiting.IsCompleted);
        Assert.Equal(2, table.LatestVersion);
        Assert.Equal(2, table.TailCount);

        // The circuit consumes what it was behind on, and the writer gets its room.
        await circuit.StepAsync();
        Assert.Equal(3, await waiting.WaitAsync(Timeout));
        Assert.Equal(Changes(new TableChange<int, Instrument>(1, null, new Instrument(1, "v2"))), Drain(changes));

        await circuit.StepAsync();
        Assert.Equal(Changes(new TableChange<int, Instrument>(1, new Instrument(1, "v2"), v3)), Drain(changes));
        Assert.Equal(2, table.TailCount);
    }

    [Fact]
    public async Task The_writer_waits_for_an_attached_circuit_that_is_a_full_tail_of_changes_behind()
    {
        var table = NewTable(new SharedTableOptions { TailChanges = 4 });
        var pipeline = Build(table);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();

        // Three unread changes leave room; six do not, however few versions they are.
        await Commit(table, Enumerable.Range(0, 3).Select(i => new Instrument(i, "v1")).ToArray());
        await Commit(table, Enumerable.Range(3, 3).Select(i => new Instrument(i, "v1")).ToArray());
        await table.Writer.UpsertAsync(new Instrument(6, "v1"));
        var waiting = table.Writer.CommitAsync().AsTask();
        await Task.Delay(Settle);
        Assert.False(waiting.IsCompleted);
        Assert.Equal(2, table.LatestVersion);

        await circuit.StepAsync();
        Assert.Equal(3, await waiting.WaitAsync(Timeout));
        Assert.Equal(6, Drain(circuit.Handle(pipeline.Changes)).Count);
    }

    [Fact]
    public async Task Cancelling_a_waiting_commit_leaves_its_changes_staged()
    {
        var table = NewTable(new SharedTableOptions { TailVersions = 1 });
        var pipeline = Build(table);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();
        await Commit(table, new Instrument(1, "v1"));

        using var cancel = new CancellationTokenSource();
        await table.Writer.UpsertAsync(new Instrument(1, "v2"));
        var waiting = table.Writer.CommitAsync(cancel.Token).AsTask();
        await Task.Delay(Settle);
        Assert.False(waiting.IsCompleted);
        cancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting.WaitAsync(Timeout));
        Assert.Equal(1, table.LatestVersion);

        await circuit.StepAsync();
        Assert.Equal(2, await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout));
        using var snapshot = table.Snapshot();
        Assert.True(snapshot.TryGet(1, out var row));
        Assert.Equal("v2", row.Symbol);
    }

    [Fact]
    public async Task A_circuit_that_is_not_attached_never_holds_the_writer()
    {
        var table = NewTable(new SharedTableOptions { TailVersions = 1 });
        var built = Build(table).Builder.Build(); // built, never started
        var pipeline = Build(table);
        var stopped = pipeline.Builder.Build();
        stopped.Start();
        await stopped.DisposeAsync();

        for (var v = 1; v <= 5; v++)
            await Commit(table, new Instrument(1, "v" + v));
        Assert.Equal(1, table.TailCount);
        await built.DisposeAsync();
    }

    [Fact]
    public async Task With_a_log_a_lagging_circuit_reads_what_the_tail_dropped_and_the_writer_never_waits()
    {
        var log = new InMemoryTableLog<int, Instrument>();
        var table = await SharedTable<int, Instrument>.OpenAsync(
            "instruments", i => i.Id, log, new SharedTableOptions { TailVersions = 2 });
        var pipeline = Build(table);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();

        // Eight versions while the circuit sits still: the tail keeps the last two.
        var v1 = new Instrument(1, "v1");
        for (var v = 1; v <= 8; v++)
            await Commit(table, new Instrument(1, "v" + v), new Instrument(100 + v, "only in v" + v));
        Assert.Equal(2, table.TailCount);

        await circuit.StepAsync();
        var changes = Drain(circuit.Handle(pipeline.Changes));
        Assert.Equal(9, changes.Count);
        Assert.Equal(1, changes[new TableChange<int, Instrument>(1, null, new Instrument(1, "v8"))]);
        for (var v = 1; v <= 8; v++)
            Assert.Equal(1, changes[new TableChange<int, Instrument>(100 + v, null, new Instrument(100 + v, "only in v" + v))]);
    }

    // ── checkpoint and restart ─────────────────────────────────────────────────────────

    [Fact]
    public async Task The_consumed_version_commits_with_the_checkpoint()
    {
        var table = NewTable();
        await Commit(table, new Instrument(1, "v1"));
        var storage = new InMemoryCheckpointStorageProvider();
        var pipeline = Build(table, storage);
        await using var circuit = pipeline.Builder.Build();
        circuit.Start();

        Assert.Null(circuit.CommittedVersion(table));
        Assert.Throws<KeyNotFoundException>(() => circuit.CommittedVersion(NewTable()));

        // A new circuit commits the version it started at, having consumed nothing.
        await circuit.StepAsync();
        Assert.Equal(1, circuit.CommittedVersion(table));

        await Commit(table, new Instrument(1, "v2"));
        await Commit(table, new Instrument(1, "v3"));
        Assert.Equal(1, circuit.CommittedVersion(table));
        await circuit.StepAsync();
        Assert.Equal(3, circuit.CommittedVersion(table));
        Assert.Equal(3, storage.Recover()["instruments#t"]);
    }

    [Fact]
    public async Task A_rebuilt_circuit_resumes_from_its_committed_version()
    {
        var table = NewTable();
        var storage = new InMemoryCheckpointStorageProvider();
        var v1 = new Instrument(1, "v1");
        var other = new Instrument(2, "other");

        await using (var first = Build(table, storage).Builder.Build())
        {
            first.Start();
            await Commit(table, v1);
            await first.StepAsync();
            Assert.Equal(1, first.CommittedVersion(table));
        }

        // While no circuit is running the table moves on.
        await Commit(table, new Instrument(1, "v2"), other);
        var v3 = new Instrument(1, "v3");
        await Commit(table, v3);

        var pipeline = Build(table, storage);
        await using var resumed = pipeline.Builder.Build();
        resumed.Start();
        Assert.Equal(1, resumed.CommittedVersion(table));

        // Exactly what it missed, coalesced — not the whole table, and not nothing.
        await resumed.StepAsync();
        Assert.Equal(
            Changes(new TableChange<int, Instrument>(1, v1, v3), new TableChange<int, Instrument>(2, null, other)),
            Drain(resumed.Handle(pipeline.Changes)));
        Assert.Equal(3, resumed.CommittedVersion(table));
    }

    [Fact]
    public async Task A_step_that_was_never_committed_is_replayed_after_a_restart()
    {
        var table = NewTable();
        var storage = new InMemoryCheckpointStorageProvider();
        var v1 = new Instrument(1, "v1");
        var v2 = new Instrument(1, "v2");

        // The second step consumes v2 and then faults before its checkpoint: a crash.
        var crashing = Build(table, storage, beforeProbe: _ => throw new InvalidOperationException("crash"));
        await using (var first = crashing.Builder.Build())
        {
            first.Start();
            await Commit(table, v1);
            await first.StepAsync();
            await Commit(table, v2);
            await first.Handle(crashing.Lookups).InsertAsync(1);
            await Assert.ThrowsAsync<InvalidOperationException>(() => first.StepAsync());
            Assert.Equal(1, first.CommittedVersion(table));
        }

        var pipeline = Build(table, storage);
        await using var resumed = pipeline.Builder.Build();
        resumed.Start();
        await resumed.StepAsync();
        Assert.Equal(Changes(new TableChange<int, Instrument>(1, v1, v2)), Drain(resumed.Handle(pipeline.Changes)));
    }

    [Fact]
    public async Task A_circuit_stopped_for_longer_than_the_tail_catches_up_from_the_log_in_one_step()
    {
        var log = new InMemoryTableLog<int, Instrument>();
        var options = new SharedTableOptions { TailVersions = 2 };
        var table = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, log, options);
        var storage = new InMemoryCheckpointStorageProvider();
        var v1 = new Instrument(1, "v1");

        await using (var first = Build(table, storage).Builder.Build())
        {
            first.Start();
            await Commit(table, v1);
            await first.StepAsync();
        }

        for (var v = 2; v <= 9; v++)
            await Commit(table, new Instrument(1, "v" + v), new Instrument(v, "added in v" + v));
        await table.Writer.DeleteAsync(5);
        await table.Writer.CommitAsync(); // v10

        // The process restarts too: the table is rebuilt from its log, and holds no tail at all.
        var restored = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, log, options);
        var pipeline = Build(restored, storage);
        await using var resumed = pipeline.Builder.Build();
        resumed.Start();
        await resumed.StepAsync();

        var changes = Drain(resumed.Handle(pipeline.Changes));
        Assert.Equal(8, changes.Count); // key 1, and keys 2..9 less key 5, which came and went
        Assert.Equal(1, changes[new TableChange<int, Instrument>(1, v1, new Instrument(1, "v9"))]);
        Assert.DoesNotContain(changes.Keys, c => c.Key == 5);
        Assert.Equal(10, resumed.CommittedVersion(restored));

        // And from there it is an ordinary attached circuit, reading the tail.
        await Commit(restored, new Instrument(1, "v11"));
        await resumed.StepAsync();
        Assert.Equal(
            Changes(new TableChange<int, Instrument>(1, new Instrument(1, "v9"), new Instrument(1, "v11"))),
            Drain(resumed.Handle(pipeline.Changes)));
    }

    [Fact]
    public async Task A_circuit_whose_committed_version_is_no_longer_retained_refuses_to_start()
    {
        var table = NewTable(new SharedTableOptions { TailVersions = 2 });
        var storage = new InMemoryCheckpointStorageProvider();
        await using (var first = Build(table, storage).Builder.Build())
        {
            first.Start();
            await Commit(table, new Instrument(1, "v1"));
            await first.StepAsync();
        }

        // Within the tail it can still resume…
        await Commit(table, new Instrument(1, "v2"));
        await Commit(table, new Instrument(1, "v3"));
        await using (var within = Build(table, storage).Builder.Build())
            within.Start(); // disposed unstepped: its checkpoint stays at v1

        // …one version further and the change after v1 is gone, with no log to read it from.
        await Commit(table, new Instrument(1, "v4"));
        await using var tooLate = Build(table, storage).Builder.Build();
        var refused = Assert.Throws<InvalidOperationException>(tooLate.Start);
        Assert.Contains("no longer retained", refused.Message);

        // A refused start leaves nothing attached to hold the writer back.
        await Commit(table, new Instrument(1, "v5"));
        await Commit(table, new Instrument(1, "v6"));
        await Commit(table, new Instrument(1, "v7"));
    }

    /// <summary>An empty log that claims another log's epoch: what a table restored from
    /// the wrong place, or from a log that lost its tail, looks like to a circuit.</summary>
    private sealed class EmptyLogWithEpoch(string epoch) : ITableLog<int, Instrument>
    {
        public string Epoch => epoch;
        public long OldestRetainedVersion => 1;
        public ValueTask<long> LastVersionAsync(CancellationToken cancellationToken = default) => new(0L);

        public ValueTask AppendAsync(TableVersion<int, Instrument> version, CancellationToken cancellationToken = default) => default;

        public IAsyncEnumerable<TableVersion<int, Instrument>> ReadAsync(
            long fromExclusive, long toInclusive, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public IAsyncEnumerable<TableVersion<int, Instrument>> FollowAsync(
            long fromExclusive, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask<TableHead> RestoreAsync(Action<int, Instrument> row, CancellationToken cancellationToken = default) =>
            new(new TableHead(0, new Dictionary<int, long>()));

        public ValueTask TrimBeforeAsync(long version, CancellationToken cancellationToken = default) => default;
    }

    [Fact]
    public async Task A_table_behind_the_version_a_checkpoint_consumed_is_refused()
    {
        var log = new InMemoryTableLog<int, Instrument>();
        var table = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, log);
        var storage = new InMemoryCheckpointStorageProvider();
        await using (var first = Build(table, storage).Builder.Build())
        {
            first.Start();
            await Commit(table, new Instrument(1, "v1"));
            await Commit(table, new Instrument(1, "v2"));
            await first.StepAsync();
        }

        // Same epoch, so the manifest matches — but this table never reached version 2.
        var behind = await SharedTable<int, Instrument>.OpenAsync("instruments", i => i.Id, new EmptyLogWithEpoch(log.Epoch));
        await using var circuit = Build(behind, storage).Builder.Build();
        var refused = Assert.Throws<InvalidOperationException>(circuit.Start);
        Assert.Contains("only at version 0", refused.Message);
    }

    [Fact]
    public async Task A_table_recreated_under_the_same_name_is_refused_by_its_epoch()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var original = NewTable();
        await using (var first = Build(original, storage).Builder.Build())
        {
            first.Start();
            await Commit(original, new Instrument(1, "v1"));
            await first.StepAsync();
        }

        // Same name, same version number even — but a different history.
        var recreated = NewTable();
        await Commit(recreated, new Instrument(1, "something else"));
        await using var circuit = Build(recreated, storage).Builder.Build();
        var refused = Assert.Throws<InvalidOperationException>(circuit.Start);
        Assert.Contains("shared table 'instruments'", refused.Message);
        Assert.Contains(original.Epoch, refused.Message);
        Assert.Contains(recreated.Epoch, refused.Message);

        // The original still resumes.
        await using var resumed = Build(original, storage).Builder.Build();
        resumed.Start();
    }

    [Fact]
    public async Task A_table_attached_after_the_checkpoint_starts_at_its_current_version()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        CircuitBuilder NewBuilder() => new(new CircuitOptions { AutoStep = false, Storage = storage });

        var before = NewBuilder();
        before.Input<int>("lookups").ToOutput();
        await using (var first = before.Build())
        {
            first.Start();
            await first.StepAsync();
        }

        var table = NewTable();
        await Commit(table, new Instrument(1, "v1"));
        var after = NewBuilder();
        after.Input<int>("lookups").ToOutput();
        var changes = after.Table(table).ChangeStream.ToOutput();
        await using var circuit = after.Build();
        circuit.Start();
        await circuit.StepAsync();
        Assert.Empty(Drain(circuit.Handle(changes)));
        Assert.Equal(1, circuit.CommittedVersion(table));
    }

    // ── build-time rules ───────────────────────────────────────────────────────────────

    [Fact]
    public void A_clustered_circuit_cannot_attach_a_table()
    {
        var mesh = InMemoryExchangeTransport.CreateMesh(2);
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false, Cluster = new ClusterOptions { Transport = mesh[0] },
        });
        builder.Table(NewTable());
        var rejected = Assert.Throws<NotSupportedException>(() => builder.Build());
        Assert.Contains("Shared tables", rejected.Message);
    }

    [Fact]
    public void Attaching_a_table_twice_gives_the_same_reference_and_a_namesake_is_rejected()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var table = NewTable();
        Assert.Same(builder.Table(table), builder.Table(table));
        Assert.Throws<ArgumentException>(() => builder.Table(NewTable()));
        Assert.Throws<ArgumentException>(() => builder.Table(new SharedTable<string, string>("instruments", s => s)));
    }

    // ── everything at once ─────────────────────────────────────────────────────────────

    /// <summary>A writer churning every key while two circuits auto-step against the table,
    /// one of them slow enough that the writer has to wait for it. Version v stamps every
    /// surviving key with v and deletes the keys with (key + v) % 3 == 0, so the table as of
    /// any version is computable from the number alone. Each circuit checks two things:
    /// every probe returns the row as of the version its step pinned, and the change deltas,
    /// applied in order, each start from the row the circuit last saw and end at the final
    /// table — a dropped, duplicated or mis-coalesced version breaks one or the other.</summary>
    [Fact]
    public async Task Circuits_stepping_under_a_churning_writer_see_consistent_versions_and_complete_changes()
    {
        const int keys = 128;
        const int versions = 600;
        var table = NewTable(new SharedTableOptions { TailVersions = 4 });
        var failures = new ConcurrentQueue<string>();

        (Circuit Circuit, Task Feeder, Task Consumers, Dictionary<int, Instrument> Model) RunCircuit(
            int workers, TimeSpan probeDelay, CancellationToken stop)
        {
            var builder = new CircuitBuilder(new CircuitOptions { AutoStep = true, Workers = workers });
            var reference = builder.Table(table);
            var probe = reference.Probe;
            var lookups = builder.Input<int>("lookups");
            var probedDef = lookups
                .Select(key =>
                {
                    if (probeDelay > TimeSpan.Zero)
                        Thread.Sleep(probeDelay);
                    var version = probe.Version;
                    var found = probe.TryGet(key, out var row);
                    var expected = version > 0 && (key + version) % 3 != 0;
                    if (found != expected)
                        failures.Enqueue($"version {version} key {key}: found={found}, expected={expected}");
                    else if (found && row!.Stamp != version)
                        failures.Enqueue($"version {version} key {key}: stamp {row.Stamp}");
                    return key;
                })
                .ToOutput();
            var changesDef = reference.ChangeStream.ToOutput();

            var circuit = builder.Build();
            circuit.Start();
            var input = circuit.Handle(lookups);
            var model = new Dictionary<int, Instrument>();

            var feeder = Task.Run(async () =>
            {
                var random = new Random(workers);
                while (!stop.IsCancellationRequested && failures.IsEmpty)
                {
                    await input.InsertAsync(random.Next(keys));
                    // The slow circuit is slow per lookup, so it is fed few of them: it lags
                    // the writer by a few versions a step, not by seconds.
                    await Task.Delay(probeDelay);
                }
            });
            var probedConsumer = Task.Run(async () =>
            {
                await foreach (var _ in circuit.Handle(probedDef).Reader.ReadAllAsync()) { }
            });
            var changesConsumer = Task.Run(async () =>
            {
                await foreach (var delta in circuit.Handle(changesDef).Reader.ReadAllAsync())
                {
                    lock (model)
                    {
                        foreach (var (change, weight) in delta)
                        {
                            if (weight != 1 || !Equals(model.GetValueOrDefault(change.Key), change.Old))
                                failures.Enqueue($"key {change.Key}: change from {change.Old} (weight {weight}) but had {model.GetValueOrDefault(change.Key)}");
                            if (change.New is null)
                                model.Remove(change.Key);
                            else
                                model[change.Key] = change.New;
                        }
                    }
                }
            });
            return (circuit, feeder, Task.WhenAll(probedConsumer, changesConsumer), model);
        }

        using var stop = new CancellationTokenSource();
        var fast = RunCircuit(workers: 2, TimeSpan.Zero, stop.Token);
        var slow = RunCircuit(workers: 1, TimeSpan.FromMilliseconds(1), stop.Token);

        for (var v = 1; v <= versions && failures.IsEmpty; v++)
        {
            for (var key = 0; key < keys; key++)
            {
                if ((key + v) % 3 == 0)
                    await table.Writer.DeleteAsync(key);
                else
                    await table.Writer.UpsertAsync(new Instrument(key, "x", v));
            }
            Assert.Equal(v, await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout));
        }

        var expected = Enumerable.Range(0, keys)
            .Where(key => (key + versions) % 3 != 0)
            .ToDictionary(key => key, key => new Instrument(key, "x", versions));

        bool CaughtUp(Dictionary<int, Instrument> model)
        {
            lock (model)
                return model.Count == expected.Count && model.All(e => expected.TryGetValue(e.Key, out var row) && row.Equals(e.Value));
        }

        // Both circuits reach the final version on their own: nothing but the table wakes them for it.
        var deadline = DateTime.UtcNow + Timeout;
        while (failures.IsEmpty && !(CaughtUp(fast.Model) && CaughtUp(slow.Model)) && DateTime.UtcNow < deadline)
            await Task.Delay(10);

        stop.Cancel();
        await Task.WhenAll(fast.Feeder, slow.Feeder).WaitAsync(Timeout);
        await fast.Circuit.DisposeAsync();
        await slow.Circuit.DisposeAsync();
        await Task.WhenAll(fast.Consumers, slow.Consumers).WaitAsync(Timeout);

        Assert.True(failures.IsEmpty, string.Join("\n", failures.Take(5)));
        Assert.True(CaughtUp(fast.Model), "the fast circuit's integrated changes do not equal the final table");
        Assert.True(CaughtUp(slow.Model), "the slow circuit's integrated changes do not equal the final table");

        // Idle again: no pins, every chain back to one entry, deleted keys gone.
        Assert.Equal(0, table.PinCount);
        table.Writer.Trim();
        Assert.Equal(table.Count, table.PhysicalKeyCount);
        for (var key = 0; key < keys; key++)
            Assert.True(table.ChainLength(key) <= 1);
    }
}
