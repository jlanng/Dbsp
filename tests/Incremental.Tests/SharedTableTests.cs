using Incremental.Tables;

namespace Incremental.Tests;

public class SharedTableTests
{
    private sealed record Instrument(int Id, string Symbol, long Stamp = 0);

    private static SharedTable<int, Instrument> NewTable() => new("instruments", i => i.Id);

    [Fact]
    public async Task A_committed_batch_is_one_version_and_reads_see_it()
    {
        var table = NewTable();
        Assert.Equal(0, table.LatestVersion);

        await table.Writer.UpsertAsync(new Instrument(1, "AAPL"));
        await table.Writer.UpsertAsync(new Instrument(2, "MSFT"));
        Assert.Equal(1, await table.Writer.CommitAsync());
        Assert.Equal(2, table.Count);

        using var snapshot = table.Snapshot();
        Assert.Equal(1, snapshot.Version);
        Assert.True(snapshot.TryGet(1, out var row));
        Assert.Equal("AAPL", row.Symbol);
        Assert.False(snapshot.TryGet(3, out _));
    }

    [Fact]
    public async Task Nothing_is_visible_before_commit()
    {
        var table = NewTable();
        await table.Writer.UpsertAsync(new Instrument(1, "AAPL"));
        using var snapshot = table.Snapshot();
        Assert.False(snapshot.TryGet(1, out _));
    }

    [Fact]
    public async Task A_batch_that_changes_nothing_mints_no_version()
    {
        var table = NewTable();
        await table.Writer.UpsertAsync(new Instrument(1, "AAPL"));
        await table.Writer.CommitAsync();

        await table.Writer.UpsertAsync(new Instrument(1, "AAPL")); // equal row: no-op
        await table.Writer.DeleteAsync(99);                        // absent key: no-op
        Assert.Equal(1, await table.Writer.CommitAsync());
        Assert.Equal(1, await table.Writer.CommitAsync());         // empty batch
        Assert.Equal(1, table.ChainLength(1));
    }

    [Fact]
    public async Task The_last_change_to_a_key_within_a_batch_wins()
    {
        var table = NewTable();
        await table.Writer.UpsertAsync(new Instrument(1, "A"));
        await table.Writer.DeleteAsync(1);
        await table.Writer.UpsertAsync(new Instrument(1, "B"));
        await table.Writer.UpsertAsync(new Instrument(2, "C"));
        await table.Writer.DeleteAsync(2);
        await table.Writer.CommitAsync();

        using var snapshot = table.Snapshot();
        Assert.True(snapshot.TryGet(1, out var row));
        Assert.Equal("B", row.Symbol);
        Assert.False(snapshot.TryGet(2, out _));
        Assert.Equal(1, table.Count);
        Assert.Equal(1, table.ChainLength(1)); // one row version, not three
    }

    [Fact]
    public async Task A_snapshot_keeps_reading_its_version_while_the_writer_moves_on()
    {
        var table = NewTable();
        await table.Writer.UpsertAsync(new Instrument(1, "v1"));
        await table.Writer.UpsertAsync(new Instrument(2, "v1"));
        await table.Writer.CommitAsync();

        var old = table.Snapshot();

        await table.Writer.UpsertAsync(new Instrument(1, "v2"));
        await table.Writer.DeleteAsync(2);
        await table.Writer.UpsertAsync(new Instrument(3, "v2"));
        await table.Writer.CommitAsync();
        await table.Writer.UpsertAsync(new Instrument(1, "v3"));
        await table.Writer.CommitAsync();

        Assert.True(old.TryGet(1, out var row));
        Assert.Equal("v1", row.Symbol);
        Assert.True(old.TryGet(2, out _));      // deleted later
        Assert.False(old.TryGet(3, out _));     // inserted later

        using (var latest = table.Snapshot())
        {
            Assert.Equal(3, latest.Version);
            Assert.True(latest.TryGet(1, out row));
            Assert.Equal("v3", row.Symbol);
            Assert.False(latest.TryGet(2, out _));
            Assert.True(latest.TryGet(3, out _));
        }
        old.Dispose();
    }

    [Fact]
    public async Task Superseded_rows_are_kept_only_while_a_snapshot_can_read_them()
    {
        var table = NewTable();
        await table.Writer.UpsertAsync(new Instrument(1, "v1"));
        await table.Writer.CommitAsync();

        var pinned = table.Snapshot();
        await table.Writer.UpsertAsync(new Instrument(1, "v2"));
        await table.Writer.CommitAsync();
        await table.Writer.UpsertAsync(new Instrument(1, "v3"));
        await table.Writer.CommitAsync();

        // v1 is pinned; v2 sits between the pin and the head and is kept with it.
        Assert.Equal(3, table.ChainLength(1));

        pinned.Dispose();
        table.Writer.Trim();
        Assert.Equal(1, table.ChainLength(1));
    }

    [Fact]
    public async Task A_deleted_key_is_dropped_once_no_snapshot_can_read_its_row()
    {
        var table = NewTable();
        await table.Writer.UpsertAsync(new Instrument(1, "v1"));
        await table.Writer.CommitAsync();

        var pinned = table.Snapshot();
        await table.Writer.DeleteAsync(1);
        await table.Writer.CommitAsync();

        Assert.Equal(0, table.Count);
        Assert.Equal(1, table.PhysicalKeyCount); // the pinned reader still sees the row
        Assert.True(pinned.TryGet(1, out _));

        pinned.Dispose();
        table.Writer.Trim();
        Assert.Equal(0, table.PhysicalKeyCount);

        // And the key can come back.
        await table.Writer.UpsertAsync(new Instrument(1, "again"));
        await table.Writer.CommitAsync();
        using var snapshot = table.Snapshot();
        Assert.True(snapshot.TryGet(1, out var row));
        Assert.Equal("again", row.Symbol);
    }

    [Fact]
    public async Task GetManyInto_appends_rows_aligned_with_the_keys()
    {
        var table = NewTable();
        await table.Writer.UpsertAsync(new Instrument(1, "A"));
        await table.Writer.UpsertAsync(new Instrument(3, "C"));
        await table.Writer.CommitAsync();

        using var snapshot = table.Snapshot();
        var rows = new List<Instrument?> { null };
        snapshot.GetManyInto([3, 2, 1], rows);
        Assert.Equal(4, rows.Count);
        Assert.Equal("C", rows[1]!.Symbol);
        Assert.Null(rows[2]);
        Assert.Equal("A", rows[3]!.Symbol);
    }

    [Fact]
    public void Releasing_a_pin_twice_is_an_error()
    {
        var table = NewTable();
        var snapshot = table.Snapshot();
        snapshot.Dispose();
        Assert.Throws<InvalidOperationException>(() => snapshot.Dispose());
    }

    [Fact]
    public async Task A_snapshot_held_past_the_retention_cap_is_revoked_and_its_next_read_throws()
    {
        var table = new SharedTable<int, Instrument>("instruments", i => i.Id, new SharedTableOptions { MaxRetainedVersions = 2 });
        await table.Writer.UpsertAsync(new Instrument(1, "v1"));
        await table.Writer.CommitAsync();
        var held = table.Snapshot();

        // Two versions behind is the cap, and still honoured.
        for (var v = 2; v <= 3; v++)
        {
            await table.Writer.UpsertAsync(new Instrument(1, "v" + v));
            await table.Writer.CommitAsync();
        }
        Assert.True(held.TryGet(1, out var row));
        Assert.Equal("v1", row.Symbol);
        Assert.Equal(3, table.ChainLength(1));

        // A third revokes it: the rows it was holding are reclaimed without waiting for it…
        await table.Writer.UpsertAsync(new Instrument(1, "v4"));
        await table.Writer.CommitAsync();
        Assert.Equal(1, table.ChainLength(1));

        // …and it finds out on its next read, whether or not the key still has a row.
        Assert.Contains("revoked", Assert.Throws<InvalidOperationException>(() => held.TryGet(1, out _)).Message);
        Assert.Throws<InvalidOperationException>(() => held.TryGet(99, out _));
        Assert.Throws<InvalidOperationException>(() => held.GetManyInto([1], []));
        held.Dispose(); // releasing a revoked pin is not an error

        using var fresh = table.Snapshot();
        Assert.True(fresh.TryGet(1, out row));
        Assert.Equal("v4", row.Symbol);
    }

    [Fact]
    public async Task A_scan_whose_pin_is_revoked_part_way_throws_instead_of_skipping_rows()
    {
        const int rows = 200;
        var table = new SharedTable<int, Instrument>("instruments", i => i.Id, new SharedTableOptions { MaxRetainedVersions = 2 });
        for (var id = 0; id < rows; id++)
            await table.Writer.UpsertAsync(new Instrument(id, "v1"));
        await table.Writer.CommitAsync();

        var pinned = table.AcquirePin();
        using var scan = table.RowsAt(pinned).GetEnumerator();
        Assert.True(scan.MoveNext());

        // The writer rewrites every row until the pin is past the cap. The rows the scan has
        // not reached are now held only as versions newer than its pin, their older ones
        // trimmed away: read as of the pin, those keys look absent.
        for (var v = 2; v <= 6; v++)
        {
            for (var id = 0; id < rows; id++)
                await table.Writer.UpsertAsync(new Instrument(id, "v" + v));
            await table.Writer.CommitAsync();
        }

        var revoked = Assert.Throws<InvalidOperationException>(() =>
        {
            while (scan.MoveNext())
            {
            }
        });
        Assert.Contains("revoked", revoked.Message);
        table.ReleasePin(pinned);
    }

    [Fact]
    public void Retention_windows_must_hold_at_least_one_version()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new SharedTable<int, Instrument>("instruments", i => i.Id, new SharedTableOptions { TailVersions = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new SharedTable<int, Instrument>("instruments", i => i.Id, new SharedTableOptions { MaxRetainedVersions = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new SharedTable<int, Instrument>("instruments", i => i.Id, new SharedTableOptions { TailChanges = 0 }));
    }

    [Fact]
    public void Table_names_cannot_contain_the_offset_key_separator() =>
        Assert.Throws<ArgumentException>(() => new SharedTable<int, Instrument>("a#b", i => i.Id));

    /// <summary>The protocol the design leans on: a writer churning every key while readers
    /// on other threads pin and probe. Version v sets every surviving key's stamp to v and
    /// deletes the keys with (key + v) % 3 == 0, so what any pinned version must contain is
    /// computable from the version number alone — a torn batch, a trimmed-too-early row or a
    /// row from a later version all show up as a wrong stamp or a wrong presence.</summary>
    [Fact]
    public async Task Readers_always_see_exactly_their_pinned_version_under_a_churning_writer()
    {
        const int keys = 512;
        const int versions = 3_000;
        var table = NewTable();
        using var stop = new CancellationTokenSource();
        var failures = new System.Collections.Concurrent.ConcurrentQueue<string>();
        long probes = 0;

        var readers = Enumerable.Range(0, 4).Select(seed => Task.Run(() =>
        {
            var random = new Random(seed);
            while (!stop.IsCancellationRequested && failures.IsEmpty)
            {
                using var snapshot = table.Snapshot();
                var v = snapshot.Version;
                for (var i = 0; i < 64; i++)
                {
                    var key = random.Next(keys);
                    var found = snapshot.TryGet(key, out var row);
                    var expected = v > 0 && (key + v) % 3 != 0;
                    if (found != expected)
                        failures.Enqueue($"version {v} key {key}: found={found}, expected={expected}");
                    else if (found && row!.Stamp != v)
                        failures.Enqueue($"version {v} key {key}: stamp {row.Stamp}");
                    Interlocked.Increment(ref probes);
                }
            }
        })).ToArray();

        for (var v = 1; v <= versions && failures.IsEmpty; v++)
        {
            for (var key = 0; key < keys; key++)
            {
                if ((key + v) % 3 == 0)
                    await table.Writer.DeleteAsync(key);
                else
                    await table.Writer.UpsertAsync(new Instrument(key, "x", v));
            }
            Assert.Equal(v, await table.Writer.CommitAsync());
        }

        stop.Cancel();
        await Task.WhenAll(readers);
        Assert.True(failures.IsEmpty, string.Join("\n", failures.Take(5)));
        Assert.True(Interlocked.Read(ref probes) > 0);

        // Idle again: every chain is back to one entry and deleted keys are gone.
        table.Writer.Trim();
        Assert.Equal(table.Count, table.PhysicalKeyCount);
        for (var key = 0; key < keys; key++)
            Assert.True(table.ChainLength(key) <= 1);
    }

    // ── snapshot sync ──────────────────────────────────────────────────────────────────

    private static Dictionary<int, Instrument> Rows(SharedTable<int, Instrument> table, IEnumerable<int> keys)
    {
        using var snapshot = table.Snapshot();
        var rows = new Dictionary<int, Instrument>();
        foreach (var key in keys)
            if (snapshot.TryGet(key, out var row))
                rows[key] = row;
        return rows;
    }

    [Fact]
    public async Task Replacing_all_rows_upserts_the_snapshot_in_chunks_and_deletes_the_keys_it_omits()
    {
        var table = new SharedTable<int, Instrument>("instruments", i => i.Id, new SharedTableOptions { ReplaceChunkRows = 10 });
        for (var id = 0; id < 30; id++)
            await table.Writer.UpsertAsync(new Instrument(id, "old-" + id));
        Assert.Equal(1, await table.Writer.CommitAsync());

        // Keys 0–9 are gone from the source, 10–19 unchanged, 20–29 changed, 30–34 new.
        var snapshot = Enumerable.Range(10, 25)
            .Select(id => new Instrument(id, id < 20 ? "old-" + id : "new-" + id))
            .ToArray();
        var latest = await table.Writer.ReplaceAllAsync(snapshot);

        // The unchanged chunk minted nothing; the two changed chunks are a version each, and
        // the last carries its five rows together with the ten deletions.
        Assert.Equal(3, latest);
        Assert.Equal(3, table.LatestVersion);
        Assert.Equal(snapshot.Length, table.Count);
        Assert.Equal(snapshot.ToDictionary(i => i.Id), Rows(table, Enumerable.Range(0, 40)));
    }

    [Fact]
    public async Task The_tail_roots_superseded_rows_only_up_to_its_budget_of_changes()
    {
        // Each version in the tail holds the rows it replaced. Bounded in versions alone,
        // the tail would keep a whole resync's worth of them, with no reader to want them.
        var table = new SharedTable<int, Instrument>(
            "instruments", i => i.Id, new SharedTableOptions { ReplaceChunkRows = 500, TailChanges = 1_000 });
        var superseded = await LoadThenReplaceEveryRow(table, rows: 20_000);
        Assert.Equal(80, table.LatestVersion);
        Assert.Equal(2, table.TailCount);

        table.Writer.Trim();
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        Assert.InRange(superseded.Count(row => row.IsAlive), 0, 1_000);
    }

    /// <summary>Loads <paramref name="rows"/> rows, then replaces every one. Returns weak
    /// references to the first generation; a method of its own so that nothing of this
    /// frame keeps those rows alive.</summary>
    private static async Task<WeakReference[]> LoadThenReplaceEveryRow(SharedTable<int, Instrument> table, int rows)
    {
        var first = Enumerable.Range(0, rows).Select(id => new Instrument(id, "first")).ToArray();
        await table.Writer.ReplaceAllAsync(first);
        await table.Writer.ReplaceAllAsync(Enumerable.Range(0, rows).Select(id => new Instrument(id, "second")));
        return first.Select(row => new WeakReference(row)).ToArray();
    }

    [Fact]
    public async Task One_commit_larger_than_the_tails_budget_is_kept_until_the_next_version()
    {
        var table = new SharedTable<int, Instrument>("instruments", i => i.Id, new SharedTableOptions { TailChanges = 4 });
        for (var id = 0; id < 10; id++)
            await table.Writer.UpsertAsync(new Instrument(id, "v1"));
        await table.Writer.CommitAsync();

        // The newest version always stays, whatever its size: it is what a circuit one
        // step behind reads.
        Assert.Equal(1, table.TailCount);

        await table.Writer.UpsertAsync(new Instrument(0, "v2"));
        await table.Writer.CommitAsync();
        Assert.Equal(1, table.TailCount);
    }

    [Fact]
    public async Task Replacing_all_rows_with_what_the_table_already_holds_mints_no_version()
    {
        var table = new SharedTable<int, Instrument>("instruments", i => i.Id, new SharedTableOptions { ReplaceChunkRows = 7 });
        var rows = Enumerable.Range(0, 50).Select(id => new Instrument(id, "S" + id)).ToArray();
        Assert.Equal(8, await table.Writer.ReplaceAllAsync(rows));

        // Equal rows, not the same objects: what a periodic re-sync from the source delivers.
        var again = Enumerable.Range(0, 50).Select(id => new Instrument(id, "S" + id));
        Assert.Equal(8, await table.Writer.ReplaceAllAsync(again));
        Assert.Equal(8, table.LatestVersion);
    }

    [Fact]
    public async Task Every_version_of_a_snapshot_sync_is_a_valid_mix_of_old_and_new_rows_and_the_last_is_the_snapshot()
    {
        var log = new InMemoryTableLog<int, Instrument>();
        var table = await SharedTable<int, Instrument>.OpenAsync(
            "instruments", i => i.Id, log, new SharedTableOptions { ReplaceChunkRows = 8 });
        var old = Enumerable.Range(0, 40).ToDictionary(id => id, id => new Instrument(id, "old-" + id));
        foreach (var row in old.Values)
            await table.Writer.UpsertAsync(row);
        var loaded = await table.Writer.CommitAsync();

        // Every third key dropped, the rest changed, a few added.
        var snapshot = Enumerable.Range(0, 45).Where(id => id % 3 != 0).ToDictionary(id => id, id => new Instrument(id, "new-" + id));
        var latest = await table.Writer.ReplaceAllAsync(ToAsync(snapshot.Values));
        Assert.True(latest - loaded >= 4, "the snapshot should have gone in as several versions");

        // Replay the log: what a reader pinned at each version saw.
        var view = new Dictionary<int, Instrument>(old);
        await foreach (var version in log.ReadAsync(loaded, latest))
        {
            foreach (var change in version.Changes)
            {
                if (change.New is null)
                    view.Remove(change.Key);
                else
                    view[change.Key] = change.New;
            }

            // No row is missing before the last version, and every row is the old or the new one.
            if (version.Version < latest)
                Assert.All(old.Keys, key => Assert.True(view.ContainsKey(key), $"key {key} vanished at version {version.Version}"));
            Assert.All(view, entry => Assert.True(
                entry.Value == old.GetValueOrDefault(entry.Key) || entry.Value == snapshot.GetValueOrDefault(entry.Key),
                $"version {version.Version} holds {entry.Value}, which is in neither the old table nor the snapshot"));
        }
        Assert.Equal(snapshot, view);
        Assert.Equal(snapshot, Rows(table, Enumerable.Range(0, 50)));

        static async IAsyncEnumerable<Instrument> ToAsync(IEnumerable<Instrument> rows)
        {
            foreach (var row in rows)
            {
                await Task.Yield();
                yield return row;
            }
        }
    }

    [Fact]
    public async Task A_snapshot_records_the_source_offsets_it_is_as_of_and_older_deltas_are_dropped()
    {
        var table = NewTable();
        await table.Writer.UpsertAsync(new Instrument(1, "one"), partition: 0, offset: 5);
        await table.Writer.UpsertAsync(new Instrument(2, "two"), partition: 1, offset: 9);
        await table.Writer.CommitAsync();

        await table.Writer.ReplaceAllAsync(
            [new Instrument(1, "one'"), new Instrument(3, "three")],
            new Dictionary<int, long> { [0] = 40, [1] = 3 });

        // Partition 0 moves to the snapshot's position; partition 1 was already past it.
        Assert.Equal(new Dictionary<int, long> { [0] = 40, [1] = 9 }, table.CommittedOffsets);
        Assert.Equal(2, table.Count);

        // A delta the snapshot already includes is dropped; the first one after it applies.
        await table.Writer.UpsertAsync(new Instrument(1, "stale"), partition: 0, offset: 40);
        await table.Writer.DeleteAsync(3, partition: 0, offset: 41);
        await table.Writer.CommitAsync();
        using var snapshot = table.Snapshot();
        Assert.True(snapshot.TryGet(1, out var one));
        Assert.Equal("one'", one.Symbol);
        Assert.False(snapshot.TryGet(3, out _));
        Assert.Equal(41, table.CommittedOffsets[0]);
    }

    [Fact]
    public async Task A_snapshot_that_stops_early_keeps_its_committed_chunks_and_deletes_nothing()
    {
        var table = new SharedTable<int, Instrument>("instruments", i => i.Id, new SharedTableOptions { ReplaceChunkRows = 4 });
        for (var id = 0; id < 10; id++)
            await table.Writer.UpsertAsync(new Instrument(id, "old-" + id));
        await table.Writer.CommitAsync();

        static IEnumerable<Instrument> Broken()
        {
            for (var id = 5; id < 11; id++)
                yield return new Instrument(id, "new-" + id);
            throw new IOException("the source went away");
        }

        await Assert.ThrowsAsync<IOException>(
            () => table.Writer.ReplaceAllAsync(Broken(), new Dictionary<int, long> { [0] = 99 }).AsTask());

        // The first chunk of four landed. The rest did not — but nothing the snapshot had not
        // reached yet was mistaken for omitted, and the offsets are not claimed.
        Assert.Equal(10, table.Count);
        var rows = Rows(table, Enumerable.Range(0, 12));
        Assert.Equal("old-0", rows[0].Symbol);
        Assert.Equal("new-5", rows[5].Symbol);
        Assert.Equal("new-8", rows[8].Symbol);
        Assert.Empty(table.CommittedOffsets);

        // The next sync completes the job, staged leftovers and all.
        await table.Writer.ReplaceAllAsync(Enumerable.Range(5, 6).Select(id => new Instrument(id, "new-" + id)));
        Assert.Equal(Enumerable.Range(5, 6), Rows(table, Enumerable.Range(0, 12)).Keys.Order());
    }
}
