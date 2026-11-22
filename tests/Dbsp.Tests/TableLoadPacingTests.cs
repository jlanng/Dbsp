using Dbsp.Tables;

namespace Dbsp.Tests;

/// <summary>The writer's pacing of a large load (item 61; docs/design/enrichment-service.md,
/// S9). What it does to the collector's pauses is measured on the scale rig; these hold
/// when it acts and when it does not.</summary>
public class TableLoadPacingTests
{
    private sealed record Row(int Id, string Payload);

    private static SharedTable<int, Row> Table(TableLoadPacing? pacing) =>
        new("rows", r => r.Id, new SharedTableOptions { LoadPacing = pacing });

    private static async Task LoadAsync(SharedTable<int, Row> table, int from, int count, int batch)
    {
        for (var id = from; id < from + count; id++)
        {
            await table.Writer.UpsertAsync(new Row(id, new string('x', 200)));
            if ((id - from + 1) % batch == 0)
                await table.Writer.CommitAsync();
        }
        await table.Writer.CommitAsync();
    }

    [Fact]
    public async Task A_large_load_is_taken_in_steps()
    {
        var table = Table(new TableLoadPacing { LargeAfterChanges = 1_000, StepBytes = 1 << 20, MemoryLoadCeiling = double.MaxValue });
        await LoadAsync(table, 0, 20_000, 500);

        var paced = table.Writer.Pacing;
        Assert.True(paced.Steps > 0, "a load past LargeAfterChanges asked for no young collection");
        Assert.Equal(0, paced.Skipped);
        Assert.Equal(20_000, table.Count);
    }

    [Fact]
    public async Task Commits_below_the_size_of_a_large_load_are_left_alone()
    {
        var table = Table(new TableLoadPacing { LargeAfterChanges = 50_000, StepBytes = 1, MemoryLoadCeiling = double.MaxValue });
        await LoadAsync(table, 0, 20_000, 500);
        Assert.Equal(0, table.Writer.Pacing.Steps);
    }

    [Fact]
    public async Task A_quiet_gap_ends_the_run()
    {
        // Two halves of a "large" load, each below the size on its own, with a gap longer
        // than QuietGap between them: neither run is large.
        var table = Table(new TableLoadPacing
        {
            LargeAfterChanges = 15_000, StepBytes = 1, QuietGap = TimeSpan.FromMilliseconds(100), MemoryLoadCeiling = double.MaxValue,
        });
        await LoadAsync(table, 0, 10_000, 500);
        await Task.Delay(300);
        await LoadAsync(table, 10_000, 10_000, 500);
        Assert.Equal(0, table.Writer.Pacing.Steps);
    }

    [Fact]
    public async Task A_table_that_changes_steadily_is_never_a_large_load()
    {
        // A CDC table or a pipeline's output: small commits that never pause for
        // QuietGap. Their sum passes LargeAfterChanges many times over, and the process
        // allocates for other reasons meanwhile; a trickle is not a load, however long.
        var table = Table(new TableLoadPacing
        {
            LargeAfterChanges = 1_000, StepBytes = 1 << 20, QuietGap = TimeSpan.FromMilliseconds(200), MemoryLoadCeiling = double.MaxValue,
        });
        var kept = new List<byte[]>();
        for (var commit = 0; commit < 120; commit++)
        {
            for (var i = 0; i < 20; i++)
                await table.Writer.UpsertAsync(new Row(i, $"v{commit}"));
            await table.Writer.CommitAsync();
            kept.Clear();
            for (var i = 0; i < 32; i++)
                kept.Add(new byte[64 << 10]); // 2 MB that has nothing to do with the table
            await Task.Delay(20);
        }
        Assert.Equal(0, table.Writer.Pacing.Steps);

        // And a load that follows the trickle is paced as any other.
        await LoadAsync(table, 1_000, 20_000, 500);
        Assert.True(table.Writer.Pacing.Steps > 0, "a load after a trickle asked for no young collection");
    }

    [Fact]
    public async Task Tables_loading_at_once_share_one_pace()
    {
        // Each writer measures the same process-wide allocation: without one shared
        // mark, N tables loading together ask for N collections per step.
        var pacing = new TableLoadPacing { LargeAfterChanges = 1_000, StepBytes = 4 << 20, MemoryLoadCeiling = double.MaxValue };
        var tables = Enumerable.Range(0, 4).Select(_ => Table(pacing)).ToArray();
        var before = GC.GetTotalAllocatedBytes(precise: true);
        for (var id = 0; id < 40_000; id++)
        {
            foreach (var table in tables)
                await table.Writer.UpsertAsync(new Row(id, new string('x', 200)));
            if ((id + 1) % 500 == 0)
                foreach (var table in tables)
                    await table.Writer.CommitAsync();
        }
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - before;
        var steps = tables.Sum(table => table.Writer.Pacing.Steps);
        Assert.True(steps > 0);
        Assert.True(steps <= allocated / pacing.StepBytes + 1,
            $"{steps} young collections asked for over {allocated >> 20} MB allocated, at a step of {pacing.StepBytes >> 20} MB");
    }

    [Fact]
    public async Task Above_the_memory_ceiling_nothing_is_asked_for()
    {
        // A ceiling of zero: memory is always above it. The collector's answer to a
        // collection asked for under memory pressure is a full blocking one.
        GC.Collect(0); // so that the collector has a memory load to report
        var table = Table(new TableLoadPacing { LargeAfterChanges = 1_000, StepBytes = 1 << 20, MemoryLoadCeiling = 0 });
        await LoadAsync(table, 0, 20_000, 500);

        var paced = table.Writer.Pacing;
        Assert.Equal(0, paced.Steps);
        Assert.True(paced.Skipped > 0);
    }

    [Fact]
    public async Task Pacing_off_does_nothing_and_the_default_is_on()
    {
        Assert.Same(TableLoadPacing.Default, new SharedTableOptions().LoadPacing);
        var table = Table(null);
        await LoadAsync(table, 0, 20_000, 500);
        Assert.Equal(default, table.Writer.Pacing);
    }
}
