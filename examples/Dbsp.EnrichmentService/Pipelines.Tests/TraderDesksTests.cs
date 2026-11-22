using Enrichment.Contracts;
using Dbsp.Service.Testing;

namespace Enrichment.Pipelines.Tests;

public class TraderDesksTests
{
    private static readonly Desk[] Desks = [new(1, "Macro", "EMEA"), new(2, "Rates", "APAC"), new(3, "Credit", "AMER")];

    [Fact]
    public async Task It_writes_traders_joined_to_desks_and_holds_one_entry_per_trader_however_often_they_move()
    {
        await using var pipeline = await PipelineTestHarness.For<TraderDesks>()
            .Table(Reference.Traders, Enumerable.Range(1, 20).Select(i => new Trader(i, $"T{i}", 1)))
            .Table(Reference.Desks, Desks)
            .Table(Reference.TraderDesks)
            .StartAsync();
        await pipeline.StepAsync();
        var written = pipeline.Table(Reference.TraderDesks);
        Assert.Equal(20, written.Rows.Count);
        var start = pipeline.StateSize.Entries;

        // Traders move desk, a thousand times over: the join's facts are the traders
        // table's rows, one each, so the state stays where it started.
        for (var move = 0; move < 1000; move++)
        {
            await pipeline.Table(Reference.Traders).UpsertAsync(new Trader(1 + move % 20, $"T{1 + move % 20}", 1 + move % 3));
            if (move % 50 == 49)
                await pipeline.StepAsync();
        }
        Assert.Equal(start, pipeline.StateSize.Entries);
        Assert.Equal("Macro", written.Rows[20].Desk);           // trader 20's last move: 999 % 3 = 0, desk 1
        Assert.InRange(start, 20, 20);
    }
}
