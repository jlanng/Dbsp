using System.Text.Json;
using Acme.Contracts;
using static Incremental.Service.Tests.SupervisorTests;
using static Incremental.Service.Tests.TestService;

namespace Incremental.Service.Tests;

/// <summary>
/// A pipeline with two inputs: both sources resume from the one checkpoint that holds both
/// their positions — after a restart of the service, and after its leader is replaced with
/// both feeds still moving.
/// </summary>
public class TwoInputTests
{
    private static bool Same<TKey, TRow>(IReadOnlyDictionary<TKey, TRow> actual, IReadOnlyDictionary<TKey, TRow> expected) where TKey : notnull =>
        actual.Count == expected.Count && expected.All(e => actual.TryGetValue(e.Key, out var row) && Equals(row, e.Value));

    // Three fills for every other trade, in an order of their own.
    private static List<Fill> Fills(int from, int count) =>
        Enumerable.Range(from, count).Select(i => new Fill(i, i * 7 % 400, 10 + i % 13)).ToList();

    private static PipelineSpec Spec => TestService.Spec("fills", "trade-fills") with
    {
        Inputs = new Dictionary<string, JsonElement> { ["trades"] = MemoryBinding("trades"), ["fills"] = MemoryBinding("fills") },
        Sinks = new Dictionary<string, JsonElement> { ["filled"] = MemoryBinding("filled"), ["fill-counts"] = MemoryBinding("fill-counts") },
    };

    private static async Task SinksEqualAsync(MemoryConnector memory, List<Trade> trades, List<Fill> fills, string when)
    {
        var filled = memory.UpsertSink<long, FilledTrade>("filled");
        var counts = memory.UpsertSink<long, long>("fill-counts");
        await Eventually(() => Same(filled.Rows, TradeFills.Filled(trades, fills)), $"the join's rows {when}");
        await Eventually(() => Same(counts.Rows, TradeFills.Counts(fills)), $"the fill counts {when}");
    }

    private static Dictionary<string, long> Read(TestService service) =>
        service.Pipelines.Get("fills").Sources.ToDictionary(s => s.Slot, s => s.Stats.Records);

    [Fact]
    public async Task Both_sources_resume_from_their_own_positions_in_one_checkpoint_after_a_restart()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        var (tradeLog, fillLog) = (memory.Log<Trade>("trades"), memory.Log<Fill>("fills"));
        var trades = Trades(0, 200);
        var fills = Fills(0, 150);

        await using (var first = await StartAsync(cluster, "a", memory))
        {
            await first.DeployAsync(Spec);
            tradeLog.AppendRange(trades);
            fillLog.AppendRange(fills);
            await SinksEqualAsync(memory, trades, fills, "before the restart");
            await Eventually(
                () => first.Pipelines.Get("fills").CommittedOffsets is var c && c.GetValueOrDefault("trades#p0", -1) == 199 && c.GetValueOrDefault("fills#p0", -1) == 149,
                "one checkpoint holding both inputs' positions");
        }

        // Down. The two feeds move on by different amounts.
        var (moreTrades, moreFills) = (Trades(200, 100), Fills(150, 130));
        tradeLog.AppendRange(moreTrades);
        fillLog.AppendRange(moreFills);
        trades.AddRange(moreTrades);
        fills.AddRange(moreFills);

        await using var second = await StartAsync(cluster, "a", memory);
        await second.Running("fills");
        await SinksEqualAsync(memory, trades, fills, "after the restart");
        // Each source read what was new on its own input, and nothing again.
        Assert.Equal(new Dictionary<string, long> { ["trades"] = 100, ["fills"] = 130 }, Read(second));
    }

    [Fact]
    public async Task Both_sources_carry_on_under_a_new_leader_that_takes_over_while_both_feeds_move()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        var (tradeLog, fillLog) = (memory.Log<Trade>("trades"), memory.Log<Fill>("fills"));
        var trades = Trades(0, 400);
        var fills = Fills(0, 900);

        await using var a = await StartAsync(cluster, "a", memory);
        await using var b = await StartAsync(cluster, "b", memory);
        await a.DeployAsync(Spec);
        tradeLog.AppendRange(trades.Take(100));
        fillLog.AppendRange(fills.Take(200));
        await SinksEqualAsync(memory, trades.Take(100).ToList(), fills.Take(200).ToList(), "under the first leader");
        await Eventually(() => b.Node.Status.Standby is { TakeoverWouldBeWarm: true }, "the standby being warm");

        // Both feeds keep moving, at different paces, across the change of leader.
        var feeding = Task.Run(async () =>
        {
            for (var i = 0; i < 30; i++)
            {
                tradeLog.AppendRange(trades.Skip(100 + i * 10).Take(10));
                fillLog.AppendRange(fills.Skip(200 + i * 20).Take(20));
                await Task.Delay(5);
            }
        });
        await Task.Delay(40);
        await b.Node.TakeOverAsync(new NodeActionOptions(), default);
        await feeding;
        fillLog.AppendRange(fills.Skip(800));
        await b.Running("fills");

        await SinksEqualAsync(memory, trades, fills, "under the second leader");
        var read = Read(b);
        Assert.InRange(read["trades"], 1, 399);         // resumed: not the whole of either log
        Assert.InRange(read["fills"], 100, 899);
        await Eventually(
            () => b.Pipelines.Get("fills").CommittedOffsets is var c && c.GetValueOrDefault("trades#p0", -1) == 399 && c.GetValueOrDefault("fills#p0", -1) == 899,
            "the new leader's checkpoint holding both positions");
    }
}
