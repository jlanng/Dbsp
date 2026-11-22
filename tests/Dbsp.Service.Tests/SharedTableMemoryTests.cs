using Acme.Contracts;
using static Dbsp.Service.Tests.SupervisorTests;
using static Dbsp.Service.Tests.TestService;

namespace Dbsp.Service.Tests;

/// <summary>Tests that read the whole process's heap run with nothing beside them:
/// what another class allocates or lets go between two readings is in the difference.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class MeasuredAloneCollection
{
    public const string Name = "measured alone";
}

/// <summary>
/// One copy of a table's rows, however many pipelines join it — held by what the
/// collector says the process holds before the table, with it, and with twelve circuits
/// on it. In a class that ran beside the others the readings were not this test's alone:
/// on a runner the table of 200,000 rows once measured 10.2 MB, where alone it measures
/// 89.6 MB to within a kilobyte — another class had let go of the difference between
/// the two readings.
/// </summary>
[Collection(MeasuredAloneCollection.Name)]
public class SharedTableMemoryTests
{
    [Fact]
    public async Task Many_pipelines_share_one_large_table_with_one_copy_of_its_rows()
    {
        const int rows = 200_000, pipelines = 12, tradesEach = 400;
        await using var service = await StartAsync();

        var empty = GC.GetTotalMemory(forceFullCollection: true);
        var table = await service.DeclareInstrumentsAsync(Instruments(rows));
        var loaded = GC.GetTotalMemory(forceFullCollection: true);
        var tableBytes = loaded - empty;
        Assert.Equal(rows, table.Count);
        Assert.True(tableBytes > 20_000_000, $"the table is only {tableBytes} bytes; the comparison below would mean nothing");

        var trades = new List<Trade>[pipelines];
        for (var p = 0; p < pipelines; p++)
        {
            // Each pipeline's trades touch a different slice of the table.
            trades[p] = Enumerable.Range(0, tradesEach).Select(i => new Trade(i, 1 + (p * 9973 + i * 31) % rows, 2, "t")).ToList();
            await service.DeployAsync(Spec($"enrich-{p}", "enrich", $"trades-{p}", $"sink-{p}"));
            service.Memory.Log<Trade>($"trades-{p}").AppendRange(trades[p]);
        }

        using var snapshot = table.Snapshot();
        for (var p = 0; p < pipelines; p++)
        {
            var sink = service.Memory.UpsertSink<long, EnrichedTrade>($"sink-{p}");
            await Eventually(() => sink.Rows.Count == tradesEach, $"pipeline {p} delivering {tradesEach} rows (it has {sink.Rows.Count})");
            foreach (var trade in trades[p])
            {
                Assert.True(snapshot.TryGet(trade.InstrumentId, out var instrument));
                var row = sink.Rows[trade.Id];
                Assert.Equal(new EnrichedTrade(trade.Id, instrument.Symbol, trade.Quantity * instrument.Multiplier), row);

                // Identity, not just equality: the string every pipeline delivered is the
                // table row's own, so each joined the one copy rather than one of its own.
                Assert.Same(instrument.Symbol, row.Symbol);
            }
        }

        // Twelve circuits joined against the table cost a fraction of one copy of it.
        var withPipelines = GC.GetTotalMemory(forceFullCollection: true);
        Assert.True(
            withPipelines - loaded < tableBytes / 2,
            $"{pipelines} pipelines added {withPipelines - loaded} bytes on top of a {tableBytes}-byte table");
        await Eventually(() => service.Tables.Stats("instruments").Readers.Count == pipelines, "every pipeline committing a version of the table");
    }
}
