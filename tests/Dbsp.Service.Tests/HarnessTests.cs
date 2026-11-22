using Acme.Contracts;
using Dbsp.Service.Testing;
using static Dbsp.Service.Tests.SupervisorTests;
using AcmeTables = Acme.Contracts.Tables;

namespace Dbsp.Service.Tests;

/// <summary>The test harness plugin authors are given, tested on this project's own definitions.</summary>
public class HarnessTests
{
    [Fact]
    public async Task A_definition_is_fed_stepped_and_read_and_follows_its_tables()
    {
        var instruments = Instruments(10);
        await using var pipeline = await PipelineTestHarness.For<Enrich>()
            .Settings("""{ "minQuantity": 3 }""")
            .Table(AcmeTables.Instruments, instruments)
            .StartAsync();
        Assert.Equal(["trades"], pipeline.Shape.Inputs.Select(i => i.Name));
        Assert.Equal(["out"], pipeline.Shape.Sinks.Select(i => i.Name));

        var trades = Trades(0, 80);
        await pipeline.InsertAsync("trades", trades);
        var sink = pipeline.UpsertSink<long, EnrichedTrade>("out");
        Assert.Empty(sink.Rows);                                        // nothing happens between steps
        await pipeline.StepAsync();
        Assert.Equal(Enrich.Model(trades, instruments, 3), sink.Rows);  // and a step returns with the sink written

        // A table change corrects what was delivered; a removal retracts it.
        instruments[0] = instruments[0] with { Multiplier = 100 };
        var table = pipeline.Table(AcmeTables.Instruments);
        await table.UpsertAsync(instruments[0]);
        await table.DeleteAsync(instruments[1].Id);
        instruments.RemoveAt(1);
        await pipeline.StepAsync();
        Assert.Equal(Enrich.Model(trades, instruments, 3), sink.Rows);
        Assert.Equal(instruments.Count, table.Rows.Count);

        await pipeline.Input<Trade>("trades").RemoveAsync(trades[5]);
        trades.RemoveAt(5);
        await pipeline.StepAsync();
        Assert.Equal(Enrich.Model(trades, instruments, 3), sink.Rows);
    }

    [Fact]
    public async Task Settings_are_bound_and_refused_as_the_service_refuses_them()
    {
        var range = await Assert.ThrowsAsync<ServiceException>(() => PipelineTestHarness.For<Enrich>().Settings("""{ "minQuantity": -1 }""").StartAsync());
        Assert.Contains(range.Details, d => d.Contains("minQuantity"));
        var unknown = await Assert.ThrowsAsync<ServiceException>(() => PipelineTestHarness.For<Enrich>().Settings("""{ "minimum": 1 }""").StartAsync());
        Assert.Contains("do not bind", unknown.Message);
        await Assert.ThrowsAsync<ArgumentException>(() => PipelineTestHarness.For<HarnessTests>().StartAsync());
    }

    [Fact]
    public async Task Dropped_records_are_read_as_dead_letters_or_thrown_by_the_step()
    {
        await using (var lenient = await PipelineTestHarness.For<Fragile>().ErrorPolicy(RecordErrorPolicy.DeadLetter).Workers(2).StartAsync())
        {
            await lenient.InsertAsync("trades", [new Trade(1, 1, 5, "a"), new Trade(2, 1, -5, "a"), new Trade(3, 1, 7, "b")]);
            await lenient.StepAsync();
            var letter = Assert.Single(lenient.DeadLetters);
            Assert.Equal(new Trade(2, 1, -5, "a"), letter.Record);
            Assert.Equal("negative quantity on trade 2", letter.Exception.Message);
            Assert.Equal(new Dictionary<string, long> { ["a"] = 1, ["b"] = 1 }, lenient.UpsertSink<string, long>("out").Rows);
        }

        await using var strict = await PipelineTestHarness.For<Fragile>().StartAsync();
        await strict.InsertAsync("trades", [new Trade(2, 1, -5, "a")]);
        var fault = await Assert.ThrowsAnyAsync<Exception>(() => strict.StepAsync());
        Assert.Contains("negative quantity on trade 2", fault.ToString());
        Assert.Empty(strict.DeadLetters);
    }

    [Fact]
    public async Task Append_sinks_keyed_inputs_and_derived_tables_are_within_reach()
    {
        await using (var big = await PipelineTestHarness.For<BigTrades>().StartAsync())
        {
            var trades = Trades(0, 30);
            await big.InsertAsync("trades", trades.Take(10));
            await big.StepAsync();
            await big.InsertAsync("trades", trades.Skip(10));
            await big.StepAsync();
            Assert.Equal(BigTrades.Model(trades).OrderBy(t => t.Id), big.AppendSink<Trade>("out").Records.OrderBy(t => t.Id));
        }

        await using (var latest = await PipelineTestHarness.For<LatestTrades>().StartAsync())
        {
            var input = latest.KeyedInput<long, Trade>("trades");
            await input.UpsertAsync(new Trade(1, 1, 5, "a"));
            await input.UpsertAsync(new Trade(2, 1, 6, "a"));
            await latest.StepAsync();
            await input.UpsertAsync(new Trade(1, 1, 50, "a"));
            await input.DeleteAsync(2);
            await latest.StepAsync();
            Assert.Equal(new Dictionary<long, Trade> { [1] = new(1, 1, 50, "a") }, latest.UpsertSink<long, Trade>("out").Rows);
        }

        // A derived table: the pipeline writes it, the test reads it.
        await using var positions = await PipelineTestHarness.For<Positions>().StartAsync();
        await positions.InsertAsync("trades", [new Trade(1, 7, 5, "a"), new Trade(2, 7, 6, "a"), new Trade(3, 8, 1, "a")]);
        await positions.StepAsync();
        Assert.Equal(
            new Dictionary<int, Position> { [7] = new(7, 11), [8] = new(8, 1) },
            positions.Table(AcmeTables.Positions).Rows);
    }

    [Fact]
    public async Task A_table_slot_can_be_bound_to_a_table_of_another_name_and_wrong_slots_are_named()
    {
        await using var pipeline = await PipelineTestHarness.For<Enrich>()
            .Table(AcmeTables.Instruments, Instruments(3), name: "instruments-eu")
            .BindTable("instruments", "instruments-eu")
            .StartAsync();
        Assert.Equal(["instruments-eu"], pipeline.Shape.TablesRead);
        await pipeline.InsertAsync("trades", Trades(0, 3));
        await pipeline.StepAsync();
        Assert.Equal(3, pipeline.UpsertSink<long, EnrichedTrade>("out").Rows.Count);
        Assert.Equal(3, pipeline.Table(AcmeTables.Instruments, "instruments-eu").Rows.Count);

        Assert.Contains("declares no input slot 'orders' (it declares: trades)", Assert.Throws<ArgumentException>(() => pipeline.Input<Trade>("orders")).Message);
        Assert.Contains("Input slot 'trades' is Input<Trade>, not Input<Int32>", Assert.Throws<ArgumentException>(() => pipeline.Input<int>("trades")).Message);
        Assert.Contains("Sink slot 'out' is UpsertSink, not AppendSink", Assert.Throws<ArgumentException>(() => pipeline.AppendSink<Trade>("out")).Message);
        Assert.Contains("has no table 'instruments'", Assert.Throws<ArgumentException>(() => pipeline.Table(AcmeTables.Instruments)).Message);
    }
}
