using Acme.Contracts;
using static Dbsp.Service.Tests.SupervisorTests;
using static Dbsp.Service.Tests.TestService;

namespace Dbsp.Service.Tests;

/// <summary>The memory connector's log with retention: what a demo host left running holds.</summary>
public class MemoryLogTests
{
    [Fact]
    public void Log_with_RetainLast_holds_a_bounded_number_of_records_and_keeps_their_offsets()
    {
        var log = new MemoryLog<int> { RetainLast = 100 };
        for (var i = 0; i < 10_000; i++)
        {
            log.Append(i);
            Assert.InRange(log.Retained, 1, 200);
        }
        Assert.Equal(10_000, log.Count);
        Assert.InRange(log.Retained, 100, 200);

        // A reader at a record that is still held gets it, under its own offset.
        var (records, first, _) = log.ReadFrom(9_950);
        Assert.Equal(9_950, first);
        Assert.Equal(Enumerable.Range(9_950, 50), records);

        // A reader behind the oldest record held starts at the oldest, and is told where that is.
        (records, first, _) = log.ReadFrom(0);
        Assert.Equal(10_000 - log.Retained, first);
        Assert.Equal((int)first, records[0]);
        Assert.Equal(log.Retained, records.Count);

        // Without it nothing is dropped: a test replays from 0.
        var whole = new MemoryLog<int>();
        whole.AppendRange(Enumerable.Range(0, 5_000));
        Assert.Equal(5_000, whole.Retained);
        Assert.Equal(0, whole.ReadFrom(0).First);
    }

    [Fact]
    public async Task Pipeline_deployed_on_a_trimmed_log_reads_from_the_oldest_record_held_with_true_offsets()
    {
        var memory = new MemoryConnector { RetainLast = 50 };
        await using var service = await StartAsync(memory: memory);
        var log = memory.Log<Trade>("in");
        Assert.Equal(50, log.RetainLast);
        log.AppendRange(Trades(0, 1_000));
        var held = log.Retained;
        Assert.InRange(held, 50, 100);

        await service.DeployAsync(Spec("p", "counts", "in", "out"));
        var sink = memory.UpsertSink<string, long>("out");
        await Eventually(() => sink.Rows.Values.Sum() == held, "the records still held being counted");
        Assert.Equal(999, service.Pipelines.Get("p").Sources.Single().Stats.Positions![0]);
        Assert.Equal(0, service.Pipelines.Get("p").Sources.Single().Stats.Lag);
    }
}
