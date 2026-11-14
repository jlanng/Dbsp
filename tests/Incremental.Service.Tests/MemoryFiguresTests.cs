using Incremental.Kafka;
using Acme.Contracts;
using Instrument = Acme.Contracts.Instrument;
using static Incremental.Service.Tests.SupervisorTests;
using static Incremental.Service.Tests.TestService;

namespace Incremental.Service.Tests;

/// <summary>What an operator is told a node holds: a table's rows, indexes and history, a
/// pipeline's state, and the process around them — on a leader and on a standby.</summary>
public class MemoryFiguresTests
{
    [Fact]
    public async Task A_table_says_what_it_holds_and_the_history_its_log_keeps_in_this_process_is_counted()
    {
        await using var service = await StartAsync();
        var table = await service.DeclareInstrumentsAsync(Enumerable.Range(1, 2000).Select(i => new Instrument(i, "SYM" + i, "ISIN" + i.ToString("D8"), 1)));
        var loaded = service.Tables.Stats("instruments").Memory!;
        Assert.Equal(2000, loaded.Rows);
        Assert.InRange(loaded.SampledRows, 256, 2000);
        // An Instrument: the object and two strings of its own.
        Assert.InRange(loaded.MeanRowBytes, 100, 200);
        Assert.InRange(loaded.RowBytes, (long)(2000 * loaded.MeanRowBytes), (long)(2000 * (loaded.MeanRowBytes + 150)));
        Assert.Equal(loaded.RowBytes + loaded.IndexBytes + loaded.HistoryBytes, loaded.TotalBytes);

        // Every row replaced, thirty-six versions over: the in-memory log keeps the
        // replaced rows of every version it retains, which is more than the table's own
        // tail of recent changes (65,536 of them) would say.
        for (var round = 0; round < 36; round++)
        {
            for (var i = 1; i <= 2000; i++)
                await table.Writer.UpsertAsync(new Instrument(i, "SYM" + i, "ISIN" + i.ToString("D8"), 2 + round));
            await table.Writer.CommitAsync();
        }
        var own = table.EstimateMemory();
        Assert.InRange(own.HistoryChanges, 60_000, 68_000);
        var revised = service.Tables.Stats("instruments").Memory!;
        Assert.Equal(2000, revised.Rows);
        Assert.Equal(74_000, revised.HistoryChanges);
        Assert.InRange(revised.HistoryBytes, (long)(74_000 * revised.MeanRowBytes), (long)(74_000 * (revised.MeanRowBytes + 64)));
        Assert.True(revised.HistoryBytes > 20 * revised.RowBytes, "thirty-six versions of history outweigh the rows");
        Assert.Equal(own.RowBytes, revised.RowBytes);
    }

    [Fact]
    public async Task A_pipeline_says_what_its_state_holds_on_the_leader_and_on_a_standby()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        await using var a = await StartAsync(cluster, "a", memory);
        memory.Log<Trade>("in").AppendRange(Enumerable.Range(0, 3000).Select(i => new Trade(i, i % 10, 1, "trader-" + i % 700)));
        await a.DeployAsync(Spec("p", "counts", "in", "out"));
        var sink = memory.UpsertSink<string, long>("out");
        await Eventually(() => sink.Rows.Values.Sum() == 3000, "the pipeline's output");
        await Eventually(() => a.Pipelines.Get("p").StateSize is { Entries: 700 }, "the state's entries, counted");

        // Entries are sized as a commit writes them: the last step's commit first.
        await a.Settled();
        var onLeader = a.Pipelines.Get("p").StateSize!;
        Assert.Equal(0, onLeader.EntriesAsBytes);
        Assert.Equal(0, onLeader.EntriesHeldTwice);
        var store = Assert.Single(onLeader.Stores, s => s.Entries > 0);
        Assert.Contains("count", store.Name);
        Assert.True(store.SampledEntries >= 256, $"{store.SampledEntries} entries sized");
        // A trader's name and a count: the string, and a slot of the store's map.
        Assert.InRange(store.MeanEntryBytes, 32, 64);
        Assert.InRange(store.MeanRecordBytes, 10, 60);
        Assert.InRange(onLeader.EstimatedBytes, 700 * 40, 700 * 300);

        // The standby holds the same state: first as the bytes it followed, then — once it
        // has built the circuit on them — typed, as the leader holds it.
        await using var b = await StartAsync(cluster, "b", memory);
        await Eventually(() => b.Pipelines.Pipelines.Count == 1 && b.Pipelines.Get("p").State == PipelineState.Built, "the standby building the circuit");
        var onStandby = b.Pipelines.Get("p").StateSize!;
        Assert.Equal(700, onStandby.Entries);
        Assert.Equal(0, onStandby.EntriesAsBytes);
        Assert.Equal(0, onStandby.EntriesHeldTwice); // the leader's same types have checkpointed
        Assert.InRange(onStandby.EstimatedBytes, onLeader.EstimatedBytes / 2, onLeader.EstimatedBytes * 2);
    }

    [Fact]
    public async Task A_pipeline_started_on_restored_state_and_left_idle_does_not_go_on_holding_it_twice()
    {
        // The cluster stands in for durable storage: it outlives the node, as a broker would.
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        long sequence;
        await using (var first = await StartAsync(cluster, "a", memory))
        {
            memory.Log<Trade>("in").AppendRange(Enumerable.Range(0, 2000).Select(i => new Trade(i, i % 10, 1, "trader-" + i % 500)));
            await first.DeployAsync(Spec("p", "counts", "in", "out"));
            var counted = memory.UpsertSink<string, long>("out");
            await Eventually(() => counted.Rows.Values.Sum() == 2000, "the first run's output");
            await Eventually(() => first.Pipelines.Get("p").StateSize is { Entries: 500 }, "the state");
        }

        // A node started from nothing reads the state as bytes and builds the circuit on
        // them: typed, with the bytes kept beside it until those types commit. Nothing
        // arrives, so no step would ever commit — the circuit commits once by itself.
        await using var service = await StartAsync(cluster, "a", memory);
        var provider = service.Node.Context.Storage.Pipeline("p");
        sequence = provider.CommitSequence;
        await service.Running("p");
        var started = service.Pipelines.Get("p").StateSize!;
        Assert.Equal(500, started.Entries);
        if (provider.CommitSequence == sequence)
        {
            Assert.Equal(500, started.EntriesHeldTwice);
            Assert.True(started.BytesHeldTwice > 500 * 20, $"{started.BytesHeldTwice} bytes held twice");
        }

        await Eventually(() => service.Pipelines.Get("p").StateSize is { EntriesHeldTwice: 0, Entries: 500 } && provider.CommitSequence > sequence,
            "the idle pipeline's confirming commit", TimeSpan.FromSeconds(20));
        Assert.Equal(sequence + 1, provider.CommitSequence);
        Assert.Equal(0, service.Pipelines.Get("p").Restarts);
        Assert.Equal(started.EstimatedBytes - started.BytesHeldTwice, service.Pipelines.Get("p").StateSize!.EstimatedBytes, tolerance: 2048.0);

        // And it carries on from the state it was started on.
        memory.Log<Trade>("in").Append(new Trade(9000, 1, 1, "trader-1"));
        var sink = memory.UpsertSink<string, long>("out");
        await Eventually(() => sink.Rows.GetValueOrDefault("trader-1") == 5, "the next record on the restored count");
    }

    [Fact]
    public void The_process_says_what_it_holds_and_how_its_collector_is_set()
    {
        // What was live "at the last collection" wants one to have happened.
        GC.Collect();
        var memory = ProcessMemory.Read();
        Assert.True(memory.WorkingSetBytes > memory.GcLiveBytes && memory.GcLiveBytes > 0, memory.ToString());
        Assert.True(memory.GcCommittedBytes >= memory.GcLiveBytes, memory.ToString());
        Assert.NotNull(memory.Gc);
        Assert.Equal(System.Runtime.GCSettings.IsServerGC, memory.Gc.Server);
        if (OperatingSystem.IsLinux())
        {
            Assert.NotNull(memory.OtherBytes);
            Assert.InRange(memory.OtherBytes.Value, 0, memory.WorkingSetBytes);
        }

        // The whole of it survives the probe line, which is how a node learns its peer's.
        var peer = new PeerInfo("b", NodeRole.Standby, 3, true, null, null, memory with { GcPauseRecentPercent = 1.25 });
        Assert.Equal(peer, PeerDetail.Read("b", NodeRole.Standby, 3, PeerDetail.Write(peer)));
        Assert.Equal("server, 8 heaps, concurrent, conserve 7", new GcSettingsInfo(true, true, 8, 7, 0, false).ToString());
        Assert.Equal("workstation, not concurrent, limit 4096 MB", new GcSettingsInfo(false, false, 0, 0, 4096L << 20, false).ToString());

        // The recent pause share needs two readings a second apart.
        Thread.Sleep(1100);
        var later = ProcessMemory.Read();
        Assert.NotNull(later.GcPauseRecentPercent);
        Assert.InRange(later.GcPauseRecentPercent.Value, 0, 100);
    }
}
