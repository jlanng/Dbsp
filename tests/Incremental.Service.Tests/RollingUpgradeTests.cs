using System.Text.Json;
using Acme.Contracts;
using Incremental.Kafka;
using Incremental.Storage;
using static Incremental.Service.Tests.SupervisorTests;
using static Incremental.Service.Tests.TestService;

namespace Incremental.Service.Tests;

/// <summary>
/// A rolling upgrade across a change of a persisted format, on Kafka: two nodes of
/// different format levels (<see cref="PersistedFormat"/>) — the lower one standing for
/// the build from before the change — taken through the upgrade an operator does. The
/// standby is replaced by the newer build; it takes over; the older node comes back as
/// its standby and is then replaced too; the leader raises the level when it hears that
/// its standby reads it; and leadership moves once more at the higher level. Trades and
/// ticks keep arriving throughout, a reference row changes, and after every stage the
/// sinks are compared with the model.
/// <para>
/// Run for the real change (level 1 → 2: state manifest format 7 → 8, the partitions'
/// event times in the checkpoint) and for a change of the format byte of every state-log
/// value and table-log record (level 2 → the proving level).
/// </para>
/// </summary>
[Collection(KafkaServiceCollection.Name)]
public class RollingUpgradeTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Timeout = KafkaTestEnv.Timeout;

    private static JsonElement KafkaBinding(string topic) =>
        Json($$"""{ "connector": "kafka", "connection": "log", "topic": "{{topic}}" }""");

    private static Task WarmAsync(TestService standby) =>
        Eventually(
            () => standby.Node.Status.Standby is { TakeoverWouldBeWarm: true, StateLagRecords: 0 },
            $"the standby to be warm (it says: {string.Join(", ", standby.Node.Status.Standby?.NotWarmBecause ?? ["not a standby"])})");

    /// <summary>Every running pipeline has committed what its Kafka sources have read (a
    /// Kafka source's position is the next offset; the checkpoint holds the last).</summary>
    private static Task CommittedAsync(TestService leader) => Eventually(
        () => leader.Pipelines.Pipelines.Where(p => p.State == PipelineState.Running).Select(p => leader.Pipelines.Get(p.Id)).All(status =>
            status.Sinks.All(sink => sink.WriteInFlightSince is null)
            && status.Sources.All(source => (source.Stats.Positions ?? new Dictionary<int, long>()).All(at =>
                status.CommittedOffsets.GetValueOrDefault($"{source.Slot}#p{at.Key}", -1) >= at.Value - 1))),
        "every running pipeline to have committed what it has read");

    private static ChangelogStorageProvider State(TestService node, string id) => node.Node.Context.Storage.Pipeline(id);

    public static TheoryData<int, int> Pairs() => new()
    {
        { FormatLevel.Base, FormatLevel.PartitionTimes },
        { FormatLevel.Current, FormatLevel.Proving },
    };

    [Theory]
    [MemberData(nameof(Pairs))]
    public async Task Standby_upgrade_takeover_and_leader_upgrade_leave_the_sinks_equal_to_the_model(int low, int high)
    {
        await using var env = KafkaTestEnv.Create();
        var tradesTopic = await env.CreateTopicAsync("trades", partitions: 2);
        var instrumentsTopic = await env.CreateTopicAsync("instruments", compacted: true);
        var enrichedTopic = await env.CreateTopicAsync("enriched", compacted: true);
        var ticksTopic = await env.CreateTopicAsync("ticks", partitions: 3);
        var liveTopic = await env.CreateTopicAsync("live", compacted: true);
        var finalTopic = await env.CreateTopicAsync("final", compacted: true);
        var ports = KafkaTestEnv.FreePorts(2);

        Task<TestService> NodeAsync(string id, int level, bool lead = false) => env.StartNodeAsync(
            id, lead: lead, probePort: ports[id == "a" ? 0 : 1], peerPort: ports[id == "a" ? 1 : 0],
            formats: PersistedFormat.ForTesting(level));

        // The feed: trades against a table, and ticks in ten-second windows over three
        // partitions. Nothing is late.
        var instruments = Instruments(10);
        var trades = new List<Trade>();
        var ticks = new List<Tick>();
        var tickId = 0L;
        async Task FeedAsync(string what)
        {
            var more = Trades(trades.Count, 150);
            trades.AddRange(more);
            foreach (var trade in more)
                env.ProduceJson(tradesTopic, trade.Id, trade);
            // A feed's ticks span a second and a half of event time, inside the lateness:
            // they arrive at once, the source reads them a partition at a time, and after
            // a quiet five seconds an idle partition holds no watermark back. Between
            // feeds event time jumps, so windows close while the upgrade goes on.
            var from = ticks.Count == 0 ? 0 : ticks[^1].AtMs + 2_500;
            for (var ms = from; ms < from + 1_500; ms += 30)
            {
                var tick = new Tick(++tickId, $"t{ms / 30 % 7}", ms, 1 + ms % 5);
                ticks.Add(tick);
                env.ProduceJson(ticksTopic, tick.Trader, tick);
            }
            instruments[trades.Count / 150 % 10] = instruments[trades.Count / 150 % 10] with { Symbol = "S" + trades.Count };
            foreach (var instrument in instruments)
                env.ProduceJson(instrumentsTopic, instrument.Id, instrument);
            await env.FlushAsync();
            await KafkaTestEnv.TopicEqualsAsync(enrichedTopic, Enrich.Model(trades, instruments), $"the trades enriched, {what}");
            await KafkaTestEnv.TopicEqualsAsync(liveTopic, WindowedTotals.Model(ticks), $"the windows, {what}");
        }

        bool HoldsHigh(TestService leader)
        {
            if (high >= FormatLevel.Proving)
            {
                var rows = KafkaTestEnv.ReadTopic($"{env.Prefix}.table.instruments.rows").Count(r => r.Message.Value is { Length: > 0 } v && v[0] == FormatLevel.EnvelopeByte);
                var state = KafkaTestEnv.ReadTopic($"{env.Prefix}.state").Count(r => r.Message.Value is { Length: > 0 } v && v[0] == FormatLevel.EnvelopeByte);
                Assert.Equal(rows > 0, state > 0);
                return rows > 0;
            }
            var manifest = State(leader, "windowed").CommittedManifest("local")!;
            var keys = State(leader, "windowed").CommittedOffsets.Keys.Any(k => k.StartsWith("$watermark#s0#m", StringComparison.Ordinal));
            Assert.Equal(keys, manifest.StartsWith("format=8\n", StringComparison.Ordinal));
            return keys;
        }

        var nodes = new List<TestService>();
        try
        {
            // Both nodes are the older build.
            var a = await NodeAsync("a", low, lead: true);
            nodes.Add(a);
            foreach (var instrument in instruments)
                env.ProduceJson(instrumentsTopic, instrument.Id, instrument);
            await env.FlushAsync();
            await a.Tables.DeclareAsync(new TableSpec
            {
                Name = "instruments",
                Definition = typeof(InstrumentsTable).FullName!,
                Loader = new TableLoaderSpec("kafka", "log", Json($$"""{ "topic": "{{instrumentsTopic}}" }""")),
            }, default);
            await Eventually(() => a.Tables.Stats("instruments").Rows == instruments.Count, "the loader to fill the table");
            await a.DeployAsync(Spec("enrich", "enrich") with
            {
                Inputs = new Dictionary<string, JsonElement> { ["trades"] = KafkaBinding(tradesTopic) },
                Sinks = new Dictionary<string, JsonElement> { ["out"] = KafkaBinding(enrichedTopic) },
            });
            await a.DeployAsync(Spec("windowed", "windowed-by-partition") with
            {
                Inputs = new Dictionary<string, JsonElement> { ["ticks"] = KafkaBinding(ticksTopic) },
                Sinks = new Dictionary<string, JsonElement> { ["live"] = KafkaBinding(liveTopic), ["final"] = KafkaBinding(finalTopic) },
            });
            var b = await NodeAsync("b", low);
            nodes.Add(b);
            await FeedAsync("both nodes the older build");
            await WarmAsync(b);
            await Eventually(() => a.Node.Context.Backend.Formats.Writes == low, "the older leader to write its own level");

            // 1. The standby is upgraded. The leader hears a standby that reads more than
            // it does itself, and goes on writing what it wrote.
            await b.DisposeAsync();
            nodes.Remove(b);
            b = await NodeAsync("b", high);
            nodes.Add(b);
            await WarmAsync(b);
            await Eventually(() => a.Node.Status.Peer is { NodeId: "b", Role: NodeRole.Standby, FormatReads: var reads } && reads == high, "the leader to hear what the new standby reads");
            await FeedAsync("the standby upgraded");
            await CommittedAsync(a);
            await WarmAsync(b);
            Assert.Equal(low, a.Node.Context.Backend.Formats.Writes);
            Assert.False(HoldsHigh(a));

            // 2. Takeover: the newer build leads, at the level it found. The older node
            // comes back as its standby and reads everything.
            await b.Node.TakeOverAsync(new NodeActionOptions(), default).WaitAsync(Timeout);
            await Eventually(() => a.Node.Status.Leadership.Role == NodeRole.Fenced, "the old leader to find itself fenced");
            await a.DisposeAsync();
            nodes.Remove(a);
            await b.Running("enrich");
            await b.Running("windowed");
            Assert.Equal(low, b.Node.Context.Backend.Formats.Writes);
            await FeedAsync("the newer build leading alone");
            a = await NodeAsync("a", low);
            nodes.Add(a);
            await WarmAsync(a);
            await Eventually(() => b.Node.Status.Peer is { NodeId: "a", Role: NodeRole.Standby, Ready: true }, "the leader to hear the older standby");
            await FeedAsync("the newer build leading, the older standing by");
            await CommittedAsync(b);
            await WarmAsync(a);
            Assert.Equal(low, b.Node.Context.Backend.Formats.Writes);
            Assert.False(HoldsHigh(b));
            Assert.DoesNotContain(b.Node.RecentEvents(), e => e.Message.Contains("format level raised"));

            // 3. The old leader is upgraded. Both read the higher level now: the leader
            // raises what it writes, by itself, and the standby follows that too.
            await a.DisposeAsync();
            nodes.Remove(a);
            a = await NodeAsync("a", high);
            nodes.Add(a);
            await Eventually(() => b.Node.Context.Backend.Formats.Writes == high, "the leader to raise the level once its standby reads it");
            Assert.Contains(b.Node.RecentEvents(), e => e.Message.Contains($"format level raised from {low} to {high}"));
            await FeedAsync("the level raised");
            await CommittedAsync(b);
            await Eventually(() => HoldsHigh(b), "the logs to hold the higher level");
            await WarmAsync(a);

            // 4. And leadership moves at the higher level: the new leader goes on at it.
            await a.Node.TakeOverAsync(new NodeActionOptions(), default).WaitAsync(Timeout);
            await Eventually(() => b.Node.Status.Leadership.Role == NodeRole.Fenced, "the leader to find itself fenced");
            await b.DisposeAsync();
            nodes.Remove(b);
            await a.Running("enrich");
            await a.Running("windowed");
            Assert.Equal(high, a.Node.Context.Backend.Formats.Writes);
            await FeedAsync("after the takeover at the higher level");

            // Every window closes: each partition moves far ahead, then once more for the
            // step that announces the closes.
            var all = WindowedTotals.Model(ticks);
            foreach (var at in new[] { 1_000_000, 1_000_500 })
                for (var trader = 0; trader < 7; trader++)
                {
                    var tick = new Tick(++tickId, $"t{trader}", at, 1);
                    ticks.Add(tick);
                    env.ProduceJson(ticksTopic, tick.Trader, tick);
                }
            await env.FlushAsync();
            await KafkaTestEnv.TopicEqualsAsync(liveTopic, WindowedTotals.Model(ticks), "every window live at the end");
            await KafkaTestEnv.TopicEqualsAsync(finalTopic, all, "every window of the run final, each once and whole");
            await KafkaTestEnv.TopicEqualsAsync(enrichedTopic, Enrich.Model(trades, instruments), "every trade enriched at the end");
            await CommittedAsync(a);
            Assert.True(HoldsHigh(a));

            // A node from before the level, started now, says why it is no standby and
            // cannot take the logs; the leader is not disturbed.
            b = await NodeAsync("b", low);
            nodes.Add(b);
            await Eventually(
                () => b.Node.Status.Standby is { TakeoverWouldBeWarm: false } standby && standby.NotWarmBecause.Contains("format-level"),
                "the older node to say the logs are above its level");
            var refused = await Assert.ThrowsAnyAsync<Exception>(() => b.Node.TakeOverAsync(new NodeActionOptions(Force: true), default)).WaitAsync(Timeout);
            output.WriteLine($"level {low} → {high}: an older node's takeover is refused with: {refused.Message}");
            Assert.Contains("format level", refused.Message);
            Assert.True(a.Node.IsLeader);
            await FeedAsync("after the older node was refused");
            Assert.Equal(0, a.Pipelines.Get("windowed").LateRecords);
        }
        finally
        {
            foreach (var node in nodes)
                await node.DisposeAsync();
        }
    }

    [Fact]
    public async Task A_leader_told_to_hold_the_level_does_not_raise_it_and_one_with_no_peer_writes_its_own()
    {
        await using var env = KafkaTestEnv.Create();

        // No peer configured: nothing to wait for.
        await using (var alone = await env.StartNodeAsync("a", lead: true))
        {
            await Eventually(() => alone.Node.Context.Backend.Formats.Writes == FormatLevel.Current, "a lone leader to write its build's level");
            Assert.Equal(FormatLevel.Current, alone.Node.Context.Backend.FormatLevelWritten);
        }

        // Held: the next leader finds the level and goes on at it, but a fresh
        // deployment told to hold stays at the base level whatever answers.
        await using var other = KafkaTestEnv.Create();
        var ports = KafkaTestEnv.FreePorts(2);
        await using var a = await other.StartNodeAsync("a", lead: true, probePort: ports[0], peerPort: ports[1], options: o => o.HoldFormatLevel = true);
        await using var b = await other.StartNodeAsync("b", probePort: ports[1], peerPort: ports[0]);
        await Eventually(() => a.Node.Status.Peer is { NodeId: "b", Role: NodeRole.Standby, FormatReads: FormatLevel.Current }, "the leader to hear its standby");
        await Task.Delay(TimeSpan.FromSeconds(2.5));
        Assert.Equal(FormatLevel.Base, a.Node.Context.Backend.Formats.Writes);
    }
}
