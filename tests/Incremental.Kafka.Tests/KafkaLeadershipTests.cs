using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Incremental.Storage;
using Incremental.Tables;

namespace Incremental.Kafka.Tests;

/// <summary>
/// The leader protocol in one process, against a real broker: heartbeats, the election by
/// silence, the probe, the manual commands, what a deposed leader learns and when, the
/// promotion order, the lease and the lag figures. The kill -9 version with real processes
/// is <see cref="KafkaFailoverProcessTests"/>.
/// </summary>
public class KafkaLeadershipTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Timeout = KafkaEnv.Timeout;
    private static readonly TimeSpan Beat = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan After = TimeSpan.FromMilliseconds(800);

    private sealed record Row(int Id, string Name);

    /// <summary>One node: its log, its leadership, and what its callbacks saw.</summary>
    private sealed class Node(KafkaLog log) : IAsyncDisposable
    {
        public readonly KafkaLog Log = log;
        public KafkaLeadership Leadership = null!;
        public readonly List<string> Calls = [];
        public readonly List<LeadershipStatus> Changes = [];

        public NodeRole Role => Leadership.Role;

        public ValueTask DisposeAsync() => Leadership.DisposeAsync();
    }

    private static async Task<Node> StartAsync(
        KafkaEnv env, string id,
        TimeSpan? after = null,
        TimeSpan? ceiling = null,
        Func<CancellationToken, ValueTask<bool>>? probe = null,
        bool automatic = true,
        Func<Node, CancellationToken, Task>? prepare = null,
        Func<Node, long, Task>? start = null,
        bool begin = true,
        TimeSpan? beat = null)
    {
        var node = new Node(await env.ConnectAsync(env.Options(nodeId: id, heartbeat: beat ?? Beat)));
        node.Leadership = new KafkaLeadership(node.Log, new KafkaLeadershipOptions
        {
            TakeoverAfter = after ?? After,
            TakeoverCeiling = ceiling ?? TimeSpan.FromSeconds(30),
            ProbeLeader = probe,
            ProbeTimeout = TimeSpan.FromMilliseconds(300),
            AutomaticTakeover = automatic,
            LagSampleInterval = TimeSpan.FromMilliseconds(200),
            PrepareAsync = async token =>
            {
                lock (node.Calls)
                    node.Calls.Add("prepare");
                if (prepare is not null)
                    await prepare(node, token);
            },
            StartLeadingAsync = async (term, _) =>
            {
                lock (node.Calls)
                    node.Calls.Add("start " + term);
                if (start is not null)
                    await start(node, term);
            },
            StopLeadingAsync = _ =>
            {
                lock (node.Calls)
                    node.Calls.Add("stop");
                return Task.CompletedTask;
            },
        });
        node.Leadership.Changed += status =>
        {
            lock (node.Changes)
                node.Changes.Add(status);
        };
        if (begin)
            await node.Leadership.StartAsync().WaitAsync(Timeout);
        return node;
    }

    private static Task LeadsAsync(Node node) => node.Leadership.WhenRoleAsync(NodeRole.Leader).WaitAsync(Timeout);

    /// <summary>The leader topic's records, oldest first.</summary>
    private static async Task<List<(string Key, JsonElement Value)>> LeaderTopicAsync(KafkaLog log)
    {
        var records = new List<(string, JsonElement)>();
        var end = await log.EndOffsetAsync(log.LeaderPartition, default);
        if (end == 0)
            return records;
        using var stop = new CancellationTokenSource(Timeout);
        await foreach (var batch in log.ReadBatchesAsync(log.LeaderPartition, Offset.Beginning, stop.Token))
        {
            foreach (var record in batch.Records)
                records.Add((Encoding.UTF8.GetString(record.Message.Key), JsonDocument.Parse(record.Message.Value.AsMemory(1)).RootElement));
            if (batch.NextOffset >= end)
                break;
        }
        return records;
    }

    /// <summary>Where these tests put their state records. A follower takes a record from
    /// whichever partition holds it.</summary>
    private const int StatePartition = 1;

    private static async Task CommitStateAsync(KafkaLog log, string pipeline, string entry)
    {
        var writer = await log.State.AcquireWriterAsync();
        using var transaction = writer.BeginTransaction();
        transaction.Append(
            StatePartition,
            Encoding.UTF8.GetBytes($"{pipeline}\00\0store\0{entry}"), [1, (byte)'1']);
        await transaction.CommitAsync().AsTask().WaitAsync(Timeout);
    }

    // ── heartbeats ─────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task An_idle_writer_heartbeats_and_every_transaction_carries_one()
    {
        await using var env = KafkaEnv.Create();
        var log = await env.ConnectAsync(env.Options(nodeId: "a", heartbeat: Beat));
        Assert.Null(log.SinceLastTransaction);
        await log.AcquireWriterAsync().WaitAsync(Timeout);

        await KafkaEnv.EventuallyAsync(() => Task.FromResult(log.IdleHeartbeats >= 5), "the idle writer has heartbeaten five times");
        Assert.Equal(0, log.TransactionsCommitted - 1); // the term's transaction and nothing else
        Assert.InRange(log.SinceLastTransaction!.Value, TimeSpan.Zero, TimeSpan.FromSeconds(2));

        var before = log.IdleHeartbeats;
        for (var i = 0; i < 10; i++)
            await CommitStateAsync(log, "p", "e" + i);
        var idleDuring = log.IdleHeartbeats - before;

        await KafkaEnv.EventuallyAsync(
            async () => (await LeaderTopicAsync(log)).Count(r => r.Key == "heartbeat") >= before + idleDuring + 10,
            "each of the ten commits left a heartbeat of its own");
        var records = await LeaderTopicAsync(log);
        Assert.Equal("term", records[0].Key);
        Assert.Equal(1, records[0].Value.GetProperty("Term").GetInt64());
        Assert.Equal("a", records[0].Value.GetProperty("Node").GetString());

        var beats = records.Where(r => r.Key == "heartbeat").Select(r => r.Value).ToList();
        Assert.All(beats, beat =>
        {
            Assert.Equal(1, beat.GetProperty("Term").GetInt64());
            Assert.Equal("a", beat.GetProperty("Node").GetString());
            Assert.False(beat.GetProperty("Resigned").GetBoolean());
        });
        var sequences = beats.Select(b => b.GetProperty("Sequence").GetInt64()).ToList();
        Assert.Equal(sequences.OrderBy(s => s), sequences);
        Assert.Equal(sequences.Count, sequences.Distinct().Count());
    }

    [Fact]
    public async Task A_busy_writer_heartbeats_with_its_commits_alone()
    {
        // An interval too long to fire: every heartbeat here rode in a commit's transaction.
        await using var env = KafkaEnv.Create();
        var log = await env.ConnectAsync(env.Options(nodeId: "a", heartbeat: TimeSpan.FromHours(1)));
        await log.AcquireWriterAsync().WaitAsync(Timeout);
        for (var i = 0; i < 10; i++)
            await CommitStateAsync(log, "p", "e" + i);

        await KafkaEnv.EventuallyAsync(
            async () => (await LeaderTopicAsync(log)).Count(r => r.Key == "heartbeat") >= 10, "each commit left a heartbeat");
        var beats = (await LeaderTopicAsync(log)).Where(r => r.Key == "heartbeat").ToList();
        Assert.Equal(Enumerable.Range(1, 10).Select(i => (long)i), beats.Select(b => b.Value.GetProperty("Sequence").GetInt64()));
        Assert.Equal(0, log.IdleHeartbeats);
        Assert.Equal(11, log.TransactionsCommitted);
        Assert.InRange(log.SinceLastTransaction!.Value, TimeSpan.Zero, TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task A_writer_fenced_while_idle_finds_out_from_its_own_heartbeat()
    {
        await using var env = KafkaEnv.Create();
        var old = await env.ConnectAsync(env.Options(nodeId: "a", heartbeat: Beat));
        await old.AcquireWriterAsync().WaitAsync(Timeout);
        var successor = await env.ConnectAsync(env.Options(nodeId: "b", heartbeat: Beat));
        await successor.AcquireWriterAsync().WaitAsync(Timeout);

        // Nothing is written through the old log; the heartbeat alone tells it.
        var clock = Stopwatch.StartNew();
        await old.WhenFenced.WaitAsync(TimeSpan.FromSeconds(10));
        KafkaEnv.Measured(output, $"an idle writer learnt it was fenced {clock.ElapsedMilliseconds} ms after its successor took over (heartbeat every {Beat.TotalMilliseconds:F0} ms)");
        Assert.True(old.IsFenced);
        Assert.False(old.IsWriter);
        Assert.Null(old.SinceLastTransaction);
        Assert.True(successor.IsWriter);
        Assert.Equal(2, successor.Term);

        // And the successor's heartbeats are the last thing in the topic.
        await KafkaEnv.EventuallyAsync(async () =>
        {
            var last = (await LeaderTopicAsync(successor))[^1];
            return last.Key == "heartbeat" && last.Value.GetProperty("Node").GetString() == "b" && last.Value.GetProperty("Term").GetInt64() == 2;
        }, "the successor heartbeats under term 2");
    }

    // ── the election ───────────────────────────────────────────────────────────────────

    [Fact]
    public async Task A_standby_waits_while_the_leader_heartbeats_and_takes_over_when_it_stops()
    {
        await using var env = KafkaEnv.Create();
        var probes = 0;
        await using var a = await StartAsync(env, "a");
        await LeadsAsync(a);
        Assert.Equal(1, a.Leadership.Term);
        Assert.Equal("silence", a.Leadership.Status.LastTakeover!.Reason);

        await using var b = await StartAsync(env, "b", probe: _ =>
        {
            Interlocked.Increment(ref probes);
            return ValueTask.FromResult(false);
        });

        // Several takeover timeouts pass; the leader is heard throughout, so nothing happens
        // and the probe is never consulted.
        for (var i = 0; i < 6; i++)
        {
            await Task.Delay(After / 2);
            var seen = b.Leadership.Status;
            Assert.Equal(NodeRole.Standby, seen.Role);
            Assert.InRange(seen.HeartbeatAge, TimeSpan.Zero, After);
        }
        var status = b.Leadership.Status;
        Assert.Equal("a", status.LeaderNodeId);
        Assert.Equal(1, status.LeaderTerm);
        Assert.Equal(0, status.Term);
        Assert.False(status.HoldsLease);
        Assert.NotNull(status.LeaderHeartbeatAt);
        Assert.Equal(0, Volatile.Read(ref probes));
        Assert.Equal(NodeRole.Leader, a.Role);
        Assert.True(a.Leadership.HoldsLease);

        // The leader dies: its log is closed without a word.
        var died = Stopwatch.StartNew();
        await a.Log.DisposeAsync();
        await LeadsAsync(b);
        var took = died.Elapsed;

        var timings = b.Leadership.Status.LastTakeover!;
        KafkaEnv.Measured(output,
            $"in-process takeover after silence: {took.TotalMilliseconds:F0} ms from the leader's death (timeout {After.TotalMilliseconds:F0} ms): " +
            $"silence {timings.Silence.TotalMilliseconds:F0}, fence and term {timings.Fence.TotalMilliseconds:F0}, total after deciding {timings.Total.TotalMilliseconds:F0}");
        Assert.Equal("silence", timings.Reason);
        Assert.True(timings.Silence >= After, $"took over after only {timings.Silence} of silence");
        Assert.True(took >= After - Beat * 2, $"took over {took} after the leader died; the timeout is {After}");
        Assert.True(Volatile.Read(ref probes) > 0, "the probe was not asked");
        Assert.Equal(2, b.Leadership.Term);
        Assert.Equal(["prepare", "start 2"], b.Calls);
        Assert.True(b.Leadership.HoldsLease);

        var after = b.Leadership.Status;
        Assert.Equal(NodeRole.Leader, after.Role);
        Assert.Null(after.Lag);
        lock (b.Changes)
        {
            Assert.Equal(
                [NodeRole.Standby, NodeRole.Promoting, NodeRole.Leader],
                b.Changes.Select(c => c.Role).Distinct().ToArray());
        }
        await KafkaEnv.EventuallyAsync(() => Task.FromResult(b.Leadership.Status is { LeaderNodeId: "b", LeaderTerm: 2 }), "the new leader sees itself lead");
    }

    [Fact]
    public async Task A_probe_that_answers_holds_the_standby_back_until_the_ceiling()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a");
        await LeadsAsync(a);
        var ceiling = TimeSpan.FromSeconds(3);
        await using var b = await StartAsync(env, "b", ceiling: ceiling, probe: _ => ValueTask.FromResult(true));
        await Task.Delay(After);

        // Kafka "stalls" for the leader: it stops heartbeating, yet answers the probe.
        var silent = Stopwatch.StartNew();
        await a.Log.DisposeAsync();
        await Task.Delay(After * 2);
        Assert.Equal(NodeRole.Standby, b.Role);

        await LeadsAsync(b);
        Assert.Equal("ceiling", b.Leadership.Status.LastTakeover!.Reason);
        Assert.True(silent.Elapsed >= ceiling - Beat * 2, $"took over after {silent.Elapsed}; the ceiling is {ceiling}");
        Assert.True(b.Leadership.Status.LastTakeover!.Silence >= ceiling);
    }

    [Fact]
    public async Task The_tcp_probe_tells_a_leader_from_a_standby_and_from_nobody()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a", begin: false);
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        var probe = LeadershipProbe.Tcp("127.0.0.1", port);

        Assert.False(await probe(default));                       // nobody listens
        await using (LeadershipProbe.Listen(a.Leadership, new IPEndPoint(IPAddress.Loopback, port)))
        {
            Assert.False(await probe(default));                   // a standby answers, and is not the leader
            await a.Leadership.StartAsync();
            await LeadsAsync(a);
            Assert.True(await probe(default));
            Assert.True(await probe(default));                    // and again: one answer per connection
        }
        Assert.False(await probe(default));

        // A socket that accepts and never answers is a stopped process: no answer in time.
        var mute = new TcpListener(IPAddress.Loopback, port);
        mute.Start();
        using var limit = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await probe(limit.Token));
        mute.Stop();
    }

    [Fact]
    public async Task Two_nodes_started_together_end_with_exactly_one_leader()
    {
        await using var env = KafkaEnv.Create();
        var nodes = await Task.WhenAll(StartAsync(env, "a", begin: false), StartAsync(env, "b", begin: false));

        // No probe and the same timeout: both decide in the same instant, and both
        // initialise the transactional id. Fencing picks the winner.
        await Task.WhenAll(nodes.Select(n => n.Leadership.StartAsync()));
        await KafkaEnv.EventuallyAsync(
            () => Task.FromResult(nodes.All(n => n.Role is not NodeRole.Promoting) && nodes.Any(n => n.Role is NodeRole.Leader)),
            "one node leads");
        await Task.Delay(After * 3);

        var leader = Assert.Single(nodes, n => n.Role is NodeRole.Leader);
        var other = nodes.Single(n => n != leader);
        KafkaEnv.Measured(output, $"simultaneous start: '{leader.Leadership.NodeId}' leads under term {leader.Leadership.Term}; the other is {other.Role}");
        Assert.Contains(other.Role, new[] { NodeRole.Fenced, NodeRole.Standby });
        Assert.True(leader.Log.IsWriter);
        Assert.False(other.Log.IsWriter);
        Assert.False(other.Leadership.HoldsLease);

        // The leader's term is the highest ever written, and it can commit; the other cannot.
        var terms = (await LeaderTopicAsync(leader.Log)).Select(r => r.Value.GetProperty("Term").GetInt64()).ToList();
        Assert.Equal(leader.Leadership.Term, terms.Max());
        await CommitStateAsync(leader.Log, "p", "e");
        if (other.Role is NodeRole.Fenced)
        {
            await other.Leadership.WhenFinished.WaitAsync(Timeout);
            Assert.IsType<KafkaLogFencedException>(other.Leadership.Status.Fault, exactMatch: false);
            await Assert.ThrowsAsync<ChangelogFencedException>(() => CommitStateAsync(other.Log, "p", "x"));
        }
        foreach (var node in nodes)
            await node.DisposeAsync();
    }

    // ── the commands ───────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Manual_takeover_fences_the_leader_which_finds_out_at_once()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a");
        await LeadsAsync(a);
        await using var b = await StartAsync(env, "b", automatic: false);
        await Task.Delay(After * 2);
        Assert.Equal(NodeRole.Standby, b.Role);

        var clock = Stopwatch.StartNew();
        await b.Leadership.TakeOverAsync().WaitAsync(Timeout);
        var took = clock.Elapsed;
        Assert.Equal(NodeRole.Leader, b.Role);
        Assert.Equal(2, b.Leadership.Term);
        Assert.Equal("manual", b.Leadership.Status.LastTakeover!.Reason);

        // The old leader reads the higher term in the leader topic and is finished: no
        // lease, no commits, and it says why.
        await a.Leadership.WhenFinished.WaitAsync(TimeSpan.FromSeconds(10));
        KafkaEnv.Measured(output, $"manual takeover: {took.TotalMilliseconds:F0} ms; the old leader knew {clock.ElapsedMilliseconds} ms after the command");
        Assert.Equal(NodeRole.Fenced, a.Role);
        Assert.False(a.Leadership.HoldsLease);
        Assert.IsType<KafkaLogFencedException>(a.Leadership.Status.Fault, exactMatch: false);
        await Assert.ThrowsAsync<KafkaLogFencedException>(async () => await a.Leadership.WaitForLeaseAsync());
        await Assert.ThrowsAsync<ChangelogFencedException>(() => CommitStateAsync(a.Log, "p", "x"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => a.Leadership.TakeOverAsync());
        await CommitStateAsync(b.Log, "p", "e");
    }

    [Fact]
    public async Task A_deposed_leader_reads_the_higher_term_long_before_its_next_heartbeat()
    {
        // Heartbeats five seconds apart: only the leader topic can tell the old leader
        // this quickly that it has been replaced.
        await using var env = KafkaEnv.Create();
        var (beat, after) = (TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(12));
        await using var a = await StartAsync(env, "a", after: after, beat: beat);
        await using var b = await StartAsync(env, "b", after: after, beat: beat, automatic: false);
        await a.Leadership.TakeOverAsync().WaitAsync(Timeout);
        Assert.Equal("manual", a.Leadership.Status.LastTakeover!.Reason);

        var clock = Stopwatch.StartNew();
        await b.Leadership.TakeOverAsync().WaitAsync(Timeout);
        await a.Leadership.WhenFinished.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.True(clock.Elapsed < beat / 2);
        Assert.Equal(NodeRole.Fenced, a.Role);
        Assert.Equal(0, a.Log.IdleHeartbeats);
        Assert.Contains("'b' has taken the log over under term 2", a.Leadership.Status.Fault!.Message);
        Assert.False(a.Leadership.HoldsLease);
    }

    [Fact]
    public async Task Step_down_hands_over_without_waiting_for_the_timeout()
    {
        await using var env = KafkaEnv.Create();
        var patient = TimeSpan.FromSeconds(20);
        await using var a = await StartAsync(env, "a");
        await LeadsAsync(a);
        await using var b = await StartAsync(env, "b", after: patient);
        await Task.Delay(After);
        await Assert.ThrowsAsync<InvalidOperationException>(() => b.Leadership.StepDownAsync());

        var clock = Stopwatch.StartNew();
        await a.Leadership.StepDownAsync().WaitAsync(Timeout);
        Assert.Equal(NodeRole.Resigned, a.Role);
        Assert.Equal(["prepare", "start 1", "stop"], a.Calls);
        await a.Leadership.WhenFinished.WaitAsync(Timeout);
        Assert.Null(a.Leadership.Status.Fault);
        Assert.False(a.Leadership.HoldsLease);
        await Assert.ThrowsAsync<ChangelogFencedException>(() => CommitStateAsync(a.Log, "p", "x"));

        await LeadsAsync(b);
        KafkaEnv.Measured(output, $"step down to the standby leading: {clock.ElapsedMilliseconds} ms (takeover timeout {patient.TotalSeconds:F0} s)");
        Assert.True(clock.Elapsed < patient / 2, $"the handover took {clock.Elapsed}");
        Assert.Equal("resigned", b.Leadership.Status.LastTakeover!.Reason);
        Assert.Equal(2, b.Leadership.Term);

        // The resignation is a heartbeat, the last one the old leader wrote.
        var resignation = (await LeaderTopicAsync(b.Log)).Last(r => r.Value.GetProperty("Node").GetString() == "a");
        Assert.Equal("heartbeat", resignation.Key);
        Assert.True(resignation.Value.GetProperty("Resigned").GetBoolean());

        // A node that starts when the last word in the topic is a resignation does not wait either.
        await b.Leadership.StepDownAsync().WaitAsync(Timeout);
        clock.Restart();
        await using var c = await StartAsync(env, "c", after: patient);
        await LeadsAsync(c);
        Assert.True(clock.Elapsed < patient / 2);
        Assert.Equal(3, c.Leadership.Term);
    }

    // ── promotion, lag, lease ──────────────────────────────────────────────────────────

    [Fact]
    public async Task Takeover_promotes_tables_then_storage_then_starts_and_lag_is_measured()
    {
        await using var env = KafkaEnv.Create();
        SharedTable<int, Row>? leaderTable = null;
        await using var a = await StartAsync(env, "a", start: async (node, _) =>
        {
            leaderTable = await SharedTable<int, Row>.OpenAsync("rows", r => r.Id, await node.Log.OpenTableLogAsync<int, Row>("rows"));
        });
        await LeadsAsync(a);
        for (var i = 0; i < 3; i++)
        {
            await leaderTable!.Writer.UpsertAsync(new Row(i, "v" + i));
            await leaderTable.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
            await CommitStateAsync(a.Log, "p", "e" + i);
        }

        // The standby follows the table and the state, and is built before it is started.
        SharedTable<int, Row>? table = null;
        ChangelogStorage? storage = null;
        string? atStart = null;
        var b = await StartAsync(env, "b", begin: false, start: (_, _) =>
        {
            atStart = $"table following={table!.IsFollowing} storage leader={storage!.IsLeader}";
            return Task.CompletedTask;
        });
        await using var disposeB = b;
        var tableLog = await b.Log.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        table = await SharedTable<int, Row>.FollowAsync("rows", r => r.Id, tableLog).WaitAsync(Timeout);
        storage = ChangelogStorage.Follow(b.Log.State);
        b.Leadership.Track(table, tableLog);
        b.Leadership.Track(storage);
        await b.Leadership.StartAsync();

        await KafkaEnv.EventuallyAsync(async () => (await b.Leadership.MeasureLagAsync()).IsCaughtUp, "the standby has caught up");
        var lag = await b.Leadership.MeasureLagAsync();
        Assert.Equal(b.Log.State.PartitionCount, lag.StateOffsets.Count);
        Assert.Equal(["rows"], lag.TableVersions.Keys);
        Assert.Equal(0, lag.TotalStateOffsets);
        await KafkaEnv.EventuallyAsync(() => Task.FromResult(b.Leadership.Status.Lag is { IsCaughtUp: true }), "the sampled lag says so too");

        // A standby that has stopped reading falls behind by exactly what the leader writes:
        // a table that was restored and never followed, a storage whose readers are stopped.
        var staleLog = await env.ConnectAsync(env.Options(nodeId: "stale", heartbeat: Beat));
        var staleTableLog = await staleLog.OpenTableLogAsync<int, Row>("rows").WaitAsync(Timeout);
        await using var staleTable = await SharedTable<int, Row>.OpenAsync("rows", r => r.Id, staleTableLog).WaitAsync(Timeout);
        var staleStorage = ChangelogStorage.Follow(staleLog.State);
        await staleStorage.WhenCaughtUpAsync().WaitAsync(Timeout);
        await staleStorage.DisposeAsync();
        await using var stale = new KafkaLeadership(staleLog, new KafkaLeadershipOptions { StartLeadingAsync = (_, _) => Task.CompletedTask });
        stale.Track(staleTable, staleTableLog);
        stale.Track(staleStorage);
        Assert.True((await stale.MeasureLagAsync()).IsCaughtUp);

        for (var i = 3; i < 5; i++)
        {
            await leaderTable!.Writer.UpsertAsync(new Row(i, "v" + i));
            await leaderTable.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
        }
        await CommitStateAsync(a.Log, "p", "late");
        const int partition = StatePartition;
        await KafkaEnv.EventuallyAsync(async () => (await stale.MeasureLagAsync()).StateOffsets[partition] > 0, "the stopped reader is behind");
        var behind = await stale.MeasureLagAsync();
        Assert.Equal(2, behind.TableVersions["rows"]);
        Assert.Equal(2, behind.StateOffsets[partition]);     // one record and its transaction's marker
        Assert.Equal(2, behind.TotalStateOffsets);
        Assert.False(behind.IsCaughtUp);
        await KafkaEnv.EventuallyAsync(async () => (await b.Leadership.MeasureLagAsync()).IsCaughtUp, "the live standby is not");

        // Takeover: by the time the circuits would start, the table is the writer's and the
        // storage leads.
        await a.Log.DisposeAsync();
        await LeadsAsync(b);
        Assert.Equal("table following=False storage leader=True", atStart);
        Assert.Equal(5, table.LatestVersion);
        Assert.Equal(5, table.Count);
        Assert.Equal(2, storage.Term);
        await table.Writer.UpsertAsync(new Row(9, "by b"));
        await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
        var timings = b.Leadership.Status.LastTakeover!;
        Assert.True(timings.Total >= timings.Fence + timings.Tables + timings.Storage);
        await storage.DisposeAsync();
    }

    [Fact]
    public async Task A_takeover_that_cannot_start_ends_fenced_and_stops_heartbeating()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a", start: (_, _) => throw new InvalidOperationException("no circuits today"));
        await a.Leadership.WhenFinished.WaitAsync(Timeout);
        Assert.Equal(NodeRole.Fenced, a.Role);
        Assert.Contains("no circuits today", a.Leadership.Status.Fault!.Message);
        Assert.True(a.Log.IsFenced);

        // It leads nothing, so it must not look alive: the next node takes over.
        var beats = a.Log.IdleHeartbeats;
        await using var b = await StartAsync(env, "b");
        await LeadsAsync(b);
        Assert.Equal(beats, a.Log.IdleHeartbeats);
        Assert.Equal(2, b.Leadership.Term);
    }

    // ── a long commit ──────────────────────────────────────────────────────────────────

    private static LogCommit StateRecords(KafkaLog log, int count)
    {
        var commit = new LogCommit();
        var value = new byte[] { 1, (byte)'1' };
        for (var i = 0; i < count; i++)
            commit.Add(new TopicPartition(log.State.Topic, StatePartition), Encoding.UTF8.GetBytes("k" + i), value);
        return commit;
    }

    /// <summary>
    /// One commit that takes several takeover timeouts to produce. Its heartbeat cannot be
    /// read until it commits, and no idle heartbeat can be written meanwhile, so to a
    /// standby that reads committed records the leader has gone silent. It has not: it is
    /// writing, and the standby must see that and wait.
    /// </summary>
    [Fact]
    public async Task A_leader_inside_one_long_commit_is_not_taken_for_dead()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a");
        await LeadsAsync(a);
        await using var b = await StartAsync(env, "b");
        await Task.Delay(After);
        Assert.Equal(NodeRole.Standby, b.Role);

        // 120 pauses of 25 ms: three seconds of producing, against a timeout of 0.8 s.
        a.Log.WhileProducing = () => Thread.Sleep(25);
        var beatsBefore = a.Log.InTransactionHeartbeats;
        var clock = Stopwatch.StartNew();
        await a.Log.CommitAsync(StateRecords(a.Log, 256 * 120)).WaitAsync(Timeout);
        clock.Stop();
        a.Log.WhileProducing = null;

        Assert.True(clock.Elapsed > After * 3, $"the commit took only {clock.Elapsed}; it must outlast the takeover timeout to prove anything");
        Assert.Equal(NodeRole.Leader, a.Role);
        Assert.Equal(NodeRole.Standby, b.Role);
        Assert.True(a.Log.IsWriter);
        Assert.True(a.Log.InTransactionHeartbeats - beatsBefore >= 10, "the long transaction carried no heartbeats of its own");
        KafkaEnv.Measured(output,
            $"a commit of {clock.Elapsed.TotalMilliseconds:F0} ms under a takeover timeout of {After.TotalMilliseconds:F0} ms: " +
            $"{a.Log.InTransactionHeartbeats - beatsBefore} in-transaction heartbeats, the standby stayed a standby");

        // The commit is whole and the leader carries on as before.
        await CommitStateAsync(a.Log, "p", "after");
        await Task.Delay(After * 2);
        Assert.Equal(NodeRole.Standby, b.Role);
        Assert.Equal(1, b.Leadership.Status.LeaderTerm);
    }

    /// <summary>The other side of the same rule: a leader that stops in the middle of a
    /// transaction — its first heartbeat written, nothing after — is silent, and the open
    /// transaction must not buy it any time.</summary>
    [Fact]
    public async Task A_leader_that_stops_inside_a_transaction_is_replaced_on_time()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a");
        await LeadsAsync(a);
        await using var b = await StartAsync(env, "b");
        await Task.Delay(After);

        using var frozen = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        a.Log.WhileProducing = () =>
        {
            frozen.Set();
            release.Wait(Timeout);
        };
        var commit = a.Log.CommitAsync(StateRecords(a.Log, 2048));
        Assert.True(frozen.Wait(Timeout));
        var stopped = Stopwatch.StartNew();

        await LeadsAsync(b);
        var took = stopped.Elapsed;
        Assert.Equal("silence", b.Leadership.Status.LastTakeover!.Reason);
        Assert.True(took < After * 3, $"the standby took {took} to replace a leader stopped mid-transaction (timeout {After})");
        KafkaEnv.Measured(output, $"leader stopped inside a transaction: replaced {took.TotalMilliseconds:F0} ms later (timeout {After.TotalMilliseconds:F0} ms)");

        // The old leader's transaction was aborted under it: the commit fails as fenced.
        release.Set();
        await Assert.ThrowsAsync<KafkaLogFencedException>(() => commit).WaitAsync(Timeout);
        await a.Leadership.WhenFinished.WaitAsync(Timeout);
        Assert.Equal(NodeRole.Fenced, a.Role);
    }

    // ── stopping part-way, stepping down twice ─────────────────────────────────────────

    [Fact]
    public async Task Disposing_the_leadership_in_the_middle_of_a_takeover_closes_the_writer()
    {
        await using var env = KafkaEnv.Create();
        var preparing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var a = await StartAsync(env, "a", prepare: async (_, token) =>
        {
            preparing.TrySetResult();
            await Task.Delay(System.Threading.Timeout.Infinite, token);
        });
        await preparing.Task.WaitAsync(Timeout);
        Assert.Equal(NodeRole.Promoting, a.Role);
        Assert.True(a.Log.IsWriter); // it has fenced whoever led, and heartbeats
        await KafkaEnv.EventuallyAsync(() => Task.FromResult(a.Log.IdleHeartbeats >= 2), "the half-promoted node heartbeats");

        await a.Leadership.DisposeAsync().AsTask().WaitAsync(Timeout);

        // It leads nothing and never will: it must not look alive.
        Assert.False(a.Log.IsWriter);
        Assert.True(a.Log.IsFenced);
        Assert.Equal(NodeRole.Fenced, a.Role);
        Assert.True(a.Leadership.WhenFinished.IsCompleted);
        var beats = a.Log.IdleHeartbeats;
        await using var b = await StartAsync(env, "b");
        await LeadsAsync(b);
        Assert.Equal(beats, a.Log.IdleHeartbeats);
        Assert.Equal(2, b.Leadership.Term);
    }

    [Fact]
    public async Task Step_downs_asked_for_together_are_one_step_down()
    {
        await using var env = KafkaEnv.Create();
        for (var round = 0; round < 3; round++)
        {
            await using var a = await StartAsync(env, "a" + round, automatic: false);
            await a.Leadership.TakeOverAsync().WaitAsync(Timeout);

            using var together = new Barrier(6);
            var calls = Enumerable.Range(0, 6).Select(_ => Task.Run(() =>
            {
                together.SignalAndWait(Timeout);
                return a.Leadership.StepDownAsync();
            })).ToArray();
            await Task.WhenAll(calls).WaitAsync(Timeout); // every caller: no exception

            Assert.Equal(NodeRole.Resigned, a.Role);
            Assert.Null(a.Leadership.Status.Fault);
            lock (a.Calls)
                Assert.Single(a.Calls, call => call == "stop");

            // And once more, afterwards: the same answer, nothing done again.
            await a.Leadership.StepDownAsync().WaitAsync(Timeout);
            lock (a.Calls)
                Assert.Single(a.Calls, call => call == "stop");
        }
    }

    [Fact]
    public async Task Measuring_lag_builds_no_clients()
    {
        await using var env = KafkaEnv.Create();
        var active = await env.ConnectWriterAsync(env.Options(nodeId: "a", heartbeat: Beat));
        var names = Enumerable.Range(0, 4).Select(i => "t" + i).ToArray();
        var writers = new List<SharedTable<int, Row>>();
        foreach (var name in names)
        {
            var table = await SharedTable<int, Row>.OpenAsync(name, r => r.Id, await active.OpenTableLogAsync<int, Row>(name)).WaitAsync(Timeout);
            await table.Writer.UpsertAsync(new Row(1, "one"));
            await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
            writers.Add(table);
        }

        await using var b = await StartAsync(env, "b", begin: false, automatic: false);
        foreach (var name in names)
        {
            var log = await b.Log.OpenTableLogAsync<int, Row>(name).WaitAsync(Timeout);
            b.Leadership.Track(await SharedTable<int, Row>.FollowAsync(name, r => r.Id, log).WaitAsync(Timeout), log);
        }
        await KafkaEnv.EventuallyAsync(async () => (await b.Leadership.MeasureLagAsync()).IsCaughtUp, "the standby has caught up");

        var clients = b.Log.ConsumersCreated;
        for (var i = 0; i < 5; i++)
            Assert.True((await b.Leadership.MeasureLagAsync()).IsCaughtUp);
        Assert.Equal(clients, b.Log.ConsumersCreated);

        // Still a measurement: a version the standby's view has not reached yet shows.
        await writers[2].Writer.UpsertAsync(new Row(2, "two"));
        await writers[2].Writer.CommitAsync().AsTask().WaitAsync(Timeout);
        await KafkaEnv.EventuallyAsync(async () => (await b.Leadership.MeasureLagAsync()).IsCaughtUp, "the standby follows the new version");
        Assert.Equal(clients, b.Log.ConsumersCreated);
        foreach (var table in writers)
            await table.DisposeAsync();
    }

    [Fact]
    public void The_lease_clock_counts_time_spent_suspended_where_the_platform_has_such_a_clock()
    {
        var first = LeaseClock.Milliseconds;
        Thread.Sleep(30);
        var second = LeaseClock.Milliseconds;
        Assert.InRange(second - first, 20, 5000);
        if (OperatingSystem.IsLinux())
        {
            // CLOCK_BOOTTIME: never behind the monotonic clock, which stops in a suspend.
            Assert.True(LeaseClock.CountsSuspendedTime);
            Assert.True(LeaseClock.Milliseconds >= Environment.TickCount64 - 50);
        }
    }

    [Fact]
    public async Task A_leased_sink_writes_for_the_leader_and_refuses_for_anyone_else()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a", begin: false);
        var sink = new InMemoryUpsertSink<int, Row>();
        var leased = a.Leadership.Leased(sink);
        var circuitBuilder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var input = circuitBuilder.Input<Row>("rows");
        var acknowledged = input.ToAcknowledgedOutput();
        await using var circuit = circuitBuilder.Build();
        circuit.Start();
        var failures = new List<Exception>();
        var delivery = circuit.Handle(acknowledged).DeliverToAsync(
            leased, r => r.Id,
            new SinkDeliveryOptions { MaxRetries = 0, OnFailedAttempt = (ex, _) => failures.Add(ex) });

        // A standby holds no lease.
        Assert.False(a.Leadership.HoldsLease);
        await Assert.ThrowsAsync<KafkaLogFencedException>(async () => await a.Leadership.WaitForLeaseAsync());

        await a.Leadership.StartAsync();
        await LeadsAsync(a);
        await a.Leadership.WaitForLeaseAsync().AsTask().WaitAsync(Timeout);
        await circuit.Handle(input).InsertAsync(new Row(1, "one"));
        await circuit.StepAsync().WaitAsync(Timeout);
        await KafkaEnv.EventuallyAsync(() => Task.FromResult(sink.Rows.ContainsKey(1)), "the leader's write reaches the sink");
        Assert.Equal("one", sink.Rows[1].Name);

        // Deposed: the write is refused before it reaches the sink, and the circuit faults.
        var b = await env.ConnectAsync(env.Options(nodeId: "b", heartbeat: Beat));
        await b.AcquireWriterAsync().WaitAsync(Timeout);
        await a.Leadership.WhenFinished.WaitAsync(Timeout);
        await circuit.Handle(input).InsertAsync(new Row(2, "two"));
        try
        {
            await circuit.StepAsync().WaitAsync(Timeout);
        }
        catch (Exception ex) when (ex is not TimeoutException)
        {
            // With no checkpoint to wait for, the step may finish before the delivery fails.
        }
        await Assert.ThrowsAnyAsync<Exception>(() => delivery.WaitAsync(Timeout));
        Assert.False(sink.Rows.ContainsKey(2));
        Assert.Contains(failures, f => f is KafkaLogFencedException);
    }

    [Fact]
    public async Task A_leader_whose_commits_stall_loses_the_lease_before_a_standby_would_take_over()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a");
        await LeadsAsync(a);
        var lease = After - After / 3;
        var sink = new InMemoryUpsertSink<int, Row>();
        var leased = a.Leadership.Leased(sink);
        var circuitBuilder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var input = circuitBuilder.Input<Row>("rows");
        var acknowledged = input.ToAcknowledgedOutput();
        await using var circuit = circuitBuilder.Build();
        circuit.Start();
        var delivery = circuit.Handle(acknowledged).DeliverToAsync(leased, r => r.Id);

        // The writer stalls — Kafka is slow, the process is starved — without being fenced.
        using var release = new ManualResetEventSlim();
        a.Log.BeforeTransaction = release.Wait;
        try
        {
            await StalledAsync();
        }
        finally
        {
            // Whatever the verdict, let the writer thread go, or nothing here can be disposed.
            a.Log.BeforeTransaction = null;
            release.Set();
        }
        await KafkaEnv.EventuallyAsync(() => Task.FromResult(sink.Rows.ContainsKey(1)), "the held delivery goes out once the lease is back");
        Assert.True(a.Leadership.HoldsLease);
        return;

        async Task StalledAsync()
        {
            var stalled = Stopwatch.StartNew();
            while (a.Leadership.HoldsLease)
            {
                Assert.True(stalled.Elapsed < After * 3, "the lease did not lapse although nothing has committed");
                await Task.Delay(10);
            }
            var lapsedAfter = stalled.Elapsed;
            Assert.True(a.Log.IsWriter);
            Assert.Equal(NodeRole.Leader, a.Role);
            Assert.False(a.Leadership.Status.HoldsLease);

            // It lapsed within the lease of the last heartbeat, which is before any standby's
            // silence can have reached the takeover timeout.
            Assert.True(lapsedAfter < After, $"the lease lapsed only after {lapsedAfter}; a standby takes over after {After}");
            Assert.True(a.Log.SinceLastTransaction >= lease);

            // Nothing is delivered while it is lapsed.
            await circuit.Handle(input).InsertAsync(new Row(1, "held"));
            await circuit.StepAsync().WaitAsync(Timeout);
            await Task.Delay(After);
            Assert.Empty(sink.Rows);
            Assert.False(delivery.IsCompleted);
        }
    }

    [Fact]
    public async Task Timeouts_that_cannot_work_are_refused()
    {
        await using var env = KafkaEnv.Create();
        var log = await env.ConnectAsync(env.Options(nodeId: "a", heartbeat: TimeSpan.FromSeconds(1)));
        Func<long, CancellationToken, Task> start = (_, _) => Task.CompletedTask;

        var tooEager = Assert.Throws<ArgumentException>(() => new KafkaLeadership(log, new KafkaLeadershipOptions
        {
            TakeoverAfter = TimeSpan.FromMilliseconds(1500), StartLeadingAsync = start,
        }));
        Assert.Contains("at least twice", tooEager.Message);
        var noLease = Assert.Throws<ArgumentException>(() => new KafkaLeadership(log, new KafkaLeadershipOptions
        {
            TakeoverAfter = TimeSpan.FromSeconds(3), LeaseMargin = TimeSpan.FromSeconds(2), StartLeadingAsync = start,
        }));
        Assert.Contains("lease", noLease.Message);
        Assert.Throws<ArgumentOutOfRangeException>(() => new KafkaLeadership(log, new KafkaLeadershipOptions
        {
            TakeoverCeiling = TimeSpan.FromSeconds(1), StartLeadingAsync = start,
        }));
    }
}
