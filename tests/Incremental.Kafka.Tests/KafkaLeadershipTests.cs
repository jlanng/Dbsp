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
    public async Task A_probe_query_returns_the_whole_answer_with_the_hosts_token()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a", begin: false);
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();

        Assert.Null(await LeadershipProbe.QueryAsync("127.0.0.1", port));          // nobody listens
        string? detail = "notready:state-behind";
        await using (LeadershipProbe.Listen(a.Leadership, new IPEndPoint(IPAddress.Loopback, port), () => detail))
        {
            Assert.Equal(new LeadershipProbeAnswer(NodeRole.Standby, 0, "a", "notready:state-behind"), await LeadershipProbe.QueryAsync("127.0.0.1", port));
            await a.Leadership.StartAsync();
            await LeadsAsync(a);
            detail = "ready";
            Assert.Equal(new LeadershipProbeAnswer(NodeRole.Leader, a.Leadership.Term, "a", "ready"), await LeadershipProbe.QueryAsync("127.0.0.1", port));

            // The old probe reads the first token and is not confused by a fourth.
            Assert.True(await LeadershipProbe.Tcp("127.0.0.1", port)(default));

            // No token, and a token that would break the line, are both left out.
            detail = null;
            Assert.Null((await LeadershipProbe.QueryAsync("127.0.0.1", port))!.Detail);
            detail = "two words";
            Assert.Equal(new LeadershipProbeAnswer(NodeRole.Leader, a.Leadership.Term, "a", null), await LeadershipProbe.QueryAsync("127.0.0.1", port));
        }

        // A listener with no token at all answers the three it always did.
        await using (LeadershipProbe.Listen(a.Leadership, new IPEndPoint(IPAddress.Loopback, port)))
            Assert.Equal(new LeadershipProbeAnswer(NodeRole.Leader, a.Leadership.Term, "a", null), await LeadershipProbe.QueryAsync("127.0.0.1", port));
    }

    [Fact]
    public async Task A_probe_listener_stopped_the_moment_it_has_answered_stops_cleanly()
    {
        // Its accept loop can be on its way back to the next accept when the listener is
        // stopped: that is a stop, not a failure (once seen as "Not listening" on CI).
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a", begin: false);
        for (var i = 0; i < 300; i++)
        {
            var free = new TcpListener(IPAddress.Loopback, 0);
            free.Start();
            var port = ((IPEndPoint)free.LocalEndpoint).Port;
            free.Stop();
            var listener = LeadershipProbe.Listen(a.Leadership, new IPEndPoint(IPAddress.Loopback, port));
            Assert.NotNull(await LeadershipProbe.QueryAsync("127.0.0.1", port));
            await listener.DisposeAsync();
        }
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
        Assert.False(a.Leadership.Status.TakeoverFailed);   // replaced, which is not a failure of its own
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

        // Fenced by its own doing, and it says so: nobody replaced it.
        Assert.True(a.Leadership.Status.TakeoverFailed);

        // It leads nothing, so it must not look alive: the next node takes over.
        var beats = a.Log.IdleHeartbeats;
        await using var b = await StartAsync(env, "b");
        await LeadsAsync(b);
        Assert.Equal(beats, a.Log.IdleHeartbeats);
        Assert.Equal(2, b.Leadership.Term);
    }

    /// <summary>The other way a takeover ends fenced: this node took the log, and before
    /// it led another node took it from it. That is a node that was replaced, however
    /// the takeover's own steps then failed, and it must not be told apart as a failure
    /// of its own.</summary>
    [Fact]
    public async Task A_takeover_that_fails_because_another_node_took_the_log_meanwhile_is_not_the_nodes_own_failure()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a", prepare: async (node, _) =>
        {
            var c = await KafkaLog.ConnectAsync(env.Options(nodeId: "c", heartbeat: Beat));
            await c.AcquireWriterAsync().WaitAsync(Timeout);
            await c.DisposeAsync();

            // The node finds out — it reads the higher term, or its next write fails —
            // and whatever it was doing for its takeover fails with something else.
            SpinWait.SpinUntil(() => node.Log.IsFenced, Timeout);
            throw new InvalidOperationException("the catalog could not be opened");
        });
        await a.Leadership.WhenFinished.WaitAsync(Timeout);
        Assert.Equal(NodeRole.Fenced, a.Role);
        Assert.True(a.Log.IsFenced);
        Assert.False(a.Leadership.Status.TakeoverFailed);
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

    // ── takeovers that are lost, given up or held back ─────────────────────────────────
    //
    // What a broker fault does to a takeover, provoked here without one: the log's
    // internal seams fence an acquisition, stall a writer or answer for the brokers. The
    // real faults are KafkaServiceProcessTests' broker tests, on the kafka3 profile.

    /// <summary>Stops a leader's heartbeats without ending it: its writer thread waits
    /// before its next transaction until the returned gate is set.</summary>
    private static ManualResetEventSlim Stall(Node node)
    {
        var gate = new ManualResetEventSlim();
        node.Log.BeforeTransaction = () => gate.Wait(Timeout);
        return gate;
    }

    /// <summary>
    /// The outcome that once left a deployment with no node at all: the standby fences the
    /// leader and is itself fenced before its term is committed. Both used to end as
    /// Fenced. The standby has promoted nothing, so it stands by again, with a new client,
    /// and leads when nobody else does.
    /// </summary>
    [Fact]
    public async Task A_standby_fenced_in_the_middle_of_its_takeover_stands_by_again_and_then_leads()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a");
        await LeadsAsync(a);
        await using var b = await StartAsync(env, "b");
        await Task.Delay(After);
        Assert.Equal(NodeRole.Standby, b.Role);

        // A third process takes the transactional id while b holds it and has not yet
        // committed its term — then dies, having led nothing.
        var intruded = 0;
        b.Log.WhileAcquiring = point =>
        {
            if (point != "term-read" || Interlocked.Exchange(ref intruded, 1) != 0)
                return;
            var c = KafkaLog.ConnectAsync(env.Options(nodeId: "c", heartbeat: Beat)).GetAwaiter().GetResult();
            c.AcquireWriterAsync().WaitAsync(Timeout).GetAwaiter().GetResult();
            c.DisposeAsync().AsTask().GetAwaiter().GetResult();
        };
        await a.Log.DisposeAsync();   // the leader goes silent

        await KafkaEnv.EventuallyAsync(() => Task.FromResult(b.Leadership.TakeoversAbandoned == 1), "b's first takeover to be lost");
        Assert.Equal(1, b.Log.WriterResets);
        Assert.False(b.Log.IsFenced);

        // Lost to a fence: it waits before it tries again, so that two nodes that fenced
        // each other do not do it in step.
        Assert.True(b.Leadership.RetryNotBefore > Environment.TickCount64, "a takeover lost to a fence is not tried again at once");
        Assert.False(b.Log.WhenFenced.IsCompleted);
        Assert.False(b.Leadership.WhenFinished.IsCompleted);

        // c took term 2 and is gone; nobody leads; b does, by itself, under the next term.
        await LeadsAsync(b);
        Assert.Equal(3, b.Leadership.Term);
        Assert.Equal(["prepare", "start 3"], b.Calls);
        List<NodeRole> roles;
        lock (b.Changes)
            roles = [.. b.Changes.Select(c => c.Role)];
        var collapsed = roles.Where((role, i) => i == 0 || roles[i - 1] != role).ToList();
        Assert.Equal([NodeRole.Standby, NodeRole.Promoting, NodeRole.Standby, NodeRole.Promoting, NodeRole.Leader], collapsed.SkipWhile(r => r != NodeRole.Standby).ToList());
        await CommitStateAsync(b.Log, "p", "after");
    }

    [Fact]
    public async Task A_log_fenced_while_it_acquires_is_a_reader_again_and_can_acquire_later()
    {
        await using var env = KafkaEnv.Create();
        var first = await env.ConnectAsync(env.Options(nodeId: "first", heartbeat: Beat));
        var second = await env.ConnectAsync(env.Options(nodeId: "second", heartbeat: Beat));
        foreach (var point in new[] { "initialised", "term-read" })
        {
            var resets = first.WriterResets;
            var term = second.Term;
            first.WhileAcquiring = at =>
            {
                if (at == point)
                    second.AcquireWriterAsync().WaitAsync(Timeout).GetAwaiter().GetResult();
            };
            if (second.IsWriter)
            {
                // Round two: the second log must take the id again to fence the first, so
                // it is a new connection.
                second = await env.ConnectAsync(env.Options(nodeId: "second", heartbeat: Beat));
            }
            var lost = await Assert.ThrowsAsync<KafkaLogNotAcquiredException>(() => first.AcquireWriterAsync()).WaitAsync(Timeout);
            Assert.IsType<KafkaLogFencedException>(lost.InnerException);
            Assert.Equal(resets + 1, first.WriterResets);
            Assert.False(first.IsWriter);
            Assert.False(first.IsFenced);
            Assert.False(first.WhenFenced.IsCompleted);
            Assert.Equal(0, first.Term);
            Assert.True(second.IsWriter);
            await CommitStateAsync(second, "p", point);
        }

        // Nothing of the first log's was ever committed: the terms in the topic are the second's.
        var terms = (await LeaderTopicAsync(second)).Where(r => r.Key == "term").Select(r => r.Value.GetProperty("Node").GetString()).ToList();
        Assert.Equal(["second", "second"], terms);

        // And it is still good for what a reader may become.
        first.WhileAcquiring = null;
        await first.AcquireWriterAsync().WaitAsync(Timeout);
        Assert.True(first.IsWriter);
        Assert.Equal(3, first.Term);
        await CommitStateAsync(first, "p", "mine");
        await Assert.ThrowsAsync<ChangelogFencedException>(() => CommitStateAsync(second, "p", "late")).WaitAsync(Timeout);
    }

    /// <summary>A leader that was only slow — its brokers were — and commits again while
    /// its standby is on the way to replacing it: the standby hears it and stops, and
    /// nobody is fenced.</summary>
    [Fact]
    public async Task A_takeover_is_given_up_when_the_leader_is_heard_before_it_is_fenced()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a");
        await LeadsAsync(a);
        await using var b = await StartAsync(env, "b");
        await Task.Delay(After);

        using var stall = Stall(a);
        var met = 0;
        b.Log.WhileAcquiring = point =>
        {
            if (point != "begin" || Interlocked.Exchange(ref met, 1) != 0)
                return;

            // The standby has decided and has asked nothing of the brokers yet. The leader
            // comes back to life; the standby's watch hears it.
            var beats = a.Log.IdleHeartbeats;
            stall.Set();
            SpinWait.SpinUntil(() => a.Log.IdleHeartbeats >= beats + 2, Timeout);
            SpinWait.SpinUntil(() => b.Leadership.Status.HeartbeatAge < Beat * 2, Timeout);
        };

        await KafkaEnv.EventuallyAsync(() => Task.FromResult(b.Leadership.TakeoversAbandoned == 1), "b's takeover to be given up");
        Assert.Equal(NodeRole.Standby, b.Role);
        Assert.Equal(1, b.Log.WriterResets);

        // Given up for a leader that was heard: nothing holds the next attempt back but
        // that leader's own heartbeats. (Were it fenced after all, by a request that had
        // left this node already, its silence would be answered like any dead leader's.)
        Assert.Equal(0, b.Leadership.RetryNotBefore);

        // The leader was never touched, and stays untouched: the client the standby put
        // away does not finish the takeover behind its back.
        await Task.Delay(After * 3);
        Assert.Equal(NodeRole.Leader, a.Role);
        Assert.True(a.Log.IsWriter);
        Assert.Equal(NodeRole.Standby, b.Role);
        Assert.Equal(1, b.Leadership.Status.LeaderTerm);
        await CommitStateAsync(a.Log, "p", "still mine");
        Assert.Empty(b.Calls);
    }

    /// <summary>A decision to take over rests on a silence, and finding the reason out
    /// takes time: the probe is asked, then the brokers, then the topic's end — seconds,
    /// when brokers are only just back. A leader heard in those seconds is a leader. As
    /// first built the decision stood on the silence measured before the questions, and
    /// a standby that had held through a whole broker outage took over from a leader it
    /// had heard twenty milliseconds earlier.</summary>
    [Fact]
    public async Task A_leader_heard_while_the_standby_was_asking_the_brokers_is_not_taken_over_from()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a");
        await LeadsAsync(a);
        var ceiling = TimeSpan.FromSeconds(2);
        await using var b = await StartAsync(env, "b", ceiling: ceiling, probe: _ => ValueTask.FromResult(true));
        await Task.Delay(After);

        using var stall = Stall(a);
        var asked = 0;
        b.Log.WritePathOverride = () =>
        {
            // The standby is at the ceiling and asks the brokers. While it waits for the
            // answer the leader comes back and is heard; the answer is "nothing wrong".
            if (Interlocked.Increment(ref asked) == 1)
            {
                var beats = a.Log.IdleHeartbeats;
                stall.Set();
                SpinWait.SpinUntil(() => a.Log.IdleHeartbeats >= beats + 2, Timeout);
                SpinWait.SpinUntil(() => b.Leadership.Status.HeartbeatAge < Beat * 2, Timeout);
            }
            return (KafkaLog.WritePath.Writable, null);
        };

        await KafkaEnv.EventuallyAsync(() => Task.FromResult(Volatile.Read(ref asked) >= 1), "b to reach the ceiling and ask");
        await Task.Delay(After * 3);
        Assert.Equal(NodeRole.Standby, b.Role);
        Assert.Equal(NodeRole.Leader, a.Role);
        Assert.True(a.Log.IsWriter);
        Assert.Empty(b.Calls);
        Assert.Equal(0, b.Leadership.TakeoversAbandoned);
        Assert.Equal(0, b.Log.WriterResets);
        Assert.Equal(1, b.Leadership.Status.LeaderTerm);
        await CommitStateAsync(a.Log, "p", "still mine");
    }

    /// <summary>The other side of that line. Once the standby has taken the transactional
    /// id the leader is fenced, whatever it committed a moment before; a heartbeat of its
    /// that is only heard now changes nothing, and a standby that stood down for it would
    /// leave a fenced leader and nobody else. The takeover goes through.</summary>
    [Theory]
    [InlineData("initialised")]
    [InlineData("term-read")]
    public async Task A_takeover_that_hears_the_leader_once_it_has_taken_the_log_goes_through(string point)
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a");
        await LeadsAsync(a);
        await using var b = await StartAsync(env, "b");
        await Task.Delay(After);

        using var stall = Stall(a);
        var met = 0;
        b.Log.WhileAcquiring = at =>
        {
            if (at != point || Interlocked.Exchange(ref met, 1) != 0)
                return;

            // A heartbeat the leader committed just before it was fenced, read by the
            // standby's watch only now.
            b.Leadership.Heard(new KafkaLog.LeaderRecord(1, "a", DateTimeOffset.UtcNow, Sequence: int.MaxValue));
        };

        await LeadsAsync(b);
        Assert.Equal(1, met);
        Assert.Equal(0, b.Leadership.TakeoversAbandoned);
        Assert.Equal(0, b.Log.WriterResets);
        Assert.Equal(2, b.Leadership.Term);
        Assert.Equal(["prepare", "start 2"], b.Calls);
        List<NodeRole> roles;
        lock (b.Changes)
            roles = [.. b.Changes.Select(c => c.Role)];
        Assert.DoesNotContain(NodeRole.Standby, roles.SkipWhile(r => r != NodeRole.Promoting));
        await CommitStateAsync(b.Log, "p", "mine now");

        // The old leader finds itself fenced, as after any takeover.
        stall.Set();
        await a.Leadership.WhenFinished.WaitAsync(Timeout);
        Assert.Equal(NodeRole.Fenced, a.Role);
        Assert.False(a.Leadership.Status.TakeoverFailed);
    }

    /// <summary>
    /// A heartbeat says the leader was alive when it wrote it. One that the brokers
    /// could not show until now — its partition waited on a replica that had died with
    /// the leader, and is released when the brokers give that replica up, nine seconds
    /// on — is news nine seconds old: it must not call off the takeover that those nine
    /// seconds of silence began, and it must not start the silence again. It did both,
    /// once in thirteen takeovers with a broker killed beside the leader, and the
    /// standby led a takeover timeout later than it could have (11.9 s for 9.2).
    /// </summary>
    [Fact]
    public async Task The_last_heartbeat_of_a_dead_leader_read_late_does_not_call_the_takeover_off()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a");
        await LeadsAsync(a);
        await using var b = await StartAsync(env, "b");
        await Task.Delay(After);      // b has read a's heartbeats as they were written

        using var stall = Stall(a);
        var met = 0;
        var silence = TimeSpan.Zero;
        b.Log.WhileAcquiring = point =>
        {
            if (point != "begin" || Interlocked.Exchange(ref met, 1) != 0)
                return;

            // The standby has decided and waits on the brokers, as the leader's last
            // record does. When they move, the watch reads that record: a heartbeat the
            // leader wrote a tenth of a second after the last one that was heard, and
            // more than the takeover timeout ago.
            Thread.Sleep(After);
            var written = DateTimeOffset.UtcNow - b.Leadership.Status.HeartbeatAge + Beat;
            b.Leadership.Heard(new KafkaLog.LeaderRecord(1, "a", written, Sequence: int.MaxValue));
            silence = b.Leadership.Status.HeartbeatAge;
        };

        await LeadsAsync(b);
        Assert.Equal(1, met);
        Assert.Equal(0, b.Leadership.TakeoversAbandoned);
        Assert.Equal(0, b.Log.WriterResets);
        Assert.Equal("silence", b.Leadership.Status.LastTakeover!.Reason);
        Assert.Equal(2, b.Leadership.Term);

        // The silence went on counting, from the moment the record was written.
        Assert.InRange(silence, After, After * 2 + TimeSpan.FromSeconds(5));

        // The old leader finds itself fenced, as after any takeover.
        stall.Set();
        await a.Leadership.WhenFinished.WaitAsync(Timeout);
        Assert.Equal(NodeRole.Fenced, a.Role);
    }

    /// <summary>The other side: a record the leader wrote within the silence a takeover
    /// was decided on calls it off, however late it is read — and at the ceiling, where
    /// the leader is known to be alive and has been silent for longer than the takeover
    /// timeout by definition, that silence is the ceiling.</summary>
    [Fact]
    public async Task A_heartbeat_written_within_the_ceiling_calls_a_takeover_at_the_ceiling_off()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a");
        await LeadsAsync(a);
        var ceiling = TimeSpan.FromSeconds(3);
        await using var b = await StartAsync(env, "b", ceiling: ceiling, probe: _ => ValueTask.FromResult(true));
        await Task.Delay(After);
        b.Log.WritePathOverride = () => (KafkaLog.WritePath.Writable, null);

        using var stall = Stall(a);
        var met = 0;
        b.Log.WhileAcquiring = point =>
        {
            if (point != "begin" || Interlocked.Exchange(ref met, 1) != 0)
                return;

            // Written longer ago than the takeover timeout and well within the ceiling:
            // the leader that the probe says is there has shown itself.
            var written = DateTimeOffset.UtcNow - After - Beat * 3;
            b.Leadership.Heard(new KafkaLog.LeaderRecord(1, "a", written, Sequence: int.MaxValue));
        };

        await KafkaEnv.EventuallyAsync(() => Task.FromResult(b.Leadership.TakeoversAbandoned >= 1), "b's takeover at the ceiling to be given up");
        Assert.Equal(1, met);
        Assert.Equal(NodeRole.Standby, b.Role);
        Assert.True(a.Log.IsWriter);
        stall.Set();
        await CommitStateAsync(a.Log, "p", "still mine");
    }

    /// <summary>After the brokers are back a leader they held up is given two takeover
    /// timeouts to be heard. An old record of its, shown by the brokers as they return,
    /// is not that: the wait goes on to its end and is not cut short for it — nor does
    /// the record count as the leader having been heard.</summary>
    [Fact]
    public async Task An_old_heartbeat_shown_as_the_brokers_return_does_not_end_the_wait_for_the_leader()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a");
        await LeadsAsync(a);
        var ceiling = TimeSpan.FromSeconds(2);
        await using var b = await StartAsync(env, "b", ceiling: ceiling, probe: _ => ValueTask.FromResult(true));
        await Task.Delay(After);
        b.Log.WritePathOverride = () => (KafkaLog.WritePath.Unwritable, "the test says so");

        using var stall = Stall(a);
        await Task.Delay(ceiling * 2);
        Assert.Equal(NodeRole.Standby, b.Role);
        Assert.True(b.Leadership.TakeoversHeld > 0);

        // The brokers are back, and with them a heartbeat the leader wrote when they went.
        b.Log.WritePathOverride = () => (KafkaLog.WritePath.Writable, null);
        var back = Stopwatch.StartNew();
        b.Leadership.Heard(new KafkaLog.LeaderRecord(1, "a", DateTimeOffset.UtcNow - ceiling, Sequence: int.MaxValue));
        await b.Leadership.WhenRoleAsync(r => r != NodeRole.Standby).WaitAsync(Timeout);
        Assert.True(back.Elapsed >= After * 2 - Beat * 2, $"the standby took over {back.Elapsed} after the brokers were back; the leader is given {After * 2}");
        await LeadsAsync(b);
        Assert.Equal("ceiling", b.Leadership.Status.LastTakeover!.Reason);
        Assert.Equal(0, b.Leadership.TakeoversAbandoned);
        stall.Set();
        await a.Leadership.WhenFinished.WaitAsync(Timeout);
    }

    /// <summary>What a standby says of the leader's silence is counted from when the
    /// leader wrote, too: a record read late does not make the leader look alive now,
    /// and an older one does not move the count back.</summary>
    [Fact]
    public async Task The_silence_is_counted_from_when_a_heartbeat_was_written()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a");
        await LeadsAsync(a);
        await using var b = await StartAsync(env, "b", ceiling: TimeSpan.FromSeconds(60), probe: _ => ValueTask.FromResult(true));
        await Task.Delay(After);

        using var stall = Stall(a);
        await KafkaEnv.EventuallyAsync(() => Task.FromResult(b.Leadership.Status.HeartbeatAge > TimeSpan.FromSeconds(3.5)), "the leader to have been silent for three and a half seconds");

        // Written two seconds ago, read now: two seconds of silence, less the shortest
        // time any of the leader's records took to reach this node (what the dating
        // cannot tell from the clocks' offset; milliseconds here, and given a second).
        b.Leadership.Heard(new KafkaLog.LeaderRecord(1, "a", DateTimeOffset.UtcNow - TimeSpan.FromSeconds(2), Sequence: int.MaxValue));
        Assert.InRange(b.Leadership.Status.HeartbeatAge, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2.8));

        // Written now.
        b.Leadership.Heard(new KafkaLog.LeaderRecord(1, "a", DateTimeOffset.UtcNow, Sequence: int.MaxValue));
        Assert.InRange(b.Leadership.Status.HeartbeatAge, TimeSpan.Zero, TimeSpan.FromMilliseconds(500));
        Assert.Equal(NodeRole.Standby, b.Role);
        stall.Set();
    }

    /// <summary>At the ceiling a standby takes over from a leader that still answers —
    /// unless the brokers say that nothing can be committed by anybody, in which case the
    /// leader's place is no better than its own.</summary>
    [Fact]
    public async Task At_the_ceiling_a_standby_does_not_take_over_while_the_brokers_can_commit_nothing()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a");
        await LeadsAsync(a);
        var ceiling = TimeSpan.FromSeconds(2);
        await using var b = await StartAsync(env, "b", ceiling: ceiling, probe: _ => ValueTask.FromResult(true));
        await Task.Delay(After);
        b.Log.WritePathOverride = () => (KafkaLog.WritePath.Unwritable, "the test says so");

        using var stall = Stall(a);
        await Task.Delay(ceiling * 2.5);
        Assert.Equal(NodeRole.Standby, b.Role);
        Assert.True(b.Leadership.TakeoversHeld > 0);
        Assert.True(a.Log.IsWriter);

        // The brokers are back and the leader is still silent. It is given two takeover
        // timeouts to be heard — its client has to find the brokers again — and then it
        // is the leader's fault.
        b.Log.WritePathOverride = () => (KafkaLog.WritePath.Writable, null);
        var back = Stopwatch.StartNew();
        await b.Leadership.WhenRoleAsync(r => r != NodeRole.Standby).WaitAsync(Timeout);
        Assert.True(back.Elapsed >= After * 2 - Beat, $"the standby took over {back.Elapsed} after the brokers were back; the leader is given {After * 2}");
        Assert.True(back.Elapsed < After * 2 + TimeSpan.FromSeconds(10), $"the standby waited {back.Elapsed} after the brokers were back; the leader is given {After * 2} and no more");
        await LeadsAsync(b);
        Assert.Equal("ceiling", b.Leadership.Status.LastTakeover!.Reason);
        Assert.Equal(2, b.Leadership.Term);

        // Let the old leader's writer go, or its disposal waits for it: it finds itself fenced.
        stall.Set();
        await a.Leadership.WhenFinished.WaitAsync(Timeout);
        Assert.Equal(NodeRole.Fenced, a.Role);
    }

    [Fact]
    public async Task What_the_brokers_say_about_writing_is_asked_of_them_and_the_coordinator_is_placed_as_they_place_it()
    {
        await using var env = KafkaEnv.Create();
        var log = await env.ConnectWriterAsync(env.Options(nodeId: "a"));
        var (path, why) = await log.ProbeWritePathAsync(default).WaitAsync(Timeout);
        Assert.Equal(KafkaLog.WritePath.Writable, path);
        Assert.Null(why);

        // The lists of who leads and who is in sync are not believed by themselves: they
        // are the controller's, and with no controller quorum they go on naming brokers
        // that are gone. Each broker named is asked directly, and the broker the real
        // cluster answers for is asked for real (above: it answered). One that is not
        // there — here, an id no broker has — is a second and a half of silence.
        var silent = Stopwatch.StartNew();
        Assert.False(await log.BrokerAnswersAsync(9_999, default).WaitAsync(Timeout));
        Assert.InRange(silent.Elapsed.TotalSeconds, 1, 6);

        // What counts as an answer when the question fails: an error the broker sent —
        // a node that may not describe brokers is refused, and the broker is there to
        // refuse it — and nothing the client made up in its place, and no wait that ran out.
        static Confluent.Kafka.Admin.DescribeConfigsException Described(ErrorCode code) => new(
            [new Confluent.Kafka.Admin.DescribeConfigsReport { ConfigResource = new Confluent.Kafka.Admin.ConfigResource { Type = Confluent.Kafka.Admin.ResourceType.Broker, Name = "1" }, Error = new Error(code) }]);
        Assert.True(KafkaLog.AnsweredWith(Described(ErrorCode.ClusterAuthorizationFailed)));
        Assert.True(KafkaLog.AnsweredWith(new KafkaException(new Error(ErrorCode.ClusterAuthorizationFailed))));
        Assert.False(KafkaLog.AnsweredWith(Described(ErrorCode.Local_TimedOut)));
        Assert.False(KafkaLog.AnsweredWith(Described(ErrorCode.Local_Transport)));
        Assert.False(KafkaLog.AnsweredWith(new KafkaException(new Error(ErrorCode.Local_Transport))));
        Assert.False(KafkaLog.AnsweredWith(new KafkaException(new Error(ErrorCode.Local_TimedOut))));
        Assert.False(KafkaLog.AnsweredWith(new TimeoutException()));

        var asked = new List<int>();
        log.BrokerAnswersOverride = broker =>
        {
            lock (asked)
                asked.Add(broker);
            return false;
        };
        (path, why) = await log.ProbeWritePathAsync(default).WaitAsync(Timeout);
        Assert.Equal(KafkaLog.WritePath.Unwritable, path);
        Assert.Matches(@"\.leader \[0\] is led by broker \d+, which does not answer$", why);
        Assert.NotEmpty(asked);
        log.BrokerAnswersOverride = null;
        foreach (var broker in asked.Distinct())
            Assert.True(await log.BrokerAnswersAsync(broker, default).WaitAsync(Timeout), $"broker {broker} leads the partition and answers");

        log.BrokerAnswersOverride = _ => true;
        Assert.Equal(KafkaLog.WritePath.Writable, (await log.ProbeWritePathAsync(default).WaitAsync(Timeout)).Path);
        log.BrokerAnswersOverride = null;

        // The judgement, on what three brokers look like after two of them have gone at
        // once: all three still listed in sync, and only one there to answer.
        static string? Why(int leader, int[] inSync, int[] answering, int? minimum) =>
            KafkaLog.WhyUnwritable("p.leader", 0, leader, inSync, answering.ToHashSet(), minimum);
        Assert.Equal("p.leader [0] has no leader", Why(-1, [1, 2, 3], [1, 2, 3], 2));
        Assert.Equal("p.leader [0] is led by broker 1, which does not answer", Why(1, [1, 2, 3], [3], 2));
        Assert.Equal("p.leader [0] has 1 replica(s) in sync that answer (3 listed) and needs 2", Why(3, [1, 2, 3], [3], 2));
        Assert.Null(Why(3, [1, 2, 3], [2, 3], 2));
        Assert.Null(Why(3, [3, 2], [2, 3], 2));
        Assert.Equal("p.leader [0] has 1 replica(s) in sync that answer (1 listed) and needs 2", Why(3, [3], [1, 2, 3], 2));
        // A minimum that could not be asked is not guessed: the leader answers, and that is all that is known.
        Assert.Null(Why(3, [1, 2, 3], [3], null));

        // Java's String.hashCode, which is what places a transactional id on a partition
        // of __transaction_state: values from a JVM.
        Assert.Equal(0, KafkaLog.JavaHash(""));
        Assert.Equal(96354, KafkaLog.JavaHash("abc"));
        Assert.Equal(984754439, KafkaLog.JavaHash("incremental.writer"));
    }

    /// <summary>A step-down whose resignation the brokers will not take. The circuits are
    /// already stopped, so a writer that went on heartbeating would hold the standby off a
    /// leader that leads nothing: the writer is closed and the standby takes over on the
    /// silence.</summary>
    [Fact]
    public async Task A_step_down_whose_resignation_cannot_commit_closes_the_writer_and_the_standby_takes_over_on_silence()
    {
        await using var env = KafkaEnv.Create();
        var a = await env.ConnectAsync(env.Options(nodeId: "a", heartbeat: Beat, maxAbortRetries: 1));
        await using var leadershipA = new KafkaLeadership(a, new KafkaLeadershipOptions
        {
            TakeoverAfter = After,
            StartLeadingAsync = (_, _) => Task.CompletedTask,
        });
        await leadershipA.StartAsync().WaitAsync(Timeout);
        await leadershipA.WhenRoleAsync(NodeRole.Leader).WaitAsync(Timeout);
        await using var b = await StartAsync(env, "b");
        await Task.Delay(After);

        a.FailTransaction = commits => commits.Any(c => c.Resigns);
        var failure = await Assert.ThrowsAsync<KafkaLogException>(() => leadershipA.StepDownAsync()).WaitAsync(Timeout);
        Assert.IsNotType<KafkaLogFencedException>(failure);
        Assert.Equal(NodeRole.Fenced, leadershipA.Role);
        Assert.True(leadershipA.WhenFinished.IsCompleted);
        Assert.False(a.IsWriter);
        Assert.True(a.IsFenced);

        // One heartbeat may be in flight: the writer was idle past its interval while the
        // resignation was being refused, so it began one in the moment between that
        // failure and the close. It lands or fails within a beat; none begins after it.
        await Task.Delay(Beat * 3);
        var beats = a.IdleHeartbeats;

        await LeadsAsync(b);
        Assert.Equal("silence", b.Leadership.Status.LastTakeover!.Reason);
        Assert.Equal(beats, a.IdleHeartbeats);
        Assert.DoesNotContain(await LeaderTopicAsync(b.Log), r => r.Value.TryGetProperty("Resigned", out var resigned) && resigned.GetBoolean());
    }

    /// <summary>Terms never fall. A takeover whose read of the last term comes back lower
    /// than a term this process has already read — the answer of a partition whose leader
    /// was elected a moment ago with a broker down — must not announce a used term.</summary>
    [Fact]
    public async Task A_takeover_does_not_announce_a_term_below_one_it_has_read()
    {
        await using var env = KafkaEnv.Create();
        var first = await env.ConnectWriterAsync(env.Options(nodeId: "first", heartbeat: Beat));
        Assert.Equal(1, first.Term);

        var second = await env.ConnectAsync(env.Options(nodeId: "second", heartbeat: Beat, operationTimeout: TimeSpan.FromSeconds(2)));
        second.NoteTermSeen(7);   // as if it had followed the topic to term 7 and the topic now gave 1
        var refused = await Assert.ThrowsAsync<KafkaLogException>(() => second.AcquireWriterAsync()).WaitAsync(Timeout);
        Assert.IsNotType<KafkaLogFencedException>(refused);
        Assert.Contains("has read term 7", refused.Message);
        Assert.False(second.IsWriter);
        Assert.Equal(0, second.Term);
        Assert.DoesNotContain(await LeaderTopicAsync(second), r => r.Key == "term" && r.Value.GetProperty("Node").GetString() == "second");

        // With the floor at what the topic really holds, the same log takes over.
        var third = await env.ConnectAsync(env.Options(nodeId: "third", heartbeat: Beat));
        third.NoteTermSeen(1);
        await third.AcquireWriterAsync().WaitAsync(Timeout);
        Assert.Equal(2, third.Term);
    }

    /// <summary>The other end of the same rule: the brokers come back, and so does the
    /// leader. Its first heartbeat ends the matter; nobody is fenced.</summary>
    [Fact]
    public async Task A_leader_that_commits_again_once_the_brokers_are_back_keeps_leading_past_the_ceiling()
    {
        await using var env = KafkaEnv.Create();
        await using var a = await StartAsync(env, "a");
        await LeadsAsync(a);
        var ceiling = TimeSpan.FromSeconds(2);
        await using var b = await StartAsync(env, "b", ceiling: ceiling, probe: _ => ValueTask.FromResult(true));
        await Task.Delay(After);
        b.Log.WritePathOverride = () => (KafkaLog.WritePath.Unwritable, "the test says so");

        using var stall = Stall(a);
        await Task.Delay(ceiling * 2);
        Assert.True(b.Leadership.TakeoversHeld > 0);

        // Back: the brokers can commit, and the leader — a moment later — does.
        b.Log.WritePathOverride = () => (KafkaLog.WritePath.Writable, null);
        await Task.Delay(After / 2);
        stall.Set();
        await Task.Delay(ceiling * 2);
        Assert.Equal(NodeRole.Standby, b.Role);
        Assert.Equal(NodeRole.Leader, a.Role);
        Assert.True(a.Log.IsWriter);
        Assert.Equal(1, b.Leadership.Status.LeaderTerm);
        Assert.Equal(0, b.Leadership.TakeoversAbandoned);
        await CommitStateAsync(a.Log, "p", "still mine");
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
