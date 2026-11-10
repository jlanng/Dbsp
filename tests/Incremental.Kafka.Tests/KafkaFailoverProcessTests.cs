using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Incremental.Kafka.FailoverApp;

namespace Incremental.Kafka.Tests;

/// <summary>Process tests measure wall-clock failover, so they do not share the machine
/// with the rest of the suite.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class FailoverProcessCollection
{
    public const string Name = "failover processes";
}

/// <summary>
/// Failover between REAL operating-system processes (tests/Incremental.Kafka.FailoverApp,
/// launched twice as <c>dotnet …FailoverApp.dll</c>): one leads, one stands by, and the
/// test does to the leader what production does — <c>kill -9</c>, <c>SIGSTOP</c> past the
/// takeover timeout, a manual takeover, a step down, a simultaneous cold start. Each run
/// ends by reading the sink topic and comparing it with the answer
/// <see cref="FailoverScenario.Expected"/> computes without running anything: no upsert
/// lost, none left stale.
/// </summary>
[Collection(FailoverProcessCollection.Name)]
public class KafkaFailoverProcessTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(90);

    // ── a node: one process, its events, its stdin ─────────────────────────────────────

    private sealed class AppNode : IDisposable
    {
        private readonly Process _process;
        private readonly List<JsonElement> _events = [];
        private readonly StringBuilder _stderr = new();
        private TaskCompletionSource _arrived = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public AppNode(string prefix, string id, long steps, int takeoverAfterMs, int heartbeatMs, int probePort = 0, int peerPort = 0)
        {
            Id = id;
            var dll = AppDllPath();
            var info = new ProcessStartInfo("dotnet")
            {
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                WorkingDirectory = Path.GetDirectoryName(dll)!,
            };
            foreach (var argument in new[]
            {
                dll,
                "--bootstrap", KafkaEnv.Bootstrap,
                "--prefix", prefix,
                "--node", id,
                "--steps", steps.ToString(CultureInfo.InvariantCulture),
                "--takeover-after-ms", takeoverAfterMs.ToString(CultureInfo.InvariantCulture),
                "--heartbeat-ms", heartbeatMs.ToString(CultureInfo.InvariantCulture),
                "--probe-port", probePort.ToString(CultureInfo.InvariantCulture),
                "--peer-port", peerPort.ToString(CultureInfo.InvariantCulture),
            })
                info.ArgumentList.Add(argument);
            _process = Process.Start(info) ?? throw new InvalidOperationException("Failed to start the failover app.");
            _process.OutputDataReceived += (_, e) =>
            {
                if (e.Data is not { Length: > 0 } line || line[0] != '{')
                    return;
                TaskCompletionSource arrived;
                lock (_events)
                {
                    _events.Add(JsonDocument.Parse(line).RootElement);
                    arrived = _arrived;
                    _arrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                }
                arrived.TrySetResult();
            };
            _process.ErrorDataReceived += (_, e) =>
            {
                lock (_stderr)
                    _stderr.AppendLine(e.Data);
            };
            _process.BeginOutputReadLine();
            _process.BeginErrorReadLine();
        }

        public string Id { get; }

        public int Pid => _process.Id;

        public bool HasExited => _process.HasExited;

        public List<JsonElement> Events(string kind)
        {
            lock (_events)
                return _events.Where(e => e.GetProperty("e").GetString() == kind).ToList();
        }

        /// <summary>The first event of <paramref name="kind"/> that satisfies
        /// <paramref name="predicate"/>, waiting for it if it has not arrived.</summary>
        public async Task<JsonElement> WaitAsync(string kind, Func<JsonElement, bool>? predicate = null, TimeSpan? timeout = null)
        {
            var deadline = Stopwatch.StartNew();
            var limit = timeout ?? Patience;
            while (true)
            {
                Task arrived;
                lock (_events)
                {
                    foreach (var e in _events)
                    {
                        if (e.GetProperty("e").GetString() == kind && (predicate is null || predicate(e)))
                            return e;
                    }
                    arrived = _arrived.Task;
                }
                if (deadline.Elapsed > limit)
                    Assert.Fail($"Node '{Id}' did not report '{kind}' within {limit}.{Environment.NewLine}{Dump()}");
                if (_process.HasExited)
                {
                    // Let the last lines arrive, then look once more.
                    await Task.Delay(200);
                    lock (_events)
                    {
                        if (!_events.Any(e => e.GetProperty("e").GetString() == kind && (predicate is null || predicate(e))))
                            Assert.Fail($"Node '{Id}' exited with code {_process.ExitCode} before reporting '{kind}'.{Environment.NewLine}{Dump()}");
                    }
                    continue;
                }
                await Task.WhenAny(arrived, Task.Delay(200));
            }
        }

        public Task<JsonElement> WaitForRoleAsync(string role, TimeSpan? timeout = null) =>
            WaitAsync("role", e => e.GetProperty("role").GetString() == role, timeout);

        public Task<JsonElement> WaitForStepAsync(long step) =>
            WaitAsync("progress", e => e.GetProperty("step").GetInt64() >= step);

        /// <summary>Asks for a status line and returns the answer.</summary>
        public async Task<JsonElement> StatusAsync()
        {
            var asked = Events("status").Count;
            Send("status");
            var deadline = Stopwatch.StartNew();
            while (Events("status").Count == asked)
            {
                if (deadline.Elapsed > Patience)
                    Assert.Fail($"Node '{Id}' did not answer a status request.{Environment.NewLine}{Dump()}");
                await Task.Delay(20);
            }
            return Events("status")[^1];
        }

        public void Send(string command)
        {
            _process.StandardInput.WriteLine(command);
            _process.StandardInput.Flush();
        }

        /// <summary><c>kill -9</c>.</summary>
        public void Kill() => _process.Kill();

        /// <summary><c>kill -STOP</c> / <c>kill -CONT</c>: the process freezes where it
        /// stands and later carries on, as after a long GC pause or a frozen VM.</summary>
        public void Signal(string signal)
        {
            using var kill = Process.Start("kill", [$"-{signal}", Pid.ToString(CultureInfo.InvariantCulture)]);
            kill.WaitForExit();
            Assert.Equal(0, kill.ExitCode);
        }

        public async Task<int> ExitCodeAsync(TimeSpan? timeout = null)
        {
            using var limit = new CancellationTokenSource(timeout ?? Patience);
            try
            {
                await _process.WaitForExitAsync(limit.Token);
            }
            catch (OperationCanceledException)
            {
                Assert.Fail($"Node '{Id}' did not exit.{Environment.NewLine}{Dump()}");
            }
            return _process.ExitCode;
        }

        public string Dump()
        {
            lock (_events)
            {
                lock (_stderr)
                    return $"--- node {Id} (pid {Pid}) last events ---{Environment.NewLine}" +
                        string.Join(Environment.NewLine, _events.TakeLast(25).Select(e => e.GetRawText())) +
                        $"{Environment.NewLine}--- stderr ---{Environment.NewLine}{_stderr}";
            }
        }

        public void Dispose()
        {
            try
            {
                if (!_process.HasExited)
                    _process.Kill(entireProcessTree: true);
            }
            catch (InvalidOperationException)
            {
            }
            _process.Dispose();
        }

        private static string AppDllPath()
        {
            // tests/Incremental.Kafka.Tests/bin/{Config}/{Tfm} → tests/Incremental.Kafka.FailoverApp/bin/{Config}/{Tfm}
            var baseDir = AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar);
            var tfm = Path.GetFileName(baseDir);
            var config = Path.GetFileName(Path.GetDirectoryName(baseDir)!);
            var dll = Path.GetFullPath(Path.Combine(
                baseDir, "..", "..", "..", "..", "Incremental.Kafka.FailoverApp", "bin", config, tfm, "Incremental.Kafka.FailoverApp.dll"));
            Assert.True(File.Exists(dll), $"Failover app not found at {dll} — build tests/Incremental.Kafka.FailoverApp.");
            return dll;
        }
    }

    private static long T(JsonElement e) => e.GetProperty("t").GetInt64();

    private static long Now => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static int[] FreePorts(int count)
    {
        var listeners = Enumerable.Range(0, count).Select(_ => new TcpListener(IPAddress.Loopback, 0)).ToArray();
        foreach (var listener in listeners)
            listener.Start();
        var ports = listeners.Select(l => ((IPEndPoint)l.LocalEndpoint).Port).ToArray();
        foreach (var listener in listeners)
            listener.Stop();
        return ports;
    }

    /// <summary>Waits until the standby has built its circuit and holds everything the
    /// leader has committed, and checks what it says about the leader meanwhile.</summary>
    private static async Task<JsonElement> WaitForCaughtUpStandbyAsync(AppNode standby, string leader, long leaderTerm)
    {
        await standby.WaitAsync("ready");
        var deadline = Stopwatch.StartNew();
        while (true)
        {
            var status = await standby.StatusAsync();
            Assert.Equal("Standby", status.GetProperty("role").GetString());
            if (status.GetProperty("stateLag").EnumerateArray().Sum(l => l.GetInt64()) < 50
                && status.GetProperty("tableLag").GetProperty(FailoverScenario.ProductsTable).GetInt64() == 0
                && status.GetProperty("leader").GetString() == leader)
            {
                Assert.Equal(leaderTerm, status.GetProperty("leaderTerm").GetInt64());
                Assert.False(status.GetProperty("holdsLease").GetBoolean());
                return status;
            }
            if (deadline.Elapsed > Patience)
                Assert.Fail($"Standby '{standby.Id}' did not catch up: {status.GetRawText()}");
            await Task.Delay(100);
        }
    }

    // ── the sink, as the test reads it ─────────────────────────────────────────────────

    private sealed record SinkRecord(long Offset, string Key, string? Value, long Term, long Sequence, string Node);

    private static List<SinkRecord> ReadSink(string prefix)
    {
        var partition = new TopicPartition(prefix + ".sink", 0);
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = KafkaEnv.Bootstrap, GroupId = prefix + ".test", EnableAutoCommit = false, EnablePartitionEof = true,
        }).Build();
        var end = consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(30)).High.Value;
        consumer.Assign(new TopicPartitionOffset(partition, 0));
        var records = new List<SinkRecord>();
        var idle = Stopwatch.StartNew();
        while (records.Count == 0 || records[^1].Offset < end - 1)
        {
            var result = consumer.Consume(TimeSpan.FromMilliseconds(200));
            if (result is null || result.IsPartitionEOF)
            {
                if (result is not null && result.Offset.Value >= end)
                    break;
                Assert.True(idle.Elapsed < Patience, "Reading the sink topic stalled.");
                continue;
            }
            idle.Restart();
            string Header(string name) => Encoding.UTF8.GetString(result.Message.Headers.GetLastBytes(name));
            records.Add(new SinkRecord(
                result.Offset.Value,
                Encoding.UTF8.GetString(result.Message.Key),
                result.Message.Value is { } value ? Encoding.UTF8.GetString(value) : null,
                long.Parse(Header("term"), CultureInfo.InvariantCulture),
                long.Parse(Header("seq"), CultureInfo.InvariantCulture),
                Header("node")));
        }
        return records;
    }

    private sealed record SinkVerdict(int Records, List<SinkRecord> Stale, IReadOnlyDictionary<long, int> RecordsByTerm, int Redelivered);

    /// <summary>
    /// Judges the sink topic against the model. Two readings of the topic are compared with
    /// it: the plain one, where the last record for a key wins — any Kafka consumer's view —
    /// and the conditional one the design describes for a sink that can compare terms,
    /// where a record from an older term than the key already holds is refused.
    /// <paramref name="plainMustMatch"/> says whether the plain reading has to be right too;
    /// it must whenever no deposed leader was alive to write late.
    /// </summary>
    private SinkVerdict Judge(string prefix, long steps, bool plainMustMatch)
    {
        var records = ReadSink(prefix);
        var plain = new Dictionary<string, string>();
        var conditional = new Dictionary<string, (long Term, string? Value)>();
        var stale = new List<SinkRecord>();
        var highestTerm = 0L;
        foreach (var record in records)
        {
            if (record.Value is null)
                plain.Remove(record.Key);
            else
                plain[record.Key] = record.Value;

            // Written under a term after a later term had already written: a deposed
            // leader's late delivery, the sink gap.
            if (record.Term < highestTerm)
                stale.Add(record);
            highestTerm = Math.Max(highestTerm, record.Term);
            if (!conditional.TryGetValue(record.Key, out var held) || record.Term >= held.Term)
                conditional[record.Key] = (record.Term, record.Value);
        }

        var (enriched, totals, counts) = FailoverScenario.Expected(steps);
        var expected = new Dictionary<string, string>();
        foreach (var (id, row) in enriched)
            expected[$"{FailoverScenario.EnrichedSink}|{id}"] = JsonSerializer.Serialize(row, FailoverScenario.Json);
        foreach (var (customer, total) in totals)
            expected[$"{FailoverScenario.TotalsSink}|{customer}"] = total.ToString(CultureInfo.InvariantCulture);
        foreach (var (bucket, count) in counts)
            expected[$"{FailoverScenario.CountsSink}|{bucket}"] = count.ToString(CultureInfo.InvariantCulture);
        Assert.Equal(steps * FailoverScenario.Batch, counts.Values.Sum());
        Assert.True(enriched.Count > 100 && totals.Count > 10, "the model is too small to mean anything");

        var settled = conditional.Where(kv => kv.Value.Value is not null).ToDictionary(kv => kv.Key, kv => kv.Value.Value!);
        AssertSame(expected, settled, "the sink, read with the term rule,");
        if (plainMustMatch)
            AssertSame(expected, plain, "the sink, read as last write wins,");
        else if (!Same(expected, plain))
            output.WriteLine("the plain reading of the sink differs from the model: a deposed leader's late write landed after its successor's");

        // A step delivered and not committed before the failure is delivered again by the
        // successor: the same sequence under two terms.
        var sequencesByTerm = records.GroupBy(r => r.Term).ToDictionary(g => g.Key, g => g.Select(r => r.Sequence).ToHashSet());
        var redelivered = sequencesByTerm.Keys.Order().Skip(1)
            .Sum(term => sequencesByTerm[term].Count(sequence => sequencesByTerm.Any(other => other.Key < term && other.Value.Contains(sequence))));
        return new SinkVerdict(records.Count, stale, records.GroupBy(r => r.Term).ToDictionary(g => g.Key, g => g.Count()), redelivered);
    }

    private static bool Same(Dictionary<string, string> expected, Dictionary<string, string> actual) =>
        expected.Count == actual.Count && expected.All(kv => actual.TryGetValue(kv.Key, out var value) && value == kv.Value);

    private static void AssertSame(Dictionary<string, string> expected, Dictionary<string, string> actual, string what)
    {
        var missing = expected.Keys.Except(actual.Keys).Order().Take(5).ToList();
        var extra = actual.Keys.Except(expected.Keys).Order().Take(5).ToList();
        var wrong = expected.Where(kv => actual.TryGetValue(kv.Key, out var value) && value != kv.Value)
            .Select(kv => $"{kv.Key}: expected {kv.Value}, sink has {actual[kv.Key]}").Take(5).ToList();
        Assert.True(missing.Count == 0 && extra.Count == 0 && wrong.Count == 0,
            $"{what} does not hold what an uninterrupted run produces ({expected.Count} rows expected, {actual.Count} held). " +
            $"Lost: [{string.Join(", ", missing)}]. Left behind: [{string.Join(", ", extra)}]. Wrong: [{string.Join("; ", wrong)}].");
    }

    private static void AssertDone(JsonElement done, long steps) =>
        Assert.Equal(steps * FailoverScenario.Batch - 1, done.GetProperty("committed").GetInt64());

    private static string Describe(JsonElement leaderRole)
    {
        var t = leaderRole.GetProperty("takeover");
        double Ms(string name) => t.GetProperty(name).GetDouble();
        return $"fence and term {Ms("fenceMs"):F0} ms, tables {Ms("tablesMs"):F0} ms, storage {Ms("storageMs"):F0} ms, " +
            $"start {Ms("startMs"):F0} ms (decision to leading {Ms("totalMs"):F0} ms; silent for {Ms("silenceMs"):F0} ms when it decided)";
    }

    // ── the tests ──────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task One_node_alone_reaches_the_model()
    {
        // The yardstick: with no failure at all, the app and the model must agree, or the
        // failover tests below would be measuring the model's mistakes.
        await using var env = KafkaEnv.Create();
        const long steps = 160;
        using var a = new AppNode(env.Prefix, "a", steps, takeoverAfterMs: 1000, heartbeatMs: 200);
        var leader = await a.WaitForRoleAsync("Leader");
        Assert.Equal(1, leader.GetProperty("term").GetInt64());
        AssertDone(await a.WaitAsync("done"), steps);

        var status = await a.StatusAsync();
        Assert.Equal("Leader", status.GetProperty("role").GetString());
        Assert.True(status.GetProperty("holdsLease").GetBoolean());
        a.Send("quit");
        Assert.Equal(0, await a.ExitCodeAsync());

        var verdict = Judge(env.Prefix, steps, plainMustMatch: true);
        Assert.Empty(verdict.Stale);
        Assert.Equal(0, verdict.Redelivered);
        Assert.Equal([1L], verdict.RecordsByTerm.Keys);
    }

    [Fact]
    public async Task Kill_9_of_the_leader_and_the_standby_takes_over_with_nothing_lost()
    {
        await using var env = KafkaEnv.Create();
        const long steps = 800;
        const int takeoverAfter = 3000, heartbeat = 1000;     // the defaults
        var ports = FreePorts(2);
        using var a = new AppNode(env.Prefix, "a", steps, takeoverAfter, heartbeat, probePort: ports[0], peerPort: ports[1]);
        await a.WaitForRoleAsync("Leader");
        using var b = new AppNode(env.Prefix, "b", steps, takeoverAfter, heartbeat, probePort: ports[1], peerPort: ports[0]);
        await a.WaitForStepAsync(200);
        var before = await WaitForCaughtUpStandbyAsync(b, leader: "a", leaderTerm: 1);
        Assert.InRange(before.GetProperty("heartbeatAgeMs").GetDouble(), 0, takeoverAfter);

        // kill -9, mid-stream.
        var lastCommitted = a.Events("progress")[^1].GetProperty("committed").GetInt64();
        a.Kill();
        var killed = Now;
        Assert.Equal(137, await a.ExitCodeAsync());

        var promoting = await b.WaitForRoleAsync("Promoting");
        var leading = await b.WaitForRoleAsync("Leader");
        var resumed = await b.WaitAsync("leading");
        var firstOutput = await b.WaitAsync("first-output");
        var firstCommit = await b.WaitAsync("progress");
        Assert.Equal(2, leading.GetProperty("term").GetInt64());
        Assert.Equal("silence", leading.GetProperty("takeover").GetProperty("reason").GetString());

        // It resumed from a checkpoint the dead leader committed: at or after the last one
        // the dead leader reported, and at the start of a checkpoint interval.
        var resumedAt = resumed.GetProperty("resumedAt").GetInt64();
        Assert.True(resumedAt >= lastCommitted + 1, $"resumed at {resumedAt}, but offset {lastCommitted} was committed");
        Assert.Equal(0, resumedAt % (FailoverScenario.Batch * FailoverScenario.CheckpointInterval));

        long detection = T(promoting) - killed, total = T(firstOutput) - killed;
        KafkaEnv.Measured(output,
            $"kill -9 failover (takeover after {takeoverAfter} ms, heartbeat {heartbeat} ms, probe refused): " +
            $"detection {detection} ms, {Describe(leading)}, first output {T(firstOutput) - T(leading)} ms after leading, " +
            $"first commit {T(firstCommit) - T(leading)} ms after leading; kill to first output {total} ms");
        Assert.InRange(detection, takeoverAfter - heartbeat - 250, 15_000);
        Assert.InRange(total, 0, 20_000);

        AssertDone(await b.WaitAsync("done"), steps);
        var status = await b.StatusAsync();
        Assert.Equal("Leader", status.GetProperty("role").GetString());
        Assert.Equal("b", status.GetProperty("leader").GetString());
        b.Send("quit");
        Assert.Equal(0, await b.ExitCodeAsync());

        // Nothing lost, nothing stale — and with the old leader dead, nothing of term 1
        // after term 2, so even a reader that knows nothing of terms sees the right table.
        var verdict = Judge(env.Prefix, steps, plainMustMatch: true);
        KafkaEnv.Measured(output,
            $"kill -9 failover: {verdict.Records} sink records ({string.Join(", ", verdict.RecordsByTerm.OrderBy(t => t.Key).Select(t => $"term {t.Key}: {t.Value}"))}), " +
            $"{verdict.Redelivered} step(s) delivered under both terms");
        Assert.Empty(verdict.Stale);
        Assert.Equal([1L, 2L], verdict.RecordsByTerm.Keys.Order());
    }

    [Fact]
    public async Task A_leader_paused_past_the_timeout_wakes_up_fenced_and_corrupts_nothing()
    {
        if (OperatingSystem.IsWindows())
            Assert.Skip("SIGSTOP and SIGCONT need a Unix.");
        await using var env = KafkaEnv.Create();
        const long steps = 600;
        const int takeoverAfter = 1500, heartbeat = 300;
        var ports = FreePorts(2);
        using var a = new AppNode(env.Prefix, "a", steps, takeoverAfter, heartbeat, probePort: ports[0], peerPort: ports[1]);
        await a.WaitForRoleAsync("Leader");
        using var b = new AppNode(env.Prefix, "b", steps, takeoverAfter, heartbeat, probePort: ports[1], peerPort: ports[0]);
        await a.WaitForStepAsync(150);
        await WaitForCaughtUpStandbyAsync(b, leader: "a", leaderTerm: 1);

        // Frozen where it stands: its sockets stay open and accept, but it answers nothing.
        a.Signal("STOP");
        var stopped = Now;
        var promoting = await b.WaitForRoleAsync("Promoting");
        var leading = await b.WaitForRoleAsync("Leader");
        var firstOutput = await b.WaitAsync("first-output");
        Assert.Equal(2, leading.GetProperty("term").GetInt64());
        KafkaEnv.Measured(output,
            $"SIGSTOP failover (takeover after {takeoverAfter} ms, heartbeat {heartbeat} ms, probe unanswered): " +
            $"detection {T(promoting) - stopped} ms, {Describe(leading)}, stop to first output {T(firstOutput) - stopped} ms");
        Assert.InRange(T(promoting) - stopped, takeoverAfter - heartbeat - 250, 15_000);
        await b.WaitForStepAsync((await b.WaitAsync("progress")).GetProperty("step").GetInt64() + 40);

        // The zombie wakes with a step half done. It must find out, write nothing durable,
        // and exit as fenced.
        var eventsBefore = a.Events("progress").Count;
        a.Signal("CONT");
        var resumedAt = Stopwatch.StartNew();
        Assert.Equal(3, await a.ExitCodeAsync(TimeSpan.FromSeconds(30)));
        var fenced = await a.WaitForRoleAsync("Fenced");
        KafkaEnv.Measured(output, $"SIGSTOP failover: the resumed leader exited as fenced {resumedAt.ElapsedMilliseconds} ms after SIGCONT");
        Assert.False(string.IsNullOrEmpty(fenced.GetProperty("fault").GetString()));
        Assert.Equal(eventsBefore, a.Events("progress").Count);   // it committed nothing after waking

        // Its supervisor restarts it; it comes back as a standby of the node that replaced it.
        using var a2 = new AppNode(env.Prefix, "a", steps, takeoverAfter, heartbeat, probePort: ports[0], peerPort: ports[1]);
        await WaitForCaughtUpStandbyAsync(a2, leader: "b", leaderTerm: 2);
        var done = await b.WaitAsync("done");
        AssertDone(done, steps);

        // The state the zombie could have damaged is whole: a third leadership, on the
        // restarted node, starts from the last offset and has nothing left to do.
        await WaitForCaughtUpStandbyAsync(a2, leader: "b", leaderTerm: 2);
        a2.Send("takeover");
        var third = await a2.WaitForRoleAsync("Leader");
        Assert.Equal(3, third.GetProperty("term").GetInt64());
        Assert.Equal("manual", third.GetProperty("takeover").GetProperty("reason").GetString());
        Assert.Equal(steps * FailoverScenario.Batch, (await a2.WaitAsync("leading")).GetProperty("resumedAt").GetInt64());
        AssertDone(await a2.WaitAsync("done"), steps);
        Assert.Equal(3, await b.ExitCodeAsync(TimeSpan.FromSeconds(30)));
        a2.Send("quit");
        Assert.Equal(0, await a2.ExitCodeAsync());

        // The lease stops the zombie before it delivers anything it had not already begun:
        // whatever it wrote after term 2 began belongs to the one step that was in flight
        // when it was stopped. The term rule discards that; the table is exact.
        var verdict = Judge(env.Prefix, steps, plainMustMatch: false);
        KafkaEnv.Measured(output,
            $"SIGSTOP failover: {verdict.Stale.Count} sink record(s) written by the zombie after its successor's first " +
            $"(steps {string.Join(", ", verdict.Stale.Select(s => s.Sequence).Distinct())}); {verdict.Redelivered} step(s) delivered under two terms");
        Assert.All(verdict.Stale, record => Assert.Equal(("a", 1L), (record.Node, record.Term)));
        Assert.True(verdict.Stale.Select(s => s.Sequence).Distinct().Count() <= 1,
            $"the zombie delivered more than the step it had in flight: steps {string.Join(", ", verdict.Stale.Select(s => s.Sequence).Distinct())}");
    }

    [Fact]
    public async Task Manual_takeover_then_step_down_move_the_leadership_and_back()
    {
        await using var env = KafkaEnv.Create();
        const long steps = 600;
        const int takeoverAfter = 6000, heartbeat = 500;      // long, so that a quick handover is the command's doing
        var ports = FreePorts(2);
        using var a = new AppNode(env.Prefix, "a", steps, takeoverAfter, heartbeat, probePort: ports[0], peerPort: ports[1]);
        a.Send("takeover");                                   // "lead now": a first node need not wait out the timeout either
        var first = await a.WaitForRoleAsync("Leader");
        Assert.Equal("manual", first.GetProperty("takeover").GetProperty("reason").GetString());
        using var b = new AppNode(env.Prefix, "b", steps, takeoverAfter, heartbeat, probePort: ports[1], peerPort: ports[0]);
        await a.WaitForStepAsync(120);
        await WaitForCaughtUpStandbyAsync(b, leader: "a", leaderTerm: 1);

        // Take over now: the leader is alive and is fenced under it.
        b.Send("takeover");
        var asked = Now;
        var second = await b.WaitForRoleAsync("Leader");
        var firstOutput = await b.WaitAsync("first-output");
        Assert.Equal(2, second.GetProperty("term").GetInt64());
        Assert.Equal("manual", second.GetProperty("takeover").GetProperty("reason").GetString());
        Assert.Equal(3, await a.ExitCodeAsync(TimeSpan.FromSeconds(30)));
        var fenced = await a.WaitForRoleAsync("Fenced");
        KafkaEnv.Measured(output,
            $"manual takeover between processes: command to leading {T(second) - asked} ms ({Describe(second)}), " +
            $"command to first output {T(firstOutput) - asked} ms; the old leader knew after {T(fenced) - asked} ms");
        Assert.InRange(T(second) - asked, 0, takeoverAfter - 1);

        // The old leader returns as a standby, and the new one steps down to it: its
        // circuits stop at a checkpoint, the resignation is written, and the standby does
        // not wait for any timeout.
        using var a2 = new AppNode(env.Prefix, "a", steps, takeoverAfter, heartbeat, probePort: ports[0], peerPort: ports[1]);
        await b.WaitForStepAsync((await b.WaitAsync("progress")).GetProperty("step").GetInt64() + 120);
        await WaitForCaughtUpStandbyAsync(a2, leader: "b", leaderTerm: 2);
        b.Send("stepdown");
        var steppedDown = Now;
        var resigned = await b.WaitAsync("resigned");
        Assert.Equal(0, await b.ExitCodeAsync());
        Assert.Equal("Resigned", b.Events("role")[^1].GetProperty("role").GetString());

        var third = await a2.WaitForRoleAsync("Leader");
        Assert.Equal(3, third.GetProperty("term").GetInt64());
        Assert.Equal("resigned", third.GetProperty("takeover").GetProperty("reason").GetString());
        KafkaEnv.Measured(output, $"step down between processes: command to the standby leading {T(third) - steppedDown} ms ({Describe(third)})");
        Assert.InRange(T(third) - steppedDown, 0, takeoverAfter / 2);

        // A step down loses no work: the successor resumes exactly where the leader stopped.
        Assert.Equal(
            resigned.GetProperty("committed").GetInt64() + 1,
            (await a2.WaitAsync("leading")).GetProperty("resumedAt").GetInt64());
        AssertDone(await a2.WaitAsync("done"), steps);
        a2.Send("quit");
        Assert.Equal(0, await a2.ExitCodeAsync());

        var verdict = Judge(env.Prefix, steps, plainMustMatch: false);
        KafkaEnv.Measured(output,
            $"manual takeover: {verdict.Stale.Count} sink record(s) from the deposed leader after its successor's first; {verdict.Redelivered} step(s) delivered under two terms");
        Assert.Equal([1L, 2L, 3L], verdict.RecordsByTerm.Keys.Order());
        Assert.All(verdict.Stale, record => Assert.Equal(("a", 1L), (record.Node, record.Term)));
        Assert.True(verdict.Stale.Select(s => s.Sequence).Distinct().Count() <= 1);
    }

    [Fact]
    public async Task Two_nodes_started_at_the_same_moment_end_with_exactly_one_leader()
    {
        await using var env = KafkaEnv.Create();
        const long steps = 240;
        const int takeoverAfter = 1500, heartbeat = 300;

        // No probe, the same timeout, the same instant: both decide to lead.
        using var a = new AppNode(env.Prefix, "a", steps, takeoverAfter, heartbeat);
        using var b = new AppNode(env.Prefix, "b", steps, takeoverAfter, heartbeat);
        var nodes = new[] { a, b };
        var waited = Stopwatch.StartNew();
        while (!nodes.Any(n => n.Events("done").Count > 0))
        {
            if (nodes.All(n => n.HasExited) || waited.Elapsed > Patience)
                Assert.Fail($"Neither node finished the work.{Environment.NewLine}{a.Dump()}{Environment.NewLine}{b.Dump()}");
            await Task.Delay(100);
        }
        await Task.Delay(takeoverAfter * 2);

        var winner = Assert.Single(nodes, n => n.Events("done").Count > 0);
        AssertDone(winner.Events("done")[0], steps);
        var loser = nodes.Single(n => n != winner);
        var status = await winner.StatusAsync();
        Assert.Equal("Leader", status.GetProperty("role").GetString());
        Assert.Equal(winner.Id, status.GetProperty("leader").GetString());

        // The other either never promoted (it saw the winner's term in time) or lost the
        // race for the transactional id and exited as fenced. It never finished the work.
        string outcome;
        if (loser.HasExited)
        {
            Assert.Equal(3, await loser.ExitCodeAsync());
            Assert.Equal("Fenced", loser.Events("role")[^1].GetProperty("role").GetString());
            outcome = "was fenced and exited";
        }
        else
        {
            var seen = await loser.StatusAsync();
            Assert.Equal("Standby", seen.GetProperty("role").GetString());
            Assert.Equal(winner.Id, seen.GetProperty("leader").GetString());
            outcome = "stayed a standby";
        }
        Assert.Empty(loser.Events("done"));
        var winnerTerm = status.GetProperty("term").GetInt64();
        KafkaEnv.Measured(output, $"simultaneous start of two processes: '{winner.Id}' leads under term {winnerTerm}; '{loser.Id}' {outcome}");

        // Restarted, the loser is the winner's standby.
        if (!loser.HasExited)
        {
            loser.Send("quit");
            Assert.Equal(0, await loser.ExitCodeAsync());
        }
        using var again = new AppNode(env.Prefix, loser.Id, steps, takeoverAfter, heartbeat);
        await WaitForCaughtUpStandbyAsync(again, leader: winner.Id, leaderTerm: winnerTerm);
        again.Send("quit");
        winner.Send("quit");
        Assert.Equal(0, await winner.ExitCodeAsync());

        var verdict = Judge(env.Prefix, steps, plainMustMatch: false);
        Assert.Equal(winnerTerm, verdict.RecordsByTerm.Keys.Max());
        Assert.True(verdict.Stale.Select(s => s.Sequence).Distinct().Count() <= 1);
    }
}
