using Acme.Contracts;
using Dbsp.Kafka;
using Dbsp.Storage;
using static Dbsp.Service.Tests.SupervisorTests;
using static Dbsp.Service.Tests.TestService;

namespace Dbsp.Service.Tests;

/// <summary>
/// The in-memory backend's fencing reaches the sinks. A leader that is replaced is told at
/// once, but its node tears its circuits down a moment later, on a thread of its own; in
/// that moment a circuit of the old leader's can still read an input, join it against a
/// table that no longer follows, and deliver. Its row then lands over the new leader's
/// (found on a slow runner: <c>WarmStandbyTests</c> timed out with four rows of the old
/// table in the sink). So a leased sink begins no write once its node does not lead, and a
/// takeover lets the write that was in flight land before it prepares anything.
/// </summary>
public class InMemoryFencingTests
{
    private static bool Same<TKey, TRow>(IReadOnlyDictionary<TKey, TRow> actual, IReadOnlyDictionary<TKey, TRow> expected) where TKey : notnull =>
        actual.Count == expected.Count && expected.All(e => actual.TryGetValue(e.Key, out var row) && Equals(row, e.Value));

    private static LeadershipCallbacks Callbacks(Action? prepared = null) => new(
        _ =>
        {
            prepared?.Invoke();
            return Task.CompletedTask;
        },
        (_, _) => Task.CompletedTask, _ => Task.CompletedTask, () => throw new NotSupportedException());

    /// <summary>A gate one caller can be stopped at: the first to pass after
    /// <see cref="Next"/> waits until <see cref="LetGo"/>.</summary>
    private sealed class Hold
    {
        private TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _armed;

        public Task Entered => _entered.Task;

        public void Next()
        {
            _entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _released = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Volatile.Write(ref _armed, 1);
        }

        public void LetGo() => _released.TrySetResult();

        public async ValueTask PassAsync()
        {
            if (Interlocked.Exchange(ref _armed, 0) == 0)
                return;
            var released = _released.Task;
            _entered.TrySetResult();
            await released;
        }
    }

    /// <summary>A sink that counts what reaches it and can be told to hold the next write
    /// inside itself until it is let go.</summary>
    private sealed class HeldSink : IUpsertSink<int, string>, IAppendSink<string>
    {
        private readonly Hold _hold = new();
        private int _written;

        public int Written => Volatile.Read(ref _written);
        public Task Entered => _hold.Entered;

        public void HoldNext() => _hold.Next();

        public void LetGo() => _hold.LetGo();

        private async ValueTask WriteAsync()
        {
            await _hold.PassAsync();
            Interlocked.Increment(ref _written);
        }

        public ValueTask WriteAsync(UpsertBatch<int, string> batch, CancellationToken cancellationToken) => WriteAsync();

        public ValueTask WriteAsync(AppendBatch<string> batch, CancellationToken cancellationToken) => WriteAsync();
    }

    [Fact]
    public async Task A_deposed_leader_begins_no_sink_write_and_the_takeover_waits_for_the_one_in_flight()
    {
        // With a bound nobody reaches: what ends the takeover's wait here is the write.
        var cluster = new InMemoryServiceCluster(new InMemoryBackendOptions { DeposedWriteTimeout = TimeSpan.FromMinutes(10) });
        var a = cluster.Node("a");
        var b = cluster.Node("b");
        var prepared = 0;
        await a.Leadership.StartAsync(Callbacks(), default);
        await b.Leadership.StartAsync(Callbacks(() => Interlocked.Increment(ref prepared)), default);

        var sink = new HeldSink();
        var upserts = a.Leadership.Leased<int, string>(sink);
        var appends = a.Leadership.Leased<string>(sink);
        await upserts.WriteAsync(default, default);
        await appends.WriteAsync(default, default);
        Assert.Equal(2, sink.Written);

        // A standby delivers nothing: it has never led.
        await Assert.ThrowsAsync<ChangelogFencedException>(() => b.Leadership.Leased<int, string>(sink).WriteAsync(default, default).AsTask());
        Assert.Equal(2, sink.Written);

        // The leader is inside a write when the standby takes over.
        sink.HoldNext();
        var inFlight = upserts.WriteAsync(default, default).AsTask();
        await sink.Entered.WaitAsync(TestService.Timeout);
        var takeover = b.Leadership.TakeOverAsync(default);
        await Eventually(() => a.Leadership.Status.Role == NodeRole.Fenced, "the old leader to be fenced");

        // Fenced, the old leader begins nothing more — while the one it had begun is
        // still on its way, too.
        await Assert.ThrowsAsync<ChangelogFencedException>(() => upserts.WriteAsync(default, default).AsTask());
        await Assert.ThrowsAsync<ChangelogFencedException>(() => appends.WriteAsync(default, default).AsTask());

        // And the takeover has prepared nothing: whatever the new leader delivers comes
        // after that write, not before it.
        await Task.Delay(150);
        Assert.Equal(0, Volatile.Read(ref prepared));
        Assert.False(takeover.IsCompleted, "the takeover went on while the deposed leader's write was in flight");
        Assert.Equal(2, sink.Written);

        sink.LetGo();
        await inFlight.WaitAsync(TestService.Timeout);
        await takeover.WaitAsync(TestService.Timeout);
        Assert.Equal(1, Volatile.Read(ref prepared));
        Assert.Equal(3, sink.Written);
        Assert.Equal(NodeRole.Leader, b.Leadership.Status.Role);

        // The new leader delivers; the old one still does not.
        await b.Leadership.Leased<int, string>(sink).WriteAsync(default, default);
        await b.Leadership.Leased<string>(sink).WriteAsync(default, default);
        Assert.Equal(5, sink.Written);
        await Assert.ThrowsAsync<ChangelogFencedException>(() => upserts.WriteAsync(default, default).AsTask());
        Assert.Equal(5, sink.Written);
    }

    [Fact]
    public async Task A_takeover_does_not_wait_past_its_bound_for_a_write_that_never_ends()
    {
        var cluster = new InMemoryServiceCluster(new InMemoryBackendOptions { DeposedWriteTimeout = TimeSpan.FromMilliseconds(200) });
        var a = cluster.Node("a");
        var b = cluster.Node("b");
        await a.Leadership.StartAsync(Callbacks(), default);
        await b.Leadership.StartAsync(Callbacks(), default);

        var sink = new HeldSink();
        sink.HoldNext();
        var stuck = a.Leadership.Leased<int, string>(sink).WriteAsync(default, default).AsTask();
        await sink.Entered.WaitAsync(TestService.Timeout);

        var clock = System.Diagnostics.Stopwatch.StartNew();
        await b.Leadership.TakeOverAsync(default).WaitAsync(TestService.Timeout);
        Assert.True(clock.Elapsed >= TimeSpan.FromMilliseconds(150), $"the takeover waited {clock.ElapsedMilliseconds} ms for the write in flight; its bound is 200 ms");
        Assert.Equal(NodeRole.Leader, b.Leadership.Status.Role);
        Assert.False(stuck.IsCompleted);

        // A resignation needs no wait at all: the leader stops its own circuits first.
        sink.LetGo();
        await stuck.WaitAsync(TestService.Timeout);
        var c = cluster.Node("c");
        await c.Leadership.StartAsync(Callbacks(), default);
        await b.Leadership.StepDownAsync(default);
        await Eventually(() => c.Leadership.Status.Role == NodeRole.Leader, "the third node to lead after the resignation");
        await c.Leadership.Leased<int, string>(sink).WriteAsync(default, default);
        await Assert.ThrowsAsync<ChangelogFencedException>(() => b.Leadership.Leased<int, string>(sink).WriteAsync(default, default).AsTask());
    }

    /// <summary>The memory connector's sink behind a gate: the first write after
    /// <see cref="HoldNext"/> waits inside the connector until it is let go.</summary>
    private sealed class HeldConnector(MemoryConnector memory) : IUpsertSinkConnector
    {
        private readonly Hold _hold = new();

        public Task Entered => _hold.Entered;

        public void HoldNext() => _hold.Next();

        public void LetGo() => _hold.LetGo();

        public ConnectorDescription Describe(System.Text.Json.JsonElement config) => new("held memory");

        public IUpsertSink<TKey, TRow> Create<TKey, TRow>(SinkContext context) where TKey : notnull =>
            new Held<TKey, TRow>(this, memory.UpsertSink<TKey, TRow>(context.Config.GetProperty("name").GetString()!));

        private sealed class Held<TKey, TRow>(HeldConnector owner, IUpsertSink<TKey, TRow> inner) : IUpsertSink<TKey, TRow> where TKey : notnull
        {
            public async ValueTask WriteAsync(UpsertBatch<TKey, TRow> batch, CancellationToken cancellationToken)
            {
                await owner._hold.PassAsync();
                await inner.WriteAsync(batch, cancellationToken);
            }
        }
    }

    [Fact]
    public async Task What_a_deposed_leader_was_delivering_reaches_the_sink_before_the_new_leaders_rows_and_not_over_them()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        var held = new HeldConnector(memory);
        Action<DbspServiceBuilder> connector = builder => builder.AddSinkConnector("held", _ => held);
        var spec = Spec("e", "enrich", "in") with
        {
            Sinks = new Dictionary<string, System.Text.Json.JsonElement> { ["out"] = Json("""{ "connector": "held", "name": "out" }""") },
        };

        await using var a = await StartAsync(cluster, "a", memory, configure: connector);
        var instruments = Instruments(5);
        var table = await a.DeclareInstrumentsAsync(instruments);
        await a.DeployAsync(spec);
        var trades = Trades(0, 20, instruments: 5);
        memory.Log<Trade>("in").AppendRange(trades);
        var sink = memory.UpsertSink<long, EnrichedTrade>("out");
        await Eventually(() => Same(sink.Rows, Enrich.Model(trades, instruments)), "the leader's output");
        await Eventually(() => a.Pipelines.Get("e").Sources.Single().Stats.Lag == 0 && a.Pipelines.Get("e").CommittedOffsets.GetValueOrDefault("trades#p0", -1) == 19,
            "the leader to have committed what it delivered");

        await using var b = await StartAsync(cluster, "b", memory, configure: connector);
        await Eventually(() => b.Node.Status.Standby is { TakeoverWouldBeWarm: true, StateLagRecords: 0 }, "the standby to be warm");

        // The leader joins one more trade to the instrument as it is and is held inside
        // the sink with the row. Then the instrument changes — a change the held circuit
        // cannot take a step for, and the standby's table follows.
        var late = new Trade(100, 3, 2, "trader0");
        held.HoldNext();
        memory.Log<Trade>("in").Append(late);
        await held.Entered.WaitAsync(TestService.Timeout);
        instruments[2] = instruments[2] with { Symbol = "RENAMED", Multiplier = 7 };
        await table.Writer.UpsertAsync(instruments[2]);
        await table.Writer.CommitAsync();
        await Eventually(() => b.Tables.Get<int, Instrument>("instruments").LatestVersion == table.LatestVersion, "the standby's table to follow the change");

        // The takeover waits for that write. Were it not to, the new leader would run
        // the trade again against the renamed instrument and deliver — and the old
        // leader's row, let go afterwards, would be the one the sink keeps.
        var model = Enrich.Model([.. trades, late], instruments);
        var takeover = b.Node.TakeOverAsync(new NodeActionOptions(), default);
        var wentOn = await Task.WhenAny(takeover, Task.Delay(300)) == takeover;
        if (wentOn)
            await Eventually(() => sink.Rows.TryGetValue(100, out var row) && row.Symbol == "RENAMED", "the new leader's row for the trade");
        held.LetGo();
        await takeover.WaitAsync(TestService.Timeout);
        await b.Running("e");
        await Eventually(
            () => b.Pipelines.Get("e").Sources.Single().Stats.Lag == 0 && b.Pipelines.Get("e").CommittedOffsets.GetValueOrDefault("trades#p0", -1) == 20,
            "the new leader to have committed the trade");
        await Eventually(() => a.Pipelines.Get("e").State == PipelineState.Stopped, "the old leader's circuit to have ended");
        Assert.False(wentOn, "the takeover went on while the deposed leader's write was in flight");
        Assert.True(Same(sink.Rows, model), $"the sink holds {sink.Rows[100]} for the trade; the new leader's row is {model[100]}");
    }
}
