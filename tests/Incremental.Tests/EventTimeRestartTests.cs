using Incremental.Storage;
using Incremental.Storage.Redis;
using Incremental.Tables;
using StackExchange.Redis;

namespace Incremental.Tests;

/// <summary>
/// Event-time state across a restart and a takeover. The watermark and every deadline an
/// event-time operator keeps (window closes, <c>EvictWhen</c>, session gaps, window-close
/// notifications) are checkpointed, so a circuit that is killed and restored must go on
/// exactly as one that never stopped: the same output changes step for step, late records
/// still late, and the same state at the end — nothing left behind for the keys that were
/// open when it died.
/// <para>
/// Each case runs one seeded workload twice: once uninterrupted, once killed at chosen
/// steps and restored — from the committed state of an
/// <see cref="InMemoryCheckpointStorageProvider"/>, from a <see cref="ChangelogStorage"/>
/// read cold, by promoting a follower that built its circuit and followed the bytes, or
/// from Redis by a provider that has read nothing yet (skipped when no Redis is reachable).
/// The harness plays the sources: after a restore it feeds again every step past the last
/// checkpoint. Every step's batch holds records below the watermark, so the first step
/// after each restore is one in which a late record arrives.
/// </para>
/// </summary>
public class EventTimeRestartTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Lateness = TimeSpan.FromSeconds(2);
    private const string PipelineId = "event-time";

    private sealed record Ev(int Id, int Key, long AtMs, int Amount)
    {
        public DateTimeOffset At => T0.AddMilliseconds(AtMs);
        public long Bucket => AtMs / 5000;
    }

    private sealed record Rate(int Id, string Tier);

    /// <summary>A key that carries its own deadline: the manual-windowing shape.</summary>
    private readonly record struct Slot(int Key, long Bucket)
    {
        public DateTimeOffset ClosesAt => T0.AddMilliseconds((Bucket + 1) * 5000);
    }

    // ── the scenarios ──────────────────────────────────────────────────────────────────

    private interface ITap
    {
        Func<List<string>> Bind(Circuit circuit);
    }

    private sealed class Tap<T>(string name, Output<T> output) : ITap where T : notnull
    {
        public Func<List<string>> Bind(Circuit circuit)
        {
            var reader = circuit.Handle(output).Reader;
            return () =>
            {
                var lines = new List<string>();
                while (reader.TryRead(out var delta))
                    foreach (var (value, weight) in delta)
                        lines.Add($"{name}|{value}|{weight}");
                return lines;
            };
        }
    }

    private sealed record Definition(CircuitBuilder Builder, InputStream<Ev> A, InputStream<Ev> B, List<ITap> Taps);

    private delegate void Scenario(CircuitBuilder builder, InputStream<Ev> a, InputStream<Ev> b, SharedTable<int, Rate> rates, List<ITap> taps);

    private static void Add<T>(List<ITap> taps, string name, Output<T> output) where T : notnull => taps.Add(new Tap<T>(name, output));

    private static void Tumbling(CircuitBuilder builder, InputStream<Ev> a, InputStream<Ev> b, SharedTable<int, Rate> rates, List<ITap> taps)
    {
        var windows = a.WithEventTime(e => e.At, Lateness).GroupBy(e => e.Key).TumblingWindow(TimeSpan.FromSeconds(5));
        Add(taps, "count", windows.Count().ToOutput(bootstrap: false));
        Add(taps, "sum", windows.Sum(e => (long)e.Amount).ToOutput(bootstrap: false));
        Add(taps, "distinct", windows.Aggregate(Aggregates.CountDistinct<Ev>()).ToOutput(bootstrap: false));
        Add(taps, "max", windows.Max(e => e.Amount).ToOutput(bootstrap: false));
        Add(taps, "final", windows.Count().OnClose().ToOutput(bootstrap: false));
        Add(taps, "snapshot", windows.Sum(e => (long)e.Amount).ToOutput());
    }

    private static void Hopping(CircuitBuilder builder, InputStream<Ev> a, InputStream<Ev> b, SharedTable<int, Rate> rates, List<ITap> taps)
    {
        var windows = a.WithEventTime(e => e.At, Lateness).GroupBy(e => e.Key)
            .HoppingWindow(TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(5));
        Add(taps, "count", windows.Count().ToOutput(bootstrap: false));
        Add(taps, "min", windows.Min(e => e.Amount).ToOutput(bootstrap: false));
        Add(taps, "final", windows.Sum(e => (long)e.Amount).OnClose().ToOutput(bootstrap: false));
    }

    private static void Session(CircuitBuilder builder, InputStream<Ev> a, InputStream<Ev> b, SharedTable<int, Rate> rates, List<ITap> taps)
    {
        var sessions = a.WithEventTime(e => e.At, Lateness).GroupBy(e => e.Key).SessionWindow(TimeSpan.FromSeconds(3));
        Add(taps, "count", sessions.Count().ToOutput(bootstrap: false));
        Add(taps, "final", sessions.Sum(e => (long)e.Amount).OnClose().ToOutput(bootstrap: false));
    }

    private static void WindowedTopK(CircuitBuilder builder, InputStream<Ev> a, InputStream<Ev> b, SharedTable<int, Rate> rates, List<ITap> taps)
    {
        // Ranked by (value, key): a tie on the value alone is broken by whichever row the
        // store happens to yield first, which no two runs need agree on.
        var sums = a.WithEventTime(e => e.At, Lateness).GroupBy(e => e.Key).TumblingWindow(TimeSpan.FromSeconds(5)).Sum(e => (long)e.Amount);
        Add(taps, "top", sums.TopK(2, row => (row.Value, row.Key)).ToOutput(bootstrap: false));
        Add(taps, "finalTop", sums.TopK(2, row => (row.Value, row.Key)).OnClose().ToOutput(bootstrap: false));
        Add(taps, "parity", sums.GroupBy(row => row.Key % 2).Count().ToOutput(bootstrap: false));
    }

    private static void EvictingJoins(CircuitBuilder builder, InputStream<Ev> a, InputStream<Ev> b, SharedTable<int, Rate> rates, List<ITap> taps)
    {
        // Two event-time sources: the watermark is the lower of the two.
        var left = a.WithEventTime(e => e.At, Lateness).GroupBy(e => new Slot(e.Key, e.Bucket)).EvictWhen(s => s.ClosesAt);
        var right = b.WithEventTime(e => e.At, Lateness).GroupBy(e => new Slot(e.Key, e.Bucket)).EvictWhen(s => s.ClosesAt);
        Add(taps, "join", left.Join(right, (slot, l, r) => (slot, l.Id, r.Id)).ToOutput());
        Add(taps, "left", left.LeftJoin(right, (slot, l, r) => (slot, l.Id, Right: r.Id), (slot, l) => (slot, l.Id, Right: -1)).ToOutput());
        Add(taps, "anti", left.AntiJoin(right).ToOutput(bootstrap: false));
        Add(taps, "distinct", left.Distinct().ToOutput(bootstrap: false));
        Add(taps, "top", left.TopK(1, e => (e.Amount, e.Id)).ToOutput(bootstrap: false));
        Add(taps, "closes", left.WindowCloses().ToOutput(bootstrap: false));
        Add(taps, "sum", left.Sum(e => (long)e.Amount).ToOutput(bootstrap: false));
    }

    private static void EvictingTableJoins(CircuitBuilder builder, InputStream<Ev> a, InputStream<Ev> b, SharedTable<int, Rate> rates, List<ITap> taps)
    {
        var table = builder.Table(rates);
        var facts = a.WithEventTime(e => e.At, Lateness);
        // The table's key is the event's key, so the deadline cannot ride in it: the
        // grouping closes a key a fixed time after the epoch, staggered by key.
        var grouped = facts.GroupBy(e => e.Key).EvictWhen(key => T0.AddSeconds(10 + 7 * key));
        Add(taps, "lookup", grouped.Lookup(table, (_, e, rate) => (e.Id, rate.Tier), (_, e) => (e.Id, "none")).ToOutput());
        Add(taps, "join", grouped.Join(table, (_, e, rate) => (e.Id, rate.Tier)).ToOutput());
    }

    private static readonly Dictionary<string, Scenario> Scenarios = new()
    {
        ["tumbling"] = Tumbling,
        ["hopping"] = Hopping,
        ["session"] = Session,
        ["topk"] = WindowedTopK,
        ["joins"] = EvictingJoins,
        ["tables"] = EvictingTableJoins,
    };

    private static Definition Define(string scenario, IStorageProvider storage, SharedTable<int, Rate> rates, int workers, int interval)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false,
            Workers = workers,
            Storage = storage,
            CheckpointInterval = interval,
        });
        var a = builder.Input<Ev>("a");
        var b = builder.Input<Ev>("b");
        var taps = new List<ITap>();
        Scenarios[scenario](builder, a, b, rates, taps);
        return new Definition(builder, a, b, taps);
    }

    // ── the workload ───────────────────────────────────────────────────────────────────

    private sealed record Batch(List<(Ev Event, int Weight)> A, List<(Ev Event, int Weight)> B);

    /// <summary>
    /// One batch per step. Event time advances about a second a step, with jitter inside
    /// the allowed lateness; every batch also carries a record five to twenty seconds
    /// behind (below the watermark: it must be dropped, whatever window it names) and now
    /// and then a retraction of something recent. The key space is fixed and time moves
    /// on, so state settles at a size: what is open at any step is the last few seconds.
    /// </summary>
    private static List<Batch> Workload(int seed, int steps, int keys = 6)
    {
        var random = new Random(seed);
        var batches = new List<Batch>();
        var recent = new List<(bool OnA, Ev Event)>();
        var id = 0;
        for (var step = 0; step < steps; step++)
        {
            var batch = new Batch([], []);
            var now = step * 1000L;
            for (var side = 0; side < 2; side++)
            {
                var into = side == 0 ? batch.A : batch.B;
                var count = random.Next(0, 5);
                for (var i = 0; i < count; i++)
                {
                    var ev = new Ev(++id, random.Next(keys), Math.Max(0, now - random.Next(0, 1800)), random.Next(1, 50));
                    into.Add((ev, 1));
                    recent.Add((side == 0, ev));
                }

                // Late: behind the watermark by more than the lateness, in a window that
                // has closed. Never before the watermark exists.
                if (step > 4)
                    into.Add((new Ev(++id, random.Next(keys), Math.Max(0, now - random.Next(5000, 20000)), 1000), 1));
            }

            if (recent.Count > 0 && random.Next(3) == 0)
            {
                var (onA, ev) = recent[random.Next(Math.Max(0, recent.Count - 6), recent.Count)];
                recent.Remove((onA, ev));
                (onA ? batch.A : batch.B).Add((ev, -1));
            }
            batches.Add(batch);
        }

        // The tail: both sources move far ahead so every window and key closes, then idle
        // steps for what the last freeze unlocked.
        var end = steps * 1000L + 60_000;
        batches.Add(new Batch([(new Ev(++id, 0, end, 1), 1)], [(new Ev(++id, 0, end, 1), 1)]));
        batches.Add(new Batch([], []));
        batches.Add(new Batch([], []));
        return batches;
    }

    // ── the harness ────────────────────────────────────────────────────────────────────

    public enum Backing { InMemory, ChangelogCold, ChangelogWarm, Redis }

    private static ConnectionMultiplexer ConnectOrSkip()
    {
        try
        {
            return ConnectionMultiplexer.Connect("localhost:6379,connectTimeout=1000,abortConnect=true,allowAdmin=true");
        }
        catch (RedisConnectionException)
        {
            throw Xunit.Sdk.SkipException.ForSkip("Redis is not reachable on localhost:6379 — run `docker compose up -d`.");
        }
    }

    /// <summary>A provider with a switch that fails every commit: how a circuit on it is
    /// killed. Disposing the circuit afterwards stops its threads and commits nothing.</summary>
    private sealed class Mortal(ICheckpointStorageProvider inner) : ICheckpointStorageProvider
    {
        public bool Dead;

        public IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull => inner.CreateWeightStore<T>(name);

        public IIndexedWeightStore<TKey, TValue> CreateIndexedWeightStore<TKey, TValue>(string name)
            where TKey : notnull
            where TValue : notnull => inner.CreateIndexedWeightStore<TKey, TValue>(name);

        public IValueStore<TKey, TValue> CreateValueStore<TKey, TValue>(string name) where TKey : notnull => inner.CreateValueStore<TKey, TValue>(name);
        public IDeadlineStore<TKey> CreateDeadlineStore<TKey>(string name) where TKey : notnull => inner.CreateDeadlineStore<TKey>(name);
        public IReadOnlyDictionary<string, long> Recover() => inner.Recover();

        public void Commit(IReadOnlyDictionary<string, long> inputOffsets)
        {
            if (Dead)
                throw new IOException("the process died here");
            inner.Commit(inputOffsets);
        }

        public string? CommittedManifest(string scope) => inner.CommittedManifest(scope);
        public void StageManifest(string scope, string manifest) => inner.StageManifest(scope, manifest);
    }

    /// <summary>One run of a scenario: the circuit of the moment, what backs it, and the
    /// standby when there is one.</summary>
    private sealed class Run(string scenario, Backing backing, int workers, int interval) : IAsyncDisposable
    {
        private readonly SharedTable<int, Rate> _rates = new("rates", r => r.Id);
        private readonly InMemoryCheckpointStorageProvider _memory = new();
        private readonly InMemoryStateChangelog _changelog = new(partitions: 2);

        private readonly ConnectionMultiplexer? _redis = backing == Backing.Redis ? ConnectOrSkip() : null;
        private readonly string _prefix = $"sp-test:{Guid.NewGuid():N}";

        private Mortal? _mortal;
        private ChangelogStorage? _storage;
        private Circuit _circuit = null!;
        private Definition _definition = null!;
        private List<Func<List<string>>> _drains = [];

        private ChangelogStorage? _standbyStorage;
        private Definition? _standbyDefinition;
        private Circuit? _standbyCircuit;

        /// <summary>Step → that step's output changes, as last produced.</summary>
        public readonly SortedDictionary<long, string> Outputs = [];
        public int Restores;
        public int ReplayedSteps;

        public async Task StartAsync()
        {
            for (var id = 0; id < 6; id += 2)
                await _rates.Writer.UpsertAsync(new Rate(id, id < 3 ? "low" : "high"));
            await _rates.Writer.CommitAsync().AsTask().WaitAsync(Timeout);

            if (backing is Backing.InMemory or Backing.Redis)
            {
                _mortal = new Mortal(Shared());
                Use(Define(scenario, _mortal, _rates, workers, interval));
            }
            else
            {
                _storage = await ChangelogStorage.OpenAsync(_changelog).WaitAsync(Timeout);
                Use(Define(scenario, _storage.Pipeline(PipelineId), _rates, workers, interval));
                StartStandby();
            }
            _circuit.Start();
        }

        /// <summary>The state a restore finds: the one in-memory provider, or a new Redis
        /// provider over the same keys — which holds nothing until it is asked.</summary>
        private ICheckpointStorageProvider Shared() =>
            backing == Backing.InMemory ? _memory : new RedisStorageProvider(_redis!, new RedisStorageOptions { KeyPrefix = _prefix });

        private void Use(Definition definition, Circuit? built = null)
        {
            _definition = definition;
            _circuit = built ?? definition.Builder.Build();
            _drains = definition.Taps.Select(tap => tap.Bind(_circuit)).ToList();
        }

        /// <summary>The warm standby: a follower with the circuit already built on it, so
        /// its stores are typed and every record it follows goes into them.</summary>
        private void StartStandby()
        {
            if (backing != Backing.ChangelogWarm)
                return;
            _standbyStorage = ChangelogStorage.Follow(_changelog);
            _standbyDefinition = Define(scenario, _standbyStorage.Pipeline(PipelineId), _rates, workers, interval);
            _standbyCircuit = _standbyDefinition.Builder.Build();
        }

        public long CommittedStep => _circuit.CommittedStep ?? 0;

        public async Task StepAsync(long step, Batch batch)
        {
            var a = _circuit.Handle(_definition.A);
            var b = _circuit.Handle(_definition.B);
            foreach (var (ev, weight) in batch.A)
                await a.SendAsync(ev, weight);
            foreach (var (ev, weight) in batch.B)
                await b.SendAsync(ev, weight);
            await _circuit.StepAsync().WaitAsync(Timeout);

            var lines = _drains.SelectMany(drain => drain()).ToList();
            lines.Sort(StringComparer.Ordinal);
            var changes = string.Join("\n", lines);

            // A step run again after a restore must say what it said the first time.
            if (Outputs.TryGetValue(step, out var before))
            {
                ReplayedSteps++;
                Assert.Equal(before, changes);
            }
            Outputs[step] = changes;
        }

        /// <summary>Kills the circuit where it stands and brings the next one up on what
        /// was committed. Returns the step that one resumes after.</summary>
        public async Task<long> KillAndRestoreAsync()
        {
            Restores++;
            var dead = _circuit;
            switch (backing)
            {
                case Backing.InMemory or Backing.Redis:
                    // Dead first, so the old circuit's shutdown — which runs one more step
                    // into the shared stores — commits nothing; the restore rolls it back.
                    _mortal!.Dead = true;
                    await dead.DisposeAsync().AsTask().WaitAsync(Timeout);
                    _mortal = new Mortal(Shared());
                    Use(Define(scenario, _mortal, _rates, workers, interval));
                    break;

                case Backing.ChangelogCold:
                {
                    // Opening the log again fences the old writer: its shutdown commits nothing.
                    var old = _storage!;
                    _storage = await ChangelogStorage.OpenAsync(_changelog).WaitAsync(Timeout);
                    await dead.DisposeAsync().AsTask().WaitAsync(Timeout);
                    await old.DisposeAsync().AsTask().WaitAsync(Timeout);
                    Use(Define(scenario, _storage.Pipeline(PipelineId), _rates, workers, interval));
                    break;
                }

                default:
                {
                    var old = _storage!;
                    await _standbyStorage!.PromoteAsync().WaitAsync(Timeout);
                    await dead.DisposeAsync().AsTask().WaitAsync(Timeout);
                    await old.DisposeAsync().AsTask().WaitAsync(Timeout);
                    _storage = _standbyStorage;
                    Use(_standbyDefinition!, _standbyCircuit);
                    StartStandby();
                    break;
                }
            }
            _circuit.Start();
            return CommittedStep;
        }

        /// <summary>Everything the run holds, as text — or, for the in-memory provider,
        /// which has no way to say it, the number of entries.</summary>
        public List<string> DescribeState() => backing switch
        {
            Backing.InMemory => [$"entries={_memory.CommittedEntries}"],
            Backing.Redis => DescribeRedis(),
            _ => _storage!.Pipeline(PipelineId).DescribeState(),
        };

        /// <summary>Every key under the prefix but the checkpoint and the manifest, with
        /// what it holds: hashes field by field, the deadline order member by member.</summary>
        private List<string> DescribeRedis()
        {
            var db = _redis!.GetDatabase();
            var lines = new List<string>();
            foreach (var key in _redis.GetServers()[0].Keys(pattern: _prefix + ":*"))
            {
                var name = key.ToString()[(_prefix.Length + 1)..];
                if (name is "checkpoint" or "manifest")
                    continue;
                if (db.KeyType(key) == RedisType.SortedSet)
                    lines.AddRange(db.SortedSetRangeByRankWithScores(key).Select(entry => $"{name}|{entry.Element}~{entry.Score}"));
                else
                    lines.AddRange(db.HashGetAll(key).Select(entry => $"{name}|{entry.Name}={entry.Value}"));
            }
            lines.Sort(StringComparer.Ordinal);
            return lines;
        }

        public IReadOnlyDictionary<string, long> CommittedOffsets() => backing switch
        {
            Backing.InMemory => _memory.CommittedOffsets,
            Backing.Redis => Shared().Recover(),
            _ => _storage!.Pipeline(PipelineId).CommittedOffsets,
        };

        public DateTimeOffset? Watermark => _circuit.CurrentWatermark;

        /// <summary>What a process that reads the log from nothing ends up holding.</summary>
        public async Task<List<string>> DescribeColdCopyAsync()
        {
            await using var copy = ChangelogStorage.Follow(_changelog);
            await copy.WhenCaughtUpAsync().WaitAsync(Timeout);
            return copy.Pipeline(PipelineId).DescribeState();
        }

        public Backing Backing => backing;

        /// <summary>A clean shutdown: commits whatever steps are still uncommitted.</summary>
        public async Task StopAsync() => await _circuit.DisposeAsync().AsTask().WaitAsync(Timeout);

        public async ValueTask DisposeAsync()
        {
            await _circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
            if (_standbyCircuit is not null)
                await _standbyCircuit.DisposeAsync().AsTask().WaitAsync(Timeout);
            if (_standbyStorage is not null)
                await _standbyStorage.DisposeAsync().AsTask().WaitAsync(Timeout);
            if (_storage is not null)
                await _storage.DisposeAsync().AsTask().WaitAsync(Timeout);
            if (_redis is not null)
            {
                foreach (var key in _redis.GetServers()[0].Keys(pattern: _prefix + ":*").ToArray())
                    _redis.GetDatabase().KeyDelete(key);
                _redis.Dispose();
            }
        }
    }

    private static async Task<Run> RunAsync(
        string scenario, Backing backing, int workers, int interval, List<Batch> batches, Func<long, bool> killAfter,
        Action<long, Run>? afterStep = null)
    {
        var run = new Run(scenario, backing, workers, interval);
        await run.StartAsync();
        for (var step = 1L; step <= batches.Count; step++)
        {
            await run.StepAsync(step, batches[(int)step - 1]);
            afterStep?.Invoke(step, run);
            if (!killAfter(step))
                continue;

            // The sources' part of a restore: everything past the checkpoint, again, cut
            // into the steps it was first run as.
            var committed = await run.KillAndRestoreAsync();
            Assert.InRange(committed, step - interval + 1, step);
            for (var replay = committed + 1; replay <= step; replay++)
                await run.StepAsync(replay, batches[(int)replay - 1]);
        }
        await run.StopAsync();
        return run;
    }

    private static async Task AssertSameAsUninterruptedAsync(
        string scenario, Backing backing, int workers, int interval, int seed, int steps, int kills)
    {
        var batches = Workload(seed, steps);
        var random = new Random(seed * 31 + 7);
        var killAt = new HashSet<long>();
        while (killAt.Count < kills)
            killAt.Add(random.Next(1, batches.Count));

        await using var expected = await RunAsync(scenario, backing, workers, interval, batches, _ => false);
        await using var actual = await RunAsync(scenario, backing, workers, interval, batches, killAt.Contains);

        Assert.Equal(kills, actual.Restores);
        Assert.Contains(expected.Outputs.Values, changes => changes.Length > 0);
        foreach (var (step, changes) in expected.Outputs)
            Assert.True(changes == actual.Outputs[step],
                $"{scenario}/{backing}/{workers}w/interval {interval}/seed {seed}: step {step} differs after kills at " +
                $"[{string.Join(",", killAt.Order())}].\nuninterrupted:\n{changes}\nrestored:\n{actual.Outputs[step]}");

        // The same state at the end — its content where the provider can say it, its size
        // where it cannot — and the same clock and positions beside it.
        Assert.Equal(expected.DescribeState(), actual.DescribeState());
        Assert.Equal(
            expected.CommittedOffsets().OrderBy(o => o.Key, StringComparer.Ordinal),
            actual.CommittedOffsets().OrderBy(o => o.Key, StringComparer.Ordinal));
        Assert.Contains("$watermark", actual.CommittedOffsets().Keys);
        if (backing is Backing.ChangelogCold or Backing.ChangelogWarm)
            Assert.Equal(expected.DescribeState(), await actual.DescribeColdCopyAsync());
    }

    public static TheoryData<string, Backing, int, int> Cases()
    {
        var cases = new TheoryData<string, Backing, int, int>();
        foreach (var scenario in Scenarios.Keys)
            foreach (var backing in Enum.GetValues<Backing>())
                foreach (var workers in new[] { 1, 4 })
                    cases.Add(scenario, backing, workers, workers == 1 ? 3 : 1);
        return cases;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Killed_and_restored_at_random_steps_it_goes_on_as_if_it_had_not_been(
        string scenario, Backing backing, int workers, int interval)
    {
        for (var seed = 1; seed <= 4; seed++)
            await AssertSameAsUninterruptedAsync(scenario, backing, workers, interval, seed, steps: 40, kills: 5);

        // And with the other commit cadence, once.
        await AssertSameAsUninterruptedAsync(scenario, backing, workers, interval == 1 ? 4 : 1, seed: 9, steps: 40, kills: 5);
    }

    [Theory]
    [InlineData("tumbling", Backing.InMemory)]
    [InlineData("tumbling", Backing.ChangelogWarm)]
    [InlineData("session", Backing.ChangelogCold)]
    [InlineData("session", Backing.InMemory)]
    [InlineData("joins", Backing.ChangelogWarm)]
    [InlineData("joins", Backing.InMemory)]
    [InlineData("topk", Backing.ChangelogCold)]
    [InlineData("tables", Backing.ChangelogWarm)]
    [InlineData("joins", Backing.Redis)]
    [InlineData("session", Backing.Redis)]
    public async Task State_does_not_grow_across_fifty_restarts(string scenario, Backing backing)
    {
        // A restore every third step of 150, with commits every other step: half the
        // restores lose a step and run it again.
        var batches = Workload(seed: 77, steps: 150);
        var sizes = new List<int>();
        void Measure(long step, Run run)
        {
            if (step % 25 == 0 && run.CommittedStep == step)
                sizes.Add(run.Backing == Backing.InMemory ? int.Parse(run.DescribeState()[0]["entries=".Length..]) : run.DescribeState().Count);
        }

        await using var expected = await RunAsync(scenario, backing, workers: 2, interval: 2, batches, _ => false);
        await using var actual = await RunAsync(scenario, backing, workers: 2, interval: 2, batches, step => step % 3 == 0 && step <= 150, Measure);

        Assert.Equal(50, actual.Restores);
        Assert.True(actual.ReplayedSteps >= 20, $"only {actual.ReplayedSteps} steps were run again");
        Assert.Equal(expected.Outputs, actual.Outputs);
        Assert.Equal(expected.DescribeState(), actual.DescribeState());

        // Settled: the workload slides, so what is held late in the run is what was held
        // early in it, give or take the windows open at that moment.
        Assert.True(sizes.Count >= 3, $"measured {sizes.Count} times");
        Assert.True(sizes[^1] <= sizes[1] * 2 + 20, $"state grew: {string.Join(", ", sizes)} entries at every 25th step");
    }

    [Theory]
    [InlineData(Backing.InMemory)]
    [InlineData(Backing.ChangelogCold)]
    [InlineData(Backing.ChangelogWarm)]
    [InlineData(Backing.Redis)]
    public async Task A_late_record_right_after_a_restore_does_not_reopen_a_closed_window(Backing backing)
    {
        // Three events in the window [0 s, 5 s) of key 1, then the clock moves to 30 s: the
        // window has closed, its count was delivered as final, its state is gone.
        var batches = new List<Batch>
        {
            new([(new Ev(1, 1, 1000, 5), 1), (new Ev(2, 1, 2000, 5), 1), (new Ev(3, 1, 3000, 5), 1)], []),
            new([(new Ev(4, 2, 30_000, 5), 1)], []),
            new([], []),
        };
        await using var run = new Run("tumbling", backing, workers: 2, interval: 1);
        await run.StartAsync();
        for (var step = 1; step <= 3; step++)
            await run.StepAsync(step, batches[step - 1]);
        Assert.Contains(run.Outputs[3].Split('\n'), line => line.StartsWith("final|") && line.EndsWith("Key = 1, Value = 3 }|1"));
        var watermark = run.Watermark;
        Assert.Equal(T0.AddSeconds(28), watermark);
        var settled = run.DescribeState();

        Assert.Equal(3, await run.KillAndRestoreAsync());
        Assert.Equal(watermark, run.Watermark);

        // The first record the restored circuit sees belongs to the closed window. A
        // circuit whose watermark started again from nothing would count it: a fresh row
        // "window [0 s, 5 s), key 1, count 1" over the delivered "count 3".
        await run.StepAsync(4, new Batch([(new Ev(5, 1, 4000, 5), 1)], []));
        await run.StepAsync(5, new Batch([], []));
        Assert.Equal("", run.Outputs[4]);
        Assert.Equal("", run.Outputs[5]);
        Assert.Equal(settled, run.DescribeState());
    }

    [Theory]
    [InlineData(Backing.InMemory)]
    [InlineData(Backing.ChangelogCold)]
    [InlineData(Backing.ChangelogWarm)]
    [InlineData(Backing.Redis)]
    public async Task Keys_open_at_a_restore_and_never_touched_again_are_still_evicted(Backing backing)
    {
        // Rows on both sides of a join under keys that close at 5 s and 10 s; then the
        // restore; then nothing but other keys' records, far ahead.
        await using var run = new Run("joins", backing, workers: 2, interval: 1);
        await run.StartAsync();
        await run.StepAsync(1, new Batch(
            [(new Ev(1, 1, 1000, 5), 1), (new Ev(2, 2, 6000, 5), 1)],
            [(new Ev(3, 1, 1500, 5), 1), (new Ev(4, 2, 6500, 5), 1)]));
        var open = run.DescribeState();
        Assert.True(backing == Backing.InMemory || open.Count(line => line.Contains(".deadlines.")) >= 2, string.Join("\n", open));

        await run.KillAndRestoreAsync();
        await run.StepAsync(2, new Batch([(new Ev(5, 3, 90_000, 5), 1)], [(new Ev(6, 3, 90_000, 5), 1)]));
        await run.StepAsync(3, new Batch([], []));

        // Only key 3's slot is left, on every operator; the two that were open are gone,
        // and their window-close notifications came out.
        Assert.Contains("closes|KeyedValue { Key = Slot { Key = 1, Bucket = 0,", run.Outputs[3]);
        Assert.Contains("closes|KeyedValue { Key = Slot { Key = 2, Bucket = 1,", run.Outputs[3]);
        var left = run.DescribeState();
        if (backing == Backing.InMemory)
            Assert.True(int.Parse(left[0]["entries=".Length..]) < int.Parse(open[0]["entries=".Length..]), $"{open[0]} then {left[0]}");
        else
            Assert.DoesNotContain(left, line => line.Contains("\"Key\":1,") || line.Contains("\"Key\":2,"));
    }

    [Fact]
    public async Task A_session_key_that_is_taken_back_whole_keeps_no_session_deadline()
    {
        // The windowed operators downstream hold the session's window until it closes;
        // the session operator itself has nothing left to close for the key.
        await using var run = new Run("session", Backing.ChangelogCold, workers: 1, interval: 1);
        await run.StartAsync();
        var only = new Ev(1, 1, 1000, 5);
        await run.StepAsync(1, new Batch([(only, 1)], []));
        var held = run.DescribeState().Count(line => line.Contains(".deadlines."));
        await run.StepAsync(2, new Batch([(only, -1)], []));
        Assert.Equal(held - 1, run.DescribeState().Count(line => line.Contains(".deadlines.")));
    }

    [Fact]
    public async Task A_checkpoint_from_before_event_time_state_was_kept_is_refused_only_where_it_matters()
    {
        // Format 6 held no watermark and no deadlines. A circuit without event time lost
        // nothing by that and resumes; one with event time must not.
        var plain = new InMemoryCheckpointStorageProvider();
        await using (var circuit = Counting(plain, eventTime: false).Build())
        {
            circuit.Start();
            await circuit.StepAsync();
        }
        Rewrite(plain, manifest => manifest.Replace("format=7", "format=6"));
        await using (var resumed = Counting(plain, eventTime: false).Build())
        {
            resumed.Start();
            Assert.Contains("format=6", plain.CommittedManifest("local"));
            await resumed.StepAsync();
        }
        Assert.Contains("format=7", plain.CommittedManifest("local"));

        var timed = new InMemoryCheckpointStorageProvider();
        await using (var circuit = Counting(timed, eventTime: true).Build())
        {
            circuit.Start();
            await circuit.StepAsync();
        }
        Assert.Contains("eventtime=1", timed.CommittedManifest("local"));
        Rewrite(timed, manifest => manifest.Replace("format=7", "format=6").Replace("eventtime=1\n", ""));
        await using var refused = Counting(timed, eventTime: true).Build();
        var mismatch = Assert.Throws<CircuitStateMismatchException>(refused.Start);
        Assert.Contains("before event-time state was checkpointed", mismatch.Message);
        Assert.Contains("empty state store", mismatch.Message);
    }

    [Fact]
    public async Task A_checkpoint_is_refused_when_the_number_of_event_time_sources_has_changed()
    {
        // The committed max event times belong to the sources by position. With another
        // number of sources they would be handed to the wrong ones.
        var storage = new InMemoryCheckpointStorageProvider();
        await using (var circuit = TwoSources(storage, both: false).Build())
        {
            circuit.Start();
            await circuit.StepAsync();
        }
        Assert.Contains("eventtime=1", storage.CommittedManifest("local"));

        await using var refused = TwoSources(storage, both: true).Build();
        var mismatch = Assert.Throws<CircuitStateMismatchException>(refused.Start);
        Assert.Contains("the number of event-time sources changed", mismatch.Message);

        await using var same = TwoSources(storage, both: false).Build();
        same.Start();
    }

    private static CircuitBuilder TwoSources(IStorageProvider storage, bool both)
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        var a = builder.Input<Ev>("a");
        var b = builder.Input<Ev>("b");
        a.WithEventTime(e => e.At, Lateness).GroupBy(e => e.Key).TumblingWindow(TimeSpan.FromSeconds(5)).Count().Named("counts").ToOutput(bootstrap: false);
        if (both)
            b.WithEventTime(e => e.At, Lateness).Select(e => e.Id).ToOutput();
        else
            b.Select(e => e.Id).ToOutput();
        return builder;
    }

    /// <summary>Commits the checkpoint again with its manifest changed: how a checkpoint
    /// of an earlier format is made.</summary>
    private static void Rewrite(InMemoryCheckpointStorageProvider storage, Func<string, string> change)
    {
        var offsets = storage.CommittedOffsets;
        storage.StageManifest("local", change(storage.CommittedManifest("local")!));
        storage.Commit(offsets);
    }

    private static CircuitBuilder Counting(IStorageProvider storage, bool eventTime)
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        var events = builder.Input<Ev>("a");
        if (eventTime)
            events.WithEventTime(e => e.At, Lateness).GroupBy(e => e.Key).TumblingWindow(TimeSpan.FromSeconds(5)).Count().ToOutput(bootstrap: false);
        else
            events.GroupBy(e => e.Key).Count().ToOutput();
        return builder;
    }

    [Fact]
    public void A_checkpointing_provider_that_keeps_no_deadlines_is_refused_at_build()
    {
        var builder = Counting(new NoDeadlines(new InMemoryCheckpointStorageProvider()), eventTime: true);
        var refused = Assert.Throws<NotSupportedException>(() => builder.Build());
        Assert.Contains("CreateDeadlineStore", refused.Message);
    }

    private sealed class NoDeadlines(InMemoryCheckpointStorageProvider inner) : ICheckpointStorageProvider
    {
        public IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull => inner.CreateWeightStore<T>(name);

        public IIndexedWeightStore<TKey, TValue> CreateIndexedWeightStore<TKey, TValue>(string name)
            where TKey : notnull
            where TValue : notnull => inner.CreateIndexedWeightStore<TKey, TValue>(name);

        public IValueStore<TKey, TValue> CreateValueStore<TKey, TValue>(string name) where TKey : notnull => inner.CreateValueStore<TKey, TValue>(name);
        public IReadOnlyDictionary<string, long> Recover() => inner.Recover();
        public void Commit(IReadOnlyDictionary<string, long> inputOffsets) => inner.Commit(inputOffsets);
        public string? CommittedManifest(string scope) => inner.CommittedManifest(scope);
        public void StageManifest(string scope, string manifest) => inner.StageManifest(scope, manifest);
    }
}
