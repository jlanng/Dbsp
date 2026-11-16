using Incremental.Storage;

namespace Incremental.Tests;

/// <summary>
/// Rolling upgrades across a change of a persisted format (<see cref="PersistedFormat"/>),
/// on the state log. A pair of storages of two format levels — the lower playing the
/// build from before the change — goes through what an upgrade is: the standby replaced
/// by the newer build, a takeover, the old leader back as it was, then replaced too, the
/// level raised, a takeover back. One workload runs through all of it and its outputs
/// are compared with a run nobody interrupted.
/// <para>
/// Two pairs. Level 1 → 2 is the real change this mechanism first carried: the state
/// manifest's format 7 → 8 and the per-partition event times in the checkpoint's marker.
/// Level 2 → the proving level is a change of the format byte of every value in the log,
/// which a node below the level refuses outright.
/// </para>
/// </summary>
public class FormatLevelTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Lateness = TimeSpan.FromSeconds(2);
    private const string PipelineId = "upgraded";
    private const int Partitions = 3;

    private sealed record Ev(int Id, int Key, long AtMs, int Amount)
    {
        public DateTimeOffset At => T0.AddMilliseconds(AtMs);
    }

    /// <summary>One batch per step: event time moves a second a step with jitter inside
    /// the lateness, so nothing is ever late, whichever node reads it and wherever its
    /// watermark stands. The last batch moves every partition far ahead.</summary>
    private static List<List<Ev>> Workload(int seed, int steps)
    {
        var random = new Random(seed);
        var id = 0;
        var batches = new List<List<Ev>>();
        for (var step = 1; step <= steps; step++)
        {
            var batch = new List<Ev>();
            for (var i = random.Next(2, 7); i > 0; i--)
                batch.Add(new Ev(++id, random.Next(9), step * 1000L + random.Next(-600, 600), random.Next(1, 50)));
            batches.Add(batch);
        }
        batches.Add([.. Enumerable.Range(0, Partitions).Select(key => new Ev(++id, key, steps * 1000L + 60_000, 1))]);
        batches.Add([]);
        batches.Add([]);
        return batches;
    }

    /// <summary>A node: its formats, its storage on the shared log, and the circuit built
    /// on it — before it leads, as a warm standby's is.</summary>
    private sealed class Node : IAsyncDisposable
    {
        private readonly Circuit _circuit;
        private readonly InputStream<Ev> _input;
        private readonly List<Func<List<string>>> _drains = [];
        private bool _started;

        public PersistedFormat Formats { get; }
        public ChangelogStorage Storage { get; }

        public Node(InMemoryStateChangelog log, PersistedFormat formats)
        {
            Formats = formats;
            Storage = ChangelogStorage.Follow(log, new ChangelogStorageOptions { Formats = formats });
            var builder = new CircuitBuilder(new CircuitOptions
            {
                AutoStep = false,
                Workers = 2,
                Storage = Storage.Pipeline(PipelineId),
                CheckpointInterval = 3,
            });
            _input = builder.Input<Ev>("a");

            // Event time on the input itself, so the clock keeps a time per partition:
            // the state format level 2 added to the checkpoint.
            var timed = _input.WithEventTime(e => e.At, Lateness);
            var windows = timed.GroupBy(e => e.Key).TumblingWindow(TimeSpan.FromSeconds(5));
            var outputs = new (string Name, Func<Circuit, Func<List<string>>> Bind)[]
            {
                Tap("open", windows.Sum(e => (long)e.Amount).ToOutput(bootstrap: false)),
                Tap("closed", windows.Count().OnClose().ToOutput(bootstrap: false)),
                Tap("total", _input.GroupBy(e => e.Key % 4).Sum(e => (long)e.Amount).ToOutput(bootstrap: false)),
            };
            _circuit = builder.Build();
            foreach (var (_, bind) in outputs)
                _drains.Add(bind(_circuit));
        }

        private static (string, Func<Circuit, Func<List<string>>>) Tap<T>(string name, Output<T> output) where T : notnull =>
            (name, circuit =>
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
            });

        /// <summary>Takes the log over, as a node does: the storage is promoted, the
        /// circuit starts on what was committed, and the source names its partitions
        /// before it sends anything (<c>KafkaSource</c> does at every start).</summary>
        public async Task<long> LeadAsync()
        {
            await Storage.PromoteAsync().WaitAsync(Timeout);
            _circuit.Start();
            _started = true;
            for (var partition = 0; partition < Partitions; partition++)
                await _circuit.Handle(_input).ExpectPartitionAsync(partition);
            return _circuit.CommittedStep ?? 0;
        }

        public async Task<List<string>> StepAsync(long step, List<Ev> batch)
        {
            var handle = _circuit.Handle(_input);
            for (var i = 0; i < batch.Count; i++)
                await handle.SendAsync(batch[i], 1, batch[i].Key % Partitions, step * 10_000 + i);
            await _circuit.StepAsync().WaitAsync(Timeout);
            return [.. _drains.SelectMany(drain => drain())];
        }

        public long CommittedStep => _circuit.CommittedStep ?? 0;
        public ChangelogStorageProvider State => Storage.Pipeline(PipelineId);

        public async ValueTask DisposeAsync()
        {
            // A node that was replaced has been fenced: its circuit's last commit fails.
            try
            {
                if (_started)
                    await _circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
            }
            catch (Exception ex) when (ex is ChangelogFencedException || ex.InnerException is ChangelogFencedException || ex is AggregateException)
            {
            }
            await Storage.DisposeAsync().AsTask().WaitAsync(Timeout);
        }
    }

    /// <summary>The run: the leader of the moment, and every step's output as last
    /// produced — a step run again after a takeover replaces what it said before, which
    /// is what a sink that commits with the checkpoint keeps.</summary>
    private sealed class Deployment(InMemoryStateChangelog log, List<List<Ev>> batches)
    {
        private readonly SortedDictionary<long, List<string>> _outputs = [];
        private long _step;
        public Node Leader = null!;

        public async Task LeadAsync(Node node)
        {
            var committed = await node.LeadAsync();
            var old = Leader;
            Leader = node;
            if (old is not null)
                await old.DisposeAsync();
            for (var replay = committed + 1; replay <= _step; replay++)
                _outputs[replay] = await node.StepAsync(replay, batches[(int)replay - 1]);
        }

        public async Task RunAsync(int steps)
        {
            for (var i = 0; i < steps && _step < batches.Count; i++)
            {
                _step++;
                _outputs[_step] = await Leader.StepAsync(_step, batches[(int)_step - 1]);
            }
        }

        public Task RunToEndAsync() => RunAsync(batches.Count);

        /// <summary>The outputs integrated: what a sink holds at the end.</summary>
        public List<string> Sinks()
        {
            var rows = new Dictionary<string, int>();
            foreach (var lines in _outputs.Values)
                foreach (var line in lines)
                {
                    var cut = line.LastIndexOf('|');
                    var row = line[..cut];
                    rows[row] = rows.GetValueOrDefault(row) + int.Parse(line[(cut + 1)..]);
                }
            return [.. rows.Where(r => r.Value != 0).Select(r => $"{r.Key} x{r.Value}").Order(StringComparer.Ordinal)];
        }

        public async Task<List<ChangelogRecord>> RecordsAsync()
        {
            var records = new List<ChangelogRecord>();
            for (var partition = 0; partition < log.PartitionCount; partition++)
            {
                var end = await log.EndOffsetAsync(partition);
                if (end == 0)
                    continue;
                using var stop = new CancellationTokenSource(Timeout);
                await foreach (var batch in log.ReadAsync(partition, 0, stop.Token))
                {
                    records.AddRange(batch.Records);
                    if (batch.NextOffset >= end)
                        break;
                }
            }
            return records;
        }
    }

    private static PersistedFormat At(int level) => PersistedFormat.ForTesting(level);

    private static async Task<List<string>> ModelAsync(List<List<Ev>> batches)
    {
        var log = new InMemoryStateChangelog(partitions: 2);
        var deployment = new Deployment(log, batches);
        await deployment.LeadAsync(new Node(log, PersistedFormat.Latest));
        await deployment.RunToEndAsync();
        var sinks = deployment.Sinks();
        await deployment.Leader.DisposeAsync();
        return sinks;
    }

    /// <summary>Whether anything in the log is of <paramref name="level"/>: a manifest of
    /// format 8 and the partitions' keys for level 2, the envelope byte for the proving
    /// level.</summary>
    private static async Task<bool> HoldsLevelAsync(Deployment deployment, int level)
    {
        if (level >= FormatLevel.Proving)
            return (await deployment.RecordsAsync()).Any(r => !r.IsTombstone && r.Value.Span[0] == FormatLevel.EnvelopeByte);
        var state = deployment.Leader.State;
        var manifest = state.CommittedManifest("local")!;
        var partitionKeys = state.CommittedOffsets.Keys.Any(key => key.StartsWith("$watermark#s0#m", StringComparison.Ordinal));
        Assert.Equal(manifest.StartsWith("format=8\n", StringComparison.Ordinal), partitionKeys);
        Assert.True(partitionKeys || manifest.StartsWith("format=7\n", StringComparison.Ordinal), manifest);
        return partitionKeys;
    }

    public static TheoryData<int, int, int> Pairs() => new()
    {
        { FormatLevel.Base, FormatLevel.PartitionTimes, 1 },
        { FormatLevel.Base, FormatLevel.PartitionTimes, 2 },
        { FormatLevel.Current, FormatLevel.Proving, 3 },
        { FormatLevel.Current, FormatLevel.Proving, 4 },
        { FormatLevel.Base, FormatLevel.Proving, 5 },
    };

    [Theory]
    [MemberData(nameof(Pairs))]
    public async Task A_pair_upgraded_one_node_at_a_time_ends_with_the_sinks_of_a_run_nobody_touched(int low, int high, int seed)
    {
        var batches = Workload(seed, steps: 60);
        var expected = await ModelAsync(batches);
        Assert.Contains(expected, row => row.StartsWith("closed|", StringComparison.Ordinal));

        var log = new InMemoryStateChangelog(partitions: 2);
        var deployment = new Deployment(log, batches);

        // Both nodes are the older build. Whatever the log was at before (nothing, here),
        // the leader writes the base level.
        var a = new Node(log, At(low));
        await deployment.LeadAsync(a);
        Assert.Equal(FormatLevel.Base, a.Formats.Writes);
        a.Formats.Raise(low);
        var b = new Node(log, At(low));
        await deployment.RunAsync(8);

        // The standby is replaced by the newer build. It follows what the older leader
        // writes, and the leader goes on writing it.
        await b.DisposeAsync();
        b = new Node(log, At(high));
        await deployment.RunAsync(8);
        await b.Storage.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.False(await HoldsLevelAsync(deployment, high));

        // Takeover. The newer build leads at the level it found: the older node, back as
        // the standby, reads everything it writes.
        await deployment.LeadAsync(b);
        Assert.Equal(low, b.Formats.Writes);
        a = new Node(log, At(low));
        await deployment.RunAsync(8);
        await a.Storage.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.False(await HoldsLevelAsync(deployment, high));

        // The old leader could lead again as it is: a takeover back to the older build,
        // before anything was raised, loses nothing.
        await deployment.LeadAsync(a);
        Assert.Equal(low, a.Formats.Writes);
        b = new Node(log, At(high));
        await deployment.RunAsync(8);
        await deployment.LeadAsync(b);
        await deployment.RunAsync(4);

        // The older node is replaced too. Both read the higher level now, and the leader
        // raises what it writes; the standby reads that as well.
        a = new Node(log, At(high));
        b.Formats.Raise(high);
        await deployment.RunAsync(8);
        Assert.True(await HoldsLevelAsync(deployment, high));
        await a.Storage.WhenCaughtUpAsync().WaitAsync(Timeout);

        // A takeover at the higher level: the new leader goes on at it without being told.
        await deployment.LeadAsync(a);
        Assert.Equal(high, a.Formats.Writes);
        await deployment.RunToEndAsync();

        Assert.Equal(expected, deployment.Sinks());

        // A node from before the level cannot follow this log any more, and says why.
        await using var late = ChangelogStorage.Follow(log, new ChangelogStorageOptions { Formats = At(low) });
        var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => late.PromoteAsync()).WaitAsync(Timeout);
        var level = Assert.IsType<FormatLevelException>(refused.InnerException);
        Assert.Equal(low, level.Reads);
        Assert.Contains("Replace it with the build the other node runs", refused.Message);
        await deployment.Leader.DisposeAsync();
    }

    [Fact]
    public async Task A_leader_that_does_not_raise_writes_what_a_build_from_before_partition_times_reads()
    {
        // Level 1 is state manifest format 7 and a marker with one time per source: the
        // newest across the source's partitions, which is what such a build would have
        // written there.
        var log = new InMemoryStateChangelog(partitions: 2);
        var batches = Workload(seed: 7, steps: 12);
        var deployment = new Deployment(log, batches);
        var node = new Node(log, new PersistedFormat());
        await deployment.LeadAsync(node);
        await deployment.RunAsync(9);

        Assert.Equal(FormatLevel.Current, node.Formats.Reads);
        Assert.Equal(FormatLevel.Base, node.Formats.Writes);
        Assert.StartsWith("format=7\n", node.State.CommittedManifest("local"));
        var offsets = node.State.CommittedOffsets;
        Assert.DoesNotContain(offsets.Keys, key => key.Contains("#m") || key.Contains("#i"));
        var newest = batches.Take((int)node.CommittedStep).SelectMany(batch => batch).Max(e => e.At.UtcTicks);
        Assert.Equal(newest, offsets["$watermark#s0"]);
        Assert.Contains("$watermark", offsets.Keys);

        // Raised, the next checkpoint is format 8 with the partitions' times beside it.
        node.Formats.Raise(FormatLevel.PartitionTimes);
        await deployment.RunAsync(3);
        Assert.StartsWith("format=8\n", node.State.CommittedManifest("local"));
        Assert.Contains("$watermark#s0#m0", node.State.CommittedOffsets.Keys);
        await node.DisposeAsync();
    }

    [Fact]
    public void A_node_reads_the_manifest_formats_up_to_its_level_and_refuses_the_ones_above()
    {
        var eight = StateManifest.Build(2, 1, 1, [], ["a", "b"]);
        Assert.StartsWith("format=8\n", eight);
        var seven = StateManifest.AtLevel(eight, FormatLevel.Base);
        Assert.StartsWith("format=7\n", seven);
        Assert.Equal(eight, StateManifest.AtLevel(seven, FormatLevel.PartitionTimes));
        Assert.Equal(FormatLevel.Base, StateManifest.LevelOf(seven));
        Assert.Equal(FormatLevel.PartitionTimes, StateManifest.LevelOf(eight));

        // Whatever the node writes now, it resumes either.
        Assert.Empty(StateManifest.Problems(seven, eight));
        Assert.Empty(StateManifest.Problems(eight, seven));
        Assert.Empty(StateManifest.Problems(seven, seven, reads: FormatLevel.Base));

        // A node of level 1 is a build from before format 8, and refuses it as one did.
        var problem = Assert.Single(StateManifest.Problems(eight, seven, reads: FormatLevel.Base));
        Assert.Contains("format=8", problem);
        Assert.Contains("must be replaced", problem);
        Assert.Single(StateManifest.Problems(eight, eight, reads: FormatLevel.Base));
    }

    [Fact]
    public void A_level_is_raised_and_never_lowered_and_not_past_what_the_node_reads()
    {
        var formats = new PersistedFormat();
        Assert.Equal((FormatLevel.Current, FormatLevel.Base), (formats.Reads, formats.Writes));
        formats.Raise(FormatLevel.Current);
        formats.Raise(FormatLevel.Base);
        Assert.Equal(FormatLevel.Current, formats.Writes);
        Assert.Throws<FormatLevelException>(() => formats.Raise(FormatLevel.Current + 1));

        var pinned = new PersistedFormat(reads: FormatLevel.Base);
        var refused = Assert.Throws<FormatLevelException>(() => pinned.NoteSeen(FormatLevel.PartitionTimes));
        Assert.Equal((FormatLevel.PartitionTimes, FormatLevel.Base), (refused.Level, refused.Reads));
        Assert.Equal(0, pinned.Seen);
        Assert.Throws<ArgumentOutOfRangeException>(() => new PersistedFormat(reads: FormatLevel.Current + 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new PersistedFormat(reads: 0));

        // Without a deployment to decide for it, a storage writes its build's level.
        Assert.Equal(FormatLevel.Current, PersistedFormat.Latest.Writes);
    }
}
