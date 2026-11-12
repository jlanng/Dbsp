using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Incremental.Service;

/// <summary>
/// What one pipeline — or, summed, the node — did in one slot of time. Counts are for the
/// slot; <c>StepSeconds</c> and <c>SinkAckWaitSeconds</c> are sums over it (divide by
/// <c>Steps</c> and <c>Commits</c> for a mean) with their largest single value beside them;
/// <c>InputLag</c> and <c>CheckpointAgeSeconds</c> are as of the slot's end, null when
/// nothing reports them.
/// </summary>
public sealed record MetricSample(
    DateTimeOffset Time, double Seconds,
    long Steps, double StepSeconds, double StepMaxSeconds,
    long RecordsIn, long RecordsOut,
    long Commits, double SinkAckWaitSeconds, double SinkAckWaitMaxSeconds,
    long DeadLetters, long? InputLag, double? CheckpointAgeSeconds);

/// <summary>A pipeline's recent history: up to 60 one-second slots and up to 60
/// one-minute slots, oldest first.</summary>
public sealed record PipelineMetrics(string Pipeline, IReadOnlyList<MetricSample> Seconds, IReadOnlyList<MetricSample> Minutes);

internal readonly record struct PipelineSampleInput(
    string Id, long Generation, long RecordsOut, long? InputLag, long DeadLetters,
    PipelineState State = PipelineState.Unbuilt, bool DesiredRunning = false, long SinkInFlightSinceTicks = 0,
    long StateEntries = 0, long StateBytes = 0);

/// <summary>
/// The numbers an operator looks at, kept in the process: one <see cref="MeterListener"/>
/// on the core's <c>Incremental</c> meter, and per pipeline two fixed rings — 60
/// one-second slots and 60 one-minute slots — so the last hour is there for someone paged
/// late. Anything longer is a collector's job. Records out and input lag come from the
/// supervisor's sink and source counters, sampled once a second.
/// </summary>
/// <remarks>The listener's callbacks run on a circuit's coordinator, once per step or
/// commit: a dictionary read and a few interlocked adds, no allocation. A measurement for
/// a circuit that is not a deployed pipeline is ignored.</remarks>
public sealed class ServiceMetrics : IDisposable
{
    private const int Slots = 60;

    private sealed class Accumulator
    {
        // Written by the listener, drained by the sampler.
        public long Steps;
        public long StepTicks;
        public long StepMaxTicks;
        public long Records;
        public long Commits;
        public long AckTicks;
        public long AckMaxTicks;
        public long LastCommitTicks; // UTC ticks of the last checkpoint commit; 0: none seen

        // Whether this node runs the pipeline's circuit. Two nodes in one process (the
        // in-memory cluster) hear each other's circuits: only the one that runs it counts.
        public volatile bool Active;

        // The sampler's own.
        public long Generation;
        public long RecordsOutSeen;
        public long DeadLettersSeen;
        public bool Primed;
        public readonly MetricSample?[] Seconds = new MetricSample?[Slots];
        public readonly MetricSample?[] Minutes = new MetricSample?[Slots];
        public int SecondAt, MinuteAt, SecondsInMinute;
    }

    private readonly PipelineSupervisor _supervisor;
    private readonly ConcurrentDictionary<string, Accumulator> _pipelines = new(StringComparer.Ordinal);
    private readonly Accumulator _node = new();
    private readonly MeterListener _listener = new();
    private readonly List<PipelineSampleInput> _inputs = [];
    private volatile PipelineGauge[] _gauges = [];

    /// <summary>Every pipeline as of the last sample, for the service's meter.</summary>
    internal PipelineGauge[] Gauges => _gauges;
    private readonly object _sampling = new();
    private readonly CancellationTokenSource _stop = new();
    private Task _loop = Task.CompletedTask;
    private DateTimeOffset _last;

    internal ServiceMetrics(PipelineSupervisor supervisor)
    {
        _supervisor = supervisor;
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "Incremental" && instrument.Name is
                "incremental.circuit.steps" or "incremental.circuit.step_duration" or "incremental.circuit.records"
                or "incremental.circuit.checkpoint_commits" or "incremental.circuit.sink_ack_wait")
                listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<long>(OnLong);
        _listener.SetMeasurementEventCallback<double>(OnDouble);
    }

    /// <summary>Raised after each one-second sample, on the sampler's thread. A handler
    /// must not block: the next sample waits for it.</summary>
    public event Action? Sampled;

    internal void Start()
    {
        _listener.Start();
        _last = DateTimeOffset.UtcNow;
        _loop = Task.Run(() => RunAsync(_stop.Token));
    }

    private Accumulator? Find(ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        foreach (var tag in tags)
        {
            if (tag.Key != "circuit" || tag.Value is not string name)
                continue;
            if (_pipelines.TryGetValue(name, out var accumulator) && accumulator.Active)
                return accumulator;
            // The first measurements of a generation arrive before the sampler has seen it.
            if (!_supervisor.RunsHere(name))
                return null;
            accumulator = _pipelines.GetOrAdd(name, static _ => new Accumulator());
            accumulator.Active = true;
            return accumulator;
        }
        return null;
    }

    private void OnLong(Instrument instrument, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        if (Find(tags) is not { } a)
            return;
        switch (instrument.Name)
        {
            case "incremental.circuit.steps":
                Interlocked.Add(ref a.Steps, value);
                break;
            case "incremental.circuit.records":
                Interlocked.Add(ref a.Records, value);
                break;
            case "incremental.circuit.checkpoint_commits":
                Interlocked.Add(ref a.Commits, value);
                Interlocked.Exchange(ref a.LastCommitTicks, DateTime.UtcNow.Ticks);
                break;
        }
    }

    private void OnDouble(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
    {
        if (Find(tags) is not { } a)
            return;
        var ticks = (long)(value * TimeSpan.TicksPerSecond);
        if (instrument.Name == "incremental.circuit.step_duration")
        {
            Interlocked.Add(ref a.StepTicks, ticks);
            Max(ref a.StepMaxTicks, ticks);
        }
        else
        {
            Interlocked.Add(ref a.AckTicks, ticks);
            Max(ref a.AckMaxTicks, ticks);
        }
    }

    private static void Max(ref long target, long value)
    {
        long seen;
        while (value > (seen = Interlocked.Read(ref target)) && Interlocked.CompareExchange(ref target, value, seen) != seen)
        {
        }
    }

    private async Task RunAsync(CancellationToken stop)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        try
        {
            while (await timer.WaitForNextTickAsync(stop).ConfigureAwait(false))
            {
                Sample(DateTimeOffset.UtcNow);
                try
                {
                    Sampled?.Invoke();
                }
                catch
                {
                    // A listener's failure is not the sampler's.
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    /// <summary>Closes one slot: drains every pipeline's counters into a sample, and the
    /// sum of them into the node's.</summary>
    internal void Sample(DateTimeOffset now)
    {
        lock (_sampling)
        {
            var seconds = Math.Max(0.001, (now - _last).TotalSeconds);
            _last = now;
            _inputs.Clear();
            _supervisor.SampleInto(_inputs);

            foreach (var id in _pipelines.Keys)
            {
                var known = false;
                foreach (var input in _inputs)
                    known |= input.Id == id;
                if (!known)
                    _pipelines.TryRemove(id, out _);
            }

            var gauges = new PipelineGauge[_inputs.Count];
            var at = 0;
            long steps = 0, recordsIn = 0, recordsOut = 0, commits = 0, deadLetters = 0;
            double stepSeconds = 0, stepMax = 0, ackSeconds = 0, ackMax = 0;
            long? lagTotal = null;
            double? oldestCheckpoint = null;
            foreach (var input in _inputs)
            {
                var a = _pipelines.GetOrAdd(input.Id, _ => new Accumulator());
                a.Active = input.Generation != 0 || _supervisor.RunsHere(input.Id);
                if (a.Generation != input.Generation)
                {
                    // A new generation's sinks count from zero.
                    a.Generation = input.Generation;
                    a.RecordsOutSeen = 0;
                }
                var outNow = Math.Max(0, input.RecordsOut - a.RecordsOutSeen);
                a.RecordsOutSeen = input.RecordsOut;
                var dead = a.Primed ? Math.Max(0, input.DeadLetters - a.DeadLettersSeen) : 0;
                a.DeadLettersSeen = input.DeadLetters;
                a.Primed = true;

                var lastCommit = Interlocked.Read(ref a.LastCommitTicks);
                double? age = lastCommit == 0 || input.Generation == 0 ? null : Math.Max(0, (now.UtcTicks - lastCommit) / (double)TimeSpan.TicksPerSecond);
                var sample = new MetricSample(
                    now, seconds,
                    Interlocked.Exchange(ref a.Steps, 0),
                    Interlocked.Exchange(ref a.StepTicks, 0) / (double)TimeSpan.TicksPerSecond,
                    Interlocked.Exchange(ref a.StepMaxTicks, 0) / (double)TimeSpan.TicksPerSecond,
                    Interlocked.Exchange(ref a.Records, 0), outNow,
                    Interlocked.Exchange(ref a.Commits, 0),
                    Interlocked.Exchange(ref a.AckTicks, 0) / (double)TimeSpan.TicksPerSecond,
                    Interlocked.Exchange(ref a.AckMaxTicks, 0) / (double)TimeSpan.TicksPerSecond,
                    dead, input.InputLag, age);
                Push(a, sample);
                gauges[at++] = new PipelineGauge(
                    input.Id, input.State, input.DesiredRunning, input.InputLag, age,
                    input.SinkInFlightSinceTicks == 0 ? 0 : Math.Max(0, (now.UtcTicks - input.SinkInFlightSinceTicks) / (double)TimeSpan.TicksPerSecond),
                    input.DeadLetters, input.StateEntries, input.StateBytes);

                steps += sample.Steps;
                stepSeconds += sample.StepSeconds;
                stepMax = Math.Max(stepMax, sample.StepMaxSeconds);
                recordsIn += sample.RecordsIn;
                recordsOut += sample.RecordsOut;
                commits += sample.Commits;
                ackSeconds += sample.SinkAckWaitSeconds;
                ackMax = Math.Max(ackMax, sample.SinkAckWaitMaxSeconds);
                deadLetters += sample.DeadLetters;
                if (sample.InputLag is { } lag)
                    lagTotal = (lagTotal ?? 0) + lag;
                if (age is { } a2)
                    oldestCheckpoint = Math.Max(oldestCheckpoint ?? 0, a2);
            }
            Push(_node, new MetricSample(
                now, seconds, steps, stepSeconds, stepMax, recordsIn, recordsOut, commits, ackSeconds, ackMax, deadLetters, lagTotal, oldestCheckpoint));
            _gauges = gauges;
        }
    }

    private static void Push(Accumulator a, MetricSample sample)
    {
        a.Seconds[a.SecondAt] = sample;
        a.SecondAt = (a.SecondAt + 1) % Slots;
        if (++a.SecondsInMinute < Slots)
            return;
        a.SecondsInMinute = 0;
        a.Minutes[a.MinuteAt] = Combine(Ordered(a.Seconds, a.SecondAt));
        a.MinuteAt = (a.MinuteAt + 1) % Slots;
    }

    private static MetricSample Combine(List<MetricSample> samples)
    {
        var last = samples[^1];
        return new MetricSample(
            last.Time, samples.Sum(s => s.Seconds),
            samples.Sum(s => s.Steps), samples.Sum(s => s.StepSeconds), samples.Max(s => s.StepMaxSeconds),
            samples.Sum(s => s.RecordsIn), samples.Sum(s => s.RecordsOut),
            samples.Sum(s => s.Commits), samples.Sum(s => s.SinkAckWaitSeconds), samples.Max(s => s.SinkAckWaitMaxSeconds),
            samples.Sum(s => s.DeadLetters), samples.Max(s => s.InputLag), samples.Max(s => s.CheckpointAgeSeconds));
    }

    private static List<MetricSample> Ordered(MetricSample?[] ring, int next)
    {
        var ordered = new List<MetricSample>(Slots);
        for (var i = 0; i < Slots; i++)
            if (ring[(next + i) % Slots] is { } sample)
                ordered.Add(sample);
        return ordered;
    }

    /// <summary>The pipeline's rings; empty for a pipeline not sampled yet.</summary>
    public PipelineMetrics Pipeline(string id)
    {
        lock (_sampling)
            return _pipelines.TryGetValue(id, out var a)
                ? new PipelineMetrics(id, Ordered(a.Seconds, a.SecondAt), Ordered(a.Minutes, a.MinuteAt))
                : new PipelineMetrics(id, [], []);
    }

    /// <summary>Every pipeline summed: lag is the total, checkpoint age the oldest.</summary>
    public PipelineMetrics Node
    {
        get
        {
            lock (_sampling)
                return new PipelineMetrics("", Ordered(_node.Seconds, _node.SecondAt), Ordered(_node.Minutes, _node.MinuteAt));
        }
    }

    /// <summary>The newest one-second sample of every pipeline, and of the node under the
    /// empty id: what a live view is fed.</summary>
    public IReadOnlyDictionary<string, MetricSample> Latest
    {
        get
        {
            lock (_sampling)
            {
                var latest = new Dictionary<string, MetricSample>(StringComparer.Ordinal);
                if (_node.Seconds[(_node.SecondAt + Slots - 1) % Slots] is { } node)
                    latest[""] = node;
                foreach (var (id, a) in _pipelines)
                    if (a.Seconds[(a.SecondAt + Slots - 1) % Slots] is { } sample)
                        latest[id] = sample;
                return latest;
            }
        }
    }

    public void Dispose()
    {
        _stop.Cancel();
        _listener.Dispose();
        try
        {
            _loop.Wait(TimeSpan.FromSeconds(2));
        }
        catch
        {
        }
    }
}
