using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;

namespace Incremental.Showcase;

/// <summary>Severity of a notice, mapped to a colour in the browser.</summary>
public enum NoticeLevel { Info, Good, Warn, Bad, Log }

public record Notice(NoticeLevel Level, string Text, string Stamp);

/// <summary>Rolling notice feed: scripted beats, log lines, warnings. Written from the
/// driver loop and from library logger callbacks (any thread), read while composing a
/// snapshot — hence the lock.</summary>
public sealed class NoticeLog
{
    readonly Queue<Notice> notices = new();
    readonly int capacity;

    public NoticeLog(int capacity) => this.capacity = capacity;

    public void Push(NoticeLevel level, string text)
    {
        lock (notices)
        {
            notices.Enqueue(new Notice(level, text, DateTime.Now.ToString("HH:mm:ss")));
            while (notices.Count > capacity) notices.Dequeue();
        }
    }

    /// <summary>Newest first.</summary>
    public List<Notice> Snapshot()
    {
        lock (notices) return notices.Reverse().ToList();
    }
}

/// <summary>Routes the circuit's ILogger output into the notice feed, so observability is
/// on screen instead of merely wired. (An ILoggerFactory of its own: the example keeps its
/// own logging rather than borrowing the web host's — the library needs only the
/// abstractions.)</summary>
public sealed class NoticeLoggerProvider : ILoggerFactory
{
    readonly NoticeLog notices;
    public NoticeLoggerProvider(NoticeLog notices) => this.notices = notices;
    public ILogger CreateLogger(string categoryName) => new NoticeLogger(notices, categoryName);
    public void AddProvider(ILoggerProvider provider) { }
    public void Dispose() { }

    sealed class NoticeLogger : ILogger
    {
        readonly NoticeLog notices;
        readonly string category;
        public NoticeLogger(NoticeLog notices, string category) => (this.notices, this.category) = (notices, category);
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel)) return;
            notices.Push(logLevel >= LogLevel.Error ? NoticeLevel.Bad : NoticeLevel.Log,
                $"log · {category}: {formatter(state, exception)}");
        }
    }
}

/// <summary>What the always-on <c>Incremental</c> meter says about one circuit, as sent to
/// the browser footer.</summary>
public record MeterSnapshot(
    double Steps, double Records, double Commits, double StepMs,
    double WatermarkLag, double InputQueue, double OutputQueue);

/// <summary>
/// Listens to the library's always-on <c>Incremental</c> meter and aggregates per circuit
/// name, so the footer shows real telemetry rather than hand-rolled counters. Counters
/// accumulate; histograms keep the last value; observable gauges are re-summed on every
/// <see cref="Poll"/>.
/// </summary>
public sealed class IncrementalMetrics : IDisposable
{
    readonly MeterListener listener;
    readonly ConcurrentDictionary<(string Circuit, string Instrument), double> counters = new();
    readonly ConcurrentDictionary<(string Circuit, string Instrument), double> lastValues = new();
    readonly ConcurrentDictionary<(string Circuit, string Instrument), double> gauges = new();
    ConcurrentDictionary<(string Circuit, string Instrument), double>? gaugeCycle;

    static readonly HashSet<string> GaugeInstruments =
    [
        "incremental.circuit.input_queue_depth",
        "incremental.circuit.output_queue_depth",
        "incremental.circuit.watermark_lag",
        "incremental.cluster.send_queue_depth",
    ];

    public IncrementalMetrics()
    {
        listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == "Incremental") l.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<long>((i, v, tags, _) => Record(i, v, tags));
        listener.SetMeasurementEventCallback<double>((i, v, tags, _) => Record(i, v, tags));
        listener.SetMeasurementEventCallback<int>((i, v, tags, _) => Record(i, v, tags));
        listener.Start();
    }

    void Record(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var circuit = "";
        foreach (var tag in tags)
            if (tag.Key == "circuit") { circuit = tag.Value?.ToString() ?? ""; break; }
        var key = (circuit, instrument.Name);

        if (GaugeInstruments.Contains(instrument.Name))
            gaugeCycle?.AddOrUpdate(key, value, (_, v) => v + value);
        else if (instrument is Histogram<double> or Histogram<long>)
            lastValues[key] = value;
        else
            counters.AddOrUpdate(key, value, (_, v) => v + value);
    }

    /// <summary>Collect the observable gauges (queue depths, watermark lag) once.</summary>
    public void Poll()
    {
        var cycle = new ConcurrentDictionary<(string, string), double>();
        gaugeCycle = cycle;
        listener.RecordObservableInstruments();
        gaugeCycle = null;
        gauges.Clear();
        foreach (var (key, value) in cycle) gauges[key] = value;
    }

    public double Counter(string circuit, string instrument) => counters.GetValueOrDefault((circuit, instrument));
    public double Last(string circuit, string instrument) => lastValues.GetValueOrDefault((circuit, instrument));
    public double Gauge(string circuit, string instrument) => gauges.GetValueOrDefault((circuit, instrument));

    public MeterSnapshot Footer(string circuit) => new(
        Counter(circuit, "incremental.circuit.steps"),
        Counter(circuit, "incremental.circuit.records"),
        Counter(circuit, "incremental.circuit.checkpoint_commits"),
        Last(circuit, "incremental.circuit.step_duration") * 1000,
        Gauge(circuit, "incremental.circuit.watermark_lag"),
        Gauge(circuit, "incremental.circuit.input_queue_depth"),
        Gauge(circuit, "incremental.circuit.output_queue_depth"));

    public void Dispose() => listener.Dispose();
}

public static class Ui
{
    public static string Truncate(string value, int width) =>
        value.Length <= width ? value : value[..(width - 1)] + "…";
}
