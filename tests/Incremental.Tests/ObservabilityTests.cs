using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Incremental.Storage;
using Microsoft.Extensions.Logging;

namespace Incremental.Tests;

/// <summary>
/// The circuit's logs (via <see cref="CircuitOptions.LoggerFactory"/>) and the metrics on
/// the static "Incremental" meter. The meter is shared by every circuit in the process —
/// tests running in parallel included — so every assertion filters measurements down to
/// this test's circuit by its unique <c>circuit</c> tag.
/// </summary>
public class ObservabilityTests
{
    [Fact]
    public async Task Lifecycle_and_fault_are_logged()
    {
        var logs = new CapturingLoggerFactory();
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false,
            Name = $"log-test-{Guid.NewGuid():N}",
            LoggerFactory = logs,
        });
        var numbers = builder.Input<int>("numbers");
        numbers
            .Select(n => n == 13 ? throw new InvalidOperationException("boom") : n)
            .ToOutput();

        await using var circuit = builder.Build();
        Assert.Contains(logs.Entries, e =>
            e is { Category: "Incremental.Circuit", Level: LogLevel.Information } && e.Message.Contains("built"));

        circuit.Start();
        Assert.Contains(logs.Entries, e =>
            e is { Category: "Incremental.Circuit", Level: LogLevel.Information } && e.Message.Contains("started"));

        var input = circuit.Handle(numbers);
        await input.InsertAsync(13);
        await Assert.ThrowsAsync<InvalidOperationException>(() => circuit.StepAsync());

        var fault = Assert.Single(logs.Entries, e => e.Level == LogLevel.Error);
        Assert.Contains("faulted", fault.Message);
        Assert.IsType<InvalidOperationException>(fault.Exception);
    }

    [Fact]
    public async Task Steps_records_durations_commits_and_faults_are_measured()
    {
        var name = $"metric-test-{Guid.NewGuid():N}";
        using var metrics = new MetricCapture(name);

        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false,
            Name = name,
            Storage = new InMemoryCheckpointStorageProvider(),
        });
        var numbers = builder.Input<int>("numbers");
        numbers
            .Select(n => n == 13 ? throw new InvalidOperationException("boom") : n)
            .ToOutput();

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);

        await input.InsertAsync(1);
        await input.InsertAsync(2);
        await input.InsertAsync(3);
        await circuit.StepAsync();

        Assert.Equal(1, metrics.Sum("incremental.circuit.steps"));
        Assert.Equal(3, metrics.Sum("incremental.circuit.records"));
        Assert.Equal(1, metrics.Count("incremental.circuit.step_duration"));
        Assert.Equal(1, metrics.Sum("incremental.circuit.checkpoint_commits"));
        Assert.Equal(0, metrics.Sum("incremental.circuit.faults"));

        // Buffered-but-undrained records show on the input queue-depth gauge.
        await input.InsertAsync(7);
        metrics.PollGauges();
        Assert.Equal(1, metrics.Sum("incremental.circuit.input_queue_depth"));

        await input.InsertAsync(13);
        await Assert.ThrowsAsync<InvalidOperationException>(() => circuit.StepAsync());
        Assert.Equal(1, metrics.Sum("incremental.circuit.faults"));
    }

    /// <summary>Sums measurements from the "Incremental" meter carrying this circuit's tag.
    /// Gauges are polled on demand; counters and histograms arrive as they are recorded.</summary>
    private sealed class MetricCapture : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly ConcurrentDictionary<string, long> _sums = new();
        private readonly ConcurrentDictionary<string, int> _counts = new();
        private readonly string _circuit;

        public MetricCapture(string circuit)
        {
            _circuit = circuit;
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == "Incremental")
                    listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Record(instrument, value, tags));
            _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Record(instrument, (long)value, tags));
            _listener.Start();
        }

        private void Record(Instrument instrument, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            foreach (var tag in tags)
            {
                if (tag.Key == "circuit" && Equals(tag.Value, _circuit))
                {
                    _sums.AddOrUpdate(instrument.Name, value, (_, sum) => sum + value);
                    _counts.AddOrUpdate(instrument.Name, 1, (_, count) => count + 1);
                    return;
                }
            }
        }

        public void PollGauges()
        {
            // Gauges report point-in-time values; drop earlier observations before polling.
            foreach (var gauge in new[] { "incremental.circuit.input_queue_depth", "incremental.circuit.output_queue_depth" })
                _sums.TryRemove(gauge, out _);
            _listener.RecordObservableInstruments();
        }

        public long Sum(string instrument) => _sums.GetValueOrDefault(instrument);
        public int Count(string instrument) => _counts.GetValueOrDefault(instrument);

        public void Dispose() => _listener.Dispose();
    }

    private sealed class CapturingLoggerFactory : ILoggerFactory
    {
        public sealed record Entry(string Category, LogLevel Level, string Message, Exception? Exception);

        private readonly List<Entry> _entries = [];

        public IReadOnlyList<Entry> Entries
        {
            get { lock (_entries) return [.. _entries]; }
        }

        public void AddProvider(ILoggerProvider provider) { }
        public ILogger CreateLogger(string categoryName) => new Logger(categoryName, _entries);
        public void Dispose() { }

        private sealed class Logger(string category, List<Entry> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                lock (entries)
                    entries.Add(new Entry(category, logLevel, formatter(state, exception), exception));
            }
        }
    }
}
