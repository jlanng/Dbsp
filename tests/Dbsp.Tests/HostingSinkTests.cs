using Dbsp.Hosting;
using Dbsp.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Dbsp.Tests;

/// <summary>
/// Sinks and the dead-letter output under the generic host: a registered upsert or append
/// sink is delivered to for the life of the circuit and through its drain, and a registered
/// dead-letter consumer keeps the dead-letter output read.
/// </summary>
[Collection("ProcessExitCode")] // StopHost sets Environment.ExitCode
public class HostingSinkTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private sealed class Counts
    {
        public InputStream<int> Numbers { get; }
        public AcknowledgedOutput<KeyedValue<int, long>> ByRemainder { get; }
        public AcknowledgedOutput<int> Evens { get; }
        public AcknowledgedOutput<(int Remainder, long Count)> Flat { get; }

        public Counts(CircuitBuilder builder)
        {
            Numbers = builder.Input<int>("numbers");
            var valid = Numbers.Select(n => n < 0 ? throw new InvalidOperationException($"negative {n}") : n);
            var counts = valid.GroupBy(n => n % 3).Count();
            ByRemainder = counts.ToAcknowledgedOutput();
            Flat = counts.Select(kv => (kv.Key, kv.Value)).ToAcknowledgedOutput();
            Evens = valid.Where(n => n % 2 == 0).ToAcknowledgedOutput();
        }
    }

    private sealed class BoundedStop(IHost host) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(1));
            try
            {
                await host.StopAsync(limit.Token);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    [Fact]
    public void Dead_letter_capacity_reaches_the_circuit()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddDbsp<Counts>().Configure(o => o.DeadLetterCapacity = 0);
        using var host = builder.Build();

        // The circuit is built when its host object is first resolved, and refuses the value.
        Assert.Throws<ArgumentOutOfRangeException>(() => host.Services.GetRequiredService<CircuitHost<Counts>>());
    }

    [Fact]
    public async Task Registered_sinks_are_delivered_to_and_dead_letters_are_consumed()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var upserts = new InMemoryUpsertSink<int, long>();
        var flat = new InMemoryUpsertSink<int, (int Remainder, long Count)>();
        var appends = new InMemoryAppendSink<int>();
        var letters = new List<DeadLetter>();

        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services
            .AddSingleton(upserts)
            .AddDbsp<Counts>()
            .Configure(o =>
            {
                o.Storage = storage;
                o.ErrorPolicy = RecordErrorPolicy.DeadLetter;
                o.DeadLetterCapacity = 1; // only a consumer that keeps reading gets the circuit past two
            })
            .AddUpsertSink(d => d.ByRemainder, sp => sp.GetRequiredService<InMemoryUpsertSink<int, long>>())
            .AddUpsertSink(d => d.Flat, row => row.Remainder, _ => flat)
            .AddAppendSink(d => d.Evens, _ => appends)
            .AddDeadLetterConsumer((letter, _) =>
            {
                lock (letters)
                    letters.Add(letter);
                return ValueTask.CompletedTask;
            });
        using var host = builder.Build();
        await using var stop = new BoundedStop(host);
        await host.StartAsync().WaitAsync(Timeout);
        var circuit = host.Services.GetRequiredService<CircuitHost<Counts>>();

        var numbers = circuit.Handle(d => d.Numbers);
        foreach (var n in new[] { 0, 1, 2, -1, 3, 4, -2, -3, 6 })
            await numbers.InsertAsync(n);
        await host.StopAsync().WaitAsync(Timeout);

        Assert.Null(circuit.Circuit.Fault);
        Assert.Equal(new Dictionary<int, long> { [0] = 3, [1] = 2, [2] = 1 }, upserts.Rows);
        Assert.Equal(
            new Dictionary<int, (int, long)> { [0] = (0, 3), [1] = (1, 2), [2] = (2, 1) }, flat.Rows);
        Assert.Equal([0, 2, 4, 6], appends.Records.Order());
        lock (letters)
            Assert.Equal([-3, -2, -1], letters.Select(l => (int)l.Record!).Order());

        // The drain's final commit waited for the sinks: everything is in the checkpoint.
        Assert.Equal(8L, circuit.Circuit.CommittedOffset("numbers"));
    }

    /// <summary>An upsert sink whose first write fails.</summary>
    private sealed class FailsOnce<TKey, TRow>(InMemoryUpsertSink<TKey, TRow> inner) : IUpsertSink<TKey, TRow>
        where TKey : notnull
    {
        private int _writes;

        public ValueTask WriteAsync(UpsertBatch<TKey, TRow> batch, CancellationToken cancellationToken) =>
            Interlocked.Increment(ref _writes) == 1 ? throw new IOException("not yet") : inner.WriteAsync(batch, cancellationToken);
    }

    /// <summary>An append sink whose first write fails.</summary>
    private sealed class AppendFailsOnce<T>(InMemoryAppendSink<T> inner) : IAppendSink<T>
    {
        private int _writes;

        public ValueTask WriteAsync(AppendBatch<T> batch, CancellationToken cancellationToken) =>
            Interlocked.Increment(ref _writes) == 1 ? throw new IOException("not yet") : inner.WriteAsync(batch, cancellationToken);
    }

    [Fact]
    public async Task Each_sink_registration_delivers_with_the_options_it_was_given()
    {
        var upserts = new InMemoryUpsertSink<int, long>();
        var flat = new InMemoryUpsertSink<int, (int Remainder, long Count)>();
        var appends = new InMemoryAppendSink<int>();

        // Each registration's options report their failed attempts to their own counter:
        // delivery under the default options would retry just the same, and report nothing.
        var failed = new int[3];
        SinkDeliveryOptions Options(int index) => new()
        {
            MaxRetries = 1,
            InitialBackoff = TimeSpan.FromMilliseconds(1),
            OnFailedAttempt = (_, _) => Interlocked.Increment(ref failed[index]),
        };

        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services
            .AddDbsp<Counts>()
            .AddUpsertSink(d => d.ByRemainder, _ => new FailsOnce<int, long>(upserts), Options(0))
            .AddUpsertSink(d => d.Flat, row => row.Remainder, _ => new FailsOnce<int, (int Remainder, long Count)>(flat), Options(1))
            .AddAppendSink(d => d.Evens, _ => new AppendFailsOnce<int>(appends), Options(2));
        using var host = builder.Build();
        await using var stop = new BoundedStop(host);
        await host.StartAsync().WaitAsync(Timeout);
        var circuit = host.Services.GetRequiredService<CircuitHost<Counts>>();

        foreach (var n in new[] { 0, 1, 2, 4 })
            await circuit.Handle(d => d.Numbers).InsertAsync(n);
        await host.StopAsync().WaitAsync(Timeout);

        Assert.Null(circuit.Circuit.Fault);
        Assert.Equal([1, 1, 1], failed);
        Assert.Equal(new Dictionary<int, long> { [0] = 1, [1] = 2, [2] = 1 }, upserts.Rows);
        Assert.Equal(new Dictionary<int, (int, long)> { [0] = (0, 1), [1] = (1, 2), [2] = (2, 1) }, flat.Rows);
        Assert.Equal([0, 2, 4], appends.Records.Order());
    }

    [Fact]
    public async Task Dead_letter_consumer_that_throws_stops_the_host_and_the_rest_are_discarded_so_the_drain_finishes()
    {
        var exitCode = Environment.ExitCode;
        try
        {
            var storage = new InMemoryCheckpointStorageProvider();
            var upserts = new InMemoryUpsertSink<int, long>();
            var fed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var handled = 0;

            var builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders();
            builder.Services
                .AddDbsp<Counts>()
                .Configure(o =>
                {
                    o.Storage = storage;
                    o.ErrorPolicy = RecordErrorPolicy.DeadLetter;
                    o.DeadLetterCapacity = 1; // unread, the second dead letter stalls the circuit
                })
                .AddUpsertSink(d => d.ByRemainder, _ => upserts)
                .AddUpsertSink(d => d.Flat, row => row.Remainder, _ => new InMemoryUpsertSink<int, (int Remainder, long Count)>())
                .AddAppendSink(d => d.Evens, _ => new InMemoryAppendSink<int>())
                .AddDeadLetterConsumer(async (_, _) =>
                {
                    Interlocked.Increment(ref handled);
                    await fed.Task; // not before everything is in the input
                    throw new IOException("the dead-letter store is down");
                });
            using var host = builder.Build();
            await using var stop = new BoundedStop(host);
            await host.StartAsync().WaitAsync(Timeout);
            var circuit = host.Services.GetRequiredService<CircuitHost<Counts>>();
            var stopping = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.Register(() => stopping.TrySetResult());

            var numbers = circuit.Handle(d => d.Numbers);
            var offset = 0;
            foreach (var n in new[] { -1, 0, -2, 1, -3, 2, -4, 3, -5, 4 })
                await numbers.InsertAsync(n, offset++);
            fed.SetResult();

            // The handler's failure stops the application, like a circuit fault…
            await stopping.Task.WaitAsync(Timeout);
            Assert.NotEqual(0, Environment.ExitCode);

            // …and the drain still finishes: the letters nobody will handle are read and dropped.
            await host.StopAsync().WaitAsync(Timeout);
            Assert.Null(circuit.Circuit.Fault);
            Assert.Equal(1, handled);
            Assert.Equal(new Dictionary<int, long> { [0] = 2, [1] = 2, [2] = 1 }, upserts.Rows);
            Assert.Equal(9L, circuit.Circuit.CommittedOffset("numbers"));
        }
        finally
        {
            Environment.ExitCode = exitCode;
        }
    }
}
