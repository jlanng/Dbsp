using Incremental.Hosting;
using Incremental.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Incremental.Tests;

/// <summary>
/// Acknowledged outputs under the generic host: the sink's return is the acknowledgement,
/// shutdown drains into the sink and then commits, a sink that throws stops the circuit
/// without committing past the batch, and a sink that hangs costs the shutdown timeout and
/// the final commit, never a hang.
/// </summary>
[Collection("ProcessExitCode")] // StopHost sets Environment.ExitCode
public class HostingAcknowledgedOutputTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private sealed class Counts
    {
        public InputStream<int> Numbers { get; }
        public AcknowledgedOutput<KeyedValue<int, long>> ByRemainder { get; }

        public Counts(CircuitBuilder builder)
        {
            Numbers = builder.Input<int>("numbers");
            ByRemainder = Numbers.GroupBy(n => n % 3).Count().ToAcknowledgedOutput();
        }
    }

    private sealed class Sink : IAcknowledgedOutputConsumer<KeyedValue<int, long>>
    {
        public Dictionary<int, long> Rows { get; } = new();
        public List<long> Sequences { get; } = [];
        public Func<OutputBatch<KeyedValue<int, long>>, CancellationToken, ValueTask>? Before { get; set; }

        public async ValueTask ConsumeAsync(OutputBatch<KeyedValue<int, long>> batch, CancellationToken cancellationToken)
        {
            if (Before is { } before)
                await before(batch, cancellationToken);
            lock (Rows)
            {
                Sequences.Add(batch.Sequence);
                foreach (var (row, weight) in batch.Changes)
                    if (weight > 0)
                        Rows[row.Key] = row.Value;
            }
        }
    }

    /// <summary>Declared after the host, so disposed before it: whatever a test's sink is
    /// blocked on is released before the host's own disposal has to wait for that sink.</summary>
    private sealed class Unblock(CancellationTokenSource source) : IDisposable
    {
        public void Dispose() => source.Cancel();
    }

    /// <summary>Declared after the host, so it runs first when a test unwinds: a test that
    /// failed before stopping its host stops it here, inside a timeout. Disposing a host
    /// that was never stopped drains its circuit with no timeout at all, and a failed test
    /// may have left that circuit waiting on an acknowledgement.</summary>
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

    private static IHost BuildHost(Sink sink, IStorageProvider storage, Action<HostedCircuitOptions>? configure = null,
        TimeSpan? shutdownTimeout = null)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        if (shutdownTimeout is { } timeout)
            builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = timeout);
        builder.Services
            .AddSingleton(sink)
            .AddIncremental<Counts>()
            .Configure(o =>
            {
                o.Storage = storage;
                configure?.Invoke(o);
            })
            .AddAcknowledgedOutputConsumer<Sink, KeyedValue<int, long>>(d => d.ByRemainder);
        return builder.Build();
    }

    [Fact]
    public async Task Shutdown_drains_into_the_sink_and_then_commits()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var sink = new Sink { Before = async (_, _) => await Task.Delay(20) }; // slower than the circuit
        // The interval is out of reach, so the only commit is the one shutdown makes.
        using var host = BuildHost(sink, storage, o => o.CheckpointInterval = 1_000_000);
        await using var stop = new BoundedStop(host);
        await host.StartAsync().WaitAsync(Timeout);
        var circuit = host.Services.GetRequiredService<CircuitHost<Counts>>();

        var numbers = circuit.Handle(d => d.Numbers);
        for (var n = 0; n < 9; n++)
            await numbers.InsertAsync(n);

        await host.StopAsync().WaitAsync(Timeout);
        await circuit.Circuit.Completion.WaitAsync(Timeout);

        Assert.Equal(new Dictionary<int, long> { [0] = 3, [1] = 3, [2] = 3 }, sink.Rows);
        Assert.Equal(8L, circuit.Circuit.CommittedOffset("numbers"));
        Assert.Equal(sink.Sequences.Max(), circuit.Circuit.CommittedStep);
        Assert.Equal(sink.Sequences.Order(), sink.Sequences);
    }

    [Fact]
    public async Task Delegate_sink_is_acknowledged_on_return()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var delivered = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services
            .AddIncremental<Counts>()
            .Configure(o => o.Storage = storage)
            .AddAcknowledgedOutputConsumer(d => d.ByRemainder, async (batch, _) =>
            {
                delivered.TrySetResult(batch.Sequence);
                await release.Task;
            });
        using var host = builder.Build();
        await using var stop = new BoundedStop(host);
        await host.StartAsync().WaitAsync(Timeout);
        var circuit = host.Services.GetRequiredService<CircuitHost<Counts>>();
        try
        {
            await circuit.Handle(d => d.Numbers).InsertAsync(1);
            Assert.Equal(1, await delivered.Task.WaitAsync(Timeout));
            await Task.Delay(100);
            Assert.Null(circuit.Circuit.CommittedOffset("numbers")); // delivered, not yet confirmed

            release.SetResult();
            var deadline = DateTime.UtcNow + Timeout;
            while (circuit.Circuit.CommittedOffset("numbers") != 0L)
            {
                Assert.True(DateTime.UtcNow < deadline, "The checkpoint never followed the acknowledgement.");
                await Task.Delay(5);
            }
            await host.StopAsync().WaitAsync(Timeout);
        }
        finally
        {
            release.TrySetResult(); // a failed assertion must not leave the host's disposal waiting on the sink
        }
    }

    [Fact]
    public async Task Throwing_sink_stops_the_application_without_committing_past_the_batch_and_the_restart_redelivers()
    {
        var exitCode = Environment.ExitCode;
        try
        {
            var storage = new InMemoryCheckpointStorageProvider();
            var sink = new Sink
            {
                Before = (batch, _) => batch.Sequence >= 2 ? throw new IOException("sink is down") : ValueTask.CompletedTask,
            };
            long failedSequence;
            using (var host = BuildHost(sink, storage))
            {
                await using var stop = new BoundedStop(host);
                await host.StartAsync().WaitAsync(Timeout);
                var circuit = host.Services.GetRequiredService<CircuitHost<Counts>>();
                var stopping = new TaskCompletionSource();
                host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.Register(() => stopping.TrySetResult());

                var numbers = circuit.Handle(d => d.Numbers);
                await numbers.InsertAsync(0, offset: 0);
                var deadline = DateTime.UtcNow + Timeout;
                while (circuit.Circuit.CommittedOffset("numbers") != 0L)
                {
                    Assert.True(DateTime.UtcNow < deadline, "The first batch was never committed.");
                    await Task.Delay(5);
                }
                await numbers.InsertAsync(1, offset: 1);

                await stopping.Task.WaitAsync(Timeout);
                Assert.NotEqual(0, Environment.ExitCode);
                await host.StopAsync().WaitAsync(Timeout);

                var fault = await Assert.ThrowsAsync<InvalidOperationException>(() => circuit.Circuit.Completion).WaitAsync(Timeout);
                Assert.IsType<IOException>(fault.InnerException);
                Assert.Equal(0L, circuit.Circuit.CommittedOffset("numbers"));
                Assert.Equal([1L], sink.Sequences);
                failedSequence = circuit.Circuit.CommittedStep!.Value + 1;
            }

            // The process is restarted against the same store, with the sink back up.
            sink.Before = null;
            using (var host = BuildHost(sink, storage))
            {
                await using var stop = new BoundedStop(host);
                await host.StartAsync().WaitAsync(Timeout);
                var circuit = host.Services.GetRequiredService<CircuitHost<Counts>>();
                Assert.Equal(0L, circuit.Circuit.CommittedOffset("numbers"));
                var numbers = circuit.Handle(d => d.Numbers);
                await numbers.InsertAsync(0, offset: 0); // replay: dropped
                await numbers.InsertAsync(1, offset: 1);
                await host.StopAsync().WaitAsync(Timeout);

                Assert.Equal([1L, failedSequence], sink.Sequences);
                Assert.Equal(new Dictionary<int, long> { [0] = 1, [1] = 1 }, sink.Rows);
                Assert.Equal(1L, circuit.Circuit.CommittedOffset("numbers"));
            }
        }
        finally
        {
            Environment.ExitCode = exitCode;
        }
    }

    [Fact]
    public async Task Sink_that_hangs_costs_the_shutdown_timeout_and_the_final_commit_not_a_hang()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var testEnd = new CancellationTokenSource(); // unblocks the sink if an assertion fails first
        var sink = new Sink
        {
            Before = async (_, cancellationToken) =>
            {
                entered.TrySetResult();
                using var either = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, testEnd.Token);
                await Task.Delay(System.Threading.Timeout.Infinite, either.Token);
            },
        };
        using var host = BuildHost(sink, storage, shutdownTimeout: TimeSpan.FromMilliseconds(300));
        await using var stop = new BoundedStop(host);
        using var unblock = new Unblock(testEnd);
        await host.StartAsync().WaitAsync(Timeout);
        var circuit = host.Services.GetRequiredService<CircuitHost<Counts>>();

        await circuit.Handle(d => d.Numbers).InsertAsync(1);
        await entered.Task.WaitAsync(Timeout);

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await host.StopAsync().WaitAsync(Timeout);
        Assert.True(stopwatch.Elapsed >= TimeSpan.FromMilliseconds(250), $"Stopped after {stopwatch.Elapsed}: the drain was not waiting.");

        // The abandoned drain ends by itself — nothing is left waiting on the sink — and
        // the step it never confirmed is not under the checkpoint.
        await Assert.ThrowsAnyAsync<Exception>(() => circuit.Circuit.Completion).WaitAsync(Timeout);
        Assert.Null(circuit.Circuit.CommittedOffset("numbers"));
    }

    [Fact]
    public async Task Sink_that_reads_everything_but_confirms_too_late_is_released_at_the_shutdown_timeout()
    {
        // The final commit is what waits here: the interval is out of reach, the output
        // completes, and the last batch's delivery outlasts the shutdown timeout.
        var storage = new InMemoryCheckpointStorageProvider();
        using var testEnd = new CancellationTokenSource(); // unblocks the sink when the test is over
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromMilliseconds(300));
        builder.Services
            .AddIncremental<Counts>()
            .Configure(o =>
            {
                o.Storage = storage;
                o.CheckpointInterval = 1_000_000;
            })
            .AddAcknowledgedOutputConsumer(d => d.ByRemainder, async (_, _) =>
                await Task.Delay(TimeSpan.FromSeconds(30), testEnd.Token)); // ignores the shutdown token
        using var host = builder.Build();
        await using var stop = new BoundedStop(host);
        using var unblock = new Unblock(testEnd);
        await host.StartAsync().WaitAsync(Timeout);
        var circuit = host.Services.GetRequiredService<CircuitHost<Counts>>();

        await circuit.Handle(d => d.Numbers).InsertAsync(1);
        await Task.Delay(100);
        await host.StopAsync().WaitAsync(Timeout);
        await circuit.Circuit.Completion.WaitAsync(Timeout); // disposal finished: the commit was skipped, not awaited
        Assert.Null(circuit.Circuit.CommittedOffset("numbers"));
    }
}
