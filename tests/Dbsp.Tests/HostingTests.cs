using System.Collections.Concurrent;
using Dbsp.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Dbsp.Tests;

/// <summary>
/// The generic-host integration: a definition class keys the circuit in the container, the
/// host starts it and drains it at shutdown with the registered consumers still reading, a
/// fault (or a throwing consumer) stops the application by default, and options bind from
/// configuration. Every wait is fenced with a timeout — a regression in shutdown ordering
/// shows up as a hang.
/// </summary>
[Collection("ProcessExitCode")] // StopHost sets Environment.ExitCode
public class HostingTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private sealed class Counts
    {
        public InputStream<int> Numbers { get; }
        public Output<KeyedValue<int, long>> ByRemainder { get; }

        public Counts(CircuitBuilder builder)
        {
            Numbers = builder.Input<int>("numbers");
            ByRemainder = Numbers
                .Select(n => n == 13 ? throw new InvalidOperationException("boom 13") : n)
                .GroupBy(n => n % 3)
                .Count()
                .ToOutput();
        }
    }

    private sealed record Modulus(int Value);

    private sealed class CountsWithDependency
    {
        public InputStream<int> Numbers { get; }
        public Output<KeyedValue<int, long>> ByRemainder { get; }

        public CountsWithDependency(CircuitBuilder builder, Modulus modulus)
        {
            Numbers = builder.Input<int>("numbers");
            ByRemainder = Numbers.GroupBy(n => n % modulus.Value).Count().ToOutput();
        }
    }

    private sealed class Collector : IOutputConsumer<KeyedValue<int, long>>
    {
        public ZSet<KeyedValue<int, long>> Integrated { get; } = new();

        public ValueTask ConsumeAsync(ZSet<KeyedValue<int, long>> batch, CancellationToken cancellationToken)
        {
            lock (Integrated)
                Integrated.AddAll(batch);
            return ValueTask.CompletedTask;
        }
    }

    private static IHost BuildHost(Action<IServiceCollection> configure, Action<IConfigurationBuilder>? configuration = null)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        configuration?.Invoke(builder.Configuration);
        configure(builder.Services);
        return builder.Build();
    }

    private static async Task<T> Eventually<T>(Func<T> read, Func<T, bool> done)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (true)
        {
            var value = read();
            if (done(value))
                return value;
            Assert.True(DateTime.UtcNow < deadline, "Timed out waiting for the expected state.");
            await Task.Delay(10);
        }
    }

    [Fact]
    public async Task Consumer_sees_everything_including_what_the_shutdown_drain_produces()
    {
        var collector = new Collector();
        using var host = BuildHost(services => services
            .AddSingleton(collector)
            .AddDbsp<Counts>()
            .Configure(o => o.Workers = 2)
            .AddOutputConsumer<Collector, KeyedValue<int, long>>(d => d.ByRemainder));

        await host.StartAsync().WaitAsync(Timeout);
        var circuit = host.Services.GetRequiredService<CircuitHost<Counts>>();
        Assert.True(circuit.IsStarted);
        Assert.Equal(nameof(Counts), circuit.Circuit.Name);

        var numbers = circuit.Handle(d => d.Numbers);
        for (var n = 0; n < 9; n++)
            await numbers.InsertAsync(n);

        // No waiting for the results first: stopping must drain them into the consumer.
        await host.StopAsync().WaitAsync(Timeout);
        await circuit.Circuit.Completion.WaitAsync(Timeout);

        for (var remainder = 0; remainder < 3; remainder++)
            Assert.Equal(1, collector.Integrated.WeightOf(new KeyedValue<int, long>(remainder, 3)));
    }

    [Fact]
    public async Task Delegate_consumer_and_definition_dependencies_come_from_the_container()
    {
        var seen = new ConcurrentQueue<ZSet<KeyedValue<int, long>>>();
        using var host = BuildHost(services => services
            .AddSingleton(new Modulus(2))
            .AddDbsp<CountsWithDependency>()
            .AddOutputConsumer(d => d.ByRemainder, (batch, _) =>
            {
                seen.Enqueue(batch);
                return ValueTask.CompletedTask;
            }));

        await host.StartAsync().WaitAsync(Timeout);
        var numbers = host.Services.GetRequiredService<CircuitHost<CountsWithDependency>>().Handle(d => d.Numbers);
        for (var n = 0; n < 4; n++)
            await numbers.InsertAsync(n);
        await host.StopAsync().WaitAsync(Timeout);

        var integrated = new ZSet<KeyedValue<int, long>>();
        foreach (var batch in seen)
            integrated.AddAll(batch);
        Assert.Equal(1, integrated.WeightOf(new KeyedValue<int, long>(0, 2)));
        Assert.Equal(1, integrated.WeightOf(new KeyedValue<int, long>(1, 2)));
    }

    [Fact]
    public async Task View_is_shared_and_serves_the_live_result()
    {
        using var host = BuildHost(services => services.AddDbsp<Counts>());
        var circuit = host.Services.GetRequiredService<CircuitHost<Counts>>();
        var view = circuit.View(d => d.ByRemainder); // before start: handles are valid from construction
        Assert.Same(view, circuit.View(d => d.ByRemainder));
        await Task.Delay(100);                        // long enough for the view's bootstrap read to be waiting on Start
        Assert.False(view.Completion.IsCompleted);

        await host.StartAsync().WaitAsync(Timeout);
        await circuit.Handle(d => d.Numbers).InsertAsync(4);
        var rows = await Eventually(view.Current, r => r.Count == 1);
        Assert.Equal(new KeyedValue<int, long>(1, 1), rows[0].Value);

        await host.StopAsync().WaitAsync(Timeout);
    }

    [Fact]
    public async Task Options_bind_from_configuration_and_later_registrations_win()
    {
        using var host = BuildHost(
            services => services
                .AddDbsp<Counts>()
                .Configure(o => { o.Workers = 8; o.CheckpointInterval = 5; })
                .BindConfiguration("Dbsp:Counts"),
            configuration => configuration.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Dbsp:Counts:Workers"] = "3",
                ["Dbsp:Counts:Name"] = "counts-from-config",
                ["Dbsp:Counts:FaultBehavior"] = "Ignore",
            }));

        var options = host.Services.GetRequiredService<Microsoft.Extensions.Options.IOptionsMonitor<HostedCircuitOptions>>()
            .Get(typeof(Counts).FullName);
        Assert.Equal(3, options.Workers);
        Assert.Equal(5, options.CheckpointInterval);
        Assert.Equal(CircuitFaultBehavior.Ignore, options.FaultBehavior);

        var circuit = host.Services.GetRequiredService<CircuitHost<Counts>>();
        Assert.Equal("counts-from-config", circuit.Circuit.Name);
        await host.StartAsync().WaitAsync(Timeout);
        await host.StopAsync().WaitAsync(Timeout);
    }

    [Fact]
    public async Task Fault_stops_the_application_with_a_failing_exit_code_and_health()
    {
        var exitCode = Environment.ExitCode;
        try
        {
            using var host = BuildHost(services => services
                .AddDbsp<Counts>()
                .AddOutputConsumer(d => d.ByRemainder, (_, _) => ValueTask.CompletedTask)
                .AddHealthCheck());
            await host.StartAsync().WaitAsync(Timeout);

            var health = host.Services.GetRequiredService<HealthCheckService>();
            Assert.Equal(HealthStatus.Healthy, (await health.CheckHealthAsync()).Status);

            var stopping = new TaskCompletionSource();
            host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.Register(() => stopping.TrySetResult());

            var circuit = host.Services.GetRequiredService<CircuitHost<Counts>>();
            await circuit.Handle(d => d.Numbers).InsertAsync(13);
            await stopping.Task.WaitAsync(Timeout);

            Assert.NotEqual(0, Environment.ExitCode);
            var report = await health.CheckHealthAsync();
            Assert.Equal(HealthStatus.Unhealthy, report.Status);
            Assert.Contains("boom 13", report.Entries["dbsp:Counts"].Exception!.ToString());
            await Assert.ThrowsAnyAsync<Exception>(() => circuit.Circuit.Completion).WaitAsync(Timeout);

            await host.StopAsync().WaitAsync(Timeout);
        }
        finally
        {
            Environment.ExitCode = exitCode;
        }
    }

    [Fact]
    public async Task Ignore_leaves_the_host_running()
    {
        using var host = BuildHost(services => services
            .AddDbsp<Counts>()
            .Configure(o => o.FaultBehavior = CircuitFaultBehavior.Ignore)
            .AddOutputConsumer(d => d.ByRemainder, (_, _) => ValueTask.CompletedTask));
        await host.StartAsync().WaitAsync(Timeout);

        var circuit = host.Services.GetRequiredService<CircuitHost<Counts>>();
        await circuit.Handle(d => d.Numbers).InsertAsync(13);
        await Assert.ThrowsAnyAsync<Exception>(() => circuit.Circuit.Completion).WaitAsync(Timeout);
        await Task.Delay(100);

        Assert.False(host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.IsCancellationRequested);
        Assert.Equal(0, Environment.ExitCode);
        await host.StopAsync().WaitAsync(Timeout);
    }

    [Fact]
    public async Task Throwing_consumer_stops_the_application_and_shutdown_still_drains()
    {
        var exitCode = Environment.ExitCode;
        try
        {
            using var host = BuildHost(services => services
                .AddDbsp<Counts>()
                .Configure(o => o.OutputChannelCapacity = 1)
                .AddOutputConsumer(d => d.ByRemainder, (batch, _) =>
                    batch.IsEmpty ? ValueTask.CompletedTask : throw new InvalidOperationException("consumer broke")));
            await host.StartAsync().WaitAsync(Timeout);

            var stopping = new TaskCompletionSource();
            host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping.Register(() => stopping.TrySetResult());

            // Far more batches than the output channel holds: only the discard path lets
            // the circuit keep stepping, and then drain, with its consumer gone.
            var numbers = host.Services.GetRequiredService<CircuitHost<Counts>>().Handle(d => d.Numbers);
            for (var n = 0; n < 12; n++)
            {
                await numbers.InsertAsync(n).AsTask().WaitAsync(Timeout);
                await Task.Delay(5);
            }

            await stopping.Task.WaitAsync(Timeout);
            await host.StopAsync().WaitAsync(Timeout);
        }
        finally
        {
            Environment.ExitCode = exitCode;
        }
    }

    [Fact]
    public async Task Unread_output_is_abandoned_at_the_shutdown_timeout_instead_of_hanging()
    {
        using var host = BuildHost(services =>
        {
            services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromMilliseconds(300));
            services.AddDbsp<Counts>().Configure(o => o.OutputChannelCapacity = 1);
        });
        await host.StartAsync().WaitAsync(Timeout);

        var numbers = host.Services.GetRequiredService<CircuitHost<Counts>>().Handle(d => d.Numbers);
        for (var n = 0; n < 6; n++)
        {
            await numbers.InsertAsync(n).AsTask().WaitAsync(Timeout);
            await Task.Delay(20);
        }

        // The drain is genuinely stuck (nothing reads the output), so stopping takes the
        // whole shutdown timeout and then returns rather than hanging.
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await host.StopAsync().WaitAsync(Timeout);
        Assert.True(stopwatch.Elapsed >= TimeSpan.FromMilliseconds(250), $"Stopped after {stopwatch.Elapsed}: the drain was not stuck.");
    }

    [Fact]
    public void Registering_a_definition_twice_keeps_one_circuit()
    {
        var services = new ServiceCollection();
        services.AddDbsp<Counts>();
        services.AddDbsp<Counts>().Configure(o => o.Workers = 2);
        Assert.Single(services, d => d.ServiceType == typeof(CircuitHost<Counts>));
        Assert.Single(services, d => d.ServiceType == typeof(IHostedService));
    }

    [Fact]
    public async Task Subscription_before_start_waits_for_it_and_an_unstarted_disposal_ends_it_cleanly()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var numbers = builder.Input<int>("numbers");
        var counts = numbers.GroupBy(n => n % 2).Count().ToOutput();

        var circuit = builder.Build();
        var view = circuit.Handle(counts).MaterializeInMemory();
        await Task.Delay(50);
        Assert.False(view.Completion.IsCompleted);

        circuit.Start();
        await circuit.Handle(numbers).InsertAsync(3);
        await circuit.StepAsync();
        var rows = await Eventually(view.Current, r => r.Count == 1);
        Assert.Equal(new KeyedValue<int, long>(1, 1), rows[0].Value);
        await circuit.DisposeAsync();
        await view.Completion.WaitAsync(Timeout);

        var unstartedBuilder = new CircuitBuilder();
        var unstartedCounts = unstartedBuilder.Input<int>("numbers").GroupBy(n => n).Count().ToOutput();
        var unstarted = unstartedBuilder.Build();
        var orphan = unstarted.Handle(unstartedCounts).MaterializeInMemory();
        await unstarted.DisposeAsync();
        await orphan.Completion.WaitAsync(Timeout);
        Assert.Empty(orphan.Current());
    }

    [Fact]
    public async Task Completion_follows_the_circuits_terminal_state()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        builder.Input<int>("numbers").ToOutput();
        var circuit = builder.Build();
        circuit.Start();
        Assert.False(circuit.Completion.IsCompleted);
        await circuit.DisposeAsync();
        await circuit.Completion.WaitAsync(Timeout);
    }
}

[CollectionDefinition("ProcessExitCode", DisableParallelization = true)]
public class ProcessExitCodeCollection;
