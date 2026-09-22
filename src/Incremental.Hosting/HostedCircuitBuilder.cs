using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace Incremental.Hosting;

/// <summary>Configures one hosted circuit; returned by
/// <c>services.AddIncremental&lt;TDefinition&gt;()</c>.</summary>
public sealed class HostedCircuitBuilder<TDefinition> where TDefinition : class
{
    internal HostedCircuitBuilder(IServiceCollection services) => Services = services;

    /// <summary>The service collection the circuit is registered in.</summary>
    public IServiceCollection Services { get; }

    private OptionsBuilder<HostedCircuitOptions> Options =>
        Services.AddOptions<HostedCircuitOptions>(CircuitHost<TDefinition>.OptionsName);

    /// <summary>Sets the circuit's options in code. Runs in registration order with
    /// <see cref="BindConfiguration"/>, so whichever comes later wins.</summary>
    public HostedCircuitBuilder<TDefinition> Configure(Action<HostedCircuitOptions> configure)
    {
        Options.Configure(configure);
        return this;
    }

    /// <summary>Sets the circuit's options with the container in reach — the way to point
    /// <see cref="HostedCircuitOptions.Storage"/> at a provider built from registered
    /// services.</summary>
    public HostedCircuitBuilder<TDefinition> Configure(Action<HostedCircuitOptions, IServiceProvider> configure)
    {
        Options.Configure(configure);
        return this;
    }

    /// <summary>Binds the scalar options (<c>Workers</c>, the channel capacities,
    /// <c>MaxStepItemsPerInput</c>, <c>CheckpointInterval</c>, <c>AutoStep</c>, <c>Name</c>,
    /// <c>FaultBehavior</c>) from a configuration section, e.g.
    /// <c>"Incremental:PageAnalytics"</c>. Options are read once, when the circuit is built:
    /// a circuit's shape cannot change under it, so configuration reloads are not observed.</summary>
    public HostedCircuitBuilder<TDefinition> BindConfiguration(string configSectionPath)
    {
        Options.BindConfiguration(configSectionPath);
        return this;
    }

    /// <summary>Consumes an output's changefeed with a delegate, for the life of the circuit.
    /// The host runs the consumer so that it outlives the circuit's drain at shutdown and
    /// sees every batch; see <see cref="IOutputConsumer{T}"/> for the contract.</summary>
    public HostedCircuitBuilder<TDefinition> AddOutputConsumer<T>(
        Func<TDefinition, Output<T>> output,
        Func<ZSet<T>, CancellationToken, ValueTask> handler)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(handler);
        Services.AddSingleton<OutputConsumerRegistration<TDefinition>>(
            new OutputConsumerRegistration<TDefinition, T>(output, _ => handler));
        return this;
    }

    /// <summary>Consumes an output's changefeed with a <typeparamref name="TConsumer"/>
    /// taken from the container — or constructed from it, if it is not registered — once,
    /// when the circuit starts. It lives as long as the circuit, so give it singleton-safe
    /// dependencies and open scopes per batch if it needs scoped ones.</summary>
    public HostedCircuitBuilder<TDefinition> AddOutputConsumer<TConsumer, T>(Func<TDefinition, Output<T>> output)
        where TConsumer : class, IOutputConsumer<T>
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(output);
        Services.AddSingleton<OutputConsumerRegistration<TDefinition>>(
            new OutputConsumerRegistration<TDefinition, T>(output, static sp =>
                ActivatorUtilities.GetServiceOrCreateInstance<TConsumer>(sp).ConsumeAsync));
        return this;
    }

    /// <summary>Adds a health check that fails once the circuit has faulted (and until it has
    /// started). <paramref name="name"/> defaults to <c>incremental:</c> plus the definition
    /// class's name.</summary>
    public HostedCircuitBuilder<TDefinition> AddHealthCheck(
        string? name = null, HealthStatus? failureStatus = null, IEnumerable<string>? tags = null)
    {
        Services.AddHealthChecks().Add(new HealthCheckRegistration(
            name ?? $"incremental:{typeof(TDefinition).Name}",
            static sp => new CircuitHealthCheck<TDefinition>(sp.GetRequiredService<CircuitHost<TDefinition>>()),
            failureStatus,
            tags));
        return this;
    }
}
