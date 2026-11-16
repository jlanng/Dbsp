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
    /// <c>MaxStepItemsPerInput</c>, <c>CheckpointInterval</c>, <c>CheckpointMaxDelay</c>,
    /// <c>AutoStep</c>, <c>Name</c>, <c>ErrorPolicy</c>, <c>DeadLetterCapacity</c>,
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

    /// <summary>Delivers an acknowledged output to a sink, for the life of the circuit: the
    /// at-least-once counterpart of <see cref="AddOutputConsumer{T}"/>. Each batch is
    /// acknowledged when <paramref name="sink"/> returns, so return only once the batch is
    /// safely delivered; the circuit's checkpoint waits for that. If the sink throws, the
    /// batch is not acknowledged and the circuit faults (<see cref="CircuitFaultBehavior"/>)
    /// without committing past it — the restart delivers it again. Nothing is read and
    /// discarded on the sink's behalf.</summary>
    public HostedCircuitBuilder<TDefinition> AddAcknowledgedOutputConsumer<T>(
        Func<TDefinition, AcknowledgedOutput<T>> output,
        Func<OutputBatch<T>, CancellationToken, ValueTask> sink)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(sink);
        Services.AddSingleton<OutputConsumerRegistration<TDefinition>>(
            new AcknowledgedOutputConsumerRegistration<TDefinition, T>(output, _ => sink));
        return this;
    }

    /// <summary>Delivers an acknowledged output to a <typeparamref name="TConsumer"/> taken
    /// from the container — or constructed from it, if it is not registered — once, when
    /// the circuit starts. See the delegate overload for the contract.</summary>
    public HostedCircuitBuilder<TDefinition> AddAcknowledgedOutputConsumer<TConsumer, T>(
        Func<TDefinition, AcknowledgedOutput<T>> output)
        where TConsumer : class, IAcknowledgedOutputConsumer<T>
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(output);
        Services.AddSingleton<OutputConsumerRegistration<TDefinition>>(
            new AcknowledgedOutputConsumerRegistration<TDefinition, T>(output, static sp =>
                ActivatorUtilities.GetServiceOrCreateInstance<TConsumer>(sp).ConsumeAsync));
        return this;
    }

    /// <summary>Delivers a keyed acknowledged output to an upsert sink, for the life of the
    /// circuit: each step collapsed to at most one upsert or delete per key, written with
    /// retries, and acknowledged when the sink returns (<see cref="SinkDelivery"/>). The
    /// sink is created once, when the circuit starts. When the retries run out the circuit
    /// faults (<see cref="CircuitFaultBehavior"/>) without committing past the batch.</summary>
    public HostedCircuitBuilder<TDefinition> AddUpsertSink<TKey, TRow>(
        Func<TDefinition, AcknowledgedOutput<KeyedValue<TKey, TRow>>> output,
        Func<IServiceProvider, IUpsertSink<TKey, TRow>> sink,
        SinkDeliveryOptions? options = null)
        where TKey : notnull
        where TRow : notnull
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(sink);
        Services.AddSingleton<OutputConsumerRegistration<TDefinition>>(
            new AcknowledgedOutputConsumerRegistration<TDefinition, KeyedValue<TKey, TRow>>(
                output, sp => SinkDelivery.Upserts(sink(sp), options)));
        return this;
    }

    /// <summary><see cref="AddUpsertSink{TKey,TRow}(Func{TDefinition,AcknowledgedOutput{KeyedValue{TKey,TRow}}}, Func{IServiceProvider,IUpsertSink{TKey,TRow}}, SinkDeliveryOptions?)"/>
    /// for a result that is not a key/value pair: <paramref name="keySelector"/> names the
    /// key, and the stream must hold at most one row per key at a time.</summary>
    public HostedCircuitBuilder<TDefinition> AddUpsertSink<T, TKey>(
        Func<TDefinition, AcknowledgedOutput<T>> output,
        Func<T, TKey> keySelector,
        Func<IServiceProvider, IUpsertSink<TKey, T>> sink,
        SinkDeliveryOptions? options = null)
        where T : notnull
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(sink);
        Services.AddSingleton<OutputConsumerRegistration<TDefinition>>(
            new AcknowledgedOutputConsumerRegistration<TDefinition, T>(
                output, sp => SinkDelivery.Upserts(sink(sp), keySelector, options)));
        return this;
    }

    /// <summary>Delivers an append-only acknowledged output to an append sink, for the life
    /// of the circuit; each batch carries its step's sequence for the sink to deduplicate by
    /// (<see cref="IAppendSink{T}"/>). Otherwise as <c>AddUpsertSink</c>.</summary>
    public HostedCircuitBuilder<TDefinition> AddAppendSink<T>(
        Func<TDefinition, AcknowledgedOutput<T>> output,
        Func<IServiceProvider, IAppendSink<T>> sink,
        SinkDeliveryOptions? options = null)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(sink);
        Services.AddSingleton<OutputConsumerRegistration<TDefinition>>(
            new AcknowledgedOutputConsumerRegistration<TDefinition, T>(
                output, sp => SinkDelivery.Appends(sink(sp), options)));
        return this;
    }

    /// <summary>Reads the circuit's dead letters (<see cref="Circuit.DeadLetters"/>) with a
    /// delegate, for the life of the circuit and through its drain at shutdown. Needed
    /// whenever the circuit, or one of its operators, has the
    /// <see cref="RecordErrorPolicy.DeadLetter"/> policy: the dead-letter output is bounded,
    /// and nobody reading it stalls the circuit. If the handler throws, the failure is
    /// treated like a circuit fault and the rest is read and discarded.</summary>
    public HostedCircuitBuilder<TDefinition> AddDeadLetterConsumer(Func<DeadLetter, CancellationToken, ValueTask> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        Services.AddSingleton<OutputConsumerRegistration<TDefinition>>(new DeadLetterConsumerRegistration<TDefinition>(handler));
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
