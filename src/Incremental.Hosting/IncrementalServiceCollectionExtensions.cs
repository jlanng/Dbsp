using Incremental.Hosting;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers Incremental circuits with a service collection.</summary>
public static class IncrementalServiceCollectionExtensions
{
    /// <summary>
    /// Hosts the circuit that <typeparamref name="TDefinition"/> declares. A definition class
    /// takes the <see cref="Incremental.CircuitBuilder"/> in its constructor — any further
    /// constructor parameters come from the container — declares the dataflow there, and
    /// exposes the inputs and outputs it wants reachable as members:
    /// <code>
    /// public sealed class PageAnalytics
    /// {
    ///     public InputStream&lt;PageView&gt; Views { get; }
    ///     public Output&lt;…&gt; TopPages { get; }
    ///
    ///     public PageAnalytics(CircuitBuilder builder)
    ///     {
    ///         Views = builder.Input&lt;PageView&gt;("views");
    ///         TopPages = Views.…ToOutput();
    ///     }
    /// }
    /// </code>
    /// The class is the circuit's key in the container: inject
    /// <see cref="CircuitHost{TDefinition}"/> to reach it, and register as many different
    /// definitions as you have circuits. Registering the same definition twice is a no-op
    /// that returns a builder over the first registration.
    /// </summary>
    public static HostedCircuitBuilder<TDefinition> AddIncremental<TDefinition>(this IServiceCollection services)
        where TDefinition : class =>
        services.AddIncremental(static (sp, builder) => ActivatorUtilities.CreateInstance<TDefinition>(sp, builder));

    /// <summary>As <see cref="AddIncremental{TDefinition}(IServiceCollection)"/>, constructing
    /// the definition yourself.</summary>
    public static HostedCircuitBuilder<TDefinition> AddIncremental<TDefinition>(
        this IServiceCollection services,
        Func<IServiceProvider, Incremental.CircuitBuilder, TDefinition> definitionFactory)
        where TDefinition : class
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(definitionFactory);

        if (!services.Any(d => d.ServiceType == typeof(CircuitHost<TDefinition>)))
        {
            services.AddOptions();
            services.AddSingleton(sp => new CircuitHost<TDefinition>(sp, definitionFactory));
            services.AddHostedService<CircuitHostedService<TDefinition>>();
        }
        return new HostedCircuitBuilder<TDefinition>(services);
    }
}
