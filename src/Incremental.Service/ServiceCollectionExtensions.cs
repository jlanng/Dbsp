using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Incremental.Service;

public static class IncrementalServiceCollectionExtensions
{
    /// <summary>
    /// Registers one <see cref="ServiceNode"/> as a singleton and as a hosted service;
    /// <see cref="ITableRegistry"/>, <see cref="IPipelineSupervisor"/> and
    /// <see cref="IPluginCatalog"/> resolve from it. The backend defaults to a lone
    /// in-memory node. The host stops the node like any hosted service: it drains its
    /// pipelines while their sinks still read, within the host's shutdown timeout — raise
    /// <c>HostOptions.ShutdownTimeout</c> above
    /// <see cref="ServiceOptions.StopTimeout"/> + <see cref="ServiceOptions.AbandonTimeout"/>
    /// when sinks are slow.
    /// </summary>
    public static IncrementalServiceBuilder AddIncrementalService(this IServiceCollection services, Action<ServiceOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddOptions<ServiceOptions>();
        if (configure is not null)
            services.Configure(configure);

        var registrations = services.Select(d => d.ImplementationInstance).OfType<ServiceRegistrations>().FirstOrDefault();
        if (registrations is null)
        {
            registrations = new ServiceRegistrations();
            registrations.Connectors["memory"] = sp => sp.GetRequiredService<MemoryConnector>();
            services.AddSingleton(registrations);
            services.TryAddSingleton<MemoryConnector>();
            services.AddSingleton(sp => new ServiceNode(sp, sp.GetRequiredService<IOptions<ServiceOptions>>().Value));
            services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<ServiceNode>());
            services.AddSingleton(sp => sp.GetRequiredService<ServiceNode>().Tables);
            services.AddSingleton(sp => sp.GetRequiredService<ServiceNode>().Pipelines);
            services.AddSingleton(sp => sp.GetRequiredService<ServiceNode>().Plugins);
        }
        return new IncrementalServiceBuilder(services, registrations);
    }
}

/// <summary>What <see cref="IncrementalServiceCollectionExtensions.AddIncrementalService"/>
/// returns: the backend, the named things specs refer to, and seeds.</summary>
public sealed class IncrementalServiceBuilder
{
    private readonly ServiceRegistrations _registrations;

    internal IncrementalServiceBuilder(IServiceCollection services, ServiceRegistrations registrations)
    {
        Services = services;
        _registrations = registrations;
    }

    public IServiceCollection Services { get; }

    /// <summary>A lone in-memory node: the default.</summary>
    public IncrementalServiceBuilder UseInMemoryBackend(InMemoryBackendOptions? options = null, string nodeId = "node") =>
        UseBackend(_ => new InMemoryServiceBackend(options, nodeId));

    /// <summary>One node of an in-memory cluster shared with other nodes in this process.</summary>
    public IncrementalServiceBuilder UseInMemoryBackend(InMemoryServiceCluster cluster, string nodeId)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        return UseBackend(_ => cluster.Node(nodeId));
    }

    public IncrementalServiceBuilder UseBackend(Func<IServiceProvider, IServiceBackend> backend)
    {
        _registrations.Backend = backend ?? throw new ArgumentNullException(nameof(backend));
        return this;
    }

    /// <summary>A rooted store of files or objects that a spec may name as its connection.</summary>
    public IncrementalServiceBuilder AddObjectStore(string name, Func<IServiceProvider, IObjectStore> store)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        _registrations.ObjectStores[name] = store ?? throw new ArgumentNullException(nameof(store));
        return this;
    }

    /// <summary>A contract assembly compiled into the host: table definitions and the row
    /// types of shared tables. Plugins that reference it get this copy.</summary>
    public IncrementalServiceBuilder AddContracts(Assembly assembly)
    {
        if (!_registrations.Contracts.Contains(assembly ?? throw new ArgumentNullException(nameof(assembly))))
            _registrations.Contracts.Add(assembly);
        return this;
    }

    /// <summary>A pipeline assembly compiled into the host. Its plugin name is the
    /// assembly's simple name in lower case; it is never unloaded.</summary>
    public IncrementalServiceBuilder AddPlugin(Assembly assembly)
    {
        if (!_registrations.Plugins.Contains(assembly ?? throw new ArgumentNullException(nameof(assembly))))
            _registrations.Plugins.Add(assembly);
        return this;
    }

    /// <summary>A seed: written to the catalog by the first leader if no table of that
    /// name is declared. After that the catalog wins.</summary>
    public IncrementalServiceBuilder AddTable(ITableDefinition definition, TableLoaderSpec? loader = null, string? name = null,
        UnreadablePolicy onUnreadable = UnreadablePolicy.Fault)
    {
        ArgumentNullException.ThrowIfNull(definition);
        _registrations.Tables.Add(new TableSpec
        {
            Name = name ?? definition.Name,
            Definition = definition.GetType().FullName!,
            Contract = definition.GetType().Assembly.GetName().Name,
            Loader = loader,
            OnUnreadable = onUnreadable,
        });
        return this;
    }

    /// <inheritdoc cref="AddTable(ITableDefinition, TableLoaderSpec?, string?, UnreadablePolicy)"/>
    public IncrementalServiceBuilder AddTable(TableSpec spec)
    {
        _registrations.Tables.Add(spec ?? throw new ArgumentNullException(nameof(spec)));
        return this;
    }

    /// <summary>A seed: deployed by the first leader if no pipeline of that id exists.</summary>
    public IncrementalServiceBuilder AddPipeline(PipelineSpec spec)
    {
        _registrations.Pipelines.Add(spec ?? throw new ArgumentNullException(nameof(spec)));
        return this;
    }

    /// <summary>A custom table loader kind.</summary>
    public IncrementalServiceBuilder AddTableSource(string kind, Func<IServiceProvider, ITableSourceFactory> factory)
    {
        ArgumentException.ThrowIfNullOrEmpty(kind);
        _registrations.TableSources[kind] = factory ?? throw new ArgumentNullException(nameof(factory));
        return this;
    }

    /// <summary>A source connector: it implements <see cref="IAppendSourceConnector"/>,
    /// <see cref="IKeyedSourceConnector"/> or both.</summary>
    public IncrementalServiceBuilder AddSourceConnector(string name, Func<IServiceProvider, IConnector> connector) => AddConnector(name, connector);

    /// <summary>A sink connector: it implements <see cref="IUpsertSinkConnector"/>,
    /// <see cref="IAppendSinkConnector"/> or both.</summary>
    public IncrementalServiceBuilder AddSinkConnector(string name, Func<IServiceProvider, IConnector> connector) => AddConnector(name, connector);

    private IncrementalServiceBuilder AddConnector(string name, Func<IServiceProvider, IConnector> connector)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        _registrations.Connectors[name] = connector ?? throw new ArgumentNullException(nameof(connector));
        return this;
    }
}
