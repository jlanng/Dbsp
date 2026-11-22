using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Dbsp.Service;

public static class DbspServiceCollectionExtensions
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
    public static DbspServiceBuilder AddDbspService(this IServiceCollection services, Action<ServiceOptions>? configure = null)
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
            registrations.Connectors["kafka"] = sp => new KafkaConnector(sp.GetRequiredService<KafkaConnections>());
            registrations.Connectors["files"] = sp => new FileConnector(sp.GetRequiredService<ObjectStores>());
            services.AddSingleton(registrations);
            services.AddSingleton(sp => new KafkaConnections(sp, registrations));
            services.AddSingleton(sp => new ObjectStores(sp, registrations));
            services.TryAddSingleton<MemoryConnector>();
            services.AddSingleton(sp => new ServiceNode(sp, sp.GetRequiredService<IOptions<ServiceOptions>>().Value));
            services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<ServiceNode>());
            services.AddSingleton(sp => sp.GetRequiredService<ServiceNode>().Tables);
            services.AddSingleton(sp => sp.GetRequiredService<ServiceNode>().Pipelines);
            services.AddSingleton(sp => sp.GetRequiredService<ServiceNode>().Plugins);
        }
        return new DbspServiceBuilder(services, registrations);
    }
}

/// <summary>What <see cref="DbspServiceCollectionExtensions.AddDbspService"/>
/// returns: the backend, the named things specs refer to, and seeds.</summary>
public sealed class DbspServiceBuilder
{
    private readonly ServiceRegistrations _registrations;

    internal DbspServiceBuilder(IServiceCollection services, ServiceRegistrations registrations)
    {
        Services = services;
        _registrations = registrations;
    }

    public IServiceCollection Services { get; }

    /// <summary>A lone in-memory node: the default.</summary>
    public DbspServiceBuilder UseInMemoryBackend(InMemoryBackendOptions? options = null, string nodeId = "node") =>
        UseBackend(_ => new InMemoryServiceBackend(options, nodeId));

    /// <summary>One node of an in-memory cluster shared with other nodes in this process.</summary>
    public DbspServiceBuilder UseInMemoryBackend(InMemoryServiceCluster cluster, string nodeId)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        return UseBackend(_ => cluster.Node(nodeId));
    }

    public DbspServiceBuilder UseBackend(Func<IServiceProvider, IServiceBackend> backend)
    {
        _registrations.Backend = backend ?? throw new ArgumentNullException(nameof(backend));
        return this;
    }

    /// <summary>
    /// The Kafka backend: this node is one of an active and its standby on one
    /// <see cref="Dbsp.Kafka.KafkaLog"/>. Both nodes are configured alike except for
    /// <see cref="Dbsp.Kafka.KafkaLogOptions.NodeId"/> and the probe endpoints. The
    /// log's own cluster settings are also registered as the Kafka connection <c>log</c>,
    /// which a spec may name, and whose sinks may ask for <c>"producer": "log"</c>.
    /// </summary>
    public DbspServiceBuilder UseKafkaBackend(Func<IServiceProvider, KafkaServiceBackendOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        KafkaServiceBackendOptions? resolved = null;
        KafkaServiceBackend? backend = null;
        var gate = new object();
        KafkaServiceBackendOptions Options(IServiceProvider services)
        {
            lock (gate)
                return resolved ??= options(services) ?? throw new InvalidOperationException("The Kafka backend's options factory returned null.");
        }

        _registrations.ReservedTopicPrefix = services => Options(services).Log.TopicPrefix;
        _registrations.KafkaConnections["log"] = services =>
        {
            var log = Options(services).Log;
            var connection = new KafkaConnection
            {
                BootstrapServers = log.BootstrapServers,
                ConfigureConsumer = log.ConfigureConsumer,
                ConfigureProducer = log.ConfigureProducer,
                Log = () => Volatile.Read(ref backend) is { } connected ? connected.Log : null,
            };
            foreach (var (key, value) in log.ClientConfig)
                connection.ClientConfig[key] = value;
            return connection;
        };
        return UseBackend(services =>
        {
            var created = new KafkaServiceBackend(Options(services), services.GetService<Microsoft.Extensions.Logging.ILoggerFactory>());
            Volatile.Write(ref backend, created);
            return created;
        });
    }

    /// <inheritdoc cref="UseKafkaBackend(Func{IServiceProvider, KafkaServiceBackendOptions})"/>
    public DbspServiceBuilder UseKafkaBackend(KafkaServiceBackendOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return UseKafkaBackend(_ => options);
    }

    /// <summary>A Kafka cluster a spec may name as its connection: bootstrap servers,
    /// client settings and the builder callbacks stay with the host; the spec carries only
    /// the name and a topic.</summary>
    public DbspServiceBuilder AddKafkaConnection(string name, Func<IServiceProvider, KafkaConnection> connection)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        _registrations.KafkaConnections[name] = connection ?? throw new ArgumentNullException(nameof(connection));
        return this;
    }

    /// <summary>
    /// A record format of the <c>kafka</c> connector and the <c>kafka</c> table loader,
    /// which a slot or a loader chooses with <c>"format": "<paramref name="name"/>"</c>:
    /// the seam for what the owner's topics carry (Avro, Protobuf, a schema registry's
    /// framing). JSON is built in as <c>json</c>, the default, and Debezium's change events
    /// as <c>debezium</c>; neither name can be taken. The factory runs once per process,
    /// when a slot first names the format; see <see cref="IRecordFormat"/> for what a format
    /// may keep.
    /// </summary>
    public DbspServiceBuilder AddRecordFormat(string name, Func<IServiceProvider, IRecordFormat> format)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(format);
        if (name is KafkaSlotConfig.JsonFormat or KafkaSlotConfig.DebeziumFormat)
            throw new ArgumentException($"'{name}' is a built-in format of the kafka connector.", nameof(name));
        if (!ServiceNames.IsValid(name))
            throw new ArgumentException($"'{name}' is not a valid format name: it must match [a-z0-9][a-z0-9._-]{{0,62}}.", nameof(name));
        _registrations.RecordFormats[name] = format;
        return this;
    }

    /// <summary>A rooted store of files or objects that a spec may name as its connection.</summary>
    public DbspServiceBuilder AddObjectStore(string name, Func<IServiceProvider, IObjectStore> store)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        _registrations.ObjectStores[name] = store ?? throw new ArgumentNullException(nameof(store));
        return this;
    }

    /// <summary>A contract assembly compiled into the host: table definitions and the row
    /// types of shared tables. Plugins that reference it get this copy.</summary>
    public DbspServiceBuilder AddContracts(Assembly assembly)
    {
        if (!_registrations.Contracts.Contains(assembly ?? throw new ArgumentNullException(nameof(assembly))))
            _registrations.Contracts.Add(assembly);
        return this;
    }

    /// <summary>A pipeline assembly compiled into the host. Its plugin name is the
    /// assembly's simple name in lower case; it is never unloaded.</summary>
    public DbspServiceBuilder AddPlugin(Assembly assembly)
    {
        if (!_registrations.Plugins.Contains(assembly ?? throw new ArgumentNullException(nameof(assembly))))
            _registrations.Plugins.Add(assembly);
        return this;
    }

    /// <summary>A seed: written to the catalog by the first leader if no table of that
    /// name is declared. After that the catalog wins.</summary>
    public DbspServiceBuilder AddTable(ITableDefinition definition, TableLoaderSpec? loader = null, string? name = null,
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
    public DbspServiceBuilder AddTable(TableSpec spec)
    {
        _registrations.Tables.Add(spec ?? throw new ArgumentNullException(nameof(spec)));
        return this;
    }

    /// <summary>A seed: deployed by the first leader if no pipeline of that id exists.</summary>
    public DbspServiceBuilder AddPipeline(PipelineSpec spec)
    {
        _registrations.Pipelines.Add(spec ?? throw new ArgumentNullException(nameof(spec)));
        return this;
    }

    /// <summary>A custom table loader kind.</summary>
    public DbspServiceBuilder AddTableSource(string kind, Func<IServiceProvider, ITableSourceFactory> factory)
    {
        ArgumentException.ThrowIfNullOrEmpty(kind);
        _registrations.TableSources[kind] = factory ?? throw new ArgumentNullException(nameof(factory));
        return this;
    }

    /// <summary>A source connector: it implements <see cref="IAppendSourceConnector"/>,
    /// <see cref="IKeyedSourceConnector"/> or both.</summary>
    public DbspServiceBuilder AddSourceConnector(string name, Func<IServiceProvider, IConnector> connector) => AddConnector(name, connector);

    /// <summary>A sink connector: it implements <see cref="IUpsertSinkConnector"/>,
    /// <see cref="IAppendSinkConnector"/> or both.</summary>
    public DbspServiceBuilder AddSinkConnector(string name, Func<IServiceProvider, IConnector> connector) => AddConnector(name, connector);

    private DbspServiceBuilder AddConnector(string name, Func<IServiceProvider, IConnector> connector)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        _registrations.Connectors[name] = connector ?? throw new ArgumentNullException(nameof(connector));
        return this;
    }
}
