using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Confluent.Kafka;
using Incremental.Kafka;
using Incremental.Tables;
using Microsoft.Extensions.Logging;

namespace Incremental.Service;

/// <summary>
/// A Kafka cluster as the host knows it, registered once under a name
/// (<c>AddKafkaConnection</c>). Slot and loader configuration is JSON in the catalog,
/// written by tenants and returned by the admin API, so it cannot hold credentials, cannot
/// name arbitrary hosts and cannot express a callback such as MSK's IAM token refresh:
/// those live here, and a spec names the connection.
/// </summary>
public sealed class KafkaConnection
{
    public required string BootstrapServers { get; init; }

    /// <summary>librdkafka settings applied to every client made from this connection
    /// (<c>security.protocol</c>, <c>sasl.mechanism</c>, TLS).</summary>
    public IDictionary<string, string> ClientConfig { get; } = new Dictionary<string, string>();

    /// <summary>Access to each consumer's builder: where an OAUTHBEARER token refresh
    /// handler goes.</summary>
    public Action<ConsumerBuilder<byte[], byte[]>>? ConfigureConsumer { get; init; }

    /// <inheritdoc cref="ConfigureConsumer"/>
    public Action<ProducerBuilder<byte[], byte[]>>? ConfigureProducer { get; init; }

    /// <summary>A spec may name only topics that start with one of these. Empty: any topic.</summary>
    public IList<string> AllowedTopicPrefixes { get; } = [];

    /// <summary>The backend's log, for the connection the Kafka backend registers as
    /// <c>log</c>: a sink bound with <c>"producer": "log"</c> writes through its fenced
    /// transactional producer.</summary>
    internal Func<KafkaLog?>? Log { get; init; }

    /// <summary>The topic prefix of the service's own log on this deployment. Its topics —
    /// the state, the leader topic, the table logs, the blobs — are never a slot's or a
    /// loader's to read or write, whatever connection is named.</summary>
    internal string? ReservedPrefix { get; set; }

    internal void RequireTopic(string topic)
    {
        if (AllowedTopicPrefixes.Count > 0 && !AllowedTopicPrefixes.Any(prefix => topic.StartsWith(prefix, StringComparison.Ordinal)))
            throw new InvalidOperationException(
                $"topic '{topic}' is outside what this connection allows (prefixes: {string.Join(", ", AllowedTopicPrefixes)})");
        if (ReservedPrefix is { } prefix
            && (topic == prefix + ".state" || topic == prefix + ".leader"
                || topic.StartsWith(prefix + ".table.", StringComparison.Ordinal) || topic.StartsWith(prefix + ".svc.", StringComparison.Ordinal)))
            throw new InvalidOperationException($"topic '{topic}' belongs to the service's own log and cannot be bound");
    }

    internal IReadOnlyDictionary<string, string> Config => new Dictionary<string, string>(ClientConfig);
}

/// <summary>The Kafka connections the host registered, by name. Resolved from DI; a custom
/// connector or table source that needs the same cluster settings asks it.</summary>
public sealed class KafkaConnections
{
    private readonly IServiceProvider _services;
    private readonly ServiceRegistrations _registrations;
    private readonly Dictionary<string, KafkaConnection> _resolved = new(StringComparer.Ordinal);

    internal KafkaConnections(IServiceProvider services, ServiceRegistrations registrations)
    {
        _services = services;
        _registrations = registrations;
    }

    public IReadOnlyCollection<string> Names => _registrations.KafkaConnections.Keys;

    /// <summary>The connection of that name. Throws, listing the registered names, when
    /// there is none.</summary>
    public KafkaConnection Get(string? name)
    {
        if (string.IsNullOrEmpty(name))
            throw new InvalidOperationException(
                $"name a Kafka connection: \"connection\": \"…\" (registered: {string.Join(", ", Names.Order())})");
        lock (_resolved)
        {
            if (_resolved.TryGetValue(name, out var connection))
                return connection;
            if (!_registrations.KafkaConnections.TryGetValue(name, out var factory))
                throw new InvalidOperationException(
                    $"no Kafka connection named '{name}' is registered (registered: {string.Join(", ", Names.Order())})");
            var created = factory(_services) ?? throw new InvalidOperationException($"the factory of Kafka connection '{name}' returned null");
            created.ReservedPrefix = _registrations.ReservedTopicPrefix?.Invoke(_services);
            return _resolved[name] = created;
        }
    }
}

/// <summary>What a slot bound to the <c>kafka</c> connector, or a <c>kafka</c> or
/// <c>debezium</c> table loader, may say. Nothing else is accepted: bootstrap servers and
/// client properties belong to the named connection.</summary>
internal sealed record KafkaSlotConfig
{
    public string? Connector { get; init; }
    public string? Connection { get; init; }
    public string? Topic { get; init; }

    /// <summary><c>earliest</c> (the default) or <c>latest</c>: where to start with no
    /// committed position.</summary>
    public string? StartFrom { get; init; }

    /// <summary><c>json</c> (the default) or <c>debezium</c>. Sources only.</summary>
    public string? Format { get; init; }

    /// <summary><c>own</c> (the default: an idempotent producer of the sink's own) or
    /// <c>log</c>: the backend's transactional producer. Sinks only.</summary>
    public string? Producer { get; init; }

    /// <summary>Table loaders only: reload the table from the start of the topic when its
    /// committed position has been deleted from under it.</summary>
    public bool? ReloadWhenPositionLost { get; init; }

    private static readonly JsonSerializerOptions Strict = new(JsonSerializerDefaults.Web)
    {
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
    };

    public static KafkaSlotConfig Parse(JsonElement config)
    {
        if (config.ValueKind != JsonValueKind.Object)
            throw new InvalidOperationException("the kafka config must be a JSON object");
        KafkaSlotConfig parsed;
        try
        {
            parsed = config.Deserialize<KafkaSlotConfig>(Strict)!;
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException(
                $"the kafka config accepts connector, connection, topic, startFrom, format, producer and reloadWhenPositionLost — bootstrap " +
                $"servers and client properties belong to the named connection ({ex.Message})");
        }
        if (string.IsNullOrEmpty(parsed.Topic))
            throw new InvalidOperationException("the kafka config needs a \"topic\"");
        if (parsed.StartFrom is not (null or "earliest" or "latest"))
            throw new InvalidOperationException($"startFrom is 'earliest' or 'latest', not '{parsed.StartFrom}'");
        if (parsed.Format is not (null or "json" or "debezium"))
            throw new InvalidOperationException($"format is 'json' or 'debezium', not '{parsed.Format}'");
        if (parsed.Producer is not (null or "own" or "log"))
            throw new InvalidOperationException($"producer is 'own' or 'log', not '{parsed.Producer}'");
        return parsed;
    }

    public bool IsDebezium => Format == "debezium";

    public KafkaSourceOptions Source(KafkaConnection connection, ILogger logger, Action<KafkaRecord, Exception>? onUnreadableTableRecord = null) => new()
    {
        OnUnreadableTableRecord = onUnreadableTableRecord,
        BootstrapServers = connection.BootstrapServers,
        Topic = Topic!,
        StartFrom = StartFrom == "latest" ? KafkaSourceStart.Latest : KafkaSourceStart.Earliest,
        ClientConfig = connection.Config,
        ConfigureConsumer = connection.ConfigureConsumer,
        ReloadTableWhenPositionLost = ReloadWhenPositionLost ?? false,
        Logger = logger,
    };

    /// <summary>Debezium's reading of JSON (snake_case columns, numbers as text) with a
    /// metadata cache of its own, so the types it has seen go when it does.</summary>
    public static JsonSerializerOptions DebeziumJson() =>
        new(Debezium.DefaultJson) { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };
}

/// <summary>
/// The <c>kafka</c> connector: a topic as an append input (each record an insertion), as a
/// keyed input (a value upserts, a tombstone deletes; or Debezium's change events with
/// <c>"format": "debezium"</c>), as an upsert sink (a compacted topic) and as an append
/// sink. Config: <c>{ "connector": "kafka", "connection": "msk-main", "topic": "…" }</c>.
/// Sources resume from the circuit's committed offsets; sinks stamp the step and the
/// leader term on what they write.
/// </summary>
public sealed class KafkaConnector(KafkaConnections connections)
    : IAppendSourceConnector, IKeyedSourceConnector, IUpsertSinkConnector, IAppendSinkConnector
{
    private (KafkaSlotConfig Config, KafkaConnection Connection) Resolve(JsonElement json)
    {
        var config = KafkaSlotConfig.Parse(json);
        var connection = connections.Get(config.Connection);
        connection.RequireTopic(config.Topic!);
        if (config.Producer == "log" && connection.Log is null)
            throw new InvalidOperationException(
                "\"producer\": \"log\" needs the connection the Kafka backend registers as 'log': only that one has the transactional producer");
        return (config, connection);
    }

    public ConnectorDescription Describe(JsonElement config)
    {
        var (parsed, _) = Resolve(config);
        return new ConnectorDescription($"kafka topic {parsed.Topic} on connection {parsed.Connection}");
    }

    /// <summary>What <see cref="Describe"/> cannot see: whether the config suits the kind
    /// of slot it is bound to. Checked at the dry run.</summary>
    internal void RequireKind(JsonElement json, SlotKind kind)
    {
        var config = KafkaSlotConfig.Parse(json);
        var isSource = kind is SlotKind.AppendInput or SlotKind.KeyedInput;
        if (config.IsDebezium && kind != SlotKind.KeyedInput)
            throw new InvalidOperationException(
                "\"format\": \"debezium\" needs a keyed input (KeyedInput<TKey, TValue>): a change topic carries updates and deletes, " +
                "which an append input or a sink cannot express");
        if (isSource && config.Producer is not null)
            throw new InvalidOperationException("\"producer\" is a sink's setting");
        if (!isSource && (config.StartFrom is not null || config.ReloadWhenPositionLost is not null))
            throw new InvalidOperationException("\"startFrom\" and \"reloadWhenPositionLost\" are a source's settings");
    }

    async Task<IRunningSource> IAppendSourceConnector.StartAsync<T>(SourceContext<T> context, CancellationToken cancellationToken)
    {
        var (config, connection) = Resolve(context.Config);
        var source = await KafkaSource.StartAsync(
            context.Circuit, context.Stream, config.Source(connection, context.Logger), KafkaDeserializers.Json<T>(context.Json), cancellationToken)
            .ConfigureAwait(false);
        return new RunningKafkaSource(source);
    }

    async Task<IRunningSource> IKeyedSourceConnector.StartAsync<TKey, TValue>(KeyedSourceContext<TKey, TValue> context, CancellationToken cancellationToken)
    {
        var (config, connection) = Resolve(context.Config);
        var decoder = config.IsDebezium
            ? Debezium.Changes<TKey, TValue>(KafkaSlotConfig.DebeziumJson())
            : KafkaDeserializers.Keyed(KafkaDeserializers.Json<TKey>(context.Json), KafkaDeserializers.Json<TValue>(context.Json));
        var source = await KafkaSource.StartAsync(
            context.Circuit, context.Stream, config.Source(connection, context.Logger), decoder, cancellationToken).ConfigureAwait(false);
        return new RunningKafkaSource(source);
    }

    private static KafkaSinkOptions Sink(KafkaSlotConfig config, KafkaConnection connection, SinkContext context)
    {
        var log = config.Producer == "log" ? connection.Log!() ?? throw new InvalidOperationException("The Kafka backend is not connected.") : null;
        return new KafkaSinkOptions
        {
            Topic = config.Topic!,
            Log = log,
            BootstrapServers = log is null ? connection.BootstrapServers : null,
            Term = () => context.Term,
            ClientConfig = connection.Config,
            ConfigureProducer = connection.ConfigureProducer,
            Logger = context.Logger,
        };
    }

    IUpsertSink<TKey, TRow> IUpsertSinkConnector.Create<TKey, TRow>(SinkContext context)
    {
        var (config, connection) = Resolve(context.Config);
        return new KafkaUpsertSink<TKey, TRow>(
            Sink(config, connection, context), KafkaSerializers.Json<TKey>(context.Json), KafkaSerializers.Json<TRow>(context.Json));
    }

    IAppendSink<T> IAppendSinkConnector.Create<T>(SinkContext context)
    {
        var (config, connection) = Resolve(context.Config);
        return new KafkaAppendSink<T>(Sink(config, connection, context), KafkaSerializers.Json<T>(context.Json));
    }

    private sealed class RunningKafkaSource : IRunningSource
    {
        private readonly KafkaSource _source;
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _sampler;
        private long _lag = -1;
        private long _pausedSinceTicks;

        public RunningKafkaSource(KafkaSource source)
        {
            _source = source;
            _sampler = Task.Run(SampleAsync);
        }

        public Task Completion => _source.Completion;

        public SourceStats Stats => new(
            _source.RecordsRead, _source.RecordsUnreadable, _source.Positions,
            Interlocked.Read(ref _lag) is >= 0 and var lag ? lag : null,
            Interlocked.Read(ref _pausedSinceTicks) is > 0 and var ticks ? new DateTimeOffset(ticks, TimeSpan.Zero) : null);

        // Lag is one offsets request; pause is the source's own flag, given a since.
        private async Task SampleAsync()
        {
            var token = _stop.Token;
            var nextLag = 0L;
            try
            {
                while (true)
                {
                    if (_source.IsPaused)
                        Interlocked.CompareExchange(ref _pausedSinceTicks, DateTimeOffset.UtcNow.UtcTicks, 0);
                    else
                        Interlocked.Exchange(ref _pausedSinceTicks, 0);
                    if (Environment.TickCount64 >= nextLag)
                    {
                        nextLag = Environment.TickCount64 + 5000;
                        try
                        {
                            Interlocked.Exchange(ref _lag, (await _source.MeasureLagAsync(token).ConfigureAwait(false)).Total);
                        }
                        catch (Exception ex) when (ex is not OperationCanceledException)
                        {
                            Interlocked.Exchange(ref _lag, -1); // unknown is an honest answer
                        }
                    }
                    await Task.Delay(500, token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        public async ValueTask DisposeAsync()
        {
            _stop.Cancel();
            await _sampler.ConfigureAwait(false);
            await _source.DisposeAsync().ConfigureAwait(false);
            _stop.Dispose();
        }
    }
}

/// <summary>
/// The <c>kafka</c> and <c>debezium</c> table loaders: <see cref="KafkaSource.StartTableLoaderAsync"/>
/// over a named connection, reading a keyed topic (a value upserts, a tombstone deletes)
/// or a Debezium change topic. It stamps each change with its partition and offset and
/// resumes from the table's committed offsets, so a takeover continues where the last
/// committed version stood. Config: <c>{ "topic": "…", "startFrom": "earliest" }</c>.
/// </summary>
internal sealed class KafkaTableSource<TKey, TRow>(KafkaConnections connections, TableLoaderSpec loader, bool debezium) : ITableSource<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    // Rows are contract types, which are never unloaded, so process-wide options serve.
    internal static KafkaSlotConfig Check(KafkaConnections connections, TableLoaderSpec loader)
    {
        var config = KafkaSlotConfig.Parse(loader.Config ?? default);
        if (config.Connector is not null || config.Connection is not null || config.Format is not null || config.Producer is not null)
            throw new InvalidOperationException(
                "a table loader's config accepts topic, startFrom and reloadWhenPositionLost; the connection is the loader's own \"connection\"");
        connections.Get(loader.Connection).RequireTopic(config.Topic!);
        return config;
    }

    public async Task RunAsync(TableSourceContext<TKey, TRow> context, CancellationToken cancellationToken)
    {
        var config = Check(connections, loader);
        var connection = connections.Get(loader.Connection);
        // An unreadable record is counted and kept in the table's ring; under the Fault
        // policy the call throws, which stops the loader at that record.
        var options = config.Source(connection, context.Logger, (record, error) =>
            context.Unreadable($"{record.Topic}[{record.Partition}]@{record.Offset}", error, record.Value));
        var decoder = debezium ? Debezium.Changes<TKey, TRow>() : KafkaDeserializers.Keyed<TKey, TRow>();

        context.Report(TableSourceState.Starting, options.Topic);
        await using var source = await KafkaSource.StartTableLoaderAsync(context.Table, options, decoder, cancellationToken).ConfigureAwait(false);
        var nextLag = 0L;
        long? lag = null;
        while (true)
        {
            if (Environment.TickCount64 >= nextLag)
            {
                nextLag = Environment.TickCount64 + 2000;
                try
                {
                    lag = (await source.MeasureLagAsync(cancellationToken).ConfigureAwait(false)).Total;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    lag = null;
                }
            }
            var positions = source.Positions;
            context.Report(
                lag is 0 ? TableSourceState.Idle : TableSourceState.Streaming,
                positions.Count == 0 ? options.Topic : $"{options.Topic} " + string.Join(",", positions.OrderBy(p => p.Key).Select(p => $"[{p.Key}]@{p.Value}")),
                lag);
            var finished = await Task.WhenAny(source.Completion, Task.Delay(250, cancellationToken)).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            if (finished == source.Completion)
            {
                await source.Completion.ConfigureAwait(false); // throws the source's fault
                return;
            }
        }
    }
}
