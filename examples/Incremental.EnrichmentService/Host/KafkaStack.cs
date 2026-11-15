using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Enrichment.Contracts;
using Incremental.Kafka;
using Npgsql;

namespace Incremental.EnrichmentService;

/// <summary>
/// The Kafka mode's surroundings on the local connector stack
/// (<c>docker compose --profile connectors up -d</c>): reference data in three Postgres
/// tables, a Debezium connector that turns their changes into topics, a trades topic and
/// the sink topics. Everything is named after one prefix, and <see cref="EnsureAsync"/>
/// can be run any number of times.
/// </summary>
public sealed partial class KafkaStack : IAsyncDisposable
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly ExampleOptions _options;
    private readonly NpgsqlDataSource _database;
    private IProducer<byte[], byte[]>? _producer;

    public KafkaStack(ExampleOptions options)
    {
        if (!PrefixPattern().IsMatch(options.Prefix))
            throw new ArgumentException("--prefix is lower-case letters and digits, starting with a letter: it names topics and Postgres tables.");
        _options = options;
        _database = NpgsqlDataSource.Create(options.Postgres);
    }

    [GeneratedRegex("^[a-z][a-z0-9]{0,30}$")]
    private static partial Regex PrefixPattern();

    public string Bootstrap => _options.Bootstrap;
    public string Prefix => _options.Prefix;
    public string TradesTopic => $"{Prefix}.trades";
    public string QuotesTopic => $"{Prefix}.quotes";
    public string DeadLetterTopic(string pipeline) => $"{Prefix}.dead.{pipeline}";
    public string SinkTopic(string name) => $"{Prefix}.out.{name}";
    public string Table(string name) => $"{Prefix}_{name}";
    public string ChangeTopic(string table) => DebeziumPostgres.Topic(Prefix, "public", Table(table));
    private string Connector => $"{Prefix}-refdata";
    private string[] SinkTopics => [SinkTopic("trade-enrichment"), SinkTopic("large-trades"), SinkTopic("desk-exposure"), SinkTopic("execution-quality")];
    private string[] PlainTopics => [TradesTopic, QuotesTopic, DeadLetterTopic("sanctions-alerts"), DeadLetterTopic("credit-alerts")];

    private DebeziumPostgresOptions Debezium
    {
        get
        {
            var database = new NpgsqlConnectionStringBuilder(_options.Postgres);
            return new DebeziumPostgresOptions
            {
                Hostname = _options.PostgresHostForConnect,
                Database = database.Database!,
                User = database.Username!,
                Password = database.Password!,
                TopicPrefix = Prefix,
                Tables = [$"public.{Table("instruments")}", $"public.{Table("traders")}", $"public.{Table("desks")}"],
                TopicPartitions = 1,
                TopicReplicationFactor = 1,
            };
        }
    }

    public async Task SqlAsync(string sql)
    {
        await using var command = _database.CreateCommand(sql);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<T?> ScalarAsync<T>(string sql)
    {
        await using var command = _database.CreateCommand(sql);
        return (T?)await command.ExecuteScalarAsync();
    }

    /// <summary>Creates what is missing: the Postgres tables and their rows, the Debezium
    /// connector, the trades topic and the compacted sink topics.</summary>
    public async Task EnsureAsync(WorldShape shape)
    {
        await SqlAsync($"create table if not exists {Table("instruments")} (id int primary key, symbol text not null, isin text not null, asset_class text not null, multiplier numeric(10,2) not null)");
        await SqlAsync($"create table if not exists {Table("traders")} (id int primary key, name text not null, desk_id int not null)");
        await SqlAsync($"create table if not exists {Table("desks")} (id int primary key, name text not null, region text not null)");

        static string Text(string value) => "'" + value.Replace("'", "''") + "'";
        foreach (var chunk in Enumerable.Range(1, shape.Instruments).Select(i => shape.Instrument(i)).Chunk(500))
        {
            await SqlAsync($"insert into {Table("instruments")} values " + string.Join(",", chunk.Select(i =>
                $"({i.Id},{Text(i.Symbol)},{Text(i.Isin)},{Text(i.AssetClass)},{i.Multiplier.ToString(CultureInfo.InvariantCulture)})")) + " on conflict (id) do nothing");
        }
        await SqlAsync($"insert into {Table("desks")} values " + string.Join(",", Enumerable.Range(1, shape.Desks).Select(shape.Desk).Select(d =>
            $"({d.Id},{Text(d.Name)},{Text(d.Region)})")) + " on conflict (id) do nothing");
        await SqlAsync($"insert into {Table("traders")} values " + string.Join(",", Enumerable.Range(1, shape.Traders).Select(shape.Trader).Select(t =>
            $"({t.Id},{Text(t.Name)},{t.DeskId})")) + " on conflict (id) do nothing");

        using var connect = new KafkaConnectClient(new Uri(_options.Connect));
        await connect.PutConnectorAsync(Connector, DebeziumPostgres.ConnectorConfig(Debezium));
        await connect.WaitUntilRunningAsync(Connector, TimeSpan.FromSeconds(60));

        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap }).Build();
        // The two feeds have several partitions, as a production topic has: a pipeline's
        // watermark is then the least over a feed's partitions (event time is declared
        // on the inputs), and a takeover's backlog, read a partition at a time, is not
        // late. Records are placed by key — a trade's id, a quote's instrument.
        var topics = PlainTopics.Select(topic => new TopicSpecification
        {
            Name = topic, NumPartitions = topic == TradesTopic || topic == QuotesTopic ? FeedPartitions : 1, ReplicationFactor = 1,
        }).ToList();
        topics.AddRange(SinkTopics.Select(topic => new TopicSpecification
        {
            Name = topic, NumPartitions = 1, ReplicationFactor = 1,
            Configs = new Dictionary<string, string> { ["cleanup.policy"] = "compact" },     // an upsert sink's topic keeps the last row per key
        }));
        try
        {
            await admin.CreateTopicsAsync(topics);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code is ErrorCode.NoError or ErrorCode.TopicAlreadyExists))
        {
        }

        // The change topics appear when Debezium has snapshotted the tables.
        var wanted = new[] { ChangeTopic("instruments"), ChangeTopic("traders"), ChangeTopic("desks") };
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (true)
        {
            var existing = admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics.Select(t => t.Topic).ToHashSet();
            if (wanted.All(existing.Contains))
                break;
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"Debezium did not create the change topics ({string.Join(", ", wanted.Where(t => !existing.Contains(t)))}) within 60 s.");
            await Task.Delay(250);
        }
    }

    // ── the trades topic ───────────────────────────────────────────────────────────────

    private IProducer<byte[], byte[]> Producer => _producer ??= new ProducerBuilder<byte[], byte[]>(
        new ProducerConfig { BootstrapServers = Bootstrap, LingerMs = 5, EnableIdempotence = true }).Build();

    /// <summary>How many partitions the trades and quotes topics have.</summary>
    public const int FeedPartitions = 3;

    /// <summary>How many trades the topic holds: its producers write no transactions, so
    /// every offset of every partition is a record, and with one producer at a time the
    /// feed's first <c>n</c> records are what the partitions hold between them.</summary>
    public long TradesPublished() => Published(TradesTopic);

    private long Published(string topic)
    {
        using var consumer = new ConsumerBuilder<byte[], byte[]>(
            new ConsumerConfig { BootstrapServers = Bootstrap, GroupId = "enrichment-example-probe", EnableAutoCommit = false }).Build();
        using var admin = new DependentAdminClientBuilder(consumer.Handle).Build();
        var partitions = admin.GetMetadata(topic, TimeSpan.FromSeconds(10)).Topics.Single().Partitions.Count;
        long total = 0;
        for (var partition = 0; partition < partitions; partition++)
            total += consumer.QueryWatermarkOffsets(new TopicPartition(topic, partition), TimeSpan.FromSeconds(10)).High.Value;
        return total;
    }

    /// <summary>Appends the feed's records <c>from … from + count - 1</c>: keyed by trade
    /// id, the value the trade as JSON.</summary>
    public async Task ProduceTradesAsync(WorldShape shape, long from, long count)
    {
        for (var offset = from; offset < from + count; offset++)
        {
            var trade = shape.TradeAt(offset);
            Producer.Produce(TradesTopic, new Message<byte[], byte[]>
            {
                Key = JsonSerializer.SerializeToUtf8Bytes(trade.Id, Json),
                Value = JsonSerializer.SerializeToUtf8Bytes(trade, Json),
            });
        }
        await Task.Run(() => Producer.Flush(TimeSpan.FromSeconds(30)));
    }

    /// <summary>How many quotes the quotes topic holds.</summary>
    public long QuotesPublished() => Published(QuotesTopic);

    /// <summary>Appends quotes to the quotes topic, keyed by instrument.</summary>
    public async Task ProduceQuotesAsync(IEnumerable<Quote> quotes)
    {
        foreach (var quote in quotes)
        {
            Producer.Produce(QuotesTopic, new Message<byte[], byte[]>
            {
                Key = JsonSerializer.SerializeToUtf8Bytes(quote.InstrumentId, Json),
                Value = JsonSerializer.SerializeToUtf8Bytes(quote, Json),
            });
        }
        await Task.Run(() => Producer.Flush(TimeSpan.FromSeconds(30)));
    }

    /// <summary>Every value on a topic, in order: what a consumer of an append sink's
    /// topic — the dead letters — reads.</summary>
    public List<string> ReadValues(string topic)
    {
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = Bootstrap, GroupId = "enrichment-example-reader", EnableAutoCommit = false, IsolationLevel = IsolationLevel.ReadCommitted,
        }).Build();
        var partition = new TopicPartition(topic, 0);
        var end = consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10)).High.Value;
        var values = new List<string>();
        if (end == 0)
            return values;
        consumer.Assign(new TopicPartitionOffset(partition, Offset.Beginning));
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (consumer.Position(partition) is var position && (position == Offset.Unset || position.Value < end))
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"Reading {topic} to offset {end} took more than 30 s.");
            if (consumer.Consume(TimeSpan.FromMilliseconds(500))?.Message is { Value: not null } message)
                values.Add(Encoding.UTF8.GetString(message.Value));
        }
        return values;
    }

    /// <summary>A compacted topic read to its end and folded: key → latest value, a
    /// tombstone removing the key. What a consumer of an upsert sink's topic ends up with.</summary>
    public Dictionary<string, string> ReadUpserts(string topic)
    {
        using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = Bootstrap, GroupId = "enrichment-example-reader", EnableAutoCommit = false, IsolationLevel = IsolationLevel.ReadCommitted,
        }).Build();
        var partition = new TopicPartition(topic, 0);
        var end = consumer.QueryWatermarkOffsets(partition, TimeSpan.FromSeconds(10)).High.Value;
        var rows = new Dictionary<string, string>(StringComparer.Ordinal);
        if (end == 0)
            return rows;
        consumer.Assign(new TopicPartitionOffset(partition, Offset.Beginning));
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (consumer.Position(partition) is var position && (position == Offset.Unset || position.Value < end))
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"Reading {topic} to offset {end} took more than 30 s.");
            var record = consumer.Consume(TimeSpan.FromMilliseconds(500));
            if (record?.Message is not { } message)
                continue;
            var key = Encoding.UTF8.GetString(message.Key);
            if (message.Value is null)
                rows.Remove(key);
            else
                rows[key] = Encoding.UTF8.GetString(message.Value);
        }
        return rows;
    }

    /// <summary>Removes everything <see cref="EnsureAsync"/> made and every topic under the
    /// prefix. Best effort: a smoke run's names are its own.</summary>
    public async Task CleanUpAsync()
    {
        try
        {
            // Connect keeps a deleted connector's offsets. Left there, the connector this
            // prefix gets next time would find its snapshot already "done", take none, and
            // never create the change topics: so the offsets go before the connector does.
            using var connect = new KafkaConnectClient(new Uri(_options.Connect));
            await connect.ResetOffsetsAsync(Connector);
            await connect.DeleteConnectorAsync(Connector);
            var config = DebeziumPostgres.ConnectorConfig(Debezium);
            for (var attempt = 0; attempt < 100; attempt++)
            {
                try
                {
                    await SqlAsync($"select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = '{config["slot.name"]}'");
                    break;
                }
                catch (PostgresException)
                {
                    await Task.Delay(100);      // the slot is released a moment after its connector stops
                }
            }
            if (config.TryGetValue("publication.name", out var publication))
                await SqlAsync($"drop publication if exists {publication}");
            foreach (var table in new[] { "instruments", "traders", "desks" })
                await SqlAsync($"drop table if exists {Table(table)}");

            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap }).Build();
            var topics = admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics.Select(t => t.Topic)
                .Where(t => t.StartsWith(Prefix + ".", StringComparison.Ordinal)).ToList();
            if (topics.Count > 0)
                await admin.DeleteTopicsAsync(topics);
        }
        catch (Exception ex) when (ex is NpgsqlException or KafkaConnectException or HttpRequestException or KafkaException)
        {
            Console.WriteLine($"  (clean-up was incomplete: {ex.Message})");
        }
    }

    public async ValueTask DisposeAsync()
    {
        _producer?.Dispose();
        await _database.DisposeAsync();
    }
}
