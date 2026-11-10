using System.Net.Sockets;
using Confluent.Kafka;
using Incremental.Tables;
using Npgsql;

namespace Incremental.Kafka.Tests;

/// <summary>
/// Change data capture end to end, on the local stack: rows are written to Postgres,
/// Debezium (registered through <see cref="KafkaConnectClient"/>) turns them into a topic,
/// a table loader with <see cref="Debezium.Changes{TKey,TRow}"/> turns the topic into a
/// shared table, and a circuit joins a stream against it. The table must follow the
/// database through the snapshot, inserts, updates, deletes, a key change, a connector
/// restart and a second snapshot of rows it already has.
/// </summary>
public class DebeziumCdcTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Timeout = KafkaEnv.Timeout;
    private const string ConnectionString = "Host=localhost;Port=5432;Username=incremental;Password=incremental;Database=incremental";
    private static readonly Uri Connect = new("http://localhost:8083");

    private sealed record Customer(int Id, string Name, string Region, decimal Credit, DateTimeOffset UpdatedAt);
    private sealed record LineKey(int OrderId, int LineNo);
    private sealed record Line(int OrderId, int LineNo, string Sku, int Qty);
    private sealed record Order(int Id, int CustomerId);
    private sealed record Enriched(int OrderId, string Name, string Region);

    /// <summary>The database side of a test: its tables, its connector, and the clean-up.</summary>
    private sealed class Cdc : IAsyncDisposable
    {
        public required KafkaEnv Kafka;
        public required NpgsqlDataSource Database;
        public required KafkaConnectClient Client;
        public required string Customers;
        public required string Lines;
        public readonly List<string> Connectors = [];
        public string Slot => Kafka.Prefix + "_slot";

        public static async Task<Cdc> CreateAsync()
        {
            var kafka = KafkaEnv.Create();
            var client = new KafkaConnectClient(Connect);
            NpgsqlDataSource? database = null;
            try
            {
                using (var probe = new TcpClient())
                    await probe.ConnectAsync("localhost", 5432).WaitAsync(TimeSpan.FromSeconds(2));
                if (!await client.IsReachableAsync())
                    throw new SocketException();
                database = NpgsqlDataSource.Create(ConnectionString);
                await using var check = database.CreateCommand("select 1");
                await check.ExecuteScalarAsync();
            }
            catch (Exception ex) when (ex is SocketException or TimeoutException or NpgsqlException)
            {
                await kafka.DisposeAsync();
                Assert.Skip("Postgres (localhost:5432) or Kafka Connect (localhost:8083) is not reachable — run `docker compose --profile connectors up -d`.");
            }

            var cdc = new Cdc
            {
                Kafka = kafka,
                Database = database!,
                Client = client,
                Customers = "customers_" + kafka.Prefix,
                Lines = "lines_" + kafka.Prefix,
            };
            await cdc.ExecuteAsync(
                $"create table {cdc.Customers} (id int primary key, name text not null, region text not null, credit numeric(10,2) not null, updated_at timestamptz not null)");
            await cdc.ExecuteAsync(
                $"create table {cdc.Lines} (order_id int not null, line_no int not null, sku text not null, qty int not null, primary key (order_id, line_no))");
            return cdc;
        }

        public string Topic(string table) => DebeziumPostgres.Topic(Kafka.Prefix, "public", table);

        public async Task ExecuteAsync(string sql)
        {
            await using var command = Database.CreateCommand(sql);
            await command.ExecuteNonQueryAsync();
        }

        /// <summary>Registers a connector over the two tables: validated first, then
        /// created, then awaited until it runs.</summary>
        public async Task<string> RegisterAsync(string name, bool embedSchemas)
        {
            var config = DebeziumPostgres.ConnectorConfig(new DebeziumPostgresOptions
            {
                Hostname = "postgres",
                Database = "incremental",
                User = "incremental",
                Password = "incremental",
                TopicPrefix = Kafka.Prefix,
                Tables = [$"public.{Customers}", $"public.{Lines}"],
                EmbedSchemas = embedSchemas,
                TopicPartitions = 2,
                TopicReplicationFactor = 1,
            });
            Assert.Equal(Slot, config["slot.name"]);
            Assert.Empty(await Client.ValidateAsync(name, config));
            await Client.PutConnectorAsync(name, config);
            Connectors.Add(name);
            await Client.WaitUntilRunningAsync(name, Timeout);
            return name;
        }

        public async Task WaitForTopicAsync(string topic)
        {
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = KafkaEnv.Bootstrap }).Build();
            await KafkaEnv.EventuallyAsync(
                () => Task.FromResult(admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics.Any(t => t.Topic == topic && t.Partitions.Count > 0)),
                $"Debezium has created {topic}");
        }

        public async Task<Dictionary<int, Customer>> CustomersInDatabaseAsync()
        {
            var rows = new Dictionary<int, Customer>();
            await using var command = Database.CreateCommand($"select id, name, region, credit, updated_at from {Customers}");
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                rows[reader.GetInt32(0)] = new Customer(
                    reader.GetInt32(0), reader.GetString(1), reader.GetString(2), reader.GetDecimal(3),
                    new DateTimeOffset(reader.GetDateTime(4), TimeSpan.Zero));
            return rows;
        }

        public async Task<Dictionary<LineKey, Line>> LinesInDatabaseAsync()
        {
            var rows = new Dictionary<LineKey, Line>();
            await using var command = Database.CreateCommand($"select order_id, line_no, sku, qty from {Lines}");
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                rows[new LineKey(reader.GetInt32(0), reader.GetInt32(1))] =
                    new Line(reader.GetInt32(0), reader.GetInt32(1), reader.GetString(2), reader.GetInt32(3));
            return rows;
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                foreach (var connector in Connectors)
                    await Client.DeleteConnectorAsync(connector);
                // The slot is released a moment after its connector stops.
                for (var attempt = 0; attempt < 100; attempt++)
                {
                    try
                    {
                        await ExecuteAsync($"select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = '{Slot}'");
                        break;
                    }
                    catch (PostgresException)
                    {
                        await Task.Delay(100);
                    }
                }
                await ExecuteAsync($"drop publication if exists {Kafka.Prefix}_publication");
                await ExecuteAsync($"drop table if exists {Customers}, {Lines}");
            }
            finally
            {
                Client.Dispose();
                await Database.DisposeAsync();
                await Kafka.DisposeAsync();
            }
        }
    }

    private static Dictionary<int, Customer> Rows(SharedTable<int, Customer> table, int keys)
    {
        using var snapshot = table.Snapshot();
        var rows = new Dictionary<int, Customer>();
        for (var id = 0; id < keys; id++)
            if (snapshot.TryGet(id, out var row))
                rows[id] = row;
        return rows;
    }

    private static bool Same<TKey, TRow>(IReadOnlyDictionary<TKey, TRow> actual, IReadOnlyDictionary<TKey, TRow> expected) where TKey : notnull =>
        actual.Count == expected.Count && expected.All(e => actual.TryGetValue(e.Key, out var row) && Equals(row, e.Value));

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task APostgresTable_ThroughDebezium_BecomesASharedTable_ThatACircuitJoins(bool embedSchemas)
    {
        const int Keys = 200;
        await using var cdc = await Cdc.CreateAsync();
        var env = cdc.Kafka;

        // Rows that are there before the connector is: they arrive as the snapshot (op "r").
        await cdc.ExecuteAsync(
            $"insert into {cdc.Customers} select i, 'name-' || i, 'region-' || (i % 4), i * 1.25, now() from generate_series(0, 49) i");
        await cdc.ExecuteAsync($"insert into {cdc.Lines} select i / 3, i % 3, 'sku-' || i, i from generate_series(0, 29) i");
        var connector = await cdc.RegisterAsync(env.Prefix, embedSchemas);
        var customersTopic = cdc.Topic(cdc.Customers);
        var linesTopic = cdc.Topic(cdc.Lines);
        await cdc.WaitForTopicAsync(customersTopic);
        await cdc.WaitForTopicAsync(linesTopic);

        // Postgres → Debezium → Kafka → SharedTable.
        var customers = new SharedTable<int, Customer>("customers", c => c.Id);
        await using var loader = await KafkaSource.StartTableLoaderAsync(
            customers, env.Source(customersTopic), Debezium.Changes<int, Customer>()).WaitAsync(Timeout);
        async Task TableFollowsDatabaseAsync(string when)
        {
            var expected = await cdc.CustomersInDatabaseAsync();
            await KafkaEnv.EventuallyAsync(() => Task.FromResult(Same(Rows(customers, Keys), expected)), $"the table equals the database {when}");
        }
        await TableFollowsDatabaseAsync("after the snapshot");
        Assert.Equal(50, customers.Count);

        // … → Join and Lookup. Orders are fed by hand; customers come from the table.
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var orders = builder.KeyedInput<int, Order>("orders", o => o.Id);
        var reference = builder.Table(customers);
        var joined = orders.GroupBy(o => o.CustomerId)
            .Join(reference, (_, o, c) => new Enriched(o.Id, c.Name, c.Region)).ToAcknowledgedOutput();
        var looked = orders
            .Lookup(reference, o => o.CustomerId, (o, c) => new Enriched(o.Id, c.Name, c.Region), missing: o => new Enriched(o.Id, "?", "?"))
            .ToAcknowledgedOutput();
        // The composite-key table goes into the same circuit as a keyed input.
        var lines = builder.KeyedInput<LineKey, Line>("lines", l => new LineKey(l.OrderId, l.LineNo));
        var lineRows = lines.ToAcknowledgedOutput();
        await using var circuit = builder.Build();
        circuit.Start();
        var joinSink = new InMemoryUpsertSink<int, Enriched>();
        var lookupSink = new InMemoryUpsertSink<int, Enriched>();
        var lineSink = new InMemoryUpsertSink<LineKey, Line>();
        _ = circuit.Handle(joined).DeliverToAsync(joinSink, e => e.OrderId);
        _ = circuit.Handle(looked).DeliverToAsync(lookupSink, e => e.OrderId);
        _ = circuit.Handle(lineRows).DeliverToAsync(lineSink, l => new LineKey(l.OrderId, l.LineNo));
        await using var lineSource = await KafkaSource.StartAsync(
            circuit, lines, env.Source(linesTopic), Debezium.Changes<LineKey, Line>()).WaitAsync(Timeout);

        using var stepping = new CancellationTokenSource();
        var stepper = Task.Run(async () =>
        {
            while (!stepping.IsCancellationRequested)
            {
                await circuit.StepAsync();
                await Task.Delay(20);
            }
        });
        Task SinkIsAsync(InMemoryUpsertSink<int, Enriched> sink, Dictionary<int, Enriched> expected, string what) =>
            KafkaEnv.EventuallyAsync(() => Task.FromResult(Same(sink.Rows, expected)), what);
        async Task LinesFollowDatabaseAsync(string when)
        {
            var expected = await cdc.LinesInDatabaseAsync();
            await KafkaEnv.EventuallyAsync(() => Task.FromResult(Same(lineSink.Rows, expected)), $"the keyed input equals the lines table {when}");
        }
        await LinesFollowDatabaseAsync("after the snapshot");

        var feed = circuit.Handle(orders);
        await feed.UpsertAsync(new Order(1, 10));
        await feed.UpsertAsync(new Order(2, 11));
        await feed.UpsertAsync(new Order(3, 77)); // no such customer yet
        var expectedJoin = new Dictionary<int, Enriched>
        {
            [1] = new(1, "name-10", "region-2"),
            [2] = new(2, "name-11", "region-3"),
        };
        var expectedLookup = new Dictionary<int, Enriched>(expectedJoin) { [3] = new(3, "?", "?") };
        await SinkIsAsync(joinSink, expectedJoin, "the orders are joined to snapshot rows");
        await SinkIsAsync(lookupSink, expectedLookup, "the orders are looked up against snapshot rows");

        // Streaming: an insert, an update and a delete in the database.
        await cdc.ExecuteAsync($"insert into {cdc.Customers} values (77, 'late', 'north', 9.99, now())");
        await cdc.ExecuteAsync($"update {cdc.Customers} set region = 'moved', updated_at = now() where id = 10");
        await cdc.ExecuteAsync($"delete from {cdc.Customers} where id = 11");
        await TableFollowsDatabaseAsync("after an insert, an update and a delete");

        // The join is retroactive: all three orders change with the table.
        expectedJoin[1] = new(1, "name-10", "moved");
        expectedJoin.Remove(2);
        expectedJoin[3] = new(3, "late", "north");
        await SinkIsAsync(joinSink, expectedJoin, "the join follows the table");

        // The lookup is as of each order's own step: what was emitted stays, and an order
        // that arrives (or changes) now sees the table as it is now.
        await Task.Delay(200);
        Assert.True(Same(lookupSink.Rows, expectedLookup), "a lookup must not be retroactive");
        await feed.UpsertAsync(new Order(4, 10));
        await feed.UpsertAsync(new Order(5, 11));
        await feed.UpsertAsync(new Order(6, 77));
        expectedLookup[4] = new(4, "name-10", "moved");
        expectedLookup[5] = new(5, "?", "?");
        expectedLookup[6] = new(6, "late", "north");
        await SinkIsAsync(lookupSink, expectedLookup, "new orders are looked up against the changed table");
        expectedJoin[4] = new(4, "name-10", "moved");
        expectedJoin[6] = new(6, "late", "north");
        await SinkIsAsync(joinSink, expectedJoin, "new orders are joined to the changed table");

        // A key change is a delete and a create; composite keys come from the record key.
        await cdc.ExecuteAsync($"update {cdc.Lines} set line_no = 9 where order_id = 0 and line_no = 0");
        await cdc.ExecuteAsync($"delete from {cdc.Lines} where order_id = 1");
        await cdc.ExecuteAsync($"update {cdc.Lines} set qty = qty + 100 where order_id = 2");
        await cdc.ExecuteAsync($"insert into {cdc.Lines} values (50, 0, 'new', 1)");
        await LinesFollowDatabaseAsync("after a key change, deletes, updates and an insert");
        Assert.Contains(new LineKey(0, 9), lineSink.Rows.Keys);
        Assert.DoesNotContain(new LineKey(0, 0), lineSink.Rows.Keys);

        // The connector is restarted in the middle of a burst of changes.
        var burst = Task.Run(async () =>
        {
            for (var i = 0; i < 60; i++)
                await cdc.ExecuteAsync((i % 3) switch
                {
                    0 => $"insert into {cdc.Customers} values ({100 + i}, 'burst', 'r', {i}, now())",
                    1 => $"update {cdc.Customers} set credit = credit + 1, updated_at = now() where id < 5",
                    _ => $"delete from {cdc.Customers} where id = {100 + i - 2}",
                });
        });
        await cdc.Client.RestartAsync(connector);
        await burst;
        await cdc.Client.WaitUntilRunningAsync(connector, Timeout);
        await TableFollowsDatabaseAsync("after a connector restart during a burst");

        // A second connector over the same slot snapshots every row again: duplicates of
        // everything, at new offsets. The table absorbs them — no row changes, so no
        // version is minted — and then follows the stream from the new connector.
        await cdc.Client.DeleteConnectorAsync(connector);
        var versionBefore = customers.LatestVersion;
        var recordsBefore = loader.RecordsRead;
        var again = await cdc.RegisterAsync(env.Prefix + "-again", embedSchemas);
        var rowCount = (await cdc.CustomersInDatabaseAsync()).Count;
        await KafkaEnv.EventuallyAsync(
            () => Task.FromResult(loader.RecordsRead >= recordsBefore + rowCount), "the second snapshot has been read");
        await Task.Delay(300);
        Assert.Equal(versionBefore, customers.LatestVersion);
        await cdc.ExecuteAsync($"update {cdc.Customers} set name = 'after-resnapshot', updated_at = now() where id = 20");
        await TableFollowsDatabaseAsync("after a second snapshot");
        Assert.Equal("after-resnapshot", Rows(customers, Keys)[20].Name);
        expectedJoin.Clear();
        var final = Rows(customers, Keys);
        foreach (var order in new[] { new Order(1, 10), new Order(2, 11), new Order(3, 77), new Order(4, 10), new Order(5, 11), new Order(6, 77) })
            if (final.TryGetValue(order.CustomerId, out var customer))
                expectedJoin[order.Id] = new(order.Id, customer.Name, customer.Region);
        await SinkIsAsync(joinSink, expectedJoin, "the join still follows the table");
        Assert.Equal(0, loader.RecordsUnreadable + lineSource.RecordsUnreadable);

        KafkaEnv.Measured(output, $"CDC (schemas {embedSchemas}): {loader.RecordsRead} customer records into {customers.LatestVersion} versions, " +
            $"{lineSource.RecordsRead} line records; connector '{again}' re-snapshotted {rowCount} rows without a new version");
        stepping.Cancel();
        await stepper;
    }
}
