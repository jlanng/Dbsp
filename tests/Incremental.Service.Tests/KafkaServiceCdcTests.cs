using System.Net.Sockets;
using System.Text.Json;
using Acme.Contracts;
using Incremental.Kafka;
using Npgsql;
using static Incremental.Service.Tests.TestService;

namespace Incremental.Service.Tests;

/// <summary>
/// Change data capture through the service, on the local stack: rows live in Postgres,
/// Debezium turns their changes into a topic, a table declared with the <c>debezium</c>
/// loader follows that topic, and a pipeline looks rows up in it. Nothing here touches the
/// table directly — it is the registry's loader, restarted by the registry on the node that
/// takes over.
/// </summary>
[Collection(KafkaServiceCollection.Name)]
public class KafkaServiceCdcTests
{
    private const string ConnectionString = "Host=localhost;Port=5432;Username=incremental;Password=incremental;Database=incremental";
    private static readonly Uri Connect = new("http://localhost:8083");
    private static readonly TimeSpan Timeout = KafkaTestEnv.Timeout;

    [Fact]
    public async Task A_postgres_table_reaches_a_pipelines_lookup_through_debezium_and_the_registry_on_both_sides_of_a_takeover()
    {
        await using var env = KafkaTestEnv.Create();
        using var connect = new KafkaConnectClient(Connect);
        NpgsqlDataSource? database = null;
        try
        {
            using (var probe = new TcpClient())
                await probe.ConnectAsync("localhost", 5432).WaitAsync(TimeSpan.FromSeconds(2));
            if (!await connect.IsReachableAsync())
                throw new SocketException();
            database = NpgsqlDataSource.Create(ConnectionString);
            await using var check = database.CreateCommand("select 1");
            await check.ExecuteScalarAsync();
        }
        catch (Exception ex) when (ex is SocketException or TimeoutException or NpgsqlException)
        {
            Assert.Skip("Postgres (localhost:5432) or Kafka Connect (localhost:8083) is not reachable — run `docker compose --profile connectors up -d`.");
        }

        var table = "instruments_" + env.Prefix;
        var connector = env.Prefix + "-instruments";
        async Task SqlAsync(string sql)
        {
            await using var command = database!.CreateCommand(sql);
            await command.ExecuteNonQueryAsync();
        }

        try
        {
            await SqlAsync($"create table {table} (id int primary key, symbol text not null, isin text not null, multiplier numeric(10,2) not null)");
            for (var i = 1; i <= 5; i++)
                await SqlAsync($"insert into {table} values ({i}, 'SYM{i}', 'ISIN{i}', {i})");

            var config = DebeziumPostgres.ConnectorConfig(new DebeziumPostgresOptions
            {
                Hostname = "postgres",
                Database = "incremental",
                User = "incremental",
                Password = "incremental",
                TopicPrefix = env.Prefix,
                Tables = [$"public.{table}"],
                TopicPartitions = 2,
                TopicReplicationFactor = 1,
            });
            await connect.PutConnectorAsync(connector, config);
            await connect.WaitUntilRunningAsync(connector, Timeout);
            var topic = DebeziumPostgres.Topic(env.Prefix, "public", table);
            await Eventually(() => KafkaTestEnv.TopicExistsAsync(topic).GetAwaiter().GetResult(), "Debezium to create the change topic");

            var memory = new MemoryConnector();
            var requests = memory.KeyedLog<string, IsinRequest>("requests");
            var answers = memory.UpsertSink<string, IsinAnswer>("answers");
            var lookup = Spec("lookup", "by-isin", null, "answers") with
            {
                Inputs = new Dictionary<string, JsonElement> { ["requests"] = MemoryBinding("requests") },
            };

            await using var a = await env.StartNodeAsync("a", memory, lead: true);
            await a.Tables.DeclareAsync(new TableSpec
            {
                Name = "instruments",
                Definition = typeof(InstrumentsTable).FullName!,
                Loader = new TableLoaderSpec("debezium", "log", Json($$"""{ "topic": "{{topic}}" }""")),
            }, default);

            // The snapshot: the rows that were there before anything listened.
            await Eventually(() => a.Tables.Stats("instruments").Rows == 5, "the snapshot to load");
            Assert.Equal("loader:debezium", a.Tables.Stats("instruments").Writer);
            await a.DeployAsync(lookup);
            requests.Upsert(new IsinRequest("ISIN3"));
            await Eventually(() => answers.Rows.GetValueOrDefault("ISIN3") == new IsinAnswer("ISIN3", "SYM3"), "the first lookup");

            // Streaming changes: an update, an insert, a delete.
            await SqlAsync($"update {table} set symbol = 'RENAMED3' where id = 3");
            await SqlAsync($"insert into {table} values (6, 'SYM6', 'ISIN6', 6)");
            await SqlAsync($"delete from {table} where id = 1");
            Instrument? Row(TestService service, int id)
            {
                using var snapshot = service.Tables.Get<int, Instrument>("instruments").Snapshot();
                return snapshot.TryGet(id, out var row) ? row : null;
            }
            await Eventually(() => Row(a, 3)?.Symbol == "RENAMED3" && Row(a, 6) is not null && Row(a, 1) is null, "the changes to reach the table");
            Assert.Equal(new Instrument(6, "SYM6", "ISIN6", 6m), Row(a, 6));
            Assert.Equal(5, a.Tables.Stats("instruments").Rows);

            // Enrich on arrival: a request made now sees the row as it is now, and one that
            // was answered before the rename keeps the answer it was given.
            requests.Upsert(new IsinRequest("ISIN1"));
            requests.Upsert(new IsinRequest("ISIN6"));
            await Eventually(() => answers.Rows.GetValueOrDefault("ISIN6") == new IsinAnswer("ISIN6", "SYM6"), "a lookup of the inserted row");
            Assert.Equal(new IsinAnswer("ISIN3", "SYM3"), answers.Rows["ISIN3"]);
            Assert.False(answers.Rows.ContainsKey("ISIN1"));
            var stats = a.Tables.Stats("instruments").Loader!;
            Assert.Equal(0, stats.Unreadable);
            Assert.Contains(topic, stats.Position);

            // The standby follows the table's log, not Debezium; when it leads, its registry
            // starts the loader where the table's last committed version left the topic.
            await using var b = await env.StartNodeAsync("b", memory);
            await Eventually(() => b.Node.Status.Standby is { TakeoverWouldBeWarm: true }, "the standby to be warm");
            Assert.Equal(Row(a, 3), Row(b, 3));
            Assert.Null(b.Tables.Stats("instruments").Loader!.Position);
            await b.Node.TakeOverAsync(new NodeActionOptions(), default).WaitAsync(Timeout);
            var versionAtTakeover = b.Tables.Stats("instruments").Version;

            await SqlAsync($"insert into {table} values (7, 'SYM7', 'ISIN7', 7)");
            await SqlAsync($"update {table} set symbol = 'RENAMED6' where id = 6");
            await Eventually(() => Row(b, 7) is not null && Row(b, 6)?.Symbol == "RENAMED6", "the new leader's loader to carry on");
            Assert.Equal(6, b.Tables.Stats("instruments").Rows);

            // Carried on, not reloaded: two changes are at most two versions, where a second
            // read of the topic from its start would have rewritten every row.
            Assert.InRange(b.Tables.Stats("instruments").Version - versionAtTakeover, 1, 2);

            await b.Running("lookup");
            requests.Upsert(new IsinRequest("ISIN7"));
            await Eventually(() => answers.Rows.GetValueOrDefault("ISIN7") == new IsinAnswer("ISIN7", "SYM7"), "a lookup on the new leader");
            Assert.Equal(new IsinAnswer("ISIN6", "SYM6"), answers.Rows["ISIN6"]);
        }
        finally
        {
            try
            {
                await connect.DeleteConnectorAsync(connector);
                var slot = DebeziumPostgres.ConnectorConfig(new DebeziumPostgresOptions
                {
                    Hostname = "postgres", Database = "incremental", User = "incremental", Password = "incremental",
                    TopicPrefix = env.Prefix, Tables = [$"public.{table}"],
                });
                for (var attempt = 0; attempt < 100; attempt++)
                {
                    try
                    {
                        await SqlAsync($"select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = '{slot["slot.name"]}'");
                        break;
                    }
                    catch (PostgresException)
                    {
                        await Task.Delay(100); // the slot is released a moment after its connector stops
                    }
                }
                if (slot.TryGetValue("publication.name", out var publication))
                    await SqlAsync($"drop publication if exists {publication}");
                await SqlAsync($"drop table if exists {table}");
            }
            catch (Exception ex) when (ex is NpgsqlException or KafkaConnectException or HttpRequestException)
            {
                // Clean-up is best effort: names are unique per run.
            }
            if (database is not null)
                await database.DisposeAsync();
        }
    }
}
