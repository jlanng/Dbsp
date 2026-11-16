using Confluent.Kafka;
using Incremental.Tables;

namespace Incremental.Kafka.Tests;

/// <summary>
/// The table loader — <see cref="KafkaSource.StartTableLoaderAsync"/> — on the local
/// broker: a topic becomes a shared table, in versions of bounded size; the position is the
/// one the table records with its versions, so a successor that restored or followed the
/// table reads only what it does not have; a standby cannot load.
/// </summary>
public class KafkaTableLoaderTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Timeout = KafkaEnv.Timeout;

    private sealed record Product(int Id, string Name, int Price);

    private static Dictionary<int, Product> Rows(SharedTable<int, Product> table, int keys)
    {
        using var snapshot = table.Snapshot();
        var rows = new Dictionary<int, Product>();
        for (var id = 0; id < keys; id++)
            if (snapshot.TryGet(id, out var row))
                rows[id] = row;
        Assert.Equal(table.Count, rows.Count);
        return rows;
    }

    [Fact]
    public async Task ALoader_FillsTheTableInVersions_AndItsSuccessorReadsOnlyWhatIsNew()
    {
        const int Keys = 3000;
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("products", partitions: 2, compacted: true);
        var model = new Dictionary<int, Product>();
        void Upsert(Product product)
        {
            model[product.Id] = product;
            env.ProduceJson(topic, product.Id % 2, product.Id, product);
        }
        void Delete(int id)
        {
            model.Remove(id);
            env.ProduceJson<int, Product>(topic, id % 2, id, null);
        }

        for (var id = 0; id < Keys; id++)
            Upsert(new Product(id, "p" + id, id));
        for (var id = 0; id < Keys; id += 3)
            Upsert(new Product(id, "p" + id, id + 1000));
        for (var id = 1; id < Keys; id += 10)
            Delete(id);
        await env.FlushAsync();
        var firstRecords = Keys + Keys / 3 + Keys / 10;

        // The leader: the table is on Kafka, the loader is its writer.
        var leaderLog = await env.ConnectWriterAsync();
        var leaderTable = await SharedTable<int, Product>.OpenAsync(
            "products", p => p.Id, await leaderLog.OpenTableLogAsync<int, Product>("products"));
        var options = new KafkaSourceOptions { BootstrapServers = KafkaEnv.Bootstrap, Topic = topic, MaxTableBatch = 500 };

        // A standby follows the table's log. It has no writer, so it cannot load.
        var standbyLog = await env.ConnectAsync(env.Options(nodeId: "standby"));
        var standbyTable = await SharedTable<int, Product>.FollowAsync(
            "products", p => p.Id, await standbyLog.OpenTableLogAsync<int, Product>("products"));
        var refused = await Assert.ThrowsAsync<InvalidOperationException>(
            () => KafkaSource.StartTableLoaderAsync(standbyTable, options));
        Assert.Contains("following its log", refused.Message);

        long loadedVersion;
        await using (var loader = await KafkaSource.StartTableLoaderAsync(leaderTable, options).WaitAsync(Timeout))
        {
            await loader.WhenCaughtUpAsync().WaitAsync(Timeout);
            Assert.Equal(model, Rows(leaderTable, Keys));
            Assert.Equal(firstRecords, loader.RecordsRead);
            Assert.True((await loader.MeasureLagAsync()).IsCaughtUp);

            // In versions: no more than MaxTableBatch records each, so at least this many;
            // and far fewer than one per record.
            loadedVersion = leaderTable.LatestVersion;
            Assert.InRange(loadedVersion, firstRecords / 500, firstRecords / 20);
            // Every record changed something, so the table's position is the topic's end.
            Assert.Equal(loader.Positions.ToDictionary(p => p.Key, p => p.Value - 1), leaderTable.CommittedOffsets);
            KafkaEnv.Measured(output, $"table loader: {firstRecords} records into {loadedVersion} versions");
        }

        // The standby got the rows from the log, not from the topic.
        await KafkaEnv.EventuallyAsync(() => Task.FromResult(standbyTable.LatestVersion == loadedVersion), "the standby's table has followed the leader's");
        Assert.Equal(model, Rows(standbyTable, Keys));

        // More arrives while nobody leads; then the standby takes over.
        for (var id = 0; id < 100; id++)
            Upsert(new Product(id, "p" + id, id + 5000));
        Delete(2);
        await env.FlushAsync();

        await standbyLog.AcquireWriterAsync().WaitAsync(Timeout);
        await standbyTable.PromoteAsync().WaitAsync(Timeout);
        Assert.Equal(leaderTable.CommittedOffsets, standbyTable.CommittedOffsets);

        // Its loader is held at its first record, so that the lag can be read: the 101 new
        // records, and nothing the table already has.
        using var gate = new ManualResetEventSlim();
        var keyed = KafkaDeserializers.Keyed<int, Product>();
        KafkaChangeDecoder<int, Product> held = (in KafkaRecord record) =>
        {
            gate.Wait(Timeout);
            return keyed(in record);
        };
        await using (var loader = await KafkaSource.StartTableLoaderAsync(standbyTable, options, held).WaitAsync(Timeout))
        {
            var lag = await loader.MeasureLagAsync();
            Assert.Equal(101, lag.Total);
            Assert.False(lag.IsCaughtUp);
            gate.Set();
            await loader.WhenCaughtUpAsync().WaitAsync(Timeout);
            Assert.Equal(101, loader.RecordsRead);
            Assert.Equal(model, Rows(standbyTable, Keys));
            Assert.Equal(0, (await loader.MeasureLagAsync()).Total);
        }

        // The deposed leader's loader cannot commit: its table's log is fenced.
        Upsert(new Product(7, "late", 1));
        await env.FlushAsync();
        await using var zombie = await KafkaSource.StartTableLoaderAsync(leaderTable, options).WaitAsync(Timeout);
        await Assert.ThrowsAnyAsync<Exception>(() => zombie.Completion).WaitAsync(Timeout);
        Assert.NotEqual("late", Rows(leaderTable, Keys)[7].Name);

        await leaderTable.DisposeAsync();
        await standbyTable.DisposeAsync();
    }

    [Fact]
    public async Task AnUnreadableTableRecord_StopsTheLoader_OrIsPassedOverWhenAsked()
    {
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("products");
        env.ProduceJson(topic, 0, 1, new Product(1, "one", 1));
        env.Produce(topic, 0, KafkaEnv.Json(2), "garbage"u8.ToArray());
        env.ProduceJson(topic, 0, 3, new Product(3, "three", 3));
        await env.FlushAsync();

        var strict = new SharedTable<int, Product>("products", p => p.Id);
        await using (var loader = await KafkaSource.StartTableLoaderAsync(strict, env.Source(topic)).WaitAsync(Timeout))
        {
            var failure = await Assert.ThrowsAsync<KafkaSourceException>(() => loader.Completion).WaitAsync(Timeout);
            Assert.Contains($"{topic}[0]@1", failure.Message);
            Assert.Null(Rows(strict, 4).GetValueOrDefault(3));
        }

        var passedOver = new List<KafkaRecord>();
        var lenient = new SharedTable<int, Product>("products", p => p.Id);
        var options = new KafkaSourceOptions
        {
            BootstrapServers = KafkaEnv.Bootstrap,
            Topic = topic,
            OnUnreadableTableRecord = (record, _) => passedOver.Add(record),
        };
        await using (var loader = await KafkaSource.StartTableLoaderAsync(lenient, options).WaitAsync(Timeout))
        {
            await loader.WhenCaughtUpAsync().WaitAsync(Timeout);
            Assert.Equal([1, 3], Rows(lenient, 4).Keys.Order());
            Assert.Equal(1L, Assert.Single(passedOver).Offset);
            Assert.Equal(1, loader.RecordsUnreadable);
        }
    }

    [Fact]
    public async Task ALoader_WhosePositionIsGone_Refuses_OrReloadsTheTableFromTheTopic()
    {
        await using var env = KafkaEnv.Create();
        var topic = await env.CreateTopicAsync("products", partitions: 2);
        for (var id = 0; id < 20; id++)
            env.ProduceJson(topic, id % 2, id, new Product(id, "old", id));
        await env.FlushAsync();

        var table = new SharedTable<int, Product>("products", p => p.Id);
        await using (var loader = await KafkaSource.StartTableLoaderAsync(table, env.Source(topic)).WaitAsync(Timeout))
            await loader.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.Equal(20, table.Count);

        // While the loader is away the topic moves on and loses its beginning — including
        // the tombstones of keys 0 to 5, which nobody will ever read now. What is left
        // describes the whole table, as a compacted topic does.
        for (var id = 0; id < 6; id++)
            env.ProduceJson<int, Product>(topic, id % 2, id, null);
        var model = new Dictionary<int, Product>();
        for (var id = 6; id < 30; id++)
        {
            model[id] = new Product(id, "new", id * 2);
            env.ProduceJson(topic, id % 2, id, model[id]);
        }
        await env.FlushAsync();
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = KafkaEnv.Bootstrap }).Build())
            await admin.DeleteRecordsAsync([new TopicPartitionOffset(topic, 0, 13), new TopicPartitionOffset(topic, 1, 13)]);

        var lost = await Assert.ThrowsAsync<KafkaSourcePositionLostException>(
            () => KafkaSource.StartTableLoaderAsync(table, env.Source(topic))).WaitAsync(Timeout);
        Assert.Contains("first retained 13", lost.Message);
        Assert.Equal(20, table.Count);

        var reload = new KafkaSourceOptions { BootstrapServers = KafkaEnv.Bootstrap, Topic = topic, ReloadTableWhenPositionLost = true };
        await using (var loader = await KafkaSource.StartTableLoaderAsync(table, reload).WaitAsync(Timeout))
        {
            // The reload is part of starting: the table is already what the topic says,
            // keys 0 to 5 gone although their deletes were never read.
            Assert.Equal(model, Rows(table, 30));
            Assert.Equal(new Dictionary<int, long> { [0] = 24, [1] = 24 }, table.CommittedOffsets);

            // And it carries on from there.
            model[40] = new Product(40, "after", 1);
            env.ProduceJson(topic, 0, 40, model[40]);
            await env.FlushAsync();
            await loader.WhenCaughtUpAsync().WaitAsync(Timeout);
            Assert.Equal(model, Rows(table, 41));
        }
    }
}
