using System.Buffers.Binary;
using System.Text.Json;
using Acme.Contracts;
using Confluent.Kafka;
using static Incremental.Service.Tests.SupervisorTests;
using static Incremental.Service.Tests.TestService;

namespace Incremental.Service.Tests;

/// <summary>
/// The record format seam (<see cref="IRecordFormat"/>): a format the test host registers —
/// JSON behind a schema registry's framing, a zero byte and a four-byte schema id — read
/// and written by every kind of kafka slot and by the kafka table loader, against the
/// broker. A record the format cannot read goes where an unreadable JSON record goes.
/// </summary>
[Collection(KafkaServiceCollection.Name)]
public class KafkaRecordFormatTests
{
    private static JsonElement Framed(string topic, string connector = "kafka") =>
        Json($$"""{ "connector": "{{connector}}", "connection": "log", "topic": "{{topic}}", "format": "framed" }""");

    [Fact]
    public void A_built_in_format_name_cannot_be_taken_and_a_name_must_be_one_a_spec_can_say()
    {
        var builder = new Microsoft.Extensions.DependencyInjection.ServiceCollection().AddIncrementalService();
        Assert.Throws<ArgumentException>(() => builder.AddRecordFormat("json", _ => new FramedFormat()));
        Assert.Throws<ArgumentException>(() => builder.AddRecordFormat("debezium", _ => new FramedFormat()));
        Assert.Throws<ArgumentException>(() => builder.AddRecordFormat("Avro Schema", _ => new FramedFormat()));
        builder.AddRecordFormat("avro", _ => new FramedFormat());
    }

    [Fact]
    public async Task A_registered_format_carries_a_source_a_keyed_source_a_table_loader_and_both_sinks_and_an_unreadable_record_is_rejected()
    {
        await using var env = KafkaTestEnv.Create();
        var tradesTopic = await env.CreateTopicAsync("trades", partitions: 2);
        var keyedTopic = await env.CreateTopicAsync("keyed", compacted: true);
        var instrumentsTopic = await env.CreateTopicAsync("instruments", compacted: true);
        var enrichedTopic = await env.CreateTopicAsync("enriched", compacted: true);
        var latestTopic = await env.CreateTopicAsync("latest", compacted: true);
        var bigTopic = await env.CreateTopicAsync("big");

        var instruments = Instruments(10);
        var trades = Trades(0, 300);
        foreach (var instrument in instruments)
            Produce(env, instrumentsTopic, instrument.Id, instrument);
        foreach (var trade in trades)
        {
            Produce(env, tradesTopic, trade.Id, trade);
            Produce(env, keyedTopic, trade.Id, trade);
        }

        // What the format cannot read: a plain JSON record on each input, as a producer
        // that skipped the registry would write it.
        Plain(env, tradesTopic, -1, trades[0]);
        Plain(env, keyedTopic, -1, trades[0]);
        Plain(env, instrumentsTopic, -1, instruments[0] with { Id = 99 });

        // A keyed topic's tombstone: the key in the format, no value.
        var deleted = trades.Where(t => t.Id % 7 == 0).Select(t => t.Id).ToHashSet();
        foreach (var id in deleted)
            env.Producer.Produce(keyedTopic, new Message<byte[], byte[]> { Key = FramedFormat.Frame(id, RecordPart.Key), Value = null! });
        await env.FlushAsync();

        var format = new FramedFormat();
        await using var a = await env.StartNodeAsync("a", lead: true, configure: b => b.AddRecordFormat("framed", _ => format));

        // The table loader: unreadable records are counted and skipped under Skip, as
        // with JSON — and with Fault the table fails at the record, as with JSON.
        await a.Tables.DeclareAsync(new TableSpec
        {
            Name = "instruments",
            Definition = typeof(InstrumentsTable).FullName!,
            Loader = new TableLoaderSpec("kafka", "log", Json($$"""{ "topic": "{{instrumentsTopic}}", "format": "framed" }""")),
            OnUnreadable = UnreadablePolicy.Skip,
        }, default);
        await Eventually(() => a.Tables.Stats("instruments").Rows == instruments.Count && a.Tables.Stats("instruments").Loader!.Unreadable == 1,
            "the loader to fill the table and pass over the one record it cannot read");
        var unreadable = Assert.Single(a.Tables.Unreadable("instruments"));
        Assert.Equal($"{instrumentsTopic}[0]@{instruments.Count}", unreadable.Position);
        Assert.Equal(nameof(FormatException), unreadable.ExceptionType);

        var deadLetter = new PipelineCircuitOptions { ErrorPolicy = RecordErrorPolicy.DeadLetter };
        await a.DeployAsync(Spec("enrich", "enrich") with
        {
            Circuit = deadLetter,
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Framed(tradesTopic) },
            Sinks = new Dictionary<string, JsonElement> { ["out"] = Framed(enrichedTopic) },
        });
        await a.DeployAsync(Spec("latest", "latest-trades") with
        {
            Circuit = deadLetter,
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Framed(keyedTopic) },
            Sinks = new Dictionary<string, JsonElement> { ["out"] = Framed(latestTopic) },
        });
        await a.DeployAsync(Spec("big", "big-trades") with
        {
            Circuit = deadLetter,
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Framed(tradesTopic) },
            Sinks = new Dictionary<string, JsonElement> { ["out"] = Framed(bigTopic) },
        });

        // Both sink kinds wrote the format, keys included.
        await EventuallyEqualAsync(enrichedTopic, Enrich.Model(trades, instruments).ToDictionary(e => e.Key, e => (object)e.Value),
            bytes => FramedFormat.Unframe<long>(bytes, RecordPart.Key), bytes => FramedFormat.Unframe<EnrichedTrade>(bytes, RecordPart.Value), "the enriched sink");
        await EventuallyEqualAsync(latestTopic, trades.Where(t => !deleted.Contains(t.Id)).ToDictionary(t => t.Id, t => (object)t),
            bytes => FramedFormat.Unframe<long>(bytes, RecordPart.Key), bytes => FramedFormat.Unframe<Trade>(bytes, RecordPart.Value), "the latest-trades sink");
        var expectedBig = BigTrades.Model(trades).OrderBy(t => t.Id).ToList();
        await Eventually(() => KafkaTestEnv.ReadTopic(bigTopic).Count >= expectedBig.Count, "the append sink's records");
        var big = KafkaTestEnv.ReadTopic(bigTopic).Select(r => FramedFormat.Unframe<Trade>(r.Message.Value, RecordPart.Value)).OrderBy(t => t.Id).ToList();
        Assert.Equal(expectedBig, big);

        // Each source rejected the one record it could not read, as a source rejects
        // broken JSON: offset consumed, a dead letter under the DeadLetter policy, and the
        // pipeline running on.
        foreach (var (id, topic) in new[] { ("enrich", tradesTopic), ("latest", keyedTopic), ("big", tradesTopic) })
        {
            await Eventually(() => a.Pipelines.DeadLetters(id).Any(d => d.Operator == "input[trades]"), $"{id}'s rejected record");
            var rejected = Assert.Single(a.Pipelines.DeadLetters(id), d => d.Operator == "input[trades]");
            Assert.Equal(nameof(FormatException), rejected.ExceptionType);
            Assert.Contains(topic, rejected.Record);
            Assert.Equal(1, a.Pipelines.Get(id).Sources.Single().Stats.Unreadable);
            Assert.Equal(PipelineState.Running, a.Pipelines.Get(id).State);
        }

        // Codecs are made per slot, part and generation, not per record: three sources
        // (the keyed one has two parts), three sinks (the upsert sinks two parts each), and
        // the loader's key and value.
        Assert.Equal(1 + 2 + 1 + 2 + 2 + 1 + 2, format.Codecs);

        // A format nobody registered is refused at the dry run, by name, and so is a
        // format on the debezium loader.
        var unknown = await Assert.ThrowsAsync<ServiceException>(() => a.Pipelines.ValidateAsync(Spec("x", "big-trades") with
        {
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Json($$"""{ "connector": "kafka", "connection": "log", "topic": "{{tradesTopic}}", "format": "avro" }""") },
            Sinks = new Dictionary<string, JsonElement> { ["out"] = Framed(bigTopic) },
        }, default));
        Assert.Contains("no record format named 'avro'", string.Join(" ", [unknown.Message, .. unknown.Details]));
        Assert.Contains("framed", string.Join(" ", [unknown.Message, .. unknown.Details]));
        await Assert.ThrowsAsync<ServiceException>(() => a.Tables.DeclareAsync(new TableSpec
        {
            Name = "cdc",
            Definition = typeof(InstrumentsTable).FullName!,
            Loader = new TableLoaderSpec("debezium", "log", Json($$"""{ "topic": "{{instrumentsTopic}}", "format": "framed" }""")),
        }, default));
    }

    [Fact]
    public async Task A_plugins_own_row_type_goes_through_a_format_and_its_load_context_still_unloads()
    {
        await using var env = KafkaTestEnv.Create();
        var tradesTopic = await env.CreateTopicAsync("trades");
        var notesTopic = await env.CreateTopicAsync("notes", compacted: true);
        var logTopic = await env.CreateTopicAsync("log");
        var trades = Trades(0, 50);
        foreach (var trade in trades)
            Produce(env, tradesTopic, trade.Id, trade);
        await env.FlushAsync();

        var format = new FramedFormat();
        await using var a = await env.StartNodeAsync("a", lead: true, configure: b => b.AddRecordFormat("framed", _ => format));
        var plugin = await a.Plugins.ImportDirectoryAsync(PluginDirectory("Acme.Pipelines"), default);
        await a.DeployAsync(new PipelineSpec
        {
            Id = "notes", Plugin = plugin.Name, Version = plugin.Version, Definition = "trade-notes",
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Framed(tradesTopic) },
            Sinks = new Dictionary<string, JsonElement> { ["notes"] = Framed(notesTopic), ["log"] = Framed(logTopic) },
        });

        // Note is the plugin's own record: read back here as JSON of the same shape.
        await Eventually(() => KafkaTestEnv.ReadTopic(logTopic).Count == trades.Count && KafkaTestEnv.ReadTopic(notesTopic).Count >= trades.Count,
            "both sinks to hold every note");
        var note = FramedFormat.Unframe<JsonElement>(KafkaTestEnv.ReadTopic(logTopic)[0].Message.Value, RecordPart.Value);
        Assert.Equal("v1", note.GetProperty("version").GetString());
        Assert.Equal(1 + 2 + 1, format.Codecs);

        // With the pipeline gone, nothing the format or the connector made for it may keep
        // the plugin's context alive.
        await a.Pipelines.RemoveAsync("notes", new ActionOptions(), default);
        for (var attempt = 0; attempt < 200 && a.Plugins.LiveContexts != 0; attempt++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            await Task.Delay(25);
        }
        Assert.Equal(0, a.Plugins.LiveContexts);
    }

    // ── the topics ─────────────────────────────────────────────────────────────────────

    private static void Produce<TKey, TValue>(KafkaTestEnv env, string topic, TKey key, TValue value) =>
        env.Producer.Produce(topic, new Message<byte[], byte[]> { Key = FramedFormat.Frame(key, RecordPart.Key), Value = FramedFormat.Frame(value, RecordPart.Value) });

    private static void Plain<TKey, TValue>(KafkaTestEnv env, string topic, TKey key, TValue value) where TValue : class =>
        env.ProduceJson(topic, key, value);

    private static async Task EventuallyEqualAsync(
        string topic, IReadOnlyDictionary<long, object> expected, Func<byte[], long> key, Func<byte[], object> value, string what)
    {
        Dictionary<long, object> actual = [];
        var deadline = DateTime.UtcNow + KafkaTestEnv.Timeout;
        while (DateTime.UtcNow < deadline)
        {
            actual = [];
            foreach (var record in await Task.Run(() => KafkaTestEnv.ReadTopic(topic)))
                if (record.Message.Value is null)
                    actual.Remove(key(record.Message.Key));
                else
                    actual[key(record.Message.Key)] = value(record.Message.Value);
            if (actual.Count == expected.Count && expected.All(e => actual.TryGetValue(e.Key, out var row) && Equals(row, e.Value)))
                return;
            await Task.Delay(200);
        }
        Assert.Fail($"{what}: {actual.Count} rows, expected {expected.Count}");
    }
}

/// <summary>
/// A test host's record format: JSON behind the framing a schema registry puts in front of
/// a value — a zero byte, then a four-byte big-endian schema id (1 for keys, 2 for values
/// here) — so a plain JSON record is one it cannot read. It keeps nothing per type: each
/// codec is its own object, and the format counts them with a number.
/// </summary>
internal sealed class FramedFormat : IRecordFormat
{
    private int _codecs;

    public int Codecs => Volatile.Read(ref _codecs);

    public IRecordCodec<T> CreateCodec<T>(RecordFormatContext context)
    {
        Interlocked.Increment(ref _codecs);
        return new Codec<T>(context.Json, context.Part);
    }

    private static int Schema(RecordPart part) => part == RecordPart.Key ? 1 : 2;

    public static byte[] Frame<T>(T value, RecordPart part, JsonSerializerOptions? json = null)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(value, json ?? JsonSerializerOptions.Web);
        var framed = new byte[5 + body.Length];
        BinaryPrimitives.WriteInt32BigEndian(framed.AsSpan(1), Schema(part));
        body.CopyTo(framed, 5);
        return framed;
    }

    public static T Unframe<T>(ReadOnlySpan<byte> data, RecordPart part, JsonSerializerOptions? json = null)
    {
        if (data.Length < 5 || data[0] != 0)
            throw new FormatException("The record is not framed: it does not start with the magic byte.");
        if (BinaryPrimitives.ReadInt32BigEndian(data[1..]) != Schema(part))
            throw new FormatException($"The record's schema is {BinaryPrimitives.ReadInt32BigEndian(data[1..])}, not {Schema(part)}.");
        return JsonSerializer.Deserialize<T>(data[5..], json ?? JsonSerializerOptions.Web)
            ?? throw new FormatException("The framed JSON is null.");
    }

    private sealed class Codec<T>(JsonSerializerOptions json, RecordPart part) : IRecordCodec<T>
    {
        public T Read(ReadOnlySpan<byte> data, in RecordOrigin origin) => Unframe<T>(data, part, json);

        public byte[] Write(T value) => Frame(value, part, json);
    }
}
