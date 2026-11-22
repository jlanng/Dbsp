using System.Text;

namespace Dbsp.Kafka.Tests;

/// <summary>
/// <see cref="Debezium"/> against records as Debezium 3.0's Postgres connector wrote them
/// to the local broker (captured from <see cref="DebeziumCdcTests"/>, shortened only in
/// the embedded schema). No broker needed.
/// </summary>
public class DebeziumDecoderTests
{
    private sealed record Customer(int Id, string Name, string Region, decimal Credit, DateTimeOffset UpdatedAt);
    private sealed record LineKey(int OrderId, int LineNo);
    private sealed record Line(int OrderId, int LineNo, string Sku, int Qty);

    private const string Source =
        """{"version":"3.0.8.Final","connector":"postgresql","name":"t5063","ts_ms":1794308880521,"snapshot":"SNAPSHOT","db":"dbsp","sequence":"[\"27621952\",\"27622288\"]","ts_us":1794308880521745,"ts_ns":1794308880521745000,"schema":"public","table":"customers","txId":1057,"lsn":27622288,"xmin":null}""";

    private static string Value(string op, string before, string after, string snapshot = "false") =>
        $$"""{"before":{{before}},"after":{{after}},"source":{{Source.Replace("SNAPSHOT", snapshot)}},"transaction":null,"op":"{{op}}","ts_ms":1794308880539,"ts_us":1794308880539351,"ts_ns":1794308880539351483}""";

    private const string Row3 = """{"id":3,"name":"name-3","region":"region-3","credit":"19.75","updated_at":"2026-11-10T11:08:00.521650Z"}""";

    // What a delete carries under Postgres's default replica identity: the key, and
    // defaults in every other column.
    private const string DeletedRow = """{"id":109,"name":"","region":"","credit":"0.00","updated_at":"1970-01-01T00:00:00Z"}""";

    private static string Enveloped(string payload) =>
        $$"""{"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"}],"optional":false,"name":"t.public.customers.Envelope","version":2},"payload":{{payload}}}""";

    private static KafkaRecord Record(string? key, string? value) =>
        new("t.public.customers", 1, 59, key is null ? null : Encoding.UTF8.GetBytes(key), value is null ? null : Encoding.UTF8.GetBytes(value), null, null);

    private static readonly Customer Three =
        new(3, "name-3", "region-3", 19.75m, new DateTimeOffset(2026, 11, 10, 11, 8, 0, TimeSpan.Zero).AddTicks(5216500));

    [Theory]
    [InlineData("c", "false", false)]
    [InlineData("u", "false", false)]
    [InlineData("r", "true", false)]
    [InlineData("r", "last", true)]
    [InlineData("u", "false", true)]
    public void ACreateUpdateOrSnapshotRead_IsAnUpsertOfAfter(string op, string snapshot, bool embedSchemas)
    {
        var key = """{"id":3}""";
        var value = Value(op, "null", Row3, snapshot);
        var record = embedSchemas ? Record(Enveloped(key), Enveloped(value)) : Record(key, value);

        var change = Debezium.Changes<int, Customer>()(in record);
        Assert.True(change.IsUpsert);
        Assert.Equal(Three, change.Value);

        var parsed = Debezium.ParseValue<Customer>(record.Value);
        Assert.Equal(op switch { "c" => DebeziumOperation.Create, "u" => DebeziumOperation.Update, _ => DebeziumOperation.Read }, parsed.Operation);
        Assert.Equal(Three, parsed.After);
        Assert.Null(parsed.Before);
        Assert.Equal(27622288, parsed.Lsn);
        Assert.Equal(1057, parsed.TransactionId);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1794308880521), parsed.SourceTimestamp);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1794308880539), parsed.Timestamp);
        Assert.Equal(snapshot, parsed.Snapshot);
        Assert.Equal(snapshot != "false", parsed.IsSnapshot);
        Assert.Equal(("public", "customers"), (parsed.Schema, parsed.Table));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ADelete_AndTheTombstoneAfterIt_DeleteTheRecordKey(bool embedSchemas)
    {
        var key = """{"id":109}""";
        var value = Value("d", DeletedRow, "null");
        var delete = embedSchemas ? Record(Enveloped(key), Enveloped(value)) : Record(key, value);
        var tombstone = embedSchemas ? Record(Enveloped(key), null) : Record(key, null);
        var decoder = Debezium.Changes<int, Customer>();

        var change = decoder(in delete);
        Assert.True(change.IsDelete);
        Assert.Equal(109, change.Key);

        change = decoder(in tombstone);
        Assert.True(change.IsDelete);
        Assert.Equal(109, change.Key);

        // The event itself still says what the identity recorded of the old row.
        var parsed = Debezium.ParseValue<Customer>(delete.Value);
        Assert.Equal(DebeziumOperation.Delete, parsed.Operation);
        Assert.Null(parsed.After);
        Assert.Equal(new Customer(109, "", "", 0m, DateTimeOffset.UnixEpoch), parsed.Before);
    }

    [Fact]
    public void AKey_IsItsOneColumn_OrARecordOfItsColumns()
    {
        Assert.Equal(109, Debezium.ParseKey<int>("""{"id":109}"""u8));
        Assert.Equal(109L, Debezium.ParseKey<long>(Encoding.UTF8.GetBytes(Enveloped("""{"id":109}"""))));
        Assert.Equal("abc", Debezium.ParseKey<string>("""{"code":"abc"}"""u8));
        Assert.Equal(Guid.Parse("7f0c1a52-9a3b-4f0e-8f0a-3c1d2e4f5a6b"), Debezium.ParseKey<Guid>("""{"id":"7f0c1a52-9a3b-4f0e-8f0a-3c1d2e4f5a6b"}"""u8));
        Assert.Equal(new LineKey(4, 2), Debezium.ParseKey<LineKey>("""{"order_id":4,"line_no":2}"""u8));
        Assert.Equal(new LineKey(4, 2), Debezium.ParseKey<LineKey>(Encoding.UTF8.GetBytes(Enveloped("""{"order_id":4,"line_no":2}"""))));

        // Several columns cannot be one scalar; that is a mistake in the key type, said plainly.
        var error = Assert.Throws<FormatException>(() => Debezium.ParseKey<int>("""{"order_id":4,"line_no":2}"""u8));
        Assert.Contains("several columns", error.Message);

        // A composite key deletes by the record key, like any other.
        var delete = Record("""{"order_id":0,"line_no":0}""", Value("d", """{"order_id":0,"line_no":0,"sku":"","qty":0}""", "null"));
        var change = Debezium.Changes<LineKey, Line>()(in delete);
        Assert.True(change.IsDelete);
        Assert.Equal(new LineKey(0, 0), change.Key);
    }

    [Fact]
    public void ATableWithNoPrimaryKey_DeletesThroughBefore_WhenToldHow_AndNotOtherwise()
    {
        // No record key; REPLICA IDENTITY FULL puts the whole old row in "before".
        var delete = Record(null, Value("d", Row3, "null"));

        var change = Debezium.Changes<int, Customer>(keyOf: c => c.Id)(in delete);
        Assert.True(change.IsDelete);
        Assert.Equal(3, change.Key);

        var error = Assert.Throws<FormatException>(() => Debezium.Changes<int, Customer>()(in delete));
        Assert.Contains("no record key", error.Message);

        // Upserts need no record key at all: the row's key comes from the row.
        var create = Record(null, Value("c", "null", Row3));
        Assert.Equal(Three, Debezium.Changes<int, Customer>()(in create).Value);
    }

    [Fact]
    public void WhatIsNotARowChange_IsPassedOver_OrRefused()
    {
        var decoder = Debezium.Changes<int, Customer>();

        var message = Record(null, """{"op":"m","ts_ms":1,"source":{"lsn":5},"message":{"prefix":"x","content":"aGk="}}""");
        Assert.True(decoder(in message).IsSkip);

        var keyless = Record(null, null);
        Assert.True(decoder(in keyless).IsSkip);

        var truncate = Record(null, """{"op":"t","ts_ms":1,"source":{"lsn":5}}""");
        Assert.Contains("truncate", Assert.Throws<FormatException>(() => decoder(in truncate)).Message);

        var notAnEvent = Record("""{"id":1}""", """{"id":1,"name":"plain row, no envelope"}""");
        Assert.Contains("no 'op'", Assert.Throws<FormatException>(() => decoder(in notAnEvent)).Message);

        var noAfter = Record("""{"id":1}""", Value("u", "null", "null"));
        Assert.Contains("no 'after'", Assert.Throws<FormatException>(() => decoder(in noAfter)).Message);

        var garbage = Record("""{"id":1}""", "{ nope");
        Assert.ThrowsAny<System.Text.Json.JsonException>(() => decoder(in garbage));
    }

    [Fact]
    public void TheEventsDeserializer_GivesTheWholeChange()
    {
        var update = Record("""{"id":3}""", Value("u", Row3.Replace("19.75", "18.75"), Row3));
        var change = Debezium.Events<Customer>()(update.Value, in update);
        Assert.Equal(DebeziumOperation.Update, change.Operation);
        Assert.Equal(18.75m, change.Before!.Credit);
        Assert.Equal(19.75m, change.After!.Credit);
        Assert.False(change.IsSnapshot);
    }

    [Fact]
    public void TheConnectorConfiguration_IsWhatTheDecoderNeeds()
    {
        var config = DebeziumPostgres.ConnectorConfig(new DebeziumPostgresOptions
        {
            Hostname = "postgres",
            Database = "shop",
            User = "u",
            Password = "p",
            TopicPrefix = "Shop-CDC",
            Tables = ["public.customers", "public.lines"],
            TopicPartitions = 6,
            Extra = new Dictionary<string, string> { ["snapshot.mode"] = "no_data", ["heartbeat.interval.ms"] = "1000" },
        });
        Assert.Equal("io.debezium.connector.postgresql.PostgresConnector", config["connector.class"]);
        Assert.Equal("pgoutput", config["plugin.name"]);
        Assert.Equal("public.customers,public.lines", config["table.include.list"]);
        Assert.Equal("shop_cdc_slot", config["slot.name"]);
        Assert.Equal("shop_cdc_publication", config["publication.name"]);
        Assert.Equal("string", config["decimal.handling.mode"]);
        Assert.Equal("true", config["tombstones.on.delete"]);
        Assert.Equal("false", config["value.converter.schemas.enable"]);
        Assert.Equal("false", config["key.converter.schemas.enable"]);
        Assert.Equal("6", config["topic.creation.default.partitions"]);
        Assert.Equal("3", config["topic.creation.default.replication.factor"]);
        Assert.Equal("compact", config["topic.creation.default.cleanup.policy"]);
        Assert.Equal("no_data", config["snapshot.mode"]); // Extra wins
        Assert.Equal("1000", config["heartbeat.interval.ms"]);
        Assert.Equal("Shop-CDC.public.customers", DebeziumPostgres.Topic("Shop-CDC", "public", "customers"));
    }

    [Fact]
    public void Lag_IsTheOffsetsStillToRead()
    {
        var lag = new KafkaSourceLag(
            new Dictionary<int, long> { [0] = 10, [1] = 7, [2] = 30 },
            new Dictionary<int, long> { [0] = 10, [1] = 20, [2] = 25, [3] = 4 });
        Assert.Equal(0 + 13 + 0 + 4, lag.Total);
        Assert.False(lag.IsCaughtUp);
        Assert.True(new KafkaSourceLag(new Dictionary<int, long> { [0] = 5 }, new Dictionary<int, long> { [0] = 5 }).IsCaughtUp);
    }
}
