using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace Dbsp.Kafka;

/// <summary>What a Debezium change event says happened to a row.</summary>
public enum DebeziumOperation
{
    /// <summary><c>c</c>: the row was inserted.</summary>
    Create,

    /// <summary><c>u</c>: the row was updated.</summary>
    Update,

    /// <summary><c>d</c>: the row was deleted.</summary>
    Delete,

    /// <summary><c>r</c>: the row was read by a snapshot, not changed.</summary>
    Read,

    /// <summary><c>t</c>: the table was truncated. Carries no row.</summary>
    Truncate,

    /// <summary><c>m</c>: a logical decoding message. Carries no row.</summary>
    Message,
}

/// <summary>
/// One Debezium change event, out of its envelope.
/// <see cref="Before"/> is what the database's replica identity makes it: with Postgres's
/// default identity it is null for an update and holds only the key columns for a delete;
/// only <c>REPLICA IDENTITY FULL</c> fills it.
/// </summary>
public sealed class DebeziumEvent<TRow>
{
    internal DebeziumEvent(
        DebeziumOperation operation, TRow? before, TRow? after, long? lsn, long? transactionId,
        DateTimeOffset? sourceTimestamp, DateTimeOffset? timestamp, string? snapshot, string? schema, string? table)
    {
        Operation = operation;
        Before = before;
        After = after;
        Lsn = lsn;
        TransactionId = transactionId;
        SourceTimestamp = sourceTimestamp;
        Timestamp = timestamp;
        Snapshot = snapshot;
        Schema = schema;
        Table = table;
    }

    public DebeziumOperation Operation { get; }

    /// <summary>The row before the change, as far as the replica identity records it.</summary>
    public TRow? Before { get; }

    /// <summary>The row after the change; null for a delete.</summary>
    public TRow? After { get; }

    /// <summary><c>source.lsn</c>: the position of the change in the database's log. It
    /// rises along a partition except where the connector replays after a restart.</summary>
    public long? Lsn { get; }

    /// <summary><c>source.txId</c>.</summary>
    public long? TransactionId { get; }

    /// <summary><c>source.ts_ms</c>: when the database made the change.</summary>
    public DateTimeOffset? SourceTimestamp { get; }

    /// <summary><c>ts_ms</c>: when the connector processed it.</summary>
    public DateTimeOffset? Timestamp { get; }

    /// <summary><c>source.snapshot</c> as written: <c>"true"</c>, <c>"first"</c>,
    /// <c>"last"</c>, <c>"dbsp"</c>, <c>"false"</c> or absent.</summary>
    public string? Snapshot { get; }

    /// <summary>True for a record of a snapshot rather than of the change stream.</summary>
    public bool IsSnapshot => Snapshot is not (null or "false");

    /// <summary><c>source.schema</c>.</summary>
    public string? Schema { get; }

    /// <summary><c>source.table</c>.</summary>
    public string? Table { get; }
}

/// <summary>
/// Reads Debezium change events written with Kafka Connect's JSON converter, with or
/// without embedded schemas (<c>schemas.enable</c>), into table changes: "Postgres table →
/// Debezium → Kafka → <see cref="KafkaSource"/> → shared table or keyed input".
/// <para>
/// <see cref="Changes{TKey,TRow}"/> is the decoder to hand a source. It reads a record so:
/// <c>c</c>, <c>u</c> and <c>r</c> (a snapshot row) upsert <c>after</c>; <c>d</c> deletes
/// the key in the record's key; the tombstone Debezium writes after a delete (a record
/// with no value) deletes the same key again, which changes nothing; a logical decoding
/// message is passed over; a truncate is unreadable — nothing here can empty a table, and
/// Debezium's Postgres connector does not emit them unless asked.
/// </para>
/// <para>
/// <b>Duplicates and order.</b> Debezium is at-least-once: after a connector restart the
/// same changes can arrive again at new offsets, in their original order. Upserts and
/// deletes by key absorb that — a row may step back for a moment and forward again, and
/// ends where the database is.
/// </para>
/// <para>
/// <b>Replica identity.</b> Deletes are keyed by the record key, which is the table's
/// primary key, so the default identity is enough for a table that has one. A table with
/// no primary key has no record key: its deletes can only be applied under
/// <c>REPLICA IDENTITY FULL</c>, through <c>before</c> and a <c>keyOf</c> selector. An
/// update that changes the primary key arrives as a delete of the old key and a create of
/// the new one. A large column Postgres stores out of line (TOAST) and that an update did
/// not touch arrives as Debezium's placeholder text unless the identity is <c>FULL</c>.
/// </para>
/// <para>
/// <b>Types.</b> Column names are matched to properties in snake_case, ignoring case, and
/// numbers may arrive as text. <c>numeric</c> needs the connector's
/// <c>decimal.handling.mode=string</c> (the default encodes it as bytes);
/// <c>timestamptz</c> is ISO-8601 text; <c>timestamp</c> and <c>date</c> are numbers
/// (microseconds or milliseconds, and days, since the epoch), so declare them as such or
/// supply converters in the options.
/// </para>
/// </summary>
public static class Debezium
{
    /// <summary>The JSON options rows and keys are read with unless others are given:
    /// snake_case column names, case ignored, numbers accepted as text.</summary>
    public static JsonSerializerOptions DefaultJson { get; } = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        PropertyNameCaseInsensitive = true,
        NumberHandling = JsonNumberHandling.AllowReadingFromString,
        TypeInfoResolver = new DefaultJsonTypeInfoResolver(),
    };

    /// <summary>
    /// The decoder for a source over one table's topic. <paramref name="keyOf"/> is only
    /// for tables without a primary key under <c>REPLICA IDENTITY FULL</c>: it derives the
    /// key of a delete from the event's <c>before</c> row when the record has no key.
    /// </summary>
    public static KafkaChangeDecoder<TKey, TRow> Changes<TKey, TRow>(JsonSerializerOptions? options = null, Func<TRow, TKey>? keyOf = null)
    {
        options ??= DefaultJson;
        return (in KafkaRecord record) =>
        {
            if (record.Value is not { } value)
            {
                // The tombstone that follows a delete, for the benefit of compaction.
                return record.Key is { } tombstoneKey
                    ? KafkaChange<TKey, TRow>.Delete(ParseKey<TKey>(tombstoneKey, options))
                    : KafkaChange<TKey, TRow>.Skip;
            }

            var change = ParseValue<TRow>(value, options, needBefore: record.Key is null && keyOf is not null);
            switch (change.Operation)
            {
                case DebeziumOperation.Create or DebeziumOperation.Update or DebeziumOperation.Read:
                    return change.After is { } after
                        ? KafkaChange<TKey, TRow>.Upsert(after)
                        : throw new FormatException($"A Debezium '{change.Operation}' event has no 'after' row.");
                case DebeziumOperation.Delete:
                    if (record.Key is { } key)
                        return KafkaChange<TKey, TRow>.Delete(ParseKey<TKey>(key, options));
                    if (keyOf is not null && change.Before is { } before)
                        return KafkaChange<TKey, TRow>.Delete(keyOf(before));
                    throw new FormatException(
                        "A Debezium delete has no record key (the table has no primary key), so there is no telling which row " +
                        "it deletes. Give the table a primary key, or REPLICA IDENTITY FULL and the decoder a keyOf selector.");
                case DebeziumOperation.Message:
                    return KafkaChange<TKey, TRow>.Skip;
                default:
                    throw new FormatException(
                        "A Debezium truncate event cannot be applied: a keyed input or a table has no 'delete everything'. " +
                        "Leave truncates out of the connector (skipped.operations=t, the Postgres connector's default).");
            }
        };
    }

    /// <summary>A deserializer for a stream of the change events themselves — an audit
    /// trail, say — with <c>before</c> and <c>after</c> and where each came from. Records
    /// with no value (tombstones) never reach it: a stream input passes them over.</summary>
    public static KafkaDeserializer<DebeziumEvent<TRow>> Events<TRow>(JsonSerializerOptions? options = null)
    {
        options ??= DefaultJson;
        return (ReadOnlySpan<byte> data, in KafkaRecord _) => ParseValue<TRow>(data, options);
    }

    /// <summary>Reads a change event's value, with or without the converter's schema
    /// envelope.</summary>
    public static DebeziumEvent<TRow> ParseValue<TRow>(ReadOnlySpan<byte> value, JsonSerializerOptions? options = null) =>
        ParseValue<TRow>(value, options ?? DefaultJson, needBefore: true);

    private static DebeziumEvent<TRow> ParseValue<TRow>(ReadOnlySpan<byte> value, JsonSerializerOptions options, bool needBefore)
    {
        var reader = new Utf8JsonReader(value);
        using var document = JsonDocument.ParseValue(ref reader);
        var payload = Payload(document.RootElement);
        if (payload.ValueKind != JsonValueKind.Object || !payload.TryGetProperty("op", out var op) || op.ValueKind != JsonValueKind.String)
            throw new FormatException("The value is not a Debezium change event: it has no 'op'.");

        var operation = op.GetString() switch
        {
            "c" => DebeziumOperation.Create,
            "u" => DebeziumOperation.Update,
            "d" => DebeziumOperation.Delete,
            "r" => DebeziumOperation.Read,
            "t" => DebeziumOperation.Truncate,
            "m" => DebeziumOperation.Message,
            var other => throw new FormatException($"Unknown Debezium operation '{other}'."),
        };

        var after = Row<TRow>(payload, "after", options);
        var before = needBefore ? Row<TRow>(payload, "before", options) : default;

        long? lsn = null, transaction = null;
        DateTimeOffset? sourceTime = null;
        string? snapshot = null, schema = null, table = null;
        if (payload.TryGetProperty("source", out var source) && source.ValueKind == JsonValueKind.Object)
        {
            lsn = Number(source, "lsn");
            transaction = Number(source, "txId");
            sourceTime = Time(source, "ts_ms");
            snapshot = Text(source, "snapshot");
            schema = Text(source, "schema");
            table = Text(source, "table");
        }
        return new DebeziumEvent<TRow>(operation, before, after, lsn, transaction, sourceTime, Time(payload, "ts_ms"), snapshot, schema, table);
    }

    /// <summary>
    /// Reads a record key: the table's primary key columns as a JSON object, with or
    /// without the schema envelope. A one-column key read as a scalar
    /// <typeparamref name="TKey"/> (<c>int</c>, <c>string</c>, <c>Guid</c>, …) is that
    /// column's value; anything else is the object deserialized as
    /// <typeparamref name="TKey"/>, column names matched as for rows.
    /// </summary>
    public static TKey ParseKey<TKey>(ReadOnlySpan<byte> key, JsonSerializerOptions? options = null)
    {
        options ??= DefaultJson;
        var reader = new Utf8JsonReader(key);
        using var document = JsonDocument.ParseValue(ref reader);
        var payload = Payload(document.RootElement);
        if (payload.ValueKind == JsonValueKind.Object && options.GetTypeInfo(typeof(TKey)).Kind == JsonTypeInfoKind.None)
        {
            using var columns = payload.EnumerateObject();
            if (!columns.MoveNext())
                throw new FormatException("The Debezium record key has no columns.");
            var column = columns.Current.Value;
            if (columns.MoveNext())
                throw new FormatException(
                    $"The Debezium record key has several columns and cannot be read as a {typeof(TKey).Name}: use a record with one property per key column.");
            payload = column;
        }
        var parsed = payload.Deserialize<TKey>(options);
        return parsed is null ? throw new FormatException("The Debezium record key is null.") : parsed;
    }

    /// <summary>The converter's envelope, when schemas are on, is
    /// <c>{"schema":{…},"payload":…}</c>; without them the payload stands alone.</summary>
    private static JsonElement Payload(JsonElement root) =>
        root.ValueKind == JsonValueKind.Object
        && root.TryGetProperty("payload", out var payload)
        && root.TryGetProperty("schema", out var schema)
        && schema.ValueKind is JsonValueKind.Object or JsonValueKind.Null
            ? payload
            : root;

    private static TRow? Row<TRow>(JsonElement payload, string name, JsonSerializerOptions options) =>
        payload.TryGetProperty(name, out var row) && row.ValueKind == JsonValueKind.Object ? row.Deserialize<TRow>(options) : default;

    private static long? Number(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var number) ? number : null;

    private static string? Text(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    private static DateTimeOffset? Time(JsonElement element, string name) =>
        Number(element, name) is { } milliseconds ? DateTimeOffset.FromUnixTimeMilliseconds(milliseconds) : null;
}
