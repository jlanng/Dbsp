using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace Incremental.Kafka;

/// <summary>
/// One record as a <see cref="KafkaSource"/> read it: where it came from and its raw bytes.
/// It is what a deserializer is given beside the bytes, and what a record that could not be
/// read is reported as — on <see cref="Circuit.DeadLetters"/>, the
/// <see cref="DeadLetter.Record"/> of a poison record is one of these.
/// </summary>
public readonly record struct KafkaRecord(
    string Topic, int Partition, long Offset, byte[]? Key, byte[]? Value, DateTimeOffset? Timestamp, Headers? Headers)
{
    internal KafkaRecord(ConsumeResult<byte[], byte[]> result)
        : this(
            result.Topic, result.Partition.Value, result.Offset.Value, result.Message.Key, result.Message.Value,
            result.Message.Timestamp.Type == TimestampType.NotAvailable
                ? null
                : new DateTimeOffset(result.Message.Timestamp.UtcDateTime, TimeSpan.Zero),
            result.Message.Headers)
    {
    }

    /// <summary>The last value of the header <paramref name="name"/>, or null.</summary>
    public byte[]? Header(string name) =>
        Headers is { } headers && headers.TryGetLastBytes(name, out var bytes) ? bytes : null;

    public override string ToString() =>
        $"{Topic}[{Partition}]@{Offset} (key {Key?.Length.ToString() ?? "null"} bytes, value {Value?.Length.ToString() ?? "null"} bytes)";
}

/// <summary>Turns a record's key or value bytes into a <typeparamref name="T"/>. It is given
/// the whole record too, for formats that need the topic or a header. Throwing marks the
/// record as unreadable; see <see cref="KafkaSource"/> for what happens to it.</summary>
public delegate T KafkaDeserializer<out T>(ReadOnlySpan<byte> data, in KafkaRecord record);

/// <summary>Turns a key or a row into the bytes a Kafka sink writes.</summary>
public delegate byte[] KafkaSerializer<in T>(T value);

/// <summary>What one record of a keyed topic means: the row to upsert, the key to delete,
/// or nothing.</summary>
public readonly struct KafkaChange<TKey, TValue>
{
    private KafkaChange(byte kind, TKey key, TValue value)
    {
        _kind = kind;
        Key = key;
        Value = value;
    }

    private readonly byte _kind; // 0 = skip, 1 = upsert, 2 = delete

    /// <summary>The key to delete; meaningful only when <see cref="IsDelete"/>.</summary>
    public TKey Key { get; }

    /// <summary>The row to upsert; meaningful only when <see cref="IsUpsert"/>. Its key is
    /// derived by the input's or the table's key selector.</summary>
    public TValue Value { get; }

    public bool IsUpsert => _kind == 1;
    public bool IsDelete => _kind == 2;
    public bool IsSkip => _kind == 0;

    /// <summary>The record carries nothing for the table: it is passed over, and its offset
    /// is covered by the next record's.</summary>
    public static KafkaChange<TKey, TValue> Skip => default;

    public static KafkaChange<TKey, TValue> Upsert(TValue value) => new(1, default!, value);

    public static KafkaChange<TKey, TValue> Delete(TKey key) => new(2, key, default!);
}

/// <summary>Reads one record of a keyed topic as an upsert, a delete or nothing. Throwing
/// marks the record as unreadable.</summary>
public delegate KafkaChange<TKey, TValue> KafkaChangeDecoder<TKey, TValue>(in KafkaRecord record);

/// <summary>The stock deserializers and decoders, and the adapters for anyone else's.</summary>
public static class KafkaDeserializers
{
    /// <summary>JSON through System.Text.Json; web defaults (camelCase, case-insensitive
    /// names) unless <paramref name="options"/> says otherwise.</summary>
    public static KafkaDeserializer<T> Json<T>(JsonSerializerOptions? options = null)
    {
        options ??= KafkaSerializers.DefaultJson;
        return (ReadOnlySpan<byte> data, in KafkaRecord _) =>
            JsonSerializer.Deserialize<T>(data, options)
            ?? throw new JsonException($"The JSON is 'null', which is not a {typeof(T).Name}.");
    }

    /// <summary>The bytes as they are (copied: the record's array is not handed out).</summary>
    public static KafkaDeserializer<byte[]> Bytes { get; } = (ReadOnlySpan<byte> data, in KafkaRecord _) => data.ToArray();

    /// <summary>The bytes as UTF-8 text.</summary>
    public static KafkaDeserializer<string> Utf8 { get; } = (ReadOnlySpan<byte> data, in KafkaRecord _) => Encoding.UTF8.GetString(data);

    /// <summary>
    /// A Confluent deserializer — which is how schema-registry formats (Avro, Protobuf, JSON
    /// Schema) plug in: build the one from <c>Confluent.SchemaRegistry.Serdes.*</c> (an
    /// <c>IAsyncDeserializer</c> becomes one of these with <c>AsSyncOverAsync()</c>) and
    /// pass it here. This package does not depend on the registry client.
    /// </summary>
    public static KafkaDeserializer<T> From<T>(IDeserializer<T> deserializer, MessageComponentType component = MessageComponentType.Value)
    {
        ArgumentNullException.ThrowIfNull(deserializer);
        return (ReadOnlySpan<byte> data, in KafkaRecord record) =>
            deserializer.Deserialize(data, false, new SerializationContext(component, record.Topic, record.Headers));
    }

    /// <summary>
    /// The plain reading of a keyed (compacted) topic: a record with a value is an upsert of
    /// that value, a record with none — a tombstone — is a delete of its key. Both default
    /// to JSON.
    /// </summary>
    public static KafkaChangeDecoder<TKey, TValue> Keyed<TKey, TValue>(
        KafkaDeserializer<TKey>? key = null, KafkaDeserializer<TValue>? value = null)
    {
        key ??= Json<TKey>();
        value ??= Json<TValue>();
        return (in KafkaRecord record) =>
        {
            if (record.Value is { } bytes)
                return KafkaChange<TKey, TValue>.Upsert(value(bytes, in record));
            if (record.Key is not { } keyBytes)
                throw new FormatException("The record has neither a key nor a value.");
            return KafkaChange<TKey, TValue>.Delete(key(keyBytes, in record));
        };
    }
}

/// <summary>The stock serializers of the Kafka sinks.</summary>
public static class KafkaSerializers
{
    internal static readonly JsonSerializerOptions DefaultJson = new(JsonSerializerDefaults.Web);

    /// <summary>JSON through System.Text.Json; web defaults unless
    /// <paramref name="options"/> says otherwise. For a key, equal keys must give equal
    /// bytes — a compacted topic finds a key by its bytes — which JSON does for integers,
    /// strings, GUIDs and records of those, and does not for <c>decimal</c> or
    /// <c>double</c>.</summary>
    public static KafkaSerializer<T> Json<T>(JsonSerializerOptions? options = null)
    {
        options ??= DefaultJson;
        return value => JsonSerializer.SerializeToUtf8Bytes(value, options);
    }

    /// <summary>The string as UTF-8 text, without JSON's quotes.</summary>
    public static KafkaSerializer<string> Utf8 { get; } = Encoding.UTF8.GetBytes;

    /// <summary>A Confluent serializer; see <see cref="KafkaDeserializers.From"/>.</summary>
    public static KafkaSerializer<T> From<T>(ISerializer<T> serializer, string topic, MessageComponentType component = MessageComponentType.Value)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        return value => serializer.Serialize(value, new SerializationContext(component, topic));
    }
}
