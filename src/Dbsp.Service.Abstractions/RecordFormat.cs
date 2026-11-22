using System.Text.Json;

namespace Dbsp.Service;

/// <summary>
/// A record format of the Kafka connectors: what the bytes of a topic's keys and values
/// are, for a topic that does not carry the built-in JSON — Avro, Protobuf, JSON behind a
/// schema registry's framing. The host registers one under a name
/// (<c>AddRecordFormat("avro", …)</c>) and a slot or a <c>kafka</c> table loader chooses it
/// with <c>"format": "avro"</c>; without one it is <c>json</c>.
/// </summary>
/// <remarks>
/// <para>A format is made once per process and asked for a codec per slot, per key or
/// value, per generation of the pipeline (and per run of a table loader): the codec is the
/// generation's and goes with it. The type a codec is made for is often a plugin's —
/// a sink's row type, an input's record — and a plugin's types live in a load context that
/// unloads only when nothing reaches them. So <b>a format keeps nothing typed by the
/// type it is asked for</b>: no cache keyed by type, no static holding
/// a codec, a serializer or a schema derived from the type. A codec may keep what it
/// likes; a cache that must outlive a generation keeps text (a schema's JSON, a subject's
/// name), never a <see cref="Type"/>.</para>
/// <para>A record the codec cannot read — it throws — is unreadable, exactly as a record of
/// broken JSON is: a source rejects it (its offset is consumed and the pipeline's error
/// policy decides: a dead letter or a fault), a table loader reports it and its table's
/// <c>onUnreadable</c> decides. A codec that cannot write a value throws too, and the sink's
/// write fails as any failed write does.</para>
/// </remarks>
public interface IRecordFormat
{
    /// <summary>A codec of <typeparamref name="T"/> for one part (key or value) of one
    /// slot's or loader's records. Called when the slot's connector starts, never per
    /// record.</summary>
    IRecordCodec<T> CreateCodec<T>(RecordFormatContext context);
}

/// <summary>Reads and writes one part of a topic's records as <typeparamref name="T"/>.
/// Made for one slot of one generation by <see cref="IRecordFormat.CreateCodec{T}"/>; a
/// source's codec is called from the source's one reading thread, a sink's from its
/// one write at a time, so it need not be thread-safe.</summary>
public interface IRecordCodec<T>
{
    /// <summary>The value the bytes hold. Throwing marks the record unreadable.</summary>
    T Read(ReadOnlySpan<byte> data, in RecordOrigin origin);

    /// <summary>The bytes a sink writes for <paramref name="value"/>. For a key, equal keys
    /// must give equal bytes: a compacted topic tells keys apart by their bytes.</summary>
    byte[] Write(T value);
}

/// <summary>Which part of a record a codec reads or writes.</summary>
public enum RecordPart { Key, Value }

/// <summary>Where a record being read lies: its topic, partition and offset, and the
/// broker's timestamp when there is one.</summary>
public readonly record struct RecordOrigin(string Topic, int Partition, long Offset, DateTimeOffset? Timestamp);

/// <summary>What a codec is made for.</summary>
public sealed class RecordFormatContext
{
    /// <summary>The topic the codec reads or writes.</summary>
    public required string Topic { get; init; }

    /// <summary>Key or value.</summary>
    public required RecordPart Part { get; init; }

    /// <summary>The pipeline whose slot this is, or the table the loader fills.</summary>
    public required string Owner { get; init; }

    /// <summary>The slot's name; for a table loader, the table's.</summary>
    public required string Slot { get; init; }

    /// <summary>The options the service gives the built-in JSON for this slot: the
    /// generation's own for a pipeline (they hold metadata of its plugin's types, and go
    /// with it), the process's for a table's contract types. A format that carries JSON
    /// inside its framing uses these, so that it reads members as the built-in does.</summary>
    public required JsonSerializerOptions Json { get; init; }

    /// <summary>The host's services, for what a format needs at run time (a schema
    /// registry's client). Resolved here, never held in a static.</summary>
    public required IServiceProvider Services { get; init; }
}
