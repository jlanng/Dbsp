namespace Incremental;

/// <summary>
/// The definition of a circuit input: a stream fed from outside. Redeem it with
/// <see cref="Circuit.Handle{T}(InputStream{T})"/> after <c>Build()</c> for the live
/// <see cref="InputHandle{T}"/>. Inputs are the one named thing in a circuit — the name
/// identifies the external feed whose checkpointed offsets must survive restarts and
/// topology edits; everything downstream is identified by reference.
/// </summary>
public sealed class InputStream<T> : ZStream<T> where T : notnull
{
    internal InputStream(CircuitBuilder builder, int nodeId, string name)
        : base(builder, nodeId)
    {
        InputName = name;
    }

    internal string InputName { get; }

    private protected override string? InputNameOrNull => InputName;
}

/// <summary>
/// The definition of a keyed circuit input (upsert/delete-by-key semantics). Use it as the
/// stream of current rows; redeem it with
/// <see cref="Circuit.Handle{TKey,TValue}(KeyedInputStream{TKey,TValue})"/> after
/// <c>Build()</c> for the live <see cref="KeyedInputHandle{TKey,TValue}"/>.
/// </summary>
public sealed class KeyedInputStream<TKey, TValue> : ZStream<TValue>
    where TKey : notnull
    where TValue : notnull
{
    private readonly KeyedInputRetention<TValue> _retention;

    internal KeyedInputStream(CircuitBuilder builder, int nodeId, string name, KeyedInputRetention<TValue> retention)
        : base(builder, nodeId)
    {
        InputName = name;
        _retention = retention;
    }

    internal string InputName { get; }

    private protected override string? InputNameOrNull => InputName;

    /// <summary>
    /// Declares event time on the keyed input's rows, as
    /// <see cref="ZStream{T}.WithEventTime(Func{T,DateTimeOffset},TimeSpan)"/> does, and with
    /// <paramref name="evictWhenFinal"/> also bounds the input's own state: the current row
    /// of a key is forgotten once the watermark has passed its event time. That changes
    /// nothing anyone sees — the retraction an upsert of that key would have owed is behind
    /// the watermark and dropped here either way — and keeps the input from holding one row
    /// for every key it ever had. A later upsert of a forgotten key is a new row.
    /// <para>
    /// So that this does not depend on how the records fall into steps, an upsert whose
    /// event time is later than the held row's by more than <paramref name="lateness"/>
    /// is a new row too, whether or not the watermark has passed the held one yet: the
    /// held row is final by the newer one's own time, it is not retracted, and the input
    /// never runs the two in one step. An upsert within <paramref name="lateness"/> of
    /// the held row amends it — and whether the amended row is still taken back
    /// downstream depends on where the watermark stands when the amendment is read. A
    /// key that is used again for another record must therefore not come back within
    /// <paramref name="lateness"/> of event time, and an amendment must not move a row's
    /// event time forward by more than that.
    /// </para>
    /// <para>
    /// With <paramref name="evictWhenFinal"/> every use of the input must go through the
    /// stream this returns; <c>Build()</c> refuses a circuit that also reads the input
    /// itself, which would see upserts without their retractions.
    /// </para>
    /// </summary>
    public EventTimeStream<TValue> WithEventTime(Func<TValue, DateTimeOffset> eventTime, TimeSpan lateness, bool evictWhenFinal)
    {
        var timed = WithEventTime(eventTime, lateness);
        if (!evictWhenFinal)
            return timed;
        if (_retention.Policy is not null)
            throw new InvalidOperationException($"Keyed input '{InputName}' already forgets its final rows.");
        _retention.Policy = new Operators.RecordEviction<TValue>(Builder.WatermarkClock!, eventTime) { OpenTicks = lateness.Ticks };
        _retention.WatermarkNode = timed.NodeId;
        return timed;
    }
}

/// <summary>What a keyed input knows of its own eviction (<c>evictWhenFinal</c>), set
/// after the input is declared and read when its operator is made.</summary>
internal abstract class KeyedInputRetention
{
    public required string InputName { get; init; }
    public int UpsertNode { get; set; } = -1;
    public int WatermarkNode { get; set; } = -1;
}

internal sealed class KeyedInputRetention<TValue> : KeyedInputRetention where TValue : notnull
{
    public Operators.RecordEviction<TValue>? Policy { get; set; }
}

/// <summary>
/// The definition of a circuit output, returned by <c>ToOutput()</c>. Redeem it with
/// <see cref="Circuit.Handle{T}(Output{T})"/> after <c>Build()</c> for the live
/// <see cref="OutputHandle{T}"/>. Outputs are unnamed: they carry no durable identity, so
/// this reference is the identity.
/// </summary>
public sealed class Output<T> where T : notnull
{
    internal Output(CircuitBuilder builder, string registrationName)
    {
        Builder = builder;
        RegistrationName = registrationName;
    }

    internal CircuitBuilder Builder { get; }
    internal string RegistrationName { get; }
}

/// <summary>
/// The definition of an acknowledged circuit output, returned by
/// <c>ToAcknowledgedOutput()</c>. Redeem it with
/// <see cref="Circuit.Handle{T}(AcknowledgedOutput{T})"/> after <c>Build()</c> for the live
/// <see cref="AcknowledgedOutputHandle{T}"/>.
/// </summary>
public sealed class AcknowledgedOutput<T> where T : notnull
{
    internal AcknowledgedOutput(CircuitBuilder builder, string registrationName)
    {
        Builder = builder;
        RegistrationName = registrationName;
    }

    internal CircuitBuilder Builder { get; }
    internal string RegistrationName { get; }
}
