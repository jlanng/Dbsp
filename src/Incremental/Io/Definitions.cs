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
    internal KeyedInputStream(CircuitBuilder builder, int nodeId, string name)
        : base(builder, nodeId)
    {
        InputName = name;
    }

    internal string InputName { get; }
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
