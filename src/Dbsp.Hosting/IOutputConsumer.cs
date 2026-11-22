namespace Dbsp.Hosting;

/// <summary>
/// Consumes one output's changefeed under the host (see
/// <c>AddOutputConsumer</c> on <see cref="HostedCircuitBuilder{TDefinition}"/>). Batches
/// arrive in step order, one call at a time; the circuit waits on a slow consumer — that is
/// its backpressure — so do the work, or hand it to something bounded, before returning.
/// On a table-shaped output the first batch is the current integrated state: treat it as a
/// reset. Throwing is treated like a circuit fault (<see cref="CircuitFaultBehavior"/>),
/// because an output nobody reads stalls the whole circuit.
/// </summary>
public interface IOutputConsumer<T> where T : notnull
{
    /// <summary>Handles one batch. <paramref name="cancellationToken"/> fires only when
    /// shutdown has run out of time: a normal stop lets the consumer see every batch the
    /// draining circuit still produces.</summary>
    ValueTask ConsumeAsync(ZSet<T> batch, CancellationToken cancellationToken);
}

/// <summary>
/// Delivers one acknowledged output to a sink under the host (see
/// <c>AddAcknowledgedOutputConsumer</c> on <see cref="HostedCircuitBuilder{TDefinition}"/>).
/// Batches arrive in step order, one call at a time, and a batch is acknowledged when its
/// call returns: return only once the sink has it. The circuit commits no checkpoint past
/// an unacknowledged batch, so after a crash — or after this method throws, which faults
/// the circuit — the batches since the last checkpoint arrive again, under the same
/// <see cref="OutputBatch{T}.Sequence"/> numbers. Apply them idempotently.
/// </summary>
public interface IAcknowledgedOutputConsumer<T> where T : notnull
{
    /// <summary>Delivers one batch. <paramref name="cancellationToken"/> fires only when
    /// shutdown has run out of time.</summary>
    ValueTask ConsumeAsync(OutputBatch<T> batch, CancellationToken cancellationToken);
}

/// <summary>One registered consumer of a hosted circuit's output; the circuit's host runs
/// them all, so that they outlive the circuit's drain at shutdown.</summary>
internal abstract class OutputConsumerRegistration<TDefinition> where TDefinition : class
{
    public abstract Task RunAsync(CircuitHost<TDefinition> host, IServiceProvider services, CancellationToken cancellationToken);

    /// <summary>Reads and discards the rest of the output, after its consumer has failed, so
    /// the circuit can still drain and shut down instead of stalling on it.</summary>
    public abstract Task DiscardAsync(CircuitHost<TDefinition> host, CancellationToken cancellationToken);

    /// <summary>Called when shutdown has run out of time, before the consumers are cut
    /// loose: lets a registration release whatever the circuit's drain is waiting on.</summary>
    public virtual void Abandon(CircuitHost<TDefinition> host)
    {
    }

    /// <summary>The log line for a consumer that threw; <c>{Circuit}</c> is its one argument.</summary>
    public virtual string FailureMessage =>
        "An output consumer of circuit {Circuit} threw; the rest of that output is being discarded";
}

internal sealed class OutputConsumerRegistration<TDefinition, T>(
    Func<TDefinition, Output<T>> select,
    Func<IServiceProvider, Func<ZSet<T>, CancellationToken, ValueTask>> handlerFactory)
    : OutputConsumerRegistration<TDefinition>
    where TDefinition : class
    where T : notnull
{
    public override async Task RunAsync(CircuitHost<TDefinition> host, IServiceProvider services, CancellationToken cancellationToken)
    {
        var handler = handlerFactory(services);
        await foreach (var batch in host.Handle(select).Changes(cancellationToken).ConfigureAwait(false))
            await handler(batch, cancellationToken).ConfigureAwait(false);
    }

    public override async Task DiscardAsync(CircuitHost<TDefinition> host, CancellationToken cancellationToken)
    {
        await foreach (var _ in host.Handle(select).Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
        }
    }
}

internal sealed class AcknowledgedOutputConsumerRegistration<TDefinition, T>(
    Func<TDefinition, AcknowledgedOutput<T>> select,
    Func<IServiceProvider, Func<OutputBatch<T>, CancellationToken, ValueTask>> sinkFactory)
    : OutputConsumerRegistration<TDefinition>
    where TDefinition : class
    where T : notnull
{
    public override Task RunAsync(CircuitHost<TDefinition> host, IServiceProvider services, CancellationToken cancellationToken) =>
        host.Handle(select).ConsumeAsync(sinkFactory(services), cancellationToken);

    /// <summary>Nothing to discard, and nothing that may be: reading the rest on the failed
    /// sink's behalf would let the checkpoint pass batches that were never delivered. The
    /// sink's failure ended the enumeration, which already faulted the circuit; it stops
    /// there, uncommitted, and the restart delivers those batches again.</summary>
    public override Task DiscardAsync(CircuitHost<TDefinition> host, CancellationToken cancellationToken) =>
        Task.CompletedTask;

    /// <summary>The drain may be at its final commit, waiting for acknowledgements that
    /// will not come now: end that wait, so the commit is skipped rather than awaited.</summary>
    public override void Abandon(CircuitHost<TDefinition> host) => host.Handle(select).Abandon();

    public override string FailureMessage =>
        "An acknowledged output's sink of circuit {Circuit} threw; the circuit stops without committing past the " +
        "failed batch, and a restart delivers it again";
}

internal sealed class DeadLetterConsumerRegistration<TDefinition>(Func<DeadLetter, CancellationToken, ValueTask> handler)
    : OutputConsumerRegistration<TDefinition>
    where TDefinition : class
{
    public override async Task RunAsync(CircuitHost<TDefinition> host, IServiceProvider services, CancellationToken cancellationToken)
    {
        await foreach (var letter in host.Circuit.DeadLetters.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            await handler(letter, cancellationToken).ConfigureAwait(false);
    }

    public override async Task DiscardAsync(CircuitHost<TDefinition> host, CancellationToken cancellationToken)
    {
        await foreach (var _ in host.Circuit.DeadLetters.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
        }
    }

    public override string FailureMessage =>
        "The dead-letter consumer of circuit {Circuit} threw; the rest of the dead letters are being discarded";
}
