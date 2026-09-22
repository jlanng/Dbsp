namespace Incremental.Hosting;

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

/// <summary>One registered consumer of a hosted circuit's output; the circuit's host runs
/// them all, so that they outlive the circuit's drain at shutdown.</summary>
internal abstract class OutputConsumerRegistration<TDefinition> where TDefinition : class
{
    public abstract Task RunAsync(CircuitHost<TDefinition> host, IServiceProvider services, CancellationToken cancellationToken);

    /// <summary>Reads and discards the rest of the output, after its consumer has failed, so
    /// the circuit can still drain and shut down instead of stalling on it.</summary>
    public abstract Task DiscardAsync(CircuitHost<TDefinition> host, CancellationToken cancellationToken);
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
