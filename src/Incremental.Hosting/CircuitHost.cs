using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Incremental.Hosting;

/// <summary>
/// A hosted circuit, keyed in the container by its definition class: inject
/// <c>CircuitHost&lt;PageAnalytics&gt;</c> wherever you feed or read that circuit and redeem
/// the definition's members for live handles — <c>host.Handle(d =&gt; d.Views)</c>.
/// <para>
/// The circuit is <em>built</em> when the host object is first resolved, so handles are
/// valid from any constructor, and <em>started</em> (checkpoint recovery, workers, pump) by
/// the generic host; records sent before that simply wait in the input's bounded channel.
/// At shutdown the host completes the inputs and drains the circuit while the registered
/// output consumers and views keep reading, and only then stops them. With an acknowledged
/// output the drain ends with the circuit's final checkpoint waiting for the consumer to
/// acknowledge the last batches, inside the host's shutdown timeout; if the timeout wins,
/// that commit is skipped and those steps are delivered again after the restart. Your own services
/// should follow the same rule: a producer stops sending when its stopping token fires; a
/// consumer written by hand reads its output <em>to completion</em> rather than cancelling
/// on the stopping token — an output nobody reads blocks the drain until the host's
/// shutdown timeout gives up on it. <c>AddOutputConsumer</c> and <see cref="View{T}"/> get
/// this right for you.
/// </para>
/// </summary>
public sealed class CircuitHost<TDefinition> : IAsyncDisposable where TDefinition : class
{
    private readonly IServiceProvider _services;
    private readonly IHostApplicationLifetime? _lifetime;
    private readonly ILogger _log;
    private readonly CircuitFaultBehavior _faultBehavior;
    private readonly OutputConsumerRegistration<TDefinition>[] _consumerRegistrations;
    private readonly ConcurrentDictionary<object, Lazy<IAsyncDisposable>> _views = new();
    private readonly CancellationTokenSource _abandon = new();
    private readonly List<Task> _consumers = [];
    private int _started;
    private int _stopped;
    private bool _abandoned; // shutdown ran out of time

    internal CircuitHost(
        IServiceProvider services,
        Func<IServiceProvider, CircuitBuilder, TDefinition> definitionFactory)
    {
        _services = services;
        _lifetime = services.GetService<IHostApplicationLifetime>();
        var loggerFactory = services.GetService<ILoggerFactory>();
        _log = (loggerFactory ?? NullLoggerFactory.Instance).CreateLogger("Incremental.Hosting");
        _consumerRegistrations = services.GetServices<OutputConsumerRegistration<TDefinition>>().ToArray();

        var options = services.GetRequiredService<IOptionsMonitor<HostedCircuitOptions>>().Get(OptionsName);
        _faultBehavior = options.FaultBehavior;

        var builder = new CircuitBuilder(options.ToCircuitOptions(typeof(TDefinition).Name, loggerFactory));
        Definition = definitionFactory(services, builder);
        Circuit = builder.Build();
    }

    /// <summary>The named-options key of this circuit's <see cref="HostedCircuitOptions"/>.</summary>
    internal static string OptionsName => typeof(TDefinition).FullName ?? typeof(TDefinition).Name;

    /// <summary>The definition instance this circuit was built from.</summary>
    public TDefinition Definition { get; }

    /// <summary>The circuit itself — <see cref="Incremental.Circuit.Fault"/>,
    /// <see cref="Incremental.Circuit.CommittedStep"/>, the watermark, and
    /// <see cref="Incremental.Circuit.StepAsync"/> when <see cref="HostedCircuitOptions.AutoStep"/>
    /// is off. The host owns its lifetime: do not start or dispose it.</summary>
    public Circuit Circuit { get; }

    /// <summary>Whether the generic host has started the circuit.</summary>
    public bool IsStarted => Volatile.Read(ref _started) != 0;

    /// <summary>Redeems one of the definition's inputs for its live handle.</summary>
    public InputHandle<T> Handle<T>(Func<TDefinition, InputStream<T>> select) where T : notnull =>
        Circuit.Handle(select(Definition));

    /// <summary>Redeems one of the definition's keyed inputs for its live upsert/delete handle.</summary>
    public KeyedInputHandle<TKey, TValue> Handle<TKey, TValue>(Func<TDefinition, KeyedInputStream<TKey, TValue>> select)
        where TKey : notnull
        where TValue : notnull =>
        Circuit.Handle(select(Definition));

    /// <summary>Redeems one of the definition's outputs for its live handle. An output has
    /// exactly one consumer: one registered with <c>AddOutputConsumer</c>, or a
    /// <see cref="View{T}"/>, already is it.</summary>
    public OutputHandle<T> Handle<T>(Func<TDefinition, Output<T>> select) where T : notnull =>
        Circuit.Handle(select(Definition));

    /// <summary>Redeems one of the definition's acknowledged outputs for its live handle.
    /// One consumer here too: one registered with <c>AddAcknowledgedOutputConsumer</c>
    /// already is it.</summary>
    public AcknowledgedOutputHandle<T> Handle<T>(Func<TDefinition, AcknowledgedOutput<T>> select) where T : notnull =>
        Circuit.Handle(select(Definition));

    /// <summary>The in-memory view of a table-shaped output
    /// (<see cref="OutputHandle{T}.MaterializeInMemory"/>), created on first use and shared
    /// from then on — the usual way to serve a circuit's results from a request handler. The
    /// view is that output's single consumer, and the host disposes it after the circuit has
    /// drained.</summary>
    public InMemoryView<T> View<T>(Func<TDefinition, Output<T>> select) where T : notnull
    {
        var output = select(Definition);
        return (InMemoryView<T>)_views.GetOrAdd(
            output,
            _ => new Lazy<IAsyncDisposable>(() => Circuit.Handle(output).MaterializeInMemory())).Value;
    }

    internal Task StartAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
            return Task.CompletedTask;
        cancellationToken.ThrowIfCancellationRequested();

        // Synchronous on purpose: recovery finishes before the host reports started, so
        // nothing serves traffic against a circuit that has not resumed yet.
        Circuit.Start();

        foreach (var registration in _consumerRegistrations)
            _consumers.Add(RunConsumerAsync(registration));
        _ = WatchForFaultAsync();
        return Task.CompletedTask;
    }

    private async Task RunConsumerAsync(OutputConsumerRegistration<TDefinition> registration)
    {
        try
        {
            await Task.Yield();
            await registration.RunAsync(this, _services, _abandon.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_abandon.IsCancellationRequested)
        {
            // Shutdown ran out of time; StopAsync has already said so.
        }
        catch when (Circuit.Fault is not null)
        {
            // The feed relayed the circuit's fault; WatchForFaultAsync owns that report.
        }
        catch (Exception ex)
        {
            // The output has lost its consumer, so its results are gone from here on: that
            // gets the same treatment as a fault. What remains is read and discarded, so the
            // circuit can still drain at shutdown instead of stalling on this output.
            OnTerminalFailure(ex, registration.FailureMessage);
            try
            {
                await registration.DiscardAsync(this, _abandon.Token).ConfigureAwait(false);
            }
            catch
            {
                // Abandoned at the shutdown timeout, or the circuit faulted: both reported.
            }
        }
    }

    private async Task WatchForFaultAsync()
    {
        try
        {
            await Circuit.Completion.ConfigureAwait(false);
        }
        catch when (Volatile.Read(ref _abandoned))
        {
            // Shutdown ran out of time and cut the consumers loose; an acknowledged output
            // losing its consumer that way faults the circuit so that its drain ends without
            // committing. StopAsync has already reported the abandoned shutdown.
        }
        catch (Exception ex)
        {
            OnTerminalFailure(ex, "Circuit {Circuit} faulted; it is terminal — restart the process to resume from the last committed checkpoint");
        }
    }

    private void OnTerminalFailure(Exception exception, string message)
    {
        _log.LogCritical(exception, message, Circuit.Name);
        if (_faultBehavior != CircuitFaultBehavior.StopHost || _lifetime is null || Volatile.Read(ref _stopped) != 0)
            return;

        // The generic host exits 0 from StopApplication; a supervisor needs to see a failure.
        if (Environment.ExitCode == 0)
            Environment.ExitCode = 1;
        _lifetime.StopApplication();
    }

    /// <summary>Completes the inputs, drains the circuit while the consumers and views keep
    /// reading, then stops them. <paramref name="cancellationToken"/> is the host's shutdown
    /// timeout: when it fires the drain is abandoned, loudly.</summary>
    internal async Task StopAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
            return;

        try
        {
            await Circuit.DisposeAsync().AsTask().WaitAsync(cancellationToken).ConfigureAwait(false);
            await Task.WhenAll(_consumers).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _log.LogWarning(
                "Circuit {Circuit} did not drain within the host's shutdown timeout and was abandoned mid-drain; " +
                "the usual cause is an output with no consumer still reading it. State up to the last committed " +
                "checkpoint is safe.",
                Circuit.Name);

            // Before the consumers are cancelled, so the fault this may cause is already
            // known to be shutdown's doing (see WatchForFaultAsync).
            Volatile.Write(ref _abandoned, true);
            foreach (var registration in _consumerRegistrations)
                registration.Abandon(this);
        }
        finally
        {
            _abandon.Cancel();
        }

        foreach (var view in _views.Values)
            await view.Value.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Stops the circuit if the generic host has not already — the container calls
    /// this, which covers a provider that is disposed without ever running a host.</summary>
    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None).ConfigureAwait(false);
        _abandon.Dispose();
    }
}

/// <summary>Hands the circuit's lifecycle to the generic host.</summary>
internal sealed class CircuitHostedService<TDefinition>(CircuitHost<TDefinition> host) : IHostedService
    where TDefinition : class
{
    public Task StartAsync(CancellationToken cancellationToken) => host.StartAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken) => host.StopAsync(cancellationToken);
}
