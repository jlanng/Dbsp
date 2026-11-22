namespace Dbsp;

/// <summary>
/// A live, in-memory materialization of a table-shaped output (see
/// <see cref="OutputHandle{T}.MaterializeInMemory"/>): a background consumer integrates the
/// output's changefeed — bootstrap batch first, so the view is correct across restarts and
/// mid-run attachment — and <see cref="Current"/> serves consistent snapshots on demand.
/// Nothing re-emits the full result per update; the snapshot cost is paid only when asked
/// for. Memory is O(live rows) for the lifetime of the view.
/// </summary>
public sealed class InMemoryView<T> : IAsyncDisposable where T : notnull
{
    private readonly ZSet<T> _current = new();
    private readonly object _lock = new();
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _run;

    internal InMemoryView(Func<CancellationToken, IAsyncEnumerable<ZSet<T>>> changes)
    {
        _run = Task.Run(async () =>
        {
            await foreach (var batch in changes(_stop.Token).ConfigureAwait(false))
            {
                lock (_lock)
                    _current.AddAll(batch);
            }
        });
    }

    /// <summary>Completes when the output completes (circuit disposed), faults when the
    /// circuit faults. Await it to observe the final state — or the failure.</summary>
    public Task Completion => _run;

    /// <summary>
    /// A consistent snapshot of the current result: each live row exactly once, with its
    /// weight (a row asserted n times appears once with <c>Weight</c> = n). Throws the
    /// circuit's fault instead of serving a stale view if the feed has failed.
    /// </summary>
    public IReadOnlyList<WeightedValue<T>> Current()
    {
        if (_run.IsFaulted)
            _run.GetAwaiter().GetResult(); // surface the original fault

        lock (_lock)
        {
            var snapshot = new List<WeightedValue<T>>(_current.DistinctCount);
            foreach (var entry in _current)
                snapshot.Add(entry);
            return snapshot;
        }
    }

    /// <summary>Stops the background consumer and waits for it to finish. Disposing the
    /// view before the circuit completes releases the output's backpressure permanently —
    /// dispose the circuit first in normal shutdown.</summary>
    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        try
        {
            await _run.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Stopped on request.
        }
        catch
        {
            // A circuit fault already surfaced on Completion and Current().
        }
        _stop.Dispose();
    }
}
