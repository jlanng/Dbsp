namespace Incremental.Service;

/// <summary>When a faulting pipeline, loader or table is tried again and when it is given up on.</summary>
public sealed record RestartPolicy
{
    /// <summary>More faults than this within <see cref="Within"/> end in <c>Failed</c>.</summary>
    public int MaxRestarts { get; init; } = 5;
    public TimeSpan Within { get; init; } = TimeSpan.FromMinutes(10);
    public TimeSpan InitialBackoff { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan MaxBackoff { get; init; } = TimeSpan.FromMinutes(1);

    /// <summary>The wait before the restart that follows the <paramref name="faults"/>th
    /// fault in the window: doubling from the initial backoff to the maximum.</summary>
    public TimeSpan BackoffAfter(int faults)
    {
        var factor = Math.Pow(2, Math.Clamp(faults - 1, 0, 30));
        return TimeSpan.FromTicks((long)Math.Min(MaxBackoff.Ticks, InitialBackoff.Ticks * factor));
    }
}

public sealed class ServiceOptions
{
    /// <summary>Overrides the backend's node id in logs and events when set.</summary>
    public string? NodeName { get; set; }

    /// <summary>An import source: at first lead every plugin package in it (a directory or
    /// a zip per package) that the catalog lacks is imported. Nodes load from the catalog
    /// and the blob store, never from each other's disks.</summary>
    public string? PluginDirectory { get; set; }

    /// <summary>Where plugin files are materialised from the blob store before loading.
    /// Defaults to a directory under the system's temporary path.</summary>
    public string? CacheDirectory { get; set; }

    /// <summary>What <c>node/leader</c> publishes as this node's admin address.</summary>
    public string? AdvertisedAdminUrl { get; set; }

    public TimeSpan CatalogPollInterval { get; set; } = TimeSpan.FromMilliseconds(100);
    public TimeSpan TrimInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>History kept for followers whatever the readers have committed: the leader
    /// cannot see where a standby's followers stand, so it keeps this much time of versions.</summary>
    public TimeSpan FollowerRetention { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>The drain a pipeline's teardown is given before its outputs are abandoned.</summary>
    public TimeSpan StopTimeout { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>What an abandoned circuit is given to finish disposing before the pipeline
    /// is <c>Wedged</c>.</summary>
    public TimeSpan AbandonTimeout { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan StepDownTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>A sink write in flight for longer than this shows as <c>sink-ack</c>.</summary>
    public TimeSpan StallAfter { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>How often a blocked pipeline looks again for the table it waits on.</summary>
    public TimeSpan BlockedRetryInterval { get; set; } = TimeSpan.FromSeconds(1);

    public int MaxCatalogValueBytes { get; set; } = 256 * 1024;
    public int AuditEntries { get; set; } = 1000;
    public int EventBuffer { get; set; } = 500;
    public int DeadLetterBuffer { get; set; } = 256;
    public int FaultBuffer { get; set; } = 16;
    public int UnreadableBuffer { get; set; } = 64;

    /// <summary>Plugin load contexts alive at once, counting those asked to unload and not
    /// yet collected. A node refuses to load another version past this.</summary>
    public int MaxLoadedPluginVersions { get; set; } = 64;
    public int MaxPluginFiles { get; set; } = 512;
    public long MaxPluginBytes { get; set; } = 128L << 20;
    public long MaxStoredPluginBytes { get; set; } = 512L << 20;

    /// <summary>The restart policy of table loaders and faulted tables, and the default
    /// for a pipeline whose spec names none.</summary>
    public RestartPolicy Restart { get; set; } = new();

    /// <summary>Whether losing leadership (fenced: exit code 3; resigned: 4) stops the
    /// host application. A process supervisor then restarts it as a standby.</summary>
    public bool ExitOnLeadershipLoss { get; set; } = true;
}

/// <summary>Something that happened on this node: a role change, a pipeline or table
/// transition, a catalog version. Kept in a ring and pushed to listeners.</summary>
public sealed record ServiceEvent(long Sequence, DateTimeOffset Time, string Kind, string Subject, string Message);

internal sealed class EventRing(int capacity)
{
    private readonly Queue<ServiceEvent> _events = new();
    private long _sequence;

    public event Action<ServiceEvent>? Published;

    public void Publish(string kind, string subject, string message)
    {
        ServiceEvent published;
        lock (_events)
        {
            published = new ServiceEvent(++_sequence, DateTimeOffset.UtcNow, kind, subject, message);
            _events.Enqueue(published);
            while (_events.Count > capacity)
                _events.Dequeue();
        }
        try
        {
            Published?.Invoke(published);
        }
        catch
        {
            // A listener's failure is not the publisher's.
        }
    }

    public IReadOnlyList<ServiceEvent> After(long sequence)
    {
        lock (_events)
            return _events.Where(e => e.Sequence > sequence).ToList();
    }
}

/// <summary>A fixed ring of the newest items, each with a sequence number.</summary>
internal sealed class Ring<T>(int capacity)
{
    private readonly Queue<(long Sequence, T Item)> _items = new();
    private long _sequence;

    public long Total => Interlocked.Read(ref _sequence);

    public long Add(T item) => Add(_ => item);

    public long Add(Func<long, T> item)
    {
        lock (_items)
        {
            var sequence = ++_sequence;
            _items.Enqueue((sequence, item(sequence)));
            while (_items.Count > capacity)
                _items.Dequeue();
            return sequence;
        }
    }

    public IReadOnlyList<T> After(long sequence = 0, int limit = int.MaxValue)
    {
        lock (_items)
            return _items.Where(i => i.Sequence > sequence).Select(i => i.Item).Take(limit).ToList();
    }
}

internal static class TaskExtensions
{
    /// <summary>Whether <paramref name="task"/> finished within <paramref name="timeout"/>.
    /// It keeps running if it did not; a fault is the caller's to observe.</summary>
    public static async Task<bool> CompletesWithin(this Task task, TimeSpan timeout)
    {
        if (task.IsCompleted)
            return true;
        using var cancel = new CancellationTokenSource();
        var finished = await Task.WhenAny(task, Task.Delay(timeout, cancel.Token)).ConfigureAwait(false);
        cancel.Cancel();
        return finished == task;
    }

    /// <summary>Observes a fault nobody else will, so it is not an unobserved exception.</summary>
    public static void Forget(this Task task) =>
        task.ContinueWith(static t => GC.KeepAlive(t.Exception), TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);
}
