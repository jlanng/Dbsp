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
    /// Defaults to a directory under the system's temporary path that belongs to this
    /// process: it is deleted when the node is disposed, and a node that starts deletes the
    /// ones left by processes that no longer run. A directory set here is kept between
    /// runs, so a restart loads from disk; versions the catalog no longer holds are pruned
    /// from it.</summary>
    public string? CacheDirectory { get; set; }

    /// <summary>What <c>node/leader</c> publishes as this node's admin address.</summary>
    public string? AdvertisedAdminUrl { get; set; }

    public TimeSpan CatalogPollInterval { get; set; } = TimeSpan.FromMilliseconds(100);
    public TimeSpan TrimInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>History kept for followers whatever the readers have committed: the leader
    /// cannot see where a standby's followers stand, so it keeps this much time of versions.</summary>
    public TimeSpan FollowerRetention { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// How long a table's loader or a pipeline's source may say that what it reads is not
    /// there (<c>waiting for …</c>) before that is an event and a reason the node is
    /// <c>Degraded</c> (<c>loader-waiting</c>, <c>source-waiting</c>). A file that is
    /// about to be delivered is not a fault; a path with a typing mistake in it never
    /// arrives, and this is when the node says so.
    /// </summary>
    public TimeSpan MissingInputGrace { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long a pipeline's state may grow without a pause before the node says so: when
    /// the entries it holds, sampled every fifteenth of this, have not fallen once in this
    /// long and rose at no fewer than twelve of the fifteen samples, the pipeline is flagged
    /// (<see cref="PipelineStatus.StateGrowingSince"/>, an event, the overview's attention
    /// list, the gauge <c>incremental.service.pipeline.state_growing</c>). A state that is
    /// bounded rises and falls once it is full; one that grows at every sample for half an
    /// hour is keeping something per record that nothing closes. Read from the counts the
    /// node already keeps, never by walking a store. Zero turns it off.
    /// </summary>
    public TimeSpan StateGrowthAlarmAfter { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>The drain a pipeline's teardown is given before its outputs are abandoned.</summary>
    public TimeSpan StopTimeout { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>What an abandoned circuit is given to finish disposing before the pipeline
    /// is <c>Wedged</c>.</summary>
    public TimeSpan AbandonTimeout { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan StepDownTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>What a connector's own call is given — a source's <c>StartAsync</c>, a
    /// sink connector's <c>ResetAsync</c> — before the pipeline is treated as faulted and
    /// tried again. These run inside a takeover, which waits for every pipeline's start, so
    /// a connector that connects without a deadline of its own is bounded by this one.</summary>
    public TimeSpan ConnectorTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How a sink's failed writes are retried before the pipeline is treated as
    /// faulted — every sink slot's, and the dead-letter sink's. Null: the core's default
    /// (five retries, 100 ms doubling to 5 s).</summary>
    public SinkDeliveryOptions? SinkDelivery { get; set; }

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

    /// <summary>Whether losing leadership (fenced: exit code 3; resigned: 4; a takeover
    /// that failed after the node had taken the log: 5) stops the host application. A
    /// process supervisor then restarts it as a standby.</summary>
    public bool ExitOnLeadershipLoss { get; set; } = true;

    /// <summary>
    /// Whether a standby builds each pipeline's circuit on the state it follows, so that a
    /// takeover starts circuits instead of building them — which is what keeps a takeover's
    /// length from growing with the state. It means the plugin's code is loaded and its
    /// definitions run on the standby (the build, never a step). False: a standby holds
    /// pipeline state as bytes, loads no plugin code, and a takeover decodes the state of
    /// every pipeline — about a second per million entries of the largest one.
    /// </summary>
    public bool WarmStandbyPipelines { get; set; } = true;

    /// <summary>
    /// How often the node gives back memory it holds and does not use: the Kafka
    /// client's freed buffers, which the C allocator keeps, and the regions the collector
    /// holds on to after a burst (<c>docs/enrichment-service.md</c>, <i>Sizing a node</i>).
    /// The first is a call that costs about 0.1 s per 2 GB returned; the second is some
    /// young-generation collections, asked for only while the collector has committed
    /// well over what is live and is collecting seldom. Zero: never. Default 30 s.
    /// </summary>
    public TimeSpan ReleaseUnusedMemoryEvery { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>A test hook: awaited, with the pipeline's id, after a generation has been
    /// found not to fit the kept state and before the catalog commit that puts the previous
    /// spec back.</summary>
    internal Func<string, Task>? BeforeRollbackCommit { get; set; }

    /// <summary>Test hooks in a standby's build of a pipeline, awaited with its id: after
    /// the plugin has been acquired and before the stores are bound to it, and after the
    /// circuit has been built and before it is kept.</summary>
    internal Func<string, Task>? BeforeStandbyBind { get; set; }

    /// <inheritdoc cref="BeforeStandbyBind"/>
    internal Func<string, Task>? AfterStandbyBuild { get; set; }
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

    /// <summary>The newest <paramref name="limit"/> items older than
    /// <paramref name="sequence"/>, oldest first: paging backwards from the newest.</summary>
    public IReadOnlyList<T> Before(long sequence = long.MaxValue, int limit = int.MaxValue)
    {
        lock (_items)
            return _items.Where(i => i.Sequence < sequence).Select(i => i.Item).TakeLast(limit).ToList();
    }

    /// <summary>The sequence of the oldest item kept; 0 when there is none.</summary>
    public long Oldest
    {
        get
        {
            lock (_items)
                return _items.Count == 0 ? 0 : _items.Peek().Sequence;
        }
    }
}

/// <summary>A length of time as an operator reads it: <c>26 min 31 s</c>, not
/// <c>0:26:31.4842111</c>. No colons, so it can stand in front of the colon that separates a
/// reason from an exception's message.</summary>
internal static class Durations
{
    public static string Text(TimeSpan time)
    {
        if (time < TimeSpan.Zero)
            time = TimeSpan.Zero;
        if (time < TimeSpan.FromSeconds(1))
            return $"{(long)time.TotalMilliseconds} ms";
        if (time < TimeSpan.FromMinutes(1))
            return time.Milliseconds == 0 || time.TotalSeconds >= 10
                ? $"{(long)time.TotalSeconds} s"
                : string.Create(System.Globalization.CultureInfo.InvariantCulture, $"{time.TotalSeconds:0.#} s");
        if (time < TimeSpan.FromHours(1))
            return time.Seconds == 0 ? $"{time.Minutes} min" : $"{time.Minutes} min {time.Seconds} s";
        if (time < TimeSpan.FromDays(1))
            return time.Minutes == 0 ? $"{time.Hours} h" : $"{time.Hours} h {time.Minutes} min";
        return time.Hours == 0 ? $"{(long)time.TotalDays} d" : $"{(long)time.TotalDays} d {time.Hours} h";
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
