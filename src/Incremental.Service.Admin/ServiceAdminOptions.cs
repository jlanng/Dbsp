namespace Incremental.Service.Admin;

/// <summary>
/// How the admin API and UI are exposed. The defaults are the cautious ones: loopback
/// only, no row data, no code loading.
/// </summary>
public sealed class ServiceAdminOptions
{
    /// <summary>The authorization policy every endpoint requires. While it is null the
    /// package enforces loopback itself: a request is refused unless it arrives from a
    /// loopback address, names a loopback host (or one in <see cref="AllowedHosts"/>),
    /// carries no forwarding header and is not HTTP/1.0. Setting a policy lifts those
    /// checks. The loopback rule is not authentication: a reverse proxy on the same
    /// machine that rewrites the host, adds no forwarding header and speaks HTTP/1.1 to
    /// this server makes every one of its callers a loopback caller, and nothing here can
    /// tell. Set a policy before the admin surface is published through any proxy.</summary>
    public string? AuthorizationPolicy { get; set; }

    /// <summary>Required for pipeline mutations; defaults to <see cref="AuthorizationPolicy"/>.</summary>
    public string? PipelinePolicy { get; set; }

    /// <summary>Required for table mutations; defaults to <see cref="AuthorizationPolicy"/>.</summary>
    public string? TablePolicy { get; set; }

    /// <summary>Required for takeover and step-down; defaults to <see cref="AuthorizationPolicy"/>.</summary>
    public string? NodePolicy { get; set; }

    /// <summary>Required for plugin upload, rescan and delete; defaults to <see cref="AuthorizationPolicy"/>.</summary>
    public string? PluginPolicy { get; set; }

    /// <summary>Every mutating endpoint answers 403 and the UI shows no actions.</summary>
    public bool ReadOnly { get; set; }

    /// <summary>Whether table rows, dead-letter and unreadable records, and exception
    /// messages and stacks are returned. Off, the row endpoints answer 403 and faults carry
    /// the exception type only: a <c>FormatException</c> or an exception thrown by plugin
    /// code quotes the offending value in its message, so the message is row data.</summary>
    public bool ExposeRowData { get; set; }

    /// <summary>Whether a plugin zip may be uploaded: code loaded over the API.</summary>
    public bool AllowPluginUpload { get; set; }

    /// <summary>Whether the plugin directory may be imported again on request.</summary>
    public bool AllowPluginRescan { get; set; }

    /// <summary>The most items any list or row read returns in one response.</summary>
    public int MaxRows { get; set; } = 200;

    /// <summary>The most rows one row read looks at before it stops and says so.</summary>
    public int MaxScannedRows { get; set; } = 100_000;

    public long MaxUploadBytes { get; set; } = 64L << 20;

    /// <summary>Host names accepted besides loopback names when no
    /// <see cref="AuthorizationPolicy"/> is set. This is <b>unauthenticated access for
    /// whoever can reach that name</b>; it exists for a container's port mapping, not for
    /// a network.</summary>
    public IList<string> AllowedHosts { get; } = [];

    /// <summary>For working on the UI: serve its files from this directory (the package's
    /// <c>wwwroot</c> in a checkout) instead of the copies embedded in the assembly, read
    /// afresh on every request.</summary>
    public string? UiDirectory { get; set; }

    /// <summary>Events queued for one event-stream client before the oldest is dropped.
    /// A slow browser loses ticks, not the server's memory.</summary>
    public int EventQueueLength { get; set; } = 64;

    /// <summary>Event-stream clients served at once; one more is answered 503.</summary>
    public int MaxEventClients { get; set; } = 32;

    /// <summary>How long one write to an event-stream client may take before the
    /// connection is aborted.</summary>
    public TimeSpan EventWriteTimeout { get; set; } = TimeSpan.FromSeconds(10);
}
