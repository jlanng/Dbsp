using System.Collections.Concurrent;
using System.Reflection;
using System.Security.Cryptography;
using Dbsp.Kafka;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Dbsp.Service.Admin;

/// <summary>What the endpoints share: the node, the options, the event hub and a short
/// cache of table statistics.</summary>
internal sealed class AdminState(ServiceNode node, ServiceAdminOptions options, AdminEventHub hub, ILogger logger)
{
    private readonly ConcurrentDictionary<string, (DateTimeOffset At, Task<TableStats> Stats)> _stats = new(StringComparer.Ordinal);

    public ServiceNode Node => node;
    public ServiceAdminOptions Options => options;
    public AdminEventHub Hub => hub;
    public ILogger Logger => logger;

    /// <summary>
    /// Who leads, as far as this node can tell. A leader and a standby read it from the
    /// catalog. A node that led and no longer does (fenced, resigned) cannot: its catalog
    /// is the copy it wrote, which still names itself and follows nothing. It answers
    /// from its probe of the other node instead — that node, if it says it leads — and
    /// with nothing when the probe has no such answer, never with itself.
    /// </summary>
    public LeaderRecord? Leader()
    {
        try
        {
            var status = node.Status;
            var record = node.Catalog.IsOpen ? node.Catalog.Get<LeaderRecord>("node/leader") : null;
            if (status.Leadership.Role is NodeRole.Leader or NodeRole.Promoting)
                return record;
            if (record is not null && record.NodeId != node.NodeId)
                return record;
            return status.Peer is { Role: NodeRole.Leader or NodeRole.Promoting } peer
                ? new LeaderRecord(peer.NodeId, peer.Term, peer.AdminUrl, new Dictionary<string, string>())
                : null;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>A table's statistics, at most a second old on the leader and two on a
    /// standby — where each read of them costs one read of the table's log, and a list of
    /// tables would otherwise cost that per table per request.</summary>
    public Task<TableStats> StatsAsync(string name)
    {
        var now = DateTimeOffset.UtcNow;
        var maxAge = node.IsLeader ? TimeSpan.FromMilliseconds(500) : TimeSpan.FromSeconds(2);
        if (_stats.TryGetValue(name, out var cached) && now - cached.At < maxAge && !cached.Stats.IsFaulted)
            return cached.Stats;
        var stats = node.Tables.StatsAsync(name);
        _stats[name] = (now, stats);
        if (_stats.Count > 4 * Math.Max(16, node.Tables.Tables.Count))
        {
            var known = node.Tables.Tables.Select(t => t.Name).ToHashSet(StringComparer.Ordinal);
            foreach (var key in _stats.Keys)
                if (!known.Contains(key))
                    _stats.TryRemove(key, out _);
        }
        return stats;
    }

    /// <summary>An exception's message, when row data may be shown. Messages quote values.</summary>
    public string? Message(string? message) => options.ExposeRowData ? message : null;

    /// <summary>An event as it may be shown. The service writes a fault's message into the
    /// event that reports it ("Failed: 4 faults …: FormatException: 'x7' is not …"), so
    /// with row data off an event keeps only what stands before its first colon.</summary>
    public ServiceEvent Event(ServiceEvent e) => options.ExposeRowData ? e : e with { Message = Head(e.Message) ?? "" };

    public string? Head(string? text)
    {
        if (text is null || options.ExposeRowData)
            return text;
        var colon = text.IndexOf(':');
        return colon < 0 ? text : text[..colon];
    }

    /// <summary>A fault kept as <c>Type: message</c>: the whole of it, or the type alone.</summary>
    public string? Fault(string? fault)
    {
        if (fault is null || options.ExposeRowData)
            return fault;
        var colon = fault.IndexOf(':');
        var head = colon > 0 ? fault[..colon] : fault;
        return head.Length <= 80 && head.All(c => char.IsLetterOrDigit(c) || c is '.' or '_' or '`') ? head : "(hidden: ExposeRowData is off)";
    }
}

/// <summary>The UI's files, read once from the assembly.</summary>
internal static class AdminAssets
{
    public sealed record Asset(byte[] Bytes, string ContentType, string ETag);

    private static readonly Lazy<IReadOnlyDictionary<string, Asset>> All = new(() =>
    {
        var assembly = typeof(AdminAssets).Assembly;
        var assets = new Dictionary<string, Asset>(StringComparer.Ordinal);
        foreach (var name in assembly.GetManifestResourceNames())
        {
            if (!name.StartsWith("admin/", StringComparison.Ordinal))
                continue;
            using var stream = assembly.GetManifestResourceStream(name)!;
            using var buffer = new MemoryStream();
            stream.CopyTo(buffer);
            var bytes = buffer.ToArray();
            var file = name["admin/".Length..].Replace('\\', '/');
            assets[file] = new Asset(bytes, ContentTypeOf(file), '"' + Convert.ToHexString(SHA256.HashData(bytes), 0, 8) + '"');
        }
        return assets;
    });

    public static IReadOnlyCollection<string> Names => (IReadOnlyCollection<string>)All.Value.Keys;

    public static Asset? Find(string file) => All.Value.GetValueOrDefault(file);

    /// <summary>A file of the UI read from a directory; the name has already been matched
    /// against the route's pattern, so it names a file in that directory and nothing else.</summary>
    public static Asset? FromDisk(string directory, string file)
    {
        var path = Path.Combine(directory, Path.GetFileName(file));
        if (!File.Exists(path))
            return null;
        var bytes = File.ReadAllBytes(path);
        return new Asset(bytes, ContentTypeOf(file), '"' + Convert.ToHexString(SHA256.HashData(bytes), 0, 8) + '"');
    }

    private static string ContentTypeOf(string file) => Path.GetExtension(file) switch
    {
        ".html" => "text/html; charset=utf-8",
        ".css" => "text/css; charset=utf-8",
        ".js" => "text/javascript; charset=utf-8",
        ".svg" => "image/svg+xml",
        ".json" => "application/json",
        _ => "application/octet-stream",
    };
}

/// <summary>A health check over <see cref="ServiceNode.Health"/>, for hosts that publish
/// health their own way: <c>services.AddHealthChecks().AddDbspService()</c>.</summary>
public sealed class DbspServiceHealthCheck(ServiceNode node) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var health = node.Health;
        var data = new Dictionary<string, object> { ["reasons"] = string.Join(",", health.Reasons), ["ready"] = health.Ready };
        return Task.FromResult(health.Level switch
        {
            HealthLevel.Healthy => HealthCheckResult.Healthy("healthy", data),
            HealthLevel.Degraded => HealthCheckResult.Degraded(string.Join(",", health.Reasons), data: data),
            _ => HealthCheckResult.Unhealthy(string.Join(",", health.Reasons), data: data),
        });
    }
}

public static class DbspAdminExtensions
{
    /// <summary>Registers the admin API's options and its event hub. The service itself is
    /// registered with <c>AddDbspService</c>.</summary>
    public static IServiceCollection AddDbspAdmin(this IServiceCollection services, Action<ServiceAdminOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddOptions<ServiceAdminOptions>();
        if (configure is not null)
            services.Configure(configure);
        services.TryAddSingleton(sp => new AdminEventHub(
            sp.GetRequiredService<ServiceNode>(),
            sp.GetRequiredService<IOptions<ServiceAdminOptions>>().Value,
            sp.GetService<IHostApplicationLifetime>()));
        services.TryAddSingleton(sp => new AdminState(
            sp.GetRequiredService<ServiceNode>(),
            sp.GetRequiredService<IOptions<ServiceAdminOptions>>().Value,
            sp.GetRequiredService<AdminEventHub>(),
            sp.GetRequiredService<ILoggerFactory>().CreateLogger("Dbsp.Service.Admin")));
        return services;
    }

    /// <summary>Adds <see cref="DbspServiceHealthCheck"/> under the name <c>dbsp-service</c>.</summary>
    public static IHealthChecksBuilder AddDbspService(this IHealthChecksBuilder builder, string name = "dbsp-service")
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddCheck<DbspServiceHealthCheck>(name);
    }

    /// <summary>
    /// Mounts the admin UI at <paramref name="prefix"/> and the JSON API under
    /// <c>{prefix}/api</c>. Until <see cref="ServiceAdminOptions.AuthorizationPolicy"/> is
    /// set, every endpoint answers loopback requests only. The returned builder covers
    /// every endpoint, for conventions of the host's own (CORS is not one to add).
    /// </summary>
    public static RouteGroupBuilder MapDbspAdmin(this IEndpointRouteBuilder endpoints, string prefix = "/admin")
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentNullException.ThrowIfNull(prefix);
        var state = endpoints.ServiceProvider.GetService<AdminState>()
            ?? throw new InvalidOperationException(
                "MapDbspAdmin needs services.AddDbspAdmin() (and services.AddDbspService()) to have been called.");

        var group = endpoints.MapGroup(prefix.TrimEnd('/'));
        if (state.Options.AuthorizationPolicy is { } policy)
            group.RequireAuthorization(policy);
        else
            state.Logger.LogInformation(
                "The admin API at {Prefix} has no authorization policy and answers loopback requests only. That is a default for a developer's " +
                "machine, not authentication: a reverse proxy on this machine makes its callers loopback callers. Set " +
                "ServiceAdminOptions.AuthorizationPolicy before publishing it through one.", prefix);
        AdminApi.Map(group, state);
        AdminUi.Map(group, state);
        return group;
    }

    /// <summary>
    /// Maps <c>{prefix}/live</c>, <c>{prefix}/ready</c> (200 or 503) and
    /// <c>{prefix}/status</c> (200 with the level and its reason codes). These sit outside
    /// the admin group and its loopback rule, so a kubelet or a load balancer can reach
    /// them; they return codes, never ids or messages. <c>{prefix}/ready?warm=true</c> is
    /// stricter on a standby: ready only once a takeover now would be warm — the gate for
    /// a rollout, which must not stop the leader before its standby can take its place.
    /// </summary>
    public static RouteGroupBuilder MapDbspHealth(this IEndpointRouteBuilder endpoints, string prefix = "/health")
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        var group = endpoints.MapGroup(prefix.TrimEnd('/'));
        group.MapGet("/live", (ServiceNode node) =>
        {
            var health = node.Health;
            return Results.Json(new { live = health.Live }, AdminJson.Options, statusCode: health.Live ? 200 : 503);
        });
        group.MapGet("/ready", (ServiceNode node, bool? warm) =>
        {
            var (ready, role) = Ready(node, warm ?? false);
            return Results.Json(new { ready, role }, AdminJson.Options, statusCode: ready ? 200 : 503);
        });
        group.MapGet("/status", (ServiceNode node) =>
        {
            var status = node.Status;
            return Results.Json(
                new { level = status.Health.Level, live = status.Health.Live, ready = status.Health.Ready, role = status.Leadership.Role, reasons = status.Health.Reasons },
                AdminJson.Options);
        });
        return group;
    }

    /// <summary>What <c>/ready</c> answers: the node's own readiness, and with
    /// <paramref name="warm"/> on a standby also that its takeover would be warm.</summary>
    internal static (bool Ready, NodeRole Role) Ready(ServiceNode node, bool warm)
    {
        var status = node.Status;
        var ready = status.Health.Ready
            && (!warm || status.Leadership.Role != NodeRole.Standby || status.Standby is { TakeoverWouldBeWarm: true });
        return (ready, status.Leadership.Role);
    }
}

/// <summary>Serves the embedded UI: <c>index.html</c> at the prefix and its files beside it.</summary>
internal static class AdminUi
{
    private const string ContentSecurityPolicy =
        "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; " +
        "base-uri 'none'; form-action 'none'; frame-ancestors 'none'";

    public static void Map(RouteGroupBuilder group, AdminState state)
    {
        var filter = new AdminEndpointFilter(state, mutation: false);
        group.MapGet("/", (HttpContext context) =>
        {
            // Relative links in the page resolve against the prefix only with the slash.
            var path = context.Request.Path.Value ?? "";
            if (!path.EndsWith('/'))
                return Results.Redirect(context.Request.PathBase + path + "/" + context.Request.QueryString);
            return Serve(context, "index.html", state.Options.UiDirectory);
        }).AddEndpointFilter(filter).ExcludeFromDescription();
        group.MapGet("/{file:regex(^[a-z0-9-]+\\.(css|js|svg)$)}", (HttpContext context, string file) => Serve(context, file, state.Options.UiDirectory))
            .AddEndpointFilter(filter).ExcludeFromDescription();
    }

    private static IResult Serve(HttpContext context, string file, string? directory)
    {
        var asset = directory is null ? AdminAssets.Find(file) : AdminAssets.FromDisk(directory, file);
        if (asset is null)
            return AdminErrors.Result(404, "not-found", $"No file '{file}'.");
        var headers = context.Response.Headers;
        headers.ETag = asset.ETag;
        headers.CacheControl = "no-cache";
        headers.XContentTypeOptions = "nosniff";
        headers["Referrer-Policy"] = "no-referrer";
        headers.ContentSecurityPolicy = ContentSecurityPolicy;
        if (context.Request.Headers.IfNoneMatch.Contains(asset.ETag))
            return Results.StatusCode(304);
        return Results.Bytes(asset.Bytes, asset.ContentType);
    }
}
