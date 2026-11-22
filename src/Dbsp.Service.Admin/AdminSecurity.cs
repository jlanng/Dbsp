using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace Dbsp.Service.Admin;

/// <summary>The one shape every refusal has:
/// <c>{ "error": { "code", "message", "details"?, "leader"? } }</c>.</summary>
internal static class AdminErrors
{
    public static object Body(string code, string message, IReadOnlyList<string>? details = null, object? leader = null) =>
        new { error = new { code, message, details = details is { Count: > 0 } ? details : null, leader } };

    public static IResult Result(int status, string code, string message, IReadOnlyList<string>? details = null, object? leader = null) =>
        Results.Json(Body(code, message, details, leader), AdminJson.Options, statusCode: status);

    public static Task WriteAsync(HttpContext context, int status, string code, string message)
    {
        context.Response.StatusCode = status;
        return context.Response.WriteAsJsonAsync(Body(code, message), AdminJson.Options);
    }
}

/// <summary>A refusal raised inside a handler; the error filter turns it into the error shape.</summary>
internal sealed class AdminException(int status, string code, string message) : Exception(message)
{
    public int Status { get; } = status;
    public string Code { get; } = code;
}

internal static class AdminSecurity
{
    private static readonly string[] ForwardingHeaders =
    [
        "Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Forwarded-Port", "X-Forwarded-Server", "X-Real-IP", "Via",
    ];

    /// <summary>
    /// With no authorization policy the package enforces loopback itself: it cannot choose
    /// what the server binds, and an admin API that is reachable is an admin API that is
    /// open. The rule is a default for a developer's machine, not authentication. A
    /// reverse proxy on the same machine makes every caller a loopback caller, and the
    /// checks below catch only the proxies that say so — by a forwarding header, by the
    /// host name they pass on, or by speaking HTTP/1.0 to their upstream as nginx does
    /// unless told otherwise. A proxy that rewrites the host, adds no header and speaks
    /// HTTP/1.1 looks exactly like a local client: behind any proxy, set a policy.
    /// </summary>
    public static IResult? RefuseUnlessLoopback(HttpContext context, ServiceAdminOptions options)
    {
        if (options.AuthorizationPolicy is not null)
            return null;

        var remote = context.Connection.RemoteIpAddress;
        if (remote is { IsIPv4MappedToIPv6: true })
            remote = remote.MapToIPv4();
        if (remote is null || !IPAddress.IsLoopback(remote))
            return AdminErrors.Result(403, "loopback-only",
                "The admin API answers loopback requests only until ServiceAdminOptions.AuthorizationPolicy is set.");

        // Behind a reverse proxy on the same machine every request arrives from loopback.
        // No browser and no current HTTP client sends HTTP/1.0; nginx's proxy_pass does
        // by default, and by default adds none of the headers looked for below.
        if (HttpProtocol.IsHttp10(context.Request.Protocol))
            return AdminErrors.Result(403, "forwarded",
                "This request arrived over HTTP/1.0, as a reverse proxy's does; the admin API answers proxied requests only when ServiceAdminOptions.AuthorizationPolicy is set.");
        foreach (var header in ForwardingHeaders)
            if (context.Request.Headers.ContainsKey(header))
                return AdminErrors.Result(403, "forwarded",
                    "This request was forwarded by a proxy; the admin API answers forwarded requests only when ServiceAdminOptions.AuthorizationPolicy is set.");

        var host = context.Request.Host.Host;
        var loopbackName = string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase)
            || (IPAddress.TryParse(host.Trim('[', ']'), out var address) && IPAddress.IsLoopback(address));
        if (!loopbackName && !options.AllowedHosts.Contains(host, StringComparer.OrdinalIgnoreCase))
            return AdminErrors.Result(403, "host-not-allowed",
                $"Host '{host}' is not a loopback name and is not in ServiceAdminOptions.AllowedHosts.");
        return null;
    }

    /// <summary>What every mutation must carry. The header is what stops a page on the
    /// operator's machine from posting to loopback: a browser will not send it cross-site
    /// without a preflight, and the API answers none.</summary>
    public static IResult? RefuseMutation(HttpContext context, ServiceAdminOptions options)
    {
        if (options.ReadOnly)
            return AdminErrors.Result(403, "read-only", "This admin API is read-only (ServiceAdminOptions.ReadOnly).");
        if (context.Request.Headers["X-Dbsp-Admin"].FirstOrDefault() != "1")
            return AdminErrors.Result(400, "missing-header", "A mutating request must carry the header 'X-Dbsp-Admin: 1'.");
        return null;
    }

    /// <summary>The header a caller may state its name in when nothing verifies who it is.</summary>
    public const string OperatorHeader = "X-Dbsp-Operator";

    /// <summary>Whether the host vouches for <see cref="Caller"/>: its hook answered, or
    /// the request is authenticated under a name.</summary>
    public static bool Verified(HttpContext context, ServiceAdminOptions options) =>
        Clean(options.CallerName?.Invoke(context)) is not null
        || context.User.Identity is { IsAuthenticated: true, Name: { Length: > 0 } };

    /// <summary>
    /// Who the audit records a change as. In order: what the host's hook says
    /// (<see cref="ServiceAdminOptions.CallerName"/>); the authenticated identity's name;
    /// the name the caller states in <see cref="OperatorHeader"/>, marked as its own word;
    /// and <c>local</c>, which is all that is known of an unauthenticated loopback caller.
    /// </summary>
    public static string Caller(HttpContext context, ServiceAdminOptions options)
    {
        if (Clean(options.CallerName?.Invoke(context)) is { } hooked)
            return hooked;
        if (context.User.Identity is { IsAuthenticated: true, Name: { Length: > 0 } name })
            return name;
        if (options.AcceptOperatorHeader && Clean(Unescaped(context.Request.Headers[OperatorHeader].FirstOrDefault())) is { } stated)
            return stated + " (self-declared)";
        return "local";
    }

    /// <summary>A header carries Latin-1 only, so a name outside it arrives percent-encoded.</summary>
    private static string? Unescaped(string? value)
    {
        if (value is null || !value.Contains('%'))
            return value;
        try
        {
            return Uri.UnescapeDataString(value);
        }
        catch (UriFormatException)
        {
            return value;
        }
    }

    /// <summary>A name fit for the audit: trimmed, printable, at most 64 characters.</summary>
    private static string? Clean(string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return null;
        var kept = new string([.. name.Trim().Where(c => !char.IsControl(c)).Take(64)]);
        return kept.Length == 0 ? null : kept;
    }
}

/// <summary>Wraps every endpoint: the loopback rule first, then the handler, with whatever
/// it throws turned into the error shape.</summary>
internal sealed class AdminEndpointFilter(AdminState state, bool mutation) : IEndpointFilter
{
    public async ValueTask<object?> InvokeAsync(EndpointFilterInvocationContext invocation, EndpointFilterDelegate next)
    {
        var context = invocation.HttpContext;
        if (AdminSecurity.RefuseUnlessLoopback(context, state.Options) is { } refused)
            return refused;
        if (mutation && AdminSecurity.RefuseMutation(context, state.Options) is { } refusedMutation)
            return refusedMutation;
        try
        {
            return await next(invocation).ConfigureAwait(false);
        }
        catch (ServiceException ex)
        {
            object? leader = null;
            if (ex.Code == "not-leader")
            {
                var record = state.Leader();
                leader = record is null ? null : new { nodeId = record.NodeId, term = record.Term, adminUrl = record.AdminUrl };
            }
            return AdminErrors.Result(ex.Status, ex.Code, ex.Message, ex.Details, leader);
        }
        catch (AdminException ex)
        {
            return AdminErrors.Result(ex.Status, ex.Code, ex.Message);
        }
        catch (JsonException ex)
        {
            return AdminErrors.Result(400, "invalid-json", $"The request body is not the JSON this endpoint reads: {ex.Message}");
        }
        catch (BadHttpRequestException ex)
        {
            return AdminErrors.Result(ex.StatusCode, ex.StatusCode == 413 ? "too-large" : "bad-request", ex.Message);
        }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            state.Logger.LogError(ex, "Admin API: {Method} {Path} failed", context.Request.Method, context.Request.Path);
            // An exception nobody expected may quote a row; its type never does.
            return AdminErrors.Result(500, "internal",
                state.Options.ExposeRowData ? $"{ex.GetType().Name}: {ex.Message}" : $"{ex.GetType().Name} (the message is hidden: ExposeRowData is off).");
        }
    }
}
