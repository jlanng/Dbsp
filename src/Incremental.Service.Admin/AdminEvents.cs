using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Incremental.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;

namespace Incremental.Service.Admin;

/// <summary>
/// The server-sent event feed. One hub listens to the node's events and to its metrics
/// sampler; every connected client has a bounded queue that drops its oldest frame when
/// full, so a browser that does not read costs a fixed amount of memory and loses ticks.
/// A frame is serialized once, whatever the number of clients.
/// </summary>
internal sealed class AdminEventHub : IDisposable
{
    private sealed record Frame(long Id, byte[] Bytes);

    private sealed class Client(int capacity, Action dropped)
    {
        public long Dropped;

        public Channel<Frame> Queue { get; } = Channel.CreateBounded<Frame>(
            new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
            },
            _ => dropped());
    }

    private readonly ServiceNode _node;
    private readonly ServiceAdminOptions _options;
    private readonly IHostApplicationLifetime? _lifetime;
    private readonly ConcurrentDictionary<long, Client> _clients = new();
    private long _nextClient;
    private long _dropped;
    private long _aborted;

    public AdminEventHub(ServiceNode node, ServiceAdminOptions options, IHostApplicationLifetime? lifetime = null)
    {
        _node = node;
        _options = options;
        _lifetime = lifetime;
        node.Events += OnEvent;
        node.Metrics.Sampled += OnSampled;
    }

    public int Clients => _clients.Count;

    /// <summary>Frames dropped from full client queues since the process started.</summary>
    public long Dropped => Interlocked.Read(ref _dropped);

    /// <summary>Connections aborted because one write outlasted the write timeout.</summary>
    public long Aborted => Interlocked.Read(ref _aborted);

    /// <summary>The deepest client queue right now: never more than the configured length.</summary>
    public int DeepestQueue => _clients.Values.Select(c => c.Queue.Reader.Count).DefaultIfEmpty(0).Max();

    private static Frame Format(string kind, long id, string json)
    {
        var text = id > 0 ? $"id: {id}\nevent: {kind}\ndata: {json}\n\n" : $"event: {kind}\ndata: {json}\n\n";
        return new Frame(id, Encoding.UTF8.GetBytes(text));
    }

    private void Broadcast(Frame frame)
    {
        foreach (var client in _clients.Values)
            client.Queue.Writer.TryWrite(frame);
    }

    /// <summary>Queues one frame for every client, as an event or a tick would: how the
    /// tests fill a client's queue faster than the node's own events can.</summary>
    internal void Publish(string kind, string json) => Broadcast(Format(kind, 0, json));

    private void OnEvent(ServiceEvent e)
    {
        if (_clients.IsEmpty)
            return;
        Broadcast(Format(e.Kind, e.Sequence, JsonSerializer.Serialize(Shown(e), AdminJson.Options)));
    }

    /// <summary>With row data off an event keeps what stands before its first colon: the
    /// service writes a fault's message, which may quote a row, after it.</summary>
    private ServiceEvent Shown(ServiceEvent e) =>
        _options.ExposeRowData || e.Message.IndexOf(':') is var colon && colon < 0 ? e : e with { Message = e.Message[..e.Message.IndexOf(':')] };

    private void OnSampled()
    {
        if (_clients.IsEmpty)
            return;
        try
        {
            Broadcast(Format("metrics", 0, JsonSerializer.Serialize(Tick(), AdminJson.Options)));
        }
        catch
        {
            // A node that is shutting down has nothing to report.
        }
    }

    /// <summary>
    /// A sample's rates. With <paramref name="recent"/> (a pipeline's one-second samples,
    /// oldest first, ending with <paramref name="s"/>) a pipeline that steps less often than
    /// once a second and a half — a sink's minimum interval holds each step back — is
    /// <c>batchedEveryS</c>, and its rates are taken over the last two of its intervals:
    /// one second alone reads 100/s and then 0/s for a steady 50/s.
    /// </summary>
    internal static object Rates(MetricSample s, IReadOnlyList<MetricSample>? recent = null)
    {
        double? every = null;
        var window = s;
        if (recent is { Count: > 1 })
        {
            // The spacing of steps over the last half minute, while the pipeline stepped at all.
            double seconds = 0;
            long steps = 0;
            for (var i = recent.Count - 1; i >= 0 && i >= recent.Count - 30; i--)
            {
                seconds += recent[i].Seconds;
                steps += recent[i].Steps;
            }
            if (steps > 0 && seconds / steps >= 1.5)
            {
                every = Math.Round(seconds / steps, 1);
                double span = 0, @in = 0, @out = 0, stepSeconds = 0, stepMax = 0;
                long windowSteps = 0;
                for (var i = recent.Count - 1; i >= 0 && span < 2 * every; i--)
                {
                    var x = recent[i];
                    span += x.Seconds;
                    @in += x.RecordsIn;
                    @out += x.RecordsOut;
                    windowSteps += x.Steps;
                    stepSeconds += x.StepSeconds;
                    stepMax = Math.Max(stepMax, x.StepMaxSeconds);
                }
                window = s with { Seconds = span, RecordsIn = (long)@in, RecordsOut = (long)@out, Steps = windowSteps, StepSeconds = stepSeconds, StepMaxSeconds = stepMax };
            }
        }
        return Rates(s, window, every);
    }

    private static object Rates(MetricSample s, MetricSample window, double? every) => new
    {
        @in = Math.Round(window.RecordsIn / window.Seconds, 1),
        @out = Math.Round(window.RecordsOut / window.Seconds, 1),
        batchedEveryS = every,
        rateOverS = every is null ? (double?)null : Math.Round(window.Seconds, 1),
        steps = s.Steps,
        stepMs = window.Steps > 0 ? Math.Round(window.StepSeconds * 1000 / window.Steps, 3) : (double?)null,
        stepMaxMs = window.Steps > 0 ? Math.Round(window.StepMaxSeconds * 1000, 3) : (double?)null,
        ackMs = s.Commits > 0 ? Math.Round(s.SinkAckWaitSeconds * 1000 / s.Commits, 3) : (double?)null,
        commits = s.Commits,
        lag = s.InputLag,
        checkpointAgeS = s.CheckpointAgeSeconds is { } age ? Math.Round(age, 1) : (double?)null,
        deadLetters = s.DeadLetters,
    };

    /// <summary>One second of the node: who it is, how it is, and every pipeline's rates.</summary>
    internal object Tick()
    {
        var status = _node.Status;
        var latest = _node.Metrics.Latest;
        var role = status.Leadership.Role;
        return new
        {
            time = DateTimeOffset.UtcNow,
            node = new
            {
                nodeId = status.NodeId,
                role,
                term = role == NodeRole.Standby ? status.Leadership.LeaderTerm : status.Leadership.Term,
                leader = status.Leadership.LeaderNodeId,
                health = status.Health,
                catalogVersion = status.CatalogVersion,
                tables = status.Tables,
                pipelines = status.Pipelines,
                pipelinesByState = status.PipelinesByState,
                heartbeatAgeMs = status.Leadership.Kafka?.HeartbeatAge.TotalMilliseconds,
                stateLag = role == NodeRole.Standby ? status.Standby?.StateLagRecords : null,
                peer = status.Peer,
            },
            total = latest.TryGetValue("", out var total) ? Rates(total) : null,
            pipelines = latest.Where(p => p.Key.Length > 0).ToDictionary(p => p.Key, p => Rates(p.Value, _node.Metrics.Pipeline(p.Key).Seconds)),
        };
    }

    public async Task StreamAsync(HttpContext context)
    {
        if (_clients.Count >= _options.MaxEventClients)
        {
            await AdminErrors.WriteAsync(context, 503, "too-many-clients",
                $"{_options.MaxEventClients} event streams are already open on this node.").ConfigureAwait(false);
            return;
        }

        using var stop = CancellationTokenSource.CreateLinkedTokenSource(
            context.RequestAborted, _lifetime?.ApplicationStopping ?? CancellationToken.None);
        var id = Interlocked.Increment(ref _nextClient);
        Client? client = null;
        client = new Client(Math.Max(1, _options.EventQueueLength), () =>
        {
            Interlocked.Increment(ref _dropped);
            if (client is not null)
                Interlocked.Increment(ref client.Dropped);
        });

        var response = context.Response;
        response.StatusCode = 200;
        response.ContentType = "text/event-stream";
        response.Headers.CacheControl = "no-store";
        response.Headers["X-Accel-Buffering"] = "no";

        async Task WriteAsync(byte[] bytes)
        {
            // Its own clock, not linked to the request: the server aborts the connection
            // when a write is cancelled, which cancels the request, and what must be told
            // apart afterwards is whether the time ran out first.
            using var timeout = new CancellationTokenSource(_options.EventWriteTimeout);
            using var write = CancellationTokenSource.CreateLinkedTokenSource(stop.Token, timeout.Token);
            try
            {
                await response.Body.WriteAsync(bytes, write.Token).ConfigureAwait(false);
                await response.Body.FlushAsync(write.Token).ConfigureAwait(false);
            }
            catch (Exception) when (timeout.IsCancellationRequested)
            {
                // The client is not reading. Its queue is bounded either way; this ends
                // the connection so that it does not stay open for nobody.
                Interlocked.Increment(ref _aborted);
                context.Abort();
                throw;
            }
        }

        // Listen first, then replay what the client missed: an event that arrives in
        // between is in both, and is written once.
        _clients[id] = client;
        try
        {
            var replayed = 0L;
            var last = context.Request.Headers["Last-Event-ID"].FirstOrDefault() ?? context.Request.Query["lastEventId"].FirstOrDefault();
            await WriteAsync(Format("hello", 0, JsonSerializer.Serialize(
                new { nodeId = _node.NodeId, queue = _options.EventQueueLength }, AdminJson.Options)).Bytes).ConfigureAwait(false);
            if (long.TryParse(last, out var after) && after >= 0)
            {
                foreach (var missed in _node.RecentEvents(after))
                {
                    await WriteAsync(Format(missed.Kind, missed.Sequence, JsonSerializer.Serialize(Shown(missed), AdminJson.Options)).Bytes).ConfigureAwait(false);
                    replayed = missed.Sequence;
                }
            }

            await foreach (var frame in client.Queue.Reader.ReadAllAsync(stop.Token).ConfigureAwait(false))
            {
                if (frame.Id != 0 && frame.Id <= replayed)
                    continue;
                if (Interlocked.Exchange(ref client.Dropped, 0) is > 0 and var dropped)
                    await WriteAsync(Format("lagged", 0, JsonSerializer.Serialize(new { dropped }, AdminJson.Options)).Bytes).ConfigureAwait(false);
                await WriteAsync(frame.Bytes).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex) when (ex is IOException or InvalidOperationException or ObjectDisposedException)
        {
            // The client went away mid-write, or was cut off for not reading.
        }
        finally
        {
            _clients.TryRemove(id, out _);
        }
    }

    public void Dispose()
    {
        _node.Events -= OnEvent;
        _node.Metrics.Sampled -= OnSampled;
    }
}
