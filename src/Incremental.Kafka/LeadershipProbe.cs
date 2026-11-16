using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Incremental.Kafka;

/// <summary>
/// The peer probe of <see cref="KafkaLeadershipOptions.ProbeLeader"/> over plain TCP: each
/// node listens, and a standby that hears no heartbeat asks the other node directly
/// whether it is still there and still leading. A connection is answered with one line —
/// <c>role term node</c>, and a fourth token when the host supplies one — and closed. The
/// answer has to be written by the process, so a process that is stopped or hung does not
/// answer even though its socket still accepts.
/// <para>
/// The probe is a hint. It keeps a stall in Kafka from looking like a dead leader; it never
/// decides who may write.
/// </para>
/// </summary>
public static class LeadershipProbe
{
    /// <summary>Answers probes for <paramref name="leadership"/> on
    /// <paramref name="endpoint"/> until the result is disposed.</summary>
    public static IAsyncDisposable Listen(KafkaLeadership leadership, IPEndPoint endpoint) => Listen(leadership, endpoint, null);

    /// <summary>
    /// As <see cref="Listen(KafkaLeadership, IPEndPoint)"/>, with a fourth token on the
    /// answer line that the host supplies each time it is asked — whether this node is
    /// ready to lead, say. The token may not contain a space or a line break (escape it);
    /// a null or empty one is left out. <see cref="Tcp"/> reads only the first token, so
    /// old and new nodes probe each other freely; <see cref="QueryAsync"/> returns all four.
    /// </summary>
    public static IAsyncDisposable Listen(KafkaLeadership leadership, IPEndPoint endpoint, Func<string?>? detail)
    {
        ArgumentNullException.ThrowIfNull(leadership);
        ArgumentNullException.ThrowIfNull(endpoint);
        var listener = new TcpListener(endpoint);
        listener.Start();
        return new Listener(leadership, listener, detail);
    }

    /// <summary>
    /// Asks the node listening on <paramref name="host"/>:<paramref name="port"/> what it
    /// is: its role, term, name and the host's detail token. Null when the connection is
    /// refused, nothing is answered or the line cannot be read. Like every probe it is a
    /// hint for an operator or a host, never a matter of who may write.
    /// </summary>
    public static async ValueTask<LeadershipProbeAnswer?> QueryAsync(string host, int port, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(host);
        try
        {
            using var client = new TcpClient { NoDelay = true };
            await client.ConnectAsync(host, port, cancellationToken).ConfigureAwait(false);
            using var reader = new StreamReader(client.GetStream(), Encoding.UTF8);
            var line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            var tokens = line?.Split(' ', 4);
            if (tokens is not { Length: >= 3 } || !Enum.TryParse<NodeRole>(tokens[0], out var role) || !long.TryParse(tokens[1], out var term))
                return null;
            return new LeadershipProbeAnswer(role, term, tokens[2], tokens.Length > 3 && tokens[3].Length > 0 ? tokens[3] : null);
        }
        catch (Exception ex) when (ex is SocketException or IOException)
        {
            return null;
        }
    }

    /// <summary>A probe of the node listening on <paramref name="host"/>:<paramref name="port"/>:
    /// true when it answers as <see cref="NodeRole.Leader"/> or <see cref="NodeRole.Promoting"/>.
    /// A refused connection, a node that answers as anything else, and no answer are all false.</summary>
    public static Func<CancellationToken, ValueTask<bool>> Tcp(string host, int port)
    {
        ArgumentException.ThrowIfNullOrEmpty(host);
        return async cancellationToken =>
        {
            try
            {
                using var client = new TcpClient { NoDelay = true };
                await client.ConnectAsync(host, port, cancellationToken).ConfigureAwait(false);
                using var reader = new StreamReader(client.GetStream(), Encoding.UTF8);
                var line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
                var role = line?.Split(' ')[0];
                return role is nameof(NodeRole.Leader) or nameof(NodeRole.Promoting);
            }
            catch (Exception ex) when (ex is SocketException or IOException)
            {
                return false;
            }
        };
    }

    private sealed class Listener : IAsyncDisposable
    {
        private readonly KafkaLeadership _leadership;
        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _loop;

        private readonly Func<string?>? _detail;

        public Listener(KafkaLeadership leadership, TcpListener listener, Func<string?>? detail)
        {
            _leadership = leadership;
            _listener = listener;
            _detail = detail;
            _loop = Task.Run(AcceptAsync);
        }

        private async Task AcceptAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    using var client = await _listener.AcceptTcpClientAsync(_stop.Token).ConfigureAwait(false);
                    try
                    {
                        var answer = Encoding.UTF8.GetBytes(Line());
                        await client.GetStream().WriteAsync(answer, _stop.Token).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (ex is SocketException or IOException)
                    {
                        // The prober went away; the next one gets its answer.
                    }
                }
            }
            catch (Exception ex) when (ex is OperationCanceledException or SocketException or ObjectDisposedException)
            {
            }
            catch (InvalidOperationException) when (_stop.IsCancellationRequested)
            {
                // Stopped between one answer and the next accept: the listener says "not
                // listening" rather than that it was cancelled (seen on a CI runner).
            }
        }

        private string Line()
        {
            string? detail = null;
            try
            {
                detail = _detail?.Invoke();
            }
            catch (Exception)
            {
                // The host's token is extra; the probe's own answer must still be given.
            }
            var line = $"{_leadership.Role} {_leadership.Term} {_leadership.NodeId}";
            return detail is { Length: > 0 } && detail.AsSpan().IndexOfAny(' ', '\n', '\r') < 0 ? $"{line} {detail}\n" : line + "\n";
        }

        public async ValueTask DisposeAsync()
        {
            _stop.Cancel();
            _listener.Stop();
            await _loop.ConfigureAwait(false);
            _stop.Dispose();
        }
    }
}

/// <summary>What a probed node says of itself (<see cref="LeadershipProbe.QueryAsync"/>).</summary>
/// <param name="Role">Its role.</param>
/// <param name="Term">The term it leads under; 0 unless it is or was the leader.</param>
/// <param name="NodeId">Its name.</param>
/// <param name="Detail">The token its host supplied, if any.</param>
public sealed record LeadershipProbeAnswer(NodeRole Role, long Term, string NodeId, string? Detail);
