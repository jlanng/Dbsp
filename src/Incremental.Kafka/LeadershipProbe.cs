using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Incremental.Kafka;

/// <summary>
/// The peer probe of <see cref="KafkaLeadershipOptions.ProbeLeader"/> over plain TCP: each
/// node listens, and a standby that hears no heartbeat asks the other node directly
/// whether it is still there and still leading. A connection is answered with one line —
/// <c>role term node</c> — and closed. The answer has to be written by the process, so a
/// process that is stopped or hung does not answer even though its socket still accepts.
/// <para>
/// The probe is a hint. It keeps a stall in Kafka from looking like a dead leader; it never
/// decides who may write.
/// </para>
/// </summary>
public static class LeadershipProbe
{
    /// <summary>Answers probes for <paramref name="leadership"/> on
    /// <paramref name="endpoint"/> until the result is disposed.</summary>
    public static IAsyncDisposable Listen(KafkaLeadership leadership, IPEndPoint endpoint)
    {
        ArgumentNullException.ThrowIfNull(leadership);
        ArgumentNullException.ThrowIfNull(endpoint);
        var listener = new TcpListener(endpoint);
        listener.Start();
        return new Listener(leadership, listener);
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

        public Listener(KafkaLeadership leadership, TcpListener listener)
        {
            _leadership = leadership;
            _listener = listener;
            _loop = Task.Run(AcceptAsync);
        }

        private async Task AcceptAsync()
        {
            try
            {
                while (true)
                {
                    using var client = await _listener.AcceptTcpClientAsync(_stop.Token).ConfigureAwait(false);
                    try
                    {
                        var answer = Encoding.UTF8.GetBytes($"{_leadership.Role} {_leadership.Term} {_leadership.NodeId}\n");
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
