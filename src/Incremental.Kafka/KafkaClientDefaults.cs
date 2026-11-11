using Confluent.Kafka;

namespace Incremental.Kafka;

/// <summary>
/// Client settings every Kafka client of this package starts from, chosen for what a
/// broker failure does to a node that must keep committing
/// (<c>docs/design/durability-and-standby.md</c>, <i>As built: broker failures</i>). The
/// client's own defaults suit a batch job that can wait: after a broker goes, they leave
/// a healthy node silent for long after the cluster has moved on.
/// <para>
/// They are applied before a caller's own <c>ClientConfig</c>, so any of them can be
/// overridden there by its librdkafka name.
/// </para>
/// </summary>
public static class KafkaClientDefaults
{
    /// <summary>The settings, by librdkafka name.</summary>
    public static IReadOnlyDictionary<string, string> Settings { get; } = new Dictionary<string, string>
    {
        // A connection that failed is tried again after a delay that doubles, by default
        // up to ten seconds. A broker that restarts refuses connections for some seconds,
        // which is enough to reach the ten: the client then sits out the rest of the delay
        // with the broker back, and a transaction coordinator that has moved is not asked
        // for until the old one's delay has run. One second is the longest a node waits.
        ["reconnect.backoff.ms"] = "50",
        ["reconnect.backoff.max.ms"] = "1000",

        // A request to a broker that has stopped answering without closing the connection
        // (a host that is gone, a frozen process, a zone cut off) waits this long — sixty
        // seconds by default, which is twice the takeover ceiling. And a client finds
        // such a broker out one connection at a time, each by a request of its own: a
        // leader's writer waits once for the partition's old leader and once more for
        // the transaction coordinator if that was the same broker, so the wait counts
        // twice against the ceiling. Six seconds, twice, is twelve.
        ["socket.timeout.ms"] = "6000",

        // How long a broker holds a produce request for its replicas' acknowledgements.
        // Shorter than the socket timeout, so that the broker answers "timed out", which
        // is retried on the same connection, before the client gives the connection up:
        // a replica that has died is waited for until the controller drops it (nine
        // seconds, broker.session.timeout.ms), and the request is simply made again.
        ["request.timeout.ms"] = "5000",

        // How often a client asks for its topics' leaders unprompted: five minutes by
        // default. A client that is idle while a broker stops answering has no request in
        // flight to time out and learns nothing — a sink's producer, waiting for the next
        // step while the writer waits on the brokers, then sends its next batch to the
        // old leader and waits out the socket timeout in its turn. Asked every two
        // seconds, it knows the new leader before it has anything to send.
        ["topic.metadata.refresh.interval.ms"] = "2000",

        // Connecting to a host that does not answer at all: thirty seconds by default.
        ["socket.connection.setup.timeout.ms"] = "5000",
    };

    /// <summary>Sets each of <see cref="Settings"/> on <paramref name="config"/> unless it
    /// is set there already. Call it before applying the caller's own settings.</summary>
    public static T Apply<T>(T config) where T : ClientConfig
    {
        ArgumentNullException.ThrowIfNull(config);
        foreach (var (key, value) in Settings)
        {
            // request.timeout.ms is the producer's; the others belong to every client —
            // but for the socket timeout of an admin client. Its calls say how long each
            // may take (a topic's creation is given the operation timeout, a leader
            // lookup half a second and then longer), and the socket timeout would cut
            // every one of them at six seconds: a node starting while the brokers were
            // electing a controller failed to create its topics that way.
            if (key == "request.timeout.ms" && config is not ProducerConfig)
                continue;
            if (key == "socket.timeout.ms" && config is AdminClientConfig)
                continue;
            if (config.Get(key) is null)
                config.Set(key, value);
        }
        return config;
    }
}
