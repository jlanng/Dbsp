using Confluent.Kafka;

namespace Incremental.Kafka;

/// <summary>
/// Reading with a consumer that is assigned its partitions and never joins a group — which
/// is every consumer this library and the service create: positions live in checkpoints,
/// and nothing is committed to Kafka.
/// </summary>
public static class AssignedConsumer
{
    /// <summary>
    /// Lets the client run its callbacks before anything but <c>Consume</c> is asked of
    /// the consumer. A consumer's callbacks — the OAUTHBEARER token refresh among them,
    /// which is how an MSK IAM token arrives — are served only from inside <c>Consume</c>.
    /// One that is asked for metadata or for a partition's watermarks first, or again
    /// after sitting unpolled past its token's life, has no token and cannot connect: the
    /// call fails after its timeout with "all broker connections are down". Call this
    /// before such a call. It reads nothing: the consumer must not be assigned.
    /// </summary>
    public static void ServeCallbacks<TKey, TValue>(this IConsumer<TKey, TValue> consumer)
    {
        try
        {
            consumer.Consume(TimeSpan.Zero);
        }
        catch (ConsumeException ex) when (!ex.Error.IsFatal)
        {
            // An event of the client's own (a refused group lookup, a broker that is
            // down): the call that follows reports what matters.
        }
    }

    /// <summary>
    /// <see cref="IConsumer{TKey,TValue}.Consume(TimeSpan)"/>, for a consumer that is only
    /// ever assigned. The Kafka client requires a <c>group.id</c> and looks the group's
    /// coordinator up whether or not the group is ever joined; where the principal has no
    /// permission on the group, the broker refuses the lookup and the client reports that
    /// through <c>Consume</c> as an error. It is not this consumer's error: it reads
    /// partitions it was assigned, which needs permission on the topic alone. So a refused
    /// group lookup is passed over (null, as if nothing had arrived) and the deployment
    /// needs no permission on any consumer group.
    /// </summary>
    public static ConsumeResult<TKey, TValue>? ConsumeAssigned<TKey, TValue>(this IConsumer<TKey, TValue> consumer, TimeSpan timeout)
    {
        try
        {
            return consumer.Consume(timeout);
        }
        catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.GroupAuthorizationFailed && !ex.Error.IsFatal)
        {
            return null;
        }
    }
}
