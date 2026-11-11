using System.Reflection;
using Confluent.Kafka;

namespace Incremental.Kafka.Tests;

/// <summary>
/// The fixes that were found on a broker cluster and on a broker that refuses unknown
/// clients (the <c>kafka3</c> and <c>kafka-sasl</c> compose profiles), pinned here without
/// either: each is a small rule about what a client's answer means, and the answer is
/// handed to the rule directly. None of these needs a broker.
/// </summary>
public class BrokerFaultSeamTests
{
    private static readonly TopicPartition Partition = new("t", 0);

    // ── ListOffsets: an unset offset is not "empty" ────────────────────────────────────

    [Fact]
    public void An_offsets_answer_with_no_offset_is_a_failure_that_passes_and_never_an_empty_partition()
    {
        // What the client hands back for a partition whose leader is down and not yet
        // replaced: no error, and the offset left unset (−1001).
        var unset = new TopicPartitionOffsetError(Partition, Offset.Unset, new Error(ErrorCode.NoError));
        var thrown = Assert.Throws<KafkaException>(() => KafkaLog.OffsetOf(unset));
        Assert.Equal(ErrorCode.LeaderNotAvailable, thrown.Error.Code);
        Assert.True(KafkaLog.IsTransient(thrown), "the call must be made again, not failed");

        foreach (var special in new[] { Offset.End, Offset.Beginning, Offset.Stored, new Offset(-1) })
            Assert.Throws<KafkaException>(() => KafkaLog.OffsetOf(new TopicPartitionOffsetError(Partition, special, new Error(ErrorCode.NoError))));

        // An error in the answer is that error …
        var failed = Assert.Throws<KafkaException>(() =>
            KafkaLog.OffsetOf(new TopicPartitionOffsetError(Partition, new Offset(5), new Error(ErrorCode.NotLeaderForPartition))));
        Assert.Equal(ErrorCode.NotLeaderForPartition, failed.Error.Code);

        // … and an offset is an offset, zero included: an empty partition says 0.
        Assert.Equal(0, KafkaLog.OffsetOf(new TopicPartitionOffsetError(Partition, new Offset(0), new Error(ErrorCode.NoError))));
        Assert.Equal(42, KafkaLog.OffsetOf(new TopicPartitionOffsetError(Partition, new Offset(42), new Error(ErrorCode.NoError))));
    }

    // ── admin calls: short attempts, the cache refreshed between them ──────────────────

    [Fact]
    public async Task An_admin_call_is_given_short_attempts_that_double_and_the_metadata_is_asked_for_between_them()
    {
        var attempts = new List<TimeSpan>();
        var refreshes = 0;
        var failures = 4;
        Task<int> Ask(TimeSpan attempt)
        {
            attempts.Add(attempt);
            return failures-- > 0
                ? Task.FromException<int>(new KafkaException(new Error(ErrorCode.Local_NoEnt, "Failed to query partition leaders: No leaders found")))
                : Task.FromResult(7);
        }

        Assert.Equal(7, await KafkaLog.RetryTransientAsync(Ask, TimeSpan.FromSeconds(60), default, () => refreshes++).WaitAsync(TimeSpan.FromSeconds(30)));

        // Half a second first — not the whole timeout, which a cached "no leader" would
        // use up — then twice as long each time.
        Assert.Equal([500, 1000, 2000, 4000, 8000], attempts.Select(a => (int)a.TotalMilliseconds).ToArray());
        Assert.Equal(4, refreshes);

        // Never longer than what is left of the timeout.
        attempts.Clear();
        failures = 0;
        await KafkaLog.RetryTransientAsync(Ask, TimeSpan.FromMilliseconds(300), default, () => refreshes++);
        Assert.InRange(attempts[0].TotalMilliseconds, 100, 300);

        // An error that will not pass is not retried, and nothing is refreshed for it.
        refreshes = 0;
        var asked = 0;
        await Assert.ThrowsAsync<KafkaException>(() => KafkaLog.RetryTransientAsync<int>(
            _ =>
            {
                asked++;
                return Task.FromException<int>(new KafkaException(new Error(ErrorCode.TopicAuthorizationFailed)));
            }, TimeSpan.FromSeconds(10), default, () => refreshes++));
        Assert.Equal(1, asked);
        Assert.Equal(0, refreshes);
    }

    // ── a consumer that is assigned and never joins a group ────────────────────────────

    /// <summary>An <see cref="IConsumer{TKey,TValue}"/> whose <c>Consume</c> does what a
    /// test says; everything else is not expected to be called.</summary>
    public class ScriptedConsumer : DispatchProxy
    {
        public Func<TimeSpan, ConsumeResult<byte[], byte[]>?> OnConsume = _ => null;
        public readonly List<TimeSpan> Consumed = [];

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod?.Name == nameof(IConsumer<byte[], byte[]>.Consume) && args is [TimeSpan timeout])
            {
                Consumed.Add(timeout);
                return OnConsume(timeout);
            }
            throw new NotSupportedException(targetMethod?.Name);
        }

        public static (IConsumer<byte[], byte[]> Consumer, ScriptedConsumer Script) Create()
        {
            var consumer = Create<IConsumer<byte[], byte[]>, ScriptedConsumer>();
            return (consumer, (ScriptedConsumer)(object)consumer);
        }
    }

    private static ConsumeException Refused(ErrorCode code, bool fatal = false) =>
        new(new ConsumeResult<byte[], byte[]>(), new Error(code, "scripted", fatal));

    [Fact]
    public void A_refused_lookup_of_the_group_that_is_never_joined_is_passed_over_and_nothing_else_is()
    {
        var (consumer, script) = ScriptedConsumer.Create();
        var record = new ConsumeResult<byte[], byte[]> { TopicPartitionOffset = new TopicPartitionOffset(Partition, 3) };

        // A broker that gives the principal nothing on the group answers the client's
        // lookup with a refusal, which the client throws from Consume.
        script.OnConsume = _ => throw Refused(ErrorCode.GroupAuthorizationFailed);
        Assert.Null(consumer.ConsumeAssigned(TimeSpan.FromMilliseconds(5)));

        // What was read is handed on, and the timeout is the caller's.
        script.OnConsume = _ => record;
        Assert.Same(record, consumer.ConsumeAssigned(TimeSpan.FromMilliseconds(7)));
        Assert.Equal(TimeSpan.FromMilliseconds(7), script.Consumed[^1]);

        // Any other refusal is the reader's to hear of: the topic's above all.
        script.OnConsume = _ => throw Refused(ErrorCode.TopicAuthorizationFailed);
        Assert.Equal(ErrorCode.TopicAuthorizationFailed, Assert.Throws<ConsumeException>(() => consumer.ConsumeAssigned(TimeSpan.Zero)).Error.Code);
        script.OnConsume = _ => throw Refused(ErrorCode.GroupAuthorizationFailed, fatal: true);
        Assert.Throws<ConsumeException>(() => consumer.ConsumeAssigned(TimeSpan.Zero));
    }

    [Fact]
    public void Serving_a_consumers_callbacks_polls_it_once_without_waiting_and_swallows_only_what_is_not_fatal()
    {
        var (consumer, script) = ScriptedConsumer.Create();

        // The poll is what lets the client run its token refresh: without it a consumer
        // asked for metadata first has no token.
        consumer.ServeCallbacks();
        Assert.Equal([TimeSpan.Zero], script.Consumed);

        // An event of the client's own must not fail the call that follows it …
        script.OnConsume = _ => throw Refused(ErrorCode.GroupAuthorizationFailed);
        consumer.ServeCallbacks();
        script.OnConsume = _ => throw Refused(ErrorCode.Local_AllBrokersDown);
        consumer.ServeCallbacks();
        Assert.Equal(3, script.Consumed.Count);

        // … and a client that is finished says so.
        script.OnConsume = _ => throw Refused(ErrorCode.Local_Fatal, fatal: true);
        Assert.Throws<ConsumeException>(() => consumer.ServeCallbacks());
    }

    // ── the client settings a node starts from ─────────────────────────────────────────

    [Fact]
    public void Every_client_starts_from_the_settings_a_broker_failure_needs_and_a_callers_own_win()
    {
        var producer = KafkaClientDefaults.Apply(new ProducerConfig());
        Assert.Equal("1000", producer.Get("reconnect.backoff.max.ms"));
        Assert.Equal("6000", producer.Get("socket.timeout.ms"));
        Assert.Equal("5000", producer.Get("request.timeout.ms"));
        Assert.Equal("2000", producer.Get("topic.metadata.refresh.interval.ms"));
        Assert.Equal("5000", producer.Get("socket.connection.setup.timeout.ms"));

        // The acknowledgement timeout is a producer's setting; a consumer is not given it.
        var consumer = KafkaClientDefaults.Apply(new ConsumerConfig());
        Assert.Equal("1000", consumer.Get("reconnect.backoff.max.ms"));
        Assert.Null(consumer.Get("request.timeout.ms"));

        // An admin client's calls carry their own timeouts, up to the operation timeout;
        // the socket timeout would cut them all at six seconds.
        var admin = KafkaClientDefaults.Apply(new AdminClientConfig());
        Assert.Null(admin.Get("socket.timeout.ms"));
        Assert.Equal("1000", admin.Get("reconnect.backoff.max.ms"));

        // What the caller set stands.
        var own = KafkaClientDefaults.Apply(new ProducerConfig { ReconnectBackoffMaxMs = 250, SocketTimeoutMs = 30_000 });
        Assert.Equal("250", own.Get("reconnect.backoff.max.ms"));
        Assert.Equal("30000", own.Get("socket.timeout.ms"));
        Assert.Equal("50", own.Get("reconnect.backoff.ms"));

        // The acknowledgement timeout must stay under the socket timeout, or the client
        // gives the connection up before the broker has said "timed out".
        Assert.True(int.Parse(KafkaClientDefaults.Settings["request.timeout.ms"]) < int.Parse(KafkaClientDefaults.Settings["socket.timeout.ms"]));

        // A writer meets a broker that has stopped answering twice in a row — as a
        // partition's leader and as the transaction coordinator — and both waits must fit
        // under the takeover ceiling with room for the brokers' own nine seconds.
        Assert.True(2 * int.Parse(KafkaClientDefaults.Settings["socket.timeout.ms"]) + 9000 < new KafkaLeadershipOptions { StartLeadingAsync = (_, _) => Task.CompletedTask }.TakeoverCeiling.TotalMilliseconds);
    }
}
