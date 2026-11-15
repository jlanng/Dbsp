namespace Incremental.Kafka;

/// <summary>
/// Says when, on this node's clock, a record of the leader topic was written — at the
/// latest. A standby counts the leader's silence from its last sign of life, and a record
/// is a sign of life at the moment it was written, which is not always the moment it is
/// read: a record the brokers could not show (its partition's high watermark waiting for
/// a replica that had died) is read when they give that replica up, nine seconds later
/// with Kafka's defaults. Counted from when it was read, the last heartbeat of a leader
/// that died with a broker starts the silence again nine seconds into it, and calls off
/// a takeover that was nine seconds old: once in thirteen such takeovers here, at the
/// cost of one more takeover timeout (the service design, S8, <i>What a broker's loss
/// costs</i>).
/// <para>
/// Each record carries its writer's wall clock (<c>At</c>). The two nodes' clocks are not
/// compared: what is kept is the smallest difference seen between this node's clock at
/// the reading of a record and the writer's clock in it. That difference is the offset
/// between the clocks plus how long the record took to be readable, so its smallest value
/// is the offset plus the shortest delivery seen — and a record's own <c>At</c> plus that
/// is no earlier than the moment it was written and no later than the moment it is read.
/// A record that was readable at once is dated "now"; one that waited is dated earlier by
/// about as long as it waited.
/// </para>
/// <para>
/// The estimate may never be too early, or silence would be counted that was not there.
/// It is only as good as the two clocks keeping step, so what is remembered is little:
/// the smallest difference of two stretches of the writer's own time, five seconds each
/// (a writer that has written nothing for a minute is still measured against its last
/// stretch — that is the dead leader's case, and all it needs is one prompt record
/// shortly before the late one). A record of another term starts again — a term is one
/// writer's, from its acquisition to its end, so another term is another clock — and
/// so does one whose <c>At</c> is before its predecessor's, which is a clock that was
/// set back. A writer whose clock is set back by less than the gap between two
/// heartbeats is dated early by that much for at most ten seconds of its heartbeats;
/// one whose clock is being slowed to correct it (a time daemon slews by a few percent
/// at the very most) is dated early by that share of ten seconds, a few tenths; one
/// whose clock is set forward is dated correctly at once.
/// </para>
/// Not thread-safe: the leadership calls it under its lock.
/// </summary>
internal sealed class HeartbeatDating
{
    /// <summary>How much of the writer's own time one remembered stretch covers.</summary>
    internal const long StretchMilliseconds = 5_000;

    private long _term = long.MinValue;
    private long _lastAt;
    private long _stretchFrom;
    private long _least = long.MaxValue;
    private long _leastBefore = long.MaxValue;

    /// <summary>
    /// The latest moment, in the ticks <paramref name="now"/> is given in, at which the
    /// record can have been written: never later than <paramref name="now"/>.
    /// </summary>
    /// <param name="term">The term the record was written under.</param>
    /// <param name="at">The writer's clock in the record, in milliseconds.</param>
    /// <param name="now">This node's clock at the reading, in milliseconds.</param>
    public long Written(long term, long at, long now)
    {
        if (term != _term || at < _lastAt)
        {
            _term = term;
            _stretchFrom = at;
            _least = _leastBefore = long.MaxValue;
        }
        else if (at - _stretchFrom >= StretchMilliseconds)
        {
            _leastBefore = _least;
            _least = long.MaxValue;
            _stretchFrom = at;
        }
        _lastAt = at;

        var difference = now - at;
        if (difference < _least)
            _least = difference;
        return at + Math.Min(_least, _leastBefore);
    }
}
