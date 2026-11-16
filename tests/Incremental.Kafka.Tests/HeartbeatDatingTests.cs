namespace Incremental.Kafka.Tests;

/// <summary>
/// When a record of the leader topic was written, on the reader's clock
/// (<see cref="HeartbeatDating"/>): the moment a standby counts the leader's silence
/// from. No broker: the rule is arithmetic on two clocks that are never compared.
/// </summary>
public class HeartbeatDatingTests
{
    // The writer's wall clock and the reader's tick count have nothing to do with each
    // other: here the reader's is behind by a little over fifty-four years.
    private const long Skew = -1_717_171_717_000;

    private static long Read(HeartbeatDating dating, long writtenAt, long delay, long term = 1) =>
        dating.Written(term, at: writtenAt, now: writtenAt + Skew + delay);

    /// <summary>A record readable at once is dated when it is read; one that waited is
    /// dated earlier by as long as it waited — the dead leader's last heartbeat, shown
    /// by the brokers nine seconds after it was written.</summary>
    [Fact]
    public void A_record_that_waited_to_be_readable_is_dated_when_it_was_written()
    {
        var dating = new HeartbeatDating();
        var at = 1_000_000L;
        for (var beat = 0; beat < 20; beat++, at += 100)
            Assert.Equal(at + Skew + 3, Read(dating, at, delay: 3));

        // Written a hundred milliseconds after the last, read 8.9 s later.
        Assert.Equal(at + Skew + 3, Read(dating, at, delay: 8_900));

        // And the one after it, in the same batch, is dated by its own writing.
        Assert.Equal(at + 50 + Skew + 3, Read(dating, at + 50, delay: 8_850));
    }

    /// <summary>What was learnt in the stretch before still counts when a new one has
    /// only just begun — a late record may be the very one that begins it.</summary>
    [Fact]
    public void A_late_record_that_begins_a_new_stretch_is_dated_by_the_stretch_before()
    {
        var dating = new HeartbeatDating();
        var at = 1_000_000L;
        for (; at < 1_000_000L + HeartbeatDating.StretchMilliseconds - 50; at += 100)
            Read(dating, at, delay: 4);

        // The first record past the stretch, shown six seconds late.
        at += 200;
        Assert.Equal(at + Skew + 4, Read(dating, at, delay: 6_000));

        // And a prompt one after it is dated when it is read.
        Assert.Equal(at + 6_100 + Skew + 4, Read(dating, at + 6_100, delay: 4));
    }

    /// <summary>The estimate is never later than the reading and never earlier than the
    /// writing, whatever order prompt and late records come in.</summary>
    [Fact]
    public void A_date_is_never_after_the_reading_and_never_before_the_writing()
    {
        var dating = new HeartbeatDating();
        var random = new Random(7);
        var at = 5_000_000L;
        for (var i = 0; i < 2_000; i++)
        {
            at += random.Next(1, 400);
            var delay = random.Next(10) == 0 ? random.Next(0, 12_000) : random.Next(2, 40);
            var now = at + Skew + delay;
            var written = dating.Written(1, at, now);
            Assert.InRange(written, at + Skew, now);
        }
    }

    /// <summary>What a watch reads when it starts — the last records of the topic, old
    /// and all at once — is dated now: nothing has yet said how the clocks stand, and a
    /// silence must not be counted that this node did not sit through.</summary>
    [Fact]
    public void Records_read_before_anything_is_known_of_the_clocks_are_dated_now()
    {
        var dating = new HeartbeatDating();
        var now = 9_000_000L;
        for (var i = 16; i >= 1; i--)
            Assert.Equal(now, dating.Written(3, at: 70_000_000L - i * 1_000, now));
    }

    /// <summary>Another term is another writer and so another clock, and a clock that
    /// went backwards is one too: what was remembered says nothing, and the record is
    /// dated when it is read.</summary>
    [Fact]
    public void A_new_term_and_a_clock_set_back_start_again()
    {
        var dating = new HeartbeatDating();
        var at = 1_000_000L;
        for (var beat = 0; beat < 5; beat++, at += 100)
            Read(dating, at, delay: 2);

        // The next writer's clock is an hour ahead; its first record is not an hour in the future.
        var hour = 3_600_000L;
        Assert.Equal(at + Skew + 2, dating.Written(2, at + hour, at + Skew + 2));
        Assert.Equal(at + 100 + Skew + 2, dating.Written(2, at + hour + 100, at + 100 + Skew + 2));

        // The same node under a new term has been a new process in between.
        Assert.Equal(at + 5_000 + Skew, dating.Written(3, at + hour + 200, at + 5_000 + Skew));

        // Its clock is set back five seconds between two heartbeats, 100 ms apart.
        var back = at + hour + 200 - 5_000;
        Assert.Equal(at + 5_100 + Skew, dating.Written(3, back + 100, at + 5_100 + Skew));
        Assert.Equal(at + 5_200 + Skew, dating.Written(3, back + 200, at + 5_200 + Skew));
    }

    /// <summary>A clock set forward is followed at once. A writer whose records start to
    /// take longer — its clock fell behind by less than a heartbeat, or the reader's ran
    /// on — is dated early for at most two stretches of its own time, and then by what
    /// its records take now.</summary>
    [Fact]
    public void What_is_remembered_of_the_clocks_is_two_stretches_of_the_writers_time()
    {
        var dating = new HeartbeatDating();
        var at = 1_000_000L;
        for (var beat = 0; beat < 10; beat++, at += 100)
            Read(dating, at, delay: 2);

        // The writer's clock jumps a minute ahead: the difference shrinks, and is followed.
        at += 60_000;
        Assert.Equal(at + Skew - 60_000 + 2, dating.Written(1, at, at + Skew - 60_000 + 2));

        // From here every record takes 700 ms longer than any did before (the writer's
        // clock lost 700 ms without ever going backwards between two records).
        var late = 700;
        var first = Read(dating, at + 100, delay: 2 - 60_000 + late);
        Assert.Equal(at + 100 + Skew - 60_000 + 2, first);             // dated early by the 700
        long last = 0, lastNow = 0;
        for (var t = 200L; t <= 2 * HeartbeatDating.StretchMilliseconds + 300; t += 100)
        {
            lastNow = at + t + Skew - 60_000 + 2 + late;
            last = dating.Written(1, at + t, lastNow);
        }
        Assert.Equal(lastNow, last);                                    // and then no longer

        // A writer that falls silent is still measured against its last stretch, however
        // long ago that was on this node's clock: its last record, shown a minute late.
        var silent = at + 2 * HeartbeatDating.StretchMilliseconds + 400;
        Assert.Equal(lastNow + 100, dating.Written(1, silent, lastNow + 100 + 60_000));
    }
}
