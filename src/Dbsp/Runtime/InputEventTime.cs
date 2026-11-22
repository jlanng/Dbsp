using Dbsp.Operators;

namespace Dbsp;

/// <summary>A record whose event time is further ahead than
/// <see cref="CircuitOptions.MaxEventTimeAhead"/> allows: a record in error, which the
/// circuit's <see cref="RecordErrorPolicy"/> deals with.</summary>
public sealed class EventTimeAheadException(string message) : Exception(message);

/// <summary>What a circuit's watermark waits for (<see cref="Circuit.WatermarkHeldBy"/>):
/// the event-time source, numbered in the order the definition declares them; the input
/// it is declared on, where it is declared on one; and the partition of that input that
/// is behind, where the source names partitions and one of them holds the others back.</summary>
public sealed record WatermarkHold(int Source, string? Input, int? Partition);

/// <summary>
/// Event time as an input sees it: set when <c>WithEventTime(…, lateness)</c> is declared
/// on the input itself, and used by the input's port as it drains — on the coordinator,
/// between steps, in the order the records were sent. Two things can only be done there.
/// A record's partition is known only where it enters, so this is where the clock is fed
/// by partition (<see cref="WatermarkClock.ObserveFromInput"/>): the declaration's
/// operator, which would otherwise feed the clock, only filters. And a record refused here
/// (<see cref="CircuitOptions.MaxEventTimeAhead"/>) never enters the circuit, so nothing
/// can come after it that takes it back: the verdict may depend on the node's clock
/// without an insertion and its retraction ever being judged differently inside.
/// </summary>
internal sealed class InputEventTime<T>(
    WatermarkClock clock, int tracker, Func<T, DateTimeOffset> eventTime, CircuitOptions options)
    where T : notnull
{
    private readonly long _maxAheadTicks = options.MaxEventTimeAhead?.Ticks ?? -1;
    private readonly TimeProvider _time = options.TimeProvider;
    private long _nowTicks;

    /// <summary>Before each drain: the node's clock is read at most once in it.</summary>
    public void BeginDrain() => _nowTicks = 0;

    public void Note(int partition, InputRejection note)
    {
        if (ReferenceEquals(note, InputRejection.IdlePartition))
            clock.IdlePartition(tracker, partition);
        else
            clock.ExpectPartition(tracker, partition);
    }

    /// <summary>Gives the record's time to the clock and returns null, or returns why the
    /// record is not taken. <paramref name="partition"/> is −1 for an unstamped record.
    /// Under the dead-letter policy (<paramref name="guarded"/>) a record whose time
    /// cannot be read is let through unobserved: the declaration downstream runs the same
    /// selector, fails the same way and reports it, once.</summary>
    public EventTimeAheadException? Observe(T value, int partition, bool guarded)
    {
        long ticks;
        if (guarded)
        {
            if (!TryRead(value, out ticks))
                return null;
        }
        else
            ticks = eventTime(value).UtcTicks;

        if (_maxAheadTicks >= 0)
        {
            if (_nowTicks == 0)
                _nowTicks = _time.GetUtcNow().UtcTicks;
            var reference = Math.Max(_nowTicks, clock.NewestFromInput(tracker));
            if (ticks - _maxAheadTicks > reference)
                return new EventTimeAheadException(
                    $"The record's event time, {new DateTimeOffset(ticks, TimeSpan.Zero):O}, is more than " +
                    $"{TimeSpan.FromTicks(_maxAheadTicks)} (MaxEventTimeAhead) ahead of the node's clock and of the newest event " +
                    $"time this input has brought ({new DateTimeOffset(reference, TimeSpan.Zero):O}). Taken, it would move the " +
                    "watermark to its time and make every record behind it late.");
        }
        clock.ObserveFromInput(tracker, partition, ticks);
        return null;
    }

    private bool TryRead(T value, out long ticks)
    {
        try
        {
            ticks = eventTime(value).UtcTicks;
            return true;
        }
        catch (Exception)
        {
            ticks = 0;
            return false;
        }
    }
}
