namespace Incremental.Runtime;

/// <summary>
/// A reusable sense-reversing barrier tuned for exchange synchronization: mid-step the
/// workers are already hot, so waiters spin (escalating to yields, never sleeping) instead
/// of paying <see cref="Barrier"/>'s kernel block + wake-up on every phase. The interlocked
/// operations provide the fences that make each worker's mailbox writes visible to the
/// others once the phase advances.
/// </summary>
internal sealed class SpinBarrier(int participants)
{
    private int _participants = participants;
    private int _remaining = participants;
    private int _phase;

    public void SignalAndWait()
    {
        var phase = Volatile.Read(ref _phase);
        if (Interlocked.Decrement(ref _remaining) == 0)
        {
            // Reset for the next phase before releasing: released waiters may re-enter
            // immediately, and they must decrement the new phase's counter.
            Volatile.Write(ref _remaining, Volatile.Read(ref _participants));
            Interlocked.Increment(ref _phase);
        }
        else
        {
            var spinner = new SpinWait();
            while (Volatile.Read(ref _phase) == phase)
                spinner.SpinOnce(sleep1Threshold: -1);
        }
    }

    /// <summary>Best-effort removal for the worker-fault path: the leaving participant will
    /// never arrive, so if peers were only waiting on it, release them.</summary>
    public void RemoveParticipant()
    {
        Interlocked.Decrement(ref _participants);
        if (Interlocked.Decrement(ref _remaining) == 0)
        {
            Volatile.Write(ref _remaining, Volatile.Read(ref _participants));
            Interlocked.Increment(ref _phase);
        }
    }
}
