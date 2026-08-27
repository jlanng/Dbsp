namespace Incremental.Runtime;

/// <summary>Thrown to a <see cref="SpinBarrier"/> waiter when the barrier is poisoned: a
/// peer faulted mid-step and will never arrive, so waiting on would deadlock. Purely an
/// unwind signal for the worker loop — the faulting worker records the real exception.</summary>
internal sealed class BarrierPoisonedException : Exception;

/// <summary>
/// A reusable sense-reversing barrier tuned for exchange synchronization: mid-step the
/// workers are already hot, so waiters spin (escalating to yields, never sleeping) instead
/// of paying <see cref="Barrier"/>'s kernel block + wake-up on every phase. The interlocked
/// operations provide the fences that make each worker's mailbox writes visible to the
/// others once the phase advances.
/// </summary>
internal sealed class SpinBarrier(int participants)
{
    private readonly int _participants = participants;
    private int _remaining = participants;
    private int _phase;
    private bool _poisoned;

    public void SignalAndWait()
    {
        ThrowIfPoisoned();
        var phase = Volatile.Read(ref _phase);
        if (Interlocked.Decrement(ref _remaining) == 0)
        {
            // Reset for the next phase before releasing: released waiters may re-enter
            // immediately, and they must decrement the new phase's counter.
            Volatile.Write(ref _remaining, _participants);
            Interlocked.Increment(ref _phase);
        }
        else
        {
            var spinner = new SpinWait();
            while (Volatile.Read(ref _phase) == phase)
                spinner.SpinOnce(sleep1Threshold: -1);
        }
        // A phase advance may have been the poisoner releasing us, not the last peer arriving.
        ThrowIfPoisoned();
    }

    /// <summary>Poisons the barrier for the worker-fault path: the faulting participant will
    /// never arrive again, so current waiters are released (the poison flag is written before
    /// the phase advance that wakes them) and every current and future
    /// <see cref="SignalAndWait"/> throws <see cref="BarrierPoisonedException"/> instead of
    /// deadlocking. Idempotent; the barrier is unusable afterwards — as is the circuit.</summary>
    public void Poison()
    {
        Volatile.Write(ref _poisoned, true);
        Interlocked.Increment(ref _phase);
    }

    private void ThrowIfPoisoned()
    {
        if (Volatile.Read(ref _poisoned))
            throw new BarrierPoisonedException();
    }
}
