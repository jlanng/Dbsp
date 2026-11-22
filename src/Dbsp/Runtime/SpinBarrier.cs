namespace Dbsp.Runtime;

/// <summary>Thrown to a <see cref="SpinBarrier"/> waiter when the barrier is poisoned: a
/// peer faulted mid-step and will never arrive, so waiting would deadlock. Purely an
/// unwind signal for the worker loop — the faulting worker records the real exception.</summary>
internal sealed class BarrierPoisonedException : Exception;

/// <summary>
/// A reusable sense-reversing barrier tuned for exchange synchronization: mid-step the
/// workers are already hot, so waiters spin (escalating to yields) instead of paying
/// <see cref="Barrier"/>'s kernel block + wake-up on every phase. Spinning is budgeted,
/// not unbounded: a wait that outlives the budget — a cluster round trip, a stalled peer,
/// an oversubscribed host — parks on a monitor so it stops burning a core, and the phase
/// advance wakes it. The interlocked operations provide the fences that make each worker's
/// mailbox writes visible to the others once the phase advances.
/// </summary>
internal sealed class SpinBarrier(int participants)
{
    // SpinOnce iterations before a waiter gives up spinning and blocks. SpinWait turns into
    // yields after ~10 iterations, so this is a short burst of pure spins plus ~100 yields —
    // comfortably longer than a healthy local exchange, far shorter than a network wait.
    private const int SpinBudget = 128;

    private readonly int _participants = participants;
    private readonly object _gate = new();
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
            AdvancePhase();
        }
        else
        {
            var spinner = new SpinWait();
            while (Volatile.Read(ref _phase) == phase)
            {
                if (spinner.Count < SpinBudget)
                {
                    spinner.SpinOnce(sleep1Threshold: -1);
                    continue;
                }
                // Budget exhausted: park until the phase advances. The advance pulses under
                // the same lock as this re-check, so a wake-up can never be lost between
                // reading the phase and calling Wait.
                lock (_gate)
                {
                    while (Volatile.Read(ref _phase) == phase)
                        Monitor.Wait(_gate);
                }
            }
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
        AdvancePhase();
    }

    private void AdvancePhase()
    {
        // The increment (a full fence, visible to spinners immediately) and the pulse happen
        // under one lock so a waiter that saw the old phase and is about to park cannot miss
        // the wake-up: it either re-checks before we enter, or waits after we pulse — never
        // in between.
        lock (_gate)
        {
            Interlocked.Increment(ref _phase);
            Monitor.PulseAll(_gate);
        }
    }

    private void ThrowIfPoisoned()
    {
        if (Volatile.Read(ref _poisoned))
            throw new BarrierPoisonedException();
    }
}
