namespace Dbsp;

/// <summary>
/// Thrown by a circuit's entry points — <see cref="InputHandle{T}"/> sends,
/// <see cref="Circuit.StepAsync"/> — after the circuit has faulted. The original fault is
/// the <see cref="Exception.InnerException"/> (an <see cref="AggregateException"/> when
/// several workers faulted in the same step) and is also exposed as
/// <see cref="Circuit.Fault"/>. A faulted circuit's in-memory state is unusable — the
/// faulting step's effects are half-applied — so the recovery story is to rebuild the
/// circuit; with a checkpointing storage provider it resumes from the last committed
/// checkpoint, which a fault never touches.
/// </summary>
public sealed class CircuitFaultedException(Exception fault) : Exception(
    "The circuit has faulted and can no longer process data; see the inner exception for the original fault. " +
    "Rebuild the circuit to recover (a checkpointing storage provider resumes from the last committed checkpoint).",
    fault);
