namespace Incremental.Operators;

/// <summary>
/// The double-buffered output Z-set an operator fills each execution instead of allocating
/// one. Delta-sized dictionaries were most of what the library allocated per step, and the
/// large ones landed on the LOH — where the only collector that reclaims them is a gen2,
/// so purely step-scoped garbage kept re-marking the whole live set on any GC
/// configuration the host happened to have (see benchmarks/Incremental.Torture/README.md).
/// A reused buffer is allocated once, grows to its edge's high-water mark, and stops
/// feeding the collector entirely. (A mark set by one enormous delta and not approached
/// again is given back after a while — see <see cref="ZSet{T}.Clear"/> — because clearing
/// a buffer costs time in proportion to its capacity.)
///
/// Reuse is safe for the same reason <see cref="ExchangeOperator{T}"/> reuses its
/// fragments: every consumer of a node's output reads it before the producing operator
/// executes again (downstream operators within the step, in topological order; output
/// ports copy when they stage or publish — they never adopt a slot's Z-set). Two buffers
/// rather than one because a Recurse region's feedback edge retains the loop result for
/// exactly one more execution of its producer (<c>SetPending</c> hands iteration k's
/// delta to iteration k+1); the producer is on the other buffer by then and only clears
/// this one at k+2, when the feedback has moved on. The rule an operator must honor:
/// everything it returns from <c>Step</c> comes from <see cref="Next"/> (or is a
/// never-mutated shared empty / a pass-through of its input), and nothing it hands out
/// is written to after return.
/// </summary>
internal sealed class OutputBuffer<T> where T : notnull
{
    private readonly ZSet<T> _a = new(), _b = new();
    private bool _useB;

    /// <summary>The buffer to fill this execution, cleared, with room for
    /// <paramref name="capacity"/> entries — operators keep sizing their output from the
    /// delta they derive it from, so a step's fill never walks the doubling ladder.</summary>
    public ZSet<T> Next(int capacity)
    {
        var output = (_useB = !_useB) ? _b : _a;
        output.Clear();
        output.EnsureCapacity(capacity);
        return output;
    }
}
