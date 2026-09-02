using Incremental.Storage;

namespace Incremental.Operators;

/// <summary>
/// Backs a bootstrapping (table-shaped) output — <c>ToOutput(bootstrap: true)</c> on a keyed
/// or windowed stream: passes its input delta through unchanged while integrating it into a
/// store, so the current state of the view can be read back as the bootstrap batch of
/// <see cref="OutputHandle{T}.Changes"/>. The store is an indexed store with a single group
/// rather than a weight store because reading a snapshot needs enumeration, which only
/// <see cref="IIndexedWeightStore{TKey,TValue}.GetGroup"/> offers. Integration happens
/// inside the step — before the checkpoint commit — so with a checkpointing provider the
/// committed snapshot always agrees with the committed input offsets, which is what lets a
/// consumer bootstrap after a restart without replaying deltas. State is per worker, as
/// everywhere; the read merges the workers' shards.
/// </summary>
internal sealed class SnapshotOperator<T> : IStepOperator where T : notnull
{
    private const int Group = 0;

    private readonly IIndexedWeightStore<int, T> _integrated;

    public SnapshotOperator(OperatorContext context) =>
        _integrated = context.Storage.CreateIndexedWeightStore<int, T>(context.StateName("integrated"));

    private readonly ZSet<T> _empty = new();

    // Step already skips the integrated-state update for an empty delta and passes it
    // straight through, so standing in a shared empty changes nothing downstream.
    public object? EmptyStepResult => _empty;

    /// <summary>Flattening buffer for the batched store write, reused across steps: the store
    /// reads it within the call and never retains it, and this operator is single-threaded
    /// per worker. Every table-shaped output has one of these on the step path, so the
    /// per-step allocation it would otherwise make is paid once per output per step.</summary>
    private readonly List<(int, T, long)> _updates = [];

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<T>)inputs[0]!;
        if (!delta.IsEmpty)
        {
            _updates.Clear();
            _updates.EnsureCapacity(delta.DistinctCount);
            foreach (var (value, weight) in delta)
                _updates.Add((Group, value, weight));
            _integrated.ApplyAll(_updates);
        }
        return delta;
    }

    /// <summary>Adds this worker's integrated shard into <paramref name="target"/>. Called
    /// by the coordinator between steps (under the step gate), when the owning worker is
    /// parked — never concurrently with <see cref="Step"/>.</summary>
    public void ReadInto(ZSet<T> target)
    {
        foreach (var entry in _integrated.GetGroup(Group))
            target.Add(entry.Value, entry.Weight);
    }
}
