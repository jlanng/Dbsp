using Incremental.Storage;

namespace Incremental.Operators;

/// <summary>
/// Backs a snapshot output (<c>Output(name, snapshot: true)</c>): passes its input delta
/// through unchanged while integrating it into a store, so the current state of the view
/// can be read back via the bootstrap read of <see cref="OutputHandle{T}.Changes"/>. The store is an
/// indexed store with a single group rather than a weight store because reading a snapshot
/// needs enumeration, which only <see cref="IIndexedWeightStore{TKey,TValue}.GetGroup"/>
/// offers. Integration happens inside the step — before the checkpoint commit — so with a
/// checkpointing provider the committed snapshot always agrees with the committed input
/// offsets, which is what lets a consumer bootstrap after a restart without replaying
/// deltas. State is per worker, as everywhere; the read merges the workers' shards.
/// </summary>
internal sealed class SnapshotOperator<T> : IStepOperator where T : notnull
{
    private const int Group = 0;

    private readonly IIndexedWeightStore<int, T> _integrated;

    public SnapshotOperator(OperatorContext context) =>
        _integrated = context.Storage.CreateIndexedWeightStore<int, T>(context.StateName("integrated"));

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<T>)inputs[0]!;
        if (!delta.IsEmpty)
        {
            var updates = new List<(int, T, long)>(delta.DistinctCount);
            foreach (var (value, weight) in delta)
                updates.Add((Group, value, weight));
            _integrated.UpdateAll(updates);
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
