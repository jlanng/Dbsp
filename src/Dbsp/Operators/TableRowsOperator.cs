using Dbsp.Tables;

namespace Dbsp.Operators;

/// <summary>
/// A shared table's changes as a Z-set of its rows (<c>TableRef.Changes()</c>): each
/// <see cref="TableChange{TKey,TRow}"/> becomes its old row retracted and its new row
/// asserted. On the first step of a circuit with no committed position in the table, the
/// table's port stages every current row in <see cref="Bootstrap"/> instead, and they go out
/// as insertions.
/// </summary>
internal sealed class TableRowsOperator<TKey, TRow> : IStepOperator
    where TKey : notnull
    where TRow : class
{
    private readonly ZSet<TRow> _empty = new();
    private readonly OutputBuffer<TRow> _output = new();

    /// <summary>This worker's share of the table's rows, to emit on the next step. Written
    /// by the coordinator between steps, like an input's pending batch, and released once
    /// emitted — it is the size of the table and needed exactly once.</summary>
    public List<TRow>? Bootstrap;

    // No EmptyStepResult: the bootstrap rows arrive beside the delta, not in it.
    public object Step(object?[] inputs)
    {
        var changes = (ZSet<TableChange<TKey, TRow>>)inputs[0]!;
        var bootstrap = Bootstrap;
        if (changes.IsEmpty && bootstrap is null)
            return _empty;

        // The bootstrap is the size of the table and happens once, so it goes out in a
        // Z-set of its own rather than leave this operator's reused buffer that large.
        var output = bootstrap is null
            ? _output.Next(2 * changes.DistinctCount)
            : new ZSet<TRow>(2 * changes.DistinctCount + bootstrap.Count);
        foreach (var (change, weight) in changes)
        {
            if (change.Old is not null)
                output.Add(change.Old, -weight);
            if (change.New is not null)
                output.Add(change.New, weight);
        }
        if (bootstrap is not null)
        {
            foreach (var row in bootstrap)
                output.Add(row, 1);
            Bootstrap = null;
        }
        return output;
    }
}

/// <summary>
/// Re-keys a table's changes by a secondary index: the old row leaves the group under its
/// index key (weight −1) and the new row enters the group under its own (+1). Both are
/// emitted even when the two keys are equal — the group's membership is unchanged then, but
/// the row in it is not, and a join has to correct every fact joined to the old one.
/// </summary>
internal sealed class TableIndexChangesOperator<TIndexKey, TKey, TRow>(TableIndex<TIndexKey, TKey, TRow> index) : IStepOperator
    where TIndexKey : notnull
    where TKey : notnull
    where TRow : class
{
    private readonly ZSet<IndexedRow<TIndexKey, TRow>> _empty = new();
    private readonly OutputBuffer<IndexedRow<TIndexKey, TRow>> _output = new();

    public object? EmptyStepResult => _empty;

    public object Step(object?[] inputs)
    {
        var changes = (ZSet<TableChange<TKey, TRow>>)inputs[0]!;
        var output = _output.Next(2 * changes.DistinctCount);
        foreach (var (change, weight) in changes)
        {
            if (index.TryKey(change.Old, out var before))
                output.Add(new IndexedRow<TIndexKey, TRow>(before, change.Old!), -weight);
            if (index.TryKey(change.New, out var after))
                output.Add(new IndexedRow<TIndexKey, TRow>(after, change.New!), weight);
        }
        return output;
    }
}
