using System.Runtime.InteropServices;
using Incremental.Storage;
using Incremental.Tables;

namespace Incremental.Operators;

/// <summary>
/// Retroactive join of a grouped stream (the facts) against a shared table through one of
/// its secondary indexes. The rule is <see cref="TableJoinOperator{TKey,TFact,TRow,TOut}"/>'s
/// — Δ(F ⋈ T) = F ⋈ ΔT + ΔF ⋈ T_t, table changes first, fact state the only state — with
/// one difference that reshapes it: a key has a <em>group</em> of rows, not one row or none.
///
/// So the table's delta arrives as rows entering and leaving groups
/// (<see cref="IndexedRow{TKey,TRow}"/>, weight ±1), each joined to the facts under its key,
/// and a fact probes the index for every row under its key. For the left join, "the key had
/// no rows" can no longer be read off a null: the group's size now comes from the index at
/// the pinned version, and its size before from that minus the step's net change to it —
/// which this worker sees whole, because the changes are routed by the same hash as the
/// facts. Only a group going from empty to not, or back, moves facts between their
/// unmatched row and their matched ones.
/// </summary>
internal sealed class TableIndexJoinOperator<TIndexKey, TKey, TFact, TRow, TOut> : IStepOperator
    where TIndexKey : notnull
    where TKey : notnull
    where TFact : notnull
    where TRow : class
    where TOut : notnull
{
    private readonly IIndexedWeightStore<TIndexKey, TFact> _facts;
    private readonly TableIndexProbe<TIndexKey, TKey, TRow> _index;
    private readonly Func<TIndexKey, TFact, TRow, TOut> _matched;
    private readonly Func<TIndexKey, TFact, TOut>? _unmatched;
    private readonly EvictionTracker<TIndexKey>? _eviction;
    private readonly FactRetention<TIndexKey, TFact>? _retention;

    public TableIndexJoinOperator(
        OperatorContext context,
        TableIndexProbe<TIndexKey, TKey, TRow> index,
        Func<TIndexKey, TFact, TRow, TOut> matched,
        Func<TIndexKey, TFact, TOut>? unmatched,
        KeyEviction<TIndexKey>? eviction,
        RecordEviction<TFact>? records = null)
    {
        _facts = context.Storage.CreateIndexedWeightStore<TIndexKey, TFact>(context.StateName("facts"));
        _index = index;
        _matched = matched;
        _unmatched = unmatched;
        _eviction = eviction is null ? null : new EvictionTracker<TIndexKey>(context, eviction);
        _retention = records is null ? null : new FactRetention<TIndexKey, TFact>(context, records, _facts);
        _errors = context.Errors;
    }

    // Null under the default policy; see TableJoinOperator.Emit for what the other one does.
    private readonly RecordErrors? _errors;

    private readonly ZSet<TOut> _empty = new();

    // As in TableJoinOperator: an all-empty step is a no-op unless eviction is attached.
    public object? EmptyStepResult => _eviction is null && _retention is null ? _empty : null;

    // Per-step scratch, reused across steps (single-threaded per worker); see JoinOperator.
    private readonly List<(TIndexKey Key, TFact Value, long Delta)> _factEntries = [];
    private readonly List<(IndexedRow<TIndexKey, TRow> Change, long Weight)> _changeEntries = [];
    private readonly List<TIndexKey> _probeKeys = [];
    private readonly Dictionary<TIndexKey, int> _probeIndex = [];
    private readonly List<int> _groupOfEntry = [];
    private readonly List<long> _netOfGroup = [];
    private readonly List<int> _factBounds = [];
    private readonly List<WeightedValue<TFact>> _factRows = [];
    private readonly List<int> _rowBounds = [];
    private readonly List<TRow> _tableRows = [];
    private readonly OutputBuffer<TOut> _output = new();

    // The output delta under construction, shared by the two applications within one step.
    private ZSet<TOut>? _current;

    public object Step(object?[] inputs)
    {
        if ((ZSet<IndexedRow<TIndexKey, TRow>>)inputs[1]! is { IsEmpty: false } changes)
            ApplyChanges(changes);
        if ((ZSet<KeyedValue<TIndexKey, TFact>>)inputs[0]! is { IsEmpty: false } deltaFacts)
            ApplyFacts(deltaFacts);

        if (_eviction?.DrainClosed() is { Count: > 0 } closed)
            _facts.RemoveGroups(closed);
        _retention?.EvictFinal();

        var output = _current ?? _output.Next(0);
        _current = null;
        return output;
    }

    /// <summary>The distinct keys of a delta go into <see cref="_probeKeys"/>; returns the
    /// index of <paramref name="key"/> among them.</summary>
    private int GroupOf(TIndexKey key)
    {
        ref var group = ref CollectionsMarshal.GetValueRefOrAddDefault(_probeIndex, key, out var known);
        if (!known)
        {
            group = _probeKeys.Count;
            _probeKeys.Add(key);
        }
        return group;
    }

    /// <summary>F ⋈ ΔT: every fact held under a key gains the result built from each row
    /// that entered the key's group and loses the one built from each row that left.</summary>
    private void ApplyChanges(ZSet<IndexedRow<TIndexKey, TRow>> changes)
    {
        var output = _current ??= _output.Next(changes.DistinctCount);
        _changeEntries.Clear();
        _probeKeys.Clear();
        _probeIndex.Clear();
        _groupOfEntry.Clear();
        _netOfGroup.Clear();
        foreach (var (change, weight) in changes)
        {
            var group = GroupOf(change.Key);
            if (group == _netOfGroup.Count)
                _netOfGroup.Add(0);
            _netOfGroup[group] += weight;
            _changeEntries.Add((change, weight));
            _groupOfEntry.Add(group);
        }

        _factRows.Clear();
        _factBounds.Clear();
        _facts.GetGroupsInto(_probeKeys, _factRows, _factBounds);
        var errors = _errors;
        for (var i = 0; i < _changeEntries.Count; i++)
        {
            var (change, changeWeight) = _changeEntries[i];
            var group = _groupOfEntry[i];
            for (var f = group == 0 ? 0 : _factBounds[group - 1]; f < _factBounds[group]; f++)
            {
                var (fact, factWeight) = _factRows[f];
                // Final, and not yet evicted only because eviction runs at the end of the step.
                if (_retention is not null && _retention.IsFinal(fact))
                    continue;
                if (errors is null)
                    output.Add(_matched(change.Key, fact, change.Row), checked(changeWeight * factWeight));
                else
                    errors.Emit(output, _matched, change.Key, fact, change.Row, checked(changeWeight * factWeight),
                        new KeyedValue<TIndexKey, TFact>(change.Key, fact));
            }
        }

        if (_unmatched is null)
            return;
        for (var group = 0; group < _probeKeys.Count; group++)
        {
            var start = group == 0 ? 0 : _factBounds[group - 1];
            if (start == _factBounds[group])
                continue;

            // The group as of this step's version, and as of the previous step's: all of
            // the difference between them is in this delta.
            var key = _probeKeys[group];
            var after = _index.Count(key);
            var before = after - _netOfGroup[group];
            if (before == 0 == (after == 0))
                continue;
            var sign = after == 0 ? 1 : -1;
            for (var f = start; f < _factBounds[group]; f++)
            {
                var (fact, factWeight) = _factRows[f];
                if (_retention is not null && _retention.IsFinal(fact))
                    continue;
                if (errors is null)
                    output.Add(_unmatched(key, fact), sign * factWeight);
                else
                    errors.Emit(output, _unmatched, key, fact, sign * factWeight);
            }
        }
    }

    /// <summary>ΔF ⋈ T_t, then ΔF integrated.</summary>
    private void ApplyFacts(ZSet<KeyedValue<TIndexKey, TFact>> delta)
    {
        var output = _current ??= _output.Next(delta.DistinctCount);
        output.EnsureCapacity(output.DistinctCount + delta.DistinctCount);

        _factEntries.Clear();
        _probeKeys.Clear();
        _probeIndex.Clear();
        _groupOfEntry.Clear();
        _factEntries.EnsureCapacity(delta.DistinctCount);
        _groupOfEntry.EnsureCapacity(delta.DistinctCount);
        foreach (var ((key, fact), weight) in delta)
        {
            _factEntries.Add((key, fact, weight));
            _groupOfEntry.Add(GroupOf(key));
        }

        _tableRows.Clear();
        _rowBounds.Clear();
        _index.GetGroupsInto(_probeKeys, _tableRows, _rowBounds);
        var errors = _errors;
        for (var i = 0; i < _factEntries.Count; i++)
        {
            var (key, fact, weight) = _factEntries[i];
            var group = _groupOfEntry[i];
            var start = group == 0 ? 0 : _rowBounds[group - 1];
            var end = _rowBounds[group];
            if (errors is null)
            {
                for (var r = start; r < end; r++)
                    output.Add(_matched(key, fact, _tableRows[r]), weight);
                if (start == end && _unmatched is not null)
                    output.Add(_unmatched(key, fact), weight);
            }
            else
            {
                for (var r = start; r < end; r++)
                    errors.Emit(output, _matched, key, fact, _tableRows[r], weight, new KeyedValue<TIndexKey, TFact>(key, fact));
                if (start == end && _unmatched is not null)
                    errors.Emit(output, _unmatched, key, fact, weight);
            }
            _eviction?.Track(key);
            if (weight > 0)
                _retention?.Track(key, fact);
        }
        _facts.ApplyAll(_factEntries);
    }
}
