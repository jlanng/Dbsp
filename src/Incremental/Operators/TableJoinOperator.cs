using Incremental.Storage;
using Incremental.Tables;

namespace Incremental.Operators;

/// <summary>
/// Retroactive join of a grouped stream (the facts) against a shared table on the table's
/// key. The same bilinear rule as <see cref="JoinOperator{TKey,TLeft,TRight,TOut}"/>, with
/// the right-hand state replaced by a probe of the table at the step's pinned version — so
/// the only state is the fact side, however many circuits join the table:
/// Δ(F ⋈ T) = F ⋈ ΔT + ΔF ⋈ T_t, in that order. Table changes are applied to the facts
/// held before this step (retract the row built from <c>Old</c>, assert the one built from
/// <c>New</c>); then the new facts probe the table at <c>t</c>, which already includes those
/// changes; then the fact delta is integrated. Integrating first would correct new facts
/// from an old row they were never joined to.
///
/// With an <c>unmatched</c> selector this is the left join: a missing row on either side of
/// a change, or a probe that finds nothing, stands for the unmatched row. A table holds at
/// most one row per key, so a key is matched or it is not, and the anti-join's second copy
/// of the facts is not needed.
///
/// The fact delta must be partitioned by key and the change delta routed by the same hash
/// (the grouping's exchange, and one like it over the table's changes), so that each worker
/// sees the changes for the keys whose facts it holds. Probes need no partitioning.
/// </summary>
internal sealed class TableJoinOperator<TKey, TFact, TRow, TOut> : IStepOperator
    where TKey : notnull
    where TFact : notnull
    where TRow : class
    where TOut : notnull
{
    private readonly IIndexedWeightStore<TKey, TFact> _facts;
    private readonly TableProbe<TKey, TRow> _table;
    private readonly Func<TKey, TFact, TRow, TOut> _matched;
    private readonly Func<TKey, TFact, TOut>? _unmatched;
    private readonly EvictionTracker<TKey>? _eviction;
    private readonly FactRetention<TKey, TFact>? _retention;

    public TableJoinOperator(
        OperatorContext context,
        TableProbe<TKey, TRow> table,
        Func<TKey, TFact, TRow, TOut> matched,
        Func<TKey, TFact, TOut>? unmatched,
        KeyEviction<TKey>? eviction,
        RecordEviction<TFact>? records = null)
    {
        _facts = context.Storage.CreateIndexedWeightStore<TKey, TFact>(context.StateName("facts"));
        _table = table;
        _matched = matched;
        _unmatched = unmatched;
        _eviction = eviction is null ? null : new EvictionTracker<TKey>(context, eviction);
        _retention = records is null ? null : new FactRetention<TKey, TFact>(context, records, _facts);
        _errors = context.Errors;
    }

    private readonly RecordErrors? _errors; // null under the default policy

    private readonly ZSet<TOut> _empty = new();

    // Every state change below is driven by a delta row, so an all-empty step is a
    // no-op — unless a KeyEviction is attached, whose DrainClosed fires off the
    // watermark instead and must run whether or not data arrived.
    public object? EmptyStepResult => _eviction is null && _retention is null ? _empty : null;

    // Per-step scratch, reused across steps (single-threaded per worker); see JoinOperator.
    private readonly List<(TKey Key, TFact Value, long Delta)> _factEntries = [];
    private readonly List<(TableChange<TKey, TRow> Change, long Weight)> _changeEntries = [];
    private readonly List<TKey> _probeKeys = [];
    private readonly Dictionary<TKey, int> _probeIndex = [];
    private readonly List<int> _groupOfEntry = [];
    private readonly List<int> _bounds = [];
    private readonly List<WeightedValue<TFact>> _factRows = [];
    private readonly List<TRow?> _tableRows = [];
    private readonly OutputBuffer<TOut> _output = new();

    // The output delta under construction, shared by the two applications within one step
    // and handed out once at the end.
    private ZSet<TOut>? _current;

    public object Step(object?[] inputs)
    {
        if ((ZSet<TableChange<TKey, TRow>>)inputs[1]! is { IsEmpty: false } changes)
            ApplyChanges(changes);
        if ((ZSet<KeyedValue<TKey, TFact>>)inputs[0]! is { IsEmpty: false } deltaFacts)
            ApplyFacts(deltaFacts);

        // Closed keys receive no further facts (the watermark drops their events), so their
        // fact state is deleted; results already emitted remain final, and a later change
        // to the key's table row finds nothing to correct.
        if (_eviction?.DrainClosed() is { Count: > 0 } closed)
            _facts.RemoveGroups(closed);
        // Under EvictWhenFinal, the facts the watermark has passed: one by one, since the
        // key (an instrument, a trader) never closes.
        _retention?.EvictFinal();

        var output = _current ?? _output.Next(0);
        _current = null;
        return output;
    }

    /// <summary>F ⋈ ΔT: every fact held under a changed key moves from the row built with
    /// the old table row to the one built with the new. Nothing is tracked for eviction —
    /// a change creates no state, and most changed keys have no facts here at all.</summary>
    private void ApplyChanges(ZSet<TableChange<TKey, TRow>> changes)
    {
        var output = _current ??= _output.Next(changes.DistinctCount);
        _changeEntries.Clear();
        _probeKeys.Clear();
        _changeEntries.EnsureCapacity(changes.DistinctCount);
        foreach (var (change, weight) in changes)
        {
            _changeEntries.Add((change, weight));
            _probeKeys.Add(change.Key);
        }

        _factRows.Clear();
        _bounds.Clear();
        _facts.GetGroupsInto(_probeKeys, _factRows, _bounds);
        for (var i = 0; i < _changeEntries.Count; i++)
        {
            var (change, changeWeight) = _changeEntries[i];
            for (var f = i == 0 ? 0 : _bounds[i - 1]; f < _bounds[i]; f++)
            {
                var (fact, factWeight) = _factRows[f];
                // Final, and only not yet evicted because eviction runs at the end of the
                // step: a final fact's result is not corrected.
                if (_retention is not null && _retention.IsFinal(fact))
                    continue;
                var weight = checked(changeWeight * factWeight);
                Emit(output, change.Key, fact, change.Old, -weight);
                Emit(output, change.Key, fact, change.New, weight);
            }
        }
    }

    /// <summary>ΔF ⋈ T_t, then ΔF integrated.</summary>
    private void ApplyFacts(ZSet<KeyedValue<TKey, TFact>> delta)
    {
        var output = _current ??= _output.Next(delta.DistinctCount);
        output.EnsureCapacity(output.DistinctCount + delta.DistinctCount);

        // Flattened into the list the store integrates, with the distinct keys to probe and
        // each entry's index into them, so a key with many new facts is probed once.
        _factEntries.Clear();
        _probeKeys.Clear();
        _probeIndex.Clear();
        _groupOfEntry.Clear();
        _factEntries.EnsureCapacity(delta.DistinctCount);
        _groupOfEntry.EnsureCapacity(delta.DistinctCount);
        foreach (var ((key, fact), weight) in delta)
        {
            _factEntries.Add((key, fact, weight));
            ref var group = ref System.Runtime.InteropServices.CollectionsMarshal
                .GetValueRefOrAddDefault(_probeIndex, key, out var known);
            if (!known)
            {
                group = _probeKeys.Count;
                _probeKeys.Add(key);
            }
            _groupOfEntry.Add(group);
        }

        _tableRows.Clear();
        _table.GetManyInto(_probeKeys, _tableRows);
        for (var i = 0; i < _factEntries.Count; i++)
        {
            var (key, fact, weight) = _factEntries[i];
            Emit(output, key, fact, _tableRows[_groupOfEntry[i]], weight);
            _eviction?.Track(key);
            if (weight > 0)
                _retention?.Track(key, fact);
        }
        _facts.ApplyAll(_factEntries);
    }

    private void Emit(ZSet<TOut> output, TKey key, TFact fact, TRow? row, long weight)
    {
        if (_errors is { } errors)
        {
            // The dead-letter policy: a (fact, row) pair the selector throws for is left
            // out, when it is asserted and — the selector being a pure function of the
            // pair — when a change to the row or to the fact takes it away again.
            if (row is not null)
                errors.Emit(output, _matched, key, fact, row, weight, new KeyedValue<TKey, TFact>(key, fact));
            else if (_unmatched is not null)
                errors.Emit(output, _unmatched, key, fact, weight);
            return;
        }
        if (row is not null)
            output.Add(_matched(key, fact, row), weight);
        else if (_unmatched is not null)
            output.Add(_unmatched(key, fact), weight);
    }
}
