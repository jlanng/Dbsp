using System.Runtime.InteropServices;
using Dbsp.Storage;
using Dbsp.Tables;

namespace Dbsp.Operators;

/// <summary>
/// What a remembered lookup emitted for one fact: <see cref="Row"/>, <see cref="Count"/>
/// times per unit of the fact's weight. A count above one is several table rows under the
/// fact's key enriching to equal results; a count of zero records a fact that found no row
/// and emitted nothing, which still has to be remembered — its weight is what a later
/// re-assertion re-enriches.
/// </summary>
internal readonly record struct LookupResult<TOut>(TOut Row, int Count);

/// <summary>
/// Enrich-on-arrival against a shared table, remembering what was emitted
/// (<c>Lookup</c>). A fact is enriched from the table's rows for its key as of the step
/// that asserts it, and never corrected when those rows change: the table's delta is not an
/// input here at all. What makes that safe to retract is the state — per fact, the enriched
/// rows it was emitted as — so retracting a fact retracts exactly those, whatever the table
/// says by then.
///
/// The invariant: the output integrates to Σ over held facts of (the fact's weight × its
/// remembered rows). A step's net change <c>d</c> to a fact keeps that true in one of two
/// ways. A retraction (<c>d &lt; 0</c>) of a fact that is held scales its remembered rows by
/// <c>d</c> and reads nothing from the table. An assertion (<c>d &gt; 0</c>), or any change
/// to a fact that is not held, probes the table at the step's version and moves the fact's
/// whole weight onto the rows found now — so a fact asserted again is enriched as of its
/// latest assertion, all of it, rather than held as copies enriched at different times.
///
/// State is keyed by the fact, so the input must be partitioned such that equal facts reach
/// the same worker; the grouping's exchange on the lookup key does that, since equal facts
/// have equal keys. Probes need no partitioning.
/// </summary>
internal sealed class TableLookupOperator<TKey, TFact, TRow, TOut> : IStepOperator
    where TKey : notnull
    where TFact : notnull
    where TRow : class
    where TOut : notnull
{
    private readonly IIndexedWeightStore<KeyedValue<TKey, TFact>, LookupResult<TOut>> _remembered;
    private readonly TableGroupProbe<TKey, TRow> _table;
    private readonly Func<TKey, TFact, TRow, TOut> _matched;
    private readonly Func<TKey, TFact, TOut>? _missing;
    private readonly EvictionTracker<KeyedValue<TKey, TFact>>? _eviction;

    public TableLookupOperator(
        OperatorContext context,
        TableGroupProbe<TKey, TRow> table,
        Func<TKey, TFact, TRow, TOut> matched,
        Func<TKey, TFact, TOut>? missing,
        KeyEviction<TKey>? eviction,
        RecordEviction<TFact>? records = null)
    {
        _remembered = context.Storage.CreateIndexedWeightStore<KeyedValue<TKey, TFact>, LookupResult<TOut>>(
            context.StateName("remembered"));
        _table = table;
        _matched = matched;
        _missing = missing;
        _errors = context.Errors;

        // The grouping's policy closes keys; the state here is per fact, so each fact is
        // tracked and closes with its key. A fact that is taken back whole is forgotten
        // (EvictionTracker.Forget): the deadlines are the facts remembered, one each, and
        // not every version a corrected fact ever had.
        // Under EvictWhenFinal each fact closes on its own, when the watermark passes its
        // event time.
        if (eviction is not null)
            _eviction = new EvictionTracker<KeyedValue<TKey, TFact>>(
                context, new KeyEviction<KeyedValue<TKey, TFact>>(eviction.Clock, fact => eviction.ClosesAt(fact.Key)));
        else if (records is not null)
            _eviction = new EvictionTracker<KeyedValue<TKey, TFact>>(context, records.ForKeyed<TKey>());
    }

    private readonly RecordErrors? _errors; // null under the default policy

    private readonly ZSet<TOut> _empty = new();

    // An all-empty step is a no-op — unless eviction is attached, which fires off the
    // watermark and must run whether or not data arrived.
    public object? EmptyStepResult => _eviction is null ? _empty : null;

    // Per-step scratch, reused across steps (single-threaded per worker); see JoinOperator.
    private readonly List<KeyedValue<TKey, TFact>> _facts = [];
    private readonly List<long> _deltas = [];
    private readonly List<WeightedValue<LookupResult<TOut>>> _held = [];
    private readonly List<int> _heldBounds = [];
    private readonly List<TKey> _probeKeys = [];
    private readonly Dictionary<TKey, int> _probeIndex = [];
    private readonly List<int> _probeOfFact = [];
    private readonly List<TRow> _rows = [];
    private readonly List<int> _rowBounds = [];
    private readonly Dictionary<TOut, int> _distinct = [];
    private readonly List<(KeyedValue<TKey, TFact> Key, LookupResult<TOut> Value, long Delta)> _updates = [];
    private readonly OutputBuffer<TOut> _output = new();

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<KeyedValue<TKey, TFact>>)inputs[0]!;
        var output = delta.IsEmpty ? _empty : Apply(delta);

        // Closed keys receive no further facts, so nothing will ever retract what is
        // remembered for them; the rows already emitted remain final.
        if (_eviction?.DrainClosed() is { Count: > 0 } closed)
            _remembered.RemoveGroups(closed);
        return output;
    }

    private ZSet<TOut> Apply(ZSet<KeyedValue<TKey, TFact>> delta)
    {
        _facts.Clear();
        _deltas.Clear();
        _facts.EnsureCapacity(delta.DistinctCount);
        _deltas.EnsureCapacity(delta.DistinctCount);
        foreach (var (fact, weight) in delta)
        {
            _facts.Add(fact);
            _deltas.Add(weight);
        }

        _held.Clear();
        _heldBounds.Clear();
        _remembered.GetGroupsInto(_facts, _held, _heldBounds);

        // Only an assertion, or a fact nothing is remembered for, reads the table; the
        // distinct keys of those are probed once each.
        _probeKeys.Clear();
        _probeIndex.Clear();
        _probeOfFact.Clear();
        for (var i = 0; i < _facts.Count; i++)
        {
            var isHeld = _heldBounds[i] > (i == 0 ? 0 : _heldBounds[i - 1]);
            if (_deltas[i] < 0 && isHeld)
            {
                _probeOfFact.Add(-1);
                continue;
            }
            ref var probe = ref CollectionsMarshal.GetValueRefOrAddDefault(_probeIndex, _facts[i].Key, out var known);
            if (!known)
            {
                probe = _probeKeys.Count;
                _probeKeys.Add(_facts[i].Key);
            }
            _probeOfFact.Add(probe);
        }
        _rows.Clear();
        _rowBounds.Clear();
        _table.GetGroupsInto(_probeKeys, _rows, _rowBounds);

        var output = _output.Next(delta.DistinctCount);
        _updates.Clear();
        for (var i = 0; i < _facts.Count; i++)
        {
            var fact = _facts[i];
            var change = _deltas[i];
            var heldStart = i == 0 ? 0 : _heldBounds[i - 1];
            var heldEnd = _heldBounds[i];
            var probe = _probeOfFact[i];

            if (probe < 0)
            {
                // A retraction of a held fact: the rows it was emitted as, scaled.
                for (var h = heldStart; h < heldEnd; h++)
                    Move(output, fact, _held[h].Value, change);
                if (_eviction is not null && _held[heldStart].Weight + change == 0)
                    _eviction.Forget(fact);
                continue;
            }

            // Every remembered row carries the fact's weight, so any one of them has it.
            var weight = heldStart < heldEnd ? _held[heldStart].Weight : 0;
            for (var h = heldStart; h < heldEnd; h++)
                Move(output, fact, _held[h].Value, -weight);

            var total = checked(weight + change);
            if (total == 0)
            {
                _eviction?.Forget(fact);
                continue;
            }
            var rowStart = probe == 0 ? 0 : _rowBounds[probe - 1];
            var rowEnd = _rowBounds[probe];
            if (_errors is not null)
            {
                EnrichGuarded(output, fact, rowStart, rowEnd, total, change, _errors);
                _eviction?.Track(fact);
                continue;
            }
            if (rowEnd - rowStart > 1)
            {
                // Several rows under the key may enrich to equal results. They are remembered
                // as one result with a count, so every entry held for a fact has its weight.
                _distinct.Clear();
                for (var r = rowStart; r < rowEnd; r++)
                    CollectionsMarshal.GetValueRefOrAddDefault(_distinct, _matched(fact.Key, fact.Value, _rows[r]), out _)++;
                foreach (var (row, count) in _distinct)
                    Move(output, fact, new LookupResult<TOut>(row, count), total);
            }
            else if (rowEnd > rowStart)
                Move(output, fact, new LookupResult<TOut>(_matched(fact.Key, fact.Value, _rows[rowStart]), 1), total);
            else if (_missing is not null)
                Move(output, fact, new LookupResult<TOut>(_missing(fact.Key, fact.Value), 1), total);
            else
                Move(output, fact, new LookupResult<TOut>(default!, 0), total);
            _eviction?.Track(fact);
        }

        _remembered.ApplyAll(_updates);
        return output;
    }

    /// <summary>The dead-letter policy's enrichment of one fact: the same three cases, with
    /// every delegate call made before anything is emitted or remembered. A fact the
    /// delegates throw for is reported and then remembered as having emitted nothing — the
    /// entry with a count of zero that a fact with no row already gets — so its retraction
    /// finds it held and takes nothing away. That is state, not an assumption about the
    /// delegate: a fact dropped here stays dropped until it is asserted again.</summary>
    private void EnrichGuarded(
        ZSet<TOut> output, KeyedValue<TKey, TFact> fact, int rowStart, int rowEnd, long total, long change, RecordErrors errors)
    {
        var single = new LookupResult<TOut>(default!, 0);
        var several = false;
        try
        {
            if (rowEnd - rowStart > 1)
            {
                _distinct.Clear();
                for (var r = rowStart; r < rowEnd; r++)
                {
                    var row = RecordErrors.Result(_matched(fact.Key, fact.Value, _rows[r]));
                    CollectionsMarshal.GetValueRefOrAddDefault(_distinct, row, out _)++;
                }
                several = true;
            }
            else if (rowEnd > rowStart)
                single = new LookupResult<TOut>(
                    RecordErrors.Result(_matched(fact.Key, fact.Value, _rows[rowStart])), 1);
            else if (_missing is not null)
                single = new LookupResult<TOut>(RecordErrors.Result(_missing(fact.Key, fact.Value)), 1);
        }
        catch (Exception ex)
        {
            errors.Add(fact, change, ex);
            several = false;
            single = new LookupResult<TOut>(default!, 0);
        }

        if (several)
            foreach (var (row, count) in _distinct)
                Move(output, fact, new LookupResult<TOut>(row, count), total);
        else
            Move(output, fact, single, total);
    }

    /// <summary>Changes by <paramref name="weight"/> how much of <paramref name="result"/>
    /// is held for <paramref name="fact"/>, and emits the same change.</summary>
    private void Move(ZSet<TOut> output, KeyedValue<TKey, TFact> fact, LookupResult<TOut> result, long weight)
    {
        if (result.Count > 0)
            output.Add(result.Row, checked(weight * result.Count));
        _updates.Add((fact, result, weight));
    }
}

/// <summary>
/// Enrich-on-arrival against a shared table with no state at all
/// (<c>LookupAppendOnly</c>): each record is enriched from the table's rows for its key as
/// of the step that processes it, and that is the end of it. Nothing is kept to retract
/// with, so the stream is declared insert-only and a retraction is an error — emitting a
/// retraction enriched from whatever the table says now would not cancel the row emitted
/// before, and nothing downstream would ever notice. Needs no partitioning: any worker can
/// probe, and there is no state to keep on one.
/// </summary>
internal sealed class TableLookupAppendOnlyOperator<T, TKey, TRow, TOut>(
    TableGroupProbe<TKey, TRow> table,
    string tableName,
    Func<T, TKey> keySelector,
    Func<T, TRow, TOut> matched,
    Func<T, TOut>? missing,
    RecordErrors? errors = null) : IStepOperator
    where T : notnull
    where TKey : notnull
    where TRow : class
    where TOut : notnull
{
    private readonly ZSet<TOut> _empty = new();
    private readonly OutputBuffer<TOut> _output = new();

    public object? EmptyStepResult => _empty;

    // Per-step scratch, reused across steps (single-threaded per worker).
    private readonly List<(T Record, long Weight)> _records = [];
    private readonly List<TKey> _probeKeys = [];
    private readonly Dictionary<TKey, int> _probeIndex = [];
    private readonly List<int> _probeOfRecord = [];
    private readonly List<TRow> _rows = [];
    private readonly List<int> _rowBounds = [];

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<T>)inputs[0]!;
        _records.Clear();
        _probeKeys.Clear();
        _probeIndex.Clear();
        _probeOfRecord.Clear();
        foreach (var (record, weight) in delta)
        {
            TKey key;
            if (errors is null)
            {
                if (weight < 0)
                    throw Retraction(record, weight);
                key = keySelector(record);
            }
            else
            {
                // The dead-letter policy: a retraction, a key selector that throws and a
                // null key each drop the one record.
                try
                {
                    if (weight < 0)
                        throw Retraction(record, weight);
                    key = RecordErrors.Key(keySelector(record));
                }
                catch (Exception ex)
                {
                    errors.Add(record, weight, ex);
                    continue;
                }
            }
            _records.Add((record, weight));
            ref var probe = ref CollectionsMarshal.GetValueRefOrAddDefault(_probeIndex, key, out var known);
            if (!known)
            {
                probe = _probeKeys.Count;
                _probeKeys.Add(key);
            }
            _probeOfRecord.Add(probe);
        }

        _rows.Clear();
        _rowBounds.Clear();
        table.GetGroupsInto(_probeKeys, _rows, _rowBounds);

        var output = _output.Next(delta.DistinctCount);
        for (var i = 0; i < _records.Count; i++)
        {
            var (record, weight) = _records[i];
            var probe = _probeOfRecord[i];
            var start = probe == 0 ? 0 : _rowBounds[probe - 1];
            var end = _rowBounds[probe];
            if (errors is not null)
            {
                EmitGuarded(output, record, weight, start, end, errors);
                continue;
            }
            for (var r = start; r < end; r++)
                output.Add(matched(record, _rows[r]), weight);
            if (start == end && missing is not null)
                output.Add(missing(record), weight);
        }
        return output;
    }

    private InvalidOperationException Retraction(T record, long weight) => new(
        $"LookupAppendOnly against table '{tableName}' received a retraction (weight {weight}) of {record}. " +
        "An append-only lookup keeps no state, so it cannot retract the row it emitted for a record, and the " +
        "stream feeding it must only ever insert. Use Lookup, which remembers what it emitted, on a stream " +
        "that retracts or upserts.");

    // Scratch for the dead-letter policy: a record's results are gathered whole before any
    // is emitted, so a record enriched from several rows is dropped entirely or not at all.
    private List<TOut>? _results;

    private void EmitGuarded(ZSet<TOut> output, T record, long weight, int start, int end, RecordErrors errors)
    {
        var results = _results ??= [];
        results.Clear();
        try
        {
            for (var r = start; r < end; r++)
                results.Add(RecordErrors.Result(matched(record, _rows[r])));
            if (start == end && missing is not null)
                results.Add(RecordErrors.Result(missing(record)));
        }
        catch (Exception ex)
        {
            errors.Add(record, weight, ex);
            return;
        }
        foreach (var result in results)
            output.Add(result, weight);
    }
}
