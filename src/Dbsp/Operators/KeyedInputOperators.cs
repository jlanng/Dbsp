using Dbsp.Storage;

namespace Dbsp.Operators;

/// <summary>A keyed input's command after the source has resolved a step's commands per key:
/// upsert <see cref="Value"/> under <see cref="Key"/>, or delete the key. Crosses the
/// exchange as a Z-set member in a clustered circuit, so it is a plain record the wire codec
/// and <see cref="StableHash"/> can both derive.</summary>
internal readonly record struct KeyedCommand<TKey, TValue>(TKey Key, bool IsDelete, TValue? Value)
    where TKey : notnull
    where TValue : notnull;

/// <summary>
/// Entry point of a keyed input. The coordinator appends commands to <see cref="Pending"/>
/// between steps, each on the worker its key routes to and in channel order (see
/// <see cref="Runtime.KeyedInputPort{TKey,TValue}"/>); <see cref="Step"/> resolves them to one
/// command per key — the last one sent — which is exactly the net effect: the intermediate
/// rows of a key amended several times within a step were never visible outside it. The
/// resolution is what lets the commands travel as a Z-set, which carries no order.
/// </summary>
internal sealed class KeyedInputOperator<TKey, TValue> : IStepOperator
    where TKey : notnull
    where TValue : notnull
{
    // Unlike InputOperator's staged Z-set, the pending list never travels downstream — Step
    // consumes it into the output buffer — so one list, cleared per step, is enough.
    private readonly Dictionary<TKey, int> _latest = [];
    private readonly List<KeyedCommand<TKey, TValue>> _resolved = [];
    private readonly OutputBuffer<KeyedCommand<TKey, TValue>> _output = new();

    /// <summary>The commands staged for the next step, in send order. Only the coordinator
    /// writes it, between steps.</summary>
    public List<KeyedCommand<TKey, TValue>> Pending { get; } = [];

    public object Step(object?[] inputs)
    {
        var pending = Pending;
        var output = _output.Next(pending.Count);
        if (pending.Count == 0)
            return output;

        _latest.Clear();
        _resolved.Clear();
        foreach (var command in pending)
        {
            if (_latest.TryGetValue(command.Key, out var index))
                _resolved[index] = command;
            else
            {
                _latest[command.Key] = _resolved.Count;
                _resolved.Add(command);
            }
        }
        pending.Clear();

        foreach (var command in _resolved)
            output.Add(command, 1);
        return output;
    }
}

/// <summary>
/// The state of a keyed input: the current row per key, in a checkpointed value store, so
/// the retraction an upsert or delete owes is derived from durable state rather than from
/// the producer's memory — which is what makes a keyed input safe to re-feed after a
/// restart. Each command is applied against the stored row: an upsert of a different row
/// retracts the stored one and asserts the new one, an upsert of the stored row and a
/// delete of an absent key change nothing. Sits on the worker that owns the key (placed
/// there by the input port, or by an exchange in a clustered circuit).
/// </summary>
internal sealed class UpsertOperator<TKey, TValue> : IStepOperator
    where TKey : notnull
    where TValue : notnull
{
    private readonly IValueStore<TKey, TValue> _rows;

    // Under evictWhenFinal: each key's deadline, from its row's event time, and the keys
    // that fell due this step. Set and removed with the row, so a key amended to a later
    // time moves its deadline with it.
    private readonly RecordEviction<TValue>? _retention;
    private readonly IDeadlineStore<TKey>? _deadlines;
    private readonly List<TKey> _due = [];

    // Non-null under the dead-letter policy: a row whose event time cannot be read is
    // then not held (see DeadlinesGuarded) where it would otherwise fault the step.
    private readonly RecordErrors? _errors;
    private readonly List<long> _closes = [];

    public UpsertOperator(OperatorContext context, RecordEviction<TValue>? retention = null)
    {
        _rows = context.Storage.CreateValueStore<TKey, TValue>(context.StateName("rows"));
        if (retention is not null)
        {
            _retention = retention;
            _deadlines = context.Storage.CreateDeadlineStore<TKey>(context.StateName("deadlines"));
            _errors = context.Errors;
        }
    }

    private readonly ZSet<TValue> _empty = new();

    // All state change is driven by commands, so an all-empty step is a no-op — unless the
    // input forgets final rows, which happens off the watermark, data or not.
    public object? EmptyStepResult => _retention is null ? _empty : null;

    // Per-step scratch, reused across steps (single-threaded per worker).
    private readonly Dictionary<TKey, int> _index = [];
    private readonly List<TKey> _keys = [];
    private readonly List<KeyedCommand<TKey, TValue>> _commands = [];
    private readonly List<(TKey, TValue)> _puts = [];
    private readonly List<TKey> _removes = [];
    private readonly OutputBuffer<TValue> _output = new();

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<KeyedCommand<TKey, TValue>>)inputs[0]!;
        if (delta.IsEmpty && _deadlines is not null)
        {
            ForgetFinal();
            return _output.Next(0);
        }

        // The source resolved each process's commands to one per key; only a clustered
        // circuit can deliver two for a key here (two processes, one step), and then which
        // wins is unspecified — whichever the merge enumerates last.
        var keys = _keys;
        var commands = _commands;
        keys.Clear();
        commands.Clear();
        _index.Clear();
        foreach (var (command, weight) in delta)
        {
            if (weight <= 0)
                continue;
            if (_index.TryGetValue(command.Key, out var at))
                commands[at] = command;
            else
            {
                _index[command.Key] = keys.Count;
                keys.Add(command.Key);
                commands.Add(command);
            }
        }

        // At most one retraction and one assertion per key.
        var output = _output.Next(keys.Count * 2);
        var stored = _rows.TryGetAll(keys);
        var puts = _puts;
        var removes = _removes;
        puts.Clear();
        removes.Clear();

        for (var i = 0; i < keys.Count; i++)
        {
            var command = commands[i];
            var (found, previous) = stored[i];
            if (command.IsDelete)
            {
                if (!found)
                    continue;
                output.Add(previous!, -1);
                removes.Add(command.Key);
                continue;
            }

            var next = command.Value!;
            // An input that forgets its final rows does not apply an upsert that is
            // itself behind the watermark: it is late, and the declaration downstream
            // drops and counts it. Applied, it would take back the row the key holds —
            // which may be open, so its retraction would pass — and put nothing in its
            // place: one late version of a record would delete the record.
            if (_retention is not null && IsLate(next))
            {
                output.Add(next, 1);
                continue;
            }
            if (found)
            {
                if (EqualityComparer<TValue>.Default.Equals(previous, next))
                    continue;
                // An input that forgets its final rows does not take back a row that the
                // new one's own time has made final: that is a new record of the key,
                // not an amendment (RecordEviction.Supersedes).
                if (_retention is null || !BeginsNewRecord(previous!, next))
                    output.Add(previous!, -1);
            }
            output.Add(next, 1);
            puts.Add((command.Key, next));
        }

        if (_deadlines is null)
        {
            _rows.PutAll(puts);
            _rows.RemoveAll(removes);
            return output;
        }

        // Each row's deadline is its event time, read by the user's selector — here,
        // upstream of the declaration whose guarded loop dead-letters a record the selector
        // throws for. So the deadlines are read before anything is stored, and under the
        // dead-letter policy by a loop of their own.
        var closes = _closes;
        closes.Clear();
        if (_errors is not null)
            DeadlinesGuarded(puts, removes, closes);
        else
            foreach (var (_, row) in puts)
                closes.Add(_retention!.ClosesAt(row).UtcTicks);

        _rows.PutAll(puts);
        _rows.RemoveAll(removes);
        for (var i = 0; i < puts.Count; i++)
            _deadlines.Set(puts[i].Item1, closes[i]);
        foreach (var key in removes)
            _deadlines.Remove(key);
        ForgetFinal();
        return output;
    }

    private bool IsLate(TValue next) =>
        _errors is null ? _retention!.IsFinal(next) : _retention!.TryEventTicks(next, out var ticks) && ticks < _retention.Clock.FrozenTicks;

    private bool BeginsNewRecord(TValue previous, TValue next) =>
        _errors is null ? _retention!.Supersedes(previous, next) : BeginsNewRecordGuarded(previous, next);

    // Under the dead-letter policy a row whose time cannot be read amends, as it always
    // did; DeadlinesGuarded deals with it next.
    private bool BeginsNewRecordGuarded(TValue previous, TValue next) =>
        _retention!.TryEventTicks(previous, out var before) && _retention.TryEventTicks(next, out var after)
        && _retention.Supersedes(before, after);

    /// <summary>The dead-letter policy's reading of the deadlines. A row whose event time
    /// cannot be read is not held: it has no deadline, so held it would stay for ever. It
    /// has already been emitted, and the event-time declaration downstream — the one way
    /// out of this operator's state — runs the same selector on it, fails the same way and
    /// reports it; nothing is reported here, or it would be reported twice. Whatever the
    /// key held before goes, since the row that replaced it is not kept: its retraction was
    /// emitted above. A later upsert of the key is then a new row, exactly as it is for a
    /// key whose row was forgotten as final.</summary>
    private void DeadlinesGuarded(List<(TKey, TValue)> puts, List<TKey> removes, List<long> closes)
    {
        var kept = 0;
        for (var i = 0; i < puts.Count; i++)
        {
            long ticks;
            try
            {
                ticks = _retention!.ClosesAt(puts[i].Item2).UtcTicks;
            }
            catch (Exception)
            {
                if (_rows.TryGet(puts[i].Item1, out _))
                    removes.Add(puts[i].Item1);
                continue;
            }
            puts[kept++] = puts[i];
            closes.Add(ticks);
        }
        puts.RemoveRange(kept, puts.Count - kept);
    }

    /// <summary>Forgets the rows the watermark has passed. Nothing is emitted: the
    /// retraction each would owe is behind the watermark, where the event-time
    /// declaration drops it.</summary>
    private void ForgetFinal()
    {
        _due.Clear();
        _deadlines!.TakeDue(_retention!.Clock.FrozenTicks, _due);
        if (_due.Count > 0)
            _rows.RemoveAll(_due);
    }
}
