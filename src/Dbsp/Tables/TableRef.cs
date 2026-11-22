using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Dbsp.Operators;
using Dbsp.Runtime;

namespace Dbsp.Tables;

/// <summary>
/// A shared table as a circuit joins it: its rows, found by <typeparamref name="TKey"/>.
/// That is either the table's own key (<see cref="TableRef{TKey,TRow}"/>, one row per key) or
/// a secondary index (<see cref="TableRef{TKey,TRow}.By"/>, any number of rows per key).
/// A staging type like <c>GroupedZStream</c>: something to join or look up against, and not
/// itself a stream.
/// </summary>
public abstract class TableView<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    private protected TableView(CircuitBuilder builder) => Builder = builder;

    internal CircuitBuilder Builder { get; }

    /// <summary>The table's name, for error messages.</summary>
    internal abstract string TableName { get; }

    /// <summary>The node the table's changes enter the circuit at: the table's node in
    /// <see cref="CircuitBuilder.Describe"/>.</summary>
    internal abstract int TableNodeId { get; }

    /// <summary>Reads the rows under a key as of the version the running step pinned.</summary>
    internal abstract TableGroupProbe<TKey, TRow> GroupProbe { get; }

    /// <summary>Adds the retroactive join of <paramref name="facts"/> against this view;
    /// the left join when <paramref name="unmatched"/> is given.</summary>
    internal abstract ZStream<TOut> Join<TFact, TOut>(
        GroupedZStream<TKey, TFact> facts,
        string operation,
        Func<TKey, TFact, TRow, TOut> matched,
        Func<TKey, TFact, TOut>? unmatched,
        RecordEviction<TFact>? records = null)
        where TFact : notnull
        where TOut : notnull;

    /// <summary>What every operation against a table checks first. A table's delta is per
    /// outer step and its probe is per pinned version; neither has a meaning per iteration
    /// of a <c>Recurse</c> body.</summary>
    internal void RequireUsableFrom(CircuitBuilder builder, string operation)
    {
        builder.RequireOutsideRecursion($"{operation} against a table");
        if (!ReferenceEquals(builder, Builder))
            throw new ArgumentException("The table was attached to a different circuit builder.", "table");
    }

    /// <summary>Sends a stream of table changes to the workers holding the facts they
    /// affect: through an exchange routed by the very hash <paramref name="facts"/> was
    /// grouped with. On an unsharded circuit there is nowhere to send them.</summary>
    private protected int RouteLikeFacts<TFact, TChange>(
        GroupedZStream<TKey, TFact> facts, string operation, int changes, Func<TChange, TKey> keyOf)
        where TFact : notnull
        where TChange : notnull
    {
        if (!Builder.Options.IsSharded)
            return changes;

        // Every grouping a sharded circuit hands out carries its hash today; one that did
        // not would have its changes routed to the wrong workers without any error.
        var keyHash = facts.RoutingHash
            ?? throw new InvalidOperationException(
                $"{operation} against a table needs to know how this grouping's keys were routed across workers.");
        return Builder.AddNode(
            "exchange",
            [changes],
            _ => new ExchangeOperator<TChange>(change => keyHash(keyOf(change))),
            isExchange: true);
    }
}

/// <summary>
/// A <see cref="SharedTable{TKey,TRow}"/> attached to a circuit under construction, returned
/// by <see cref="CircuitBuilder.Table{TKey,TRow}"/>. Join or look up against it on the
/// table's key, on another column through <see cref="By"/>, or read the table itself as a
/// stream with <see cref="Changes"/>.
/// </summary>
public sealed class TableRef<TKey, TRow> : TableView<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    internal TableRef(CircuitBuilder builder, SharedTable<TKey, TRow> table, int changesNodeId)
        : base(builder)
    {
        Table = table;
        ChangesNodeId = changesNodeId;
        Probe = new TableProbe<TKey, TRow>(table);
    }

    internal SharedTable<TKey, TRow> Table { get; }

    /// <summary>The node whose per-step output is the table's net changes since the
    /// circuit's previous step: a <c>ZSet&lt;TableChange&gt;</c>, one entry per changed key,
    /// each with weight +1, spread across workers like any input's records.</summary>
    internal int ChangesNodeId { get; }

    /// <summary>The node behind <see cref="Changes"/>, once something has asked for it. The
    /// table's port stages the bootstrap rows of a new circuit straight into it.</summary>
    internal int? RowsNodeId { get; private set; }

    /// <summary>Reads the table as of the version the running step pinned.</summary>
    internal TableProbe<TKey, TRow> Probe { get; }

    /// <summary><see cref="ChangesNodeId"/> as a stream, for operators to consume. A join
    /// routes it through its key exchange, so each worker receives the changes for the keys
    /// whose facts it holds.</summary>
    internal ZStream<TableChange<TKey, TRow>> ChangeStream => new(Builder, ChangesNodeId);

    internal override string TableName => Table.Name;

    internal override int TableNodeId => ChangesNodeId;

    internal override TableGroupProbe<TKey, TRow> GroupProbe => Probe;

    /// <summary>
    /// The same table found by another column: an index from <paramref name="selector"/>'s
    /// value to the rows that have it, for joining facts that carry that value rather than
    /// the table's key. Several rows may share a value — a join then yields one result per
    /// row — and a row whose selector returns null is under no value at all.
    /// <para>
    /// The index belongs to the table, not to this circuit: it is identified by
    /// <paramref name="name"/>, every circuit that asks for the name shares it, and the
    /// first selector declared under a name is the one used (delegates cannot be compared,
    /// so use one name for one meaning). Asking for a new name on a table that is already
    /// loaded builds the index with one scan, there and then; see
    /// <see cref="SharedTable{TKey,TRow}.AddIndex"/>.
    /// </para>
    /// </summary>
    public TableView<TIndexKey, TRow> By<TIndexKey>(string name, Func<TRow, TIndexKey?> selector)
        where TIndexKey : notnull =>
        new TableIndexView<TIndexKey, TKey, TRow>(this, Table.Index(name, selector));

    /// <summary>
    /// The table itself as a stream: every change as the old row retracted and the new row
    /// asserted, for aggregating or re-keying reference data. In a new circuit the stream
    /// begins with every row the table holds at the circuit's first step, as insertions, so
    /// whatever is computed from it covers the rows loaded before the circuit existed. A
    /// circuit restarted from a checkpoint resumes from the version it committed and is not
    /// bootstrapped again — its state already holds those rows. That first delta is the
    /// size of the table, in one step.
    /// </summary>
    public ZStream<TRow> Changes()
    {
        Builder.RequireOutsideRecursion("Changes");
        RowsNodeId ??= Builder.AddNode("tableRows", [ChangesNodeId], _ => new TableRowsOperator<TKey, TRow>());
        return new ZStream<TRow>(Builder, RowsNodeId.Value);
    }

    internal override ZStream<TOut> Join<TFact, TOut>(
        GroupedZStream<TKey, TFact> facts,
        string operation,
        Func<TKey, TFact, TRow, TOut> matched,
        Func<TKey, TFact, TOut>? unmatched,
        RecordEviction<TFact>? records = null)
    {
        var changes = RouteLikeFacts<TFact, TableChange<TKey, TRow>>(facts, operation, ChangesNodeId, change => change.Key);
        var probe = Probe;
        var eviction = facts.Eviction;
        return new ZStream<TOut>(
            Builder,
            Builder.AddNode(
                "tableJoin",
                [facts.NodeId, changes],
                ctx => new TableJoinOperator<TKey, TFact, TRow, TOut>(ctx, probe, matched, unmatched, eviction, records)));
    }
}

/// <summary>A table seen through one of its secondary indexes; what
/// <see cref="TableRef{TKey,TRow}.By"/> returns.</summary>
internal sealed class TableIndexView<TIndexKey, TKey, TRow>(TableRef<TKey, TRow> table, TableIndex<TIndexKey, TKey, TRow> index)
    : TableView<TIndexKey, TRow>(table.Builder)
    where TIndexKey : notnull
    where TKey : notnull
    where TRow : class
{
    private readonly TableIndexProbe<TIndexKey, TKey, TRow> _probe = new(table.Table, index, table.Probe);

    internal override string TableName => table.Table.Name;

    internal override int TableNodeId => table.ChangesNodeId;

    internal override TableGroupProbe<TIndexKey, TRow> GroupProbe => _probe;

    internal override ZStream<TOut> Join<TFact, TOut>(
        GroupedZStream<TIndexKey, TFact> facts,
        string operation,
        Func<TIndexKey, TFact, TRow, TOut> matched,
        Func<TIndexKey, TFact, TOut>? unmatched,
        RecordEviction<TFact>? records = null)
    {
        // The table's changes are by primary key. Re-keyed by the index — the old row
        // leaving its group, the new row entering its own — they are the delta of exactly
        // the relation the facts are joined to, and can be routed like the facts.
        var rekeyed = Builder.AddNode(
            "tableIndexChanges",
            [table.ChangesNodeId],
            _ => new TableIndexChangesOperator<TIndexKey, TKey, TRow>(index));
        var changes = RouteLikeFacts<TFact, IndexedRow<TIndexKey, TRow>>(facts, operation, rekeyed, change => change.Key);
        var probe = _probe;
        var eviction = facts.Eviction;
        return new ZStream<TOut>(
            Builder,
            Builder.AddNode(
                "tableIndexJoin",
                [facts.NodeId, changes],
                ctx => new TableIndexJoinOperator<TIndexKey, TKey, TFact, TRow, TOut>(ctx, probe, matched, unmatched, eviction, records)));
    }
}

/// <summary>
/// How an operator reads the rows under a key of an attached table — its primary key or an
/// index key — as of the version the circuit pinned for the step in flight. Shaped like the
/// stores' <c>GetGroupsInto</c>, so one operator serves keys with one row and keys with many.
/// </summary>
internal abstract class TableGroupProbe<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    /// <summary>Appends every requested key's rows to <paramref name="rows"/>, end to end,
    /// and one entry per key to <paramref name="bounds"/>: the exclusive end offset of that
    /// key's run. Both buffers are appended to, never cleared — they are the caller's.</summary>
    public abstract void GetGroupsInto(List<TKey> keys, List<TRow> rows, List<int> bounds);
}

/// <summary>
/// How an operator reads an attached table: always as of the version the circuit pinned for
/// the step in flight, from any worker, without locking or allocating. One instance per
/// circuit per table, shared by every worker; the coordinator sets the version before it
/// releases the workers into a step, and no operator may read outside one.
/// </summary>
internal sealed class TableProbe<TKey, TRow>(SharedTable<TKey, TRow> table) : TableGroupProbe<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    /// <summary>The step's pinned version. Written by the coordinator between steps only.</summary>
    internal long Version;

    /// <summary>The row for <paramref name="key"/> as of the step's version. Throws if the
    /// step has held its pin past <see cref="SharedTableOptions.MaxRetainedVersions"/>.</summary>
    public bool TryGet(TKey key, [MaybeNullWhen(false)] out TRow row) => table.TryGet(key, Version, out row);

    /// <summary>Batched <see cref="TryGet"/>: appends one entry per key to
    /// <paramref name="into"/>, null where the key has no row, aligned with
    /// <paramref name="keys"/>. Appends, never clears — the buffer is the caller's.</summary>
    public void GetManyInto(List<TKey> keys, List<TRow?> into)
    {
        var version = Version;
        foreach (var key in keys)
        {
            table.TryGet(key, version, out var row);
            into.Add(row);
        }
    }

    public override void GetGroupsInto(List<TKey> keys, List<TRow> rows, List<int> bounds)
    {
        var version = Version;
        foreach (var key in keys)
        {
            if (table.TryGet(key, version, out var row))
                rows.Add(row);
            bounds.Add(rows.Count);
        }
    }
}

/// <summary>
/// Reads an attached table through a secondary index, as of the step's pinned version: the
/// index gives the primary keys under an index key and the table gives their rows, both at
/// that one version. Lock-free and allocation-free like <see cref="TableProbe{TKey,TRow}"/>,
/// whose version it shares.
/// </summary>
internal sealed class TableIndexProbe<TIndexKey, TKey, TRow>(
    SharedTable<TKey, TRow> table, TableIndex<TIndexKey, TKey, TRow> index, TableProbe<TKey, TRow> pin)
    : TableGroupProbe<TIndexKey, TRow>
    where TIndexKey : notnull
    where TKey : notnull
    where TRow : class
{
    /// <summary>How many rows are under <paramref name="indexKey"/> as of the step's version.</summary>
    public int Count(TIndexKey indexKey)
    {
        var version = pin.Version;
        var count = index.Read(indexKey, version)?.Length ?? 0;
        table.ThrowIfRevoked(version);
        return count;
    }

    public override void GetGroupsInto(List<TIndexKey> keys, List<TRow> rows, List<int> bounds)
    {
        var version = pin.Version;
        foreach (var indexKey in keys)
        {
            // Every key in the group has a row at this version: the writer links a
            // version's rows and its index entries before it publishes either.
            if (index.Read(indexKey, version) is { } group)
                foreach (var key in group)
                    if (table.TryGet(key, version, out var row))
                        rows.Add(row);
            bounds.Add(rows.Count);
        }

        // A group read is a read like any other: checked after it, as TryGet checks its own.
        table.ThrowIfRevoked(version);
    }
}

/// <summary>
/// One row entering (weight +1) or leaving (−1) the group of rows under an index key: the
/// table's delta as an index join consumes it. Rows are compared by identity. They are
/// immutable and shared, a step's delta never carries two different instances of one row
/// under one key, and comparing or hashing whole rows for every change would cost far more
/// than it could ever save.
/// </summary>
internal readonly struct IndexedRow<TKey, TRow>(TKey key, TRow row) : IEquatable<IndexedRow<TKey, TRow>>
    where TKey : notnull
    where TRow : class
{
    public TKey Key { get; } = key;
    public TRow Row { get; } = row;

    public bool Equals(IndexedRow<TKey, TRow> other) =>
        ReferenceEquals(Row, other.Row) && EqualityComparer<TKey>.Default.Equals(Key, other.Key);

    public override bool Equals(object? obj) => obj is IndexedRow<TKey, TRow> other && Equals(other);

    public override int GetHashCode() =>
        HashCode.Combine(EqualityComparer<TKey>.Default.GetHashCode(Key), RuntimeHelpers.GetHashCode(Row));
}

/// <summary>
/// Folds a run of consecutive versions into one change per key: the <c>Old</c> of the key's
/// first change and the <c>New</c> of its last. A key that ends where it started — inserted
/// and deleted again, or changed and changed back — yields nothing. This needs only the
/// versions themselves, never the table as of the starting version, which is why the table
/// retains no rows for circuits that are between steps or stopped.
/// </summary>
internal sealed class TableChangeCoalescer<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    private readonly Dictionary<TKey, int> _index = [];
    private readonly List<TableChange<TKey, TRow>> _changes = [];

    public bool IsEmpty => _changes.Count == 0;

    /// <summary>What has been folded in so far: one entry per key, a key that ended where
    /// it started included.</summary>
    public IReadOnlyList<TableChange<TKey, TRow>> Changes => _changes;

    /// <summary>Folds in the next version. Versions must be added oldest first.</summary>
    public void Add(TableVersion<TKey, TRow> version)
    {
        var changes = version.Changes;
        for (var i = 0; i < changes.Count; i++)
        {
            var change = changes[i];
            if (_index.TryGetValue(change.Key, out var at))
                _changes[at] = _changes[at] with { New = change.New };
            else
            {
                _index[change.Key] = _changes.Count;
                _changes.Add(change);
            }
        }
    }

    /// <summary>Forgets everything folded in so far.</summary>
    public void Clear()
    {
        _index.Clear();
        _changes.Clear();
    }

    /// <summary>Adds the net changes to <paramref name="staging"/>, round-robin, each with
    /// weight +1, and empties the coalescer. Returns how many there were.</summary>
    public int DrainInto(ZSet<TableChange<TKey, TRow>>[] staging)
    {
        var staged = 0;
        foreach (var change in _changes)
        {
            if (change.Old is null ? change.New is null
                : change.New is not null && EqualityComparer<TRow>.Default.Equals(change.Old, change.New))
                continue;
            staging[staged++ % staging.Length].Add(change);
        }
        Clear();
        return staged;
    }
}

internal sealed class TableRegistration<TKey, TRow> : InputRegistration
    where TKey : notnull
    where TRow : class
{
    public required TableRef<TKey, TRow> Ref { get; init; }

    public override IInputPort CreatePort(CircuitOptions options) => new TablePort<TKey, TRow>(Name, NodeId, Ref);
}

/// <summary>
/// A circuit's connection to one shared table. To the circuit a table is an input whose
/// records are <see cref="TableChange{TKey,TRow}"/>s and whose replay position is a version,
/// so this is an input port: its position commits in the offsets marker (under
/// <see cref="OffsetKeys.Table"/>, which is also the port's name) and is seeded from it on
/// restart, and a newly published version wakes the pump like a newly written record. What
/// a table adds is the pin — see <see cref="ITablePort"/>.
/// </summary>
internal sealed class TablePort<TKey, TRow> : ITablePort
    where TKey : notnull
    where TRow : class
{
    private readonly SharedTable<TKey, TRow> _table;
    private readonly TableProbe<TKey, TRow> _probe;
    private readonly int _nodeId;
    private readonly TaskCompletionSource _completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

    // Per-step scratch, cleared as each step's changes are staged.
    private readonly List<TableVersion<TKey, TRow>> _versions = [];
    private readonly TableChangeCoalescer<TKey, TRow> _coalescer = new();
    private ZSet<TableChange<TKey, TRow>>[]? _staging;

    private SharedTable<TKey, TRow>.TailReader? _reader;
    private long? _resumeFrom;
    private long _pinned = -1;   // the step's pinned version; -1 when no pin is held
    private long _consumed = -1; // changes up to this version have been staged; -1 before Attach

    // Changes() in a circuit with no committed position for this table: the first step
    // stages the table's rows instead of its changes.
    // Set on the starting thread and cleared by the first step; read by the pump.
    private readonly int? _rowsNodeId;
    private volatile bool _bootstrap;

    public TablePort(string name, int nodeId, TableRef<TKey, TRow> table)
    {
        Name = name;
        _nodeId = nodeId;
        _table = table.Table;
        _probe = table.Probe;
        _rowsNodeId = table.RowsNodeId;
    }

    public string Name { get; }
    public string TableName => _table.Name;
    public string Epoch => _table.Epoch;
    public object Handle => _table;

    public bool IsCompleted => _completed.Task.IsCompleted;

    /// <summary>A version the circuit has not consumed is pending work, as a buffered record
    /// is. Not once the input side is completed, though: shutdown drains what is buffered,
    /// and a table never runs dry. The rows a new circuit is owed are different. They are
    /// pending from the moment it starts, whether or not the table ever changes again, and
    /// they are one step's work, so shutdown drains them like anything else buffered.</summary>
    public bool HasPending => _bootstrap || (!IsCompleted && _table.LatestVersion > Volatile.Read(ref _consumed));

    /// <summary>Versions published but not yet consumed.</summary>
    public int BufferedCount =>
        (int)Math.Clamp(_table.LatestVersion - Volatile.Read(ref _consumed), 0, int.MaxValue);

    public async Task<bool> WaitToReadAsync(CancellationToken cancellationToken)
    {
        if (_bootstrap)
            return true;
        var published = _table.WaitForVersionAfterAsync(Volatile.Read(ref _consumed));
        await Task.WhenAny(published, _completed.Task).WaitAsync(cancellationToken).ConfigureAwait(false);
        return !IsCompleted;
    }

    public void SeedUnstamped(long offset) => _resumeFrom = offset;

    public void SeedPartition(int partition, long offset) =>
        throw new InvalidOperationException($"Table position '{Name}' has no partitions.");

    public void AppendConsumedOffsets(Dictionary<string, long> into)
    {
        if (_consumed >= 0)
            into[Name] = _consumed;
    }

    public void Attach()
    {
        _reader = _table.Attach(_resumeFrom);
        _bootstrap = _rowsNodeId is not null && _resumeFrom is null;
        Volatile.Write(ref _consumed, _reader.Position);
    }

    public ValueTask BeginStepAsync(CancellationToken cancellationToken)
    {
        _pinned = _table.BeginStep(_reader!, _versions, out var logFrom, out var logTo);
        _probe.Version = _pinned;
        return logTo > logFrom ? new ValueTask(ReadLogAsync(logFrom, logTo, cancellationToken)) : default;
    }

    /// <summary>The circuit is further behind than the tail reaches: the older versions come
    /// from the log, and are folded in ahead of the tail's.</summary>
    private async Task ReadLogAsync(long fromExclusive, long toInclusive, CancellationToken cancellationToken)
    {
        await foreach (var version in _table.ReadLogAsync(fromExclusive, toInclusive, cancellationToken).ConfigureAwait(false))
            _coalescer.Add(version);
    }

    /// <summary>Stages the net changes from the previous step's version to the pinned one on
    /// the workers, round-robin: a join's exchange takes each to the worker that owns its
    /// key. All of them, whatever <paramref name="maxItems"/> says — the circuit can only
    /// probe at the pinned version, so it cannot stop part-way there.</summary>
    public int DrainInto(Worker[] workers, int maxItems)
    {
        if (_bootstrap)
            return Bootstrap(workers);

        if (_versions.Count == 0 && _coalescer.IsEmpty)
        {
            Volatile.Write(ref _consumed, _pinned);
            return 0;
        }

        var staging = _staging is { } cached && cached.Length == workers.Length
            ? cached
            : _staging = new ZSet<TableChange<TKey, TRow>>[workers.Length];
        for (var i = 0; i < workers.Length; i++)
            staging[i] = ((InputOperator<TableChange<TKey, TRow>>)workers[i].Operators[_nodeId]).Pending;

        int staged;
        if (_versions.Count == 1 && _coalescer.IsEmpty)
        {
            // The usual step: one version behind, whose changes are already one per key.
            var changes = _versions[0].Changes;
            for (staged = 0; staged < changes.Count; staged++)
                staging[staged % staging.Length].Add(changes[staged]);
        }
        else
        {
            foreach (var version in _versions)
                _coalescer.Add(version);
            staged = _coalescer.DrainInto(staging);
        }
        _versions.Clear();
        Volatile.Write(ref _consumed, _pinned);
        return staged;
    }

    /// <summary>The first step of a circuit that reads the table as a stream and has no
    /// committed position in it: every row at the pinned version, as insertions. The rows
    /// stand in for the whole history up to that version, so the changes gathered since the
    /// circuit attached are dropped rather than applied on top — and nothing else in the
    /// circuit misses them, because a join that has no checkpoint has no facts to correct.</summary>
    private int Bootstrap(Worker[] workers)
    {
        _bootstrap = false;
        _versions.Clear();
        _coalescer.Clear();

        var staged = 0;
        foreach (var (_, row) in _table.RowsAt(_pinned))
        {
            var rows = (TableRowsOperator<TKey, TRow>)workers[staged++ % workers.Length].Operators[_rowsNodeId!.Value];
            (rows.Bootstrap ??= []).Add(row);
        }
        Volatile.Write(ref _consumed, _pinned);
        return staged;
    }

    public void ReleasePin()
    {
        if (_pinned < 0)
            return;
        _table.ReleasePin(_pinned);
        _pinned = -1;
    }

    public void Detach()
    {
        _bootstrap = false; // nothing is owed to a circuit that has stopped reading
        ReleasePin();
        if (_reader is { } reader)
        {
            _table.Detach(reader);
            _reader = null;
        }
    }

    public void CompleteWriter() => _completed.TrySetResult();

    public void Fail(Exception error)
    {
        Detach();
        _completed.TrySetResult();
    }
}
