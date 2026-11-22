using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace Dbsp.Tables;

/// <summary>Sizing of a <see cref="SharedTable{TKey,TRow}"/>'s two retention windows.</summary>
public sealed class SharedTableOptions
{
    /// <summary>Recent versions kept in memory for circuits to read their changes from. A
    /// circuit that is keeping up never needs more than a few. When an attached circuit
    /// falls this far behind (or <see cref="TailChanges"/> behind, if that comes first) and
    /// the table has no log holding the older versions, the writer waits for it.</summary>
    public int TailVersions { get; init; } = 1024;

    /// <summary>Row changes kept in memory across those recent versions. Every change holds
    /// the row it replaced, so this — not <see cref="TailVersions"/> — is what bounds the
    /// superseded rows the tail keeps alive after a bulk load or a resync, when a version
    /// is thousands of rows. The oldest versions go first once the tail holds more, with
    /// the same wait for an attached circuit that has not read them; the newest version is
    /// always kept, whatever its size.</summary>
    public int TailChanges { get; init; } = 65_536;

    /// <summary>How many versions behind the latest a pinned version may fall before its pin
    /// is revoked: the bound on superseded rows one reader that never finishes can hold in
    /// memory. A revoked reader's next read throws.</summary>
    public int MaxRetainedVersions { get; init; } = 4096;

    /// <summary>Rows per version while the writer's <c>ReplaceAllAsync</c> loads a snapshot. A version is atomic and the writer's scratch keeps the capacity of
    /// the largest one it has built, so a snapshot goes in as many modest versions rather
    /// than one the size of the table.</summary>
    public int ReplaceChunkRows { get; init; } = 4096;

    /// <summary>How the writer paces a large load so that the collector never stops the
    /// process for long (<see cref="TableLoadPacing"/>); null leaves the collector to
    /// itself.</summary>
    public TableLoadPacing? LoadPacing { get; init; } = TableLoadPacing.Default;
}

/// <summary>
/// A reference dataset held once per process and read by any number of circuits: the current
/// row per key, readable <em>as of</em> a version. See <c>docs/design/shared-tables.md</c>.
/// <para>
/// <b>Rows must be immutable.</b> A row object is handed to every reader in the process, on
/// any thread; a row mutated after it is written corrupts every circuit that joined it.
/// Use records with init-only members.
/// </para>
/// <para>
/// This is the library's one deliberate exception to "state is never shared across
/// threads", and the rules that make it safe are narrow. There is one writer
/// (<see cref="Writer"/>), which applies changes in batches; each batch that changes
/// anything becomes the next <em>version</em>. Readers never write and never lock per read:
/// a reader pins a version (<see cref="Snapshot"/>) and every read through that pin sees
/// exactly that version, whatever the writer publishes meanwhile. Each key maps to a short
/// chain of row versions, newest first; the writer trims a chain back to the newest entry
/// any pinned reader can still need, so in the steady state a chain is one entry long.
/// </para>
/// <para>
/// Beside the rows the table keeps a bounded <em>tail</em> of its most recent versions'
/// changes. A circuit attached with <c>builder.Table(...)</c> reads from it, each step, the
/// net changes since the version it saw last.
/// </para>
/// <para>
/// Rows are found by their key, and by any <em>secondary index</em> declared with
/// <see cref="AddIndex"/>: a second map, from some other column to the rows under it,
/// kept by the writer in the same version as the rows.
/// </para>
/// </summary>
public sealed class SharedTable<TKey, TRow> : ITableNode, IAsyncDisposable
    where TKey : notnull
    where TRow : class
{
    private static readonly IReadOnlyDictionary<int, long> NoOffsets = new Dictionary<int, long>();

    private readonly VersionChains<TKey, TRow> _rows = new();

    // What a row costs on the heap, from rows sized as they are written or restored.
    private readonly Diagnostics.SizeSampler _rowSizes = new();

    // Secondary indexes. The list is the writer's, touched only while it holds the commit
    // gate; the name lookup is what a circuit under construction reads, on any thread.
    private readonly List<TableIndex<TKey, TRow>> _indexes = [];
    private readonly ConcurrentDictionary<string, object> _indexesByName = new(StringComparer.Ordinal);
    private readonly SharedTableOptions _options;
    private readonly ITableLog<TKey, TRow>? _log;

    // Guards the pin registry, the tail and the attached readers, and makes "read the latest
    // version and register a pin" atomic against the writer's "publish and compute the trim
    // floor": either the writer sees the pin, or the pin sees the newer version. Taken once
    // per pin (so once per circuit step) and once per version written — never per read.
    private readonly object _lock = new();
    private readonly Dictionary<long, int> _pins = [];

    private long _latest;
    private long _count;
    private IReadOnlyDictionary<int, long> _committedOffsets = NoOffsets;

    // Versions below this have had their pins revoked (MaxRetainedVersions) and may already
    // be trimmed. Written by the writer before it trims; read by every probe after it reads.
    private long _revokedBelow;

    /// <param name="name">The table's durable name: circuits commit the version they have
    /// consumed under it, so it must stay stable across restarts.</param>
    /// <param name="keySelector">The row's primary key. A table holds one row per key.</param>
    /// <param name="options">Retention sizing; the defaults suit most tables.</param>
    public SharedTable(string name, Func<TRow, TKey> keySelector, SharedTableOptions? options = null)
        : this(name, keySelector, null, options)
    {
    }

    private SharedTable(string name, Func<TRow, TKey> keySelector, ITableLog<TKey, TRow>? log, SharedTableOptions? options)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(keySelector);
        if (name.Contains('#'))
            throw new ArgumentException(
                $"Table name '{name}' contains '#', which is reserved for checkpoint offset keys.", nameof(name));
        _options = options ?? new SharedTableOptions();
        ArgumentOutOfRangeException.ThrowIfLessThan(_options.TailVersions, 1, nameof(options));
        ArgumentOutOfRangeException.ThrowIfLessThan(_options.TailChanges, 1, nameof(options));
        ArgumentOutOfRangeException.ThrowIfLessThan(_options.MaxRetainedVersions, 1, nameof(options));
        ArgumentOutOfRangeException.ThrowIfLessThan(_options.ReplaceChunkRows, 1, nameof(options));
        Name = name;
        KeySelector = keySelector;
        _log = log;
        Epoch = log?.Epoch ?? Guid.NewGuid().ToString("N");
        _tail = new TableVersion<TKey, TRow>[Math.Min(_options.TailVersions, 16)];
        _tailSuperseded = new int[_tail.Length];
        Writer = new TableWriter<TKey, TRow>(this, log, _options.LoadPacing);
    }

    /// <summary>Opens a table over a change log: restores the log's current rows, version and
    /// source offsets, then appends every new version to it before readers can see it. An
    /// empty log yields an empty table. The log must have no other writer.</summary>
    public static async Task<SharedTable<TKey, TRow>> OpenAsync(
        string name,
        Func<TRow, TKey> keySelector,
        ITableLog<TKey, TRow> log,
        SharedTableOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(log);
        var table = new SharedTable<TKey, TRow>(name, keySelector, log, options);

        // Restored rows carry version 0: they are the table as of the head, and every pin is
        // at or above the head, so they are visible to all of them.
        var head = await log.RestoreAsync(
            (key, row) =>
            {
                table._rows.Seed(table.KeyToHold(key, row), 0, row);
                table._rowSizes.Offer(row);
            }, cancellationToken).ConfigureAwait(false);
        table._count = table._rows.KeyCount;
        table._committedOffsets = head.Offsets;
        table.Writer.SeedPositions(head.Offsets);
        lock (table._lock)
        {
            table._latest = head.Version;
            table._tailEnd = head.Version;
        }
        return table;
    }

    // ── following a log: the standby's table ───────────────────────────────────────────

    private CancellationTokenSource? _followStop;
    private Task _follower = Task.CompletedTask;
    private volatile Exception? _followFault;

    /// <summary>
    /// Opens a table that follows a change log another process is writing: the standby's
    /// copy. It restores the log's current rows, then applies every version the log gains
    /// to its own view, so readers here see what the active's readers see, a little later.
    /// Circuits may be built against it. Its <see cref="Writer"/> refuses every call until
    /// <see cref="PromoteAsync"/> makes this table the log's writer. Dispose a follower
    /// that is dropped without being promoted.
    /// </summary>
    public static async Task<SharedTable<TKey, TRow>> FollowAsync(
        string name,
        Func<TRow, TKey> keySelector,
        ITableLog<TKey, TRow> log,
        SharedTableOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var table = await OpenAsync(name, keySelector, log, options, cancellationToken).ConfigureAwait(false);
        table.Writer.Following = true;
        var stop = table._followStop = new CancellationTokenSource();
        table._follower = Task.Run(() => table.FollowLogAsync(stop.Token));
        return table;
    }

    /// <summary>Whether this table is following its log rather than writing it.</summary>
    public bool IsFollowing => Writer.Following;

    /// <summary>Why this table stopped following its log, or null while it follows (or
    /// writes). A follower that faults — its next version was trimmed from the log, say —
    /// keeps serving the last version it applied and can never be promoted: dispose it and
    /// follow the log again.</summary>
    public Exception? FollowFault => _followFault;

    private async Task FollowLogAsync(CancellationToken stop)
    {
        try
        {
            await foreach (var version in _log!.FollowAsync(LatestVersion, stop).ConfigureAwait(false))
                await Writer.ApplyFollowedAsync(version).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            _followFault = ex;
        }
    }

    /// <summary>
    /// Turns a following table into the log's writer — the table's part of a takeover.
    /// Call it once the previous writer can append nothing more (it has been fenced):
    /// this reads the log to its end, stops following, and opens <see cref="Writer"/>,
    /// which continues from the last version and the source offsets recorded with it.
    /// Promote tables before starting the circuits that read or write them.
    /// </summary>
    public async Task PromoteAsync(CancellationToken cancellationToken = default)
    {
        if (!Writer.Following)
            throw new InvalidOperationException($"Table '{Name}' is not following its log; it is already the writer.");

        var end = await _log!.LastVersionAsync(cancellationToken).ConfigureAwait(false);

        // Waits on "a version at or past the end", not on "a version after the one seen a
        // moment ago": the follower may publish the last version between that look and the
        // wait, and then nothing further would ever arrive to end it.
        while (LatestVersion < end && !_follower.IsCompleted)
            await Task.WhenAny(WaitForVersionAfterAsync(end - 1), _follower).WaitAsync(cancellationToken).ConfigureAwait(false);

        _followStop!.Cancel();
        await _follower.ConfigureAwait(false);
        if (_followFault is { } fault)
            throw new InvalidOperationException(
                $"Table '{Name}' stopped following its log at version {LatestVersion} (the log ends at {end}): {fault.Message} " +
                "It cannot be promoted; open the table from the log again.", fault);
        Writer.Following = false;
    }

    /// <summary>Stops following, if this table is a follower. A table that writes holds
    /// nothing to release.</summary>
    public async ValueTask DisposeAsync()
    {
        _followStop?.Cancel();
        await _follower.ConfigureAwait(false);
    }

    public string Name { get; }

    /// <summary>Identifies this table's history. Version numbers mean something only within
    /// one epoch: a circuit commits the epoch beside the version it has consumed and refuses
    /// to resume against a table with a different one. A table opened over a log takes the
    /// log's epoch; one without a log mints its own, so it is new every time it is created.</summary>
    public string Epoch { get; }

    internal Func<TRow, TKey> KeySelector { get; }

    /// <summary>The table's single writer. While a circuit writes the table
    /// (<c>stream.ToTable(table)</c>) the circuit is that writer, and every call here throws.</summary>
    public TableWriter<TKey, TRow> Writer { get; }

    /// <summary>The circuit currently writing this table through <c>ToTable</c>, or null.
    /// Guarded by <see cref="TableLineage"/>'s lock.</summary>
    TableWriterClaim? ITableNode.WriterClaim
    {
        get => Writer.Owner;
        set => Writer.Owner = value;
    }

    /// <summary>The newest published version; 0 for a table nothing has been written to.</summary>
    public long LatestVersion => Volatile.Read(ref _latest);

    /// <summary>Rows present as of <see cref="LatestVersion"/>.</summary>
    public long Count => Volatile.Read(ref _count);

    /// <summary>Source partition → offset of the last stamped record whose effect is in a
    /// published (and, with a log, durable) version. After a restart the source adapter
    /// resumes each partition just above its entry; records re-sent at or below it are
    /// dropped. Offsets of records that changed nothing are carried into the next version
    /// that does change something, so this can trail the last record the writer accepted.</summary>
    public IReadOnlyDictionary<int, long> CommittedOffsets => Volatile.Read(ref _committedOffsets);

    /// <summary>Pins the latest version for reading. Every read through the returned
    /// snapshot sees that version until it is disposed; dispose it promptly, because the
    /// table keeps superseded rows for as long as a snapshot that could read them is open.
    /// A snapshot held until it is more than <see cref="SharedTableOptions.MaxRetainedVersions"/>
    /// versions old is revoked: its next read throws.</summary>
    public TableSnapshot<TKey, TRow> Snapshot() => new(this, AcquirePin());

    /// <summary>The row for <paramref name="key"/> as of <paramref name="version"/>, which
    /// must be a version the caller currently has pinned — reading an unpinned version races
    /// with trimming and may miss rows. Lock-free and allocation-free.</summary>
    internal bool TryGet(TKey key, long version, [MaybeNullWhen(false)] out TRow row)
    {
        row = _rows.Read(key, version);
        ThrowIfRevoked(version);
        return row is not null;
    }

    /// <summary>Faults a reader whose pin has been revoked. Called after the read it guards,
    /// not before: the writer raises the cutoff before it trims, so a read that trimming
    /// could have disturbed always observes it.</summary>
    internal void ThrowIfRevoked(long version)
    {
        if (version < Volatile.Read(ref _revokedBelow))
            throw new InvalidOperationException(
                $"The pin on version {version} of table '{Name}' was revoked: it was held while the table moved more than " +
                $"{_options.MaxRetainedVersions} versions ahead (SharedTableOptions.MaxRetainedVersions), and the rows it " +
                "could read have been reclaimed.");
    }

    /// <summary>Every row as of <paramref name="version"/>, which the caller must have
    /// pinned. A scan of the whole table: for bootstrapping a reader, never per step.
    /// <para>
    /// A scan takes long enough for the writer to revoke its pin part-way, and once the
    /// chains are trimmed past the pin a key changed since reads as absent. So the scan is
    /// checked like any other read, after each row and once more after the last: it throws
    /// rather than end short.
    /// </para>
    /// </summary>
    internal IEnumerable<KeyValuePair<TKey, TRow>> RowsAt(long version)
    {
        foreach (var row in _rows.At(version))
        {
            ThrowIfRevoked(version);
            yield return row;
        }

        // The keys skipped since the last row that was found are read too.
        ThrowIfRevoked(version);
    }

    internal IReadOnlyList<KeyValuePair<TKey, TRow>> RowsBy<TIndexKey>(string index, TIndexKey indexKey, long version)
        where TIndexKey : notnull
    {
        ArgumentException.ThrowIfNullOrEmpty(index);
        if (!_indexesByName.TryGetValue(index, out var declared))
            throw new ArgumentException($"Table '{Name}' has no index named '{index}'.", nameof(index));
        if (declared is not TableIndex<TIndexKey, TKey, TRow> typed)
            throw new ArgumentException(
                $"Index '{index}' of table '{Name}' is keyed by {declared.GetType().GetGenericArguments()[0]}, not {typeof(TIndexKey)}.",
                nameof(indexKey));
        if (typed.Read(indexKey, version) is not { } keys)
            return [];
        var rows = new List<KeyValuePair<TKey, TRow>>(keys.Length);
        foreach (var key in keys)
            if (TryGet(key, version, out var row))
                rows.Add(new KeyValuePair<TKey, TRow>(key, row));
        return rows;
    }

    /// <summary>A window of <see cref="RowsBy{TIndexKey}(string,TIndexKey,long)"/>: the
    /// keys under the index key are passed over up to <paramref name="skip"/> and at most
    /// <paramref name="take"/> rows are read, so a page of a key that holds a great many
    /// rows costs a page. <paramref name="total"/> is how many keys the index holds under
    /// the key.</summary>
    internal IReadOnlyList<KeyValuePair<TKey, TRow>> RowsBy<TIndexKey>(string index, TIndexKey indexKey, long version, int skip, int take, out int total)
        where TIndexKey : notnull
    {
        ArgumentException.ThrowIfNullOrEmpty(index);
        ArgumentOutOfRangeException.ThrowIfNegative(skip);
        ArgumentOutOfRangeException.ThrowIfNegative(take);
        if (!_indexesByName.TryGetValue(index, out var declared))
            throw new ArgumentException($"Table '{Name}' has no index named '{index}'.", nameof(index));
        if (declared is not TableIndex<TIndexKey, TKey, TRow> typed)
            throw new ArgumentException(
                $"Index '{index}' of table '{Name}' is keyed by {declared.GetType().GetGenericArguments()[0]}, not {typeof(TIndexKey)}.",
                nameof(indexKey));
        total = 0;
        if (typed.Read(indexKey, version) is not { } keys)
            return [];
        total = keys.Length;
        var rows = new List<KeyValuePair<TKey, TRow>>(Math.Min(take, Math.Max(0, keys.Length - skip)));
        for (var i = skip; i < keys.Length && rows.Count < take; i++)
            if (TryGet(keys[i], version, out var row))
                rows.Add(new KeyValuePair<TKey, TRow>(keys[i], row));
        return rows;
    }

    internal int ReplaceChunkRows => _options.ReplaceChunkRows;

    // ── secondary indexes ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Declares a secondary index: rows become findable by <paramref name="selector"/>'s
    /// value as well as by their key, which is what joining on a column other than the key
    /// needs (<c>builder.Table(table).By(name, selector)</c> declares one too). The index is
    /// identified by <paramref name="name"/>, because delegates cannot be compared:
    /// declaring a name again is a no-op and the first selector stands. Several rows may
    /// share an index value; a row whose selector returns null is not indexed.
    /// <para>
    /// The writer maintains the index in the same version as the rows from here on, at a
    /// cost per changed row for as long as the table lives. Declared on a table that
    /// already has rows, it is built by one scan between versions — this call waits for a
    /// commit in flight and holds the next one off while it scans — so declare indexes
    /// before loading where that is possible. The selector runs on the writer and must be
    /// pure; one that throws faults the writer.
    /// </para>
    /// </summary>
    public void AddIndex<TIndexKey>(string name, Func<TRow, TIndexKey?> selector)
        where TIndexKey : notnull =>
        Index(name, selector);

    internal TableIndex<TIndexKey, TKey, TRow> Index<TIndexKey>(string name, Func<TRow, TIndexKey?> selector)
        where TIndexKey : notnull
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(selector);
        if (!_indexesByName.TryGetValue(name, out var declared))
        {
            declared = Writer.Exclusive(() =>
            {
                if (_indexesByName.TryGetValue(name, out var raced))
                    return raced;

                // Between versions: no commit is in flight, so the latest version is the
                // whole table and nothing changes under the scan. No reader holds the index
                // yet, so stamping its entries with the latest version hides nothing.
                var index = new TableIndex<TIndexKey, TKey, TRow>(selector);
                index.Build(_rows.At(_latest), _latest);
                _indexes.Add(index);
                _indexesByName[name] = index;
                return index;
            });
        }
        return declared as TableIndex<TIndexKey, TKey, TRow>
            ?? throw new ArgumentException(
                $"Table '{Name}' already has an index named '{name}' with a different key type " +
                $"({declared.GetType().GetGenericArguments()[0]}, not {typeof(TIndexKey)}).", nameof(name));
    }

    /// <summary>Has every index work out what <paramref name="changes"/> do to it. Called
    /// by the writer before the version is appended; see <see cref="TableIndex{TKey,TRow}.Prepare"/>.</summary>
    internal void PrepareIndexes(IReadOnlyList<TableChange<TKey, TRow>> changes)
    {
        foreach (var index in _indexes)
            index.Prepare(changes);
    }

    /// <summary>Registers a pin on the latest version and returns it. Pair every call with
    /// one <see cref="ReleasePin"/>.</summary>
    internal long AcquirePin()
    {
        lock (_lock)
            return PinLatest();
    }

    private long PinLatest()
    {
        var version = _latest;
        _pins[version] = _pins.GetValueOrDefault(version) + 1;
        return version;
    }

    internal void ReleasePin(long version)
    {
        lock (_lock)
        {
            // A revoked pin is already out of the registry; releasing it is not an error.
            if (version < _revokedBelow)
                return;
            if (!_pins.TryGetValue(version, out var count))
                throw new InvalidOperationException($"Table '{Name}' has no pin on version {version} to release.");
            if (count == 1)
                _pins.Remove(version);
            else
                _pins[version] = count - 1;
        }
    }

    /// <summary>The oldest version any reader can still ask for: the oldest pin, or the
    /// latest version when nothing is pinned. Call under <see cref="_lock"/>.</summary>
    private long TrimFloor()
    {
        var floor = _latest;
        foreach (var version in _pins.Keys)
            if (version < floor)
                floor = version;
        return floor;
    }

    // ── the tail: recent versions' changes, for attached circuits ──────────────────────

    // A ring of consecutive versions ending at _tailEnd. It holds TailVersions of them or
    // TailChanges row changes between them, whichever is less, and more only for as long as
    // an attached reader still needs the oldest and no log has it.
    private TableVersion<TKey, TRow>[] _tail;
    private int _tailStart, _tailCount;
    private long _tailEnd;
    private long _tailChanges;

    // Beside each version in the ring, how many of its changes replaced or removed a row:
    // the superseded rows that version keeps alive, which nothing else does once the
    // chains are trimmed. The total is what the tail's history costs in rows.
    private int[] _tailSuperseded;
    private long _tailSupersededRows;

    private readonly List<TailReader> _readers = [];

    // What a waiting writer and waiting circuit pumps are parked on; null when nobody is.
    private TaskCompletionSource? _roomSignal;
    private TaskCompletionSource? _publishSignal;

    /// <summary>One attached circuit's place in the table's history. <see cref="Position"/>
    /// is the version it has consumed changes up to; the tail keeps everything after it.</summary>
    internal sealed class TailReader
    {
        public long Position;
    }

    private long TailOldest => _tailEnd - _tailCount + 1;

    /// <summary>Whether the tail is at either of its bounds. Call under <see cref="_lock"/>.</summary>
    private bool TailFull => _tailCount >= _options.TailVersions || _tailChanges >= _options.TailChanges;

    // The version a circuit writing this table (ToTable) last checkpointed at, or -1. After
    // a crash that circuit undoes what it wrote past its checkpoint, so the tail keeps those
    // versions — but the writer never waits on them: it would be waiting for its own
    // checkpoint, which waits for the write. The hold outlives the circuit, for the one
    // that replaces it, and ends when that one starts without resuming or when someone
    // writes the table directly instead (EndWriterHoldForDirectWrite).
    private long _writerHold = -1;

    /// <summary>Whether the tail may drop <paramref name="version"/>: every attached reader
    /// is past it and the writing circuit's checkpoint covers it, or the log still has it
    /// for whoever is not. <paramref name="forWriter"/> asks on behalf of a writer deciding
    /// whether to wait, which the writing circuit's own checkpoint never makes it do.</summary>
    private bool Evictable(long version, bool forWriter = false)
    {
        var consumed = forWriter || _writerHold < 0 || _writerHold >= version;
        foreach (var reader in _readers)
            consumed &= reader.Position >= version;
        return consumed || (_log is not null && _log.OldestRetainedVersion <= version);
    }

    /// <summary>Records that the writing circuit's checkpoint covers the table up to
    /// <paramref name="version"/> (-1: no circuit depends on the tail for recovery).</summary>
    internal void SetWriterHold(long version)
    {
        lock (_lock)
            _writerHold = version;
    }

    /// <summary>Ends the hold a writing circuit left behind, on behalf of a direct write
    /// through <see cref="Writer"/>. Nothing advances a hold whose circuit is gone, so from
    /// its version on the tail could evict nothing and would grow with every direct commit.
    /// A circuit that later resumes from that checkpoint finds the versions it needs still
    /// in the tail, or is refused at <c>Start()</c>. Not while a circuit holds the writer:
    /// the hold is then that circuit's, set as it started.</summary>
    internal void EndWriterHoldForDirectWrite()
    {
        if (Volatile.Read(ref _writerHold) < 0)
            return;
        lock (_lock)
        {
            if (Writer.Owner is null)
                _writerHold = -1;
        }
    }

    /// <summary>For a circuit that wrote this table and checkpointed at
    /// <paramref name="position"/>: checks that it can resume — the table must have reached
    /// that version and still hold the versions after it, which the circuit undoes before
    /// it replays — and holds those versions in the tail from here on.</summary>
    internal void HoldForWriterResuming(long position)
    {
        lock (_lock)
        {
            if (position > _latest)
                throw new InvalidOperationException(
                    $"This circuit's checkpoint has written table '{Name}' up to version {position}, but the table is only at " +
                    $"version {_latest}: the table has lost history the checkpoint depends on. Restore the table from its log " +
                    "before starting the circuit that writes it, or start that circuit against an empty state store.");
            if (!Readable(position))
                throw new InvalidOperationException(
                    $"This circuit's checkpoint has written table '{Name}' up to version {position} and the table is at version " +
                    $"{_latest}, but the versions in between are no longer retained, so the circuit cannot undo what it wrote " +
                    "past its checkpoint. Start it against an empty state store: it then rebuilds the table from its sources.");
            _writerHold = position;
        }
    }

    /// <summary>Collects the tail's versions after <paramref name="position"/> into
    /// <paramref name="into"/>, oldest first, without pinning. As with <see cref="BeginStep"/>,
    /// versions in <c>(logFromExclusive, logToInclusive]</c> come from the log, first. Only
    /// for the writer, between versions.</summary>
    internal void CollectSince(
        long position, List<TableVersion<TKey, TRow>> into, out long logFromExclusive, out long logToInclusive)
    {
        lock (_lock)
        {
            logFromExclusive = logToInclusive = position;
            var oldest = TailOldest;
            if (position + 1 < oldest)
                logToInclusive = oldest - 1;
            for (var v = Math.Max(position + 1, oldest); v <= _latest; v++)
                into.Add(_tail[(_tailStart + (int)(v - oldest)) % _tail.Length]);
        }
    }

    /// <summary>Whether the changes after <paramref name="position"/> can still be read, from
    /// the tail or the log.</summary>
    private bool Readable(long position) =>
        position + 1 >= TailOldest || (_log is not null && _log.OldestRetainedVersion <= position + 1);

    /// <summary>Attaches a circuit: from now on the tail keeps every version after the
    /// reader's position. A new circuit (<paramref name="resumeFrom"/> null) starts at the
    /// latest version; a restarted one starts at the version it committed, which must still
    /// be readable.</summary>
    internal TailReader Attach(long? resumeFrom)
    {
        lock (_lock)
        {
            var position = resumeFrom ?? _latest;
            if (position > _latest)
                throw new InvalidOperationException(
                    $"This circuit's checkpoint has consumed table '{Name}' up to version {position}, but the table is only at " +
                    $"version {_latest}: the table has lost history the checkpoint depends on. Restore the table from its log " +
                    "before starting circuits, or start this circuit against an empty state store.");
            if (!Readable(position))
                throw new InvalidOperationException(
                    $"This circuit's checkpoint has consumed table '{Name}' up to version {position}, but the changes after it are " +
                    $"no longer retained (the table is at version {_latest}): the circuit was stopped for longer than the table's " +
                    "history reaches. It cannot catch up; start it against an empty state store.");
            var reader = new TailReader { Position = position };
            _readers.Add(reader);
            return reader;
        }
    }

    internal void Detach(TailReader reader)
    {
        TaskCompletionSource? room;
        lock (_lock)
        {
            _readers.Remove(reader);
            (room, _roomSignal) = (_roomSignal, null);
        }
        room?.TrySetResult();
    }

    /// <summary>Starts a circuit step: pins the latest version and collects the tail's
    /// versions after the reader's position into <paramref name="into"/>, oldest first. If
    /// the tail no longer reaches back to the reader, the versions in
    /// <c>(logFromExclusive, logToInclusive]</c> must be read from the log and come before
    /// the collected ones; otherwise that range is empty. The reader's position moves to the
    /// pinned version either way — the caller now holds everything it needs.</summary>
    internal long BeginStep(
        TailReader reader, List<TableVersion<TKey, TRow>> into, out long logFromExclusive, out long logToInclusive)
    {
        TaskCompletionSource? room = null;
        long version;
        lock (_lock)
        {
            version = PinLatest();
            logFromExclusive = logToInclusive = reader.Position;
            if (version > reader.Position)
            {
                var oldest = TailOldest;
                if (reader.Position + 1 < oldest)
                    logToInclusive = oldest - 1;
                for (var v = Math.Max(reader.Position + 1, oldest); v <= version; v++)
                    into.Add(_tail[(_tailStart + (int)(v - oldest)) % _tail.Length]);
                reader.Position = version;
                (room, _roomSignal) = (_roomSignal, null);
            }
        }
        room?.TrySetResult();
        return version;
    }

    /// <summary>Reads versions the tail has dropped from the log. Only called for a range
    /// <see cref="BeginStep"/> reported, which <see cref="Evictable"/> allowed to leave the
    /// tail because the log had it.</summary>
    internal IAsyncEnumerable<TableVersion<TKey, TRow>> ReadLogAsync(
        long fromExclusive, long toInclusive, CancellationToken cancellationToken) =>
        _log!.ReadAsync(fromExclusive, toInclusive, cancellationToken);

    /// <summary>A task that completes once a version newer than <paramref name="version"/>
    /// has been published.</summary>
    internal Task WaitForVersionAfterAsync(long version)
    {
        lock (_lock)
        {
            return _latest > version
                ? Task.CompletedTask
                : (_publishSignal ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
        }
    }

    /// <summary>The instance to hold a row from a log under. A log reads the key and the
    /// row apart, so a key that is an object arrives as a second one beside the row's own
    /// — for a table keyed by a string, a string more per row that nothing but the map
    /// refers to. Where the selector gives an equal key off the row, that one is held and
    /// the log's is left to the collector: what a table restored or followed holds is then
    /// what the writer that built it holds, and what <see cref="EstimateMemory"/> counts.</summary>
    private TKey KeyToHold(TKey key, TRow row)
    {
        if (typeof(TKey).IsValueType)
            return key;
        var own = KeySelector(row);
        return EqualityComparer<TKey>.Default.Equals(own, key) ? own : key;
    }

    // ── writer side: called only by TableWriter, one commit at a time ──────────────────

    /// <summary>The current row for <paramref name="key"/>, as the writer sees it.</summary>
    internal TRow? Current(TKey key) => _rows.Current(key);

    /// <summary>The keys that currently have a row, as the writer sees them: the scan a
    /// snapshot sync deletes from.</summary>
    internal IEnumerable<KeyValuePair<TKey, TRow>> CurrentRows() => _rows.At(_latest);

    /// <summary>Completes when the tail can take one more version. This is where the writer
    /// waits for an attached circuit that has fallen a full tail behind — in versions or in
    /// row changes — when no log holds the versions it still needs: bounded memory, paid
    /// for with a stalled writer.</summary>
    internal async ValueTask WaitForTailRoomAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            Task room;
            lock (_lock)
            {
                if (!TailFull || Evictable(TailOldest, forWriter: true))
                    return;
                room = (_roomSignal ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
            }
            await room.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Makes <paramref name="version"/> — already appended to the log, if there is
    /// one — the latest: links its rows, publishes it, adds it to the tail, and trims.</summary>
    internal void Publish(TableVersion<TKey, TRow> version, long countDelta)
    {
        // Linked in before the version is published: a reader pinned below it steps over
        // these entries by their version number, so a half-applied batch is never visible.
        // The indexes go in the same way, so a reader never sees rows of one version
        // through an index of another.
        var superseded = 0;
        var followed = !typeof(TKey).IsValueType && Writer.Following;
        foreach (var change in version.Changes)
        {
            _rows.Write(followed && change.New is { } held ? KeyToHold(change.Key, held) : change.Key, version.Version, change.New);
            if (change.New is { } row)
                _rowSizes.Offer(row);
            if (change.Old is not null)
                superseded++;
        }
        foreach (var index in _indexes)
            index.Apply(version.Version);
        Volatile.Write(ref _count, _count + countDelta);
        Volatile.Write(ref _committedOffsets, version.Offsets);

        long floor;
        TaskCompletionSource? published;
        lock (_lock)
        {
            // Publication point, under the lock so a pin never observes the new version
            // half-registered against a trim floor computed for the old one.
            _latest = version.Version;

            if (_tailCount == _tail.Length)
                GrowTail();
            _tailSuperseded[(_tailStart + _tailCount) % _tail.Length] = superseded;
            _tail[(_tailStart + _tailCount++) % _tail.Length] = version;
            _tailEnd = version.Version;
            _tailChanges += version.Changes.Count;
            _tailSupersededRows += superseded;
            // Normally what leaves makes room for what arrived. More than the bounds stay
            // only when a reader attached, needing the oldest, after the writer checked for
            // room — and the version just published stays whatever its size: evicting it
            // would send every circuit to the log for the step it is about to take.
            while (_tailCount > 1
                && (_tailCount > _options.TailVersions || _tailChanges > _options.TailChanges)
                && Evictable(TailOldest))
            {
                _tailChanges -= _tail[_tailStart].Changes.Count;
                _tailSupersededRows -= _tailSuperseded[_tailStart];
                _tail[_tailStart] = null!;
                _tailStart = (_tailStart + 1) % _tail.Length;
                _tailCount--;
            }

            // Pins that have fallen too far behind are dropped from the registry, which is
            // what lets the floor — and so the trim below — move past them.
            var cutoff = version.Version - _options.MaxRetainedVersions;
            if (cutoff > 0)
            {
                Volatile.Write(ref _revokedBelow, cutoff);
                foreach (var pinned in _pins.Keys)
                    if (pinned < cutoff)
                        _pins.Remove(pinned);
            }

            floor = TrimFloor();
            (published, _publishSignal) = (_publishSignal, null);
        }
        published?.TrySetResult();
        Trim(floor);
    }

    private void GrowTail()
    {
        var grown = new TableVersion<TKey, TRow>[_tail.Length * 2];
        var grownSuperseded = new int[grown.Length];
        for (var i = 0; i < _tailCount; i++)
        {
            grown[i] = _tail[(_tailStart + i) % _tail.Length];
            grownSuperseded[i] = _tailSuperseded[(_tailStart + i) % _tail.Length];
        }
        (_tail, _tailSuperseded, _tailStart) = (grown, grownSuperseded, 0);
    }

    /// <summary>Cuts every changed key's chain back to the newest entry a pinned reader can
    /// still need, and removes keys whose surviving entry is a deletion.</summary>
    internal void Trim()
    {
        long floor;
        lock (_lock)
            floor = TrimFloor();
        Trim(floor);
    }

    private void Trim(long floor)
    {
        _rows.Trim(floor);
        foreach (var index in _indexes)
            index.Trim(floor);
    }

    // ── what it costs ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// An estimate of what this table holds on the managed heap of this process, in three
    /// parts. Cheap enough to ask every few seconds: counters and one short lock, never a
    /// walk of the rows.
    /// <para>
    /// How it is made. A row's cost is the mean over rows sized as they were written or
    /// restored — each of the first 256, then one in 512 — where sizing a row means
    /// reading its fields: the object, its strings and whatever else only it refers to,
    /// with headers and rounding as the runtime allocates them
    /// (<see cref="TableMemoryEstimate.SampledRows"/> says how many). To that is added what
    /// the key map costs per row, which follows from the map's layout. An index costs its
    /// own map per group and one primary key per indexed row. History is the recent
    /// versions kept for attached circuits (<see cref="SharedTableOptions.TailChanges"/>):
    /// the rows those versions replaced or removed, which nothing else keeps alive, at the
    /// same mean, and the change lists themselves.
    /// </para>
    /// <para>
    /// What it does not see: a row that shares an object with other rows (one string
    /// instance for a country code) is sized as if it had its own, so such a table is
    /// overstated; a key that is an object the selector makes rather than reads off the
    /// row (two members joined into a string) is an object per row beside it, counted
    /// only as its reference; superseded rows a slow reader's pin keeps in the chains
    /// are not counted; a log that keeps history in this process (the in-memory log) is
    /// the log's to report.
    /// </para>
    /// </summary>
    public TableMemoryEstimate EstimateMemory()
    {
        long tailChanges, supersededRows;
        int tailVersions;
        lock (_lock)
        {
            tailChanges = _tailChanges;
            supersededRows = _tailSupersededRows;
            tailVersions = _tailCount;
        }
        var rows = Count;
        var mean = _rowSizes.Mean;
        long indexBytes = 0;
        foreach (var index in _indexesByName.Values)
            indexBytes += ((TableIndex<TKey, TRow>)index).EstimatedBytes;
        return new TableMemoryEstimate(
            rows, _rowSizes.Sampled, Math.Round(mean, 1),
            (long)(rows * mean) + rows * VersionChains<TKey, TRow>.BytesPerKey,
            indexBytes,
            tailChanges, supersededRows,
            (long)(supersededRows * mean) + tailChanges * System.Runtime.CompilerServices.Unsafe.SizeOf<TableChange<TKey, TRow>>() + tailVersions * 96L);
    }

    // ── test and diagnostics hooks ─────────────────────────────────────────────────────

    /// <summary>Entries in <paramref name="key"/>'s version chain (0 when the key is absent).</summary>
    internal int ChainLength(TKey key) => _rows.ChainLength(key);

    /// <summary>Keys physically held, deletions awaiting a trim included.</summary>
    internal int PhysicalKeyCount => _rows.KeyCount;

    /// <summary>Pins currently registered, across all versions.</summary>
    internal int PinCount
    {
        get
        {
            lock (_lock)
                return _pins.Values.Sum();
        }
    }

    /// <summary>Versions currently held in the tail.</summary>
    internal int TailCount
    {
        get
        {
            lock (_lock)
                return _tailCount;
        }
    }
}

/// <summary>What <see cref="SharedTable{TKey,TRow}.EstimateMemory"/> answers: bytes of
/// managed heap, estimated. <c>MeanRowBytes</c> is one row's objects, from
/// <c>SampledRows</c> rows sized as they were written; <c>RowBytes</c> is the rows at that
/// mean plus the key map; <c>IndexBytes</c> the secondary indexes' own maps and groups;
/// <c>HistoryBytes</c> the <c>HistoryRows</c> superseded rows and <c>HistoryChanges</c>
/// change entries that the recent versions kept for attached circuits hold.</summary>
public sealed record TableMemoryEstimate(
    long Rows, long SampledRows, double MeanRowBytes, long RowBytes, long IndexBytes, long HistoryChanges, long HistoryRows, long HistoryBytes)
{
    public long TotalBytes => RowBytes + IndexBytes + HistoryBytes;
}

/// <summary>
/// A pinned, read-only view of a <see cref="SharedTable{TKey,TRow}"/> at one version.
/// Reads are lock-free and may be made from any thread. Dispose exactly once: the pin is
/// what keeps this version readable, and this is a struct, so a copy disposed twice
/// releases a pin it does not hold.
/// </summary>
public readonly struct TableSnapshot<TKey, TRow> : IDisposable
    where TKey : notnull
    where TRow : class
{
    private readonly SharedTable<TKey, TRow> _table;

    internal TableSnapshot(SharedTable<TKey, TRow> table, long version)
    {
        _table = table;
        Version = version;
    }

    /// <summary>The version this snapshot reads.</summary>
    public long Version { get; }

    /// <summary>The row for <paramref name="key"/> as of <see cref="Version"/>.</summary>
    public bool TryGet(TKey key, [MaybeNullWhen(false)] out TRow row) => _table.TryGet(key, Version, out row);

    /// <summary>Batched <see cref="TryGet"/>: appends one entry per key to
    /// <paramref name="into"/>, null where the key has no row, aligned with
    /// <paramref name="keys"/>. Appends, never clears — the buffer is the caller's.</summary>
    public void GetManyInto(IReadOnlyList<TKey> keys, List<TRow?> into)
    {
        for (var i = 0; i < keys.Count; i++)
        {
            _table.TryGet(keys[i], Version, out var row);
            into.Add(row);
        }
    }

    /// <summary>Every row as of <see cref="Version"/>, in no particular order: one scan of
    /// the table, for small tables and for sampling. Throws if the writer revokes the pin
    /// part-way (<see cref="SharedTableOptions.MaxRetainedVersions"/>) rather than end
    /// short.</summary>
    public IEnumerable<KeyValuePair<TKey, TRow>> Rows() => _table.RowsAt(Version);

    /// <summary>The rows under <paramref name="indexKey"/> in the index declared as
    /// <paramref name="index"/> (<see cref="SharedTable{TKey,TRow}.AddIndex"/>), as of
    /// <see cref="Version"/>: one index read and one row read per row under the key.
    /// Throws if no index of that name and key type is declared.</summary>
    public IReadOnlyList<KeyValuePair<TKey, TRow>> RowsBy<TIndexKey>(string index, TIndexKey indexKey)
        where TIndexKey : notnull =>
        _table.RowsBy(index, indexKey, Version);

    /// <summary>A page of <see cref="RowsBy{TIndexKey}(string,TIndexKey)"/>: the keys under
    /// <paramref name="indexKey"/> are passed over up to <paramref name="skip"/> and at
    /// most <paramref name="take"/> rows are read. <paramref name="total"/> is how many
    /// keys the index holds under the key. The order is the index's own: stable for one
    /// version, so pages of one snapshot neither overlap nor miss a row.</summary>
    public IReadOnlyList<KeyValuePair<TKey, TRow>> RowsBy<TIndexKey>(string index, TIndexKey indexKey, int skip, int take, out int total)
        where TIndexKey : notnull =>
        _table.RowsBy(index, indexKey, Version, skip, take, out total);

    public void Dispose() => _table.ReleasePin(Version);
}

/// <summary>
/// The single writer of a <see cref="SharedTable{TKey,TRow}"/>. Stage changes with
/// <see cref="UpsertAsync(TRow, int, long)"/> and <see cref="DeleteAsync(TKey, int, long)"/>,
/// then <see cref="CommitAsync"/> them as one version: readers see all of a batch or none of
/// it. Within a batch the last change to a key wins. A table has one logical producer; the
/// writer serializes callers, but two producers interleaving batches would still be racing
/// each other's rows.
/// <para>
/// Changes read from a replayable source carry the source's partition and offset. The table
/// records them with each version (<see cref="SharedTable{TKey,TRow}.CommittedOffsets"/>)
/// and drops a change stamped at or below its partition's position, so an adapter that
/// resumes a little early after a restart cannot step a row back to an older value.
/// </para>
/// </summary>
public sealed class TableWriter<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    private readonly SharedTable<TKey, TRow> _table;
    private readonly ITableLog<TKey, TRow>? _log;

    // Staging and committing are separate so that changes can be staged while a commit is
    // waiting on the log: _stageLock guards the batch being filled, _commitGate admits one
    // commit at a time. The two batches swap roles at each commit and are reused.
    private readonly object _stageLock = new();
    private readonly SemaphoreSlim _commitGate = new(1, 1);
    private Batch _staging = new();
    private Batch _committing = new();

    // Partition → highest offset accepted, staged or committed: what a replayed record is
    // compared against. Guarded by _stageLock.
    private readonly Dictionary<int, long> _positions = [];

    // The version under construction; emptied into an exact-size array per version.
    private readonly List<TableChange<TKey, TRow>> _changes = [];

    // The circuit writing this table through ToTable, or null. While it is set the public
    // methods refuse: a table has one writer.
    private volatile TableWriterClaim? _owner;

    internal TableWriterClaim? Owner
    {
        get => _owner;
        set => _owner = value;
    }

    // Set while the table follows its log (a standby): the log's versions are the only
    // writes, and the public methods refuse until the table is promoted.
    private volatile bool _following;

    internal bool Following
    {
        get => _following;
        set => _following = value;
    }

    /// <summary>Applies a version another process wrote, read from the log: the follower's
    /// one write path. Versions arrive in order, already durable, so there is nothing to
    /// stage or append — only the view, the indexes and the replay positions to bring up.</summary>
    internal async ValueTask ApplyFollowedAsync(TableVersion<TKey, TRow> version)
    {
        await _commitGate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (version.Version != _table.LatestVersion + 1)
                throw new InvalidOperationException(
                    $"Table '{_table.Name}' is at version {_table.LatestVersion} and was handed version {version.Version} by its log: " +
                    "versions must be followed in order, without gaps.");

            long countDelta = 0;
            foreach (var change in version.Changes)
                countDelta += (change.New is null ? 0 : 1) - (_table.Current(change.Key) is null ? 0 : 1);
            _table.PrepareIndexes(version.Changes);
            _table.Publish(version, countDelta);
            lock (_stageLock)
                SeedPositions(version.Offsets);
        }
        finally
        {
            _commitGate.Release();
        }
    }

    // Why nothing further can be written, and the exception that caused it. The reason is
    // written first, so a thread that sees the fault sees its reason.
    private string? _faultReason;
    private volatile Exception? _fault;

    /// <summary>A batch under construction: one entry per key, in first-touched order, so a
    /// key changed twice before a commit costs one row version rather than two.</summary>
    private sealed class Batch
    {
        public readonly Dictionary<TKey, int> Index = [];
        public readonly List<KeyValuePair<TKey, TRow?>> Changes = [];
        public readonly Dictionary<int, long> Offsets = [];

        /// <summary>The offsets are to be recorded even if nothing changes
        /// (<see cref="AdvanceAsync"/>).</summary>
        public bool RecordOffsets;

        public void Clear()
        {
            Index.Clear();
            Changes.Clear();
            Offsets.Clear();
            RecordOffsets = false;
        }
    }

    internal TableWriter(SharedTable<TKey, TRow> table, ITableLog<TKey, TRow>? log, TableLoadPacing? pacing)
    {
        _table = table;
        _log = log;
        _pacer = pacing is null ? null : new LoadPacer(pacing);
    }

    // Null when the table's options turn pacing off. Called under the commit gate.
    private readonly LoadPacer? _pacer;

    /// <summary>What pacing large loads has done (<see cref="SharedTableOptions.LoadPacing"/>);
    /// all zero when pacing is off.</summary>
    public TableLoadPacingStats Pacing => _pacer?.Stats ?? default;

    /// <summary>Starts the replay positions at a restored table's committed offsets.</summary>
    internal void SeedPositions(IReadOnlyDictionary<int, long> offsets)
    {
        foreach (var (partition, offset) in offsets)
            _positions[partition] = offset;
    }

    /// <summary>Stages an insert of <paramref name="row"/>, or a replacement of the current
    /// row with its key, read from <paramref name="partition"/> at <paramref name="offset"/>
    /// of the source. Nothing is visible until <see cref="CommitAsync"/>.</summary>
    public ValueTask UpsertAsync(TRow row, int partition, long offset)
    {
        ThrowIfOwned();
        ArgumentNullException.ThrowIfNull(row);
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        Stage(_table.KeySelector(row), row, partition, offset);
        return default;
    }

    /// <summary>Stages an upsert from a source with no offsets to resume from.</summary>
    public ValueTask UpsertAsync(TRow row)
    {
        ThrowIfOwned();
        ArgumentNullException.ThrowIfNull(row);
        Stage(_table.KeySelector(row), row, 0, -1);
        return default;
    }

    /// <summary>Stages a delete of <paramref name="key"/>'s row (a no-op if there is none),
    /// read from <paramref name="partition"/> at <paramref name="offset"/> of the source.</summary>
    public ValueTask DeleteAsync(TKey key, int partition, long offset)
    {
        ThrowIfOwned();
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        Stage(key, null, partition, offset);
        return default;
    }

    /// <summary>
    /// Records that the source has been read up to and including <paramref name="offset"/>
    /// of <paramref name="partition"/>, whether or not anything read there changed the
    /// table. Offsets normally ride with the versions that change something, so a tail of
    /// records that changed nothing — or of offsets with no record behind them, such as a
    /// Kafka transaction's marker — is read again after a restart. That is harmless until
    /// the source no longer holds those offsets. After this call the next
    /// <see cref="CommitAsync"/> records the position even if it has no change to publish:
    /// it then mints a version with no changes, which is the one case in which that
    /// happens. An offset below the partition's position changes nothing.
    /// </summary>
    public ValueTask AdvanceAsync(int partition, long offset)
    {
        ThrowIfOwned();
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ThrowIfFaulted();
        lock (_stageLock)
        {
            if (_positions.TryGetValue(partition, out var position) && offset < position)
                return default;
            _positions[partition] = offset;
            _staging.Offsets[partition] = offset;
            _staging.RecordOffsets = true;
        }
        return default;
    }

    /// <summary>Stages a delete from a source with no offsets to resume from.</summary>
    public ValueTask DeleteAsync(TKey key)
    {
        ThrowIfOwned();
        Stage(key, null, 0, -1);
        return default;
    }

    private void Stage(TKey key, TRow? row, int partition, long offset)
    {
        if (key is null)
            throw new ArgumentNullException(nameof(key), "A table key cannot be null.");
        ThrowIfFaulted();
        lock (_stageLock)
        {
            if (offset >= 0)
            {
                if (_positions.TryGetValue(partition, out var position) && offset <= position)
                    return;
                _positions[partition] = offset;
                _staging.Offsets[partition] = offset;
            }

            if (_staging.Index.TryGetValue(key, out var index))
                _staging.Changes[index] = new(key, row);
            else
            {
                _staging.Index[key] = _staging.Changes.Count;
                _staging.Changes.Add(new(key, row));
            }
        }
    }

    /// <summary>
    /// Publishes everything staged as one version and returns the table's latest version —
    /// unchanged when the batch altered nothing (re-upserting a row that is already current
    /// and deleting an absent key are no-ops, and a version with no changes is never minted,
    /// unless <see cref="AdvanceAsync"/> asked for a position to be recorded).
    /// <para>
    /// Visible means durable: with a log, the version is appended first and readers see it
    /// only once the append completes. Changes staged meanwhile go into the next version.
    /// This also waits while an attached circuit is a full tail behind
    /// (<see cref="SharedTableOptions.TailVersions"/>, <see cref="SharedTableOptions.TailChanges"/>)
    /// and no log holds the versions it still needs.
    /// </para>
    /// <para>
    /// <paramref name="cancellationToken"/> cancels the waiting — for another commit to
    /// finish, and for that lagging circuit — and a commit cancelled there leaves the staged
    /// changes staged. Once the batch has been handed to the log the commit is no longer
    /// cancellable and runs to its end: the token is not passed to the append.
    /// </para>
    /// <para>
    /// A failed append is terminal: whether the version reached the log is unknown, so the
    /// writer faults and every later call throws.
    /// </para>
    /// </summary>
    /// <exception cref="TableLogFencedException">Another process has taken over the log.</exception>
    public async ValueTask<long> CommitAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfOwned();
        ThrowIfFaulted();
        await _commitGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfOwned();
            _table.EndWriterHoldForDirectWrite();
            return await CommitGatedAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _commitGate.Release();
        }
    }

    /// <summary>The commit itself; the caller holds the commit gate.</summary>
    internal async ValueTask<long> CommitGatedAsync(CancellationToken cancellationToken)
    {
        ThrowIfFaulted();
        lock (_stageLock)
        {
            if (_staging.Changes.Count == 0 && !_staging.RecordOffsets)
                return _table.LatestVersion;
        }

        // Before the batch is taken, so that a cancelled wait loses nothing.
        await _table.WaitForTailRoomAsync(cancellationToken).ConfigureAwait(false);

        Batch batch;
        lock (_stageLock)
        {
            batch = _staging;
            (_staging, _committing) = (_committing, batch);
        }

        long countDelta = 0;
        TableVersion<TKey, TRow> version;
        try
        {
            foreach (var (key, row) in batch.Changes)
            {
                var current = _table.Current(key);
                if (row is null ? current is null : current is not null && EqualityComparer<TRow>.Default.Equals(current, row))
                    continue;
                _changes.Add(new TableChange<TKey, TRow>(key, current, row));
                countDelta += (row is null ? 0 : 1) - (current is null ? 0 : 1);
            }

            var offsets = _table.CommittedOffsets;
            if (_changes.Count == 0 && batch.RecordOffsets)
            {
                // Asked to record the position (AdvanceAsync). If it is already recorded
                // there is still nothing to publish.
                var ahead = false;
                foreach (var (partition, offset) in batch.Offsets)
                    ahead |= !offsets.TryGetValue(partition, out var committed) || offset > committed;
                if (!ahead)
                    return _table.LatestVersion;
            }
            else if (_changes.Count == 0)
            {
                // Nothing to publish, so nothing to record the offsets in: they ride with
                // the next version that changes something. Replaying these records after
                // a crash is harmless — they changed nothing the first time either.
                lock (_stageLock)
                {
                    foreach (var (partition, offset) in batch.Offsets)
                        if (!_staging.Offsets.ContainsKey(partition))
                            _staging.Offsets[partition] = offset;
                }
                return _table.LatestVersion;
            }

            if (batch.Offsets.Count > 0)
            {
                var advanced = new Dictionary<int, long>(offsets);
                foreach (var (partition, offset) in batch.Offsets)
                    advanced[partition] = offset;
                offsets = advanced;
            }
            version = new TableVersion<TKey, TRow>(_table.LatestVersion + 1, _changes.ToArray(), offsets);
        }
        finally
        {
            // Both are scratch: the next commit expects to find them empty.
            _changes.Clear();
            batch.Clear();
        }

        // The indexes' selectors run here, before the append: once the version is in
        // the log there is no failing it. The batch is already consumed, so a selector
        // that throws has lost changes the source believes delivered — a fault.
        try
        {
            _table.PrepareIndexes(version.Changes);
        }
        catch (Exception ex)
        {
            Fault("a secondary index's selector threw while a version was being built, and that version's changes are lost", ex);
            throw;
        }

        if (_log is not null)
        {
            try
            {
                // Not cancellable: the batch is taken, and an append abandoned half-way
                // is one whose outcome nobody knows — the fault below, brought on by a
                // caller's timeout. Cancellation ends at the two waits above.
                await _log.AppendAsync(version, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Fault("appending a version to the table's log failed, so whether that version is durable is unknown", ex);
                throw;
            }
        }

        _table.Publish(version, countDelta);
        // After the version is visible: a step of pacing never delays what readers see.
        _pacer?.Committed(version.Changes.Count);
        return version.Version;
    }

    // ── a circuit as the writer (ToTable); see Tables/DerivedTable.cs ──────────────────

    /// <summary>Takes the commit gate for the circuit holding <paramref name="claim"/>. Until
    /// <see cref="Exit"/> nothing else commits, so whatever the circuit reads of the table
    /// through the writer is the table. Refused once the claim has been given up: a
    /// circuit that faulted with a write in flight must not land another.</summary>
    internal async ValueTask EnterAsync(TableWriterClaim claim, CancellationToken cancellationToken)
    {
        await _commitGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        if (_following)
        {
            _commitGate.Release();
            throw new InvalidOperationException(
                $"Table '{_table.Name}' is following its log: promote it (SharedTable.PromoteAsync) before starting the circuit that writes it.");
        }
        if (!ReferenceEquals(_owner, claim))
        {
            _commitGate.Release();
            throw new InvalidOperationException(
                $"The circuit no longer holds the writer of table '{_table.Name}': it faulted or was disposed.");
        }
    }

    internal void Exit() => _commitGate.Release();

    /// <summary>Drops whatever is staged: what a previous writer left behind uncommitted.</summary>
    internal void DiscardStaged()
    {
        lock (_stageLock)
            _staging.Clear();
    }

    /// <summary>Stages a row (or, with null, a delete) for the owning circuit.</summary>
    internal void StageOwned(TKey key, TRow? row) => Stage(key, row, 0, -1);

    /// <summary>The row <paramref name="key"/> will have once what is staged commits.</summary>
    internal TRow? StagedOrCurrent(TKey key)
    {
        lock (_stageLock)
        {
            if (_staging.Index.TryGetValue(key, out var index))
                return _staging.Changes[index].Value;
        }
        return _table.Current(key);
    }

    /// <summary>Gives back batch scratch grown past <paramref name="keys"/> entries: one
    /// table-sized step (a derived table's bootstrap) would otherwise leave its capacity
    /// with the writer for good. The caller holds the commit gate.</summary>
    internal void ReleaseScratchAbove(int keys)
    {
        if (_changes.Capacity > keys)
            _changes.Capacity = 0;
        lock (_stageLock)
        {
            if (_staging.Changes.Capacity > keys)
                _staging = new Batch();
            if (_committing.Changes.Capacity > keys)
                _committing = new Batch();
        }
    }

    /// <summary>
    /// Snapshot sync: makes the table equal to <paramref name="rows"/>. The rows are
    /// upserted and committed a chunk at a time (<see cref="SharedTableOptions.ReplaceChunkRows"/>),
    /// and the last version also deletes every key the snapshot did not mention — the one
    /// thing a stream of upserts cannot express. Rows that are already current change
    /// nothing, so re-syncing an unchanged snapshot mints no version at all. Readers may see
    /// the versions in between: each is a valid mix of old and new rows, and the last equals
    /// the snapshot.
    /// <para>
    /// <paramref name="offsets"/> is the position in the source's change stream that the
    /// snapshot is as of, by partition. It is recorded with the last version, so an adapter
    /// that restarts resumes its deltas from <see cref="SharedTable{TKey,TRow}.CommittedOffsets"/>
    /// instead of taking the snapshot again, and delta records at or below it are dropped as
    /// already applied. If the snapshot stops early — the sequence throws, or the call is
    /// cancelled — the chunks already committed stay, nothing is deleted, and the offsets
    /// are not recorded. Cancellation takes effect between chunks, never part-way through
    /// one that is being appended to the log (see <see cref="CommitAsync"/>).
    /// </para>
    /// <para>
    /// Anything staged before the call is committed with the first chunk. Do not stage
    /// other changes while this runs: a row upserted meanwhile is not in the snapshot and
    /// would be deleted by the last version.
    /// </para>
    /// </summary>
    /// <returns>The table's latest version once the snapshot is in.</returns>
    public async ValueTask<long> ReplaceAllAsync(
        IAsyncEnumerable<TRow> rows,
        IReadOnlyDictionary<int, long>? offsets = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfOwned();
        ArgumentNullException.ThrowIfNull(rows);
        var sync = new SnapshotSync(this);
        await foreach (var row in rows.WithCancellation(cancellationToken).ConfigureAwait(false))
            if (sync.Stage(row))
                await CommitAsync(cancellationToken).ConfigureAwait(false);
        return await sync.FinishAsync(offsets, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc cref="ReplaceAllAsync(IAsyncEnumerable{TRow}, IReadOnlyDictionary{int, long}?, CancellationToken)"/>
    public async ValueTask<long> ReplaceAllAsync(
        IEnumerable<TRow> rows,
        IReadOnlyDictionary<int, long>? offsets = null,
        CancellationToken cancellationToken = default)
    {
        ThrowIfOwned();
        ArgumentNullException.ThrowIfNull(rows);
        var sync = new SnapshotSync(this);
        foreach (var row in rows)
            if (sync.Stage(row))
                await CommitAsync(cancellationToken).ConfigureAwait(false);
        return await sync.FinishAsync(offsets, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>One <c>ReplaceAllAsync</c> in progress: the keys the snapshot has mentioned
    /// so far, and how full the chunk being staged is.</summary>
    private sealed class SnapshotSync(TableWriter<TKey, TRow> writer)
    {
        private readonly HashSet<TKey> _seen = [];
        private int _staged;

        /// <summary>Stages one snapshot row; true when that completes a chunk, which the
        /// caller commits.</summary>
        public bool Stage(TRow row)
        {
            ArgumentNullException.ThrowIfNull(row);
            var key = writer._table.KeySelector(row);
            writer.Stage(key, row, 0, -1);
            _seen.Add(key);
            if (++_staged < writer._table.ReplaceChunkRows)
                return false;
            _staged = 0;
            return true;
        }

        /// <summary>Commits the last rows together with the deletion of every key the
        /// snapshot left out, and the snapshot's source offsets.</summary>
        public ValueTask<long> FinishAsync(IReadOnlyDictionary<int, long>? offsets, CancellationToken cancellationToken)
        {
            // The table as last committed: rows of the chunk still staged are in _seen, so
            // it does not matter that the scan cannot see them.
            foreach (var (key, _) in writer._table.CurrentRows())
                if (!_seen.Contains(key))
                    writer.Stage(key, null, 0, -1);
            if (offsets is not null)
                writer.StageOffsets(offsets);
            return writer.CommitAsync(cancellationToken);
        }
    }

    /// <summary>Records that the source has been consumed up to <paramref name="offsets"/>,
    /// to be committed with the staged changes. A partition already past its entry stays
    /// where it is.</summary>
    private void StageOffsets(IReadOnlyDictionary<int, long> offsets)
    {
        lock (_stageLock)
        {
            foreach (var (partition, offset) in offsets)
            {
                ArgumentOutOfRangeException.ThrowIfNegative(partition, nameof(offsets));
                ArgumentOutOfRangeException.ThrowIfNegative(offset, nameof(offsets));
                if (_positions.TryGetValue(partition, out var position) && offset <= position)
                    continue;
                _positions[partition] = offset;
                _staging.Offsets[partition] = offset;

                // A snapshot's position is recorded even when the snapshot changed nothing:
                // it was taken because the old position was no good.
                _staging.RecordOffsets = true;
            }
        }
    }

    /// <summary>Drops superseded rows no reader can reach any more. <see cref="CommitAsync"/>
    /// does this as it goes; call it when readers have released pins and no commit is due,
    /// to reclaim the rows they were holding. Does nothing while a commit is in flight —
    /// that commit trims as it finishes.</summary>
    public void Trim()
    {
        if (!_commitGate.Wait(0))
            return;
        try
        {
            _table.Trim();
        }
        finally
        {
            _commitGate.Release();
        }
    }

    /// <summary>Runs <paramref name="action"/> as the writer, between versions: no commit
    /// is in flight while it runs and none starts until it returns. Blocks for a commit that
    /// is in flight.</summary>
    internal T Exclusive<T>(Func<T> action)
    {
        _commitGate.Wait();
        try
        {
            return action();
        }
        finally
        {
            _commitGate.Release();
        }
    }

    private void Fault(string reason, Exception cause)
    {
        _faultReason = reason;
        _fault = cause;
    }

    private void ThrowIfOwned()
    {
        if (_following)
            throw new InvalidOperationException(
                $"Table '{_table.Name}' is following its log: another process is its writer. Promote the table " +
                "(SharedTable.PromoteAsync) before writing to it.");
        if (_owner is { } owner)
            throw new InvalidOperationException(
                $"Table '{_table.Name}' is written by {owner.Circuit} (ToTable). A table has one writer: while that circuit " +
                "runs, nothing else may write to it.");
    }

    private void ThrowIfFaulted()
    {
        if (_fault is { } fault)
            throw new InvalidOperationException(
                $"The writer of table '{_table.Name}' has faulted: {_faultReason}. Nothing further can be written; " +
                "restore the table from its source or its log.",
                fault);
    }
}
