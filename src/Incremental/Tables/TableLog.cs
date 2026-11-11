using System.Runtime.CompilerServices;

namespace Incremental.Tables;

/// <summary>
/// One key's change between two versions of a <see cref="SharedTable{TKey,TRow}"/>:
/// <see cref="Old"/> is the row before (null when the key had none), <see cref="New"/> the
/// row after (null when it was deleted). Circuits receive these as their table delta, one
/// per changed key per step.
/// </summary>
public readonly record struct TableChange<TKey, TRow>(TKey Key, TRow? Old, TRow? New)
    where TKey : notnull
    where TRow : class
{
    /// <summary>By key alone. A delta carries one change per key, so the key already spreads
    /// the entries, and hashing two whole rows for every change a Z-set holds would cost far
    /// more than the rare collision it avoids. Equality still compares the rows.</summary>
    public override int GetHashCode() => EqualityComparer<TKey>.Default.GetHashCode(Key);
}

/// <summary>
/// One version of a shared table as the change log records it: the rows it changed, and the
/// source offsets the writer had consumed when it produced it. Immutable once built — the
/// same instance is held by the log, by the table's in-memory tail and by every circuit
/// reading it.
/// </summary>
public sealed class TableVersion<TKey, TRow>(
    long version,
    IReadOnlyList<TableChange<TKey, TRow>> changes,
    IReadOnlyDictionary<int, long> offsets)
    where TKey : notnull
    where TRow : class
{
    /// <summary>Assigned by the writer: 1, 2, 3, … with no gaps.</summary>
    public long Version { get; } = version;

    /// <summary>One change per key, each a real change (<c>Old</c> differs from <c>New</c>).</summary>
    public IReadOnlyList<TableChange<TKey, TRow>> Changes { get; } = changes;

    /// <summary>Source partition → offset of the last record consumed up to this version
    /// (cumulative, not just this version's records).</summary>
    public IReadOnlyDictionary<int, long> Offsets { get; } = offsets;
}

/// <summary>Where a table log ends: its last version and the source offsets consumed up to it.</summary>
public sealed record TableHead(long Version, IReadOnlyDictionary<int, long> Offsets);

/// <summary>
/// The change log behind a <see cref="SharedTable{TKey,TRow}"/>: the seam between the table
/// and whatever makes it durable (see <c>docs/design/shared-tables.md</c>, <em>The change
/// log</em>). A log has exactly one appender — the table's writer — and any number of readers.
/// </summary>
public interface ITableLog<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    /// <summary>The log's identity, minted when it is created. A table that is dropped and
    /// recreated gets a new epoch, which is how a circuit notices that the version it
    /// committed belongs to a different history.</summary>
    string Epoch { get; }

    /// <summary>The oldest version <see cref="ReadAsync"/> can still return; one past the
    /// last version when no history is retained.</summary>
    long OldestRetainedVersion { get; }

    /// <summary>The last durable version; 0 for an empty log. A standby asks this at
    /// takeover, once the previous writer can append nothing more, to know how far it must
    /// have followed before it becomes the writer.</summary>
    ValueTask<long> LastVersionAsync(CancellationToken cancellationToken = default);

    /// <summary>Appends one version — its changes and source offsets, atomically — and
    /// completes once it is durable. The table publishes the version to readers only after
    /// this completes. <paramref name="version"/> must be the next one in sequence.</summary>
    /// <exception cref="TableLogFencedException">Another writer has taken the log over.</exception>
    ValueTask AppendAsync(TableVersion<TKey, TRow> version, CancellationToken cancellationToken = default);

    /// <summary>The versions in <c>(fromExclusive, toInclusive]</c>, in order. Fails if the
    /// range reaches below <see cref="OldestRetainedVersion"/> or past the last version.</summary>
    IAsyncEnumerable<TableVersion<TKey, TRow>> ReadAsync(
        long fromExclusive, long toInclusive, CancellationToken cancellationToken = default);

    /// <summary>Every version after <paramref name="fromExclusive"/>, in order, waiting for
    /// new ones instead of ending: how a standby keeps up. Ends only by cancellation.</summary>
    IAsyncEnumerable<TableVersion<TKey, TRow>> FollowAsync(
        long fromExclusive, CancellationToken cancellationToken = default);

    /// <summary>Hands every current row to <paramref name="row"/> and returns the head those
    /// rows are as of. Bounded by the table's size, not by its history.</summary>
    ValueTask<TableHead> RestoreAsync(Action<TKey, TRow> row, CancellationToken cancellationToken = default);

    /// <summary>Drops history below <paramref name="version"/>; afterwards
    /// <see cref="OldestRetainedVersion"/> is at least that. Current rows are unaffected.</summary>
    ValueTask TrimBeforeAsync(long version, CancellationToken cancellationToken = default);
}

/// <summary>
/// Thrown by <see cref="ITableLog{TKey,TRow}.AppendAsync"/> when another process has taken
/// over as the log's writer. The version was not appended; the table's writer faults.
/// </summary>
public sealed class TableLogFencedException(string message, Exception? innerException = null)
    : Exception(message, innerException);

/// <summary>
/// A heap-backed <see cref="ITableLog{TKey,TRow}"/>: the current rows, the head, and the
/// last <c>maxRetainedVersions</c> versions of history. It stands in for a durable log in
/// tests and in single-process runs that want a table to outlive the
/// <see cref="SharedTable{TKey,TRow}"/> object reading it. History is capped rather than
/// trimmed on request alone because nothing here may grow without bound; a reader further
/// behind than the cap finds its range truncated, exactly as with a durable log's
/// retention cap.
/// <para>
/// An instance is one process's connection to the log. The one that created the log is its
/// writer; <see cref="Connect"/> gives another connection to the same log, as a standby
/// would hold, which reads and follows. A connection becomes the writer with
/// <see cref="TakeOver"/>, and from then on an append through any other throws
/// <see cref="TableLogFencedException"/>.
/// </para>
/// </summary>
public sealed class InMemoryTableLog<TKey, TRow> : ITableLog<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    private static readonly IReadOnlyDictionary<int, long> NoOffsets = new Dictionary<int, long>();

    /// <summary>The log itself, shared by every connection to it.</summary>
    private sealed class Shared(int maxRetainedVersions, long maxRetainedChanges)
    {
        public readonly int MaxRetainedVersions = maxRetainedVersions;
        public readonly long MaxRetainedChanges = maxRetainedChanges;

        // Row changes across History.
        public long RetainedChanges;
        public readonly string Epoch = Guid.NewGuid().ToString("N");
        public readonly Dictionary<TKey, TRow> Rows = [];

        // Contiguous and ending at Last, so the oldest retained version is derived, not stored.
        public readonly Queue<TableVersion<TKey, TRow>> History = new();
        public long Last;
        public IReadOnlyDictionary<int, long> Offsets = NoOffsets;

        // Completed and replaced by every append: what a follower that has caught up waits on.
        public TaskCompletionSource Appended = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // The one connection that may append.
        public object? Writer;
    }

    private readonly Shared _log;

    /// <param name="maxRetainedVersions">Versions of history kept for
    /// <see cref="ReadAsync"/>; the oldest is dropped as each new one arrives. Zero keeps
    /// rows and head only.</param>
    /// <param name="maxRetainedChanges">Row changes kept across those versions: a count of
    /// versions bounds nothing when one version can change a million rows. Past it the
    /// oldest versions go, down to the newest one, which is always kept (a follower that
    /// is up to date must be able to read the version just appended). The default keeps
    /// whatever the version cap keeps.</param>
    public InMemoryTableLog(int maxRetainedVersions = 1024, long maxRetainedChanges = long.MaxValue)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxRetainedVersions);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxRetainedChanges, 1);
        _log = new Shared(maxRetainedVersions, maxRetainedChanges) { Writer = this };
    }

    /// <summary>Row changes held across the retained versions of history.</summary>
    public long RetainedChanges
    {
        get
        {
            lock (_log)
                return _log.RetainedChanges;
        }
    }

    private InMemoryTableLog(Shared log) => _log = log;

    public string Epoch => _log.Epoch;

    /// <summary>Another connection to this log: it reads and follows, and cannot append
    /// until it calls <see cref="TakeOver"/>.</summary>
    public InMemoryTableLog<TKey, TRow> Connect() => new(_log);

    /// <summary>Makes this connection the log's writer and fences every other: the
    /// stand-in for a new active initialising the transactional producer.</summary>
    public void TakeOver()
    {
        lock (_log)
            _log.Writer = this;
    }

    public long OldestRetainedVersion
    {
        get
        {
            lock (_log)
                return _log.Last - _log.History.Count + 1;
        }
    }

    public ValueTask<long> LastVersionAsync(CancellationToken cancellationToken = default)
    {
        lock (_log)
            return new ValueTask<long>(_log.Last);
    }

    public ValueTask AppendAsync(TableVersion<TKey, TRow> version, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(version);
        TaskCompletionSource appended;
        lock (_log)
        {
            if (!ReferenceEquals(_log.Writer, this))
                throw new TableLogFencedException(
                    $"This connection is not the table log's writer: another has taken the log over. Version {version.Version} was not appended.");
            if (version.Version != _log.Last + 1)
                throw new InvalidOperationException(
                    $"Table log is at version {_log.Last}; the next append must be version {_log.Last + 1}, not {version.Version}.");

            foreach (var change in version.Changes)
            {
                if (change.New is null)
                    _log.Rows.Remove(change.Key);
                else
                    _log.Rows[change.Key] = change.New;
            }
            _log.Last = version.Version;
            _log.Offsets = version.Offsets;
            _log.History.Enqueue(version);
            _log.RetainedChanges += version.Changes.Count;
            while (_log.History.Count > _log.MaxRetainedVersions
                   || (_log.RetainedChanges > _log.MaxRetainedChanges && _log.History.Count > 1))
                _log.RetainedChanges -= _log.History.Dequeue().Changes.Count;

            appended = _log.Appended;
            _log.Appended = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        appended.TrySetResult();
        return default;
    }

    public IAsyncEnumerable<TableVersion<TKey, TRow>> ReadAsync(
        long fromExclusive, long toInclusive, CancellationToken cancellationToken = default)
    {
        lock (_log)
        {
            if (toInclusive > _log.Last)
                throw new ArgumentOutOfRangeException(nameof(toInclusive), toInclusive,
                    $"Table log ends at version {_log.Last}.");
        }
        return ReadCore(fromExclusive, toInclusive, cancellationToken);
    }

    public IAsyncEnumerable<TableVersion<TKey, TRow>> FollowAsync(
        long fromExclusive, CancellationToken cancellationToken = default) =>
        ReadCore(fromExclusive, long.MaxValue, cancellationToken);

    private async IAsyncEnumerable<TableVersion<TKey, TRow>> ReadCore(
        long fromExclusive, long toInclusive, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var next = fromExclusive + 1;
        var batch = new List<TableVersion<TKey, TRow>>();
        while (next <= toInclusive)
        {
            Task appended;
            lock (_log)
            {
                var oldest = _log.Last - _log.History.Count + 1;
                if (next < oldest)
                    throw new InvalidOperationException(
                        $"Table log no longer retains version {next}: its history starts at version {oldest}.");
                foreach (var version in _log.History)
                    if (version.Version >= next && version.Version <= toInclusive)
                        batch.Add(version);
                appended = _log.Appended.Task;
            }

            foreach (var version in batch)
            {
                yield return version;
                next = version.Version + 1;
            }
            batch.Clear();

            if (next <= toInclusive)
                await appended.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public ValueTask<TableHead> RestoreAsync(Action<TKey, TRow> row, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(row);
        lock (_log)
        {
            foreach (var (key, current) in _log.Rows)
                row(key, current);
            return new ValueTask<TableHead>(new TableHead(_log.Last, _log.Offsets));
        }
    }

    public ValueTask TrimBeforeAsync(long version, CancellationToken cancellationToken = default)
    {
        lock (_log)
        {
            while (_log.History.TryPeek(out var oldest) && oldest.Version < version)
                _log.RetainedChanges -= _log.History.Dequeue().Changes.Count;
        }
        return default;
    }
}
