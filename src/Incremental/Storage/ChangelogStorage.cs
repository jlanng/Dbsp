using System.Collections.Concurrent;
using System.Text;

namespace Incremental.Storage;

/// <summary>Settings of a <see cref="ChangelogStorage"/>.</summary>
public sealed class ChangelogStorageOptions
{
    /// <summary>
    /// Serializes store keys and values into changelog records. The output is a persisted
    /// format and — because the log compacts by comparing key bytes — must be the same
    /// bytes every time for equal values, and must never contain a NUL character (keys are
    /// NUL-separated segments). The default is JSON, which meets both. Every record value
    /// carries a format byte in front of the serializer's output.
    /// </summary>
    public IStateSerializer Serializer { get; init; } = JsonStateSerializer.Instance;
}

/// <summary>
/// Checkpointed operator state for every circuit in a process, held on the heap and logged
/// to one <see cref="IStateChangelog"/>. Each circuit ("pipeline") gets its own
/// <see cref="ChangelogStorageProvider"/> from <see cref="Pipeline"/>; a checkpoint commit
/// appends that pipeline's changed entries, and a commit record carrying its offsets, as
/// one changelog transaction.
/// <para>
/// A storage is in one of two roles. A <b>follower</b> (<see cref="Follow"/>) reads the
/// changelog continuously and applies it to its providers' stores; circuits may be built
/// against those providers but not started. A <b>leader</b> (<see cref="OpenAsync"/>, or a
/// follower after <see cref="PromoteAsync"/>) has read the changelog to its end, holds the
/// changelog's writer, and stops reading: its circuits start from the state it holds and
/// commit through it. The change of role is the warm-standby takeover: build every circuit
/// on the standby, promote, then <c>Start()</c> each one.
/// </para>
/// </summary>
public sealed class ChangelogStorage : IAsyncDisposable
{
    private readonly IStateChangelog _changelog;
    private readonly ChangelogStorageOptions _options;
    private readonly ConcurrentDictionary<string, ChangelogStorageProvider> _pipelines = new(StringComparer.Ordinal);

    // Guards the followers' positions, their fault and the progress signal.
    private readonly object _lock = new();
    private readonly long[] _positions;
    private readonly CancellationTokenSource _followStop = new();
    private readonly Task[] _followers;
    private TaskCompletionSource _progress = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Exception? _followFault;

    private volatile IStateChangelogWriter? _writer;
    private volatile ChangelogFencedException? _fenced;
    private readonly TaskCompletionSource _whenFenced = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _promoting;
    private int _disposed;

    private ChangelogStorage(IStateChangelog changelog, ChangelogStorageOptions? options)
    {
        ArgumentNullException.ThrowIfNull(changelog);
        _changelog = changelog;
        _options = options ?? new ChangelogStorageOptions();
        ArgumentOutOfRangeException.ThrowIfLessThan(changelog.PartitionCount, 1, nameof(changelog));
        _positions = new long[changelog.PartitionCount];
        _followers = new Task[changelog.PartitionCount];
        for (var p = 0; p < _followers.Length; p++)
        {
            var partition = p;
            _followers[p] = Task.Run(() => FollowPartitionAsync(partition));
        }
    }

    /// <summary>
    /// Starts a follower: it reads every partition of <paramref name="changelog"/> from the
    /// start and keeps applying what the active commits. This is the standby. It returns at
    /// once; <see cref="WhenCaughtUpAsync"/> says when the first read has reached the end.
    /// </summary>
    public static ChangelogStorage Follow(IStateChangelog changelog, ChangelogStorageOptions? options = null) =>
        new(changelog, options);

    /// <summary>
    /// Opens the storage as the leader: acquires the changelog's writer — fencing whoever
    /// held it — and restores every pipeline's state by reading the changelog to its end.
    /// This is a cold start of the active; it is <see cref="Follow"/> followed by
    /// <see cref="PromoteAsync"/>.
    /// </summary>
    public static async Task<ChangelogStorage> OpenAsync(
        IStateChangelog changelog, ChangelogStorageOptions? options = null, CancellationToken cancellationToken = default)
    {
        var storage = new ChangelogStorage(changelog, options);
        try
        {
            await storage.PromoteAsync(cancellationToken).ConfigureAwait(false);
            return storage;
        }
        catch
        {
            await storage.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Whether this storage holds the changelog's writer and has not lost it.</summary>
    public bool IsLeader => _writer is not null && _fenced is null;

    /// <summary>Whether any partition is still being read: true for a follower, false once
    /// promoted or disposed.</summary>
    internal bool IsReading => _followers.Any(f => !f.IsCompleted);

    /// <summary>The leader term this storage writes under; 0 while it follows.</summary>
    public long Term => _writer?.Term ?? 0;

    /// <summary>Whether a commit has found the writer fenced. Nothing further can be
    /// committed through this storage; the process should exit.</summary>
    public bool IsFenced => _fenced is not null;

    /// <summary>Completes when a commit finds the writer fenced: the signal for the process
    /// to stop being the active. Never faults.</summary>
    public Task WhenFenced => _whenFenced.Task;

    /// <summary>
    /// The storage provider of one pipeline: pass it as <see cref="CircuitOptions.Storage"/>
    /// of the one circuit that is that pipeline. The same instance is returned every time.
    /// <paramref name="pipelineId"/> is the pipeline's durable identity — it keys the
    /// pipeline's records — so it must be stable across restarts and unique among the
    /// circuits sharing the changelog.
    /// </summary>
    public ChangelogStorageProvider Pipeline(string pipelineId)
    {
        ArgumentException.ThrowIfNullOrEmpty(pipelineId);
        if (pipelineId.Contains('\0'))
            throw new ArgumentException("A pipeline id cannot contain a NUL character.", nameof(pipelineId));
        return _pipelines.GetOrAdd(pipelineId, CreateProvider);
    }

    private ChangelogStorageProvider CreateProvider(string pipelineId) =>
        new(this, pipelineId, PartitionOf(pipelineId, _changelog.PartitionCount), _options.Serializer);

    /// <summary>The partition that holds all of a pipeline's records. Process-stable, like
    /// exchange routing, and for the same reason.</summary>
    internal static int PartitionOf(string pipelineId, int partitionCount) =>
        (int)(StableHash.Of(pipelineId) % (ulong)partitionCount);

    /// <summary>
    /// Completes once the follower has applied everything that was committed to the
    /// changelog when this was called. On a leader it completes at once.
    /// </summary>
    public async Task WhenCaughtUpAsync(CancellationToken cancellationToken = default)
    {
        if (_writer is not null)
            return;
        await WaitForAsync(await EndOffsetsAsync(cancellationToken).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Turns a follower into the leader — the takeover. It acquires the changelog's writer,
    /// which fences the previous leader and settles its last transaction; reads every
    /// partition to the end of what is then committed; and stops following. When it
    /// returns, every provider holds its pipeline's last committed state and commit record,
    /// and the circuits built against them can be started. If it fails or is cancelled the
    /// storage is still a follower and the call can be repeated.
    /// </summary>
    public async Task PromoteAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (_writer is not null)
            throw new InvalidOperationException("This changelog storage is already the leader.");
        if (Interlocked.Exchange(ref _promoting, 1) != 0)
            throw new InvalidOperationException("This changelog storage is already being promoted.");

        IStateChangelogWriter? writer = null;
        try
        {
            writer = await _changelog.AcquireWriterAsync(cancellationToken).ConfigureAwait(false);
            await WaitForAsync(await EndOffsetsAsync(cancellationToken).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);

            // From here it cannot be undone: the followers stop, and the stores belong to
            // whoever starts the circuits.
            _followStop.Cancel();
            await Task.WhenAll(_followers).ConfigureAwait(false);
            lock (_lock)
            {
                if (_followFault is { } fault)
                    throw FollowerFailed(fault);
            }
            _writer = writer;
            writer = null;
        }
        finally
        {
            if (writer is not null)
                await writer.DisposeAsync().ConfigureAwait(false);
            Volatile.Write(ref _promoting, 0);
        }
    }

    private async ValueTask<long[]> EndOffsetsAsync(CancellationToken cancellationToken)
    {
        var ends = new long[_positions.Length];
        for (var p = 0; p < ends.Length; p++)
            ends[p] = await _changelog.EndOffsetAsync(p, cancellationToken).ConfigureAwait(false);
        return ends;
    }

    private async Task WaitForAsync(long[] ends, CancellationToken cancellationToken)
    {
        while (true)
        {
            Task progress;
            lock (_lock)
            {
                if (_followFault is { } fault)
                    throw FollowerFailed(fault);
                var reached = true;
                for (var p = 0; p < ends.Length; p++)
                    reached &= _positions[p] >= ends[p];
                if (reached)
                    return;
                progress = _progress.Task;
            }
            await progress.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private static InvalidOperationException FollowerFailed(Exception fault) => new(
        "Following the state changelog failed, so this storage no longer mirrors it: " + fault.Message +
        " Dispose it and open a new one from the changelog.", fault);

    private async Task FollowPartitionAsync(int partition)
    {
        var token = _followStop.Token;
        var dispatcher = new Dispatcher(this);
        try
        {
            await foreach (var batch in _changelog.ReadAsync(partition, 0, token).ConfigureAwait(false))
            {
                var records = batch.Records;
                for (var i = 0; i < records.Count; i++)
                    dispatcher.Dispatch(records[i]);
                Advance(() => _positions[partition] = batch.NextOffset);

                // Between batches, never inside one: a batch is applied whole or not at all.
                if (token.IsCancellationRequested)
                    return;
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Advance(() => _followFault ??= ex);
        }
    }

    private void Advance(Action change)
    {
        TaskCompletionSource progress;
        lock (_lock)
        {
            change();
            progress = _progress;
            _progress = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        progress.TrySetResult();
    }

    /// <summary>Routes one partition's records to their pipelines. A partition's records
    /// arrive in runs of one pipeline (a commit at a time), so the last provider is
    /// remembered and found again by comparing bytes.</summary>
    private sealed class Dispatcher(ChangelogStorage storage)
    {
        private byte[] _lastPipeline = [];
        private ChangelogStorageProvider? _last;

        public void Dispatch(in ChangelogRecord record)
        {
            // pipeline NUL incarnation NUL ( "$commit" | store NUL entry )
            var key = record.Key.Span;
            var first = key.IndexOf((byte)0);
            var second = first < 0 ? -1 : key[(first + 1)..].IndexOf((byte)0);
            if (second < 0
                || !System.Buffers.Text.Utf8Parser.TryParse(key.Slice(first + 1, second), out long incarnation, out var consumed)
                || consumed != second)
                throw new InvalidDataException(
                    $"Changelog record at offset {record.Offset} has a key this version does not understand: '{Printable(key)}'.");

            var pipeline = key[..first];
            if (_last is null || !pipeline.SequenceEqual(_lastPipeline))
            {
                _lastPipeline = pipeline.ToArray();
                _last = storage.Pipeline(Encoding.UTF8.GetString(pipeline));
            }
            _last.ApplyReplicated(incarnation, key[(first + second + 2)..], record);
        }
    }

    internal static string Printable(ReadOnlySpan<byte> bytes) =>
        Encoding.UTF8.GetString(bytes).Replace('\0', '␀');

    /// <summary>The writer a commit goes through. Throws on a follower and once fenced.</summary>
    internal IStateChangelogWriter RequireWriter(string pipelineId)
    {
        if (_fenced is { } fenced)
            throw new ChangelogFencedException(
                $"Pipeline '{pipelineId}' cannot commit: this process is no longer the changelog's writer. {fenced.Message}", fenced);
        return _writer
            ?? throw new InvalidOperationException(
                $"Pipeline '{pipelineId}' cannot start or commit: its changelog storage is a follower (a standby). " +
                "Promote the storage (ChangelogStorage.PromoteAsync) before starting circuits.");
    }

    internal void MarkFenced(ChangelogFencedException exception)
    {
        _fenced ??= exception;
        _whenFenced.TrySetResult();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        _followStop.Cancel();
        await Task.WhenAll(_followers).ConfigureAwait(false);
        _followStop.Dispose();
        if (_writer is { } writer)
            await writer.DisposeAsync().ConfigureAwait(false);
    }
}
