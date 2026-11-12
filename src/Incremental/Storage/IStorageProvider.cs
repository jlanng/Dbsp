using System.Diagnostics.CodeAnalysis;

namespace Incremental.Storage;

/// <summary>
/// Factory for the state stores that stateful operators (join, aggregate, distinct) use to
/// hold their integrated state. Stores are created per operator, per worker, and the runtime
/// guarantees a store is only ever touched by the worker thread that owns it — so a store
/// need not be thread-safe, though different stores are used concurrently by different workers.
/// </summary>
public interface IStorageProvider
{
    /// <summary>A value → weight map (an integrated Z-set). Used by e.g. <c>Distinct</c>.</summary>
    IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull;

    /// <summary>A key → (value → weight) map (an integrated indexed Z-set). Used by joins and aggregations.</summary>
    IIndexedWeightStore<TKey, TValue> CreateIndexedWeightStore<TKey, TValue>(string name)
        where TKey : notnull
        where TValue : notnull;

    /// <summary>A plain key → value map. Used to remember previously emitted results for retraction.</summary>
    IValueStore<TKey, TValue> CreateValueStore<TKey, TValue>(string name) where TKey : notnull;

    /// <summary>
    /// A key → deadline map that hands back the keys whose deadline has passed. It is what
    /// event-time operators keep their eviction and window-close deadlines in, so that a
    /// circuit restored from a checkpoint closes exactly the keys an uninterrupted run
    /// would have: a deadline is state like any other, staged with the step that set it
    /// and committed with the checkpoint that covers that step.
    /// <para>
    /// The default serves a provider that does not checkpoint: a heap store, gone with the
    /// operator. A checkpointing provider must implement this method — the default refuses
    /// for one, because deadlines that do not survive a restart leave every key that was
    /// open at the restart, and is never touched again, in state for good.
    /// </para>
    /// </summary>
    IDeadlineStore<TKey> CreateDeadlineStore<TKey>(string name) where TKey : notnull =>
        this is ICheckpointStorageProvider
            ? throw new NotSupportedException(
                $"{GetType().Name} checkpoints but does not implement {nameof(IStorageProvider)}.{nameof(CreateDeadlineStore)} " +
                $"(asked for '{name}'): the deadlines of event-time operators (window closes, EvictWhen, session gaps) are " +
                "checkpointed state, and must be staged and committed like the provider's other stores.")
            : new InMemoryStorageProvider.DeadlineStore<TKey>();
}

/// <summary>
/// A storage provider that checkpoints: operators' writes are staged (visible through the
/// stores immediately, durable only on commit), and <see cref="Commit"/> persists everything
/// staged since the previous commit atomically together with the input offsets whose records
/// produced it — so durable state and offsets always agree. A circuit whose
/// <see cref="CircuitOptions.Storage"/> implements this interface commits automatically every
/// <see cref="CircuitOptions.CheckpointInterval"/> steps (through <see cref="CommitAsync"/>),
/// after all workers' shards for those steps are complete: one commit point for the whole
/// circuit. After a crash, rebuild the
/// circuit against the same provider — <see cref="Circuit.Start"/> recovers the checkpoint,
/// <see cref="Circuit.CommittedOffset"/> tells the producer where to resume each source, and
/// replayed records stamped at or below the checkpoint are dropped, making the restart
/// exactly-once from the state's point of view.
/// </summary>
public interface ICheckpointStorageProvider : IStorageProvider
{
    /// <summary>Discards any staged (uncommitted) writes and returns the input offsets of
    /// the last committed checkpoint: one entry per consumed position — an unstamped
    /// input's running count under its plain name, a stamped input's per-partition
    /// positions under one <c>{name}#p{partition}</c> entry each. The provider treats the
    /// keys as opaque strings; the circuit does the encoding. Empty when no checkpoint
    /// exists. The circuit calls this once, on <see cref="Circuit.Start"/>, before any
    /// step runs.</summary>
    IReadOnlyDictionary<string, long> Recover();

    /// <summary>Atomically persists all staged state changes together with
    /// <paramref name="inputOffsets"/>: either the new checkpoint lands in full, or the
    /// previous one remains intact.</summary>
    void Commit(IReadOnlyDictionary<string, long> inputOffsets);

    /// <summary><see cref="Commit"/>, awaited: this is what the circuit calls. A provider
    /// whose commit waits on I/O (a network round trip, a log transaction) overrides it so
    /// that a process running many circuits does not hold a thread per commit in flight.
    /// The default runs <see cref="Commit"/> synchronously. The contract is the same: when
    /// the returned task completes the checkpoint has landed in full, and a failure leaves
    /// the previous one intact. The circuit runs one commit at a time and no step while it
    /// is pending, so an implementation need not guard against stores changing under it.</summary>
    ValueTask CommitAsync(IReadOnlyDictionary<string, long> inputOffsets, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Commit(inputOffsets);
        return default;
    }

    /// <summary>Whether the provider is keeping, beside the stores a circuit was built on,
    /// a second copy of the committed state they were made from, which it lets go of at
    /// the next commit (<see cref="ChangelogStorageProvider"/> does: the bytes, so that
    /// types that have committed nothing have changed nothing). An auto-stepping circuit
    /// that starts on such state and has nothing to read commits once, without a step,
    /// so that an idle pipeline does not hold its state twice for as long as it is idle.
    /// False by default.</summary>
    bool HoldsRestoredCopy => false;

    /// <summary>The state manifest committed with the last checkpoint under
    /// <paramref name="scope"/> (one scope per process sharing the physical store), or null
    /// when none has been committed. The circuit compares it against its own state layout on
    /// <see cref="Circuit.Start"/> and refuses to run on a mismatch, so a rebuilt circuit
    /// whose operators would read the wrong state faults loudly instead of corrupting
    /// silently.</summary>
    string? CommittedManifest(string scope);

    /// <summary>Stages <paramref name="manifest"/> for <paramref name="scope"/>: it is
    /// persisted atomically with the next <see cref="Commit"/>, never on its own — a
    /// committed manifest always describes committed state. <see cref="Recover"/> discards
    /// staged manifests along with staged writes.</summary>
    void StageManifest(string scope, string manifest);
}

public interface IWeightStore<T> where T : notnull
{
    /// <summary>Adds <paramref name="delta"/> to the weight of <paramref name="value"/> and returns the
    /// resulting weight. Implementations must drop entries whose weight reaches zero.</summary>
    long Update(T value, long delta);

    /// <summary>The current weight of <paramref name="value"/>, or 0 if absent.</summary>
    long WeightOf(T value);

    /// <summary>Drops <paramref name="value"/> regardless of its weight (watermark-driven eviction). No-op if absent.</summary>
    void Remove(T value);

    /// <summary>Batched <see cref="Update"/>: applies every entry in order and returns the resulting
    /// weights, aligned with the input. Providers with per-call latency override this to issue one
    /// round trip for the whole batch.</summary>
    long[] UpdateAll(IReadOnlyList<(T Value, long Delta)> entries)
    {
        var results = new long[entries.Count];
        for (var i = 0; i < entries.Count; i++)
            results[i] = Update(entries[i].Value, entries[i].Delta);
        return results;
    }

    /// <summary>Batched <see cref="Update"/> for callers that discard the resulting weights —
    /// the common case, and the one worth not allocating a result array for. Defaults to
    /// <see cref="UpdateAll"/> so a provider whose batching does real work (prefetching a
    /// round trip, say) keeps it without overriding anything.</summary>
    void ApplyAll(IReadOnlyList<(T Value, long Delta)> entries) => UpdateAll(entries);

    /// <summary>Batched <see cref="WeightOf"/>, aligned with the input.</summary>
    long[] WeightOfAll(IReadOnlyList<T> values)
    {
        var results = new long[values.Count];
        for (var i = 0; i < values.Count; i++)
            results[i] = WeightOf(values[i]);
        return results;
    }

    /// <summary>Batched <see cref="Remove"/>.</summary>
    void RemoveAll(IReadOnlyList<T> values)
    {
        foreach (var value in values)
            Remove(value);
    }
}

public interface IIndexedWeightStore<TKey, TValue>
    where TKey : notnull
    where TValue : notnull
{
    /// <summary>Adds <paramref name="delta"/> to the weight of (<paramref name="key"/>, <paramref name="value"/>)
    /// and returns the resulting weight. Entries whose weight reaches zero must be dropped.</summary>
    long Update(TKey key, TValue value, long delta);

    /// <summary>All values (with weights) currently grouped under <paramref name="key"/>. Empty if none.</summary>
    IReadOnlyList<WeightedValue<TValue>> GetGroup(TKey key);

    /// <summary>Drops the entire group for <paramref name="key"/> (watermark-driven eviction). No-op if absent.</summary>
    void RemoveGroup(TKey key);

    /// <summary>Batched <see cref="Update"/>: applies every entry in order and returns the resulting
    /// weights, aligned with the input. Providers with per-call latency override this to issue one
    /// round trip for the whole batch.</summary>
    long[] UpdateAll(IReadOnlyList<(TKey Key, TValue Value, long Delta)> entries)
    {
        var results = new long[entries.Count];
        for (var i = 0; i < entries.Count; i++)
            results[i] = Update(entries[i].Key, entries[i].Value, entries[i].Delta);
        return results;
    }

    /// <summary>Batched <see cref="Update"/> for callers that discard the resulting weights —
    /// the common case, and the one worth not allocating a result array for. Defaults to
    /// <see cref="UpdateAll"/> so a provider whose batching does real work (prefetching a
    /// round trip, say) keeps it without overriding anything.</summary>
    void ApplyAll(IReadOnlyList<(TKey Key, TValue Value, long Delta)> entries) => UpdateAll(entries);

    /// <summary>Batched <see cref="GetGroup"/>, aligned with the input.</summary>
    IReadOnlyList<WeightedValue<TValue>>[] GetGroups(IReadOnlyList<TKey> keys)
    {
        var results = new IReadOnlyList<WeightedValue<TValue>>[keys.Count];
        for (var i = 0; i < keys.Count; i++)
            results[i] = GetGroup(keys[i]);
        return results;
    }

    /// <summary>Batched <see cref="GetGroup"/> that appends into caller-owned buffers instead
    /// of returning a list per group: <paramref name="into"/> receives every requested group's
    /// entries end to end, and <paramref name="bounds"/> receives one entry per key — the
    /// exclusive end offset of that key's run, so group <c>i</c> occupies
    /// <c>[i == 0 ? 0 : bounds[i-1], bounds[i])</c>. Both buffers are appended to, never
    /// cleared. This is the shape the hot operators want (they scan each group once and keep
    /// the buffers across steps); providers that must materialize anyway inherit the default.</summary>
    void GetGroupsInto(IReadOnlyList<TKey> keys, List<WeightedValue<TValue>> into, List<int> bounds)
    {
        foreach (var group in GetGroups(keys))
        {
            for (var i = 0; i < group.Count; i++)
                into.Add(group[i]);
            bounds.Add(into.Count);
        }
    }

    /// <summary>Batched <see cref="RemoveGroup"/>.</summary>
    void RemoveGroups(IReadOnlyList<TKey> keys)
    {
        foreach (var key in keys)
            RemoveGroup(key);
    }
}

public interface IValueStore<TKey, TValue> where TKey : notnull
{
    bool TryGet(TKey key, [MaybeNullWhen(false)] out TValue value);
    void Put(TKey key, TValue value);
    void Remove(TKey key);

    /// <summary>Batched <see cref="TryGet"/>, aligned with the input. <c>Value</c> is default
    /// when <c>Found</c> is false. Providers with per-call latency override this to issue one
    /// round trip for the whole batch.</summary>
    (bool Found, TValue? Value)[] TryGetAll(IReadOnlyList<TKey> keys)
    {
        var results = new (bool, TValue?)[keys.Count];
        for (var i = 0; i < keys.Count; i++)
            results[i] = (TryGet(keys[i], out var value), value);
        return results;
    }

    /// <summary>Batched <see cref="Put"/>.</summary>
    void PutAll(IReadOnlyList<(TKey Key, TValue Value)> pairs)
    {
        foreach (var (key, value) in pairs)
            Put(key, value);
    }

    /// <summary>Batched <see cref="Remove"/>.</summary>
    void RemoveAll(IReadOnlyList<TKey> keys)
    {
        foreach (var key in keys)
            Remove(key);
    }
}

/// <summary>
/// A key → deadline map, ordered by deadline: see
/// <see cref="IStorageProvider.CreateDeadlineStore{TKey}"/>. Deadlines are UTC ticks. Like
/// every store it belongs to one operator on one worker and is only touched by that
/// worker's thread; writes are staged until the provider commits.
/// </summary>
public interface IDeadlineStore<TKey> where TKey : notnull
{
    /// <summary>Holds each of <paramref name="keys"/> that is not held yet, with the
    /// deadline <paramref name="deadlineOf"/> gives for it. A key already held keeps its
    /// deadline, and <paramref name="deadlineOf"/> is not called for it. The list may name
    /// a key more than once. One call per step is the shape: a provider with per-call
    /// latency answers it in one round trip.</summary>
    void AddAll(IReadOnlyList<TKey> keys, Func<TKey, long> deadlineOf);

    /// <summary>Holds <paramref name="key"/> with <paramref name="deadline"/>, replacing
    /// the deadline it had.</summary>
    void Set(TKey key, long deadline);

    /// <summary>Forgets <paramref name="key"/>. No-op if it is not held.</summary>
    void Remove(TKey key);

    /// <summary>Removes every key whose deadline is at or below <paramref name="upTo"/> and
    /// appends it to <paramref name="into"/>, in no particular order. Called once per step
    /// by every operator that holds one, so finding nothing due must cost next to nothing.</summary>
    void TakeDue(long upTo, List<TKey> into);
}
