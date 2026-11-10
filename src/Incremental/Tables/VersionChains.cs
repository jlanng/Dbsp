using System.Collections.Concurrent;

namespace Incremental.Tables;

/// <summary>
/// The multi-version map under a <see cref="SharedTable{TKey,TRow}"/>: each key maps to a
/// short chain of values, newest first, each stamped with the version that wrote it. A
/// table's rows are one of these and each of its secondary indexes is another, which is
/// what keeps an index exactly as readable-as-of-a-version as the rows it points at.
/// <para>
/// One writer, any number of readers. <see cref="Read"/> is lock-free and allocation-free
/// and may run on any thread, for a version the caller has pinned; every other member
/// belongs to the table's writer. A null value is a deletion.
/// </para>
/// </summary>
internal sealed class VersionChains<TKey, TValue>
    where TKey : notnull
    where TValue : class
{
    private readonly ConcurrentDictionary<TKey, Slot> _heads = new();

    // Keys written by each version, oldest first: what trimming has left to visit. Trimming
    // therefore costs O(changes), never a scan of the map.
    private readonly Queue<(long Version, TKey Key)> _untrimmed = new();

    /// <summary>The value for <paramref name="key"/> as of <paramref name="version"/>, or
    /// null when it had none. The version must be pinned: reading an unpinned one races with
    /// trimming and may miss values.</summary>
    public TValue? Read(TKey key, long version)
    {
        if (!_heads.TryGetValue(key, out var slot))
            return null;

        // Newest first: the first entry at or below the pinned version is the value as of
        // it. Entries above it were written after the pin was taken — possibly by a version
        // that is not published yet, which is why the version is tested before the value is
        // touched.
        while (slot is not null && slot.Version > version)
            slot = Volatile.Read(ref slot.Older);
        return slot is null ? null : Volatile.Read(ref slot.Value);
    }

    /// <summary>The newest value for <paramref name="key"/>, as the writer sees it —
    /// including what the version it is still building has written.</summary>
    public TValue? Current(TKey key) => _heads.TryGetValue(key, out var head) ? head.Value : null;

    /// <summary>Places a value that is not a change: a restored row, or an entry of an
    /// index built over rows already present. Nothing is queued for trimming, so the key
    /// must have no chain yet.</summary>
    public void Seed(TKey key, long version, TValue value) => _heads[key] = new Slot(version, value, null);

    /// <summary>Links <paramref name="value"/> in front of <paramref name="key"/>'s chain as
    /// of <paramref name="version"/>, which must not be published yet: a reader pinned below
    /// it steps over the entry by its version number, so a half-written version is never
    /// visible. A key written twice by one version keeps one entry for it.</summary>
    public void Write(TKey key, long version, TValue? value)
    {
        if (_heads.TryGetValue(key, out var head) && head.Version == version)
        {
            Volatile.Write(ref head.Value, value);
            return;
        }
        _heads[key] = new Slot(version, value, head);
        _untrimmed.Enqueue((version, key));
    }

    /// <summary>Cuts every written key's chain back to the newest entry a reader pinned at
    /// <paramref name="floor"/> or later can still need, and removes keys whose surviving
    /// entry is a deletion.</summary>
    public void Trim(long floor)
    {
        while (_untrimmed.TryPeek(out var pending) && pending.Version <= floor)
        {
            _untrimmed.Dequeue();
            if (!_heads.TryGetValue(pending.Key, out var head))
                continue;

            // The newest entry at or below the floor is the oldest one any reader — now or
            // later, since pins only ever take the latest version — can reach.
            var keeper = head;
            while (keeper is not null && keeper.Version > floor)
                keeper = keeper.Older;
            if (keeper is null)
                continue;
            Volatile.Write(ref keeper.Older, null);

            // A deletion with nothing newer in front of it is the whole chain: drop the key.
            // Conditional on the head being unchanged, though only this writer could change it.
            if (ReferenceEquals(keeper, head) && keeper.Value is null)
                _heads.TryRemove(new KeyValuePair<TKey, Slot>(pending.Key, head));
        }
    }

    /// <summary>Every key with a value as of <paramref name="version"/>, which must be
    /// pinned (or be the writer's own latest). Safe beside the writer: a key it adds
    /// meanwhile has no value at the version and is skipped, and a key trimming removes
    /// meanwhile had none either.</summary>
    public IEnumerable<KeyValuePair<TKey, TValue>> At(long version)
    {
        foreach (var (key, head) in _heads)
        {
            var slot = head;
            while (slot is not null && slot.Version > version)
                slot = Volatile.Read(ref slot.Older);
            if (slot is not null && Volatile.Read(ref slot.Value) is { } value)
                yield return new KeyValuePair<TKey, TValue>(key, value);
        }
    }

    /// <summary>Keys physically held, deletions awaiting a trim included.</summary>
    public int KeyCount => _heads.Count;

    /// <summary>Entries in <paramref name="key"/>'s chain (0 when the key is absent).</summary>
    public int ChainLength(TKey key)
    {
        var length = 0;
        for (var slot = _heads.GetValueOrDefault(key); slot is not null; slot = slot.Older)
            length++;
        return length;
    }

    /// <summary>One version of one key's value; a null <see cref="Value"/> is a deletion.
    /// The value is rewritten only by the version that created the entry, before that
    /// version is published.</summary>
    private sealed class Slot(long version, TValue? value, Slot? older)
    {
        public readonly long Version = version;
        public TValue? Value = value;
        public Slot? Older = older;
    }
}
