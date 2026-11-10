using System.Diagnostics.CodeAnalysis;

namespace Incremental.Tables;

/// <summary>
/// A secondary index as the table's writer sees it: something to keep in step with the rows,
/// version by version. The key type is the subclass's business.
/// </summary>
internal abstract class TableIndex<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    /// <summary>Works out what <paramref name="changes"/> do to the index, without touching
    /// it. This is where the user's selector runs, so it is called before the version is
    /// appended to the log: a selector that throws must fail the commit, not leave a durable
    /// version the index could not follow.</summary>
    public abstract void Prepare(IReadOnlyList<TableChange<TKey, TRow>> changes);

    /// <summary>Writes what <see cref="Prepare"/> worked out, as of
    /// <paramref name="version"/>, which is not published yet.</summary>
    public abstract void Apply(long version);

    public abstract void Trim(long floor);
}

/// <summary>
/// A secondary index of a <see cref="SharedTable{TKey,TRow}"/>: index key → the primary keys
/// of the rows under it, versioned exactly as the rows are, so a reader pinned at a version
/// sees the groups and the rows of that same version. A group is an immutable array replaced
/// whole when its membership changes — a row whose index key is unchanged by an update
/// touches nothing here — which suits unique and small groups and is the known limit for
/// very large ones. A row whose selector returns null is not indexed.
/// </summary>
internal sealed class TableIndex<TIndexKey, TKey, TRow>(Func<TRow, TIndexKey?> selector) : TableIndex<TKey, TRow>
    where TIndexKey : notnull
    where TKey : notnull
    where TRow : class
{
    private readonly VersionChains<TIndexKey, TKey[]> _groups = new();

    // What the version being committed does to the groups; filled by Prepare, emptied by Apply.
    private readonly List<(TIndexKey IndexKey, TKey Key, bool Add)> _pending = [];

    /// <summary>Indexes rows that were in the table before the index was declared. Called by
    /// the writer between versions, with <paramref name="version"/> the latest one.</summary>
    public void Build(IEnumerable<KeyValuePair<TKey, TRow>> rows, long version)
    {
        var groups = new Dictionary<TIndexKey, List<TKey>>();
        foreach (var (key, row) in rows)
        {
            if (selector(row) is not { } indexKey)
                continue;
            if (!groups.TryGetValue(indexKey, out var group))
                groups[indexKey] = group = [];
            group.Add(key);
        }
        foreach (var (indexKey, group) in groups)
            _groups.Seed(indexKey, version, group.ToArray());
    }

    /// <summary>The primary keys under <paramref name="indexKey"/> as of
    /// <paramref name="version"/>, which the caller must have pinned; null when there are
    /// none. Lock-free and allocation-free.</summary>
    public TKey[]? Read(TIndexKey indexKey, long version) => _groups.Read(indexKey, version);

    public override void Prepare(IReadOnlyList<TableChange<TKey, TRow>> changes)
    {
        _pending.Clear();
        var comparer = EqualityComparer<TIndexKey>.Default;
        for (var i = 0; i < changes.Count; i++)
        {
            var change = changes[i];
            var had = TryKey(change.Old, out var before);
            var has = TryKey(change.New, out var after);

            // The group holds the row's primary key, not the row: an update that leaves the
            // index key alone leaves the group alone.
            if (had && has && comparer.Equals(before, after))
                continue;
            if (had)
                _pending.Add((before!, change.Key, false));
            if (has)
                _pending.Add((after!, change.Key, true));
        }
    }

    /// <summary>The index key <paramref name="row"/> is under; false for no row, and for a
    /// row the selector leaves unindexed.</summary>
    public bool TryKey(TRow? row, [MaybeNullWhen(false)] out TIndexKey indexKey)
    {
        indexKey = row is null ? default : selector(row);
        return indexKey is not null && row is not null;
    }

    public override void Apply(long version)
    {
        foreach (var (indexKey, key, add) in _pending)
        {
            var group = _groups.Current(indexKey);
            _groups.Write(indexKey, version, add ? With(group, key) : Without(group!, key));
        }
        _pending.Clear();
    }

    public override void Trim(long floor) => _groups.Trim(floor);

    private static TKey[] With(TKey[]? group, TKey key)
    {
        if (group is null)
            return [key];
        var grown = new TKey[group.Length + 1];
        group.CopyTo(grown, 0);
        grown[^1] = key;
        return grown;
    }

    /// <summary>The group without <paramref name="key"/>; null — a deletion — when that
    /// leaves it empty, so an emptied group's chain is trimmed away like a deleted row's.</summary>
    private static TKey[]? Without(TKey[] group, TKey key)
    {
        if (group.Length == 1)
            return null;
        var at = Array.IndexOf(group, key);
        var shrunk = new TKey[group.Length - 1];
        Array.Copy(group, 0, shrunk, 0, at);
        Array.Copy(group, at + 1, shrunk, at, shrunk.Length - at);
        return shrunk;
    }

    /// <summary>Entries in <paramref name="indexKey"/>'s version chain; a test hook.</summary>
    public int ChainLength(TIndexKey indexKey) => _groups.ChainLength(indexKey);
}
