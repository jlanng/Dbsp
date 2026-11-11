using Incremental.Operators;
using Incremental.Storage;

namespace Incremental;

/// <summary>
/// Thrown by <see cref="Circuit.Start"/> when the circuit's state layout does not match the
/// checkpoint it would resume from, and by a checkpointing provider when the committed
/// bytes of a store cannot be read as the types the circuit declares for it. Either way
/// nothing has run and nothing was written: the committed state is exactly what it was,
/// and a circuit that does match it can still be built on the same provider.
/// </summary>
public sealed class CircuitStateMismatchException : InvalidOperationException
{
    public CircuitStateMismatchException(IReadOnlyList<string> problems, string message, Exception? innerException = null)
        : base(message, innerException) =>
        Problems = problems;

    /// <summary>What differs, one entry per finding, without advice.</summary>
    public IReadOnlyList<string> Problems { get; }
}

public enum CircuitStateStoreKind { Weight, IndexedWeight, Value }

/// <summary>One state store a circuit's operators would create, as
/// <see cref="CircuitBuilder.DescribeState"/> reports it: its name (which carries the
/// node's state identity, the store's purpose and the worker), the node that owns it and
/// the types it holds. <c>KeyType</c> is null for a weight store, whose entries are the
/// values themselves.</summary>
public sealed record CircuitStateStore(string Name, int NodeId, CircuitStateStoreKind Kind, Type? KeyType, Type ValueType);

/// <summary>The state manifest: what is committed with every checkpoint and compared at
/// <see cref="Circuit.Start"/>. One place builds it and one compares it, for the circuit
/// and for a builder that is asked beforehand (<see cref="CircuitBuilder.CheckState"/>).</summary>
internal static class StateManifest
{
    public static string Build(int workers, int processes, IEnumerable<(string Table, string Epoch)> tables, IEnumerable<string> sortedStoreNames)
    {
        var manifest = new System.Text.StringBuilder();
        // format 6: every checkpoint commits the step sequence under `$step` and a restart
        // resumes it, which is what keeps an acknowledged output's batch sequences stable
        // across a crash. A format-5 checkpoint has none: resuming it would restart the
        // sequence at 1, below what the sinks have already seen.
        // (format 5: attached shared tables add a `table={name}@{epoch}` line each, and their
        // consumed versions commit in the offsets marker under `{name}#t`. A new line kind,
        // which format-4 validation would have filed under facts and ignored.)
        // (format 4: keyed inputs keep their current rows as circuit state (an input-named
        // upsert store) instead of in the producer's handle; a format-3 checkpoint of a
        // circuit with keyed inputs has no rows to retract against, so it must not resume.)
        // (format 3: the 0.2 API surface rewrite — operator kinds and node layouts changed
        // (groupBy/select naming, windowed groupings' composite WindowKey state, snapshot
        // nodes on table-shaped outputs), so store names moved wholesale. No deployed
        // format-2 checkpoints existed; the bump is hygiene.)
        // (format 2: the committed-offsets marker became per-partition — see OffsetKeys.)
        manifest.Append("format=6\n");
        manifest.Append($"workers={workers}\n");
        manifest.Append($"processes={processes}\n");
        // A table the circuit writes has a line like one it reads: either way the checkpoint
        // holds a version number that means something only in that table's history. (A
        // circuit cannot do both to one table, so the names do not collide.)
        foreach (var (table, epoch) in tables.OrderBy(t => t.Table, StringComparer.Ordinal))
            manifest.Append($"table={table}@{epoch}\n");
        foreach (var store in sortedStoreNames)
            manifest.Append($"store={store}\n");
        return manifest.ToString();
    }

    /// <summary>What stops a circuit of manifest <paramref name="current"/> resuming the
    /// checkpoint of manifest <paramref name="committed"/>; empty when nothing does.</summary>
    public static List<string> Problems(string committed, string current)
    {
        var problems = new List<string>();
        if (committed == current)
            return problems;

        static (Dictionary<string, string> Facts, List<string> Stores, Dictionary<string, string> Tables) Parse(string manifest)
        {
            var facts = new Dictionary<string, string>();
            var stores = new List<string>();
            var tables = new Dictionary<string, string>(); // name → epoch
            foreach (var line in manifest.Split('\n', StringSplitOptions.RemoveEmptyEntries))
            {
                var separator = line.IndexOf('=');
                var (key, value) = (line[..separator], line[(separator + 1)..]);
                if (key == "store")
                    stores.Add(value);
                else if (key == "table")
                    tables[value[..value.LastIndexOf('@')]] = value[(value.LastIndexOf('@') + 1)..];
                else
                    facts[key] = value;
            }
            return (facts, stores, tables);
        }

        var (committedFacts, committedStores, committedTables) = Parse(committed);
        var (currentFacts, currentStores, currentTables) = Parse(current);

        foreach (var fact in new[] { "format", "workers", "processes" })
            if (committedFacts.GetValueOrDefault(fact) != currentFacts.GetValueOrDefault(fact))
                problems.Add($"{fact} changed: the checkpoint was committed with {fact}={committedFacts.GetValueOrDefault(fact)}, " +
                             $"this circuit has {fact}={currentFacts.GetValueOrDefault(fact)}");

        // A table attached since the checkpoint starts at its current version, and one no
        // longer attached is ignored like a removed input. The same name with another epoch
        // is a table that was dropped and recreated: the committed version counts positions
        // in a history that no longer exists.
        foreach (var (table, epoch) in committedTables)
            if (currentTables.TryGetValue(table, out var currentEpoch) && currentEpoch != epoch)
                problems.Add($"shared table '{table}' is not the table the checkpoint read: it was recreated since (epoch {epoch} " +
                             $"then, {currentEpoch} now), so the version this circuit committed does not refer to its history " +
                             "(a table this circuit writes needs a log to outlive the process: open it with SharedTable.OpenAsync)");

        // New stores are fine — they start empty. Committed stores this circuit no longer
        // mints are not: their state would be orphaned, and whatever operator inherited
        // their position would be reading someone else's.
        var currentSet = new HashSet<string>(currentStores, StringComparer.Ordinal);
        var missing = committedStores.Where(s => !currentSet.Contains(s)).ToList();
        if (missing.Count > 0)
        {
            const int shown = 12;
            problems.Add($"committed state with no home in this circuit:\n      " +
                         string.Join("\n      ", missing.Take(shown)) +
                         (missing.Count > shown ? $"\n      … and {missing.Count - shown} more" : ""));
        }
        return problems;
    }

    public static CircuitStateMismatchException Mismatch(List<string> problems) => new(
        problems,
        "This circuit does not match the checkpoint it is resuming from — refusing to start, because a mismatched " +
        "restore silently reads the wrong operators' state:\n  - " + string.Join("\n  - ", problems) + "\n" +
        "Operator state is keyed by circuit position unless named, so inserting, removing, or reordering operators " +
        "renumbers everything downstream. Options: revert the topology or worker-count change; give stateful " +
        "operators stable identities with .Named(\"…\") (before their first checkpoint) so unrelated changes leave " +
        "them alone; or start fresh against an empty state store (for Redis, a new KeyPrefix).");

    /// <summary>A provider that hands out throwaway stores and remembers what was asked
    /// for: how a builder learns its circuit's state layout without building it.</summary>
    internal sealed class Recorder : IStorageProvider
    {
        private readonly InMemoryStorageProvider _stores = new();
        public readonly List<CircuitStateStore> Stores = [];
        public int NodeId;

        public IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull
        {
            Stores.Add(new CircuitStateStore(name, NodeId, CircuitStateStoreKind.Weight, null, typeof(T)));
            return _stores.CreateWeightStore<T>(name);
        }

        public IIndexedWeightStore<TKey, TValue> CreateIndexedWeightStore<TKey, TValue>(string name)
            where TKey : notnull
            where TValue : notnull
        {
            Stores.Add(new CircuitStateStore(name, NodeId, CircuitStateStoreKind.IndexedWeight, typeof(TKey), typeof(TValue)));
            return _stores.CreateIndexedWeightStore<TKey, TValue>(name);
        }

        public IValueStore<TKey, TValue> CreateValueStore<TKey, TValue>(string name) where TKey : notnull
        {
            Stores.Add(new CircuitStateStore(name, NodeId, CircuitStateStoreKind.Value, typeof(TKey), typeof(TValue)));
            return _stores.CreateValueStore<TKey, TValue>(name);
        }
    }
}
