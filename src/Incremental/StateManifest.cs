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

public enum CircuitStateStoreKind { Weight, IndexedWeight, Value, Deadline }

/// <summary>One state store a circuit's operators would create, as
/// <see cref="CircuitBuilder.DescribeState"/> reports it: its name (which carries the
/// node's state identity, the store's purpose and the worker), the node that owns it and
/// the types it holds. <c>KeyType</c> is null for a weight store, whose entries are the
/// values themselves; a deadline store's value is its deadline, a <c>long</c> of UTC ticks.</summary>
public sealed record CircuitStateStore(string Name, int NodeId, CircuitStateStoreKind Kind, Type? KeyType, Type ValueType);

/// <summary>The state manifest: what is committed with every checkpoint and compared at
/// <see cref="Circuit.Start"/>. One place builds it and one compares it, for the circuit
/// and for a builder that is asked beforehand (<see cref="CircuitBuilder.CheckState"/>).</summary>
internal static class StateManifest
{
    /// <summary>The newest format this version writes: <see cref="Build"/> writes it, and
    /// <see cref="AtLevel"/> turns the manifest into the one a lower format level asks
    /// for. Every format from 6 on has a reader here, kept for good.</summary>
    private const int Format = 8;

    /// <summary>The format before an input's partitions had event times of their own; see
    /// <see cref="Problems"/>. What format level 1 writes.</summary>
    private const int FormatWithoutPartitionTimes = 7;

    /// <summary>The format before event-time state was checkpointed; see <see cref="Problems"/>.</summary>
    private const int FormatWithoutEventTime = 6;

    /// <summary><paramref name="manifest"/> as a node writes it at format
    /// <paramref name="level"/>: the same lines under that level's format. Formats 7 and
    /// 8 differ in the marker's keys only (<see cref="Operators.WatermarkClock.AppendCheckpoint"/>
    /// writes them or not by the same level), so nothing else of the manifest moves.</summary>
    public static string AtLevel(string manifest, int level)
    {
        var end = manifest.IndexOf('\n');
        var line = $"format={FormatLevel.ManifestFormat(level)}";
        return end < 0 || manifest.AsSpan(0, end).SequenceEqual(line) ? manifest : line + manifest[end..];
    }

    /// <summary>The lowest format level that reads <paramref name="manifest"/>.</summary>
    public static int LevelOf(string manifest)
    {
        var end = manifest.IndexOf('\n');
        var line = end < 0 ? manifest.AsSpan() : manifest.AsSpan(0, end);
        return line.StartsWith("format=") && int.TryParse(line["format=".Length..], out var format) && format >= Format
            ? FormatLevel.PartitionTimes
            : FormatLevel.Base;
    }

    public static string Build(
        int workers, int processes, int eventTimeSources,
        IEnumerable<(string Table, string Epoch)> tables, IEnumerable<string> sortedStoreNames)
    {
        var manifest = new System.Text.StringBuilder();
        // format 8: an input's partitions have event times of their own. Where event time
        // is declared on an input and its source names its partitions, the offsets marker
        // holds each one's max event time under `$watermark#s{n}#m{partition}` and
        // whether it is idle under `…#i{partition}`, beside the source's own. A format-7
        // checkpoint has none of these keys and everything else a format-8 one has: it
        // resumes as it is (see Problems) — the clock then has the source's one time
        // until the source names its partitions, which hold every claim back until each
        // has brought a record or is idle, so the watermark stays where it was committed
        // and never regresses. A version that writes format 7 refuses a format-8
        // checkpoint, as it must: it would drop the partitions' times and go on from the
        // newest of them.
        // (format 7: event-time state is checkpointed. The watermark and each source's max
        // event time commit in the offsets marker (`$watermark`, `$watermark#s{n}`), and
        // every eviction, window-close and session deadline lives in a deadline store
        // (`{node}.deadlines.w{n}`). A circuit with event time adds an `eventtime={sources}`
        // line. A format-6 checkpoint of such a circuit has neither: resumed, its watermark
        // would start again from nothing and the keys it holds would have no deadline, so
        // it must not resume. A format-6 checkpoint of a circuit with no event time holds
        // exactly what format 7 would and resumes as it is (see Problems).)
        // (format 6: every checkpoint commits the step sequence under `$step` and a restart
        // resumes it, which is what keeps an acknowledged output's batch sequences stable
        // across a crash. A format-5 checkpoint has none: resuming it would restart the
        // sequence at 1, below what the sinks have already seen.)
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
        manifest.Append($"format={Format}\n");
        manifest.Append($"workers={workers}\n");
        manifest.Append($"processes={processes}\n");
        // The clock's sources are numbered in declaration order, and the committed max
        // event times are theirs by number.
        if (eventTimeSources > 0)
            manifest.Append($"eventtime={eventTimeSources}\n");
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
    /// checkpoint of manifest <paramref name="committed"/> on a node that reads format
    /// level <paramref name="reads"/>; empty when nothing does. The format
    /// <paramref name="current"/> itself carries is not looked at: what the circuit will
    /// write is the deployment's level at each commit.</summary>
    public static List<string> Problems(string committed, string current, int reads = FormatLevel.Current)
    {
        var problems = new List<string>();
        if (committed == current && reads >= FormatLevel.Current)
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

        var committedText = committedFacts.GetValueOrDefault("format");
        var committedFormat = int.TryParse(committedText, System.Globalization.NumberStyles.None, null, out var parsed) ? parsed : -1;
        var readable = FormatLevel.ManifestFormat(reads);
        var eventTime = currentFacts.ContainsKey("eventtime");

        // The readers. A format-8 checkpoint is read by a node of format level 2 and
        // refused below it: such a node would drop the partitions' times and go on from
        // the newest of them. Format 7 → 8 added keys to the offsets marker and changed
        // nothing else: a format-7 checkpoint resumes as it is, and the next commit is of
        // whatever format the deployment writes by then. Format 6 → 7 changed nothing for
        // a circuit without event time, whose checkpoint resumes the same way.
        if (committedFormat > readable)
            problems.Add(
                $"the checkpoint was committed with format={committedText} and this node reads up to format={readable} " +
                $"(format level {reads}): it was built before that format, or is pinned below it, and must be replaced " +
                "with the build that wrote the checkpoint");
        else if (committedFormat == FormatWithoutEventTime)
        {
            if (eventTime)
                problems.Add(
                    "the checkpoint was committed before event-time state was checkpointed (format=6, this version writes " +
                    $"format={FormatWithoutPartitionTimes} and up) and this circuit uses event time: the checkpoint holds neither the watermark nor the " +
                    "deadlines of the windows and keys that were open, so resumed from it late records would reopen closed " +
                    "windows and the open keys would never be evicted. Start it from an empty state store and replay its inputs");
        }
        else if (committedFormat < FormatWithoutEventTime)
            problems.Add($"format changed: the checkpoint was committed with format={committedText}, " +
                         $"this version reads format={FormatWithoutEventTime} to format={Format}");

        foreach (var fact in new[] { "workers", "processes" })
            if (committedFacts.GetValueOrDefault(fact) != currentFacts.GetValueOrDefault(fact))
                problems.Add($"{fact} changed: the checkpoint was committed with {fact}={committedFacts.GetValueOrDefault(fact)}, " +
                             $"this circuit has {fact}={currentFacts.GetValueOrDefault(fact)}");

        // A circuit that gains its first event-time source starts a clock; one that loses
        // its last has none to restore. Between two counts the committed max event times
        // would be handed to the wrong sources.
        if (committedFormat is >= FormatWithoutPartitionTimes and <= Format
            && committedFacts.TryGetValue("eventtime", out var committedSources)
            && currentFacts.TryGetValue("eventtime", out var currentSources)
            && committedSources != currentSources)
            problems.Add($"the number of event-time sources changed: the checkpoint was committed with {committedSources} " +
                         $"WithEventTime source(s), this circuit has {currentSources}, and the committed watermark state is kept per " +
                         "source in declaration order");

        // A table attached since the checkpoint starts at its current version, and one no
        // longer attached is ignored like a removed input. The same name with another epoch
        // is a table that was dropped and recreated: the committed version counts positions
        // in a history that no longer exists.
        foreach (var (table, epoch) in committedTables)
            if (currentTables.TryGetValue(table, out var currentEpoch) && currentEpoch != epoch)
                problems.Add($"shared table '{table}' is not the table the checkpoint read: it was recreated since (epoch {epoch} " +
                             $"then, {currentEpoch} now), so the version this circuit committed does not refer to its history " +
                             "(a table this circuit writes needs a log to outlive the process: open it with SharedTable.OpenAsync)");

        // New stores are fine — they start empty — with one exception: the deadlines of an
        // operator the checkpoint already holds state of. A deadline is set when a key or a
        // record is written, so what that operator held before it had deadlines would have
        // none and would never fall due: resumed, it would keep all of it for good while
        // everything written from then on comes and goes, which looks bounded and is not.
        var committedSet = new HashSet<string>(committedStores, StringComparer.Ordinal);
        var unbounded = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var store in currentStores)
        {
            if (committedSet.Contains(store) || DeadlinesOwner(store) is not { } owner)
                continue;
            if (committedStores.Any(c => c.StartsWith(owner, StringComparison.Ordinal) && c.Length > owner.Length && c[owner.Length] == '.'))
                unbounded.Add(owner);
        }
        foreach (var owner in unbounded)
            problems.Add(
                $"'{owner}' now forgets what it holds on the watermark (EvictWhenFinal, evictWhenFinal, EvictWhen), and the " +
                "checkpoint holds state of it written before it did: that state has no deadlines, so resumed it would never be " +
                "evicted. Start it from an empty state store to adopt the eviction");

        // Committed stores this circuit no longer
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

    /// <summary>The operator a deadline store belongs to — its state name root, as in
    /// <c>{root}.deadlines.w{n}</c> — or null for any other store.</summary>
    private static string? DeadlinesOwner(string store)
    {
        const string marker = ".deadlines.w";
        var at = store.LastIndexOf(marker, StringComparison.Ordinal);
        if (at <= 0 || at + marker.Length >= store.Length)
            return null;
        for (var i = at + marker.Length; i < store.Length; i++)
            if (!char.IsAsciiDigit(store[i]))
                return null;
        return store[..at];
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
    
        public IDeadlineStore<TKey> CreateDeadlineStore<TKey>(string name) where TKey : notnull
        {
            Stores.Add(new CircuitStateStore(name, NodeId, CircuitStateStoreKind.Deadline, typeof(TKey), typeof(long)));
            return _stores.CreateDeadlineStore<TKey>(name);
        }
    }
}
