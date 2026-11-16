using System.Runtime.InteropServices;
using Incremental.Runtime;
using Incremental.Tables;

namespace Incremental
{
    /// <summary>
    /// <c>ToTable</c>: a circuit's table-shaped result written into a
    /// <see cref="SharedTable{TKey,TRow}"/> that other circuits join against — a <em>derived
    /// table</em> (<c>docs/design/shared-tables.md</c>, <em>Derived tables</em>).
    /// </summary>
    public static class DerivedTableExtensions
    {
        /// <summary>
        /// Writes this stream into <paramref name="table"/>: the table's rows become exactly
        /// the rows the stream currently holds, keyed by the table's own key selector. The
        /// circuit is the table's single writer from <c>Build()</c> until it is disposed or
        /// faults — <c>table.Writer</c> refuses every call meanwhile — and whatever the table
        /// held before the circuit's first step is replaced by what the circuit computes.
        /// <para>
        /// Each step's changes are collapsed to one upsert or delete per key and committed as
        /// one table version. The stream must be table-shaped: at most one row per key at a
        /// time, an old row retracted in the step that asserts its replacement (the result of
        /// an aggregate, a keyed input, a join whose result is unique per key). A step that
        /// leaves a key with two rows, or retracts a row the table does not hold, faults the
        /// circuit.
        /// </para>
        /// <para>
        /// The table version is written before the circuit's checkpoint commits
        /// (commit after delivery), and the checkpoint records the version it covers. A
        /// circuit that restarts from a checkpoint first takes back whatever it wrote after
        /// that version, then writes the replayed steps, so the table always equals the
        /// circuit's state; readers of the table may see rows step back and forward again. A
        /// table whose writer waits — for its log, or for a reader that has fallen a full tail
        /// behind — holds this circuit's step, and through it the circuit's inputs.
        /// </para>
        /// <para>
        /// A circuit cannot read (<c>builder.Table</c>) a table it writes, directly or through
        /// other derived tables, and a table has one writing circuit; <c>Build()</c> rejects
        /// both. Not available on clustered circuits or inside <c>Recurse</c>.
        /// </para>
        /// </summary>
        public static void ToTable<TKey, TRow>(this ZStream<TRow> stream, SharedTable<TKey, TRow> table)
            where TKey : notnull
            where TRow : class
        {
            ArgumentNullException.ThrowIfNull(stream);
            stream.Builder.AddDerivedTable(stream.NodeId, table);
        }

        /// <summary>Writes a keyed result — an aggregate's, say — into
        /// <paramref name="table"/>, one row per key built by <paramref name="row"/>. The
        /// row's key under the table's key selector should be the result's key; see
        /// <see cref="ToTable{TKey,TRow}(ZStream{TRow}, SharedTable{TKey,TRow})"/>.</summary>
        public static void ToTable<TKey, TValue, TRow>(
            this KeyedZStream<TKey, TValue> stream, SharedTable<TKey, TRow> table, Func<TKey, TValue, TRow> row)
            where TKey : notnull
            where TValue : notnull
            where TRow : class
        {
            ArgumentNullException.ThrowIfNull(stream);
            ArgumentNullException.ThrowIfNull(row);
            stream.Select(kv => row(kv.Key, kv.Value)).ToTable(table);
        }
    }
}

namespace Incremental.Tables
{
    /// <summary>A shared table as the lineage check sees it: a name, and the circuit writing
    /// it, if one is.</summary>
    internal interface ITableNode
    {
        string Name { get; }

        /// <summary>The table's identity in a checkpoint's state manifest.</summary>
        string Epoch { get; }

        /// <summary>Read and written only under <see cref="TableLineage"/>'s lock, except by
        /// the table's writer, which reads it to refuse other callers.</summary>
        TableWriterClaim? WriterClaim { get; set; }
    }

    /// <summary>One circuit's hold on a table as its writer: who it is, and which tables it
    /// reads — the edges the cycle check follows.</summary>
    internal sealed class TableWriterClaim(string circuit, IReadOnlyList<ITableNode> sources)
    {
        /// <summary>The writing circuit, for error messages.</summary>
        public string Circuit { get; } = circuit;

        public IReadOnlyList<ITableNode> Sources { get; } = sources;
    }

    /// <summary>
    /// Which circuit writes which table, across the process. A table has one writer, and a
    /// circuit may not write a table it reads — directly, or through a chain of other derived
    /// tables — because such a loop has no step in which it settles. Claims are taken at
    /// <c>Build()</c>, all of a circuit's at once, so whichever circuit would close a loop is
    /// the one refused.
    /// </summary>
    internal static class TableLineage
    {
        private static readonly object Lock = new();

        /// <summary>Claims every table in <paramref name="writes"/> for a circuit that reads
        /// <paramref name="reads"/>; all or none. Returns the claim for each, in order.</summary>
        public static TableWriterClaim[] Claim(string circuit, IReadOnlyList<ITableNode> reads, IReadOnlyList<ITableNode> writes)
        {
            lock (Lock)
            {
                foreach (var write in writes)
                {
                    if (PathTo(write, reads) is { } path)
                        throw new InvalidOperationException(path.Count == 1
                            ? $"This circuit both reads table '{write.Name}' and writes it (ToTable). A circuit cannot write a table " +
                              "it reads: the table's change would be the circuit's own output, a loop with no step in which it settles."
                            : $"This circuit writes table '{write.Name}' (ToTable) and reads table '{path[0].Name}', which is derived from " +
                              $"it ({string.Join(" → ", path.AsEnumerable().Reverse().Select(t => $"'{t.Name}'"))}). A circuit cannot " +
                              "write a table it reads, directly or through other derived tables.");
                    if (write.WriterClaim is { } held)
                        throw new InvalidOperationException(
                            $"Table '{write.Name}' is already written by {held.Circuit}. A table has one writer: dispose that circuit " +
                            "first, or write a different table.");
                }

                var claims = new TableWriterClaim[writes.Count];
                for (var i = 0; i < writes.Count; i++)
                    writes[i].WriterClaim = claims[i] = new TableWriterClaim(circuit, reads);
                return claims;
            }
        }

        public static void Release(ITableNode table, TableWriterClaim claim)
        {
            lock (Lock)
            {
                if (ReferenceEquals(table.WriterClaim, claim))
                    table.WriterClaim = null;
            }
        }

        /// <summary>A chain of tables from one of <paramref name="from"/> back to
        /// <paramref name="target"/>, each derived from the next, or null if there is none.</summary>
        private static List<ITableNode>? PathTo(ITableNode target, IReadOnlyList<ITableNode> from)
        {
            var visited = new HashSet<ITableNode>(ReferenceEqualityComparer.Instance);
            var path = new List<ITableNode>();
            foreach (var start in from)
                if (Visit(start))
                    return path;
            return null;

            bool Visit(ITableNode table)
            {
                if (!visited.Add(table))
                    return false;
                path.Add(table);
                if (ReferenceEquals(table, target))
                    return true;
                if (table.WriterClaim is { } claim)
                    foreach (var source in claim.Sources)
                        if (Visit(source))
                            return true;
                path.RemoveAt(path.Count - 1);
                return false;
            }
        }
    }

    internal abstract class DerivedTableRegistration : OutputRegistration
    {
        public abstract ITableNode Table { get; }

        /// <summary>Set by <c>Build()</c> once the table is claimed; the port carries it.</summary>
        public TableWriterClaim? Claim { get; set; }
    }

    internal sealed class DerivedTableRegistration<TKey, TRow> : DerivedTableRegistration
        where TKey : notnull
        where TRow : class
    {
        public required SharedTable<TKey, TRow> Target { get; init; }

        public override ITableNode Table => Target;

        public override IOutputPort CreatePort(CircuitOptions options) =>
            new DerivedTablePort<TKey, TRow>(Name, NodeId, Target, Claim!);
    }

    /// <summary>
    /// The port of a derived table (<c>ToTable</c>): an output whose consumer is a shared
    /// table's writer. It is an acknowledged output, so the circuit commits after delivery —
    /// and here delivery and acknowledgement are one thing, the table version committing.
    /// <para>
    /// Per step: the workers' deltas are merged (the stream is not partitioned by the
    /// table's key, so one key's retraction and assertion may come from different workers),
    /// collapsed to one upsert or delete per key, checked against the table's own rows, and
    /// committed as one version.
    /// </para>
    /// <para>
    /// Crash consistency is by position, not by hoping the replay repeats. Every checkpoint
    /// records the table version it covers. A circuit resuming from a checkpoint undoes the
    /// versions written after that one — it reads them back and restores each changed key's
    /// old row — in the same version as its first step's changes, so a replay that cuts the
    /// same step nets out to nothing and one that cuts differently still starts from the
    /// table its state describes. A circuit with no checkpoint starts from empty state, so
    /// its first step replaces whatever the table held.
    /// </para>
    /// </summary>
    internal sealed class DerivedTablePort<TKey, TRow> : IDerivedTablePort
        where TKey : notnull
        where TRow : class
    {
        private enum Recovery : byte { None, Rebuild, Rollback }

        private struct KeyDelta
        {
            public TRow? Retracted;
            public TRow? Asserted;
            public long Live;
        }

        // Scratch that has grown past this is given back after the write: see TrimScratch.
        private const int ScratchCeiling = 65_536;

        private readonly SharedTable<TKey, TRow> _table;
        private readonly TableWriterClaim _claim;
        private readonly int _nodeId;

        // The step's merged delta, staged on the coordinator and consumed by the flush. One
        // step is staged at a time (the circuit awaits a flush before staging again), so
        // this and the scratch below are reused.
        private readonly ZSet<TRow> _merged = new();
        private readonly Dictionary<TKey, KeyDelta> _perKey = [];
        private bool _hasStaged;

        // What the first write has to do before it applies the step, and from where.
        private Recovery _recovery;
        private long _resumeFrom;
        private List<TableVersion<TKey, TRow>>? _undoVersions;
        private TableChangeCoalescer<TKey, TRow>? _undo;

        // The table's version as of this circuit's last write: what a checkpoint records.
        private long _written = -1;
        private bool _checkpointing;

        // Set on the starting thread and cleared by the first step; read by the pump.
        private volatile bool _needsStep;
        private volatile Exception? _failure;

        public DerivedTablePort(string name, int nodeId, SharedTable<TKey, TRow> table, TableWriterClaim claim)
        {
            Name = name;
            _nodeId = nodeId;
            _table = table;
            _claim = claim;
        }

        public string Name { get; }
        public object Handle => _table;
        public int BufferedCount => _hasStaged ? 1 : 0;
        public string TableName => _table.Name;
        public string Epoch => _table.Epoch;
        public bool NeedsStep => _needsStep;

        public void BindSnapshot(Circuit circuit)
        {
        }

        public void Open(bool resuming, long? committedVersion, bool checkpointing)
        {
            _checkpointing = checkpointing;
            if (!resuming)
            {
                // No checkpoint: the circuit's state is empty, so the table it describes is
                // too. Whatever the table holds goes, in the version that brings the first
                // step's rows — a table that already matches loses nothing.
                _recovery = Recovery.Rebuild;
                _table.SetWriterHold(-1);
            }
            else if (committedVersion is not { } version)
            {
                throw new InvalidOperationException(
                    $"This circuit writes table '{_table.Name}' (ToTable), but the checkpoint it is resuming from was taken " +
                    "before it did: the circuit's state already holds rows the table was never given, and only changes flow " +
                    "from here. Start the circuit against an empty state store, so that it computes the table from the start.");
            }
            else
            {
                _table.HoldForWriterResuming(version);
                _recovery = Recovery.Rollback;
                _resumeFrom = _written = version;
            }
            _needsStep = true;
        }

        public void AppendWrittenVersion(Dictionary<string, long> into)
        {
            if (_written >= 0)
                into[OffsetKeys.TableWrite(_table.Name)] = _written;
        }

        public void CheckpointCommitted()
        {
            // Not once another circuit has taken the table over: the hold is then its.
            if (_checkpointing && _written >= 0 && _table.Writer.Owner is var owner && (owner is null || ReferenceEquals(owner, _claim)))
                _table.SetWriterHold(_written);
        }

        public bool Stage(Worker[] workers, long step)
        {
            _needsStep = false;
            _merged.Clear();
            foreach (var worker in workers)
                if (worker.Slots[_nodeId] is ZSet<TRow> { IsEmpty: false } delta)
                    _merged.AddAll(delta);

            // The first step writes even when it changed nothing: the recovery is its work.
            return _hasStaged = _recovery != Recovery.None || !_merged.IsEmpty;
        }

        public async ValueTask PublishAsync(Worker[] workers, long step, CancellationToken cancellationToken)
        {
            if (Stage(workers, step))
                await FlushStagedAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Commits the staged step as a table version. This is where the circuit
        /// waits for the table — for its log, and for a reader a full tail behind — and that
        /// wait is the backpressure a derived table puts on its circuit.</summary>
        public async ValueTask FlushStagedAsync(CancellationToken cancellationToken)
        {
            if (!_hasStaged)
                return;

            var writer = _table.Writer;
            await writer.EnterAsync(_claim, cancellationToken).ConfigureAwait(false);
            try
            {
                var recovering = _recovery != Recovery.None;
                if (recovering)
                    await StageRecoveryAsync(cancellationToken).ConfigureAwait(false);
                StageChanges(writer, recovering);
                _written = await writer.CommitGatedAsync(cancellationToken).ConfigureAwait(false);
                _recovery = Recovery.None;
                _hasStaged = false;
                TrimScratch(writer, recovering);
            }
            catch
            {
                // The step is rejected whole — a key that is not table-shaped part-way
                // through, a commit that was cancelled waiting for room. What it had staged
                // must not stay with the writer: the circuit faults and gives the table up,
                // and the next direct commit would publish half of the rejected step.
                writer.DiscardStaged();
                throw;
            }
            finally
            {
                writer.Exit();
            }
        }

        /// <summary>Stages what turns the table into the one this circuit's state describes,
        /// under the commit gate: nothing — not even a write the previous circuit had in
        /// flight when it faulted — lands between reading the table here and committing.</summary>
        private async ValueTask StageRecoveryAsync(CancellationToken cancellationToken)
        {
            var writer = _table.Writer;
            writer.DiscardStaged();

            if (_recovery == Recovery.Rebuild)
            {
                foreach (var (key, _) in _table.CurrentRows())
                    writer.StageOwned(key, null);
                return;
            }

            // Everything written after the checkpoint, folded to one change per key; its Old
            // is the row the checkpoint's table had.
            var versions = _undoVersions ??= [];
            var undo = _undo ??= new TableChangeCoalescer<TKey, TRow>();
            versions.Clear();
            undo.Clear();
            _table.CollectSince(_resumeFrom, versions, out var logFrom, out var logTo);
            if (logTo > logFrom)
                await foreach (var version in _table.ReadLogAsync(logFrom, logTo, cancellationToken).ConfigureAwait(false))
                    undo.Add(version);
            foreach (var version in versions)
                undo.Add(version);
            foreach (var change in undo.Changes)
                writer.StageOwned(change.Key, change.Old);
            (_undoVersions, _undo) = (null, null);
        }

        /// <summary>Collapses the merged delta to one upsert or delete per key and stages
        /// it, checking each key against the row the table holds — which a derived table,
        /// unlike an external sink, can do. The stream must hold at most one row per key.</summary>
        private void StageChanges(TableWriter<TKey, TRow> writer, bool recovering)
        {
            if (_merged.IsEmpty)
                return;

            var keyOf = _table.KeySelector;
            _perKey.Clear();
            foreach (var (row, weight) in _merged)
            {
                var key = keyOf(row);
                if (RecordErrors.IsNull(key))
                    throw new InvalidOperationException(
                        $"The key selector of table '{_table.Name}' returned null for {row}. Every row written to a table needs a key.");
                ref var delta = ref CollectionsMarshal.GetValueRefOrAddDefault(_perKey, key, out _);
                if (weight > 0)
                {
                    delta.Asserted = row;
                    delta.Live += weight;
                }
                else if (weight < -1 || delta.Retracted is not null)
                    throw NotTableShaped(key, $"a step retracted more than one row for it (one of them {row})");
                else
                    delta.Retracted = row;
            }

            foreach (var (key, delta) in _perKey)
            {
                if (delta.Live > 1)
                    throw NotTableShaped(key, $"a step left it with more than one row (one of them {delta.Asserted})");

                // While the recovery is staged the table's row is the staged one.
                var current = recovering ? writer.StagedOrCurrent(key) : _table.Current(key);
                if (delta.Retracted is { } retracted)
                {
                    if (current is null || !EqualityComparer<TRow>.Default.Equals(current, retracted))
                        throw NotTableShaped(key,
                            $"a step retracted {retracted}, but the table holds {(current is null ? "no row" : current.ToString())} for it");
                }
                else if (current is not null)
                    throw NotTableShaped(key, $"a step asserted {delta.Asserted} while {current} is still there");

                writer.StageOwned(key, delta.Asserted);
            }
        }

        private InvalidOperationException NotTableShaped(TKey key, string what) => new(
            $"The stream written to table '{_table.Name}' is not table-shaped at key '{key}': {what}. A table holds one row per " +
            "key, so the stream must too — retracting a key's old row in the step that asserts its new one, as the result of " +
            "an aggregate or a keyed input does.");

        /// <summary>A table-sized step — the bootstrap, a rebuild — leaves its capacity in
        /// the scratch here and in the writer; clearing a dictionary costs its capacity, not
        /// its count, and the memory would be held for good. A circuit whose every step is
        /// this large pays a reallocation per step instead, which is the cheaper mistake.</summary>
        private void TrimScratch(TableWriter<TKey, TRow> writer, bool recovered)
        {
            if (_perKey.Count > ScratchCeiling)
            {
                _perKey.Clear();
                _perKey.TrimExcess();
                writer.ReleaseScratchAbove(ScratchCeiling);
            }
            else if (recovered)
                writer.ReleaseScratchAbove(ScratchCeiling);
        }

        /// <summary>Nothing waits here: a step counts as acknowledged when its version has
        /// committed, which is when its flush returned, and the circuit awaits the flush
        /// before it asks.</summary>
        public Task WaitAcknowledgedAsync() =>
            _failure is { } failure ? Task.FromException(failure)
            : _hasStaged ? Task.FromException(new InvalidOperationException(
                $"Table '{_table.Name}' has a step staged that was never written; the checkpoint cannot pass it."))
            : Task.CompletedTask;

        public void Complete(Exception? error)
        {
            if (error is not null)
                _failure ??= error;
            _needsStep = false;
            TableLineage.Release(_table, _claim);
        }
    }
}
