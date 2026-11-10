using System.Buffers;
using System.Buffers.Text;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace Incremental.Storage;

/// <summary>
/// One pipeline's checkpointing storage on a <see cref="ChangelogStorage"/>: state lives on
/// the heap, in the same stores the default provider uses, and every checkpoint appends
/// what changed since the previous one to the changelog as a single transaction — so the
/// state can be rebuilt by another process, or kept warm by one that follows the log.
/// <para>
/// Record layout (see <c>docs/design/durability-and-standby.md</c>). Keys are NUL-separated
/// UTF-8 segments; values start with a format byte (1 = the serializer's output); a
/// tombstone removes the key.
/// </para>
/// <code>
/// pipeline NUL incarnation NUL store NUL entry        entry of a store:
///     weight store          entry = value             → weight
///     indexed weight store  entry = key NUL value     → weight
///     value store           entry = key               → value
/// pipeline NUL incarnation NUL $commit                → sequence, term, offsets, manifests
/// </code>
/// <para>
/// Values are absolute, never increments, so applying a record twice changes nothing and
/// the log may be compacted. The commit record is the last record of each transaction and
/// holds exactly what <see cref="Recover"/> and <see cref="CommittedManifest"/> return.
/// </para>
/// <para>
/// A store's writes go straight to the live state; beside it the store remembers, for each
/// entry touched since the last commit, the value it had then. That is what a commit
/// serializes (the entries touched, not the store) and what <see cref="Recover"/> undoes.
/// </para>
/// <para>
/// A store finds an entry by <c>Equals</c>; the log finds a record by its key's bytes. So
/// an entry's record is always written under one instance — the one that first created it,
/// or the one read back from the log — however many equal instances touch the entry
/// (<see cref="HeldInstances{T}"/>): <c>10.0m</c> and <c>10.00m</c> are one entry and one
/// record. A serializer must give back the same bytes for a key it has just deserialized.
/// </para>
/// </summary>
public sealed class ChangelogStorageProvider : ICheckpointStorageProvider
{
    internal const byte FormatSerializer = 1;
    private static ReadOnlySpan<byte> CommitKey => "$commit"u8;

    private readonly ChangelogStorage _storage;
    private readonly int _partition;
    // Replaced only by UseSerializer, under the lock, while no typed store exists.
    private Utf8StateSerializer _serializer;
    private RecordEncoder _encoder;
    private readonly byte[] _pipelineUtf8;

    // Guards everything below. Taken by the follower per record, and by the circuit when
    // it creates a store, recovers and commits — never by a store operation: once the
    // circuit has started, nothing but its own threads touches the stores.
    private readonly object _lock = new();
    private readonly Dictionary<string, ChangelogStore> _stores = new(StringComparer.Ordinal);

    // Records of stores no circuit has created yet, kept as the bytes they arrived as and
    // handed to the store when it is created: restore runs before the circuit is built,
    // and only the circuit knows the stores' types.
    private readonly Dictionary<string, Dictionary<byte[], byte[]>> _pending = new(StringComparer.Ordinal);

    private readonly Dictionary<string, string> _committedManifests = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _stagedManifests = new(StringComparer.Ordinal);
    private Dictionary<string, long> _committedOffsets = new();
    private long _incarnation; // 0: the changelog holds nothing for this pipeline yet
    private long _commitSequence;
    private long _commitTerm;

    // The store the follower last applied a record to, found again by comparing bytes.
    private byte[] _lastStoreName = [];
    private ChangelogStore? _lastStore;
    private Dictionary<byte[], byte[]>? _lastPending;

    internal ChangelogStorageProvider(ChangelogStorage storage, string pipelineId, int partition, IStateSerializer serializer)
    {
        _storage = storage;
        _partition = partition;
        _serializer = new Utf8StateSerializer(serializer);
        _pipelineUtf8 = Encoding.UTF8.GetBytes(pipelineId);
        _encoder = new RecordEncoder(_serializer, _pipelineUtf8);
        PipelineId = pipelineId;
    }

    /// <summary>The offsets of the last commit — input positions, the step marker and the
    /// versions of the tables the pipeline reads (<c>{table}#t</c>) and writes
    /// (<c>{table}#w</c>) — as a copy. Readable in any role and with no circuit built: on a
    /// follower it is the last commit applied. Empty before the first commit.</summary>
    public IReadOnlyDictionary<string, long> CommittedOffsets
    {
        get
        {
            lock (_lock)
                return new Dictionary<string, long>(_committedOffsets);
        }
    }

    /// <summary>
    /// Forgets the typed stores handed out so far and keeps what they held as the bytes the
    /// changelog holds for them, exactly as if no circuit had been built on this provider
    /// yet. The next circuit creates its stores from those bytes, with whatever key and
    /// value types it declares — which is how a pipeline's state passes from one version
    /// of its code to another (the types of two loads of an assembly are different types),
    /// and what lets the first version's types be unloaded. Writes since the last commit
    /// are undone first. No circuit may be running on this provider, and the circuits built
    /// on it before the call must not be started afterwards. Valid in either role.
    /// </summary>
    public void ReleaseStores()
    {
        lock (_lock)
        {
            if (_stores.Count == 0)
                return;
            var collect = new ReleasingTransaction();
            _encoder.Begin(collect, _partition, 0);
            foreach (var (name, store) in _stores)
            {
                store.Rollback();
                // Bytes that never reached the store (a failed Apply leaves them) stay.
                if (!_pending.TryGetValue(name, out var entries))
                    _pending[name] = entries = new Dictionary<byte[], byte[]>(ByteArrayComparer.Instance);
                collect.Into = entries;
                _encoder.Store(store.NameUtf8);
                store.WriteEntries(_encoder);
            }
            _stores.Clear();
            _lastStore = null;
            _lastPending = null;
            _lastStoreName = [];
        }
    }

    /// <summary>Replaces the serializer the stores created from now on read and write
    /// with. Allowed only while the provider holds no typed store (before the first
    /// circuit, or after <see cref="ReleaseStores"/>): a serializer caches metadata for
    /// every type it has seen, so giving each generation of a pipeline its own lets that
    /// cache go with the generation. It must produce the format the changelog already
    /// holds.</summary>
    public void UseSerializer(IStateSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        lock (_lock)
        {
            if (_stores.Count != 0)
                throw new InvalidOperationException(
                    $"Pipeline '{PipelineId}' has typed stores created with the current serializer; call ReleaseStores first.");
            _serializer = new Utf8StateSerializer(serializer);
            _encoder = new RecordEncoder(_serializer, _pipelineUtf8);
        }
    }

    /// <summary>The pipeline's durable identity; see <see cref="ChangelogStorage.Pipeline"/>.</summary>
    public string PipelineId { get; }

    /// <summary>The changelog partition holding this pipeline's records.</summary>
    public int Partition => _partition;

    /// <summary>The pipeline's current incarnation: 0 until its first commit, then 1, and
    /// one more for every <see cref="StartNewIncarnationAsync"/>.</summary>
    public long Incarnation
    {
        get
        {
            lock (_lock)
                return _incarnation;
        }
    }

    /// <summary>How many checkpoints this incarnation has committed; it continues across
    /// restarts and failovers. On a follower, the last commit applied.</summary>
    public long CommitSequence
    {
        get
        {
            lock (_lock)
                return _commitSequence;
        }
    }

    /// <summary>The leader term the last commit was written under.</summary>
    public long CommitTerm
    {
        get
        {
            lock (_lock)
                return _commitTerm;
        }
    }

    public IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull =>
        GetOrCreate(name, static () => new WeightStore<T>());

    public IIndexedWeightStore<TKey, TValue> CreateIndexedWeightStore<TKey, TValue>(string name)
        where TKey : notnull
        where TValue : notnull =>
        GetOrCreate(name, static () => new IndexedWeightStore<TKey, TValue>());

    public IValueStore<TKey, TValue> CreateValueStore<TKey, TValue>(string name) where TKey : notnull =>
        GetOrCreate(name, static () => new ValueStore<TKey, TValue>());

    private TStore GetOrCreate<TStore>(string name, Func<TStore> factory) where TStore : ChangelogStore
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        if (name.Contains('\0'))
            throw new ArgumentException("A store name cannot contain a NUL character.", nameof(name));
        lock (_lock)
        {
            if (_stores.TryGetValue(name, out var existing))
                return existing as TStore
                    ?? throw new InvalidOperationException($"Store '{name}' was already created with different key/value types.");

            var store = factory();
            store.NameUtf8 = Encoding.UTF8.GetBytes(name);

            // What was restored or followed for this store before it existed. Removed only
            // once all of it has applied: a failure leaves the bytes for another attempt.
            if (_pending.TryGetValue(name, out var pending))
            {
                foreach (var (entry, value) in pending)
                    store.Apply(_serializer, entry, value, tombstone: false);
                _pending.Remove(name);
            }

            _stores[name] = store;
            _lastStore = null;
            _lastPending = null;
            _lastStoreName = [];
            return store;
        }
    }

    /// <summary>Undoes whatever was written to the stores since the last commit and returns
    /// that commit's offsets. Throws on a follower: a standby's circuits are built but not
    /// started until the storage is promoted.</summary>
    public IReadOnlyDictionary<string, long> Recover()
    {
        _storage.RequireWriter(PipelineId);
        lock (_lock)
        {
            foreach (var store in _stores.Values)
                store.Rollback();
            _stagedManifests.Clear();
            return new Dictionary<string, long>(_committedOffsets);
        }
    }

    public void Commit(IReadOnlyDictionary<string, long> inputOffsets) =>
        CommitAsync(inputOffsets).AsTask().GetAwaiter().GetResult();

    /// <summary>
    /// Appends every entry changed since the last commit, then the commit record, as one
    /// changelog transaction, and completes when it is durable. The work is proportional to
    /// what changed, not to what the stores hold. A failure leaves the previous checkpoint
    /// in the log; a <see cref="ChangelogFencedException"/> also ends this process's time
    /// as the writer (<see cref="ChangelogStorage.IsFenced"/>).
    /// </summary>
    public async ValueTask CommitAsync(IReadOnlyDictionary<string, long> inputOffsets, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(inputOffsets);
        cancellationToken.ThrowIfCancellationRequested();
        var writer = _storage.RequireWriter(PipelineId);
        var transaction = writer.BeginTransaction();
        long incarnation, sequence;
        try
        {
            // The circuit runs no step while a commit is pending, so nothing touches the
            // stores between this and the promotion below; the lock is against the
            // provider's other callers (a monitor cannot be held across an await).
            lock (_lock)
            {
                incarnation = Math.Max(_incarnation, 1);
                sequence = _commitSequence + 1;
                _encoder.Begin(transaction, _partition, incarnation);
                foreach (var store in _stores.Values)
                {
                    if (!store.HasChanges)
                        continue;
                    _encoder.Store(store.NameUtf8);
                    store.WriteChanges(_encoder);
                }
                _encoder.Commit(sequence, writer.Term, inputOffsets, _committedManifests, _stagedManifests);
            }

            try
            {
                // Not cancellable: once handed over, the outcome is the log's.
                await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (ChangelogFencedException ex)
            {
                _storage.MarkFenced(ex);
                throw;
            }
        }
        finally
        {
            transaction.Dispose();
        }

        lock (_lock)
        {
            foreach (var store in _stores.Values)
                store.Promote();
            foreach (var (scope, manifest) in _stagedManifests)
                _committedManifests[scope] = manifest;
            _stagedManifests.Clear();
            _committedOffsets = new Dictionary<string, long>(inputOffsets);
            _incarnation = incarnation;
            _commitSequence = sequence;
            _commitTerm = writer.Term;
        }
    }

    public string? CommittedManifest(string scope)
    {
        lock (_lock)
            return _committedManifests.GetValueOrDefault(scope);
    }

    public void StageManifest(string scope, string manifest)
    {
        lock (_lock)
            _stagedManifests[scope] = manifest;
    }

    /// <summary>
    /// Discards the pipeline's state and starts it again from nothing, durably: the
    /// "empty state store" a circuit needs after its layout changed or a table version it
    /// depends on was truncated. One transaction removes every record of the current
    /// incarnation and writes an empty commit record under the next; followers reset when
    /// they read it. The stores already handed out are emptied and stay valid, so a
    /// circuit built against this provider can be started afterwards — but none may be
    /// running while this is called. Leader only. Does nothing if the pipeline has never
    /// committed.
    /// </summary>
    public async Task StartNewIncarnationAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var writer = _storage.RequireWriter(PipelineId);
        var transaction = writer.BeginTransaction();
        long next;
        try
        {
            lock (_lock)
            {
                if (_incarnation == 0)
                    return;
                next = _incarnation + 1;

                _encoder.Begin(transaction, _partition, _incarnation);
                foreach (var store in _stores.Values)
                {
                    // Undone first, so the tombstones cover exactly what the log holds.
                    store.Rollback();
                    _encoder.Store(store.NameUtf8);
                    store.WriteTombstones(_encoder);
                }
                foreach (var (name, entries) in _pending)
                {
                    _encoder.Store(Encoding.UTF8.GetBytes(name));
                    foreach (var entry in entries.Keys)
                        _encoder.RawTombstone(entry);
                }
                _encoder.CommitTombstone();

                _encoder.Begin(transaction, _partition, next);
                _encoder.Commit(0, writer.Term, offsets: null, committed: null, staged: null);
            }

            try
            {
                await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (ChangelogFencedException ex)
            {
                _storage.MarkFenced(ex);
                throw;
            }
        }
        finally
        {
            transaction.Dispose();
        }

        lock (_lock)
        {
            ResetTo(next);
            _commitTerm = writer.Term;
        }
    }

    /// <summary>Empties the pipeline for a new incarnation. Caller holds the lock.</summary>
    private void ResetTo(long incarnation)
    {
        foreach (var store in _stores.Values)
            store.Clear();
        _pending.Clear();
        _committedManifests.Clear();
        _stagedManifests.Clear();
        _committedOffsets = new Dictionary<string, long>();
        _commitSequence = 0;
        _commitTerm = 0;
        _incarnation = incarnation;
        _lastStore = null;
        _lastPending = null;
        _lastStoreName = [];
    }

    // ── the follower ───────────────────────────────────────────────────────────────────

    /// <summary>Applies one record read from the changelog: <paramref name="rest"/> is the
    /// key after <c>pipeline NUL incarnation NUL</c>. Called by the storage's follower, in
    /// log order, only while no circuit on this provider has started.</summary>
    internal void ApplyReplicated(long incarnation, ReadOnlySpan<byte> rest, in ChangelogRecord record)
    {
        lock (_lock)
        {
            // An older incarnation's records are whatever compaction has not yet removed.
            if (incarnation < _incarnation)
                return;
            if (incarnation > _incarnation)
                ResetTo(incarnation);

            var separator = rest.IndexOf((byte)0);
            if (separator < 0)
            {
                if (!rest.SequenceEqual(CommitKey))
                    throw new InvalidDataException(
                        $"Changelog record at offset {record.Offset} of pipeline '{PipelineId}' is neither a store entry nor a commit record.");
                ReadCommit(record);
                return;
            }

            var storeName = rest[..separator];
            var entry = rest[(separator + 1)..];
            if (!storeName.SequenceEqual(_lastStoreName))
            {
                var name = Encoding.UTF8.GetString(storeName);
                _lastStoreName = storeName.ToArray();
                _lastPending = null;
                if (!_stores.TryGetValue(name, out _lastStore) && !_pending.TryGetValue(name, out _lastPending))
                    _pending[name] = _lastPending = new Dictionary<byte[], byte[]>(ByteArrayComparer.Instance);
            }

            if (_lastStore is { } store)
                store.Apply(_serializer, entry, record.Value.Span, record.IsTombstone);
            else if (record.IsTombstone)
                _lastPending!.Remove(entry.ToArray());
            else
                _lastPending![entry.ToArray()] = record.Value.ToArray();
        }
    }

    private void ReadCommit(in ChangelogRecord record)
    {
        // A tombstoned commit record is an incarnation being swept. The next incarnation's
        // commit record follows in the same transaction and resets everything, so there is
        // nothing to do for this one.
        if (record.IsTombstone)
            return;

        _committedManifests.Clear();
        var offsets = new Dictionary<string, long>();

        var reader = new Utf8JsonReader(Payload(record.Value.Span));
        using var document = JsonDocument.ParseValue(ref reader);
        var commit = document.RootElement;
        foreach (var offset in commit.GetProperty("offsets").EnumerateObject())
            offsets[offset.Name] = offset.Value.GetInt64();
        foreach (var manifest in commit.GetProperty("manifests").EnumerateObject())
            _committedManifests[manifest.Name] = manifest.Value.GetString()!;
        _committedOffsets = offsets;
        _commitSequence = commit.GetProperty("sequence").GetInt64();
        _commitTerm = commit.GetProperty("term").GetInt64();
    }

    /// <summary>A record value without its format byte.</summary>
    internal static ReadOnlySpan<byte> Payload(ReadOnlySpan<byte> value)
    {
        if (value.Length == 0 || value[0] != FormatSerializer)
            throw new InvalidDataException(
                value.Length == 0
                    ? "A changelog record has an empty value: every value starts with a format byte."
                    : $"A changelog record has format byte {value[0]}, which this version cannot read (it reads {FormatSerializer}).");
        return value[1..];
    }

    // ── test and diagnostics hooks ─────────────────────────────────────────────────────

    /// <summary>Every entry held, as text, sorted: stores a circuit has created and records
    /// still waiting for theirs render the same, so two providers can be compared whatever
    /// has been built on each. Uncommitted writes are included.</summary>
    internal List<string> DescribeState()
    {
        lock (_lock)
        {
            var lines = new List<string>();
            var tombstones = new CollectingTransaction(lines);
            _encoder.Begin(tombstones, _partition, 0);
            foreach (var store in _stores.Values)
            {
                _encoder.Store(store.NameUtf8);
                store.WriteEntries(_encoder);
            }
            foreach (var (name, entries) in _pending)
                foreach (var (entry, value) in entries)
                    lines.Add($"{name}|{ChangelogStorage.Printable(entry)}={Encoding.UTF8.GetString(Payload(value))}");
            lines.Sort(StringComparer.Ordinal);
            return lines;
        }
    }

    /// <summary>Entries touched since the last commit, across all stores.</summary>
    internal long UncommittedEntries
    {
        get
        {
            lock (_lock)
                return _stores.Values.Sum(s => s.TouchedCount);
        }
    }

    /// <summary>Room in the stores' touched-entry maps, summed.</summary>
    internal long TouchedCapacity
    {
        get
        {
            lock (_lock)
                return _stores.Values.Sum(s => (long)s.TouchedCapacity);
        }
    }

    /// <summary>Instances the stores hold for entries whose equal keys can serialize
    /// differently, summed; 0 when no store's types need any.</summary>
    internal long HeldInstances
    {
        get
        {
            lock (_lock)
                return _stores.Values.Sum(s => (long)s.HeldCount);
        }
    }

    /// <summary>Takes the records <see cref="ReleaseStores"/> has a store write, keyed by
    /// the entry part of the record key.</summary>
    private sealed class ReleasingTransaction : IStateChangelogTransaction
    {
        public Dictionary<byte[], byte[]> Into = null!;

        public void Append(int partition, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            // Past "pipeline NUL incarnation NUL store NUL".
            var rest = key[(key.IndexOf((byte)0) + 1)..];
            rest = rest[(rest.IndexOf((byte)0) + 1)..];
            Into[rest[(rest.IndexOf((byte)0) + 1)..].ToArray()] = value.ToArray();
        }

        public void AppendTombstone(int partition, ReadOnlySpan<byte> key) => throw new NotSupportedException();
        public ValueTask CommitAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public void Dispose() { }
    }

    /// <summary>Renders the records <see cref="DescribeState"/> has the stores write.</summary>
    private sealed class CollectingTransaction(List<string> lines) : IStateChangelogTransaction
    {
        public void Append(int partition, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            // Past "pipeline NUL incarnation NUL"; the store's NUL becomes the bar.
            var rest = key[(key.IndexOf((byte)0) + 1)..];
            rest = rest[(rest.IndexOf((byte)0) + 1)..];
            var separator = rest.IndexOf((byte)0);
            lines.Add($"{Encoding.UTF8.GetString(rest[..separator])}|{ChangelogStorage.Printable(rest[(separator + 1)..])}=" +
                      Encoding.UTF8.GetString(Payload(value)));
        }

        public void AppendTombstone(int partition, ReadOnlySpan<byte> key) => throw new NotSupportedException();
        public ValueTask CommitAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public void Dispose() { }
    }

    private sealed class ByteArrayComparer : IEqualityComparer<byte[]>
    {
        public static readonly ByteArrayComparer Instance = new();

        public bool Equals(byte[]? x, byte[]? y) => x.AsSpan().SequenceEqual(y);

        public int GetHashCode(byte[] bytes)
        {
            var hash = new HashCode();
            hash.AddBytes(bytes);
            return hash.ToHashCode();
        }
    }

    // ── serialization ──────────────────────────────────────────────────────────────────

    /// <summary>The <see cref="IStateSerializer"/> as UTF-8. The JSON serializer is written
    /// to and read from bytes directly — the same text, without a string per value; any
    /// other serializer goes through its strings.</summary>
    internal sealed class Utf8StateSerializer(IStateSerializer serializer)
    {
        private readonly JsonSerializerOptions? _json = (serializer as JsonStateSerializer)?.Options;
        private Utf8JsonWriter? _writer;

        public void Write<T>(IBufferWriter<byte> into, T value)
        {
            if (_json is null)
            {
                Encoding.UTF8.GetBytes(serializer.Serialize(value), into);
                return;
            }

            if (_writer is null)
                _writer = new Utf8JsonWriter(into, new JsonWriterOptions
                {
                    Encoder = _json.Encoder,
                    Indented = _json.WriteIndented,
                    SkipValidation = true,
                });
            else
                _writer.Reset(into);
            JsonSerializer.Serialize(_writer, value, _json);
            _writer.Flush();
        }

        public T Read<T>(ReadOnlySpan<byte> utf8)
        {
            if (_json is null)
                return serializer.Deserialize<T>(Encoding.UTF8.GetString(utf8));
            return JsonSerializer.Deserialize<T>(utf8, _json)
                ?? throw new InvalidOperationException(
                    $"Deserializing state of type {typeof(T).Name} produced null; the stored value is missing or corrupt.");
        }
    }

    /// <summary>A growable byte buffer that can be cut back to a prefix: a record key is
    /// built as a fixed prefix plus the entry, over and over.</summary>
    internal sealed class ByteBuffer : IBufferWriter<byte>
    {
        private byte[] _bytes = new byte[256];

        public int Length { get; set; }

        public ReadOnlySpan<byte> Written => _bytes.AsSpan(0, Length);

        public void Advance(int count) => Length += count;

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            Reserve(sizeHint);
            return _bytes.AsMemory(Length);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            Reserve(sizeHint);
            return _bytes.AsSpan(Length);
        }

        public void Append(ReadOnlySpan<byte> bytes)
        {
            Reserve(bytes.Length);
            bytes.CopyTo(_bytes.AsSpan(Length));
            Length += bytes.Length;
        }

        public void Append(byte value)
        {
            Reserve(1);
            _bytes[Length++] = value;
        }

        private void Reserve(int count)
        {
            if (count < 1)
                count = 1;
            if (_bytes.Length - Length < count)
                Array.Resize(ref _bytes, Math.Max(_bytes.Length * 2, Length + count));
        }
    }

    /// <summary>Builds a commit's records into a transaction. One per provider, reused;
    /// used under the provider's lock.</summary>
    internal sealed class RecordEncoder(Utf8StateSerializer serializer, byte[] pipeline)
    {
        private readonly ByteBuffer _key = new();
        private readonly ByteBuffer _value = new();
        private Utf8JsonWriter? _commitWriter;
        private IStateChangelogTransaction _transaction = null!;
        private int _partition;
        private int _incarnationLength; // of "pipeline NUL incarnation NUL"
        private int _storeLength;       // of that plus "store NUL"

        public void Begin(IStateChangelogTransaction transaction, int partition, long incarnation)
        {
            _transaction = transaction;
            _partition = partition;
            _key.Length = 0;
            _key.Append(pipeline);
            _key.Append(0);
            Utf8Formatter.TryFormat(incarnation, _key.GetSpan(20), out var written);
            _key.Advance(written);
            _key.Append(0);
            _incarnationLength = _storeLength = _key.Length;
        }

        /// <summary>The entries that follow belong to this store.</summary>
        public void Store(byte[] nameUtf8)
        {
            _key.Length = _incarnationLength;
            _key.Append(nameUtf8);
            _key.Append(0);
            _storeLength = _key.Length;
        }

        /// <summary>Starts an entry's key; follow with its parts.</summary>
        public RecordEncoder Entry()
        {
            _key.Length = _storeLength;
            return this;
        }

        public RecordEncoder Part<T>(T value)
        {
            serializer.Write(_key, value);
            return this;
        }

        public RecordEncoder Separator()
        {
            _key.Append(0);
            return this;
        }

        /// <summary>Ends the entry with its weight; a weight of zero is a tombstone.</summary>
        public void Weight(long weight)
        {
            if (weight == 0)
            {
                Tombstone();
                return;
            }
            _value.Length = 0;
            _value.Append(FormatSerializer);
            Utf8Formatter.TryFormat(weight, _value.GetSpan(20), out var written);
            _value.Advance(written);
            _transaction.Append(_partition, _key.Written, _value.Written);
        }

        public void Value<T>(T value)
        {
            _value.Length = 0;
            _value.Append(FormatSerializer);
            serializer.Write(_value, value);
            _transaction.Append(_partition, _key.Written, _value.Written);
        }

        public void Tombstone() => _transaction.AppendTombstone(_partition, _key.Written);

        public void RawTombstone(ReadOnlySpan<byte> entry)
        {
            _key.Length = _storeLength;
            _key.Append(entry);
            Tombstone();
        }

        private void CommitRecordKey()
        {
            _key.Length = _incarnationLength;
            _key.Append(CommitKey);
        }

        public void CommitTombstone()
        {
            CommitRecordKey();
            Tombstone();
        }

        /// <summary>The commit record: last in the transaction, and complete in itself —
        /// compaction keeps only the newest, so it repeats the manifests every time.</summary>
        public void Commit(
            long sequence,
            long term,
            IReadOnlyDictionary<string, long>? offsets,
            Dictionary<string, string>? committed,
            Dictionary<string, string>? staged)
        {
            CommitRecordKey();
            _value.Length = 0;
            _value.Append(FormatSerializer);
            if (_commitWriter is null)
                _commitWriter = new Utf8JsonWriter(_value, new JsonWriterOptions { SkipValidation = true });
            else
                _commitWriter.Reset(_value);
            var json = _commitWriter;
            json.WriteStartObject();
            json.WriteNumber("sequence"u8, sequence);
            json.WriteNumber("term"u8, term);
            json.WriteStartObject("offsets"u8);
            if (offsets is not null)
                foreach (var (name, offset) in offsets)
                    json.WriteNumber(name, offset);
            json.WriteEndObject();
            json.WriteStartObject("manifests"u8);
            if (committed is not null)
                foreach (var (scope, manifest) in committed)
                    if (staged is null || !staged.ContainsKey(scope))
                        json.WriteString(scope, manifest);
            if (staged is not null)
                foreach (var (scope, manifest) in staged)
                    json.WriteString(scope, manifest);
            json.WriteEndObject();
            json.WriteEndObject();
            json.Flush();
            _transaction.Append(_partition, _key.Written, _value.Written);
        }
    }

    // ── the stores ─────────────────────────────────────────────────────────────────────

    /// <summary>What the provider asks of each store, whatever its types.</summary>
    internal abstract class ChangelogStore
    {
        // A touched-entry map keeps its capacity from one commit to the next: a circuit
        // whose every commit touches thousands of entries must not regrow the map each
        // time. Capacity that one large commit left behind (a bootstrap, a replay) is given
        // back, but only after SlackLimit commits in a row at a small fraction of it — the
        // rule, and the numbers, of ZSet.Clear.
        private const int ShrinkFloor = 1024;
        private const int SlackRatio = 16;
        private const int SlackLimit = 64;
        private int _slack;

        public byte[] NameUtf8 = [];

        /// <summary>Entries touched since the last commit.</summary>
        public abstract int TouchedCount { get; }

        /// <summary>Entries the touched-entry map has room for without growing.</summary>
        public abstract int TouchedCapacity { get; }

        /// <summary>Instances held for entries whose equal keys can serialize differently
        /// (<see cref="HeldInstances{T}"/>); 0 for a store that needs none.</summary>
        public abstract int HeldCount { get; }

        public bool HasChanges => TouchedCount > 0;

        /// <summary>Writes a record for every touched entry whose value differs from the
        /// one it had at the last commit.</summary>
        public abstract void WriteChanges(RecordEncoder encoder);

        /// <summary>The commit landed: what is live is what is committed.</summary>
        public abstract void Promote();

        /// <summary>Puts every touched entry back to its value at the last commit.</summary>
        public abstract void Rollback();

        /// <summary>Applies a record read from the log. <paramref name="value"/> still has
        /// its format byte.</summary>
        public abstract void Apply(Utf8StateSerializer serializer, ReadOnlySpan<byte> entry, ReadOnlySpan<byte> value, bool tombstone);

        /// <summary>Writes a tombstone for every entry held.</summary>
        public abstract void WriteTombstones(RecordEncoder encoder);

        /// <summary>Writes a record for every entry held.</summary>
        public abstract void WriteEntries(RecordEncoder encoder);

        public abstract void Clear();

        protected static long ReadWeight(ReadOnlySpan<byte> value)
        {
            var payload = Payload(value);
            if (!Utf8Parser.TryParse(payload, out long weight, out var consumed) || consumed != payload.Length)
                throw new InvalidDataException($"A changelog weight record holds '{Encoding.UTF8.GetString(payload)}', not a number.");
            return weight;
        }

        /// <summary>Empties the touched-entry map after a commit or a rollback.</summary>
        protected void Reset<TKey, TValue>(ref Dictionary<TKey, TValue> touched) where TKey : notnull
        {
            var capacity = touched.EnsureCapacity(0);
            if (capacity < ShrinkFloor || touched.Count >= capacity / SlackRatio)
                _slack = 0;
            else if (++_slack == SlackLimit)
            {
                _slack = 0;
                touched = new Dictionary<TKey, TValue>();
                return;
            }
            touched.Clear();
        }
    }

    private sealed class WeightStore<T> : ChangelogStore, IWeightStore<T> where T : notnull
    {
        private readonly InMemoryStorageProvider.WeightStore<T> _live = new();

        // Value → its weight at the last commit, for every value touched since. The key is
        // the instance the entry's record is written under (see HeldInstances).
        private Dictionary<T, long> _touched = new();
        private readonly HeldInstances<T> _held = new();

        public long Update(T value, long delta)
        {
            var updated = _live.Update(value, delta);
            Touch(value, updated - delta);
            return updated;
        }

        private void Touch(T value, long before)
        {
            if (!_held.Active)
                _touched.TryAdd(value, before);
            else if (!_touched.ContainsKey(value))
                _touched.Add(_held.Hold(value), before);
        }

        public void ApplyAll(IReadOnlyList<(T Value, long Delta)> entries)
        {
            for (var i = 0; i < entries.Count; i++)
                Update(entries[i].Value, entries[i].Delta);
        }

        public long WeightOf(T value) => _live.WeightOf(value);

        public void Remove(T value)
        {
            var before = _live.WeightOf(value);
            if (before == 0)
                return;
            Touch(value, before);
            _live.Remove(value);
        }

        public override int TouchedCount => _touched.Count;
        public override int TouchedCapacity => _touched.EnsureCapacity(0);
        public override int HeldCount => _held.Count;

        public override void WriteChanges(RecordEncoder encoder)
        {
            foreach (var (value, before) in _touched)
            {
                var now = _live.WeightOf(value);
                if (now != before)
                    encoder.Entry().Part(value).Weight(now);
            }
        }

        public override void Promote()
        {
            // An entry that is gone has no record any more, so no instance to remember: the
            // next value to open it is the one its new record is written under.
            if (_held.Active)
                foreach (var value in _touched.Keys)
                    if (_live.WeightOf(value) == 0)
                        _held.Release(value);
            Reset(ref _touched);
        }

        public override void Rollback()
        {
            foreach (var (value, before) in _touched)
            {
                _live.Set(value, before);
                if (before == 0)
                    _held.Release(value);
            }
            Reset(ref _touched);
        }

        public override void Apply(Utf8StateSerializer serializer, ReadOnlySpan<byte> entry, ReadOnlySpan<byte> value, bool tombstone)
        {
            var read = serializer.Read<T>(entry);
            var weight = tombstone ? 0 : ReadWeight(value);
            _live.Set(read, weight);
            if (weight == 0)
                _held.Release(read);
            else
                _held.Hold(read);
        }

        public override void WriteTombstones(RecordEncoder encoder)
        {
            foreach (var value in _live.Entries.Keys)
                encoder.Entry().Part(_held.Of(value)).Tombstone();
        }

        public override void WriteEntries(RecordEncoder encoder)
        {
            foreach (var (value, weight) in _live.Entries)
                encoder.Entry().Part(_held.Of(value)).Weight(weight);
        }

        public override void Clear()
        {
            _live.Clear();
            _held.Clear();
            _touched = new Dictionary<T, long>();
        }
    }

    private sealed class IndexedWeightStore<TKey, TValue> : ChangelogStore, IIndexedWeightStore<TKey, TValue>
        where TKey : notnull
        where TValue : notnull
    {
        private readonly InMemoryStorageProvider.IndexedWeightStore<TKey, TValue> _live = new();

        // (key, value) → its weight at the last commit and its weight now, for every entry
        // touched since. "Now" is kept here because the commit must not look each one up
        // again in a store that has no single-entry read worth the name.
        // The key is the instance the entry's record is written under (see HeldInstances).
        private Dictionary<(TKey Key, TValue Value), (long Before, long Now)> _touched = new();
        private readonly HeldInstances<(TKey Key, TValue Value)> _held = new();
        private readonly List<WeightedValue<TValue>> _group = [];

        public long Update(TKey key, TValue value, long delta)
        {
            var updated = _live.Update(key, value, delta);
            ref var slot = ref Touched(key, value, out var seen);
            if (!seen)
                slot.Before = updated - delta;
            slot.Now = updated;
            return updated;
        }

        private ref (long Before, long Now) Touched(TKey key, TValue value, out bool seen)
        {
            var entry = (key, value);
            if (_held.Active && !_touched.ContainsKey(entry))
                entry = _held.Hold(entry);
            return ref CollectionsMarshal.GetValueRefOrAddDefault(_touched, entry, out seen);
        }

        public void ApplyAll(IReadOnlyList<(TKey Key, TValue Value, long Delta)> entries)
        {
            for (var i = 0; i < entries.Count; i++)
                Update(entries[i].Key, entries[i].Value, entries[i].Delta);
        }

        public IReadOnlyList<WeightedValue<TValue>> GetGroup(TKey key) => _live.GetGroup(key);

        public void GetGroupsInto(IReadOnlyList<TKey> keys, List<WeightedValue<TValue>> into, List<int> bounds) =>
            _live.GetGroupsInto(keys, into, bounds);

        /// <summary>One tombstone per member: the log has a record per entry, and no way to
        /// remove a group in one.</summary>
        public void RemoveGroup(TKey key)
        {
            _group.Clear();
            _live.CopyGroup(key, _group);
            foreach (var member in _group)
            {
                ref var slot = ref Touched(key, member.Value, out var seen);
                if (!seen)
                    slot.Before = member.Weight;
                slot.Now = 0;
            }
            _group.Clear();
            _live.RemoveGroup(key);
        }

        public override int TouchedCount => _touched.Count;
        public override int TouchedCapacity => _touched.EnsureCapacity(0);
        public override int HeldCount => _held.Count;

        public override void WriteChanges(RecordEncoder encoder)
        {
            foreach (var (entry, weights) in _touched)
                if (weights.Now != weights.Before)
                    encoder.Entry().Part(entry.Key).Separator().Part(entry.Value).Weight(weights.Now);
        }

        public override void Promote()
        {
            if (_held.Active)
                foreach (var (entry, weights) in _touched)
                    if (weights.Now == 0)
                        _held.Release(entry);
            Reset(ref _touched);
        }

        public override void Rollback()
        {
            foreach (var (entry, weights) in _touched)
            {
                _live.Set(entry.Key, entry.Value, weights.Before);
                if (weights.Before == 0)
                    _held.Release(entry);
            }
            Reset(ref _touched);
        }

        public override void Apply(Utf8StateSerializer serializer, ReadOnlySpan<byte> entry, ReadOnlySpan<byte> value, bool tombstone)
        {
            var separator = entry.IndexOf((byte)0);
            if (separator < 0)
                throw new InvalidDataException(
                    $"A changelog record for an indexed store has no key/value separator: '{ChangelogStorage.Printable(entry)}'.");
            var read = (serializer.Read<TKey>(entry[..separator]), serializer.Read<TValue>(entry[(separator + 1)..]));
            var weight = tombstone ? 0 : ReadWeight(value);
            _live.Set(read.Item1, read.Item2, weight);
            if (weight == 0)
                _held.Release(read);
            else
                _held.Hold(read);
        }

        public override void WriteTombstones(RecordEncoder encoder)
        {
            foreach (var (key, value, _) in _live.Entries())
            {
                var held = _held.Of((key, value));
                encoder.Entry().Part(held.Key).Separator().Part(held.Value).Tombstone();
            }
        }

        public override void WriteEntries(RecordEncoder encoder)
        {
            foreach (var (key, value, weight) in _live.Entries())
            {
                var held = _held.Of((key, value));
                encoder.Entry().Part(held.Key).Separator().Part(held.Value).Weight(weight);
            }
        }

        public override void Clear()
        {
            _live.Clear();
            _held.Clear();
            _touched = new Dictionary<(TKey, TValue), (long, long)>();
        }
    }

    private sealed class ValueStore<TKey, TValue> : ChangelogStore, IValueStore<TKey, TValue> where TKey : notnull
    {
        private readonly InMemoryStorageProvider.ValueStore<TKey, TValue> _live = new();

        // Key → what it held at the last commit, for every key touched since. The key is
        // the instance the entry's record is written under (see HeldInstances).
        private Dictionary<TKey, (bool Existed, TValue? Value)> _touched = new();
        private readonly HeldInstances<TKey> _held = new();

        public bool TryGet(TKey key, [MaybeNullWhen(false)] out TValue value) => _live.TryGet(key, out value);

        public void Put(TKey key, TValue value)
        {
            Touch(key);
            _live.Put(key, value);
        }

        public void Remove(TKey key)
        {
            Touch(key);
            _live.Remove(key);
        }

        private void Touch(TKey key)
        {
            if (_held.Active && !_touched.ContainsKey(key))
                key = _held.Hold(key);
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(_touched, key, out var seen);
            if (seen)
                return;
            slot.Existed = _live.TryGet(key, out var before);
            slot.Value = before;
        }

        public override int TouchedCount => _touched.Count;
        public override int TouchedCapacity => _touched.EnsureCapacity(0);
        public override int HeldCount => _held.Count;

        public override void WriteChanges(RecordEncoder encoder)
        {
            foreach (var (key, before) in _touched)
            {
                if (_live.TryGet(key, out var now))
                    encoder.Entry().Part(key).Value(now);
                else if (before.Existed)
                    encoder.Entry().Part(key).Tombstone();
            }
        }

        public override void Promote()
        {
            if (_held.Active)
                foreach (var key in _touched.Keys)
                    if (!_live.TryGet(key, out _))
                        _held.Release(key);
            Reset(ref _touched);
        }

        public override void Rollback()
        {
            foreach (var (key, before) in _touched)
            {
                if (before.Existed)
                    _live.Put(key, before.Value!);
                else
                {
                    _live.Remove(key);
                    _held.Release(key);
                }
            }
            Reset(ref _touched);
        }

        public override void Apply(Utf8StateSerializer serializer, ReadOnlySpan<byte> entry, ReadOnlySpan<byte> value, bool tombstone)
        {
            var key = serializer.Read<TKey>(entry);
            if (tombstone)
            {
                _live.Remove(key);
                _held.Release(key);
            }
            else
            {
                _live.Put(key, serializer.Read<TValue>(Payload(value)));
                _held.Hold(key);
            }
        }

        public override void WriteTombstones(RecordEncoder encoder)
        {
            foreach (var key in _live.Entries.Keys)
                encoder.Entry().Part(_held.Of(key)).Tombstone();
        }

        public override void WriteEntries(RecordEncoder encoder)
        {
            foreach (var (key, value) in _live.Entries)
                encoder.Entry().Part(_held.Of(key)).Value(value);
        }

        public override void Clear()
        {
            _live.Entries.Clear();
            _held.Clear();
            _touched = new Dictionary<TKey, (bool, TValue?)>();
        }
    }
}
