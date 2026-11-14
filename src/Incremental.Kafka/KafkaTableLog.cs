using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Confluent.Kafka;
using Incremental.Storage;
using Incremental.Tables;
using Microsoft.Extensions.Logging;

namespace Incremental.Kafka;

/// <summary>
/// A shared table's change log on Kafka: <see cref="ITableLog{TKey,TRow}"/> over two topics
/// written in the same transaction (see <c>docs/design/durability-and-standby.md</c>,
/// <em>Kafka-backed table log</em>). Get one from <see cref="KafkaLog.OpenTableLogAsync"/>.
/// <list type="bullet">
/// <item><c>{prefix}.table.{name}.changes</c> — one partition, never compacted: the version
/// history. A version is one record, or several chunks when it is large; the transaction
/// makes the chunks atomic.</item>
/// <item><c>{prefix}.table.{name}.rows</c> — compacted: one record per live key, a null
/// value for a deleted one, and a <c>$head</c> record per partition rewritten by every
/// version (epoch, version, source offsets, and where the changes topic's recent history
/// starts). Restoring reads this topic only, so it is
/// bounded by the table's size.</item>
/// </list>
/// <para>
/// Keys are found again by their serialized bytes (compaction compares bytes), so the key
/// type must serialize equal values alike; a type that does not (<c>decimal</c>,
/// <c>double</c>, <c>DateTimeOffset</c>, a class with its own equality) is refused when the
/// log is opened.
/// </para>
/// </summary>
public sealed class KafkaTableLog<TKey, TRow> : ITableLog<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    // NUL-led, so it cannot collide with a serialized key (a serializer's output never
    // contains a NUL — the changelog provider's rule).
    private static readonly byte[] HeadKey = "\0$head"u8.ToArray();
    private static readonly JsonSerializerOptions HeadJson = new(JsonSerializerDefaults.Web);
    private static readonly IReadOnlyDictionary<int, long> NoOffsets = new Dictionary<int, long>();

    private readonly KafkaLog _log;
    private readonly string _name;
    private readonly TopicPartition _changes;
    private readonly TopicPartition[] _rows;
    private readonly RecordCodec _codec;
    private readonly OffsetIndex _index = new();

    private string _epoch = "";
    private long _last;
    private long _oldest;

    // True once this instance knows the log's last version for certain: it read the head as
    // the writer, or it appended. Until then the head is read from Kafka when asked.
    private volatile bool _lastIsExact;

    private KafkaTableLog(KafkaLog log, string name, int rowPartitions)
    {
        _log = log;
        _name = name;
        ChangesTopic = ChangesTopicName(log.Options, name);
        RowsTopic = RowsTopicName(log.Options, name);
        _changes = new TopicPartition(ChangesTopic, 0);
        _rows = new TopicPartition[rowPartitions];
        for (var p = 0; p < rowPartitions; p++)
            _rows[p] = new TopicPartition(RowsTopic, p);
        _codec = RecordCodec.For(log.Options.Serializer);
    }

    private static string ChangesTopicName(KafkaLogOptions options, string name) => $"{options.TopicPrefix}.table.{name}.changes";

    private static string RowsTopicName(KafkaLogOptions options, string name) => $"{options.TopicPrefix}.table.{name}.rows";

    internal static async Task<KafkaTableLog<TKey, TRow>> OpenAsync(KafkaLog log, string name, CancellationToken cancellationToken)
    {
        if (!ByteStableEquality.Holds<TKey>())
            throw new NotSupportedException(
                $"Table '{name}' has key type {typeof(TKey).Name}, whose equal values can serialize differently (10.0 and 10.00, " +
                "one instant under two offsets, a hand-written Equals). The rows topic finds a key by its bytes, so a delete " +
                "written under one form would leave the row written under another. Key the table by an integer, a string, a " +
                "Guid, an enum, or a tuple or record of those.");

        var options = log.Options;
        var changesConfig = new Dictionary<string, string>
        {
            ["cleanup.policy"] = "delete",
            ["retention.ms"] = "-1",
            ["retention.bytes"] = "-1",
        };
        foreach (var (key, value) in options.TableChangesTopicConfig)
            changesConfig[key] = value;
        await log.EnsureTopicAsync(ChangesTopicName(options, name), 1, changesConfig, fixedPartitions: true, cancellationToken)
            .ConfigureAwait(false);
        var rowPartitions = await log.EnsureTopicAsync(
            RowsTopicName(options, name), options.TableRowPartitions, KafkaLog.CompactedConfig(options, options.TableRowsTopicConfig),
            fixedPartitions: false, cancellationToken).ConfigureAwait(false);

        var table = new KafkaTableLog<TKey, TRow>(log, name, rowPartitions);
        var asWriter = log.IsWriter;
        var head = await table.ReadHeadAsync(cancellationToken).ConfigureAwait(false);
        var undecidedUntil = Environment.TickCount64 + (long)options.OperationTimeout.TotalMilliseconds;
        while (head is null)
        {
            if (log.IsWriter)
            {
                // Read as the writer, the head is final: every earlier writer's transactions
                // are decided. The read above may have been made as a reader, a moment
                // before this process took the log over — and missed a table the previous
                // writer had just created.
                asWriter = true;
                head = await table.ReadHeadAsync(cancellationToken).ConfigureAwait(false);
                if (head is not null)
                    break;

                // No head, read as the writer. Before this is taken for a new table: every
                // record its partitions hold must be decided (the committed end is the
                // end). Where it is not — a partition whose leader was elected a moment
                // ago and has not caught up — the table may well exist, and a new
                // identity written over it would orphan every row it has.
                if (!await table.RowsDecidedAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (Environment.TickCount64 > undecidedUntil)
                        throw new KafkaLogException(
                            $"Table '{name}': '{table.RowsTopic}' holds records that are neither committed nor aborted after {options.OperationTimeout}, " +
                            "and no head record. It cannot be told whether the table exists.");
                    await Task.Delay(200, cancellationToken).ConfigureAwait(false);
                    head = null;
                    continue;
                }

                // A new table: mint its identity. Version 0, in every partition, so that a
                // restore finds a head wherever it looks.
                head = new Head(Guid.NewGuid().ToString("N"), 0, rowPartitions, new Dictionary<int, long>());
                var commit = new LogCommit();
                table.AddHeads(commit, head);
                await table.CommitAsync(commit).ConfigureAwait(false);
                asWriter = true;
                break;
            }

            // A reader cannot create it: wait for the active to.
            await Task.Delay(200, cancellationToken).ConfigureAwait(false);
            asWriter = log.IsWriter;
            head = await table.ReadHeadAsync(cancellationToken).ConfigureAwait(false);
        }

        if (head.Partitions != rowPartitions)
            throw new KafkaLogException(
                $"Topic '{table.RowsTopic}' has {rowPartitions} partition(s) but the table was created with {head.Partitions}. " +
                "A row's partition is derived from the count, so it can never change.");
        table._epoch = head.Epoch;
        table._last = head.Version;
        table._lastIsExact = asWriter && log.IsWriter;
        await table.RefreshOldestAsync(cancellationToken).ConfigureAwait(false);
        return table;
    }

    /// <summary>The table's name.</summary>
    public string Name => _name;

    /// <summary>The version-history topic: <c>{prefix}.table.{name}.changes</c>.</summary>
    public string ChangesTopic { get; }

    /// <summary>The snapshot topic: <c>{prefix}.table.{name}.rows</c>.</summary>
    public string RowsTopic { get; }

    /// <summary>Partitions of the rows topic.</summary>
    public int RowPartitions => _rows.Length;

    public string Epoch => _epoch;

    /// <summary>
    /// The oldest version the changes topic still holds, as this process last saw it: at
    /// open, after its own <see cref="TrimBeforeAsync"/>, and whenever
    /// <see cref="LastVersionAsync"/> is asked on a reader (a standby's promotion asks). A
    /// trim by another process in between is noticed by the read that runs into it.
    /// </summary>
    public long OldestRetainedVersion => Interlocked.Read(ref _oldest);

    // ── the head ───────────────────────────────────────────────────────────────────────

    /// <summary>The head record. <c>IndexVersion</c> and <c>IndexOffset</c> are a version at
    /// or below this one and the changes-topic offset of its first record, as the writer
    /// knew it — normally the version before. They let a process that has only read the
    /// head seek near the end of the changes topic instead of reading its history.
    /// <c>IndexVersion</c> 0: none.</summary>
    private sealed record Head(
        string Epoch, long Version, int Partitions, Dictionary<int, long> Offsets, long IndexVersion = 0, long IndexOffset = -1);

    private Head Seed(Head head)
    {
        if (head.IndexVersion > 0 && head.IndexOffset >= 0)
            _index.Add(head.IndexVersion, head.IndexOffset);
        return head;
    }

    private void AddHeads(LogCommit commit, Head head)
    {
        var value = JsonSerializer.SerializeToUtf8Bytes(head, HeadJson).Prefixed(KafkaLog.JsonFormat);
        foreach (var partition in _rows)
            commit.Add(partition, HeadKey, value);
    }

    private static Head ParseHead(byte[] value)
    {
        CheckFormat(value);
        return JsonSerializer.Deserialize<Head>(value.AsSpan(1), HeadJson)
            ?? throw new InvalidDataException("A table head record is empty.");
    }

    private static void CheckFormat(ReadOnlySpan<byte> value)
    {
        if (value.Length == 0 || value[0] != KafkaLog.JsonFormat)
            throw new InvalidDataException(
                $"A table log record has format byte {(value.Length == 0 ? "(none)" : value[0].ToString())}; this version reads format {KafkaLog.JsonFormat}.");
    }

    /// <summary>The last committed head, or null for a table nobody has created yet. Every
    /// version ends, in each rows partition, with its head record, so the head is at the
    /// tail of partition 0 and one short read finds it.</summary>
    private async Task<Head?> ReadHeadAsync(CancellationToken cancellationToken)
    {
        var partition = _rows[0];
        var end = await _log.EndOffsetAsync(partition, cancellationToken).ConfigureAwait(false);
        if (end == 0)
            return null;
        var start = await _log.StartOffsetAsync(partition, cancellationToken).ConfigureAwait(false);
        return await _log.ShortReadAsync(consumer =>
        {
            for (long window = 64; ; window *= 16)
            {
                var from = Math.Max(start, end - window);
                Head? head = null;
                _log.AssignForShortRead(consumer, new TopicPartitionOffset(partition, from));
                _log.ReadTo(consumer, end, record =>
                {
                    if (record.Message.Value is { } value && record.Message.Key.AsSpan().SequenceEqual(HeadKey))
                        head = ParseHead(value);
                }, cancellationToken);
                if (head is not null || from == start)
                    return head is null ? null : Seed(head);
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Whether, in every rows partition, the end of committed data is the end of
    /// everything written: nothing is undecided, so what a read finds is all there is.</summary>
    private async Task<bool> RowsDecidedAsync(CancellationToken cancellationToken)
    {
        foreach (var partition in _rows)
        {
            var committed = await _log.EndOffsetAsync(partition, cancellationToken).ConfigureAwait(false);
            var written = await _log.UncommittedEndOffsetAsync(partition, cancellationToken).ConfigureAwait(false);
            if (committed != written)
                return false;
        }
        return true;
    }

    public async ValueTask<long> LastVersionAsync(CancellationToken cancellationToken = default)
    {
        if (_lastIsExact)
            return Interlocked.Read(ref _last);

        // As a reader, or as a writer that has not looked since it took over: ask Kafka.
        // After the takeover's InitTransactions this is final — the old writer's last
        // transaction is decided and its markers written.
        var asWriter = _log.IsWriter;

        // This log was opened on a table that exists, and a table's versions only grow. A
        // read that finds no head, or one older than this process has already read, was
        // answered from a partition whose end is not yet what it will be — its leader was
        // elected a moment ago, after brokers came back. That passes, and until it has the
        // answer must not be believed: a takeover that did would fail (no head), or go on
        // from a version the table has left behind.
        var began = Environment.TickCount64;
        var deadline = began + (long)_log.Options.OperationTimeout.TotalMilliseconds;
        Head? head;
        var reads = 0;
        while (true)
        {
            head = await ReadHeadAsync(cancellationToken).ConfigureAwait(false);
            reads++;
            if (head is not null)
            {
                CheckEpoch(head.Epoch);
                if (head.Version >= Interlocked.Read(ref _last))
                    break;
            }
            if (Environment.TickCount64 > deadline)
                throw new KafkaLogException(head is null
                    ? $"Table '{_name}' has no head record in '{RowsTopic}'."
                    : $"Table '{_name}': the head record in '{RowsTopic}' is at version {head.Version}, and this process has read version {Interlocked.Read(ref _last)}.");
            await Task.Delay(200, cancellationToken).ConfigureAwait(false);
        }
        NoteLast(head.Version);
        if (asWriter)
            _lastIsExact = true;
        var read = Environment.TickCount64;
        await RefreshOldestAsync(cancellationToken).ConfigureAwait(false);
        if (Environment.TickCount64 - began > 250)
            _log.Options.Logger?.LogDebug(
                "Table log {Table}: the last version took {Ms} ms to read: {HeadMs} ms and {Reads} read(s) for the head, {OldestMs} ms for where the changes begin",
                _name, Environment.TickCount64 - began, read - began, reads, Environment.TickCount64 - read);
        return head.Version;
    }

    private void NoteLast(long version)
    {
        long seen;
        while ((seen = Interlocked.Read(ref _last)) < version)
            Interlocked.CompareExchange(ref _last, version, seen);
    }

    private void CheckEpoch(string epoch)
    {
        if (!string.Equals(epoch, _epoch, StringComparison.Ordinal))
            throw new InvalidDataException(
                $"Table '{_name}' was opened at epoch {_epoch} and its log now carries epoch {epoch}: the table has been dropped " +
                "and recreated. Open the log again.");
    }

    // ── append ─────────────────────────────────────────────────────────────────────────

    public async ValueTask AppendAsync(TableVersion<TKey, TRow> version, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(version);
        cancellationToken.ThrowIfCancellationRequested();
        var last = Interlocked.Read(ref _last);
        if (version.Version != last + 1)
            throw new InvalidOperationException(
                $"Table log '{_name}' is at version {last}; the next append must be version {last + 1}, not {version.Version}.");

        var commit = Build(version);
        await CommitAsync(commit).ConfigureAwait(false);
        Interlocked.Exchange(ref _last, version.Version);
        _lastIsExact = true;
        if (Volatile.Read(ref commit.TrackedOffset) is >= 0 and var offset)
            _index.Add(version.Version, offset);
    }

    private async Task CommitAsync(LogCommit commit)
    {
        try
        {
            await _log.CommitAsync(commit).ConfigureAwait(false);
        }
        catch (KafkaLogFencedException ex)
        {
            throw new TableLogFencedException($"Table log '{_name}': {ex.Message}", ex);
        }
    }

    /// <summary>One version as one atomic commit: its change record(s), a row record per
    /// changed key, and the head in every rows partition.</summary>
    private LogCommit Build(TableVersion<TKey, TRow> version)
    {
        var commit = new LogCommit();
        var rows = new List<LogRecord>(version.Changes.Count + _rows.Length);
        var versionKey = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(versionKey, version.Version);

        var chunk = new ArrayBufferWriter<byte>(Math.Min(_log.Options.MaxRecordBytes, 64 * 1024));
        var row = new ArrayBufferWriter<byte>(1024);
        using var chunkWriter = new Utf8JsonWriter(chunk, new JsonWriterOptions { SkipValidation = true });
        using var rowWriter = new Utf8JsonWriter(row, new JsonWriterOptions { SkipValidation = true });
        var chunkIndex = 0;

        void StartChunk()
        {
            chunk.ResetWrittenCount();
            chunk.GetSpan(1)[0] = KafkaLog.JsonFormat;
            chunk.Advance(1);
            chunkWriter.Reset(chunk);
            chunkWriter.WriteStartObject();
            chunkWriter.WriteString("epoch"u8, _epoch);
            chunkWriter.WriteNumber("version"u8, version.Version);
            chunkWriter.WriteNumber("chunk"u8, chunkIndex);
            chunkWriter.WriteStartArray("changes"u8);
        }

        void EndChunk(bool last)
        {
            chunkWriter.WriteEndArray();
            chunkWriter.WriteBoolean("last"u8, last);
            if (last)
            {
                chunkWriter.WriteStartObject("offsets"u8);
                foreach (var (partition, offset) in version.Offsets)
                    chunkWriter.WriteNumber(partition.ToString(), offset);
                chunkWriter.WriteEndObject();
            }
            chunkWriter.WriteEndObject();
            chunkWriter.Flush();
            if (chunkIndex == 0)
                commit.TrackNext(); // the offset the version starts at, for the index
            commit.Add(_changes, versionKey, chunk.WrittenSpan.ToArray());
            chunkIndex++;
        }

        StartChunk();
        foreach (var change in version.Changes)
        {
            if (chunk.WrittenCount + chunkWriter.BytesPending >= _log.Options.MaxRecordBytes)
            {
                EndChunk(last: false);
                StartChunk();
            }

            var keyBytes = _codec.KeyBytes(change.Key);
            var changeStart = chunk.WrittenCount + chunkWriter.BytesPending;
            chunkWriter.WriteStartObject();
            chunkWriter.WritePropertyName("k"u8);
            _codec.WriteKey(chunkWriter, keyBytes);
            if (change.Old is not null)
            {
                chunkWriter.WritePropertyName("o"u8);
                _codec.Write(chunkWriter, change.Old);
            }

            byte[]? rowValue = null;
            if (change.New is not null)
            {
                // Serialized once, written twice: into the change, and as the row record.
                row.ResetWrittenCount();
                row.GetSpan(1)[0] = KafkaLog.JsonFormat;
                row.Advance(1);
                rowWriter.Reset(row);
                rowWriter.WriteStartObject();
                rowWriter.WriteNumber("v"u8, version.Version);
                rowWriter.WritePropertyName("r"u8);
                rowWriter.Flush();
                var before = row.WrittenCount;
                _codec.Write(rowWriter, change.New);
                rowWriter.Flush();
                var serialized = row.WrittenSpan[before..];
                chunkWriter.WritePropertyName("n"u8);
                chunkWriter.WriteRawValue(serialized, skipInputValidation: true);
                rowWriter.WriteEndObject();
                rowWriter.Flush();
                rowValue = row.WrittenSpan.ToArray();
            }
            chunkWriter.WriteEndObject();

            // One change is never split across records, so one that is too large cannot be
            // written at all. Refused here, before anything is produced, so that the
            // message names the row: the broker's refusal would name nothing.
            var changeBytes = chunk.WrittenCount + chunkWriter.BytesPending - changeStart;
            var largest = Math.Max(changeBytes, rowValue?.Length ?? 0);
            if (largest > _log.Options.MaxRecordBytes)
                throw new KafkaLogException(
                    $"Table log '{_name}': the row with key {System.Text.Encoding.UTF8.GetString(keyBytes)} makes a record of {largest} bytes " +
                    $"in version {version.Version}; a record may be at most MaxRecordBytes ({_log.Options.MaxRecordBytes}). Nothing of the " +
                    "version was written.");

            rows.Add(new LogRecord(_rows[PartitionOf(change.Key)], keyBytes, rowValue));
        }
        EndChunk(last: true);

        commit.Records.AddRange(rows);
        var (indexVersion, indexOffset) = _index.Last ?? (0, -1);
        AddHeads(commit, new Head(
            _epoch, version.Version, _rows.Length, new Dictionary<int, long>(version.Offsets), indexVersion, indexOffset));
        return commit;
    }

    private int PartitionOf(TKey key) => (int)(StableHash.Of(key) % (ulong)_rows.Length);

    // ── restore ────────────────────────────────────────────────────────────────────────

    public async ValueTask<TableHead> RestoreAsync(Action<TKey, TRow> row, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(row);
        var asWriter = _log.IsWriter;
        var ends = await _log.EndOffsetsAsync(_rows, cancellationToken).ConfigureAwait(false);

        // Numbers, not "the beginning": a consumer asked for a logical offset before it
        // knows the partition's leader waits half a second before it asks again.
        var starts = new long[_rows.Length];
        for (var p = 0; p < starts.Length; p++)
            starts[p] = await _log.StartOffsetAsync(_rows[p], cancellationToken).ConfigureAwait(false);
        var (rows, head) = await Task.Factory.StartNew(
            () => ReadSnapshot(starts, ends, cancellationToken), cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default)
            .ConfigureAwait(false);
        foreach (var (key, current) in rows)
            row(key, current);
        NoteLast(head.Version);
        if (asWriter && _log.IsWriter)
            _lastIsExact = true;
        return new TableHead(head.Version, head.Offsets);
    }

    /// <summary>Reads the rows topic into a consistent snapshot: every partition from its
    /// start, stopped where <see cref="SnapshotCut"/> says they all stand at one version.</summary>
    private (Dictionary<TKey, TRow> Rows, Head Head) ReadSnapshot(long[] starts, long[] ends, CancellationToken cancellationToken)
    {
        var rows = new Dictionary<TKey, TRow>();
        var cut = new SnapshotCut(ends);
        var paused = new bool[_rows.Length];
        Head? head = null;

        using var consumer = _log.CreateConsumer();
        consumer.Assign(_rows.Select((partition, p) => new TopicPartitionOffset(partition, starts[p])));
        var idleSince = Environment.TickCount64;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var result = consumer.ConsumeAssigned(TimeSpan.FromMilliseconds(100));
            if (result is null)
            {
                if (Environment.TickCount64 - idleSince > (long)_log.Options.OperationTimeout.TotalMilliseconds)
                    throw new TimeoutException(
                        $"Restoring table '{_name}' stalled: no partition of '{RowsTopic}' produced a record or reached version {cut.Target}.");
                continue;
            }
            idleSince = Environment.TickCount64;

            var p = result.Partition.Value;
            if (result.IsPartitionEOF)
            {
                cut.End(p);
            }
            else if (result.Message.Key.AsSpan().SequenceEqual(HeadKey))
            {
                if (result.Message.Value is not { } headValue)
                    continue;
                var read = ParseHead(headValue);
                CheckEpoch(read.Epoch);
                if (head is null || read.Version >= head.Version)
                    head = read;
                cut.Head(p, result.Offset.Value, read.Version);
            }
            else
            {
                var rowKey = _codec.KeyFromBytes<TKey>(result.Message.Key);
                if (result.Message.Value is { } value)
                    rows[rowKey] = ParseRow(value);
                else
                    rows.Remove(rowKey);
                cut.Row(p, result.Offset.Value);
            }

            if (cut.Done)
                return (rows, Seed(head!));
            for (var partition = 0; partition < paused.Length; partition++)
            {
                if (cut.ShouldPause(partition) == paused[partition])
                    continue;
                paused[partition] = !paused[partition];
                if (paused[partition])
                    consumer.Pause([_rows[partition]]);
                else
                    consumer.Resume([_rows[partition]]);
            }
        }
    }

    private TRow ParseRow(byte[] value)
    {
        CheckFormat(value);
        var reader = new Utf8JsonReader(value.AsSpan(1));
        reader.Read();
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            var isRow = reader.ValueTextEquals("r"u8);
            reader.Read();
            if (isRow)
                return _codec.Read<TRow>(ref reader);
            reader.Skip();
        }
        throw new InvalidDataException($"A row record of table '{_name}' carries no row.");
    }

    // ── reading versions ───────────────────────────────────────────────────────────────

    public IAsyncEnumerable<TableVersion<TKey, TRow>> ReadAsync(
        long fromExclusive, long toInclusive, CancellationToken cancellationToken = default) =>
        ReadVersionsAsync(fromExclusive, toInclusive, bounded: true, cancellationToken);

    public IAsyncEnumerable<TableVersion<TKey, TRow>> FollowAsync(long fromExclusive, CancellationToken cancellationToken = default) =>
        ReadVersionsAsync(fromExclusive, long.MaxValue, bounded: false, cancellationToken);

    private async IAsyncEnumerable<TableVersion<TKey, TRow>> ReadVersionsAsync(
        long fromExclusive, long toInclusive, bool bounded, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var next = fromExclusive + 1;
        if (next > toInclusive)
            yield break;
        if (bounded)
        {
            var last = await LastVersionAsync(cancellationToken).ConfigureAwait(false);
            if (toInclusive > last)
                throw new ArgumentOutOfRangeException(nameof(toInclusive), toInclusive, $"Table log '{_name}' ends at version {last}.");
            if (next < OldestRetainedVersion)
                throw LostHistory(next, OldestRetainedVersion);
        }

        var start = await StartNearAsync(next, cancellationToken).ConfigureAwait(false);
        List<TableChange<TKey, TRow>>? changes = null;
        var expectedChunk = 0;
        await foreach (var batch in _log.ReadBatchesAsync(_changes, start, cancellationToken).ConfigureAwait(false))
        {
            foreach (var record in batch.Records)
            {
                if (record.Message.Value is not { } value || record.Message.Key.Length != 8)
                    continue;
                var version = BinaryPrimitives.ReadInt64BigEndian(record.Message.Key);
                if (version < next)
                    continue;
                if (version > next)
                {
                    Interlocked.Exchange(ref _oldest, Math.Max(Interlocked.Read(ref _oldest), version));
                    throw LostHistory(next, version);
                }

                changes ??= [];
                var chunk = ParseChunk(value, changes);
                CheckEpoch(chunk.Epoch);
                if (chunk.Version != version || chunk.Index != expectedChunk)
                    throw new InvalidDataException(
                        $"Table log '{_name}': the record at offset {record.Offset.Value} is chunk {chunk.Index} of version {chunk.Version}; " +
                        $"chunk {expectedChunk} of version {version} was expected.");
                if (chunk.Index == 0)
                    _index.Add(version, record.Offset.Value);
                if (!chunk.Last)
                {
                    expectedChunk++;
                    continue;
                }

                var read = new TableVersion<TKey, TRow>(version, changes, chunk.Offsets ?? NoOffsets);
                changes = null;
                expectedChunk = 0;
                next++;
                NoteLast(version);
                yield return read;
                if (next > toInclusive)
                    yield break;
            }
        }
    }

    /// <summary>Where to start reading for <paramref name="version"/>: the nearest offset
    /// known to be at or before its first record, or failing that the start of what the
    /// topic still holds. A known offset may since have been trimmed away — by this process
    /// or another — so it is never taken below the topic's start.</summary>
    private async Task<Offset> StartNearAsync(long version, CancellationToken cancellationToken)
    {
        // Always a number: see RestoreAsync on what a logical offset costs.
        var low = await _log.StartOffsetAsync(_changes, cancellationToken).ConfigureAwait(false);
        return new Offset(Math.Max(_index.Floor(version) ?? low, low));
    }

    /// <summary>The offset the index would start a read for <paramref name="version"/> at,
    /// or null when it knows none. For tests.</summary>
    internal long? KnownOffsetAtOrBelow(long version) => _index.Floor(version);

    private InvalidOperationException LostHistory(long wanted, long oldest) => new(
        $"Table log '{_name}' no longer retains version {wanted}: its history starts at version {oldest}.");

    private readonly record struct Chunk(string Epoch, long Version, int Index, bool Last, Dictionary<int, long>? Offsets);

    private Chunk ParseChunk(byte[] value, List<TableChange<TKey, TRow>> into)
    {
        CheckFormat(value);
        var reader = new Utf8JsonReader(value.AsSpan(1));
        string epoch = "";
        long version = -1;
        var index = -1;
        var last = false;
        Dictionary<int, long>? offsets = null;

        reader.Read();
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            if (reader.ValueTextEquals("epoch"u8))
            {
                reader.Read();
                epoch = reader.GetString() ?? "";
            }
            else if (reader.ValueTextEquals("version"u8))
            {
                reader.Read();
                version = reader.GetInt64();
            }
            else if (reader.ValueTextEquals("chunk"u8))
            {
                reader.Read();
                index = reader.GetInt32();
            }
            else if (reader.ValueTextEquals("last"u8))
            {
                reader.Read();
                last = reader.GetBoolean();
            }
            else if (reader.ValueTextEquals("changes"u8))
            {
                reader.Read();
                while (reader.Read() && reader.TokenType == JsonTokenType.StartObject)
                {
                    TKey? key = default;
                    var hasKey = false;
                    TRow? before = null, after = null;
                    while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                    {
                        if (reader.ValueTextEquals("k"u8))
                        {
                            reader.Read();
                            key = _codec.ReadKey<TKey>(ref reader);
                            hasKey = true;
                        }
                        else if (reader.ValueTextEquals("o"u8))
                        {
                            reader.Read();
                            before = _codec.Read<TRow>(ref reader);
                        }
                        else if (reader.ValueTextEquals("n"u8))
                        {
                            reader.Read();
                            after = _codec.Read<TRow>(ref reader);
                        }
                        else
                        {
                            reader.Read();
                            reader.Skip();
                        }
                    }
                    if (!hasKey)
                        throw new InvalidDataException($"Table log '{_name}': a change of version {version} has no key.");
                    into.Add(new TableChange<TKey, TRow>(key!, before, after));
                }
            }
            else if (reader.ValueTextEquals("offsets"u8))
            {
                reader.Read();
                offsets = [];
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    var partition = int.Parse(reader.GetString()!);
                    reader.Read();
                    offsets[partition] = reader.GetInt64();
                }
            }
            else
            {
                reader.Read();
                reader.Skip();
            }
        }
        return new Chunk(epoch, version, index, last, offsets);
    }

    // ── retention ──────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Deletes the changes topic's records below <paramref name="version"/>
    /// (<c>DeleteRecords</c>). The rows topic is untouched. This is an admin call, which
    /// Kafka does not fence: a deposed active can still trim. That is harmless as long as
    /// the floor it trims to only ever rises, which is the caller's business.
    /// </summary>
    public async ValueTask TrimBeforeAsync(long version, CancellationToken cancellationToken = default)
    {
        if (version <= OldestRetainedVersion)
            return;
        var last = await LastVersionAsync(cancellationToken).ConfigureAwait(false);
        if (AfterTrimReadsLast is { } hook)
            await hook().ConfigureAwait(false);
        var target = Math.Min(version, last + 1);
        var offset = await LocateAsync(target, cancellationToken).ConfigureAwait(false);
        await _log.DeleteRecordsBeforeAsync(_changes, offset, cancellationToken).ConfigureAwait(false);
        Interlocked.Exchange(ref _oldest, Math.Max(Interlocked.Read(ref _oldest), target));
        _index.TrimBefore(target);
    }

    /// <summary>Called by <see cref="TrimBeforeAsync"/> between reading the last version and
    /// locating the offset to trim to. For tests: appending here is a version committed
    /// while a trim is under way.</summary>
    internal Func<Task>? AfterTrimReadsLast { get; set; }

    /// <summary>The offset of the first record at or above <paramref name="version"/>, or the
    /// end of committed data when there is none. Always found by looking: a version past the
    /// last one this process knows of may have been committed since it looked — appends are
    /// not serialised with trims, and another process may be the writer — and "the end of
    /// the topic" would then lie past that version's records and delete them.</summary>
    private async Task<long> LocateAsync(long version, CancellationToken cancellationToken)
    {
        if (_index.Exact(version) is { } exact)
            return exact;

        var end = await _log.EndOffsetAsync(_changes, cancellationToken).ConfigureAwait(false);
        var start = (await StartNearAsync(version, cancellationToken).ConfigureAwait(false)).Value;
        if (start >= end)
            return end;
        return await _log.ShortReadAsync(consumer =>
        {
            var found = end;
            _log.AssignForShortRead(consumer, new TopicPartitionOffset(_changes, start));
            _log.ReadTo(consumer, end, record =>
            {
                if (record.Message.Key.Length != 8 || BinaryPrimitives.ReadInt64BigEndian(record.Message.Key) < version)
                    return false;
                found = record.Offset.Value;
                return true;
            }, cancellationToken);
            return found;
        }, cancellationToken).ConfigureAwait(false);
    }

    // The changes topic's start offset when the oldest version was last read from it, and
    // the version found there: while the start has not moved, neither has the oldest.
    private long _oldestReadAtStart = -1;
    private long _oldestReadVersion;

    private async Task RefreshOldestAsync(CancellationToken cancellationToken)
    {
        var start = await _log.StartOffsetAsync(_changes, cancellationToken).ConfigureAwait(false);
        var end = await _log.EndOffsetAsync(_changes, cancellationToken).ConfigureAwait(false);
        var oldest = Interlocked.Read(ref _last) + 1;
        if (start < end)
        {
            if (Interlocked.Read(ref _oldestReadAtStart) != start)
            {
                var first = await _log.ShortReadAsync(consumer =>
                {
                    long? version = null;
                    _log.AssignForShortRead(consumer, new TopicPartitionOffset(_changes, start));
                    _log.ReadTo(consumer, end, record =>
                    {
                        if (record.Message.Key.Length != 8)
                            return false;
                        version = BinaryPrimitives.ReadInt64BigEndian(record.Message.Key);
                        return true;
                    }, cancellationToken);
                    return version;
                }, cancellationToken).ConfigureAwait(false);
                if (first is { } read)
                {
                    Interlocked.Exchange(ref _oldestReadVersion, read);
                    Interlocked.Exchange(ref _oldestReadAtStart, start);
                }
                else
                {
                    Interlocked.Exchange(ref _oldestReadAtStart, -1);
                }
            }
            if (Interlocked.Read(ref _oldestReadAtStart) == start)
                oldest = Interlocked.Read(ref _oldestReadVersion);
        }
        Interlocked.Exchange(ref _oldest, oldest);
    }

    /// <summary>Version → offset of the version's first record in the changes topic, for
    /// seeking. Filled by appends and by reads; sparse and bounded (when full, every other
    /// entry is dropped). A miss costs a longer scan, never a wrong answer.</summary>
    private sealed class OffsetIndex
    {
        private const int MaxEntries = 8192;
        private readonly List<(long Version, long Offset)> _entries = [];

        public void Add(long version, long offset)
        {
            lock (_entries)
            {
                if (_entries.Count > 0 && _entries[^1].Version >= version)
                    return;
                if (_entries.Count >= MaxEntries)
                {
                    var kept = 0;
                    for (var i = 0; i < _entries.Count; i += 2)
                        _entries[kept++] = _entries[i];
                    _entries.RemoveRange(kept, _entries.Count - kept);
                }
                _entries.Add((version, offset));
            }
        }

        /// <summary>The newest entry, or null when there is none.</summary>
        public (long Version, long Offset)? Last
        {
            get
            {
                lock (_entries)
                    return _entries.Count > 0 ? _entries[^1] : null;
            }
        }

        public long? Floor(long version)
        {
            lock (_entries)
            {
                var i = Search(version);
                return i >= 0 ? _entries[i].Offset : null;
            }
        }

        public long? Exact(long version)
        {
            lock (_entries)
            {
                var i = Search(version);
                return i >= 0 && _entries[i].Version == version ? _entries[i].Offset : null;
            }
        }

        public void TrimBefore(long version)
        {
            lock (_entries)
                _entries.RemoveAll(entry => entry.Version < version);
        }

        /// <summary>Index of the last entry at or below <paramref name="version"/>, or -1.</summary>
        private int Search(long version)
        {
            int low = 0, high = _entries.Count;
            while (low < high)
            {
                var middle = (low + high) >>> 1;
                if (_entries[middle].Version <= version)
                    low = middle + 1;
                else
                    high = middle;
            }
            return low - 1;
        }
    }
}

/// <summary>How keys and rows become JSON inside the table log's records. The default
/// serializer's JSON is written and read directly as UTF-8; any other serializer goes
/// through its strings, which are embedded as JSON strings.</summary>
internal abstract class RecordCodec
{
    public static RecordCodec For(IStateSerializer serializer) =>
        serializer is JsonStateSerializer json ? new Direct(json.Options) : new ViaText(serializer);

    public abstract void Write<T>(Utf8JsonWriter writer, T value);
    public abstract T Read<T>(ref Utf8JsonReader reader);

    /// <summary>The bytes a key's row record is keyed by.</summary>
    public abstract byte[] KeyBytes<T>(T key);
    public abstract T KeyFromBytes<T>(ReadOnlySpan<byte> bytes);
    public abstract void WriteKey(Utf8JsonWriter writer, ReadOnlySpan<byte> keyBytes);
    public abstract T ReadKey<T>(ref Utf8JsonReader reader);

    private sealed class Direct(JsonSerializerOptions options) : RecordCodec
    {
        public override void Write<T>(Utf8JsonWriter writer, T value) => JsonSerializer.Serialize(writer, value, options);

        public override T Read<T>(ref Utf8JsonReader reader) =>
            JsonSerializer.Deserialize<T>(ref reader, options)
            ?? throw new InvalidDataException($"A table log record holds null where a {typeof(T).Name} was expected.");

        public override byte[] KeyBytes<T>(T key) => JsonSerializer.SerializeToUtf8Bytes(key, options);

        public override T KeyFromBytes<T>(ReadOnlySpan<byte> bytes) =>
            JsonSerializer.Deserialize<T>(bytes, options)
            ?? throw new InvalidDataException($"A table log record's key is null where a {typeof(T).Name} was expected.");

        public override void WriteKey(Utf8JsonWriter writer, ReadOnlySpan<byte> keyBytes) =>
            writer.WriteRawValue(keyBytes, skipInputValidation: true);

        public override T ReadKey<T>(ref Utf8JsonReader reader) => Read<T>(ref reader);
    }

    private sealed class ViaText(IStateSerializer serializer) : RecordCodec
    {
        public override void Write<T>(Utf8JsonWriter writer, T value) => writer.WriteStringValue(serializer.Serialize(value));

        public override T Read<T>(ref Utf8JsonReader reader) => serializer.Deserialize<T>(reader.GetString()!);

        public override byte[] KeyBytes<T>(T key) => System.Text.Encoding.UTF8.GetBytes(serializer.Serialize(key));

        public override T KeyFromBytes<T>(ReadOnlySpan<byte> bytes) =>
            serializer.Deserialize<T>(System.Text.Encoding.UTF8.GetString(bytes));

        public override void WriteKey(Utf8JsonWriter writer, ReadOnlySpan<byte> keyBytes) => writer.WriteStringValue(keyBytes);

        public override T ReadKey<T>(ref Utf8JsonReader reader) => Read<T>(ref reader);
    }
}
