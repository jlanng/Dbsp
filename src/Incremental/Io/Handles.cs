using System.Threading.Channels;

namespace Incremental;

/// <summary>One queued input record: a weighted value plus the source (partition, offset)
/// the producer stamped it with, or offset -1 when unstamped — the input then numbers
/// records as it consumes them (partition is ignored).</summary>
/// With <see cref="Rejection"/> set it is a record the source could not turn into a value:
/// it has no value and is on its way to the dead letters (or to fault the circuit).
internal readonly record struct InputEntry<T>(T Value, long Weight, int Partition, long Offset, InputRejection? Rejection = null)
    where T : notnull;

/// <summary>A source record that never became an input value — it would not deserialize,
/// say — with the exception that says why. It travels through the input's channel in the
/// record's place, so that its offset is consumed in order like any other.</summary>
internal sealed class InputRejection(object? record, Exception error)
{
    public object? Record { get; } = record;
    public Exception Error { get; } = error;

    /// <summary>What the drain throws under <see cref="RecordErrorPolicy.Fault"/>.</summary>
    public InvalidOperationException Fault(string inputName, int partition, long offset) => new(
        $"Input '{inputName}' was handed a record its source could not read (partition {partition}, offset {offset}): " +
        $"{Error.Message} Under RecordErrorPolicy.DeadLetter the record is reported on Circuit.DeadLetters and skipped instead.",
        Error);
}

/// <summary>One queued keyed-input command: an upsert of <see cref="Value"/> under
/// <see cref="Key"/>, or a delete of the key (<see cref="Value"/> default), stamped as
/// <see cref="InputEntry{T}"/> is. With <see cref="Error"/> set it is an upsert whose key
/// could not be derived, on its way to the dead letters: it has no key.</summary>
internal readonly record struct KeyedInputEntry<TKey, TValue>(
    TKey Key, bool IsDelete, TValue? Value, int Partition, long Offset, Exception? Error = null,
    InputRejection? Rejection = null)
    where TKey : notnull
    where TValue : notnull;

/// <summary>
/// Producer-side handle to a circuit input. Writes go through a bounded channel: when the
/// circuit cannot keep up, the returned <see cref="ValueTask"/>s suspend until capacity
/// frees up — backpressure is the await, there is nothing else to configure.
///
/// The overloads taking an <c>offset</c> stamp the record with its position in the source
/// it was read from (e.g. a Kafka partition offset); the overloads also taking a
/// <c>partition</c> say which partition of the source that position belongs to, so one
/// input can front a multi-partition source — the overloads without one use partition 0.
/// Offsets must be non-negative and increase monotonically within their partition; a
/// record whose offset is at or below its partition's current position is dropped as a
/// duplicate. With a checkpointing storage provider each partition's position starts at
/// the committed checkpoint after a restart, so a producer can replay every partition
/// from <see cref="Circuit.CommittedOffsets"/> (or earlier) and already-processed records
/// are ignored — exactly-once from the state's point of view.
///
/// An input carries either stamped or unstamped records, never both: mixing them would
/// silently corrupt the replay position, so the handle locks to whichever kind it sends
/// first and throws on the other kind.
/// </summary>
public sealed class InputHandle<T> where T : notnull
{
    private readonly ChannelWriter<InputEntry<T>> _writer;
    private StampingLock _stamping;

    internal InputHandle(ChannelWriter<InputEntry<T>> writer) => _writer = writer;

    /// <summary>Inserts <paramref name="value"/> (weight +1), unstamped.</summary>
    public ValueTask InsertAsync(T value, CancellationToken cancellationToken = default) =>
        SendAsync(value, 1, cancellationToken);

    /// <summary>Inserts <paramref name="value"/> stamped with its source offset (partition 0).</summary>
    public ValueTask InsertAsync(T value, long offset, CancellationToken cancellationToken = default) =>
        SendAsync(value, 1, offset, cancellationToken);

    /// <summary>Inserts <paramref name="value"/> stamped with its source partition and offset.</summary>
    public ValueTask InsertAsync(T value, int partition, long offset, CancellationToken cancellationToken = default) =>
        SendAsync(value, 1, partition, offset, cancellationToken);

    /// <summary>Retracts <paramref name="value"/> (weight -1), unstamped.</summary>
    public ValueTask RemoveAsync(T value, CancellationToken cancellationToken = default) =>
        SendAsync(value, -1, cancellationToken);

    /// <summary>Retracts <paramref name="value"/>, stamped with its source offset (partition 0).</summary>
    public ValueTask RemoveAsync(T value, long offset, CancellationToken cancellationToken = default) =>
        SendAsync(value, -1, offset, cancellationToken);

    /// <summary>Retracts <paramref name="value"/>, stamped with its source partition and offset.</summary>
    public ValueTask RemoveAsync(T value, int partition, long offset, CancellationToken cancellationToken = default) =>
        SendAsync(value, -1, partition, offset, cancellationToken);

    /// <summary>Sends <paramref name="value"/> with an explicit signed <paramref name="weight"/>, unstamped.</summary>
    public ValueTask SendAsync(T value, long weight, CancellationToken cancellationToken = default)
    {
        _stamping.Lock(stamped: false);
        return InputWrites.WriteAsync(_writer, new InputEntry<T>(value, weight, -1, -1), cancellationToken);
    }

    /// <summary>Sends a weighted record stamped with its source offset (partition 0).</summary>
    public ValueTask SendAsync(T value, long weight, long offset, CancellationToken cancellationToken = default) =>
        SendAsync(value, weight, 0, offset, cancellationToken);

    /// <summary>Sends a weighted record stamped with its source partition and offset.</summary>
    public ValueTask SendAsync(T value, long weight, int partition, long offset, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        _stamping.Lock(stamped: true);
        return InputWrites.WriteAsync(_writer, new InputEntry<T>(value, weight, partition, offset), cancellationToken);
    }

    /// <summary>Sends every entry of <paramref name="batch"/>, unstamped, in order. Not atomic:
    /// a step may drain part of the batch before the rest is written.</summary>
    public async ValueTask SendAsync(IEnumerable<WeightedValue<T>> batch, CancellationToken cancellationToken = default)
    {
        _stamping.Lock(stamped: false);
        foreach (var entry in batch)
            await InputWrites.WriteAsync(_writer, new InputEntry<T>(entry.Value, entry.Weight, -1, -1), cancellationToken)
                .ConfigureAwait(false);
    }

    /// <summary>
    /// Reports a source record that could not be turned into a value — one that does not
    /// deserialize, say — in the record's place in the feed. Its position is consumed like
    /// any record's, so the checkpoint covers it and a replay drops it. Under
    /// <see cref="RecordErrorPolicy.DeadLetter"/> it appears on
    /// <see cref="Circuit.DeadLetters"/> with <paramref name="record"/> (whatever the source
    /// has to identify it: topic, partition, offset, the raw bytes) and
    /// <paramref name="error"/>; under <see cref="RecordErrorPolicy.Fault"/> it faults the
    /// circuit, as a record that throws inside an operator does.
    /// </summary>
    public ValueTask RejectAsync(object? record, Exception error, int partition, long offset, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(error);
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        _stamping.Lock(stamped: true);
        return InputWrites.WriteAsync(_writer, new InputEntry<T>(default!, 0, partition, offset, new InputRejection(record, error)), cancellationToken);
    }

    /// <summary>Signals that no more data will arrive on this input. Once every input of a
    /// circuit is complete and drained, its outputs complete too.</summary>
    public void Complete() => _writer.TryComplete();
}

/// <summary>
/// Producer-side handle to a keyed circuit input: upsert/delete-by-key semantics on top of
/// the Z-set delta stream. The circuit keeps the current row per key as checkpointed
/// operator state, so <see cref="UpsertAsync(TValue, CancellationToken)"/> retracts the
/// previous version of the row before asserting the new one — the natural way to feed
/// reference/master data and any stream whose rows are amended in place. Upserting the
/// current row is a no-op, and so is deleting an absent key.
///
/// Because the current rows live in the circuit's state, a keyed input is safe to re-feed
/// after a restart: re-upserting rows the recovered state already holds changes nothing, so
/// a producer may replay its whole table (a keyed input fed that way converges on the
/// replayed rows, though a key deleted at the source while the circuit was down is only
/// retracted if the replay includes its delete). The offset-stamped overloads work as on
/// <see cref="InputHandle{T}"/>: replay a partition from <see cref="Circuit.CommittedOffsets"/>
/// and commands at or below the checkpoint are dropped, so nothing is even re-applied.
///
/// Commands for one key take effect in the order they were sent, the last one within a step
/// winning — so a key needs one logical producer. In a clustered circuit that holds per
/// process: if two processes send commands for the same key within one step, which one wins
/// is unspecified.
/// </summary>
public sealed class KeyedInputHandle<TKey, TValue>
    where TKey : notnull
    where TValue : notnull
{
    private readonly ChannelWriter<KeyedInputEntry<TKey, TValue>> _writer;
    private readonly Func<TValue, TKey> _keySelector;
    private StampingLock _stamping;

    internal KeyedInputHandle(ChannelWriter<KeyedInputEntry<TKey, TValue>> writer, Func<TValue, TKey> keySelector)
    {
        _writer = writer;
        _keySelector = keySelector;
    }

    /// <summary>Inserts the row, or replaces the current row with its key (retraction +
    /// assertion), unstamped.</summary>
    public ValueTask UpsertAsync(TValue value, CancellationToken cancellationToken = default)
    {
        _stamping.Lock(stamped: false);
        return Upsert(value, -1, -1, cancellationToken);
    }

    /// <summary>Set by the circuit when its policy is <see cref="RecordErrorPolicy.DeadLetter"/>:
    /// an upsert whose key selector throws or returns null then travels to the circuit as a
    /// record with no key, which the drain reports on the dead-letter output — in the
    /// record's place in the feed, so its offset is consumed like any other — instead of
    /// the exception reaching the producer. Written once, before the circuit is handed out.</summary>
    internal bool ReportsBadKeys { get; set; }

    private ValueTask Upsert(TValue value, int partition, long offset, CancellationToken cancellationToken)
    {
        if (!ReportsBadKeys)
            return Write(new KeyedInputEntry<TKey, TValue>(_keySelector(value), false, value, partition, offset), cancellationToken);

        TKey key;
        try
        {
            key = RecordErrors.Key(_keySelector(value));
        }
        catch (Exception ex)
        {
            return InputWrites.WriteAsync(
                _writer, new KeyedInputEntry<TKey, TValue>(default!, false, value, partition, offset, ex), cancellationToken);
        }
        return Write(new KeyedInputEntry<TKey, TValue>(key, false, value, partition, offset), cancellationToken);
    }

    /// <summary>Upserts the row, stamped with its source offset (partition 0).</summary>
    public ValueTask UpsertAsync(TValue value, long offset, CancellationToken cancellationToken = default) =>
        UpsertAsync(value, 0, offset, cancellationToken);

    /// <summary>Upserts the row, stamped with its source partition and offset.</summary>
    public ValueTask UpsertAsync(TValue value, int partition, long offset, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        _stamping.Lock(stamped: true);
        return Upsert(value, partition, offset, cancellationToken);
    }

    /// <summary>Retracts the current row for <paramref name="key"/>, if any, unstamped.</summary>
    public ValueTask DeleteAsync(TKey key, CancellationToken cancellationToken = default)
    {
        _stamping.Lock(stamped: false);
        return Write(new KeyedInputEntry<TKey, TValue>(key, true, default, -1, -1), cancellationToken);
    }

    /// <summary>Deletes <paramref name="key"/>, stamped with its source offset (partition 0).</summary>
    public ValueTask DeleteAsync(TKey key, long offset, CancellationToken cancellationToken = default) =>
        DeleteAsync(key, 0, offset, cancellationToken);

    /// <summary>Deletes <paramref name="key"/>, stamped with its source partition and offset.</summary>
    public ValueTask DeleteAsync(TKey key, int partition, long offset, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        _stamping.Lock(stamped: true);
        return Write(new KeyedInputEntry<TKey, TValue>(key, true, default, partition, offset), cancellationToken);
    }

    private ValueTask Write(KeyedInputEntry<TKey, TValue> entry, CancellationToken cancellationToken)
    {
        if (entry.Key is null)
            throw new ArgumentNullException("key", "A keyed input's key cannot be null.");
        return InputWrites.WriteAsync(_writer, entry, cancellationToken);
    }

    /// <inheritdoc cref="InputHandle{T}.RejectAsync"/>
    public ValueTask RejectAsync(object? record, Exception error, int partition, long offset, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(error);
        ArgumentOutOfRangeException.ThrowIfNegative(partition);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        _stamping.Lock(stamped: true);
        return InputWrites.WriteAsync(_writer,
            new KeyedInputEntry<TKey, TValue>(default!, false, default, partition, offset, null, new InputRejection(record, error)), cancellationToken);
    }

    /// <summary>Signals that no more data will arrive on this input; see <see cref="InputHandle{T}.Complete"/>.</summary>
    public void Complete() => _writer.TryComplete();
}

/// <summary>An input handle's stamped/unstamped lock: 0 = no record sent yet, 1 = unstamped,
/// 2 = stamped. Locked by the first send so a producer that mixes the two kinds fails on the
/// spot instead of corrupting the input's replay position. A mutable struct held in a field
/// of its handle — never copied.</summary>
internal struct StampingLock
{
    private int _mode;

    public void Lock(bool stamped)
    {
        var mode = stamped ? 2 : 1;
        var seen = Interlocked.CompareExchange(ref _mode, mode, 0);
        if (seen != 0 && seen != mode)
            throw new InvalidOperationException(
                $"This input already carries {(stamped ? "unstamped" : "offset-stamped")} records, so it cannot accept " +
                $"{(stamped ? "an offset-stamped" : "an unstamped")} one: mixing the two would corrupt the input's replay " +
                "position (exactly-once dedup). Stamp every record of an input with source offsets, or none of them.");
    }
}

internal static class InputWrites
{
    /// <summary>The channel write, with the fault path surfaced as what it is: a faulting
    /// circuit completes each input's channel with a <see cref="CircuitFaultedException"/>,
    /// and this unwraps the resulting channel-closed error so producers — including ones
    /// blocked mid-write on a full channel — see that exception (original fault as its
    /// inner) rather than a bare closed-channel error. A channel completed normally (via
    /// <c>Complete</c> or circuit disposal) still throws
    /// <see cref="ChannelClosedException"/>. The synchronous completion path stays
    /// allocation-free.</summary>
    public static ValueTask WriteAsync<TEntry>(ChannelWriter<TEntry> writer, TEntry entry, CancellationToken cancellationToken)
    {
        var write = writer.WriteAsync(entry, cancellationToken);
        return write.IsCompletedSuccessfully ? write : Awaited(write);

        static async ValueTask Awaited(ValueTask write)
        {
            try
            {
                await write.ConfigureAwait(false);
            }
            catch (ChannelClosedException ex) when (ex.InnerException is CircuitFaultedException fault)
            {
                throw new CircuitFaultedException(fault.InnerException!);
            }
        }
    }
}

/// <summary>
/// Consumer-side handle to a circuit output: a changefeed of Z-set batches, one per step
/// that produced changes. An output is a single bounded channel with exactly one consumer —
/// <see cref="Changes"/> (or <see cref="MaterializeInMemory"/>, which is that consumer) can
/// be claimed once; a second subscription throws instead of silently splitting the feed.
/// A slow consumer stalls the whole circuit via backpressure, deliberately — always run and
/// await your consumer.
/// </summary>
public sealed class OutputHandle<T> where T : notnull
{
    private readonly ChannelReader<ZSet<T>> _reader;
    private int _claimed;

    internal OutputHandle(string name, ChannelReader<ZSet<T>> reader)
    {
        Name = name;
        _reader = reader;
    }

    internal string Name { get; }

    /// <summary>Set by the circuit for bootstrap-enabled (table-shaped) outputs.</summary>
    internal Func<CancellationToken, Task<ZSet<T>>>? SnapshotReader { get; set; }

    /// <summary>Advanced: the output's raw channel, for step-level consumption (one batch
    /// per completed step) and non-blocking emptiness checks. Bypasses the bootstrap batch
    /// and the consumer claim — do not mix with <see cref="Changes"/> or
    /// <see cref="MaterializeInMemory"/>, which read the same channel.</summary>
    public ChannelReader<ZSet<T>> Reader => _reader;

    /// <summary>
    /// The output's changefeed: one batch per step that produced changes, holding every row
    /// the step touched with its signed weight. On a bootstrap-enabled output (a keyed or
    /// windowed result's <c>ToOutput()</c>, by default) the first batch asserts the current
    /// integrated state — empty on a fresh start — so a consumer that maintains state treats
    /// it as a reset; this is what makes consumers correct across restarts and mid-run
    /// subscription, since batches emitted between the last checkpoint and a crash are never
    /// replayed. A plain stream output starts straight into live batches (at-most-once
    /// across a crash). Subscribing before <see cref="Circuit.Start"/> is fine: the bootstrap
    /// read waits for it. The enumeration completes when the circuit disposes and throws
    /// <see cref="CircuitFaultedException"/> if the circuit faults. One consumer only: a
    /// second call throws.
    /// </summary>
    public IAsyncEnumerable<ZSet<T>> Changes(CancellationToken cancellationToken = default)
    {
        Claim();
        return ChangesCore(cancellationToken);
    }

    private async IAsyncEnumerable<ZSet<T>> ChangesCore(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // The snapshot read discards batches buffered before it — they are contained in it.
        if (SnapshotReader is { } bootstrap)
            yield return await bootstrap(cancellationToken).ConfigureAwait(false);

        await foreach (var batch in _reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            yield return batch;
    }

    /// <summary>
    /// Maintains the output's integrated result in memory and becomes the output's single
    /// consumer. Requires a bootstrap-enabled output (a keyed or windowed result's
    /// <c>ToOutput()</c>) — the automatic resync is what makes the view correct across
    /// restarts and mid-run attachment. The cost is in the name: the view holds the full
    /// live result set on the heap — fine for windowed/evicted results and small
    /// dimensions; for an unbounded keyspace, make it a deliberate decision.
    /// </summary>
    public InMemoryView<T> MaterializeInMemory()
    {
        if (SnapshotReader is null)
            throw new InvalidOperationException(
                $"Output '{Name}' is a plain changefeed with no current state to materialize. " +
                "MaterializeInMemory needs a table-shaped output: call ToOutput() on a keyed or windowed " +
                "result (with its default bootstrap), not on a plain stream.");
        Claim();
        return new InMemoryView<T>(ChangesCore);
    }

    private void Claim()
    {
        if (Interlocked.Exchange(ref _claimed, 1) != 0)
            throw new InvalidOperationException(
                $"Output '{Name}' already has a consumer. An output is a single channel — a second subscription " +
                "would silently split the feed. Declare a second output on the same stream to consume it twice.");
    }
}

/// <summary>
/// One step's changes on an acknowledged output: every row the step touched, with its
/// signed weight, and the <see cref="Sequence"/> of the step that produced them.
/// </summary>
public readonly struct OutputBatch<T> where T : notnull
{
    private readonly AcknowledgedOutputHandle<T> _owner;

    internal OutputBatch(AcknowledgedOutputHandle<T> owner, long sequence, ZSet<T> changes)
    {
        _owner = owner;
        Sequence = sequence;
        Changes = changes;
    }

    /// <summary>The sequence number of the step that produced this batch. Sequences rise
    /// by one per step, are committed with every checkpoint and continue from the committed
    /// value after a restart — so they increase strictly along one run, steps that produced
    /// nothing for this output leave gaps, and a batch whose sequence is not above the
    /// highest one the sink has already applied is a redelivery: the circuit restarted and
    /// is re-running the steps after its last checkpoint.</summary>
    public long Sequence { get; }

    /// <summary>The step's changes: value → signed weight. Never empty.</summary>
    public ZSet<T> Changes { get; }

    /// <summary>Confirms this batch, and every earlier one, as delivered; see
    /// <see cref="AcknowledgedOutputHandle{T}.Acknowledge"/>.</summary>
    public void Acknowledge() => _owner.Acknowledge(Sequence);
}

/// <summary>
/// Consumer-side handle to an acknowledged output: the at-least-once way to read a circuit.
/// Batches arrive as on a plain output — one per step that produced changes, through one
/// bounded channel with one consumer — but each carries its step's sequence number, and the
/// consumer confirms it once the batch is safely in the sink. With a checkpointing storage
/// provider the circuit does not commit a checkpoint until every batch published so far has
/// been acknowledged, so a crash can never lose a batch: after a restart the circuit re-runs
/// the steps since its last checkpoint and delivers their output again. What a sink must
/// therefore tolerate is repetition — apply batches idempotently (keyed upserts do), or use
/// <see cref="OutputBatch{T}.Sequence"/> to recognise a redelivery.
/// <para>
/// Redelivered batches repeat sequence numbers, not necessarily contents: an auto-stepping
/// circuit may cut the replayed input into different steps. The batches of the replayed
/// window still add up to the same change.
/// </para>
/// <para>
/// The price is that the circuit waits at each commit point (every
/// <see cref="CircuitOptions.CheckpointInterval"/> steps) for the acknowledgement, running no
/// further step meanwhile; inputs back up behind it as with any slow consumer. A consumer
/// that stops reading before the output completes faults the circuit rather than leave it
/// waiting for ever. An acknowledged output keeps no state and has no bootstrap batch.
/// </para>
/// </summary>
public sealed class AcknowledgedOutputHandle<T> where T : notnull
{
    private readonly ChannelReader<OutputBatch<T>> _reader;
    private readonly Runtime.AcknowledgedOutputPort<T> _port;
    private int _claimed;

    internal AcknowledgedOutputHandle(string name, ChannelReader<OutputBatch<T>> reader, Runtime.AcknowledgedOutputPort<T> port)
    {
        Name = name;
        _reader = reader;
        _port = port;
    }

    internal string Name { get; }

    /// <summary>Advanced: the output's raw channel, for step-level consumption and
    /// non-blocking emptiness checks. Bypasses the consumer claim and the detection of a
    /// consumer that has gone away — do not mix with <see cref="Batches"/> or
    /// <see cref="ConsumeAsync"/>, which read the same channel. Batches read here are
    /// acknowledged the same way.</summary>
    public ChannelReader<OutputBatch<T>> Reader => _reader;

    /// <summary>The highest sequence acknowledged so far; 0 before the first.</summary>
    public long AcknowledgedSequence => _port.AcknowledgedSequence;

    /// <summary>
    /// The output's batches, in step order. Acknowledge each (or a later one — an
    /// acknowledgement covers everything before it) once the sink has it. The enumeration
    /// completes when the circuit disposes and throws <see cref="CircuitFaultedException"/>
    /// if it faults. Ending it early — cancelling, breaking out of the loop, an exception in
    /// the loop body — tells the circuit its consumer is gone, and the circuit faults. One
    /// consumer only: a second call throws.
    /// </summary>
    public IAsyncEnumerable<OutputBatch<T>> Batches(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _claimed, 1) != 0)
            throw new InvalidOperationException(
                $"Output '{Name}' already has a consumer. An output is a single channel — a second subscription " +
                "would silently split the feed. Declare a second output on the same stream to consume it twice.");
        return BatchesCore(cancellationToken);
    }

    private async IAsyncEnumerable<OutputBatch<T>> BatchesCore(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var completed = false;
        try
        {
            await foreach (var batch in _reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                yield return batch;
            completed = true;
        }
        finally
        {
            if (!completed)
                _port.Abandon();
        }
    }

    /// <summary>Runs <paramref name="sink"/> over every batch, in order, one at a time, and
    /// acknowledges each batch when its call returns — so returning means "delivered". If
    /// the sink throws, the batch is not acknowledged, the exception propagates, and the
    /// circuit faults (its consumer is gone). Completes when the output does.</summary>
    public async Task ConsumeAsync(
        Func<OutputBatch<T>, CancellationToken, ValueTask> sink, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sink);
        await foreach (var batch in Batches(cancellationToken).ConfigureAwait(false))
        {
            try
            {
                await sink(batch, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Before the enumeration unwinds, so the circuit's fault names the cause.
                _port.Abandon(ex);
                throw;
            }
            batch.Acknowledge();
        }
    }

    /// <summary>Tells the circuit this output's consumer is gone for good and will
    /// acknowledge nothing more — what ending <see cref="Batches"/> early does implicitly.
    /// A commit waiting on this output stops waiting and fails: a running circuit faults,
    /// and a circuit being disposed finishes without its final commit, so the unacknowledged
    /// steps are delivered again after the restart. This is the way out for a shutdown that
    /// has run out of time. Idempotent.</summary>
    public void Abandon() => _port.Abandon();

    /// <summary>Confirms every batch with a sequence up to and including
    /// <paramref name="sequence"/> as delivered. Safe from any thread, and to repeat. Throws
    /// if no batch with that sequence has been published.</summary>
    public void Acknowledge(long sequence) => _port.Acknowledge(sequence);
}
