namespace Dbsp;

/// <summary>
/// A sink for a table-shaped result, written with keyed upserts: the delivery contract is
/// at-least-once, so the same batch — or an older one — may arrive again after a crash, and
/// applying it must be harmless. Upsert-by-key and delete-by-key are: writing a row that is
/// already there, or deleting one that is already gone, changes nothing.
/// </summary>
public interface IUpsertSink<TKey, TRow> where TKey : notnull
{
    /// <summary>
    /// Applies one step's changes: every key in <see cref="UpsertBatch{TKey,TRow}.Upserts"/>
    /// now has that row, every key in <see cref="UpsertBatch{TKey,TRow}.Deletes"/> has none.
    /// A key appears at most once in the batch. Return only once the batch is durable in the
    /// sink — the circuit's checkpoint waits for that, and commits past the batch as soon as
    /// this returns. Throwing asks for the batch again, after a backoff
    /// (<see cref="SinkDeliveryOptions"/>). The batch's lists are reused for the next call:
    /// copy what must outlive this one.
    /// </summary>
    ValueTask WriteAsync(UpsertBatch<TKey, TRow> batch, CancellationToken cancellationToken);
}

/// <summary>One step's changes to a keyed result, collapsed to at most one upsert or delete
/// per key.</summary>
public readonly struct UpsertBatch<TKey, TRow> where TKey : notnull
{
    internal UpsertBatch(long sequence, IReadOnlyList<KeyValuePair<TKey, TRow>> upserts, IReadOnlyList<TKey> deletes)
    {
        Sequence = sequence;
        Upserts = upserts;
        Deletes = deletes;
    }

    /// <summary>The sequence number of the step that produced the batch; see
    /// <see cref="OutputBatch{T}.Sequence"/>. A sink that can store it beside its rows can
    /// recognise a redelivery, but a keyed sink does not need to.</summary>
    public long Sequence { get; }

    /// <summary>The keys whose row changed or appeared, each with its row as of this step.</summary>
    public IReadOnlyList<KeyValuePair<TKey, TRow>> Upserts { get; }

    /// <summary>The keys whose row was removed.</summary>
    public IReadOnlyList<TKey> Deletes { get; }
}

/// <summary>
/// A sink for an append-only result — events, enriched facts, log lines — where there is no
/// key to make a repeated write harmless. Each batch carries the stable sequence number of
/// its step instead, and the sink uses that to deduplicate.
/// </summary>
public interface IAppendSink<T>
{
    /// <summary>
    /// Appends one step's records. Batches arrive in rising
    /// <see cref="AppendBatch{T}.Sequence"/> order; one whose sequence is not above the
    /// highest already written is a redelivery — the circuit restarted and is re-running the
    /// steps after its last checkpoint. With manual stepping a redelivered batch repeats the
    /// original's contents and can simply be skipped. An auto-stepping circuit may cut the
    /// replayed input into different steps, so a record can come back under another
    /// sequence than it first had, even a higher one than any the sink has seen; comparing
    /// sequences batch by batch is then not enough. The rule that is always exact: after a
    /// restart, before delivery resumes, drop everything the sink holds above
    /// <see cref="Circuit.CommittedStep"/> — the checkpoint covers exactly the steps up to
    /// it, and everything after is about to be delivered again. A sink that cannot drop
    /// (a topic) should carry the sequence on each record and leave that to its readers.
    /// Return only once the batch is durable; throwing asks for it again
    /// (<see cref="SinkDeliveryOptions"/>). The list is reused for the next call.
    /// </summary>
    ValueTask WriteAsync(AppendBatch<T> batch, CancellationToken cancellationToken);
}

/// <summary>One step's records on an append-only output.</summary>
public readonly struct AppendBatch<T>
{
    internal AppendBatch(long sequence, IReadOnlyList<T> records)
    {
        Sequence = sequence;
        Records = records;
    }

    /// <summary>The sequence number of the step that produced the batch; see
    /// <see cref="OutputBatch{T}.Sequence"/>.</summary>
    public long Sequence { get; }

    /// <summary>The step's records, in no particular order; a record the step inserted
    /// several times appears that many times.</summary>
    public IReadOnlyList<T> Records { get; }
}

/// <summary>How a sink's failures are handled by <see cref="SinkDelivery"/>.</summary>
public sealed class SinkDeliveryOptions
{
    /// <summary>How many times a failed write is tried again before the delivery gives up
    /// and the circuit faults — without committing past the batch, so a restart delivers it
    /// again. Null retries for ever: a sink that is down then stalls its pipeline (and,
    /// through backpressure, its producers) until it comes back, which is the right choice
    /// when nothing else can usefully take over. Default 5.</summary>
    public int? MaxRetries { get; init; } = 5;

    /// <summary>The wait before the first retry; it doubles with each further one, up to
    /// <see cref="MaxBackoff"/>. Default 100 ms.</summary>
    public TimeSpan InitialBackoff { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>The longest wait between two tries. Default 5 s.</summary>
    public TimeSpan MaxBackoff { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Called after each failed write with the exception and the number of the
    /// attempt that failed (1 for the first), before the backoff: the place to log or count.</summary>
    public Action<Exception, int>? OnFailedAttempt { get; init; }

    internal static readonly SinkDeliveryOptions Default = new();

    internal void Validate()
    {
        if (MaxRetries is < 0)
            throw new ArgumentOutOfRangeException(nameof(MaxRetries), MaxRetries, "MaxRetries cannot be negative; null retries for ever.");
        ArgumentOutOfRangeException.ThrowIfLessThan(InitialBackoff, TimeSpan.Zero, nameof(InitialBackoff));
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxBackoff, InitialBackoff, nameof(MaxBackoff));
    }
}

/// <summary>
/// Delivers an acknowledged output to a sink: at-least-once, in step order, one batch at a
/// time. Each step's delta is turned into what the sink takes — for an upsert sink at most
/// one upsert or delete per key, for an append sink the inserted records — the sink is
/// awaited, and the step is acknowledged when it returns, which is what lets the circuit's
/// checkpoint pass it (see <see cref="AcknowledgedOutputHandle{T}"/>). Nothing is buffered
/// beyond the output's bounded channel: a slow sink slows the circuit down and, through it,
/// the producers.
/// <para>
/// A write that throws is retried with a doubling backoff. When the retries run out the
/// delivery ends, the circuit faults without committing, and the batch is delivered again
/// after the restart; <see cref="SinkDeliveryOptions.MaxRetries"/> null retries for ever
/// instead.
/// </para>
/// <para>
/// What "at-least-once, keyed upserts" does and does not give. After a crash the circuit
/// re-runs the steps since its last checkpoint and delivers them again. If it cuts the
/// replayed input into the same steps (manual stepping does) the batches repeat exactly and
/// the sink ends up as if nothing had happened. An auto-stepping circuit may cut it
/// differently, and then the replayed batches are deltas between different points: a key
/// can step back to an older row and forward again, which converges — but a key whose row
/// was delivered before the crash and which is absent again by the end of a merged replay
/// step nets out to no change in that step, so nothing tells the sink to delete it. Such a
/// row stays until the key next changes. Use manual stepping, or reconcile the sink against
/// a snapshot output, where a transient key left behind is not acceptable.
/// </para>
/// </summary>
public static class SinkDelivery
{
    /// <summary>Delivers a keyed result to <paramref name="sink"/> until the output
    /// completes. The key is the result's own.</summary>
    public static Task DeliverToAsync<TKey, TRow>(
        this AcknowledgedOutputHandle<KeyedValue<TKey, TRow>> output,
        IUpsertSink<TKey, TRow> sink,
        SinkDeliveryOptions? options = null,
        CancellationToken cancellationToken = default)
        where TKey : notnull
        where TRow : notnull
    {
        ArgumentNullException.ThrowIfNull(output);
        return output.ConsumeAsync(Upserts(sink, options), cancellationToken);
    }

    /// <summary>Delivers a result to <paramref name="sink"/> as upserts keyed by
    /// <paramref name="keySelector"/>, until the output completes. The stream must hold at
    /// most one row per key at a time — that is what makes it table-shaped.</summary>
    public static Task DeliverToAsync<T, TKey>(
        this AcknowledgedOutputHandle<T> output,
        IUpsertSink<TKey, T> sink,
        Func<T, TKey> keySelector,
        SinkDeliveryOptions? options = null,
        CancellationToken cancellationToken = default)
        where T : notnull
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(output);
        return output.ConsumeAsync(Upserts(sink, keySelector, options), cancellationToken);
    }

    /// <summary>Delivers an append-only result to <paramref name="sink"/> until the output
    /// completes. A retraction on the stream ends the delivery and faults the circuit: an
    /// append sink has no way to take a record back.</summary>
    public static Task DeliverToAsync<T>(
        this AcknowledgedOutputHandle<T> output,
        IAppendSink<T> sink,
        SinkDeliveryOptions? options = null,
        CancellationToken cancellationToken = default)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(output);
        return output.ConsumeAsync(Appends(sink, options), cancellationToken);
    }

    /// <summary>The per-batch delivery behind <c>DeliverToAsync</c>, for
    /// <see cref="AcknowledgedOutputHandle{T}.ConsumeAsync"/> or a host's acknowledged
    /// consumer: collapse, write with retries, return when the sink has the batch. The
    /// returned delegate keeps scratch between calls, so use one per output and do not call
    /// it concurrently.</summary>
    public static Func<OutputBatch<KeyedValue<TKey, TRow>>, CancellationToken, ValueTask> Upserts<TKey, TRow>(
        IUpsertSink<TKey, TRow> sink, SinkDeliveryOptions? options = null)
        where TKey : notnull
        where TRow : notnull
    {
        ArgumentNullException.ThrowIfNull(sink);
        return new UpsertDelivery<KeyedValue<TKey, TRow>, TKey, TRow>(
            sink, static row => row.Key, static row => row.Value, Validated(options)).DeliverAsync;
    }

    /// <inheritdoc cref="Upserts{TKey,TRow}(IUpsertSink{TKey,TRow}, SinkDeliveryOptions?)"/>
    public static Func<OutputBatch<T>, CancellationToken, ValueTask> Upserts<T, TKey>(
        IUpsertSink<TKey, T> sink, Func<T, TKey> keySelector, SinkDeliveryOptions? options = null)
        where T : notnull
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(sink);
        ArgumentNullException.ThrowIfNull(keySelector);
        return new UpsertDelivery<T, TKey, T>(sink, keySelector, static row => row, Validated(options)).DeliverAsync;
    }

    /// <inheritdoc cref="Upserts{TKey,TRow}(IUpsertSink{TKey,TRow}, SinkDeliveryOptions?)"/>
    public static Func<OutputBatch<T>, CancellationToken, ValueTask> Appends<T>(
        IAppendSink<T> sink, SinkDeliveryOptions? options = null)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(sink);
        return new AppendDelivery<T>(sink, Validated(options)).DeliverAsync;
    }

    /// <summary>
    /// Delivers the circuit's dead letters to <paramref name="sink"/> until the circuit
    /// ends: at-least-once, a step's dropped records as one batch under the step's sequence
    /// number, each turned into what the sink stores by <paramref name="envelope"/>. The
    /// circuit must have <see cref="CircuitOptions.AcknowledgeDeadLetters"/>: a batch is
    /// acknowledged when the sink has it, and the checkpoint waits for that. A write that
    /// throws is retried as for any sink; when the retries run out the dead letters are
    /// abandoned, so the circuit faults without committing past the batch and a restart
    /// drops and delivers those records again. <paramref name="observe"/> sees each batch
    /// before it is written.
    /// </summary>
    public static async Task DeliverDeadLettersToAsync<T>(
        this Circuit circuit,
        IAppendSink<T> sink,
        Func<DeadLetter, T> envelope,
        SinkDeliveryOptions? options = null,
        Action<DeadLetterBatch>? observe = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(circuit);
        ArgumentNullException.ThrowIfNull(sink);
        ArgumentNullException.ThrowIfNull(envelope);
        options = Validated(options);
        var reader = circuit.DeadLetterBatches;
        var records = new List<T>();
        Func<AppendBatch<T>, CancellationToken, ValueTask> write = sink.WriteAsync;
        while (true)
        {
            try
            {
                if (!await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
                    return;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // The circuit faulted and completed the output with its fault: that is the
                // circuit's news, which it carries itself.
                return;
            }
            while (reader.TryRead(out var batch))
            {
                observe?.Invoke(batch);
                records.Clear();
                foreach (var letter in batch.Letters)
                    records.Add(envelope(letter));
                try
                {
                    await WriteWithRetriesAsync(new AppendBatch<T>(batch.Sequence, records), write, options, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    circuit.AbandonDeadLetters(ex);
                    throw;
                }
                batch.Acknowledge();
            }
        }
    }

    private static SinkDeliveryOptions Validated(SinkDeliveryOptions? options)
    {
        options ??= SinkDeliveryOptions.Default;
        options.Validate();
        return options;
    }

    /// <summary>Runs <paramref name="write"/> until it succeeds or the retries run out.
    /// Cancellation is never retried.</summary>
    internal static async ValueTask WriteWithRetriesAsync<TState>(
        TState state, Func<TState, CancellationToken, ValueTask> write, SinkDeliveryOptions options,
        CancellationToken cancellationToken)
    {
        var backoff = options.InitialBackoff;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await write(state, cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                options.OnFailedAttempt?.Invoke(ex, attempt);
                if (options.MaxRetries is { } retries && attempt > retries)
                    throw new SinkDeliveryException(attempt, ex);
            }

            await Task.Delay(backoff, cancellationToken).ConfigureAwait(false);
            backoff = backoff * 2 < options.MaxBackoff ? backoff * 2 : options.MaxBackoff;
        }
    }
}

/// <summary>A sink kept failing until <see cref="SinkDeliveryOptions.MaxRetries"/> ran out.
/// The last failure is the <see cref="Exception.InnerException"/>. It ends the delivery and
/// faults the circuit, whose fault carries it.</summary>
public sealed class SinkDeliveryException(int attempts, Exception lastFailure) : Exception(
    $"The sink failed {attempts} time{(attempts == 1 ? "" : "s")} in a row and delivery has given up; the circuit stops " +
    "without committing past the batch, and a restart delivers it again. See the inner exception for the last failure.",
    lastFailure)
{
    /// <summary>How many writes of the batch were attempted.</summary>
    public int Attempts { get; } = attempts;
}

/// <summary>
/// Collapses a step's Z-set delta over a table-shaped stream to keyed upserts and deletes.
/// Per key the delta holds the old row retracted, the new row asserted, or both: a key left
/// with one live row is an upsert of it, a key left with none is a delete. A key left with
/// more — two different rows asserted, or one row twice — is not a table row at all, and
/// faults: a keyed sink could only keep one of them, silently.
/// </summary>
internal sealed class UpsertDelivery<T, TKey, TRow>(
    IUpsertSink<TKey, TRow> sink, Func<T, TKey> keyOf, Func<T, TRow> rowOf, SinkDeliveryOptions options)
    where T : notnull
    where TKey : notnull
{
    // Scratch reused across batches; one batch is in flight at a time.
    private readonly Dictionary<TKey, (T Row, long Live)> _perKey = [];
    private readonly List<KeyValuePair<TKey, TRow>> _upserts = [];
    private readonly List<TKey> _deletes = [];
    private readonly Func<UpsertBatch<TKey, TRow>, CancellationToken, ValueTask> _write = sink.WriteAsync;

    public ValueTask DeliverAsync(OutputBatch<T> batch, CancellationToken cancellationToken)
    {
        Collapse(batch.Changes);
        return SinkDelivery.WriteWithRetriesAsync(
            new UpsertBatch<TKey, TRow>(batch.Sequence, _upserts, _deletes), _write, options, cancellationToken);
    }

    private void Collapse(ZSet<T> changes)
    {
        _perKey.Clear();
        _upserts.Clear();
        _deletes.Clear();
        foreach (var (row, weight) in changes)
        {
            var key = keyOf(row);
            if (RecordErrors.IsNull(key))
                throw new InvalidOperationException(
                    $"The key selector of an upsert sink returned null for {row}. Every row of a keyed result needs a key.");
            ref var entry = ref System.Runtime.InteropServices.CollectionsMarshal.GetValueRefOrAddDefault(_perKey, key, out _);
            if (weight > 0)
                entry = (row, entry.Live + weight);
        }

        foreach (var (key, (row, live)) in _perKey)
        {
            if (live == 0)
                _deletes.Add(key);
            else if (live == 1)
                _upserts.Add(new KeyValuePair<TKey, TRow>(key, rowOf(row)));
            else
                throw new InvalidOperationException(
                    $"A step left key '{key}' with more than one live row (the last being {row}). An upsert sink holds one " +
                    "row per key: the stream delivered to it must retract a key's old row when it asserts a new one — the " +
                    "result of an aggregate or a keyed input does — and must not carry the same row twice.");
        }
    }
}

/// <summary>Turns a step's delta over an append-only stream into the records to append: each
/// inserted record, as many times as its weight.</summary>
internal sealed class AppendDelivery<T>(IAppendSink<T> sink, SinkDeliveryOptions options) where T : notnull
{
    private readonly List<T> _records = [];
    private readonly Func<AppendBatch<T>, CancellationToken, ValueTask> _write = sink.WriteAsync;

    public ValueTask DeliverAsync(OutputBatch<T> batch, CancellationToken cancellationToken)
    {
        _records.Clear();
        foreach (var (record, weight) in batch.Changes)
        {
            if (weight < 0)
                throw new InvalidOperationException(
                    $"An append sink received a retraction (weight {weight}) of {record}. An append-only sink cannot take a " +
                    "record back: deliver a stream that only ever inserts, or use an upsert sink for one that amends its rows.");
            for (var i = 0L; i < weight; i++)
                _records.Add(record);
        }
        return SinkDelivery.WriteWithRetriesAsync(
            new AppendBatch<T>(batch.Sequence, _records), _write, options, cancellationToken);
    }
}

/// <summary>
/// An upsert sink that keeps its rows in memory: for tests, and as the reference for what a
/// real one must do. Thread-safe; <see cref="Rows"/> is a copy.
/// </summary>
public sealed class InMemoryUpsertSink<TKey, TRow> : IUpsertSink<TKey, TRow> where TKey : notnull
{
    private readonly Dictionary<TKey, TRow> _rows = [];
    private long _batches;
    private long _lastSequence;
    private long _redeliveries;

    /// <summary>The sink's current contents.</summary>
    public IReadOnlyDictionary<TKey, TRow> Rows
    {
        get
        {
            lock (_rows)
                return new Dictionary<TKey, TRow>(_rows);
        }
    }

    /// <summary>Batches written so far, redeliveries included.</summary>
    public long Batches => Interlocked.Read(ref _batches);

    /// <summary>The highest sequence written; 0 before the first batch.</summary>
    public long LastSequence => Interlocked.Read(ref _lastSequence);

    /// <summary>Batches that arrived with a sequence at or below one already written.</summary>
    public long Redeliveries => Interlocked.Read(ref _redeliveries);

    public ValueTask WriteAsync(UpsertBatch<TKey, TRow> batch, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_rows)
        {
            foreach (var key in batch.Deletes)
                _rows.Remove(key);
            foreach (var (key, row) in batch.Upserts)
                _rows[key] = row;
            if (batch.Sequence <= _lastSequence)
                _redeliveries++;
            else
                _lastSequence = batch.Sequence;
            _batches++;
        }
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// An append sink that keeps its records in memory, deduplicating by sequence the way
/// <see cref="IAppendSink{T}"/> describes: a batch whose sequence is not above the highest
/// written replaces everything written at or above that sequence. Thread-safe;
/// <see cref="Records"/> is a copy.
/// </summary>
public sealed class InMemoryAppendSink<T> : IAppendSink<T>
{
    private readonly List<(long Sequence, T Record)> _records = [];
    private long _batches;
    private long _redeliveries;

    /// <summary>Everything appended and not superseded by a redelivery, in sequence order.</summary>
    public IReadOnlyList<T> Records
    {
        get
        {
            lock (_records)
                return _records.ConvertAll(entry => entry.Record);
        }
    }

    /// <summary>Batches written so far, redeliveries included.</summary>
    public long Batches => Interlocked.Read(ref _batches);

    /// <summary>The highest sequence written; 0 before the first batch.</summary>
    public long LastSequence
    {
        get
        {
            lock (_records)
                return _records.Count == 0 ? 0 : _records[^1].Sequence;
        }
    }

    /// <summary>Batches that arrived with a sequence at or below one already written.</summary>
    public long Redeliveries => Interlocked.Read(ref _redeliveries);

    /// <summary>Drops every record written under a sequence above
    /// <paramref name="sequence"/>. Called with <see cref="Circuit.CommittedStep"/> (0 when
    /// it is null) after a restart and before delivery resumes, this leaves exactly what
    /// the checkpoint covers, whatever steps the replay goes on to cut.</summary>
    public void TruncateAfter(long sequence)
    {
        lock (_records)
            _records.RemoveAll(entry => entry.Sequence > sequence);
    }

    public ValueTask WriteAsync(AppendBatch<T> batch, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_records)
        {
            if (_records.Count > 0 && batch.Sequence <= _records[^1].Sequence)
            {
                _redeliveries++;
                var keep = _records.FindIndex(entry => entry.Sequence >= batch.Sequence);
                _records.RemoveRange(keep, _records.Count - keep);
            }
            foreach (var record in batch.Records)
                _records.Add((batch.Sequence, record));
            _batches++;
        }
        return ValueTask.CompletedTask;
    }
}
