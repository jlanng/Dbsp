using System.Threading.Channels;
using Dbsp.Diagnostics;
using Dbsp.Runtime;

namespace Dbsp;

/// <summary>
/// What a circuit does when one record cannot be processed: a user delegate throws on it, or
/// it yields a null key or a null result. Set for the whole circuit with
/// <see cref="CircuitOptions.ErrorPolicy"/> and overridden per operator with
/// <c>OnError</c> on the stream the operator returns.
/// </summary>
public enum RecordErrorPolicy
{
    /// <summary>The exception faults the circuit, as any other failure in a step does. The
    /// default: nothing is ever dropped silently, and the cost is that one bad record stops
    /// the pipeline until someone removes it.</summary>
    Fault,

    /// <summary>The record is taken out of the step and reported on
    /// <see cref="Circuit.DeadLetters"/> with its weight, the operator and the exception; the
    /// step carries on without it. See <see cref="DeadLetter"/> for what that requires of
    /// the delegates.</summary>
    DeadLetter,
}

/// <summary>
/// One record a circuit dropped under <see cref="RecordErrorPolicy.DeadLetter"/>.
/// <para>
/// Dropping is only correct if it is consistent: a record dropped when it was inserted must
/// be dropped again when it is retracted, or the retraction would take away something that
/// was never added. The engine relies on the delegates for that — a deterministic delegate
/// that threw on the insertion throws on the retraction too, and both are dropped, each
/// reported here with its own weight (so the dead letters of one operator are themselves a
/// stream of changes: the retraction's entry cancels the insertion's). A delegate that
/// throws for a record at one time and not at another breaks this exactly as a
/// non-deterministic <c>Select</c> already breaks retraction without any error policy, and
/// an operator with no state of its own cannot detect it. The operators that do keep state
/// per record or per key (aggregates, the remembered table lookup) use it, so for them the
/// consistency does not rest on the delegate.
/// </para>
/// </summary>
public sealed class DeadLetter
{
    internal DeadLetter(string @operator, object? record, long weight, Exception exception)
    {
        Operator = @operator;
        Record = record;
        Weight = weight;
        Exception = exception;
    }

    /// <summary>The operator that dropped the record: its state name root, positional
    /// (<c>n3.select</c>) or as given with <c>Named</c> (<c>named.enrich.join</c>); for a
    /// keyed input, <c>input[name]</c>.</summary>
    public string Operator { get; }

    /// <summary>The record the operator was processing: the row of its input delta. For an
    /// aggregate that is each row the step added to or removed from the failing group; for a
    /// join it is the row whose arrival paired it with the other side.</summary>
    public object? Record { get; }

    /// <summary>The record's signed weight in the delta it was dropped from: positive for an
    /// insertion, negative for a retraction.</summary>
    public long Weight { get; }

    /// <summary>What went wrong: the delegate's exception, or an
    /// <see cref="InvalidOperationException"/> describing a null key or result.</summary>
    public Exception Exception { get; }

    /// <summary>The sequence number of the step that dropped the record; see
    /// <see cref="OutputBatch{T}.Sequence"/>. Steps re-run after a crash report their dead
    /// letters again under the same numbers.</summary>
    public long Step { get; internal set; }

    public override string ToString() => $"{Operator}: {Record} ({Weight:+#;-#;0}) — {Exception.Message}";
}

/// <summary>
/// Where one operator instance (one node on one worker) reports the records it drops. Null
/// in an operator means its policy is <see cref="RecordErrorPolicy.Fault"/>, and the
/// operator then runs the loop it always ran, with no exception handling in it at all —
/// which is how the default policy costs nothing. Appends to its worker's list, which only
/// that worker's thread touches during a step; the dead-letter port collects the lists
/// between steps.
/// </summary>
internal sealed class RecordErrors(string operatorName, List<DeadLetter> sink)
{
    public void Add(object? record, long weight, Exception exception) =>
        sink.Add(new DeadLetter(operatorName, record, weight, exception));

    /// <summary>The dead-letter policy's form of <c>output.Add(selector(key, a, b), weight)</c>,
    /// for the joins: a pair the selector throws for is left out and
    /// <paramref name="record"/> — the delta row that brought the pair about — is reported.
    /// The selector is a pure function of the pair, so the pair is left out again when
    /// either side is retracted. Generic in the record so that nothing is boxed unless it
    /// is reported.</summary>
    public void Emit<TKey, TA, TB, TOut, TRecord>(
        ZSet<TOut> output, Func<TKey, TA, TB, TOut> selector, TKey key, TA a, TB b, long weight, TRecord record)
        where TOut : notnull
    {
        try
        {
            output.Add(Result(selector(key, a, b)), weight);
        }
        catch (Exception ex)
        {
            Add(record, weight, ex);
        }
    }

    /// <summary><see cref="Emit{TKey,TA,TB,TOut,TRecord}"/> for the unmatched side of a left
    /// join; the record reported is the fact itself.</summary>
    public void Emit<TKey, TA, TOut>(ZSet<TOut> output, Func<TKey, TA, TOut> selector, TKey key, TA a, long weight)
        where TKey : notnull
        where TA : notnull
        where TOut : notnull
    {
        try
        {
            output.Add(Result(selector(key, a)), weight);
        }
        catch (Exception ex)
        {
            Add(new KeyedValue<TKey, TA>(key, a), weight, ex);
        }
    }

    /// <summary>Whether <paramref name="value"/> is null, without boxing it to find out:
    /// asking a generic value <c>is null</c> boxes a value type in unoptimised code, and
    /// these checks sit in the per-record loops.</summary>
    public static bool IsNull<T>(T value) => NullCheck<T>.Possible && value is null;

    /// <summary><paramref name="value"/>, or the null-result exception if it is null.</summary>
    public static T Result<T>(T value) => IsNull(value) ? throw NullResult() : value;

    /// <summary><paramref name="value"/>, or the null-key exception if it is null.</summary>
    public static T Key<T>(T value) => IsNull(value) ? throw NullKey() : value;

    private static class NullCheck<T>
    {
        public static readonly bool Possible = default(T) is null;
    }

    public static InvalidOperationException NullKey() => new(
        "The key selector returned null for this record. A key cannot be null: there is no group, table row or " +
        "partition to put the record under.");

    public static InvalidOperationException NullResult() => new(
        "The delegate returned null for this record. A stream cannot carry null.");
}

internal static class KeySelectors
{
    /// <summary>The projection behind <c>GroupBy</c>. With <paramref name="guarded"/> a null
    /// key is turned into an exception inside the projection, where the dead-letter policy
    /// catches it; without, the projection is exactly what it always was.</summary>
    public static Func<T, KeyedValue<TKey, T>> Keyed<T, TKey>(Func<T, TKey> keySelector, bool guarded)
        where T : notnull
        where TKey : notnull =>
        guarded
            ? value => new KeyedValue<TKey, T>(RecordErrors.Key(keySelector(value)), value)
            : value => new KeyedValue<TKey, T>(keySelector(value), value);

    /// <summary>The projection behind <c>MapValues</c>. The operator's own null check sees
    /// the keyed row, a struct that is never null, so with <paramref name="guarded"/> a null
    /// from the selector is turned into the null-result exception here.</summary>
    public static Func<KeyedValue<TKey, TValue>, KeyedValue<TKey, TOut>> MapValues<TKey, TValue, TOut>(
        Func<TValue, TOut> selector, bool guarded)
        where TKey : notnull
        where TValue : notnull
        where TOut : notnull =>
        guarded
            ? kv => new KeyedValue<TKey, TOut>(kv.Key, RecordErrors.Result(selector(kv.Value)))
            : kv => new KeyedValue<TKey, TOut>(kv.Key, selector(kv.Value));
}

/// <summary>The lists the operators of a circuit report dropped records to, one per worker
/// and one for the coordinator, and their collection between steps. Shared by the two
/// dead-letter ports.</summary>
internal abstract class DeadLetterPortBase(int workers, CircuitDiagnostics diag)
{
    public const string PortName = "deadLetters";

    private readonly List<DeadLetter>[] _sources = Sources(workers); // one per worker, then the coordinator's

    private static List<DeadLetter>[] Sources(int workers)
    {
        var sources = new List<DeadLetter>[workers + 1];
        for (var i = 0; i < sources.Length; i++)
            sources[i] = [];
        return sources;
    }

    /// <summary>The list worker <paramref name="worker"/>'s operators append to.</summary>
    public List<DeadLetter> WorkerSink(int worker) => _sources[worker];

    /// <summary>The list the coordinator appends to while draining inputs.</summary>
    public List<DeadLetter> CoordinatorSink => _sources[^1];

    public string Name => PortName;

    public void BindSnapshot(Circuit circuit)
    {
    }

    /// <summary>Runs on the coordinator once every worker has left the step, so the lists
    /// are quiescent. Allocates only when something was dropped.</summary>
    protected List<DeadLetter>? Collect(long step)
    {
        List<DeadLetter>? staged = null;
        foreach (var source in _sources)
        {
            if (source.Count == 0)
                continue;
            staged ??= new List<DeadLetter>(source.Count);
            foreach (var letter in source)
            {
                letter.Step = step;
                staged.Add(letter);
                diag.RecordDeadLettered(letter.Operator);
            }
            source.Clear();
        }
        return staged;
    }
}

/// <summary>
/// The circuit's dead-letter output: an output port whose "node" is every operator at once.
/// After each step it collects what the workers' operators (and the keyed inputs' drains)
/// dropped and publishes it to one bounded channel, like any other output — so an unread
/// dead-letter queue is backpressure, never unbounded memory.
/// </summary>
internal sealed class DeadLetterPort : DeadLetterPortBase, IOutputPort
{
    private readonly Channel<DeadLetter> _channel;
    private List<DeadLetter>? _staged;

    public DeadLetterPort(int workers, int capacity, CircuitDiagnostics diag) : base(workers, diag)
    {
        _channel = Channel.CreateBounded<DeadLetter>(new BoundedChannelOptions(capacity)
        {
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
    }

    public ChannelReader<DeadLetter> Reader => _channel.Reader;

    public object Handle => _channel.Reader;
    public int BufferedCount => _channel.Reader.Count;

    public async ValueTask PublishAsync(Worker[] workers, long step, CancellationToken cancellationToken)
    {
        if (Stage(workers, step))
            await FlushStagedAsync(cancellationToken).ConfigureAwait(false);
    }

    public bool Stage(Worker[] workers, long step)
    {
        _staged = Collect(step);
        return _staged is not null;
    }

    public async ValueTask FlushStagedAsync(CancellationToken cancellationToken)
    {
        if (_staged is not { } staged)
            return;
        _staged = null;
        foreach (var letter in staged)
            await _channel.Writer.WriteAsync(letter, cancellationToken).ConfigureAwait(false);
    }

    public void Complete(Exception? error) => _channel.Writer.TryComplete(error);
}

/// <summary>One step's dead letters on an acknowledged dead-letter output
/// (<see cref="CircuitOptions.AcknowledgeDeadLetters"/>).</summary>
public readonly struct DeadLetterBatch
{
    private readonly AcknowledgedDeadLetterPort _owner;

    internal DeadLetterBatch(AcknowledgedDeadLetterPort owner, long sequence, IReadOnlyList<DeadLetter> letters)
    {
        _owner = owner;
        Sequence = sequence;
        Letters = letters;
    }

    /// <summary>The sequence number of the step that dropped the records; see
    /// <see cref="OutputBatch{T}.Sequence"/>. A step re-run after a crash reports its dead
    /// letters again; an auto-stepping circuit may cut the replayed input into other steps.</summary>
    public long Sequence { get; }

    /// <summary>Every record the step dropped, in no particular order.</summary>
    public IReadOnlyList<DeadLetter> Letters { get; }

    /// <summary>Confirms that this batch, and every one before it, is durable wherever the
    /// reader puts dead letters. The circuit's checkpoint does not pass a batch until then.</summary>
    public void Acknowledge() => _owner.Acknowledge(Sequence);
}

/// <summary>
/// The dead-letter output as an acknowledged output: one batch per step that dropped
/// something, and the circuit's checkpoint waits for the reader to acknowledge each — so a
/// record dropped by a step the checkpoint covers has reached the reader's store, and one
/// dropped by a step it does not cover is dropped and reported again after a restart.
/// At-least-once, like a sink.
/// </summary>
internal sealed class AcknowledgedDeadLetterPort : DeadLetterPortBase, IAcknowledgedOutputPort
{
    private readonly Channel<DeadLetterBatch> _channel;
    private readonly object _ackLock = new();
    private long _published;
    private long _acknowledged;
    private TaskCompletionSource? _waiter;
    private Exception? _failure;
    private Exception? _abandonment;
    private List<DeadLetter>? _staged;
    private long _stagedStep;

    public AcknowledgedDeadLetterPort(int workers, int capacity, CircuitDiagnostics diag) : base(workers, diag)
    {
        _channel = Channel.CreateBounded<DeadLetterBatch>(new BoundedChannelOptions(Math.Max(1, capacity))
        {
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
    }

    public ChannelReader<DeadLetterBatch> Reader => _channel.Reader;

    public object Handle => _channel.Reader;
    public int BufferedCount => _channel.Reader.Count;

    public async ValueTask PublishAsync(Worker[] workers, long step, CancellationToken cancellationToken)
    {
        if (Stage(workers, step))
            await FlushStagedAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The batch counts as published from here: the commit gate waits for it
    /// whether or not the flush has landed.</summary>
    public bool Stage(Worker[] workers, long step)
    {
        _staged = Collect(step);
        if (_staged is null)
            return false;
        _stagedStep = step;
        lock (_ackLock)
            _published = step;
        return true;
    }

    public async ValueTask FlushStagedAsync(CancellationToken cancellationToken)
    {
        if (_staged is not { } staged)
            return;
        _staged = null;
        try
        {
            await _channel.Writer.WriteAsync(new DeadLetterBatch(this, _stagedStep, staged), cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException) when (Volatile.Read(ref _abandonment) is { } abandoned)
        {
            throw abandoned;
        }
    }

    public Task WaitAcknowledgedAsync()
    {
        lock (_ackLock)
        {
            if (_failure is { } failure)
                return Task.FromException(failure);
            if (_acknowledged >= _published)
                return Task.CompletedTask;
            return (_waiter ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
        }
    }

    public void Acknowledge(long sequence)
    {
        TaskCompletionSource? release = null;
        lock (_ackLock)
        {
            if (sequence <= _acknowledged)
                return;
            _acknowledged = Math.Min(sequence, _published);
            if (_acknowledged >= _published)
                (release, _waiter) = (_waiter, null);
        }
        release?.TrySetResult();
    }

    /// <summary>The reader stopped before the output completed: the publish in flight and
    /// the commit gate fail, which faults the circuit instead of leaving it stalled.</summary>
    public void Abandon(Exception? cause)
    {
        // The sink's own failure, where the delivery gave up on it after its retries.
        var root = cause is SinkDeliveryException { InnerException: { } last } ? last : cause;
        var failure = new InvalidOperationException(
            (root is null
                ? "The reader of the acknowledged dead-letter output stopped before the output completed, so the records " +
                  "dropped since can no longer be stored or acknowledged."
                : $"The circuit's dead letters could not be stored ({root.GetType().Name}: {root.Message}), so the circuit stops " +
                  "rather than commit past records it dropped with no trace.") +
            " The checkpoint does not advance past them: rebuild the circuit from the last committed checkpoint and they " +
            "are dropped and reported again.",
            cause);
        if (Interlocked.CompareExchange(ref _abandonment, failure, null) is not null)
            return;
        Fail(failure);
        _channel.Writer.TryComplete(failure);
    }

    private void Fail(Exception failure)
    {
        TaskCompletionSource? release;
        lock (_ackLock)
        {
            _failure ??= failure;
            (release, _waiter) = (_waiter, null);
        }
        release?.TrySetException(failure);
    }

    public void Complete(Exception? error)
    {
        if (error is not null)
            Fail(error);
        _channel.Writer.TryComplete(error);
    }
}
