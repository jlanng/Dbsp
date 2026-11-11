using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Incremental.Service;

/// <summary>What a connector makes of a slot's config: a line for an operator to read, and
/// the config properties that are secret (the admin API returns those as <c>***</c>).</summary>
public sealed record ConnectorDescription(string Summary, IReadOnlyList<string>? SecretProperties = null);

/// <summary>
/// A source or sink kind, registered under a name (<c>AddSourceConnector</c>,
/// <c>AddSinkConnector</c>) and given a slot's JSON config. A connector implements the
/// shapes it supports and no others; a slot bound to a connector that does not implement
/// the slot's kind is refused at the dry run.
/// </summary>
public interface IConnector
{
    /// <summary>Checks the config; throws on a bad one.</summary>
    ConnectorDescription Describe(JsonElement config);
}

public interface IAppendSourceConnector : IConnector
{
    /// <summary>Starts feeding the input. It is called after the circuit has started, so
    /// the committed offsets in the context are where to resume.</summary>
    Task<IRunningSource> StartAsync<T>(SourceContext<T> context, CancellationToken cancellationToken) where T : notnull;
}

public interface IKeyedSourceConnector : IConnector
{
    /// <inheritdoc cref="IAppendSourceConnector.StartAsync"/>
    Task<IRunningSource> StartAsync<TKey, TValue>(KeyedSourceContext<TKey, TValue> context, CancellationToken cancellationToken)
        where TKey : notnull
        where TValue : notnull;
}

public interface IRunningSource : IAsyncDisposable
{
    /// <summary>Faults when the source fails, which faults the pipeline.</summary>
    Task Completion { get; }
    SourceStats Stats { get; }
}

public sealed record SourceStats(
    long Records = 0, long Unreadable = 0, IReadOnlyDictionary<int, long>? Positions = null, long? Lag = null,
    DateTimeOffset? PausedSince = null);

public interface IUpsertSinkConnector : IConnector
{
    IUpsertSink<TKey, TRow> Create<TKey, TRow>(SinkContext context) where TKey : notnull;
}

public interface IAppendSinkConnector : IConnector
{
    IAppendSink<T> Create<T>(SinkContext context);
}

/// <summary>Optional on a sink connector: forget everything this slot wrote. Called when
/// a pipeline is reset; a sink without it keeps what the old run wrote.</summary>
public interface IResettableSinkConnector
{
    Task ResetAsync(SinkContext context, CancellationToken cancellationToken);
}

public abstract class SlotContext
{
    private readonly SlotStart _start;

    private protected SlotContext(SlotStart start) => _start = start;

    public string PipelineId => _start.PipelineId;
    public string Slot => _start.Slot;
    public JsonElement Config => _start.Config;

    /// <summary>Options for everything of the pipeline's that is serialized: one instance
    /// per loaded plugin version, so its metadata cache goes when the version unloads.
    /// Never use a process-wide options instance for a pipeline's types.</summary>
    public JsonSerializerOptions Json => _start.Json;
    public ILogger Logger => _start.Logger;
    public IServiceProvider Services => _start.Services;
    private protected Circuit CircuitCore => _start.Circuit;
}

public sealed class SourceContext<T> : SlotContext where T : notnull
{
    internal SourceContext(SlotStart start, InputStream<T> stream) : base(start)
    {
        Stream = stream;
        Input = start.Circuit.Handle(stream);
    }

    public Circuit Circuit => CircuitCore;
    public InputHandle<T> Input { get; }

    /// <summary>The input as the definition declared it, for a source that is started
    /// with the circuit and the stream (<c>KafkaSource.StartAsync</c>).</summary>
    public InputStream<T> Stream { get; }

    /// <summary>Partition → offset of the last record in the committed checkpoint: resume
    /// each partition just above it.</summary>
    public IReadOnlyDictionary<int, long> CommittedOffsets => Circuit.CommittedOffsets(Slot);
}

public sealed class KeyedSourceContext<TKey, TValue> : SlotContext
    where TKey : notnull
    where TValue : notnull
{
    internal KeyedSourceContext(SlotStart start, KeyedInputStream<TKey, TValue> stream) : base(start)
    {
        Stream = stream;
        Input = start.Circuit.Handle(stream);
    }

    public Circuit Circuit => CircuitCore;
    public KeyedInputHandle<TKey, TValue> Input { get; }

    /// <inheritdoc cref="SourceContext{T}.Stream"/>
    public KeyedInputStream<TKey, TValue> Stream { get; }

    /// <inheritdoc cref="SourceContext{T}.CommittedOffsets"/>
    public IReadOnlyDictionary<int, long> CommittedOffsets => Circuit.CommittedOffsets(Slot);
}

public sealed class SinkContext : SlotContext
{
    private readonly SlotStart _start;

    internal SinkContext(SlotStart start) : base(start) => _start = start;

    /// <summary>The leader term this node writes under.</summary>
    public long Term => _start.Leadership.Term;

    /// <summary>The step the circuit's checkpoint covers; batches above it are delivered
    /// again after a restart. Null before the first commit, and at a reset.</summary>
    public long? CommittedStep => _start.Circuit?.CommittedStep;
}

// ── what the supervisor sees of a sink ─────────────────────────────────────────────────

internal sealed class SinkSlotStats
{
    public long Batches;
    public long Upserts;
    public long Deletes;
    public long Appends;
    public long InFlightSinceTicks; // 0: none

    public SinkSlotStatus Status(string slot) => new(
        slot, Interlocked.Read(ref Batches), Interlocked.Read(ref Upserts), Interlocked.Read(ref Deletes), Interlocked.Read(ref Appends),
        Interlocked.Read(ref InFlightSinceTicks) is > 0 and var ticks ? new DateTimeOffset(ticks, TimeSpan.Zero) : null);
}

internal sealed class ObservedUpsertSink<TKey, TRow>(IUpsertSink<TKey, TRow> inner, SinkSlotStats stats) : IUpsertSink<TKey, TRow>
    where TKey : notnull
{
    public async ValueTask WriteAsync(UpsertBatch<TKey, TRow> batch, CancellationToken cancellationToken)
    {
        Interlocked.Exchange(ref stats.InFlightSinceTicks, DateTimeOffset.UtcNow.UtcTicks);
        try
        {
            await inner.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
            Interlocked.Increment(ref stats.Batches);
            Interlocked.Add(ref stats.Upserts, batch.Upserts.Count);
            Interlocked.Add(ref stats.Deletes, batch.Deletes.Count);
        }
        finally
        {
            Interlocked.Exchange(ref stats.InFlightSinceTicks, 0);
        }
    }
}

internal sealed class ObservedAppendSink<T>(IAppendSink<T> inner, SinkSlotStats stats) : IAppendSink<T>
{
    public async ValueTask WriteAsync(AppendBatch<T> batch, CancellationToken cancellationToken)
    {
        Interlocked.Exchange(ref stats.InFlightSinceTicks, DateTimeOffset.UtcNow.UtcTicks);
        try
        {
            await inner.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
            Interlocked.Increment(ref stats.Batches);
            Interlocked.Add(ref stats.Appends, batch.Records.Count);
        }
        finally
        {
            Interlocked.Exchange(ref stats.InFlightSinceTicks, 0);
        }
    }
}

// ── the memory connector ───────────────────────────────────────────────────────────────

/// <summary>
/// The <c>memory</c> connector: in-process logs and sinks by name, for tests, the test
/// harness and an example's generator. A log keeps every record appended to it — it is the
/// stand-in for a topic, so a pipeline that restarts resumes it from its committed offset —
/// which makes it test storage, not a queue: do not point production traffic at it.
/// Config: <c>{ "connector": "memory", "name": "trades" }</c>; the name defaults to
/// <c>{pipeline}/{slot}</c>.
/// </summary>
public sealed class MemoryConnector
    : IAppendSourceConnector, IKeyedSourceConnector, IUpsertSinkConnector, IAppendSinkConnector, IResettableSinkConnector, IConfigDescribed
{
    public ConfigSchema ConfigSchema { get; } = new(
        [new("name", "string", Description: "The log or sink in this process; \"{pipeline}/{slot}\" when left out.")],
        "In this process's memory: a log a test or a demo appends to, a sink it reads.", Open: true);

    private readonly ConcurrentDictionary<string, object> _logs = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, object> _sinks = new(StringComparer.Ordinal);

    public ConnectorDescription Describe(JsonElement config) => new("in-process memory");

    private static string NameOf(SlotContext context) =>
        context.Config.ValueKind == JsonValueKind.Object && context.Config.TryGetProperty("name", out var name) && name.GetString() is { Length: > 0 } given
            ? given
            : $"{context.PipelineId}/{context.Slot}";

    private static T Typed<T>(ConcurrentDictionary<string, object> items, string name, Func<T> create) where T : class
    {
        var item = items.GetOrAdd(name, _ => create());
        return item as T ?? throw new InvalidOperationException(
            $"Memory connector: '{name}' is a {Describe(item.GetType())}, not a {Describe(typeof(T))}.");
    }

    private static string Describe(Type type) =>
        type.IsGenericType ? $"{type.Name[..type.Name.IndexOf('`')]}<{string.Join(", ", type.GetGenericArguments().Select(a => a.Name))}>" : type.Name;

    /// <summary>The log an append input bound to <paramref name="name"/> reads.</summary>
    public MemoryLog<T> Log<T>(string name) where T : notnull => Typed(_logs, name, () => new MemoryLog<T> { RetainLast = RetainLast });

    /// <summary><see cref="MemoryLog{T}.RetainLast"/> for the logs this connector creates
    /// from now on. Null, the default, keeps every record.</summary>
    public int? RetainLast { get; set; }

    /// <summary>The log a keyed input bound to <paramref name="name"/> reads.</summary>
    public MemoryKeyedLog<TKey, TValue> KeyedLog<TKey, TValue>(string name)
        where TKey : notnull
        where TValue : notnull =>
        Typed(_logs, name, () => new MemoryKeyedLog<TKey, TValue> { Entries = { RetainLast = RetainLast } });

    public InMemoryUpsertSink<TKey, TRow> UpsertSink<TKey, TRow>(string name) where TKey : notnull =>
        Typed(_sinks, name, () => new InMemoryUpsertSink<TKey, TRow>());

    public InMemoryAppendSink<T> AppendSink<T>(string name) => Typed(_sinks, name, () => new InMemoryAppendSink<T>());

    /// <summary>Forgets a log or sink — and with it the types it holds.</summary>
    public void Remove(string name)
    {
        _logs.TryRemove(name, out _);
        _sinks.TryRemove(name, out _);
    }

    Task<IRunningSource> IAppendSourceConnector.StartAsync<T>(SourceContext<T> context, CancellationToken cancellationToken)
    {
        var log = Log<T>(NameOf(context));
        var from = context.CommittedOffsets.GetValueOrDefault(0, -1) + 1;
        return Task.FromResult<IRunningSource>(new Reader<T>(log, from,
            (record, offset, ct) => context.Input.InsertAsync(record, 0, offset, ct)));
    }

    Task<IRunningSource> IKeyedSourceConnector.StartAsync<TKey, TValue>(KeyedSourceContext<TKey, TValue> context, CancellationToken cancellationToken)
    {
        var log = KeyedLog<TKey, TValue>(NameOf(context));
        var from = context.CommittedOffsets.GetValueOrDefault(0, -1) + 1;
        return Task.FromResult<IRunningSource>(new Reader<MemoryKeyedLog<TKey, TValue>.Entry>(log.Entries, from,
            (entry, offset, ct) => entry.Delete
                ? context.Input.DeleteAsync(entry.Key, 0, offset, ct)
                : context.Input.UpsertAsync(entry.Value!, 0, offset, ct)));
    }

    IUpsertSink<TKey, TRow> IUpsertSinkConnector.Create<TKey, TRow>(SinkContext context) => UpsertSink<TKey, TRow>(NameOf(context));

    IAppendSink<T> IAppendSinkConnector.Create<T>(SinkContext context) => AppendSink<T>(NameOf(context));

    Task IResettableSinkConnector.ResetAsync(SinkContext context, CancellationToken cancellationToken)
    {
        // The next generation creates the sink afresh; a holder of the old one keeps a
        // record of what the run before the reset delivered.
        _sinks.TryRemove(NameOf(context), out _);
        return Task.CompletedTask;
    }

    private sealed class Reader<T> : IRunningSource where T : notnull
    {
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _run;
        private long _records;
        private long _position;

        private readonly MemoryLog<T> _log;

        public Reader(MemoryLog<T> log, long from, Func<T, long, CancellationToken, ValueTask> send)
        {
            _log = log;
            _position = from - 1;
            _run = Task.Run(async () =>
            {
                var next = from;
                while (!_stop.IsCancellationRequested)
                {
                    var (records, first, appended) = log.ReadFrom(next);
                    next = first;
                    foreach (var record in records)
                    {
                        await send(record, next, _stop.Token).ConfigureAwait(false);
                        Volatile.Write(ref _position, next++);
                        Interlocked.Increment(ref _records);
                    }
                    if (records.Count == 0)
                        await appended.WaitAsync(_stop.Token).ConfigureAwait(false);
                }
            });
            Completion = _run.ContinueWith(
                t => t.IsFaulted && !_stop.IsCancellationRequested ? Task.FromException(t.Exception!.GetBaseException()) : Task.CompletedTask,
                TaskScheduler.Default).Unwrap();
        }

        public Task Completion { get; }

        public SourceStats Stats => new(
            Interlocked.Read(ref _records), 0, new Dictionary<int, long> { [0] = Volatile.Read(ref _position) },
            Math.Max(0, _log.Count - 1 - Volatile.Read(ref _position)));

        public async ValueTask DisposeAsync()
        {
            _stop.Cancel();
            try
            {
                await _run.ConfigureAwait(false);
            }
            catch
            {
                // Cancellation, or the fault Completion already carries.
            }
        }
    }
}

/// <summary>An append-only in-process log: partition 0, offsets from 0.</summary>
public class MemoryLog<T> where T : notnull
{
    private readonly List<T> _records = [];
    private TaskCompletionSource _appended = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Exception? _failure;

    // The offset of _records[0]: above 0 once RetainLast has dropped records.
    private long _first;

    /// <summary>
    /// Null (the default) keeps every record, so a pipeline can always be replayed from
    /// offset 0 — what a test wants. With a number the log keeps about that many of the
    /// newest records and drops the rest, as a topic with retention does, so a host left
    /// running stays small; a source that asks for a record already dropped is given the
    /// oldest one held, with its true offset (a consumer's <c>auto.offset.reset=earliest</c>).
    /// </summary>
    public int? RetainLast { get; set; }

    /// <summary>Records ever appended: the offset the next one gets.</summary>
    public long Count
    {
        get
        {
            lock (_records)
                return _first + _records.Count;
        }
    }

    /// <summary>Records held now.</summary>
    public int Retained
    {
        get
        {
            lock (_records)
                return _records.Count;
        }
    }

    // Dropped in blocks, so that removing from the front costs O(1) per record appended.
    private void Trim()
    {
        if (RetainLast is not { } retain || _records.Count < 2L * Math.Max(retain, 1))
            return;
        var drop = _records.Count - Math.Max(retain, 1);
        _records.RemoveRange(0, drop);
        _first += drop;
    }

    public void Append(T record)
    {
        TaskCompletionSource signal;
        lock (_records)
        {
            _records.Add(record);
            Trim();
            signal = _appended;
            _appended = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        signal.TrySetResult();
    }

    public void AppendRange(IEnumerable<T> records)
    {
        TaskCompletionSource signal;
        lock (_records)
        {
            _records.AddRange(records);
            Trim();
            signal = _appended;
            _appended = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        signal.TrySetResult();
    }

    /// <summary>Fails every source reading this log once, as a broken connection would:
    /// the pipelines reading it fault and restart from their committed offsets.</summary>
    public void FailReaders(Exception error)
    {
        TaskCompletionSource signal;
        lock (_records)
        {
            _failure = error;
            signal = _appended;
            _appended = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        signal.TrySetResult();
    }

    /// <summary>Lets sources read again after <see cref="FailReaders"/>.</summary>
    public void Mend()
    {
        lock (_records)
            _failure = null;
    }

    /// <summary>Up to 1024 records from <paramref name="offset"/>, and the offset of the
    /// first of them: <paramref name="offset"/> itself unless that record has been dropped.</summary>
    internal (IReadOnlyList<T> Records, long First, Task Appended) ReadFrom(long offset)
    {
        lock (_records)
        {
            if (_failure is { } failure)
                throw failure;
            var first = Math.Max(offset, _first);
            var index = (int)(first - _first);
            var available = _records.Count - index;
            return (available > 0 ? _records.GetRange(index, Math.Min(available, 1024)) : [], first, _appended.Task);
        }
    }
}

/// <summary>An in-process log of upserts and deletes by key.</summary>
public sealed class MemoryKeyedLog<TKey, TValue>
    where TKey : notnull
    where TValue : notnull
{
    internal readonly record struct Entry(bool Delete, TKey Key, TValue? Value);

    internal MemoryLog<Entry> Entries { get; } = new();

    public long Count => Entries.Count;

    public void Upsert(TValue value) => Entries.Append(new Entry(false, default!, value));

    public void Delete(TKey key) => Entries.Append(new Entry(true, key, default));
}
