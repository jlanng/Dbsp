using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Dbsp.Service;

/// <summary>The object stores the host registered, by name (<c>AddObjectStore</c>).
/// Resolved from DI; a custom connector that reads or writes objects asks it.</summary>
public sealed class ObjectStores
{
    private readonly IServiceProvider _services;
    private readonly ServiceRegistrations _registrations;
    private readonly Dictionary<string, IObjectStore> _resolved = new(StringComparer.Ordinal);

    internal ObjectStores(IServiceProvider services, ServiceRegistrations registrations)
    {
        _services = services;
        _registrations = registrations;
    }

    public IReadOnlyCollection<string> Names => _registrations.ObjectStores.Keys;

    /// <summary>The store of that name. Throws, listing the registered names, when there
    /// is none.</summary>
    public IObjectStore Get(string? name)
    {
        if (string.IsNullOrEmpty(name))
            throw new InvalidOperationException($"name an object store: \"store\": \"…\" (registered: {string.Join(", ", Names.Order())})");
        lock (_resolved)
        {
            if (_resolved.TryGetValue(name, out var store))
                return store;
            if (!_registrations.ObjectStores.TryGetValue(name, out var factory))
                throw new InvalidOperationException(
                    $"no object store named '{name}' is registered (registered: {string.Join(", ", Names.Order())})");
            return _resolved[name] = factory(_services) ?? throw new InvalidOperationException($"the factory of object store '{name}' returned null");
        }
    }
}

/// <summary>A line of an object that the <c>files</c> source could not read as a record:
/// what a dead letter of such a line carries as its record.</summary>
public sealed record FileRecord(string Object, long Line, string Text)
{
    public override string ToString() => $"{Object}:{Line + 1}: {(Text.Length > 4096 ? Text[..4096] : Text)}";
}

/// <summary>What a slot bound to the <c>files</c> connector may say. Nothing else is
/// accepted: where the store is and how it is reached belong to the host's registration.</summary>
internal sealed record FilesSlotConfig
{
    public string? Connector { get; init; }
    public string? Store { get; init; }
    public string? Prefix { get; init; }

    /// <summary>Sources only: read objects whose name ends with this.</summary>
    public string? Suffix { get; init; }

    /// <summary>Sources only: how often the prefix is listed for new objects (hh:mm:ss).</summary>
    public string? PollInterval { get; init; }

    /// <summary>Sinks only: the least time between two objects (hh:mm:ss).</summary>
    public string? MinInterval { get; init; }

    /// <summary>Sources only: the longest line, in characters, that is read as a record.</summary>
    public int? MaxLineLength { get; init; }

    /// <summary>Sources only: how long an object has to have been left alone before it is
    /// read (hh:mm:ss), and so how late an object may arrive behind a greater name.</summary>
    public string? Settle { get; init; }

    /// <summary>Keyed inputs only: <c>rows</c> (a line is a row) or <c>changes</c> (a line
    /// is <c>{"key":…,"row":…}</c> or <c>{"key":…,"deleted":true}</c>).</summary>
    public string? Format { get; init; }

    /// <summary>Upsert sinks only: fold the step objects into one snapshot whenever this
    /// many have been written since the last.</summary>
    public int? CompactAfter { get; init; }

    /// <summary>Append sinks only: keep this many of the newest step objects.</summary>
    public int? Retain { get; init; }

    public static readonly TimeSpan DefaultSettle = TimeSpan.FromSeconds(5);

    public TimeSpan SettleFor => Settle is null ? DefaultSettle : TimeSpan.Parse(Settle, CultureInfo.InvariantCulture);

    public bool Changes => Format == "changes";

    public const int DefaultMaxLineLength = 1 << 20;

    public int LineLimit => MaxLineLength ?? DefaultMaxLineLength;

    private static readonly JsonSerializerOptions Strict = new(JsonSerializerDefaults.Web)
    {
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
    };

    public TimeSpan Spacing => MinInterval is null ? TimeSpan.Zero : TimeSpan.Parse(MinInterval, CultureInfo.InvariantCulture);

    public TimeSpan Poll => PollInterval is null ? TimeSpan.FromSeconds(5) : TimeSpan.Parse(PollInterval, CultureInfo.InvariantCulture);

    public static FilesSlotConfig Parse(JsonElement config)
    {
        if (config.ValueKind != JsonValueKind.Object)
            throw new InvalidOperationException("the files config must be a JSON object");
        FilesSlotConfig parsed;
        try
        {
            parsed = config.Deserialize<FilesSlotConfig>(Strict)!;
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException(
                $"the files config accepts connector, store, prefix, suffix, pollInterval, settle, maxLineLength, format, minInterval, compactAfter and retain — where the store is belongs to the host ({ex.Message})");
        }
        if (string.IsNullOrEmpty(parsed.Prefix))
            throw new InvalidOperationException("the files config needs a \"prefix\": the objects of this slot are the ones whose name starts with it");
        if (Path.IsPathRooted(parsed.Prefix) || parsed.Prefix.Split('/', '\\').Contains(".."))
            throw new InvalidOperationException($"prefix '{parsed.Prefix}' must be relative to the store and stay inside it");
        if (parsed.PollInterval is not null
            && (!TimeSpan.TryParse(parsed.PollInterval, CultureInfo.InvariantCulture, out var poll) || poll <= TimeSpan.Zero))
            throw new InvalidOperationException($"pollInterval is a positive duration (hh:mm:ss), not '{parsed.PollInterval}'");
        if (parsed.MinInterval is not null
            && (!TimeSpan.TryParse(parsed.MinInterval, CultureInfo.InvariantCulture, out var spacing) || spacing < TimeSpan.Zero))
            throw new InvalidOperationException($"minInterval is a duration (hh:mm:ss), not '{parsed.MinInterval}'");
        if (parsed.Settle is not null
            && (!TimeSpan.TryParse(parsed.Settle, CultureInfo.InvariantCulture, out var settle) || settle < TimeSpan.Zero))
            throw new InvalidOperationException($"settle is a duration (hh:mm:ss), not '{parsed.Settle}'");
        if (parsed.Format is not (null or "rows" or "changes"))
            throw new InvalidOperationException($"format is \"rows\" or \"changes\", not '{parsed.Format}'");
        if (parsed.CompactAfter is < 2 or > MaxObjects)
            throw new InvalidOperationException($"compactAfter is a number of objects from 2 to {MaxObjects}, not {parsed.CompactAfter}");
        if (parsed.Retain is < 1 or > MaxObjects)
            throw new InvalidOperationException($"retain is a number of objects from 1 to {MaxObjects}, not {parsed.Retain}");
        if (parsed.MaxLineLength is < 1 or > MaxMaxLineLength)
            throw new InvalidOperationException($"maxLineLength is a number of characters from 1 to {MaxMaxLineLength}, not {parsed.MaxLineLength}");
        return parsed;
    }

    // A line is held whole, as text, while it is parsed: this is what one costs at most.
    public const int MaxMaxLineLength = 1 << 26;

    // A sink that keeps or counts objects holds their names: this many at most.
    public const int MaxObjects = 100_000;
}

/// <summary>
/// The <c>files</c> connector: JSON-lines objects under a prefix of a named
/// <see cref="IObjectStore"/>, as an append input, as a keyed input of upserts, and as an
/// append or an upsert sink. Config:
/// <c>{ "connector": "files", "store": "landing", "prefix": "trades/" }</c>.
/// <para>
/// <b>As a source</b> it reads the objects under the prefix in ordinal order of their
/// names, one record per line, and then waits for objects that sort after the last one it
/// read. Its position — the object and the line — is in the circuit's checkpoint, so a
/// restart or a takeover carries on at the next line. Two things follow from "in name
/// order": an object must be complete when it appears (write it elsewhere and move it, or
/// put it whole), and one that arrives with a name sorting before the position is never
/// read. A line that is not a record is rejected in its place
/// (<see cref="InputHandle{T}.RejectAsync"/>): the circuit's error policy decides. The
/// position survives the objects it names being deleted.
/// </para>
/// <para>
/// <b>As a sink</b> it writes one object per step that has output, named
/// <c>{prefix}{step:D19}-{term:D6}.jsonl</c>: the step's sequence number, then the leader
/// term it was written under. A step delivered again writes the same name with the same
/// content. When a generation starts it deletes the objects of steps above the committed
/// one, because those steps are about to be run again and may be cut differently. An
/// append sink's lines are the records. An upsert sink's are
/// <c>{"key":…,"row":…}</c> and <c>{"key":…,"deleted":true}</c>, at most one per key per
/// object: the latest row of a key is the last line that names it, reading the objects in
/// name order — <see cref="FileSinkFormat"/> does exactly that. Where two objects carry
/// the same step (a leader that was replaced finished a write late) the one of the higher
/// term is the step and the other is to be ignored. A prefix belongs to one slot of one
/// pipeline. Nothing compacts an upsert sink's objects.
/// </para>
/// <para>
/// One object per step is many small objects when steps are small. <c>minInterval</c>
/// spaces them: having written a step's object the sink waits that long before it
/// acknowledges the step. The circuit's checkpoint waits for the acknowledgement, so the
/// circuit pauses, its input queues up behind the bounded channel, and the next step —
/// and its object — is larger. It trades latency of every sink of the pipeline for
/// fewer objects.
/// </para>
/// </summary>
public sealed class FileConnector(ObjectStores stores)
    : IAppendSourceConnector, IKeyedSourceConnector, IAppendSinkConnector, IUpsertSinkConnector, IResettableSinkConnector, IConfigDescribed,
        IPositionDescribing, IPresenceChecked
{
    /// <summary>An input whose prefix holds no object: nothing wrong with that if the
    /// first is yet to be delivered, and exactly what a mistyped prefix looks like.</summary>
    public async ValueTask<string?> MissingAsync(JsonElement config, SlotKind kind, CancellationToken cancellationToken)
    {
        if (kind is not (SlotKind.AppendInput or SlotKind.KeyedInput))
            return null;
        var (parsed, store) = Resolve(config);
        var page = await store.ListPageAsync(parsed.Prefix!, null, 1, cancellationToken).ConfigureAwait(false);
        return page.Objects.Count == 0 ? $"no object under {parsed.Prefix} in store {parsed.Store}: the source waits until one is there" : null;
    }

    public ConfigSchema ConfigSchema { get; } = new(
    [
        new("store", "string", Required: true, Description: "An object store the host registered; it holds the root and the credentials."),
        new("prefix", "string", Required: true, Description: "The objects of this slot are those whose name starts with it. One slot of one pipeline per prefix."),
        new("suffix", "string", Description: "Read only objects whose name ends with this.", AppliesTo: ConfigProperty.Sources),
        new("pollInterval", "string", Description: "How often the prefix is asked for objects after the last one read, as a duration (hh:mm:ss).", Default: "00:00:05", AppliesTo: ConfigProperty.Sources),
        new("settle", "string", Description: "An object is read once it has been left unchanged this long (hh:mm:ss), and nothing after it in name order is read before it. So an object may arrive up to this long after one with a greater name and still be read. Measured by the store's own modification times against each other, or by how long this source has seen the object unchanged: never by this node's clock against the store's. 00:00:00 when names only ever increase and objects appear whole.", Default: "00:00:05", AppliesTo: ConfigProperty.Sources),
        new("maxLineLength", "number", Description: "The longest line, in characters, read as a record. A longer one is rejected like a line that is not a record, without being held.", Default: "1048576", AppliesTo: ConfigProperty.Sources),
        new("format", "string", Description: "rows: every line is a row, and nothing deletes. changes: a line is {\"key\":…,\"row\":…} or {\"key\":…,\"deleted\":true}, which is also what an upsert sink of this connector writes.", Values: ["rows", "changes"], Default: "rows", AppliesTo: [SlotKind.KeyedInput]),
        new("minInterval", "string", Description: "The least time between two objects, as a duration (hh:mm:ss). The pipeline pauses to keep it, so steps and objects grow.", Default: "00:00:00", AppliesTo: ConfigProperty.Sinks),
        new("compactAfter", "number", Description: "Fold the step objects into one snapshot object whenever this many have been written since the last, and delete them. Left out, nothing is folded and every step's object stays.", AppliesTo: [SlotKind.UpsertSink]),
        new("retain", "number", Description: "Keep this many of the newest step objects and delete older ones. Left out, every object stays: retention is then the store's business.", AppliesTo: [SlotKind.AppendSink]),
    ], "JSON-lines objects under a prefix: read in name order as an append or keyed input, written one object per step as an append or upsert sink.");

    private (FilesSlotConfig Config, IObjectStore Store) Resolve(JsonElement json)
    {
        var config = FilesSlotConfig.Parse(json);
        return (config, stores.Get(config.Store));
    }

    public ConnectorDescription Describe(JsonElement config)
    {
        var (parsed, _) = Resolve(config);
        // A sink deletes step objects under its prefix (above the checkpoint at a start,
        // all of them at a reset): the prefix is one sink slot's alone. And not a
        // source's either: see ReadsSinkObjects.
        var name = $"files:{parsed.Store}:{parsed.Prefix}";
        return new ConnectorDescription($"objects under {parsed.Prefix} in store {parsed.Store}")
        {
            Owns = name,
            Reads = ReadsSinkObjects(parsed.Suffix) ? name : null,
        };
    }

    /// <summary>
    /// Whether a source with this suffix would read a sink's objects, were its prefix one
    /// a sink's objects lie under. That is refused (<see cref="ConnectorDescription.Reads"/>),
    /// because a source follows names and a sink's objects are not a log of names:
    /// <list type="bullet">
    /// <item>a step above the sink's checkpoint is run again after a restart and written
    /// under the name it had, with whatever the new cut of the step holds — a source that
    /// has read past the name never sees the difference;</item>
    /// <item><c>compactAfter</c> folds step objects into a snapshot that leaves deleted
    /// keys out and deletes the objects: a source that had not read them yet never sees
    /// the deletes, and reads the snapshot as so many upserts;</item>
    /// <item><c>retain</c> deletes objects a source may not have reached, and an object
    /// a replaced leader wrote late is void by the reader's rule 3, which needs the
    /// whole listing to apply.</item>
    /// </list>
    /// A sink's objects all end in <c>.jsonl</c>; a source whose suffix cannot end such a
    /// name reads none of them.
    /// </summary>
    internal static bool ReadsSinkObjects(string? suffix) =>
        string.IsNullOrEmpty(suffix)
        || FileSinkFormat.Extension.EndsWith(suffix, StringComparison.Ordinal)
        || suffix.EndsWith(FileSinkFormat.Extension, StringComparison.Ordinal);

    public string? DescribePosition(JsonElement config, IReadOnlyDictionary<int, long> committed) => FilePosition.Describe(committed);

    internal static void RequireKind(JsonElement json, SlotKind kind)
    {
        var config = FilesSlotConfig.Parse(json);
        var isSink = kind is SlotKind.AppendSink or SlotKind.UpsertSink;
        if (isSink && (config.Suffix is not null || config.PollInterval is not null || config.MaxLineLength is not null || config.Settle is not null))
            throw new InvalidOperationException("\"suffix\", \"pollInterval\", \"settle\" and \"maxLineLength\" are a source's settings");
        if (!isSink && config.MinInterval is not null)
            throw new InvalidOperationException("\"minInterval\" is a sink's setting");
        if (kind != SlotKind.KeyedInput && config.Format is not null)
            throw new InvalidOperationException("\"format\" is a keyed input's setting (KeyedInput<TKey, TValue>): only that has rows to delete");
        if (kind != SlotKind.UpsertSink && config.CompactAfter is not null)
            throw new InvalidOperationException("\"compactAfter\" is an upsert sink's setting: only its objects have a latest row per key to fold into");
        if (kind != SlotKind.AppendSink && config.Retain is not null)
            throw new InvalidOperationException("\"retain\" is an append sink's setting; an upsert sink is kept short with \"compactAfter\"");
    }

    Task<IRunningSource> IAppendSourceConnector.StartAsync<T>(SourceContext<T> context, CancellationToken cancellationToken)
    {
        var (config, store) = Resolve(context.Config);
        var input = context.Input;
        var json = context.Json;
        return Task.FromResult<IRunningSource>(new FileSourceReader(
            store, config, context.CommittedOffsets,
            (text, offset, ct) => input.InsertAsync(FileSourceReader.Read<T>(text, json), 0, offset, ct),
            (record, error, offset, ct) => input.RejectAsync(record, error, 0, offset, ct),
            input.AdvanceAsync));
    }

    Task<IRunningSource> IKeyedSourceConnector.StartAsync<TKey, TValue>(KeyedSourceContext<TKey, TValue> context, CancellationToken cancellationToken)
    {
        var (config, store) = Resolve(context.Config);
        var input = context.Input;
        var json = context.Json;
        Func<string, long, CancellationToken, ValueTask> send = config.Changes
            ? (text, offset, ct) =>
            {
                // What an upsert sink of this connector writes, and the one way a line can
                // take a row away: {"key":…,"deleted":true}.
                var change = FileSourceReader.Read<FileSinkFormat.UpsertLine<TKey, TValue>>(text, json);
                if (change.Key is null)
                    throw new JsonException("A change needs a \"key\".");
                if (change.Deleted)
                    return input.DeleteAsync(change.Key, 0, offset, ct);
                return input.UpsertAsync(change.Row ?? throw new JsonException("A change carries a \"row\", or \"deleted\": true."), 0, offset, ct);
            }
            : (text, offset, ct) => input.UpsertAsync(FileSourceReader.Read<TValue>(text, json), 0, offset, ct);
        return Task.FromResult<IRunningSource>(new FileSourceReader(
            store, config, context.CommittedOffsets, send,
            (record, error, offset, ct) => input.RejectAsync(record, error, 0, offset, ct),
            input.AdvanceAsync));
    }

    IAppendSink<T> IAppendSinkConnector.Create<T>(SinkContext context)
    {
        var (config, store) = Resolve(context.Config);
        return new AppendSink<T>(store, config, context);
    }

    IUpsertSink<TKey, TRow> IUpsertSinkConnector.Create<TKey, TRow>(SinkContext context)
    {
        var (config, store) = Resolve(context.Config);
        return new UpsertSink<TKey, TRow>(store, config, context);
    }

    async Task IResettableSinkConnector.ResetAsync(SinkContext context, CancellationToken cancellationToken)
    {
        var (config, store) = Resolve(context.Config);
        await FileSinkFormat.DeleteAboveAsync(store, config.Prefix!, 0, null, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>What both sinks share: the clean-up a generation owes before its first
    /// write, and one object per step.</summary>
    private abstract class StepObjectSink(IObjectStore store, FilesSlotConfig config, SinkContext context)
    {
        private static readonly byte[] NewLine = "\n"u8.ToArray();

        // The checkpoint this generation started from. Read now and not at the first
        // write: by then the circuit may have committed steps that gave this sink
        // nothing, and an object an earlier generation left for one of them would pass
        // for committed. It goes into every object's name: it is what tells a reader
        // which objects of an earlier term this generation's replace.
        private readonly long _startedAt = context.CommittedStep ?? 0;
        private bool _cleaned;

        protected IObjectStore Store => store;
        protected FilesSlotConfig Config => config;
        protected string Prefix => config.Prefix!;
        protected JsonSerializerOptions Json => context.Json;
        protected long Term => context.Term;

        /// <summary>The step the circuit's checkpoint covers now: what is at or below it
        /// is never run again, by this node or by one that replaces it.</summary>
        protected long Committed => Math.Max(_startedAt, context.CommittedStep ?? 0);

        protected async ValueTask WriteStepAsync<TState>(
            long sequence, TState state, Func<TState, Stream, CancellationToken, ValueTask> write, CancellationToken cancellationToken)
        {
            if (!_cleaned)
            {
                // Steps above the checkpoint are about to be run again, and an auto-stepping
                // circuit may cut them differently: what an earlier generation wrote for
                // them is not the result.
                await FileSinkFormat.DeleteAboveAsync(store, Prefix, _startedAt, Existing, cancellationToken).ConfigureAwait(false);
                _cleaned = true;
            }
            // Whole or not at all: a write that fails or is cancelled part-way leaves
            // nothing under the step's name (IObjectStore.WriteAsync).
            var name = FileSinkFormat.ObjectName(Prefix, sequence, Term, _startedAt);
            await store.WriteAsync(name, (stream, ct) => write(state, stream, ct), cancellationToken).ConfigureAwait(false);
            await WrittenAsync(new FileSinkFormat.StepObject(name, sequence, Term, _startedAt, false), cancellationToken).ConfigureAwait(false);

            // The object is written at once — the lease was checked a moment ago — and
            // the acknowledgement is what waits: the circuit commits, and steps again,
            // only after it.
            if (config.Spacing > TimeSpan.Zero)
                await Task.Delay(config.Spacing, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>An object the clean-up found and left, in name order.</summary>
        protected virtual ValueTask Existing(FileSinkFormat.StepObject found, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        /// <summary>A step's object is in the store.</summary>
        protected virtual ValueTask WrittenAsync(FileSinkFormat.StepObject written, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        protected static ValueTask EndLineAsync(Stream stream, CancellationToken cancellationToken) => stream.WriteAsync(NewLine, cancellationToken);
    }

    private sealed class AppendSink<T>(IObjectStore store, FilesSlotConfig config, SinkContext context)
        : StepObjectSink(store, config, context), IAppendSink<T>
    {
        // "retain": the names of the newest objects, oldest first — never more than the
        // number kept, plus the one just written.
        private readonly Queue<string>? _kept = config.Retain is null ? null : new Queue<string>();
        private string? _newest;

        public ValueTask WriteAsync(AppendBatch<T> batch, CancellationToken cancellationToken) =>
            batch.Records.Count == 0
                ? ValueTask.CompletedTask
                : WriteStepAsync(batch.Sequence, batch.Records, async (records, stream, ct) =>
                {
                    foreach (var record in records)
                    {
                        await JsonSerializer.SerializeAsync(stream, record, Json, ct).ConfigureAwait(false);
                        await EndLineAsync(stream, ct).ConfigureAwait(false);
                    }
                }, cancellationToken);

        protected override ValueTask Existing(FileSinkFormat.StepObject found, CancellationToken cancellationToken) => KeepAsync(found.Name, cancellationToken);

        protected override ValueTask WrittenAsync(FileSinkFormat.StepObject written, CancellationToken cancellationToken) => KeepAsync(written.Name, cancellationToken);

        private async ValueTask KeepAsync(string name, CancellationToken cancellationToken)
        {
            if (_kept is null || name == _newest)
                return;                     // no retention, or the same step delivered again
            _kept.Enqueue(_newest = name);
            while (_kept.Count > Config.Retain)
                await Store.DeleteAsync(_kept.Dequeue(), cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class UpsertSink<TKey, TRow>(IObjectStore store, FilesSlotConfig config, SinkContext context)
        : StepObjectSink(store, config, context), IUpsertSink<TKey, TRow>
        where TKey : notnull
    {
        // Step objects written since the newest snapshot, as far as this sink has seen.
        private int _sinceSnapshot;

        public ValueTask WriteAsync(UpsertBatch<TKey, TRow> batch, CancellationToken cancellationToken) =>
            batch.Upserts.Count + batch.Deletes.Count == 0
                ? ValueTask.CompletedTask
                : WriteStepAsync(batch.Sequence, batch, async (changes, stream, ct) =>
                {
                    foreach (var key in changes.Deletes)
                    {
                        await stream.WriteAsync("{\"key\":"u8.ToArray(), ct).ConfigureAwait(false);
                        await JsonSerializer.SerializeAsync(stream, key, Json, ct).ConfigureAwait(false);
                        await stream.WriteAsync(",\"deleted\":true}"u8.ToArray(), ct).ConfigureAwait(false);
                        await EndLineAsync(stream, ct).ConfigureAwait(false);
                    }
                    foreach (var (key, row) in changes.Upserts)
                    {
                        await stream.WriteAsync("{\"key\":"u8.ToArray(), ct).ConfigureAwait(false);
                        await JsonSerializer.SerializeAsync(stream, key, Json, ct).ConfigureAwait(false);
                        await stream.WriteAsync(",\"row\":"u8.ToArray(), ct).ConfigureAwait(false);
                        await JsonSerializer.SerializeAsync(stream, row, Json, ct).ConfigureAwait(false);
                        await stream.WriteAsync("}"u8.ToArray(), ct).ConfigureAwait(false);
                        await EndLineAsync(stream, ct).ConfigureAwait(false);
                    }
                }, cancellationToken);

        protected override ValueTask Existing(FileSinkFormat.StepObject found, CancellationToken cancellationToken)
        {
            _sinceSnapshot = found.Snapshot ? 0 : _sinceSnapshot + 1;
            return ValueTask.CompletedTask;
        }

        protected override async ValueTask WrittenAsync(FileSinkFormat.StepObject written, CancellationToken cancellationToken)
        {
            _sinceSnapshot++;
            if (Config.CompactAfter is { } limit && _sinceSnapshot >= limit)
                _sinceSnapshot = await FileSinkFormat.CompactAsync<TKey>(Store, Prefix, Committed, Term, Json, cancellationToken).ConfigureAwait(false);
        }
    }
}

/// <summary>
/// The names and lines the <c>files</c> sinks write, and how to read them back: the
/// reader's half of the format, for tests, for a job that loads the objects somewhere, and
/// as the definition of "the latest row per key".
/// <para>
/// A step's object is <c>{prefix}{step:D19}-{term:D6}-{base:D19}.jsonl</c>: the step's
/// sequence number, the leader term it was written under, and the step of the checkpoint
/// its writer had started from. A snapshot — what <c>compactAfter</c> folds an upsert
/// sink's objects into — ends in <c>.snapshot.jsonl</c> and holds the latest row of every
/// key as of its step. <b>The reader's rules</b>, which <see cref="StepObjectsAsync"/>
/// applies and anything else that reads the objects must:
/// </para>
/// <list type="number">
/// <item>Start from the snapshot of the highest step, if there is one, and ignore every
/// object at or below its step.</item>
/// <item>Where a step has objects of two terms, the higher term's is the step.</item>
/// <item>An object of term <c>t</c> at step <c>s</c> is void when an object of a later
/// term has a base below <c>s</c>: a later leader started from a checkpoint that does not
/// cover the step, ran it again, and may have cut it differently — or given this sink
/// nothing for it, which is why rule 2 alone is not enough.</item>
/// <item>Read what is left in step order; within an upsert sink's objects the latest row
/// of a key is the last line that names it, and <c>"deleted": true</c> takes it away.</item>
/// <item>An object that was listed and is gone when it is opened was folded into a
/// snapshot (or passed <c>retain</c>) meanwhile: an upsert reader lists again and starts
/// over.</item>
/// </list>
/// </summary>
public static class FileSinkFormat
{
    internal const string Extension = ".jsonl";
    private const string SnapshotExtension = ".snapshot.jsonl";

    // The default for a caller that gives no options. Never used by the service itself:
    // a pipeline's types go through its plugin version's own options.
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    /// <summary>One object under a sink's prefix, as its name says. <c>Base</c> is the
    /// step of the checkpoint its writer started from (<see cref="long.MaxValue"/> for a
    /// name of the older form, which did not say).</summary>
    public readonly record struct StepObject(string Name, long Step, long Term, long Base, bool Snapshot);

    /// <summary>The object a step is written to:
    /// <c>{prefix}{step:D19}-{term:D6}-{base:D19}.jsonl</c>.</summary>
    public static string ObjectName(string prefix, long step, long term, long @base) =>
        string.Create(CultureInfo.InvariantCulture, $"{prefix}{step:D19}-{term:D6}-{@base:D19}{Extension}");

    /// <summary>A step's object as builds before the base was in the name wrote it:
    /// <c>{prefix}{step:D19}-{term:D6}.jsonl</c>. Still read; it voids nothing.</summary>
    public static string ObjectName(string prefix, long step, long term) =>
        string.Create(CultureInfo.InvariantCulture, $"{prefix}{step:D19}-{term:D6}{Extension}");

    /// <summary>The snapshot of everything at or below a step.</summary>
    public static string SnapshotName(string prefix, long step, long term) =>
        string.Create(CultureInfo.InvariantCulture, $"{prefix}{step:D19}-{term:D6}-{step:D19}{SnapshotExtension}");

    /// <summary>What a sink's object name says; false for an object that is not one of
    /// this prefix's.</summary>
    public static bool TryParse(string prefix, string name, out StepObject parsed)
    {
        parsed = default;
        if (!name.StartsWith(prefix, StringComparison.Ordinal) || !name.EndsWith(Extension, StringComparison.Ordinal))
            return false;
        var snapshot = name.EndsWith(SnapshotExtension, StringComparison.Ordinal);
        var middle = name.AsSpan(prefix.Length, name.Length - prefix.Length - (snapshot ? SnapshotExtension : Extension).Length);
        if (middle.Length < 26 || middle[19] != '-'
            || !long.TryParse(middle[..19], NumberStyles.None, CultureInfo.InvariantCulture, out var step)
            || !long.TryParse(middle.Slice(20, 6), NumberStyles.None, CultureInfo.InvariantCulture, out var term))
            return false;
        var @base = long.MaxValue;
        if (middle.Length != 26
            && (middle.Length != 46 || middle[26] != '-' || !long.TryParse(middle[27..], NumberStyles.None, CultureInfo.InvariantCulture, out @base)))
            return false;
        if (snapshot && middle.Length == 26)
            return false;
        parsed = new StepObject(name, step, term, @base, snapshot);
        return true;
    }

    /// <summary>The step and term a sink's object was written under; false for an object
    /// that is not one of this prefix's.</summary>
    public static bool TryParse(string prefix, string name, out long step, out long term)
    {
        var known = TryParse(prefix, name, out StepObject parsed);
        (step, term) = (parsed.Step, parsed.Term);
        return known;
    }

    /// <summary>The objects under the prefix that are the sink's content, in the order to
    /// read them: the newest snapshot first if there is one, then one object per step
    /// above it — the reader's rules 1 to 3 applied.</summary>
    public static async Task<IReadOnlyList<string>> StepObjectsAsync(IObjectStore store, string prefix, CancellationToken cancellationToken = default) =>
        [.. Valid(await ListAsync(store, prefix, cancellationToken).ConfigureAwait(false)).Select(o => o.Name)];

    private static async Task<List<StepObject>> ListAsync(IObjectStore store, string prefix, CancellationToken cancellationToken)
    {
        var all = new List<StepObject>();
        await foreach (var info in store.ListAsync(prefix, cancellationToken).ConfigureAwait(false))
        {
            if (TryParse(prefix, info.Path, out StepObject parsed))
                all.Add(parsed);
        }
        return all;
    }

    /// <summary>Rules 1 to 3 over a listing: what is left, in reading order.</summary>
    internal static List<StepObject> Valid(IReadOnlyList<StepObject> listed)
    {
        StepObject? snapshot = null;
        // The lowest base each term's objects carry: what a term started from.
        var bases = new SortedDictionary<long, long>();
        foreach (var found in listed)
        {
            if (found.Snapshot)
            {
                if (snapshot is not { } held || found.Step > held.Step || (found.Step == held.Step && found.Term > held.Term))
                    snapshot = found;
            }
            else if (!bases.TryGetValue(found.Term, out var lowest) || found.Base < lowest)
                bases[found.Term] = found.Base;
        }
        // For each term, the lowest base of any later term: a step above it was run again.
        var rerunAbove = new Dictionary<long, long>();
        var later = long.MaxValue;
        foreach (var (term, lowest) in bases.Reverse())
        {
            rerunAbove[term] = later;
            later = Math.Min(later, lowest);
        }
        var perStep = new SortedDictionary<long, StepObject>();
        foreach (var found in listed)
        {
            if (found.Snapshot || (snapshot is { } from && found.Step <= from.Step) || found.Step > rerunAbove[found.Term])
                continue;
            if (!perStep.TryGetValue(found.Step, out var held) || found.Term > held.Term)
                perStep[found.Step] = found;
        }
        var valid = new List<StepObject>(perStep.Count + 1);
        if (snapshot is { } first)
            valid.Add(first);
        valid.AddRange(perStep.Values);
        return valid;
    }

    /// <summary>
    /// Deletes the objects of steps above <paramref name="step"/> — a generation's
    /// clean-up before its first write, and with 0 a reset — and hands every object it
    /// leaves to <paramref name="kept"/>, in name order. A page of names at a time.
    /// </summary>
    internal static async Task DeleteAboveAsync(
        IObjectStore store, string prefix, long step, Func<StepObject, CancellationToken, ValueTask>? kept, CancellationToken cancellationToken)
    {
        string? after = null;
        while (true)
        {
            var page = await store.ListPageAsync(prefix, after, 1024, cancellationToken).ConfigureAwait(false);
            foreach (var info in page.Objects)
            {
                after = info.Path;
                if (!TryParse(prefix, info.Path, out StepObject found))
                    continue;
                if (found.Step > step)
                    await store.DeleteAsync(info.Path, cancellationToken).ConfigureAwait(false);
                else if (kept is not null)
                    await kept(found, cancellationToken).ConfigureAwait(false);
            }
            if (!page.More || page.Objects.Count == 0)
                return;
        }
    }

    /// <summary>
    /// Folds an upsert sink's objects at or below <paramref name="upTo"/> — a step the
    /// checkpoint covers, so one that is never run again — into one snapshot, then deletes
    /// them. Returns how many step objects are left above the snapshot.
    /// <para>The objects are read newest first and a key's first line is its latest, so
    /// what is held is the set of keys, not the rows; a key whose latest line deletes it
    /// is left out. The snapshot is written whole before anything is deleted, so a reader
    /// sees either the objects or the snapshot that replaces them (rules 1 and 5). If an
    /// object goes missing while it is being folded — another writer folded it — nothing
    /// is written.</para>
    /// </summary>
    internal static async Task<int> CompactAsync<TKey>(
        IObjectStore store, string prefix, long upTo, long term, JsonSerializerOptions json, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var listed = await ListAsync(store, prefix, cancellationToken).ConfigureAwait(false);
        var valid = Valid(listed);
        var folded = valid.Where(o => o.Step <= upTo).ToList();
        var above = valid.Count - folded.Count;
        if (folded.Count < 2)
            return above + folded.Count(o => !o.Snapshot);
        var snapshot = new StepObject(SnapshotName(prefix, folded[^1].Step, term), folded[^1].Step, term, folded[^1].Step, true);
        try
        {
            await store.WriteAsync(snapshot.Name, async (stream, ct) =>
            {
                var seen = new HashSet<TKey>();
                for (var i = folded.Count - 1; i >= 0; i--)
                {
                    await foreach (var line in LinesAsync(store, folded[i].Name, missingIsEmpty: false, ct).ConfigureAwait(false))
                    {
                        var change = JsonSerializer.Deserialize<UpsertLine<TKey, JsonElement>>(line, json)!;
                        if (change.Key is null || !seen.Add(change.Key) || change.Deleted)
                            continue;
                        await stream.WriteAsync(Encoding.UTF8.GetBytes(line), ct).ConfigureAwait(false);
                        await stream.WriteAsync("\n"u8.ToArray(), ct).ConfigureAwait(false);
                    }
                }
            }, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            return above + folded.Count(o => !o.Snapshot);
        }
        // Everything the snapshot stands for goes — and every void object with it, at
        // whatever step: what makes one void is an object of a later term, and that
        // object may be among the ones folded here.
        var kept = new HashSet<string>(valid.Skip(folded.Count).Select(o => o.Name), StringComparer.Ordinal) { snapshot.Name };
        foreach (var found in listed)
        {
            if (!kept.Contains(found.Name))
                await store.DeleteAsync(found.Name, cancellationToken).ConfigureAwait(false);
        }
        return above;
    }

    /// <summary>Every record an append sink wrote under the prefix, in step order.</summary>
    public static async IAsyncEnumerable<T> ReadAppendsAsync<T>(
        IObjectStore store, string prefix, JsonSerializerOptions? json = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var name in await StepObjectsAsync(store, prefix, cancellationToken).ConfigureAwait(false))
        {
            // Listed, then deleted by the sink's own clean-up or its retention: skipped.
            await foreach (var line in LinesAsync(store, name, missingIsEmpty: true, cancellationToken).ConfigureAwait(false))
                yield return JsonSerializer.Deserialize<T>(line, json ?? Web)!;
        }
    }

    /// <summary>What an upsert sink's objects under the prefix add up to: each key's
    /// latest row, the deleted keys gone.</summary>
    public static async Task<Dictionary<TKey, TRow>> ReadUpsertsAsync<TKey, TRow>(
        IObjectStore store, string prefix, JsonSerializerOptions? json = null, CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        for (var attempt = 0; ; attempt++)
        {
            var rows = new Dictionary<TKey, TRow>();
            try
            {
                foreach (var name in await StepObjectsAsync(store, prefix, cancellationToken).ConfigureAwait(false))
                {
                    await foreach (var line in LinesAsync(store, name, missingIsEmpty: false, cancellationToken).ConfigureAwait(false))
                    {
                        var change = JsonSerializer.Deserialize<UpsertLine<TKey, TRow>>(line, json ?? Web)!;
                        if (change.Deleted)
                            rows.Remove(change.Key!);
                        else
                            rows[change.Key!] = change.Row!;
                    }
                }
                return rows;
            }
            catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException && attempt < 20)
            {
                // Rule 5: an object was folded into a snapshot, or deleted to be run
                // again, between the listing and the read. The listing is stale: again.
            }
        }
    }

    internal sealed record UpsertLine<TKey, TRow>(
        [property: JsonPropertyName("key"), JsonRequired] TKey? Key,
        [property: JsonPropertyName("row")] TRow? Row,
        [property: JsonPropertyName("deleted")] bool Deleted = false);

    private static async IAsyncEnumerable<string> LinesAsync(
        IObjectStore store, string name, bool missingIsEmpty, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Stream stream;
        try
        {
            stream = await store.OpenReadAsync(name, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (missingIsEmpty && ex is FileNotFoundException or DirectoryNotFoundException)
        {
            yield break;
        }
        await using (stream.ConfigureAwait(false))
        {
            using var reader = new StreamReader(stream, Encoding.UTF8);
            while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
            {
                if (line.Length > 0)
                    yield return line;
            }
        }
    }
}

/// <summary>
/// The position of a <c>files</c> source, as the checkpoint's partition → offset map can
/// hold it. An input's position is one non-decreasing number per partition, and this
/// source's is an object's name and a line. So objects are numbered as the source moves
/// to them (the ordinal), and:
/// <list type="bullet">
/// <item>partition 0 is <c>ordinal · 2³² + line</c>, stamped on every record;</item>
/// <item>partition 1 is <c>ordinal · 2³² + length</c> of the name of object
/// <c>ordinal</c>, in UTF-8 bytes;</item>
/// <item>partitions <c>2 + bank · 64 + i</c> are <c>ordinal · 2³² + </c> the name's bytes
/// <c>4i … 4i+3</c>, where <c>bank</c> is the ordinal's parity.</item>
/// </list>
/// The name's parts are advanced before partition 1 and that before the object's first
/// record, all through the input's channel, so whatever prefix of them a checkpoint holds,
/// partition 1 names an object whose name is whole (the two banks keep the previous name
/// intact while the next is written), and partition 0 says how far into it — or, with a
/// lower ordinal, that it has not been started.
/// </summary>
internal static class FilePosition
{
    public const int MaxNameBytes = 256;
    private const int Chunks = MaxNameBytes / 4;

    public static long Line(long ordinal, long line) => (ordinal << 32) | line;

    /// <summary>The advances that record object <paramref name="ordinal"/>'s name, in the
    /// order they must be sent.</summary>
    public static IEnumerable<(int Partition, long Offset)> Name(long ordinal, string name)
    {
        var bytes = Encoding.UTF8.GetBytes(name);
        if (bytes.Length > MaxNameBytes)
            throw new InvalidOperationException(
                $"Object '{name}' has a name of {bytes.Length} bytes; the files source can checkpoint names of up to {MaxNameBytes}.");
        var padded = new byte[(bytes.Length + 3) / 4 * 4];
        bytes.CopyTo(padded, 0);
        var bank = (int)(ordinal & 1);
        for (var i = 0; i < padded.Length / 4; i++)
            yield return (2 + bank * Chunks + i, (ordinal << 32) | BinaryPrimitives.ReadUInt32BigEndian(padded.AsSpan(i * 4)));
        yield return (1, (ordinal << 32) | (uint)bytes.Length);
    }

    /// <summary>The highest ordinal any committed partition carries. It can be above the
    /// one partition 1 names: a checkpoint may hold the first parts of the next object's
    /// name and not yet its length.</summary>
    public static long Highest(IReadOnlyDictionary<int, long> committed)
    {
        long highest = 0;
        foreach (var offset in committed.Values)
            highest = Math.Max(highest, offset >> 32);
        return highest;
    }

    /// <summary>
    /// The ordinal for the object after <paramref name="current"/>, given the
    /// <paramref name="highest"/> ordinal ever sent. It is above every ordinal sent, so
    /// that no part of the new name is at or below something a checkpoint already holds
    /// and dropped for it (an input admits only offsets above its position) — which is
    /// what would mix two names when the object read after a restart is not the one whose
    /// name was half committed before it. And it has the other parity than
    /// <paramref name="current"/>, so the new name goes to the bank the current name is
    /// not in.
    /// </summary>
    public static long Next(long current, long highest)
    {
        var next = highest + 1;
        if (((next ^ current) & 1) == 0)
            next++;
        if (next > int.MaxValue)
            throw new InvalidOperationException("The files source has numbered 2³¹ objects; reset the pipeline to start the numbering again.");
        return next;
    }

    /// <summary>The committed position as a person reads it: <c>object:line</c>, the
    /// line being the last one consumed, counted from 1 (0: none of it yet). Null with
    /// nothing committed.</summary>
    public static string? Describe(IReadOnlyDictionary<int, long> committed)
    {
        try
        {
            return Decode(committed) is { } at ? $"{at.Name}:{at.Line + 1}" : null;
        }
        catch (InvalidOperationException)
        {
            return null;
        }
    }

    /// <summary>The committed position: the ordinal and name of the object the source had
    /// moved to, and the last line of it that is consumed (-1: none). Null with nothing
    /// committed.</summary>
    public static (long Ordinal, string Name, long Line)? Decode(IReadOnlyDictionary<int, long> committed)
    {
        if (!committed.TryGetValue(1, out var header))
            return null;
        var ordinal = header >> 32;
        var length = (int)(header & 0xFFFFFFFF);
        var bytes = new byte[(length + 3) / 4 * 4];
        var bank = (int)(ordinal & 1);
        for (var i = 0; i < bytes.Length / 4; i++)
        {
            if (!committed.TryGetValue(2 + bank * Chunks + i, out var chunk) || chunk >> 32 != ordinal)
                throw new InvalidOperationException(
                    "The committed position of the files source is not whole: the checkpoint names an object whose name it does not hold. " +
                    "Reset the pipeline to read the prefix again from its start.");
            BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan(i * 4), (uint)(chunk & 0xFFFFFFFF));
        }
        var line = committed.TryGetValue(0, out var at) && at >> 32 == ordinal ? at & 0xFFFFFFFF : -1;
        return (ordinal, Encoding.UTF8.GetString(bytes, 0, length), line);
    }
}

/// <summary>Reads the objects under a prefix, in name order, line by line, into an input.
/// It holds one line — of at most <c>maxLineLength</c> characters — and a bounded list of
/// names at a time, and asks the store only for names after the one it is at.</summary>
internal sealed class FileSourceReader : IRunningSource
{
    private const int ListAhead = 1024;

    private readonly IObjectStore _store;
    private readonly FilesSlotConfig _config;
    private readonly Func<string, long, CancellationToken, ValueTask> _send;
    private readonly Func<object?, Exception, long, CancellationToken, ValueTask> _reject;
    private readonly Func<int, long, CancellationToken, ValueTask> _advance;
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _run;
    private long _records;
    private long _unreadable;
    private long _offset = -1;
    private long _unread;
    private volatile string? _at;
    private volatile string? _waitingFor;

    // settle, without comparing this node's clock with the store's: the newest
    // modification time any listing has shown (the store's clock against itself), and for
    // the objects not yet old by that measure, what each looked like and when this reader
    // first saw it so (this node's clock against itself). Only the objects of the last
    // listing that were not read are held: a page or two of names.
    private DateTimeOffset _newestModified = DateTimeOffset.MinValue;
    private Dictionary<string, (string Identity, long FirstSeen)> _sightings = new(StringComparer.Ordinal);

    public FileSourceReader(
        IObjectStore store, FilesSlotConfig config, IReadOnlyDictionary<int, long> committed,
        Func<string, long, CancellationToken, ValueTask> send,
        Func<object?, Exception, long, CancellationToken, ValueTask> reject,
        Func<int, long, CancellationToken, ValueTask> advance)
    {
        _store = store;
        _config = config;
        _send = send;
        _reject = reject;
        _advance = advance;
        // Decoded here, on the caller's thread: a checkpoint that cannot be read fails the start.
        var position = FilePosition.Decode(committed);
        if (committed.TryGetValue(0, out var offset))
            _offset = offset;
        var highest = FilePosition.Highest(committed);
        if (position is null)
            _waitingFor = $"objects under {config.Prefix} in store {config.Store}";     // until a listing shows one
        _run = Task.Run(() => RunAsync(position, highest, _stop.Token));
        Completion = _run.ContinueWith(
            t => t.IsFaulted && !_stop.IsCancellationRequested ? Task.FromException(t.Exception!.GetBaseException()) : Task.CompletedTask,
            TaskScheduler.Default).Unwrap();
    }

    public Task Completion { get; }

    public SourceStats Stats => new(
        Interlocked.Read(ref _records), Interlocked.Read(ref _unreadable),
        new Dictionary<int, long> { [0] = Interlocked.Read(ref _offset) }, Interlocked.Read(ref _unread)) { At = _at, WaitingFor = _waitingFor };

    public static T Read<T>(string text, JsonSerializerOptions json) =>
        JsonSerializer.Deserialize<T>(text, json) ?? throw new JsonException("The line is null.");

    private async Task RunAsync((long Ordinal, string Name, long Line)? position, long highest, CancellationToken stop)
    {
        var ordinal = position?.Ordinal ?? 0;
        var current = position?.Name;
        _at = current is null ? null : $"{current}:{position!.Value.Line + 1}";

        // The committed object may have lines left: those first. It was old enough when it
        // was started, and it is not listed again — a listing starts after it.
        if (position is { } resumed)
            await ReadObjectAsync(resumed.Name, ordinal, resumed.Line + 1, stop).ConfigureAwait(false);

        while (true)
        {
            stop.ThrowIfCancellationRequested();
            var (next, unread, empty) = await NextAsync(current, stop).ConfigureAwait(false);
            var listed = Stopwatch.GetTimestamp();
            Interlocked.Exchange(ref _unread, unread);
            // Nothing read and nothing there: the prefix may be a typing mistake, and a
            // source that waits on one looks exactly like a source with no input yet.
            _waitingFor = current is null && empty ? $"objects under {_config.Prefix} in store {_config.Store}" : null;
            if (next.Count == 0)
            {
                await Task.Delay(_config.Poll, stop).ConfigureAwait(false);
                continue;
            }
            foreach (var name in next)
            {
                // A new object: its name goes into the checkpoint before its first line.
                ordinal = highest = FilePosition.Next(ordinal, highest);
                foreach (var (partition, offset) in FilePosition.Name(ordinal, name))
                    await _advance(partition, offset, stop).ConfigureAwait(false);
                current = name;
                await ReadObjectAsync(name, ordinal, 0, stop).ConfigureAwait(false);
                Interlocked.Decrement(ref _unread);

                // A listing is good for one poll interval. With a backlog the names it
                // gave can take long to read, and an object that arrives meanwhile with a
                // name among them is only seen by asking again before passing it.
                if (Stopwatch.GetElapsedTime(listed) >= _config.Poll)
                    break;
            }
        }
    }

    /// <summary>
    /// The next names to read, in order, at most <see cref="ListAhead"/> of them; how many
    /// unread objects were seen (a floor: the listing is not followed to its end); and
    /// whether the prefix showed nothing at all. The listing starts after
    /// <paramref name="current"/> and stops at the first object that has not been left
    /// alone for <c>settle</c>: nothing after it is read before it, which is what lets an
    /// object arrive a little behind a greater name and still be read in its place.
    /// <para>
    /// "Left alone for <c>settle</c>" is never this node's clock against the store's: a
    /// node whose clock runs ahead of the store's would call every object old the moment
    /// it appears, and <c>settle</c> would be off with nothing to say so. An object has
    /// settled when the store's own stamps say so — a listing has shown an object
    /// modified at least <c>settle</c> after it — or when this reader has seen it,
    /// unchanged, for <c>settle</c> of its own time. The first serves a backlog, which is
    /// read as fast as it lists; the second is what the newest objects wait out.
    /// </para>
    /// </summary>
    private async Task<(List<string> Names, long Unread, bool Empty)> NextAsync(string? current, CancellationToken stop)
    {
        var names = new List<string>();
        var settle = _config.SettleFor;
        var after = current;
        long unread = 0;
        var empty = true;
        var now = Stopwatch.GetTimestamp();
        var sightings = new Dictionary<string, (string Identity, long FirstSeen)>(StringComparer.Ordinal);
        try
        {
            while (true)
            {
                var page = await _store.ListPageAsync(_config.Prefix!, after, ListAhead, stop).ConfigureAwait(false);
                if (page.Objects.Count > 0)
                    empty = false;

                // The store's newest stamp first, over the whole page: an object is old
                // against anything listed, wherever in name order that is.
                foreach (var info in page.Objects)
                {
                    if (info.Modified > _newestModified)
                        _newestModified = info.Modified;
                }
                var held = false;
                foreach (var info in page.Objects)
                {
                    after = info.Path;
                    if (_config.Suffix is { } suffix && !info.Path.EndsWith(suffix, StringComparison.Ordinal))
                        continue;
                    unread++;
                    var settled = settle <= TimeSpan.Zero || _newestModified - info.Modified >= settle;
                    if (!settled)
                    {
                        // Seen before and unchanged since: the wait counts from then.
                        var first = _sightings.TryGetValue(info.Path, out var seen) && seen.Identity == info.Identity ? seen.FirstSeen : now;
                        sightings[info.Path] = (info.Identity, first);
                        settled = Stopwatch.GetElapsedTime(first, now) >= settle;
                    }
                    if (held || names.Count == ListAhead)
                        continue;
                    if (!settled)
                        held = true;            // and everything after it waits with it
                    else
                        names.Add(info.Path);
                }
                // Another page only while it could still add a name: with a suffix a page can
                // hold none that match.
                if (held || names.Count == ListAhead || !page.More || page.Objects.Count == 0)
                    return (names, unread, empty);
            }
        }
        finally
        {
            _sightings = sightings;
        }
    }

    private async Task ReadObjectAsync(string name, long ordinal, long from, CancellationToken stop)
    {
        Stream stream;
        try
        {
            stream = await _store.OpenReadAsync(name, stop).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            return;                 // listed and then deleted: nothing of it is left to read
        }
        await using (stream.ConfigureAwait(false))
        {
            using var reader = new StreamReader(stream, Encoding.UTF8);
            var lines = new BoundedLineReader(reader, _config.LineLimit);
            long line = -1;
            while (await lines.ReadLineAsync(stop).ConfigureAwait(false) is { } read)
            {
                line++;
                if (line < from || (!read.TooLong && string.IsNullOrWhiteSpace(read.Text)))
                    continue;
                if (line > uint.MaxValue)
                    throw new InvalidOperationException($"Object '{name}' has more than {uint.MaxValue} lines.");
                var offset = FilePosition.Line(ordinal, line);
                if (read.TooLong)
                {
                    // Not held, not parsed: its first characters and its place are what is reported.
                    Interlocked.Increment(ref _unreadable);
                    await _reject(
                        new FileRecord(name, line, read.Text),
                        new FormatException(
                            $"{name}:{line + 1} is a line of {read.Length} characters; the source reads lines of up to {_config.LineLimit} (maxLineLength). " +
                            "An object that is not JSON lines does not belong under the prefix."),
                        offset, stop).ConfigureAwait(false);
                }
                else
                {
                    try
                    {
                        await _send(read.Text, offset, stop).ConfigureAwait(false);
                        Interlocked.Increment(ref _records);
                    }
                    catch (Exception ex) when (ex is JsonException or NotSupportedException)
                    {
                        Interlocked.Increment(ref _unreadable);
                        // The position is in the message too: an input's offset says nothing to a person.
                        await _reject(new FileRecord(name, line, read.Text), new FormatException($"{name}:{line + 1} is not a record: {ex.Message}", ex), offset, stop)
                            .ConfigureAwait(false);
                    }
                }
                Interlocked.Exchange(ref _offset, offset);
                _at = $"{name}:{line + 1}";
            }
        }
    }

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
        _stop.Dispose();
    }
}

/// <summary>One line of a text, or the start of one that is longer than the limit.</summary>
internal readonly record struct LineRead(string Text, long Length, bool TooLong);

/// <summary>
/// <see cref="TextReader.ReadLine"/> with a bound: a line of more than
/// <paramref name="limit"/> characters is read past without being held, and returned as
/// its first <see cref="Kept"/> characters and its length. Lines end at <c>\n</c>,
/// <c>\r\n</c> or <c>\r</c>, as they do for <see cref="TextReader"/>.
/// </summary>
internal sealed class BoundedLineReader(TextReader reader, int limit)
{
    public const int Kept = 4096;

    private readonly char[] _buffer = new char[16 * 1024];
    private readonly StringBuilder _line = new();
    private int _start;
    private int _end;
    private bool _skipLineFeed;

    /// <summary>The most characters of one line this reader has held at a time: bounded
    /// by the limit whatever the lines are.</summary>
    internal int Held { get; private set; }

    public async ValueTask<LineRead?> ReadLineAsync(CancellationToken cancellationToken)
    {
        _line.Clear();
        if (_line.Capacity > 64 * 1024)
            _line.Capacity = Kept;          // one long line does not keep its buffer for the rest
        long length = 0;
        var any = false;
        while (true)
        {
            if (_start == _end)
            {
                _start = 0;
                _end = await reader.ReadAsync(_buffer.AsMemory(), cancellationToken).ConfigureAwait(false);
                if (_end == 0)
                    return any ? Line(length) : null;
            }
            if (_skipLineFeed)
            {
                _skipLineFeed = false;
                if (_buffer[_start] == '\n')
                {
                    _start++;
                    continue;
                }
            }
            var span = _buffer.AsSpan(_start, _end - _start);
            var end = span.IndexOfAny('\r', '\n');
            var take = end < 0 ? span.Length : end;
            any = true;
            length += take;

            // A line within the limit is held whole; of a longer one, what the report shows.
            var room = (length > limit ? Kept : limit) - _line.Length;
            if (room > 0)
                _line.Append(span[..Math.Min(take, room)]);
            if (_line.Length > Held)
                Held = _line.Length;
            if (end < 0)
            {
                _start = _end;
                continue;
            }
            _skipLineFeed = span[end] == '\r';
            _start += end + 1;
            return Line(length);
        }
    }

    private LineRead Line(long length)
    {
        var tooLong = length > limit;
        if (tooLong && _line.Length > Kept)
            _line.Length = Kept;
        return new LineRead(_line.ToString(), length, tooLong);
    }
}
