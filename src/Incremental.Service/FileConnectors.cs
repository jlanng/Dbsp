using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Incremental.Service;

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
                $"the files config accepts connector, store, prefix, suffix, pollInterval, maxLineLength and minInterval — where the store is belongs to the host ({ex.Message})");
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
        if (parsed.MaxLineLength is < 1 or > MaxMaxLineLength)
            throw new InvalidOperationException($"maxLineLength is a number of characters from 1 to {MaxMaxLineLength}, not {parsed.MaxLineLength}");
        return parsed;
    }

    // A line is held whole, as text, while it is parsed: this is what one costs at most.
    public const int MaxMaxLineLength = 1 << 26;
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
        IPositionDescribing
{
    public ConfigSchema ConfigSchema { get; } = new(
    [
        new("store", "string", Required: true, Description: "An object store the host registered; it holds the root and the credentials."),
        new("prefix", "string", Required: true, Description: "The objects of this slot are those whose name starts with it. One slot of one pipeline per prefix."),
        new("suffix", "string", Description: "Sources: read only objects whose name ends with this."),
        new("pollInterval", "string", Description: "Sources: how often the prefix is listed for new objects, as a duration (hh:mm:ss).", Default: "00:00:05"),
        new("maxLineLength", "number", Description: "Sources: the longest line, in characters, read as a record. A longer one is rejected like a line that is not a record, without being held.", Default: "1048576"),
        new("minInterval", "string", Description: "Sinks: the least time between two objects, as a duration (hh:mm:ss). The pipeline pauses to keep it, so steps and objects grow.", Default: "00:00:00"),
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
        // all of them at a reset): the prefix is one sink slot's alone.
        return new ConnectorDescription($"objects under {parsed.Prefix} in store {parsed.Store}") { Owns = $"files:{parsed.Store}:{parsed.Prefix}" };
    }

    public string? DescribePosition(JsonElement config, IReadOnlyDictionary<int, long> committed) => FilePosition.Describe(committed);

    internal static void RequireKind(JsonElement json, SlotKind kind)
    {
        var config = FilesSlotConfig.Parse(json);
        if (kind is SlotKind.AppendSink or SlotKind.UpsertSink && (config.Suffix is not null || config.PollInterval is not null || config.MaxLineLength is not null))
            throw new InvalidOperationException("\"suffix\", \"pollInterval\" and \"maxLineLength\" are a source's settings");
        if (kind is SlotKind.AppendInput or SlotKind.KeyedInput && config.MinInterval is not null)
            throw new InvalidOperationException("\"minInterval\" is a sink's setting");
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
        return Task.FromResult<IRunningSource>(new FileSourceReader(
            store, config, context.CommittedOffsets,
            (text, offset, ct) => input.UpsertAsync(FileSourceReader.Read<TValue>(text, json), 0, offset, ct),
            (record, error, offset, ct) => input.RejectAsync(record, error, 0, offset, ct),
            input.AdvanceAsync));
    }

    IAppendSink<T> IAppendSinkConnector.Create<T>(SinkContext context)
    {
        var (config, store) = Resolve(context.Config);
        return new AppendSink<T>(store, config.Prefix!, config.Spacing, context);
    }

    IUpsertSink<TKey, TRow> IUpsertSinkConnector.Create<TKey, TRow>(SinkContext context)
    {
        var (config, store) = Resolve(context.Config);
        return new UpsertSink<TKey, TRow>(store, config.Prefix!, config.Spacing, context);
    }

    async Task IResettableSinkConnector.ResetAsync(SinkContext context, CancellationToken cancellationToken)
    {
        var (config, store) = Resolve(context.Config);
        await FileSinkFormat.DeleteAboveAsync(store, config.Prefix!, 0, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>What both sinks share: the clean-up a generation owes before its first
    /// write, and one object per step.</summary>
    private abstract class StepObjectSink(IObjectStore store, string prefix, TimeSpan spacing, SinkContext context)
    {
        private static readonly byte[] NewLine = "\n"u8.ToArray();

        // The checkpoint this generation started from. Read now and not at the first
        // write: by then the circuit may have committed steps that gave this sink
        // nothing, and an object an earlier generation left for one of them would pass
        // for committed.
        private readonly long _startedAt = context.CommittedStep ?? 0;
        private bool _cleaned;

        protected JsonSerializerOptions Json => context.Json;

        protected async ValueTask WriteStepAsync<TState>(
            long sequence, TState state, Func<TState, Stream, CancellationToken, ValueTask> write, CancellationToken cancellationToken)
        {
            if (!_cleaned)
            {
                // Steps above the checkpoint are about to be run again, and an auto-stepping
                // circuit may cut them differently: what an earlier generation wrote for
                // them is not the result.
                await FileSinkFormat.DeleteAboveAsync(store, prefix, _startedAt, cancellationToken).ConfigureAwait(false);
                _cleaned = true;
            }
            // Whole or not at all: a write that fails or is cancelled part-way leaves
            // nothing under the step's name (IObjectStore.WriteAsync).
            await store.WriteAsync(
                FileSinkFormat.ObjectName(prefix, sequence, context.Term), (stream, ct) => write(state, stream, ct), cancellationToken).ConfigureAwait(false);

            // The object is written at once — the lease was checked a moment ago — and
            // the acknowledgement is what waits: the circuit commits, and steps again,
            // only after it.
            if (spacing > TimeSpan.Zero)
                await Task.Delay(spacing, cancellationToken).ConfigureAwait(false);
        }

        protected static ValueTask EndLineAsync(Stream stream, CancellationToken cancellationToken) => stream.WriteAsync(NewLine, cancellationToken);
    }

    private sealed class AppendSink<T>(IObjectStore store, string prefix, TimeSpan spacing, SinkContext context)
        : StepObjectSink(store, prefix, spacing, context), IAppendSink<T>
    {
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
    }

    private sealed class UpsertSink<TKey, TRow>(IObjectStore store, string prefix, TimeSpan spacing, SinkContext context)
        : StepObjectSink(store, prefix, spacing, context), IUpsertSink<TKey, TRow>
        where TKey : notnull
    {
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
    }
}

/// <summary>
/// The names and lines the <c>files</c> sinks write, and how to read them back: the
/// reader's half of the format, for tests, for a job that loads the objects somewhere, and
/// as the definition of "the latest row per key".
/// </summary>
public static class FileSinkFormat
{
    private const string Extension = ".jsonl";

    // The default for a caller that gives no options. Never used by the service itself:
    // a pipeline's types go through its plugin version's own options.
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    /// <summary>The object a step is written to: <c>{prefix}{step:D19}-{term:D6}.jsonl</c>.</summary>
    public static string ObjectName(string prefix, long step, long term) =>
        string.Create(CultureInfo.InvariantCulture, $"{prefix}{step:D19}-{term:D6}{Extension}");

    /// <summary>The step and term a sink's object was written under; false for an object
    /// that is not one of this prefix's step objects.</summary>
    public static bool TryParse(string prefix, string name, out long step, out long term)
    {
        (step, term) = (0, 0);
        if (!name.StartsWith(prefix, StringComparison.Ordinal) || !name.EndsWith(Extension, StringComparison.Ordinal))
            return false;
        var middle = name.AsSpan(prefix.Length, name.Length - prefix.Length - Extension.Length);
        var dash = middle.IndexOf('-');
        return dash == 19
            && long.TryParse(middle[..dash], NumberStyles.None, CultureInfo.InvariantCulture, out step)
            && long.TryParse(middle[(dash + 1)..], NumberStyles.None, CultureInfo.InvariantCulture, out term);
    }

    /// <summary>The step objects under the prefix, in step order, one per step: where a
    /// step was written under two terms, the higher term's.</summary>
    public static async Task<IReadOnlyList<string>> StepObjectsAsync(IObjectStore store, string prefix, CancellationToken cancellationToken = default)
    {
        var perStep = new SortedDictionary<long, (long Term, string Name)>();
        await foreach (var info in store.ListAsync(prefix, cancellationToken).ConfigureAwait(false))
        {
            if (TryParse(prefix, info.Path, out var step, out var term) && (!perStep.TryGetValue(step, out var held) || term > held.Term))
                perStep[step] = (term, info.Path);
        }
        return [.. perStep.Values.Select(v => v.Name)];
    }

    internal static async Task DeleteAboveAsync(IObjectStore store, string prefix, long step, CancellationToken cancellationToken)
    {
        var doomed = new List<string>();
        await foreach (var info in store.ListAsync(prefix, cancellationToken).ConfigureAwait(false))
        {
            if (TryParse(prefix, info.Path, out var written, out _) && written > step)
                doomed.Add(info.Path);
        }
        foreach (var name in doomed)
            await store.DeleteAsync(name, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Every record an append sink wrote under the prefix, in step order.</summary>
    public static async IAsyncEnumerable<T> ReadAppendsAsync<T>(
        IObjectStore store, string prefix, JsonSerializerOptions? json = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var name in await StepObjectsAsync(store, prefix, cancellationToken).ConfigureAwait(false))
        {
            await foreach (var line in LinesAsync(store, name, cancellationToken).ConfigureAwait(false))
                yield return JsonSerializer.Deserialize<T>(line, json ?? Web)!;
        }
    }

    /// <summary>What an upsert sink's objects under the prefix add up to: each key's
    /// latest row, the deleted keys gone.</summary>
    public static async Task<Dictionary<TKey, TRow>> ReadUpsertsAsync<TKey, TRow>(
        IObjectStore store, string prefix, JsonSerializerOptions? json = null, CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        var rows = new Dictionary<TKey, TRow>();
        foreach (var name in await StepObjectsAsync(store, prefix, cancellationToken).ConfigureAwait(false))
        {
            await foreach (var line in LinesAsync(store, name, cancellationToken).ConfigureAwait(false))
            {
                var change = JsonSerializer.Deserialize<UpsertLine<TKey, TRow>>(line, json ?? Web)!;
                if (change.Deleted)
                    rows.Remove(change.Key);
                else
                    rows[change.Key] = change.Row!;
            }
        }
        return rows;
    }

    private sealed record UpsertLine<TKey, TRow>(
        [property: JsonPropertyName("key")] TKey Key,
        [property: JsonPropertyName("row")] TRow? Row,
        [property: JsonPropertyName("deleted")] bool Deleted = false);

    private static async IAsyncEnumerable<string> LinesAsync(IObjectStore store, string name, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Stream stream;
        try
        {
            stream = await store.OpenReadAsync(name, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            // Listed, then deleted by the sink's own clean-up: a step that is being run again.
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
/// names at a time.</summary>
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
        _run = Task.Run(() => RunAsync(position, highest, _stop.Token));
        Completion = _run.ContinueWith(
            t => t.IsFaulted && !_stop.IsCancellationRequested ? Task.FromException(t.Exception!.GetBaseException()) : Task.CompletedTask,
            TaskScheduler.Default).Unwrap();
    }

    public Task Completion { get; }

    public SourceStats Stats => new(
        Interlocked.Read(ref _records), Interlocked.Read(ref _unreadable),
        new Dictionary<int, long> { [0] = Interlocked.Read(ref _offset) }, Interlocked.Read(ref _unread)) { At = _at };

    public static T Read<T>(string text, JsonSerializerOptions json) =>
        JsonSerializer.Deserialize<T>(text, json) ?? throw new JsonException("The line is null.");

    private async Task RunAsync((long Ordinal, string Name, long Line)? position, long highest, CancellationToken stop)
    {
        var ordinal = position?.Ordinal ?? 0;
        var current = position?.Name;
        var resumeLine = position?.Line ?? -1;
        var resumeCurrent = current is not null;        // the committed object may have lines left
        _at = current is null ? null : $"{current}:{resumeLine + 1}";

        while (true)
        {
            stop.ThrowIfCancellationRequested();
            var (next, unread) = await NextAsync(current, resumeCurrent, stop).ConfigureAwait(false);
            var listed = Stopwatch.GetTimestamp();
            Interlocked.Exchange(ref _unread, unread);
            if (next.Count == 0)
            {
                await Task.Delay(_config.Poll, stop).ConfigureAwait(false);
                continue;
            }
            foreach (var name in next)
            {
                long from = 0;
                if (resumeCurrent && name == current)
                    from = resumeLine + 1;
                else
                {
                    // A new object: its name goes into the checkpoint before its first line.
                    ordinal = highest = FilePosition.Next(ordinal, highest);
                    foreach (var (partition, offset) in FilePosition.Name(ordinal, name))
                        await _advance(partition, offset, stop).ConfigureAwait(false);
                }
                resumeCurrent = false;
                current = name;
                await ReadObjectAsync(name, ordinal, from, stop).ConfigureAwait(false);
                Interlocked.Decrement(ref _unread);

                // A listing is good for one poll interval. With a backlog the names it
                // gave can take long to read, and an object that arrives meanwhile with a
                // name among them is only seen by listing again before passing it.
                if (Stopwatch.GetElapsedTime(listed) >= _config.Poll)
                    break;
            }
        }
    }

    /// <summary>The next names to read, in order, at most <see cref="ListAhead"/> of them,
    /// and how many unread objects the listing showed in all.</summary>
    private async Task<(List<string> Names, long Unread)> NextAsync(string? current, bool includeCurrent, CancellationToken stop)
    {
        var smallest = new SortedSet<string>(StringComparer.Ordinal);
        long unread = 0;
        await foreach (var info in _store.ListAsync(_config.Prefix!, stop).ConfigureAwait(false))
        {
            var name = info.Path;
            if (_config.Suffix is { } suffix && !name.EndsWith(suffix, StringComparison.Ordinal))
                continue;
            var order = current is null ? 1 : string.CompareOrdinal(name, current);
            if (order < 0 || (order == 0 && !includeCurrent))
                continue;
            unread++;
            smallest.Add(name);
            if (smallest.Count > ListAhead)
                smallest.Remove(smallest.Max!);
        }
        return ([.. smallest], unread);
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
