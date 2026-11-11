using System.Collections.Concurrent;
using System.Text.Json;
using Enrichment.Contracts;
using Incremental.Service;

namespace Incremental.EnrichmentService;

// The adapters a host writes for the systems only it knows: two table sources, one source
// connector and one sink connector. They are registered by name in Program.cs
// (AddTableSource, AddSourceConnector, AddSinkConnector) and named by specs.

// <snippet:table-source-positions>
/// <summary>
/// A table source for a feed with positions. It stamps every change with the feed's
/// position, and resumes from the positions the table's last committed version carries —
/// so a restart, or a takeover by another node, continues exactly where the table stands.
/// Registered as the loader kind <c>instrument-feed</c>.
/// </summary>
public sealed class InstrumentFeedSource(PositionedFeed<Instrument> feed) : ITableSource<int, Instrument>
{
    public async Task RunAsync(TableSourceContext<int, Instrument> context, CancellationToken cancellationToken)
    {
        // Partition 0, offset of the last change in the table; -1 when the table is new.
        var next = context.Table.CommittedOffsets.GetValueOrDefault(0, -1) + 1;
        while (true)
        {
            var published = feed.Published;
            while (next < published)
            {
                context.Report(TableSourceState.Streaming, $"feed@{next}", published - next);
                // One version per chunk: readers never see half of it.
                var end = Math.Min(published, next + 4096);
                for (; next < end; next++)
                    await context.Writer.UpsertAsync(feed.At(next), partition: 0, offset: next);
                await context.Writer.CommitAsync(cancellationToken);
            }
            context.Report(TableSourceState.Idle, $"feed@{next}", lag: 0);
            await feed.WaitForAsync(next, cancellationToken);
        }
    }
}
// </snippet:table-source-positions>

// <snippet:table-source-snapshot>
/// <summary>
/// A table source for a system with no positions. Every start makes the table equal to
/// the system's current rows (<c>ReplaceAllAsync</c> upserts them and deletes what they do
/// not mention), then applies changes as they are heard. Registered as the loader kind
/// <c>directory</c>.
/// </summary>
public sealed class DirectorySource<TKey, TRow>(LiveDirectory<TKey, TRow> directory) : ITableSource<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    public async Task RunAsync(TableSourceContext<TKey, TRow> context, CancellationToken cancellationToken)
    {
        context.Report(TableSourceState.Snapshotting);
        var (rows, change) = directory.Snapshot();
        await context.Writer.ReplaceAllAsync(rows, cancellationToken: cancellationToken);
        while (true)
        {
            var (changed, upTo, next) = directory.Since(change);
            if (changed.Count > 0)
            {
                foreach (var row in changed)
                    await context.Writer.UpsertAsync(row);
                await context.Writer.CommitAsync(cancellationToken);
            }
            change = upTo;
            context.Report(TableSourceState.Idle, $"change {change}");
            await next.WaitAsync(cancellationToken);
        }
    }
}
// </snippet:table-source-snapshot>

/// <summary>Table sources are created per table, with the table's key and row types; a
/// factory that serves one row type says so when asked for another.</summary>
public sealed class InstrumentFeedSourceFactory(PositionedFeed<Instrument> feed) : ITableSourceFactory
{
    public ITableSource<TKey, TRow> Create<TKey, TRow>(TableLoaderSpec loader)
        where TKey : notnull
        where TRow : class =>
        new InstrumentFeedSource(feed) as ITableSource<TKey, TRow>
        ?? throw new InvalidOperationException($"The instrument feed loads tables of int → Instrument, not {typeof(TKey).Name} → {typeof(TRow).Name}.");
}

public sealed class DirectorySourceFactory(LiveDirectory<int, Trader> traders, LiveDirectory<int, Desk> desks) : ITableSourceFactory
{
    public ITableSource<TKey, TRow> Create<TKey, TRow>(TableLoaderSpec loader)
        where TKey : notnull
        where TRow : class =>
        new DirectorySource<int, Trader>(traders) as ITableSource<TKey, TRow>
        ?? new DirectorySource<int, Desk>(desks) as ITableSource<TKey, TRow>
        ?? throw new InvalidOperationException($"The directory holds traders and desks, not {typeof(TRow).Name}.");
}

// <snippet:source-connector>
/// <summary>
/// A source connector for the trade feed, as an append input (every record an execution)
/// and as a keyed input (a later record for an id amends the trade). It resumes from the
/// circuit's committed offsets, which the service hands it after the circuit has started.
/// Registered as the connector <c>trade-feed</c>; a spec binds a slot to it with
/// <c>{ "connector": "trade-feed" }</c>.
/// </summary>
public sealed class TradeFeedConnector(PositionedFeed<Trade> feed) : IAppendSourceConnector, IKeyedSourceConnector, IConfigDescribed
{
    // Called at the dry run: check the slot's config here, and name any secret properties.
    public ConnectorDescription Describe(JsonElement config) => new("the example's generated trade feed");

    // Optional (IConfigDescribed): what the config accepts, for whoever composes a spec.
    // The admin API lists it under "connectors", and a spec template is built from it.
    public ConfigSchema ConfigSchema { get; } = new([], "The example's generated trade feed. It takes no config.");

    public Task<IRunningSource> StartAsync<T>(SourceContext<T> context, CancellationToken cancellationToken) where T : notnull
    {
        var from = context.CommittedOffsets.GetValueOrDefault(0, -1) + 1;
        return Task.FromResult<IRunningSource>(new Reader(feed, from,
            (trade, offset, ct) => context.Input.InsertAsync(Typed<T>(trade), 0, offset, ct)));
    }

    public Task<IRunningSource> StartAsync<TKey, TValue>(KeyedSourceContext<TKey, TValue> context, CancellationToken cancellationToken)
        where TKey : notnull
        where TValue : notnull
    {
        var from = context.CommittedOffsets.GetValueOrDefault(0, -1) + 1;
        return Task.FromResult<IRunningSource>(new Reader(feed, from,
            (trade, offset, ct) => context.Input.UpsertAsync(Typed<TValue>(trade), 0, offset, ct)));
    }

    // The slot's type is the definition's; this feed carries one type.
    private static T Typed<T>(Trade trade) =>
        trade is T typed ? typed : throw new InvalidOperationException($"The trade feed carries {nameof(Trade)}, and the slot wants {typeof(T).Name}.");

    private sealed class Reader : IRunningSource
    {
        private readonly PositionedFeed<Trade> _feed;
        private readonly CancellationTokenSource _stop = new();
        private readonly Task _run;
        private readonly long _from;
        private long _next;

        public Reader(PositionedFeed<Trade> feed, long from, Func<Trade, long, CancellationToken, ValueTask> send)
        {
            _feed = feed;
            _from = from;
            _next = from;
            _run = Task.Run(async () =>
            {
                while (true)
                {
                    await feed.WaitForAsync(_next, _stop.Token);
                    // The input is a bounded channel: a slow circuit makes this wait.
                    await send(feed.At(_next), _next, _stop.Token);
                    Interlocked.Increment(ref _next);
                }
            });
            // A fault here faults the pipeline, which restarts it under its restart policy.
            Completion = _run.ContinueWith(
                t => t.IsFaulted && !_stop.IsCancellationRequested ? Task.FromException(t.Exception!.GetBaseException()) : Task.CompletedTask,
                TaskScheduler.Default).Unwrap();
        }

        public Task Completion { get; }

        public SourceStats Stats
        {
            get
            {
                var next = Interlocked.Read(ref _next);
                return new SourceStats(next - _from, 0, new Dictionary<int, long> { [0] = next - 1 }, Math.Max(0, _feed.Published - next));
            }
        }

        public async ValueTask DisposeAsync()
        {
            _stop.Cancel();
            try
            {
                await _run;
            }
            catch
            {
                // Cancellation, or the fault Completion already carries.
            }
        }
    }
}
// </snippet:source-connector>

// <snippet:sink-connector>
/// <summary>
/// A sink connector: an upsert sink that keeps each key's latest row as JSON, to be read
/// at <c>/views/{name}</c>. It stands in for a database table or a cache. Rows are
/// serialized with the pipeline's own options (<c>context.Json</c>), never a process-wide
/// instance, so a plugin version's types can be unloaded with it. Registered as the
/// connector <c>view</c>: <c>{ "connector": "view", "name": "enriched" }</c>.
/// </summary>
public sealed class ViewConnector(Views views) : IUpsertSinkConnector, IResettableSinkConnector, IConfigDescribed
{
    public ConnectorDescription Describe(JsonElement config) => new($"the in-process view '{NameOf(config, "…")}'");

    public ConfigSchema ConfigSchema { get; } = new(
        [new("name", "string", Description: "The view's name under /views; \"{pipeline}/{slot}\" when left out.")],
        "An in-process view of each key's latest row.");

    public IUpsertSink<TKey, TRow> Create<TKey, TRow>(SinkContext context) where TKey : notnull =>
        new Sink<TKey, TRow>(views.Open(NameOf(context.Config, $"{context.PipelineId}/{context.Slot}")), context.Json);

    // A reset pipeline computes its results again from nothing; what the old run wrote goes.
    public Task ResetAsync(SinkContext context, CancellationToken cancellationToken)
    {
        views.Open(NameOf(context.Config, $"{context.PipelineId}/{context.Slot}")).Clear();
        return Task.CompletedTask;
    }

    private static string NameOf(JsonElement config, string fallback) =>
        config.ValueKind == JsonValueKind.Object && config.TryGetProperty("name", out var name) && name.GetString() is { Length: > 0 } given ? given : fallback;

    private sealed class Sink<TKey, TRow>(View view, JsonSerializerOptions json) : IUpsertSink<TKey, TRow> where TKey : notnull
    {
        // One batch per step, at most one upsert or delete per key. Returning is the
        // acknowledgement: the step's checkpoint is committed only after this returns.
        public ValueTask WriteAsync(UpsertBatch<TKey, TRow> batch, CancellationToken cancellationToken)
        {
            foreach (var key in batch.Deletes)
                view.Delete(JsonSerializer.Serialize(key, json));
            foreach (var (key, row) in batch.Upserts)
                view.Upsert(JsonSerializer.Serialize(key, json), JsonSerializer.Serialize(row, json));
            return ValueTask.CompletedTask;
        }
    }
}
// </snippet:sink-connector>

/// <summary>What the <c>view</c> sinks hold, by name: key (as JSON) → row (as JSON).</summary>
public sealed class Views
{
    private readonly ConcurrentDictionary<string, View> _views = new(StringComparer.Ordinal);

    public IEnumerable<string> Names => _views.Keys.Order(StringComparer.Ordinal);

    public View Open(string name) => _views.GetOrAdd(name, _ => new View());

    public View? Find(string name) => _views.GetValueOrDefault(name);
}

/// <summary>One view. It is a demonstration, not a store: past <see cref="Capacity"/> keys
/// it forgets the keys inserted longest ago, so a host left running for days stays small.</summary>
public sealed class View
{
    public const int Capacity = 100_000;

    // Each key the view holds, with its place in the order of insertion. A deleted key
    // leaves the order with its row, so the order's length is the number of keys held —
    // not the number ever inserted — and nothing is forgotten below the capacity.
    private readonly Dictionary<string, (string Row, LinkedListNode<string> Place)> _rows = new(StringComparer.Ordinal);
    private readonly LinkedList<string> _order = new();

    public long Writes { get; private set; }

    public void Upsert(string key, string row)
    {
        lock (_rows)
        {
            Writes++;
            if (_rows.TryGetValue(key, out var held))
            {
                _rows[key] = (row, held.Place);
                return;
            }
            _rows[key] = (row, _order.AddLast(key));
            while (_order.Count > Capacity)
            {
                _rows.Remove(_order.First!.Value);
                _order.RemoveFirst();
            }
        }
    }

    public void Delete(string key)
    {
        lock (_rows)
        {
            Writes++;
            if (_rows.Remove(key, out var held))
                _order.Remove(held.Place);
        }
    }

    public void Clear()
    {
        lock (_rows)
        {
            _rows.Clear();
            _order.Clear();
        }
    }

    public int Count
    {
        get
        {
            lock (_rows)
                return _rows.Count;
        }
    }

    public Dictionary<string, string> Rows()
    {
        lock (_rows)
            return _rows.ToDictionary(r => r.Key, r => r.Value.Row, StringComparer.Ordinal);
    }

    public List<KeyValuePair<string, string>> Page(int limit)
    {
        lock (_rows)
            return _rows.OrderBy(r => r.Key, StringComparer.Ordinal).Take(limit)
                .Select(r => new KeyValuePair<string, string>(r.Key, r.Value.Row)).ToList();
    }
}
