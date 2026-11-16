using System.Text.Json;
using Incremental.Storage;
using Incremental.Tables;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Incremental.Service.Testing;

/// <summary>
/// Runs one pipeline definition in a test, with no node, no catalog and no HTTP: the
/// definition is bound to its settings and built exactly as the service builds it, its
/// tables are in-memory tables the test fills, its inputs are fed by hand, and its sinks
/// are in memory. Steps are taken by the test (<see cref="StepAsync"/>), and a step
/// returns only when every sink has what the step produced, so a test is: change the
/// world, step, look.
/// <code>
/// await using var pipeline = await PipelineTestHarness.For&lt;TradeEnrichment&gt;()
///     .Settings("""{ "minNotional": 1000 }""")
///     .Table(Reference.Instruments, [new Instrument(1, "ABC", …)])
///     .StartAsync();
/// await pipeline.KeyedInput&lt;long, Trade&gt;("trades").UpsertAsync(trade);
/// await pipeline.StepAsync();
/// Assert.Equal(expected, pipeline.UpsertSink&lt;long, EnrichedTrade&gt;("enriched").Rows[trade.Id]);
/// </code>
/// What it does not cover is what the service adds round a circuit: connectors, restarts
/// from a checkpoint, replacing a version on kept state, leadership. Those are the
/// service's own tests; a definition's test is about what the definition computes.
/// </summary>
public sealed class PipelineTestHarness : IAsyncDisposable
{
    private readonly Circuit _circuit;
    private readonly PipelineBuilder _pipeline;
    private readonly MemoryConnector _memory = new();
    private readonly List<Task> _deliveries = [];
    private readonly List<DeadLetter> _deadLetters = [];
    private readonly CancellationTokenSource _stop = new();
    private readonly string _id;
    private readonly InMemoryCheckpointStorageProvider _storage;
    private readonly long? _maxStateEntries;
    private StateLimitExceededException? _overLimit;

    /// <summary>Starts describing a harness for the definition class
    /// <typeparamref name="TDefinition"/>.</summary>
    public static Builder For<TDefinition>() where TDefinition : class, new() => new(typeof(TDefinition));

    private PipelineTestHarness(string id, Circuit circuit, PipelineBuilder pipeline, JsonSerializerOptions json, InMemoryCheckpointStorageProvider storage, long? maxStateEntries)
    {
        _maxStateEntries = maxStateEntries;
        _id = id;
        _storage = storage;
        _circuit = circuit;
        _pipeline = pipeline;
        Json = json;
    }

    /// <summary>The slots and tables the definition declared under these settings.</summary>
    public PipelineShape Shape => _pipeline.Shape;

    /// <summary>The circuit itself, for what the harness does not wrap.</summary>
    public Circuit Circuit => _circuit;

    /// <summary>The options the service serializes a pipeline's records with.</summary>
    public JsonSerializerOptions Json { get; }

    /// <summary>The append input declared as <c>p.Input&lt;T&gt;(slot)</c>. Records sent are
    /// processed by the next <see cref="StepAsync"/>.</summary>
    public InputHandle<T> Input<T>(string slot) where T : notnull =>
        Slot(slot).Stream is InputStream<T> stream
            ? _circuit.Handle(stream)
            : throw Mismatch(slot, $"Input<{typeof(T).Name}>");

    /// <summary>The keyed input declared as <c>p.KeyedInput&lt;TKey, TValue&gt;(slot, …)</c>.</summary>
    public KeyedInputHandle<TKey, TValue> KeyedInput<TKey, TValue>(string slot)
        where TKey : notnull
        where TValue : notnull =>
        Slot(slot).Stream is KeyedInputStream<TKey, TValue> stream
            ? _circuit.Handle(stream)
            : throw Mismatch(slot, $"KeyedInput<{typeof(TKey).Name}, {typeof(TValue).Name}>");

    private InputSlot Slot(string slot) =>
        _pipeline.Inputs.FirstOrDefault(i => i.Name == slot)
        ?? throw new ArgumentException(
            $"The definition declares no input slot '{slot}' (it declares: {string.Join(", ", _pipeline.Inputs.Select(i => i.Name))}).", nameof(slot));

    private ArgumentException Mismatch(string slot, string asked)
    {
        var declared = _pipeline.Inputs.First(i => i.Name == slot);
        var shape = declared.Kind == SlotKind.KeyedInput ? $"KeyedInput<{declared.KeyType}, {declared.Type}>" : $"Input<{declared.Type}>";
        return new ArgumentException($"Input slot '{slot}' is {shape}, not {asked}.", nameof(slot));
    }

    /// <summary>Inserts the records into an append input.</summary>
    public async Task InsertAsync<T>(string slot, IEnumerable<T> records) where T : notnull
    {
        var input = Input<T>(slot);
        foreach (var record in records)
            await input.InsertAsync(record).ConfigureAwait(false);
    }

    /// <summary>The rows an upsert sink slot holds: each key's latest row.</summary>
    public InMemoryUpsertSink<TKey, TRow> UpsertSink<TKey, TRow>(string slot) where TKey : notnull =>
        _memory.UpsertSink<TKey, TRow>(SinkName(slot, SlotKind.UpsertSink));

    /// <summary>The records an append sink slot was given, in step order.</summary>
    public InMemoryAppendSink<T> AppendSink<T>(string slot) => _memory.AppendSink<T>(SinkName(slot, SlotKind.AppendSink));

    private string SinkName(string slot, SlotKind kind)
    {
        var declared = _pipeline.Sinks.FirstOrDefault(s => s.Name == slot)
            ?? throw new ArgumentException(
                $"The definition declares no sink slot '{slot}' (it declares: {string.Join(", ", _pipeline.Sinks.Select(s => s.Name))}).", nameof(slot));
        if (declared.Kind != kind)
            throw new ArgumentException($"Sink slot '{slot}' is {declared.Kind}, not {kind}.", nameof(slot));
        return $"{_id}/{slot}";
    }

    /// <summary>A table the definition reads or writes, under the name its slot is bound
    /// to (the definition's own name unless <see cref="Builder.BindTable"/> said otherwise).</summary>
    public HarnessTable<TKey, TRow> Table<TKey, TRow>(TableDefinition<TKey, TRow> definition, string? name = null)
        where TKey : notnull
        where TRow : class
    {
        name ??= definition.Name;
        if (!_pipeline.StandIns.TryGetValue(name, out var table))
            throw new ArgumentException(
                $"The pipeline has no table '{name}' (it has: {string.Join(", ", _pipeline.StandIns.Keys.Order(StringComparer.Ordinal))}).", nameof(name));
        return new HarnessTable<TKey, TRow>(
            table as SharedTable<TKey, TRow> ?? throw new ArgumentException($"Table '{name}' is not a table of {typeof(TKey).Name} → {typeof(TRow).Name}.", nameof(name)));
    }

    /// <summary>Every record the pipeline has dropped so far, in step order. Empty unless
    /// the harness was started with <see cref="RecordErrorPolicy.DeadLetter"/> (or the
    /// definition sets it on an operator).</summary>
    public IReadOnlyList<DeadLetter> DeadLetters
    {
        get
        {
            lock (_deadLetters)
                return [.. _deadLetters];
        }
    }

    /// <summary>The event-time watermark after the last step; null before any input with
    /// event time has seen a record.</summary>
    public DateTimeOffset? Watermark => _circuit.CurrentWatermark;

    /// <summary>Records the pipeline has dropped as late so far: insertions whose event
    /// time was behind the watermark when they reached an event-time declaration — an
    /// amendment of a record that is final, or a record never seen before. They are not
    /// dead letters; a node shows the same count on the pipeline's page.</summary>
    public long LateRecords => _circuit.LateRecords;

    /// <summary>What the watermark waits for; see <see cref="Circuit.WatermarkHeldBy"/>.</summary>
    public WatermarkHold? WatermarkHeldBy => _circuit.WatermarkHeldBy;

    /// <summary>
    /// What the pipeline's state holds as of the last step: entries per store and in all,
    /// the same count a node shows as the pipeline's state entries. A store is named after
    /// its operator — <c>named.priced.tableJoin.facts.w0</c> for the facts a join named
    /// <c>priced</c> keeps on worker 0, <c>input.trades.upsert.rows.w0</c> for a keyed
    /// input's rows, <c>….deadlines.w0</c> for what an operator waits on the watermark for.
    /// For a test that feeds a definition a long stream and asserts that its state returns
    /// to a bound once its windows close and its records are final.
    /// </summary>
    public HarnessStateSize StateSize
    {
        get
        {
            var stores = _storage.CommittedEntriesByStore();
            return new HarnessStateSize(stores.Values.Sum(), stores);
        }
    }

    /// <summary>
    /// Runs one step: everything sent to the inputs and every change made to the tables
    /// since the last one. It returns when the sinks have the step's output and the dead
    /// letters are in <see cref="DeadLetters"/>. A record that faults the circuit — an
    /// exception under the <c>Fault</c> policy, a retraction on an append sink — is thrown
    /// from here. So is <see cref="StateLimitExceededException"/> when the state passes
    /// <see cref="Builder.MaxStateEntries"/>, after the step, and from every later step:
    /// a node would have failed the pipeline.
    /// </summary>
    public async Task StepAsync(CancellationToken cancellationToken = default)
    {
        if (_overLimit is not null)
            throw _overLimit;
        // More than one step of the circuit where an input holds a record back for the
        // next: a keyed input that forgets its final rows never runs two records of one
        // key in a step (KeyedInputStream.WithEventTime). A running pipeline's pump does
        // the same.
        do
            await _circuit.StepAsync(cancellationToken).ConfigureAwait(false);
        while (_circuit.HasPendingInput);

        if (_maxStateEntries is { } limit && StateSize.Entries is var entries && entries > limit)
            throw _overLimit = new StateLimitExceededException(_id, entries, limit);
    }

    private async Task StartAsync()
    {
        _circuit.Start();
        foreach (var sink in _pipeline.Sinks)
        {
            var config = JsonSerializer.SerializeToElement(new Dictionary<string, string> { ["connector"] = "memory" });
            _deliveries.Add(sink.Start(_memory, new SlotStart
            {
                Circuit = _circuit,
                PipelineId = _id,
                Slot = sink.Name,
                Config = config,
                Json = Json,
                Logger = NullLogger.Instance,
                Services = _pipeline.Services,
                Leadership = null,
                Cancellation = _stop.Token,
            }));
        }
        System.Threading.Channels.ChannelReader<DeadLetterBatch>? batches = null;
        try
        {
            batches = _circuit.DeadLetterBatches;
        }
        catch (InvalidOperationException)
        {
            // Nothing in this circuit drops records.
        }
        if (batches is { } reader)
        {
            _deliveries.Add(Task.Run(async () =>
            {
                try
                {
                    await foreach (var batch in reader.ReadAllAsync().ConfigureAwait(false))
                    {
                        lock (_deadLetters)
                            _deadLetters.AddRange(batch.Letters);
                        batch.Acknowledge();
                    }
                }
                catch
                {
                    // The circuit's fault, which StepAsync throws.
                }
            }));
        }
        await Task.CompletedTask.ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await _circuit.DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
            // A circuit a test faulted on purpose.
        }
        try
        {
            await Task.WhenAll(_deliveries).WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
        }
        catch
        {
            // Deliveries end with the circuit; one that faulted has said so through StepAsync.
        }
        _stop.Cancel();
        _stop.Dispose();
        foreach (var table in _pipeline.StandIns.Values.OfType<IAsyncDisposable>())
            await table.DisposeAsync().ConfigureAwait(false);
    }

    private sealed class NoServices : IServiceProvider
    {
        public static readonly NoServices Instance = new();

        public object? GetService(Type serviceType) => null;
    }

    /// <summary>What the harness is given before it starts.</summary>
    public sealed class Builder
    {
        private readonly Type _definition;
        private readonly Dictionary<string, object> _tables = new(StringComparer.Ordinal);
        private readonly List<Func<Task>> _fills = [];
        private readonly Dictionary<string, string> _bindings = new(StringComparer.Ordinal);
        private JsonElement? _settings;
        private string? _id;
        private int _workers = 1;
        private RecordErrorPolicy _errorPolicy = RecordErrorPolicy.Fault;
        private TimeSpan? _maxEventTimeAhead = new ServiceOptions().MaxEventTimeAhead;
        private long? _maxStateEntries;
        private TimeProvider _clock = TimeProvider.System;
        private IServiceProvider? _services;
        private ILoggerFactory? _logging;

        internal Builder(Type definition) => _definition = definition;

        /// <summary>The settings as a spec carries them: JSON, camelCase. They are bound and
        /// validated as the service does it, so a test of a refused setting is a test of
        /// what an operator would be told.</summary>
        public Builder Settings(string json)
        {
            using var document = JsonDocument.Parse(json);
            _settings = document.RootElement.Clone();
            return this;
        }

        /// <summary>The pipeline's id: what <c>p.PipelineId</c> returns. The definition's name by default.</summary>
        public Builder Id(string id)
        {
            _id = id;
            return this;
        }

        /// <summary>A table the definition reads, with its first rows. A table the
        /// definition names and the test does not give starts empty.</summary>
        public Builder Table<TKey, TRow>(TableDefinition<TKey, TRow> definition, IEnumerable<TRow>? rows = null, string? name = null)
            where TKey : notnull
            where TRow : class
        {
            ArgumentNullException.ThrowIfNull(definition);
            var configured = definition.Configured();
            var table = new SharedTable<TKey, TRow>(name ?? definition.Name, definition.KeyOf, configured.Options);
            foreach (var index in configured.Indexes)
                index.Declare(table);
            _tables[name ?? definition.Name] = table;
            if (rows is not null)
            {
                var copy = rows.ToList();
                _fills.Add(async () => await table.Writer.ReplaceAllAsync(copy).ConfigureAwait(false));
            }
            return this;
        }

        /// <summary>Binds a table slot to a table of another name, as a spec's
        /// <c>tables</c> does.</summary>
        public Builder BindTable(string slot, string table)
        {
            _bindings[slot] = table;
            return this;
        }

        /// <summary>The circuit's error policy, as a spec's <c>circuit.errorPolicy</c>.</summary>
        public Builder ErrorPolicy(RecordErrorPolicy policy)
        {
            _errorPolicy = policy;
            return this;
        }

        /// <summary>How far ahead of the clock a record's event time may be, as a spec's
        /// <c>circuit.maxEventTimeAhead</c>; the node's default (a day) unless set, null
        /// for no bound. A test whose records are stamped in the future sets one of the
        /// two — or the <see cref="Clock"/>.</summary>
        public Builder MaxEventTimeAhead(TimeSpan? bound)
        {
            _maxEventTimeAhead = bound;
            return this;
        }

        /// <summary>The most state entries the pipeline may hold, as a spec's
        /// <c>circuit.maxStateEntries</c>: a step after which <see cref="StateSize"/> holds
        /// more throws <see cref="StateLimitExceededException"/>, as a node would fail the
        /// pipeline. Null (the default) for no limit.</summary>
        public Builder MaxStateEntries(long? limit)
        {
            if (limit is < 1)
                throw new ArgumentOutOfRangeException(nameof(limit), limit, "circuit.maxStateEntries must be at least 1.");
            _maxStateEntries = limit;
            return this;
        }

        /// <summary>The clock <see cref="MaxEventTimeAhead"/> is judged against; the
        /// system's unless set.</summary>
        public Builder Clock(TimeProvider clock)
        {
            _clock = clock;
            return this;
        }

        /// <summary>The worker count, as a spec's <c>circuit.workers</c>. A definition
        /// whose keyed state is wrongly partitioned shows it only with more than one.</summary>
        public Builder Workers(int workers)
        {
            _workers = workers;
            return this;
        }

        /// <summary>What <c>p.Services</c> returns.</summary>
        public Builder Services(IServiceProvider services)
        {
            _services = services;
            return this;
        }

        /// <summary>Where the circuit and <c>p.Logger</c> log.</summary>
        public Builder Logging(ILoggerFactory logging)
        {
            _logging = logging;
            return this;
        }

        /// <summary>Binds the settings, runs the definition's <c>Build</c> and starts the
        /// circuit. Throws <see cref="ServiceException"/> where the service's dry run
        /// would refuse: settings that do not bind or validate.</summary>
        public async Task<PipelineTestHarness> StartAsync()
        {
            var binding = DefinitionBinding.ForType(_definition)
                ?? throw new ArgumentException($"{_definition} is not a pipeline definition: it implements neither IPipelineDefinition nor IPipelineDefinition<TSettings>.");
            var json = LoadedPlugin.NewJson();
            var settings = binding.BindSettings(_settings, json);
            var id = _id ?? (ServiceNames.IsValid(binding.Name) ? binding.Name : "pipeline");
            var spec = new PipelineSpec { Id = id, Plugin = "test", Version = "test", Definition = binding.Name, Settings = _settings, Tables = _bindings };
            var storage = new InMemoryCheckpointStorageProvider();
            var circuit = new CircuitBuilder(new CircuitOptions
            {
                Name = id,
                Workers = _workers,
                AutoStep = false,
                // A checkpoint store makes a step wait for its sinks, as it does in the service.
                Storage = storage,
                ErrorPolicy = _errorPolicy,
                MaxEventTimeAhead = _maxEventTimeAhead,
                TimeProvider = _clock,
                AcknowledgeDeadLetters = true,
                InputChannelCapacity = 1 << 20,
                MaxStepItemsPerInput = 1 << 20,
                LoggerFactory = _logging,
            });
            var logger = (_logging ?? NullLoggerFactory.Instance).CreateLogger("Incremental.Service.Pipelines");
            var pipeline = new PipelineBuilder(spec, circuit, null!, logger, _services ?? NoServices.Instance, detached: true);
            foreach (var (name, table) in _tables)
                pipeline.StandIns[name] = table;
            binding.Build(pipeline, settings);
            foreach (var fill in _fills)
                await fill().ConfigureAwait(false);
            var harness = new PipelineTestHarness(id, circuit.Build(), pipeline, json, storage, _maxStateEntries);
            await harness.StartAsync().ConfigureAwait(false);
            return harness;
        }
    }
}

/// <summary>What a <see cref="PipelineTestHarness"/>'s pipeline holds:
/// <paramref name="Entries"/> in all, and per store, by the store's name.</summary>
public sealed record HarnessStateSize(long Entries, IReadOnlyDictionary<string, long> Stores);

/// <summary>A table of a <see cref="PipelineTestHarness"/>: what the test changes between
/// steps, and what it reads of a table the pipeline writes.</summary>
public sealed class HarnessTable<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    private readonly SharedTable<TKey, TRow> _table;

    internal HarnessTable(SharedTable<TKey, TRow> table) => _table = table;

    /// <summary>The table's rows now.</summary>
    public IReadOnlyDictionary<TKey, TRow> Rows
    {
        get
        {
            using var snapshot = _table.Snapshot();
            return snapshot.Rows().ToDictionary(r => r.Key, r => r.Value);
        }
    }

    /// <summary>Upserts the rows as one version of the table; the pipeline sees it at its next step.</summary>
    public async Task UpsertAsync(params TRow[] rows)
    {
        foreach (var row in rows)
            await _table.Writer.UpsertAsync(row).ConfigureAwait(false);
        await _table.Writer.CommitAsync().ConfigureAwait(false);
    }

    /// <summary>Deletes the keys as one version of the table.</summary>
    public async Task DeleteAsync(params TKey[] keys)
    {
        foreach (var key in keys)
            await _table.Writer.DeleteAsync(key).ConfigureAwait(false);
        await _table.Writer.CommitAsync().ConfigureAwait(false);
    }
}
