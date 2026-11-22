using Dbsp.Tables;
using Microsoft.Extensions.Logging;

namespace Dbsp.Service;

/// <summary>
/// What a definition builds against. <see cref="Circuit"/> is the core's builder, so
/// lookups, aggregates, event time and windows are the core API with nothing in between;
/// the methods here bind the circuit's edges to named slots, which the spec binds to
/// connectors and declared tables.
/// </summary>
/// <remarks>Made by the service (and by the test harness, through the same code); a
/// definition never makes one. What a slot is bound to — a connector, a declared table —
/// is the service's business, behind <see cref="IPipelineBinder"/>, so that nothing a
/// definition sees names hosting or Kafka.</remarks>
public sealed class PipelineBuilder
{
    private readonly IPipelineBinder _binder;

    internal PipelineBuilder(IPipelineBinder binder, CircuitBuilder circuit, ILogger logger, IServiceProvider services)
    {
        _binder = binder;
        Circuit = circuit;
        Logger = logger;
        Services = services;
    }

    public string PipelineId => _binder.PipelineId;

    /// <summary>Everything the core offers.</summary>
    public CircuitBuilder Circuit { get; }
    public ILogger Logger { get; }

    /// <summary>The host's services. A service the definition needs (a pricing library, a
    /// clock) comes from here, not from a static: statics pin the plugin's load context.</summary>
    public IServiceProvider Services { get; }

    /// <summary>A shared table to join or look up against. The definition gives the key
    /// and row types and the default slot name; the spec's <c>tables</c> binds the slot to
    /// a declared table, so one definition can read <c>instruments-eu</c> in one
    /// deployment and <c>instruments-us</c> in another.</summary>
    public TableRef<TKey, TRow> Table<TKey, TRow>(TableDefinition<TKey, TRow> table, string? slot = null)
        where TKey : notnull
        where TRow : class
    {
        ArgumentNullException.ThrowIfNull(table);
        return Circuit.Table(_binder.ReadTable(table, slot ?? table.Name));
    }

    /// <summary>An append input: records, each inserted or removed.</summary>
    public InputStream<T> Input<T>(string slot) where T : notnull
    {
        _binder.Claim(slot);
        var stream = Circuit.Input<T>(slot);
        _binder.Input(slot, stream);
        return stream;
    }

    /// <summary>A keyed input: upserts and deletes by key.</summary>
    public KeyedInputStream<TKey, TValue> KeyedInput<TKey, TValue>(string slot, Func<TValue, TKey> key)
        where TKey : notnull
        where TValue : notnull
    {
        _binder.Claim(slot);
        var stream = Circuit.KeyedInput(slot, key);
        _binder.KeyedInput(slot, stream);
        return stream;
    }

    /// <summary>A keyed result delivered as upserts and deletes, at least once.</summary>
    public void UpsertSink<TKey, TRow>(string slot, ZStream<KeyedValue<TKey, TRow>> rows)
        where TKey : notnull
        where TRow : notnull
    {
        _binder.Claim(slot);
        _binder.UpsertSink(slot, rows);
    }

    /// <summary>A result delivered as upserts keyed by <paramref name="key"/>. The stream
    /// must hold at most one row per key at a time.</summary>
    public void UpsertSink<T, TKey>(string slot, ZStream<T> rows, Func<T, TKey> key)
        where T : notnull
        where TKey : notnull
    {
        _binder.Claim(slot);
        _binder.UpsertSink(slot, rows, key);
    }

    /// <summary>An append-only result. A retraction on the stream faults the pipeline.</summary>
    public void AppendSink<T>(string slot, ZStream<T> rows) where T : notnull
    {
        _binder.Claim(slot);
        _binder.AppendSink(slot, rows);
    }

    /// <summary>Writes the stream into a declared table (a derived table): this pipeline
    /// is then the table's one writer.</summary>
    public void ToTable<TKey, TRow>(TableDefinition<TKey, TRow> table, ZStream<TRow> rows, string? slot = null)
        where TKey : notnull
        where TRow : class
    {
        ArgumentNullException.ThrowIfNull(table);
        rows.ToTable(_binder.WriteTable(table, slot ?? table.Name));
    }

    /// <inheritdoc cref="ToTable{TKey,TRow}(TableDefinition{TKey,TRow}, ZStream{TRow}, string?)"/>
    public void ToTable<TKey, TValue, TRow>(
        TableDefinition<TKey, TRow> table, KeyedZStream<TKey, TValue> rows, Func<TKey, TValue, TRow> row, string? slot = null)
        where TKey : notnull
        where TValue : notnull
        where TRow : class
    {
        ArgumentNullException.ThrowIfNull(row);
        ToTable(table, rows.Select(kv => row(kv.Key, kv.Value)), slot);
    }
}

/// <summary>
/// The service's half of a <see cref="PipelineBuilder"/>: it records each slot a
/// definition declares, with what it takes to start the slot's connector, and resolves a
/// table slot to the shared table the spec binds it to. Implemented by the service's
/// <c>PipelineBinding</c>, which the supervisor, the dry run, discovery and the test harness
/// all build a definition through.
/// </summary>
internal interface IPipelineBinder
{
    string PipelineId { get; }

    /// <summary>Takes the slot name for this definition, or throws: names are unique and
    /// the dead-letter slot is the spec's.</summary>
    void Claim(string slot);

    SharedTable<TKey, TRow> ReadTable<TKey, TRow>(TableDefinition<TKey, TRow> table, string slot)
        where TKey : notnull
        where TRow : class;

    SharedTable<TKey, TRow> WriteTable<TKey, TRow>(TableDefinition<TKey, TRow> table, string slot)
        where TKey : notnull
        where TRow : class;

    void Input<T>(string slot, InputStream<T> stream) where T : notnull;

    void KeyedInput<TKey, TValue>(string slot, KeyedInputStream<TKey, TValue> stream)
        where TKey : notnull
        where TValue : notnull;

    void UpsertSink<TKey, TRow>(string slot, ZStream<KeyedValue<TKey, TRow>> rows)
        where TKey : notnull
        where TRow : notnull;

    void UpsertSink<T, TKey>(string slot, ZStream<T> rows, Func<T, TKey> key)
        where T : notnull
        where TKey : notnull;

    void AppendSink<T>(string slot, ZStream<T> rows) where T : notnull;
}

public enum SlotKind { AppendInput, KeyedInput, UpsertSink, AppendSink }

/// <summary>An input or sink slot: its kind and the type of its records; for a keyed input
/// and an upsert sink also the type of the key. <c>Members</c> spells the record type
/// out — one line per type it is made of, <c>Name { member: Type; … }</c> — so that two
/// versions of a slot can be compared by what they carry, not only by a type's name.</summary>
public sealed record SlotShape(string Name, SlotKind Kind, string Type, string? KeyType = null, IReadOnlyList<string>? Members = null);

/// <summary>A table a definition reads or writes: the slot (which a spec's <c>tables</c>
/// can bind to a declared table), the table it resolves to, and the types.</summary>
public sealed record TableSlotShape(string Slot, string Table, string Access, string KeyType, string RowType, string Definition);

/// <summary>What a definition's <c>Build</c> declared: its slots and the tables it reads
/// and writes, by declared table name. Known for any pipeline that has ever validated.</summary>
public sealed record PipelineShape(
    IReadOnlyList<SlotShape> Inputs, IReadOnlyList<SlotShape> Sinks,
    IReadOnlyList<string> TablesRead, IReadOnlyList<string> TablesWritten,
    IReadOnlyList<TableSlotShape>? TableSlots = null);
