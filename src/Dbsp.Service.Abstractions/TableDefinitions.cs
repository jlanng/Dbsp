using System.Text.Json;
using Dbsp.Tables;

namespace Dbsp.Service;

/// <summary>A table definition as the registry holds it, whatever its key and row types.</summary>
public interface ITableDefinition
{
    /// <summary>The default table name, and the default slot name in a pipeline.</summary>
    string Name { get; }
    Type KeyType { get; }
    Type RowType { get; }
}

/// <summary>
/// A shared table's shape, declared once in a contract assembly: its name, how a row gives
/// its key, and its indexes. A table spec names a definition class and a
/// table name, so one definition can be declared under several names.
/// </summary>
public abstract class TableDefinition<TKey, TRow> : ITableDefinition, ITableDefinitionInternal
    where TKey : notnull
    where TRow : class
{
    public abstract string Name { get; }
    public abstract TKey KeyOf(TRow row);

    /// <summary>Declares indexes and options. Runs once per table opened.</summary>
    protected virtual void Configure(TableBuilder<TKey, TRow> table)
    {
    }

    public Type KeyType => typeof(TKey);
    public Type RowType => typeof(TRow);

    internal TableBuilder<TKey, TRow> Configured()
    {
        var builder = new TableBuilder<TKey, TRow>();
        Configure(builder);
        return builder;
    }

    TResult ITableDefinitionInternal.Accept<TResult>(ITableDefinitionVisitor<TResult> visitor) => visitor.Visit(this);
}

/// <summary>Reaches a definition's key and row types from code that holds it only as an
/// <see cref="ITableDefinition"/>: the service opens a table, or asks a loader for a
/// source, through one of these.</summary>
internal interface ITableDefinitionInternal
{
    TResult Accept<TResult>(ITableDefinitionVisitor<TResult> visitor);
}

internal interface ITableDefinitionVisitor<out TResult>
{
    TResult Visit<TKey, TRow>(TableDefinition<TKey, TRow> definition)
        where TKey : notnull
        where TRow : class;
}

/// <summary>A secondary index, declared once with its selector, so that no pipeline
/// repeats a selector the core would silently ignore: read it with
/// <c>table.By(InstrumentsTable.ByIsin)</c>.</summary>
public sealed record IndexDefinition<TRow, TIndexKey>(string Name, Func<TRow, TIndexKey?> Selector)
    where TIndexKey : notnull;

public sealed class TableBuilder<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    internal List<DeclaredIndex> Indexes { get; } = [];

    /// <summary>A declared index: how to add it to a table, and how to read it with a key
    /// that arrives as JSON (the admin API's row lookup).</summary>
    internal sealed record DeclaredIndex(
        string Name, Type KeyType, Action<SharedTable<TKey, TRow>> Declare,
        ReadIndexPage ReadBy);

    /// <summary>A page of the rows under an index key given as text, and how many keys the
    /// index holds under it.</summary>
    internal delegate IReadOnlyList<KeyValuePair<TKey, TRow>> ReadIndexPage(
        TableSnapshot<TKey, TRow> snapshot, string key, int skip, int take, out int total);

    public SharedTableOptions Options { get; set; } = new();

    public TableBuilder<TKey, TRow> Index<TIndexKey>(IndexDefinition<TRow, TIndexKey> index) where TIndexKey : notnull
    {
        ArgumentNullException.ThrowIfNull(index);
        Indexes.Add(new DeclaredIndex(
            index.Name, typeof(TIndexKey), table => table.AddIndex(index.Name, index.Selector),
            (TableSnapshot<TKey, TRow> snapshot, string key, int skip, int take, out int total) =>
                snapshot.RowsBy(index.Name, TableKeys.Parse<TIndexKey>(key), skip, take, out total)));
        return this;
    }
}

public static class TableDefinitionExtensions
{
    /// <summary>The table's rows under a declared index.</summary>
    public static TableView<TIndexKey, TRow> By<TKey, TRow, TIndexKey>(
        this TableRef<TKey, TRow> table, IndexDefinition<TRow, TIndexKey> index)
        where TKey : notnull
        where TRow : class
        where TIndexKey : notnull =>
        table.By(index.Name, index.Selector);
}

internal static class TableKeys
{
    // Keys are contract or primitive types, which never unload, so one options instance serves.
    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    /// <summary>A key given as text: JSON (<c>42</c>, <c>"GB00"</c>, <c>{"a":1}</c>), or —
    /// for a key that is a JSON string, such as a string, a Guid or a date — the bare text.</summary>
    public static T Parse<T>(string text) where T : notnull
    {
        try
        {
            if (JsonSerializer.Deserialize<T>(text, Json) is { } parsed)
                return parsed;
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException or FormatException)
        {
        }
        try
        {
            if (JsonSerializer.Deserialize<T>(JsonSerializer.Serialize(text), Json) is { } quoted)
                return quoted;
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException or FormatException)
        {
        }
        throw ServiceException.Invalid($"'{(text.Length > 80 ? text[..80] + "…" : text)}' is not a key of type {typeof(T).Name}; give it as JSON.");
    }
}
