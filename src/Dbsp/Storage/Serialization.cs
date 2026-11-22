using System.Text.Json;

namespace Dbsp.Storage;

/// <summary>Serializes state values for storage providers that persist outside the process
/// (e.g. Redis). The output is a persisted format — what one run writes, a later run reads
/// back — so an implementation must keep it stable across versions.</summary>
public interface IStateSerializer
{
    string Serialize<T>(T value);
    T Deserialize<T>(string data);
}

public sealed class JsonStateSerializer : IStateSerializer
{
    // IncludeFields so ValueTuple keys/values round-trip (tuples expose fields, not properties).
    // Must be initialized before Instance below — static initializers run in declaration order.
    private static readonly JsonSerializerOptions DefaultOptions = new() { IncludeFields = true };

    public static JsonStateSerializer Instance { get; } = new();

    private readonly JsonSerializerOptions _options;

    public JsonStateSerializer(JsonSerializerOptions? options = null) =>
        _options = options ?? DefaultOptions;

    /// <summary>For the changelog provider, which writes the same JSON straight to UTF-8
    /// instead of through a string.</summary>
    internal JsonSerializerOptions Options => _options;

    public string Serialize<T>(T value) => JsonSerializer.Serialize(value, _options);

    public T Deserialize<T>(string data) =>
        JsonSerializer.Deserialize<T>(data, _options)
        ?? throw new InvalidOperationException(
            $"Deserializing state of type {typeof(T).Name} produced null; the stored value is missing or corrupt.");
}
