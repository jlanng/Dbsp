using System.Text.Json;

namespace Incremental.Storage;

/// <summary>Serializes state values for storage providers that persist outside the process (e.g. Redis).</summary>
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

    public string Serialize<T>(T value) => JsonSerializer.Serialize(value, _options);

    public T Deserialize<T>(string data) =>
        JsonSerializer.Deserialize<T>(data, _options)
        ?? throw new InvalidOperationException($"Deserializing state of type {typeof(T).Name} produced null.");
}
