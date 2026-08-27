using System.Text.Json;

namespace StreamProcessor.Storage;

/// <summary>Serializes state values for storage providers that persist outside the process (e.g. Redis).</summary>
public interface IStateSerializer
{
    string Serialize<T>(T value);
    T Deserialize<T>(string data);
}

public sealed class JsonStateSerializer : IStateSerializer
{
    public static JsonStateSerializer Instance { get; } = new();

    private readonly JsonSerializerOptions _options;

    public JsonStateSerializer(JsonSerializerOptions? options = null) =>
        _options = options ?? JsonSerializerOptions.Default;

    public string Serialize<T>(T value) => JsonSerializer.Serialize(value, _options);

    public T Deserialize<T>(string data) =>
        JsonSerializer.Deserialize<T>(data, _options)
        ?? throw new InvalidOperationException($"Deserializing state of type {typeof(T).Name} produced null.");
}
