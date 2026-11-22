using System.Text.Json;
using System.Text.Json.Serialization;

namespace Dbsp.Service.Admin;

internal static class AdminJson
{
    /// <summary>For everything the API writes except specs: camelCase, enums by name,
    /// nulls left out, and every duration a number of milliseconds.</summary>
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        // Served as application/json and never written into HTML, so quotes and plus
        // signs stay readable.
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        Converters = { new JsonStringEnumConverter(), new MillisecondsConverter() },
    };

    private sealed class MillisecondsConverter : JsonConverter<TimeSpan>
    {
        public override TimeSpan Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            reader.TokenType == JsonTokenType.Number ? TimeSpan.FromMilliseconds(reader.GetDouble()) : TimeSpan.Parse(reader.GetString()!);

        public override void Write(Utf8JsonWriter writer, TimeSpan value, JsonSerializerOptions options) =>
            writer.WriteNumberValue(Math.Round(value.TotalMilliseconds, 3));
    }

    /// <summary>A spec as the catalog holds it (the service's own format, so that what the
    /// API returns can be sent back).</summary>
    public static JsonElement Spec<T>(T spec) => JsonSerializer.SerializeToElement(spec, ServiceJson.Options);
}
