using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace Incremental.Kafka;

/// <summary>The state of a connector and its tasks, as Kafka Connect reports it.</summary>
public sealed record KafkaConnectorStatus(string Name, string State, IReadOnlyList<KafkaConnectorTaskStatus> Tasks)
{
    /// <summary>The connector and at least one task are running, and no task has failed.</summary>
    public bool IsRunning => State == "RUNNING" && Tasks.Count > 0 && Tasks.All(t => t.State == "RUNNING");

    /// <summary>The first failed task's stack trace, if a task has failed.</summary>
    public string? Failure => Tasks.FirstOrDefault(t => t.State == "FAILED")?.Trace;
}

/// <summary>One task of a connector.</summary>
public sealed record KafkaConnectorTaskStatus(int Id, string State, string? Trace);

/// <summary>Kafka Connect refused a request.</summary>
public sealed class KafkaConnectException(string message, HttpStatusCode? status = null) : Exception(message)
{
    public HttpStatusCode? Status { get; } = status;
}

/// <summary>
/// The handful of Kafka Connect REST calls that registering a Debezium connector needs:
/// create or update, validate, status, restart, delete. For tests, examples and a
/// deployment's set-up step; nothing in a running pipeline calls it.
/// </summary>
public sealed class KafkaConnectClient : IDisposable
{
    private readonly HttpClient _http;
    private readonly bool _ownsClient;

    /// <param name="baseAddress">Connect's REST endpoint, <c>http://localhost:8083</c> on the local stack.</param>
    /// <param name="http">A client to use instead of one of this object's own (for
    /// authentication or a handler); its base address is left alone.</param>
    public KafkaConnectClient(Uri baseAddress, HttpClient? http = null)
    {
        ArgumentNullException.ThrowIfNull(baseAddress);
        _ownsClient = http is null;
        _http = http ?? new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
        BaseAddress = new Uri(baseAddress.AbsoluteUri.TrimEnd('/') + "/");
    }

    public Uri BaseAddress { get; }

    /// <summary>Whether Connect answers at all.</summary>
    public async Task<bool> IsReachableAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var response = await _http.GetAsync(new Uri(BaseAddress, "connectors"), cancellationToken).ConfigureAwait(false);
            return response.IsSuccessStatusCode;
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException && !cancellationToken.IsCancellationRequested)
        {
            return false;
        }
    }

    /// <summary>Creates the connector, or replaces its configuration if it exists
    /// (<c>PUT /connectors/{name}/config</c>).</summary>
    public async Task PutConnectorAsync(string name, IReadOnlyDictionary<string, string> config, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        using var response = await _http.PutAsJsonAsync(
            new Uri(BaseAddress, $"connectors/{Uri.EscapeDataString(name)}/config"), config, cancellationToken).ConfigureAwait(false);
        await EnsureAsync(response, $"registering connector '{name}'", cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Asks Connect to check a configuration without applying it
    /// (<c>PUT /connector-plugins/{class}/config/validate</c>) and returns the problems it
    /// found, one per line of the form <c>setting: message</c>; empty when it is valid.
    /// For Debezium's Postgres connector this includes whether the database can be reached
    /// with the credentials given.
    /// </summary>
    public async Task<IReadOnlyList<string>> ValidateAsync(
        string name, IReadOnlyDictionary<string, string> config, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        if (!config.TryGetValue("connector.class", out var connectorClass))
            throw new ArgumentException("The configuration has no 'connector.class'.", nameof(config));
        // Validation wants the name inside the configuration; registration takes it in the path.
        var named = new Dictionary<string, string>(config) { ["name"] = name };
        using var response = await _http.PutAsJsonAsync(
            new Uri(BaseAddress, $"connector-plugins/{Uri.EscapeDataString(connectorClass)}/config/validate"), named, cancellationToken)
            .ConfigureAwait(false);
        await EnsureAsync(response, "validating a connector configuration", cancellationToken).ConfigureAwait(false);
        using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false));
        var problems = new List<string>();
        if (document.RootElement.TryGetProperty("configs", out var configs))
            foreach (var entry in configs.EnumerateArray())
            {
                if (!entry.TryGetProperty("value", out var value) || !value.TryGetProperty("errors", out var errors))
                    continue;
                foreach (var error in errors.EnumerateArray())
                    problems.Add($"{value.GetProperty("name").GetString()}: {error.GetString()}");
            }
        return problems;
    }

    /// <summary>The connector's status, or null if Connect has no such connector (or has
    /// not yet published a status for one just created).</summary>
    public async Task<KafkaConnectorStatus?> GetStatusAsync(string name, CancellationToken cancellationToken = default)
    {
        using var response = await _http.GetAsync(
            new Uri(BaseAddress, $"connectors/{Uri.EscapeDataString(name)}/status"), cancellationToken).ConfigureAwait(false);
        if (response.StatusCode == HttpStatusCode.NotFound)
            return null;
        await EnsureAsync(response, $"reading the status of connector '{name}'", cancellationToken).ConfigureAwait(false);
        using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false));
        var root = document.RootElement;
        var tasks = new List<KafkaConnectorTaskStatus>();
        if (root.TryGetProperty("tasks", out var taskList))
            foreach (var task in taskList.EnumerateArray())
                tasks.Add(new KafkaConnectorTaskStatus(
                    task.GetProperty("id").GetInt32(),
                    task.GetProperty("state").GetString() ?? "",
                    task.TryGetProperty("trace", out var trace) ? trace.GetString() : null));
        return new KafkaConnectorStatus(name, root.GetProperty("connector").GetProperty("state").GetString() ?? "", tasks);
    }

    /// <summary>Waits until the connector and its tasks are running. Throws if a task
    /// fails, or if they are not running within <paramref name="timeout"/>.</summary>
    public async Task<KafkaConnectorStatus> WaitUntilRunningAsync(string name, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        var deadline = Environment.TickCount64 + (long)timeout.TotalMilliseconds;
        while (true)
        {
            var status = await GetStatusAsync(name, cancellationToken).ConfigureAwait(false);
            if (status is { IsRunning: true })
                return status;
            if (status?.Failure is { } failure)
                throw new KafkaConnectException($"Connector '{name}' failed: {failure}");
            if (Environment.TickCount64 > deadline)
                throw new TimeoutException($"Connector '{name}' is not running after {timeout}: {status?.State ?? "no status"}.");
            await Task.Delay(200, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Restarts the connector and its tasks
    /// (<c>POST /connectors/{name}/restart?includeTasks=true</c>).</summary>
    public async Task RestartAsync(string name, CancellationToken cancellationToken = default)
    {
        using var response = await _http.PostAsync(
            new Uri(BaseAddress, $"connectors/{Uri.EscapeDataString(name)}/restart?includeTasks=true&onlyFailed=false"), null, cancellationToken)
            .ConfigureAwait(false);
        await EnsureAsync(response, $"restarting connector '{name}'", cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Removes the connector. Removing one that does not exist is not an error.
    /// A Debezium Postgres connector's replication slot and publication stay in the
    /// database: drop them there.</summary>
    public async Task DeleteConnectorAsync(string name, CancellationToken cancellationToken = default)
    {
        using var response = await _http.DeleteAsync(
            new Uri(BaseAddress, $"connectors/{Uri.EscapeDataString(name)}"), cancellationToken).ConfigureAwait(false);
        if (response.StatusCode != HttpStatusCode.NotFound)
            await EnsureAsync(response, $"deleting connector '{name}'", cancellationToken).ConfigureAwait(false);
    }

    private static async Task EnsureAsync(HttpResponseMessage response, string what, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode)
            return;
        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        throw new KafkaConnectException($"Kafka Connect refused {what} ({(int)response.StatusCode}): {body}", response.StatusCode);
    }

    public void Dispose()
    {
        if (_ownsClient)
            _http.Dispose();
    }
}

/// <summary>
/// The configuration of a Debezium Postgres connector whose topics
/// <see cref="Debezium.Changes{TKey,TRow}"/> can read, for
/// <see cref="KafkaConnectClient.PutConnectorAsync"/>. The database needs
/// <c>wal_level=logical</c> and a user allowed to replicate.
/// </summary>
public sealed class DebeziumPostgresOptions
{
    /// <summary>The database host as Kafka Connect reaches it (<c>postgres</c> on the
    /// local stack's network, not <c>localhost</c>).</summary>
    public required string Hostname { get; init; }

    public int Port { get; init; } = 5432;
    public required string Database { get; init; }
    public required string User { get; init; }
    public required string Password { get; init; }

    /// <summary>Prefix of the topics: a table's changes go to
    /// <c>{prefix}.{schema}.{table}</c> (<see cref="DebeziumPostgres.Topic"/>).</summary>
    public required string TopicPrefix { get; init; }

    /// <summary>The tables to capture, each as <c>schema.table</c>.</summary>
    public required IReadOnlyList<string> Tables { get; init; }

    /// <summary>The replication slot. One connector per slot; default derived from the
    /// topic prefix.</summary>
    public string? SlotName { get; init; }

    /// <summary>The publication, created for the listed tables if missing. Default derived
    /// from the topic prefix.</summary>
    public string? PublicationName { get; init; }

    /// <summary>Whether the JSON converter embeds a schema in every key and value
    /// (<c>schemas.enable</c>). The decoder reads both; without is a fraction of the size.</summary>
    public bool EmbedSchemas { get; init; }

    /// <summary><c>snapshot.mode</c>: <c>initial</c> reads the tables as they are (events
    /// with <c>op</c> <c>r</c>), then streams changes.</summary>
    public string SnapshotMode { get; init; } = "initial";

    /// <summary>Partitions of the topics Connect creates for the tables. Null leaves topic
    /// creation to the broker's defaults.</summary>
    public int? TopicPartitions { get; init; }

    /// <summary>Replication factor of those topics; used with <see cref="TopicPartitions"/>.</summary>
    public short TopicReplicationFactor { get; init; } = 3;

    /// <summary>Whether those topics are compacted, which is what a table's topic should
    /// be: it then always holds every row, and a loader that starts from nothing can read
    /// the table from it. Used with <see cref="TopicPartitions"/>.</summary>
    public bool CompactTopics { get; init; } = true;

    /// <summary>Any other connector setting, applied last.</summary>
    public IReadOnlyDictionary<string, string> Extra { get; init; } = new Dictionary<string, string>();
}

/// <summary>Names and configuration of Debezium's Postgres connector.</summary>
public static class DebeziumPostgres
{
    /// <summary>The topic a table's changes are written to.</summary>
    public static string Topic(string topicPrefix, string schema, string table) => $"{topicPrefix}.{schema}.{table}";

    /// <summary>The connector configuration for <paramref name="options"/>.</summary>
    public static Dictionary<string, string> ConnectorConfig(DebeziumPostgresOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.Tables.Count == 0)
            throw new ArgumentException("List at least one table to capture.", nameof(options));
        var safe = new string(options.TopicPrefix.Select(c => char.IsAsciiLetterOrDigit(c) ? char.ToLowerInvariant(c) : '_').ToArray());
        var config = new Dictionary<string, string>
        {
            ["connector.class"] = "io.debezium.connector.postgresql.PostgresConnector",
            ["tasks.max"] = "1",
            ["plugin.name"] = "pgoutput",
            ["database.hostname"] = options.Hostname,
            ["database.port"] = options.Port.ToString(),
            ["database.dbname"] = options.Database,
            ["database.user"] = options.User,
            ["database.password"] = options.Password,
            ["topic.prefix"] = options.TopicPrefix,
            ["table.include.list"] = string.Join(",", options.Tables),
            ["slot.name"] = options.SlotName ?? safe + "_slot",
            ["publication.name"] = options.PublicationName ?? safe + "_publication",
            ["publication.autocreate.mode"] = "filtered",
            ["snapshot.mode"] = options.SnapshotMode,
            // numeric as text: the default encodes it as bytes, which no JSON reader takes.
            ["decimal.handling.mode"] = "string",
            // The tombstone after a delete is what lets compaction forget the key.
            ["tombstones.on.delete"] = "true",
            ["key.converter"] = "org.apache.kafka.connect.json.JsonConverter",
            ["key.converter.schemas.enable"] = options.EmbedSchemas ? "true" : "false",
            ["value.converter"] = "org.apache.kafka.connect.json.JsonConverter",
            ["value.converter.schemas.enable"] = options.EmbedSchemas ? "true" : "false",
        };
        if (options.TopicPartitions is { } partitions)
        {
            config["topic.creation.default.partitions"] = partitions.ToString();
            config["topic.creation.default.replication.factor"] = options.TopicReplicationFactor.ToString();
            if (options.CompactTopics)
                config["topic.creation.default.cleanup.policy"] = "compact";
        }
        foreach (var (key, value) in options.Extra)
            config[key] = value;
        return config;
    }
}
