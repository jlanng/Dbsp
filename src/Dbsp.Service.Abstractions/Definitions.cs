namespace Dbsp.Service;

/// <summary>
/// A pipeline definition with no settings: a class with a public parameterless constructor
/// whose <see cref="Build"/> describes one circuit. <c>Build</c> runs many times — at
/// import, at every dry run, for every generation — and must do nothing but describe the
/// circuit: no I/O, no threads, no static writes.
/// </summary>
public interface IPipelineDefinition
{
    void Build(PipelineBuilder pipeline);
}

/// <summary>A pipeline definition with one settings type, bound from the spec's
/// <c>settings</c> and validated before anything is built.</summary>
public interface IPipelineDefinition<TSettings> where TSettings : class
{
    void Build(PipelineBuilder pipeline, TSettings settings);
}

/// <summary>Names a definition; without it the class name is the name.</summary>
[AttributeUsage(AttributeTargets.Class)]
public sealed class PipelineAttribute(string name) : Attribute
{
    public string Name { get; } = name;
    public string? Description { get; init; }
}

/// <summary>
/// Marks a contract assembly: it holds <see cref="TableDefinition{TKey,TRow}"/>s and the
/// row and key types of shared tables. It is loaded once per process and never unloaded,
/// and every pipeline plugin that references it gets that one copy — even if it ships its
/// own — so two plugins agree on a row type.
/// </summary>
[AttributeUsage(AttributeTargets.Assembly)]
public sealed class DbspContractAttribute : Attribute;

/// <summary>Marks a settings property that changes no stored row — a label, a batch size —
/// so that changing it keeps the pipeline's state without being asked.</summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class StateNeutralAttribute : Attribute;

/// <summary>A refusal with a stable code and the HTTP status the admin API answers with.
/// The service throws it for what it will not do; the test harness throws it where the
/// service's dry run would refuse (settings that do not bind or validate).</summary>
public sealed class ServiceException(string code, string message, int status = 409, IReadOnlyList<string>? details = null)
    : Exception(message)
{
    public string Code { get; } = code;
    public int Status { get; } = status;
    public IReadOnlyList<string> Details { get; } = details ?? [];

    public static ServiceException NotFound(string what) => new("not-found", $"{what} does not exist.", 404);
    public static ServiceException Invalid(string message, IReadOnlyList<string>? details = null) => new("invalid", message, 400, details);
    public static ServiceException Conflict(string message) => new("conflict", message);
}

/// <summary>A pipeline's state passed its spec's <c>circuit.maxStateEntries</c>.</summary>
public sealed class StateLimitExceededException(string pipeline, long entries, long limit)
    : Exception($"Pipeline '{pipeline}' holds {entries:N0} state entries, more than its limit of {limit:N0} (circuit.maxStateEntries). " +
                "It is not restarted, since it would resume the same state: raise the limit by a replace that keeps the state, or reset it.")
{
    public string Pipeline { get; } = pipeline;
    public long Entries { get; } = entries;
    public long Limit { get; } = limit;
}
