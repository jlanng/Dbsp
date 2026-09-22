using Incremental.Storage;
using Microsoft.Extensions.Logging;

namespace Incremental.Hosting;

/// <summary>What the host does when a hosted circuit faults — or when one of its registered
/// output consumers throws, which would otherwise stall the circuit on that output's
/// backpressure forever. A fault is terminal either way: the recovery story is a rebuild from
/// the last committed checkpoint, which under a host means a process restart.</summary>
public enum CircuitFaultBehavior
{
    /// <summary>Log at Critical, set a non-zero process exit code, and stop the application,
    /// so whatever supervises the process restarts it and the restart resumes from the
    /// checkpoint. The default, and the counterpart of
    /// <c>BackgroundServiceExceptionBehavior.StopHost</c>.</summary>
    StopHost,

    /// <summary>Log at Critical and leave the host running with a dead circuit. For
    /// applications where the circuit is a side feature; pair it with
    /// <c>AddHealthCheck</c> so the fault stays
    /// visible.</summary>
    Ignore,
}

/// <summary>
/// The settings of a hosted circuit: the mutable, configuration-bindable counterpart of
/// <see cref="CircuitOptions"/>, plus the host's own policy. The scalar members bind from
/// configuration (<c>BindConfiguration</c>);
/// <see cref="Storage"/> and <see cref="LoggerFactory"/> are set in code. Clustered circuits
/// are not hostable yet — they step in explicit lockstep across processes, which needs a
/// host-side stepping protocol this package does not have — so there is no <c>Cluster</c>
/// member.
/// </summary>
public sealed class HostedCircuitOptions
{
    /// <summary><see cref="CircuitOptions.Name"/>. Null (default) uses the definition
    /// class's name, so logs and the <c>circuit</c> metric tag read as the thing you wrote.</summary>
    public string? Name { get; set; }

    /// <summary><see cref="CircuitOptions.LoggerFactory"/>. Null (default) uses the
    /// container's <see cref="ILoggerFactory"/>, if one is registered.</summary>
    public ILoggerFactory? LoggerFactory { get; set; }

    /// <summary><see cref="CircuitOptions.Workers"/>. Remember the matching GC heap count
    /// (see "Running it fast" in the introduction) — it is read before <c>Main</c>, so the
    /// host cannot set it for you.</summary>
    public int Workers { get; set; } = 1;

    /// <summary><see cref="CircuitOptions.InputChannelCapacity"/>.</summary>
    public int InputChannelCapacity { get; set; } = 8192;

    /// <summary><see cref="CircuitOptions.OutputChannelCapacity"/>.</summary>
    public int OutputChannelCapacity { get; set; } = 256;

    /// <summary><see cref="CircuitOptions.MaxStepItemsPerInput"/>.</summary>
    public int MaxStepItemsPerInput { get; set; } = 8192;

    /// <summary><see cref="CircuitOptions.AutoStep"/>. With false, step the circuit yourself
    /// through <see cref="CircuitHost{TDefinition}.Circuit"/>.</summary>
    public bool AutoStep { get; set; } = true;

    /// <summary><see cref="CircuitOptions.Storage"/>. Null (default) is in-memory state.</summary>
    public IStorageProvider? Storage { get; set; }

    /// <summary><see cref="CircuitOptions.CheckpointInterval"/>.</summary>
    public int CheckpointInterval { get; set; } = 1;

    /// <summary>What the host does when the circuit faults or a registered output consumer
    /// throws.</summary>
    public CircuitFaultBehavior FaultBehavior { get; set; } = CircuitFaultBehavior.StopHost;

    internal CircuitOptions ToCircuitOptions(string defaultName, ILoggerFactory? containerLoggerFactory) => new()
    {
        Name = Name ?? defaultName,
        LoggerFactory = LoggerFactory ?? containerLoggerFactory,
        Workers = Workers,
        InputChannelCapacity = InputChannelCapacity,
        OutputChannelCapacity = OutputChannelCapacity,
        MaxStepItemsPerInput = MaxStepItemsPerInput,
        AutoStep = AutoStep,
        Storage = Storage ?? new InMemoryStorageProvider(),
        CheckpointInterval = CheckpointInterval,
    };
}
