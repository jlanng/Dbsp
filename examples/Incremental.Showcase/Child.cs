using System.Diagnostics;

namespace Incremental.Showcase;

/// <summary>
/// Spawning this binary again: the cluster peer, and the relaunch-into-another-topology
/// path. Both go through here so neither inherits the parent's diagnostic-port setup.
/// </summary>
public static class Child
{
    // An IDE or profiler that launches the showcase — Rider's Dynamic Program Analysis,
    // dotnet-monitor, dotnet-trace, VS diagnostics — sets DOTNET_DiagnosticPorts with the
    // `suspend` flag, and the runtime then blocks before Main until the tool sends
    // ResumeStartup ("The runtime has been configured to pause during startup…"). The tool
    // resumes the process it launched. A process *we* spawn inherits the same variable,
    // connects to the same port, and waits for a resume addressed to someone else: it hangs
    // at startup, before any of this code runs, so the peer never reports in and a relaunch
    // never rebinds its port. Neither child is a thing anyone wants to profile, so drop the
    // configuration rather than propagate it.
    static readonly string[] DiagnosticPortVariables =
    [
        "DOTNET_DiagnosticPorts", "COMPlus_DiagnosticPorts",
        "DOTNET_DefaultDiagnosticPortSuspend", "COMPlus_DefaultDiagnosticPortSuspend",
    ];

    public static ProcessStartInfo Spawn(IEnumerable<string> arguments, int workers)
    {
        var start = new ProcessStartInfo(Environment.ProcessPath!) { UseShellExecute = false };
        foreach (var argument in arguments) start.ArgumentList.Add(argument);
        foreach (var name in DiagnosticPortVariables) start.Environment.Remove(name);
        start.Environment["DOTNET_GCHeapCount"] = GcAdvice.HeapCountVariable(workers);
        return start;
    }
}

/// <summary>
/// The GC heap count wants to match the worker count (see examples/Directory.Build.props). The
/// csproj bakes in the default topology's, but --workers and the cluster peer pick another one,
/// and the heap count is fixed before Main runs — so a process with a different worker count has
/// to be spawned with the variable set, and one launched by hand can only be told about it.
/// </summary>
public static class GcAdvice
{
    /// <summary>DOTNET_GC* variables are read as hexadecimal, so 16 heaps is "10", not "16".</summary>
    public static string HeapCountVariable(int workers) => workers.ToString("x");

    /// <summary>The heap count actually in force, whether it came from the runtimeconfig or the
    /// environment. Null if the runtime does not report one (workstation GC).</summary>
    public static int? HeapCount =>                            // reported as Int64, not Int32
        System.GC.GetConfigurationVariables().TryGetValue("HeapCount", out var value) && value is long count && count > 0
            ? (int)count
            : null;

    /// <summary>The advice to print when this process's heap count does not match its workers.</summary>
    public static string? Mismatch(int workers) =>
        HeapCount is { } heaps && heaps != workers
            ? $"GC heap count is {heaps} for {workers} worker{(workers == 1 ? "" : "s")} — " +
              $"relaunch with DOTNET_GCHeapCount={HeapCountVariable(workers)}" +
              $"{(workers > 9 ? $" (hex for {workers})" : "")} to match"
            : null;
}
