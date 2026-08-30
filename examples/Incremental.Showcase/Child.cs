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

    public static ProcessStartInfo Spawn(IEnumerable<string> arguments)
    {
        var start = new ProcessStartInfo(Environment.ProcessPath!) { UseShellExecute = false };
        foreach (var argument in arguments) start.ArgumentList.Add(argument);
        foreach (var name in DiagnosticPortVariables) start.Environment.Remove(name);
        return start;
    }
}
