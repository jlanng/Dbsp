using System.Diagnostics;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Dbsp.Service;

/// <summary>Settings of <see cref="NodeSupervisor.RunAsync"/>.</summary>
public sealed class NodeSupervisorOptions
{
    /// <summary>How long after the node's process ends the next one is started, when the
    /// last one ran for at least <see cref="StableAfter"/> or ended because it lost the
    /// leadership (exit codes 3 and 4), which is how a healthy node ends. Default 200 ms.</summary>
    public TimeSpan RestartDelay { get; init; } = TimeSpan.FromMilliseconds(200);

    /// <summary>The longest delay between starts. A process that keeps ending early —
    /// it cannot reach its brokers, it crashes at start — is started again after a delay
    /// that doubles from <see cref="RestartDelay"/> up to this. Default 30 s.</summary>
    public TimeSpan MaxRestartDelay { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>A process that ran this long is not counted as ending early. Default 60 s.</summary>
    public TimeSpan StableAfter { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>How long the node's process is given to stop after the supervisor has been
    /// asked to (and has passed the request on) before it is killed. Longer than the
    /// node's own <see cref="ServiceOptions.StopTimeout"/>. Default 60 s.</summary>
    public TimeSpan StopTimeout { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>Whether a process that ended with this exit code is started again. Default:
    /// every code but 0 (it was asked to stop) and 2 (it refused its arguments).</summary>
    public Func<int, bool>? ShouldRestart { get; init; }

    /// <summary>Receives the supervisor's own lines (a start, an exit and what follows).
    /// Default: standard error.</summary>
    public Action<string>? Log { get; init; }
}

/// <summary>
/// The process supervisor a service node needs, for a host that has none.
/// <para>
/// A node that has been the leader and no longer is ends its process: exit code 3 when it
/// was fenced, 4 when it stepped down, 5 when it could not lead. That is by design — what
/// it holds was the leader's and nothing demotes it in place — and it is only half of the
/// design: <b>something must start the process again</b>, and then it comes back as the
/// standby. In production that something is the platform: a systemd unit with
/// <c>Restart=always</c>, a Kubernetes pod (<c>restartPolicy: Always</c>), an ECS service.
/// Where there is none — a terminal, a test, a bare container entry point — this is it:
/// the program starts itself again as a child process and keeps doing so.
/// </para>
/// <para>
/// The contract, which is the one to ask of any supervisor: start the process again after
/// every exit except 0 (it was asked to stop) and 2 (bad arguments); at once after 3 and
/// 4; with a delay that doubles, up to half a minute, while it keeps ending within a
/// minute of starting; pass a stop request (SIGTERM, Ctrl+C) on to it and wait for it. A
/// supervisor that gives up after some number of restarts will, some day, leave a
/// deployment with one node.
/// </para>
/// </summary>
public static class NodeSupervisor
{
    /// <summary>Set to <c>1</c> in the environment of the process this supervisor starts,
    /// which is how that process knows not to supervise in its turn.</summary>
    public const string EnvironmentVariable = "DBSP_SERVICE_SUPERVISED";

    /// <summary>Whether this process was started by <see cref="RunAsync"/>: it is the node,
    /// and must run it.</summary>
    public static bool IsSupervised => Environment.GetEnvironmentVariable(EnvironmentVariable) == "1";

    /// <summary>
    /// Runs this same program, with <paramref name="args"/>, as a child process, and again
    /// each time it ends in a way that calls for it (see the class). Standard input, output
    /// and error are the child's. Returns the child's exit code when it ended and is not to
    /// be started again, or when <paramref name="stop"/> or a termination signal asked for
    /// the end.
    /// </summary>
    /// <example><code>
    /// if (!NodeSupervisor.IsSupervised)
    ///     return await NodeSupervisor.RunAsync(args);
    /// // … build the host and run the node …
    /// </code></example>
    public static async Task<int> RunAsync(string[] args, NodeSupervisorOptions? options = null, CancellationToken stop = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        options ??= new NodeSupervisorOptions();
        var log = options.Log ?? (line => Console.Error.WriteLine(line));
        var shouldRestart = options.ShouldRestart ?? (code => code is not (0 or 2));

        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(stop);
        Process? current = null;
        var gate = new object();

        void Stop()
        {
            // The request is the node's to act on: it steps down or drains, and exits 0.
            lock (gate)
            {
                try
                {
                    stopping.Cancel();
                }
                catch (ObjectDisposedException)
                {
                    return;
                }
                if (current is { HasExited: false } running)
                    Terminate(running);
            }
        }

        // Ctrl+C reaches the child by itself (it shares the terminal); SIGTERM does not.
        using var term = PosixSignalRegistration.Create(PosixSignal.SIGTERM, context =>
        {
            context.Cancel = true;
            Stop();
        });
        using var interrupt = PosixSignalRegistration.Create(PosixSignal.SIGINT, context =>
        {
            context.Cancel = true;
            lock (gate)
                stopping.Cancel();
        });
        using var onStop = stop.Register(Stop);

        var delay = options.RestartDelay;
        while (true)
        {
            var info = StartInfo(args);
            var started = Stopwatch.StartNew();
            Process process;
            lock (gate)
            {
                if (stopping.IsCancellationRequested)
                    return 0;
                process = Process.Start(info) ?? throw new InvalidOperationException("The node's process could not be started.");
                current = process;
            }
            using (process)
            {
                log($"node supervisor: started process {process.Id}");
                var exited = process.WaitForExitAsync(CancellationToken.None);
                var asking = Task.Delay(Timeout.Infinite, stopping.Token);
                if (await Task.WhenAny(exited, asking).ConfigureAwait(false) != exited
                    && await Task.WhenAny(exited, Task.Delay(options.StopTimeout, CancellationToken.None)).ConfigureAwait(false) != exited)
                {
                    log($"node supervisor: process {process.Id} did not stop within {options.StopTimeout.TotalSeconds:F0} s of being asked; killing it");
                    process.Kill(entireProcessTree: true);
                }
                await exited.ConfigureAwait(false);
                var asked = stopping.IsCancellationRequested;
                var code = process.ExitCode;
                lock (gate)
                    current = null;
                if (asked || !shouldRestart(code))
                {
                    log($"node supervisor: process {process.Id} ended with exit code {code}; not starting it again ({(asked ? "asked to stop" : "its exit code says so")})");
                    return code;
                }

                // Losing the leadership is how a healthy node ends; so is having run for a
                // while. Anything else that keeps happening is slowed down.
                var healthy = code is 3 or 4 || started.Elapsed >= options.StableAfter;
                delay = healthy ? options.RestartDelay : TimeSpan.FromTicks(Math.Min(options.MaxRestartDelay.Ticks, Math.Max(options.RestartDelay.Ticks, delay.Ticks * 2)));
                log($"node supervisor: process {process.Id} ended with exit code {code} ({Meaning(code)}) after {started.Elapsed.TotalSeconds:F1} s; starting it again in {delay.TotalMilliseconds:F0} ms");
            }
            try
            {
                await Task.Delay(delay, stopping.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return 0;
            }
        }
    }

    /// <summary>
    /// The same contract for nodes that share a process, where a node that lost the
    /// leadership cannot end the process without ending the others: the in-memory
    /// backend's two nodes, as the example and the admin test host run them. Starts a host
    /// with <paramref name="start"/>, waits for its node's <see cref="ServiceNode.Ended"/>,
    /// disposes the host, and starts another the same way after the delay
    /// <see cref="RunAsync"/> would give the exit code — 3 and 4 at once, anything else
    /// doubling while it keeps ending within <see cref="NodeSupervisorOptions.StableAfter"/>.
    /// The node's <see cref="ServiceOptions.ExitOnLeadershipLoss"/> must be off.
    /// <paramref name="started"/> is told of each host as it is started; the one running
    /// when <paramref name="stop"/> is cancelled is the caller's to dispose, in whatever
    /// order its other nodes need, after this task has completed.
    /// <see cref="NodeSupervisorOptions.ShouldRestart"/> and <see cref="NodeSupervisorOptions.StopTimeout"/>
    /// do not apply.
    /// </summary>
    public static async Task RunInProcessAsync<THost>(
        Func<CancellationToken, Task<THost>> start, Func<THost, ServiceNode> node, Action<THost>? started = null,
        NodeSupervisorOptions? options = null, CancellationToken stop = default)
        where THost : IAsyncDisposable
    {
        ArgumentNullException.ThrowIfNull(start);
        ArgumentNullException.ThrowIfNull(node);
        options ??= new NodeSupervisorOptions();
        var log = options.Log ?? (line => Console.Error.WriteLine(line));
        var delay = options.RestartDelay;
        while (!stop.IsCancellationRequested)
        {
            THost host;
            try
            {
                host = await start(stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // A start that fails is a node that ended early: slowed down, never given up.
                delay = TimeSpan.FromTicks(Math.Min(options.MaxRestartDelay.Ticks, Math.Max(options.RestartDelay.Ticks, delay.Ticks * 2)));
                log($"node supervisor: a node did not start ({ex.GetType().Name}: {ex.Message}); trying again in {delay.TotalMilliseconds:F0} ms");
                try
                {
                    await Task.Delay(delay, stop).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                continue;
            }
            started?.Invoke(host);
            var serviceNode = node(host);
            var ran = Stopwatch.StartNew();
            int code;
            try
            {
                code = await serviceNode.Ended.WaitAsync(stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                return;
            }
            var healthy = code is 3 or 4 || ran.Elapsed >= options.StableAfter;
            delay = healthy ? options.RestartDelay : TimeSpan.FromTicks(Math.Min(options.MaxRestartDelay.Ticks, Math.Max(options.RestartDelay.Ticks, delay.Ticks * 2)));
            log($"node supervisor: node {serviceNode.NodeId} ended with exit code {code} ({Meaning(code)}) after {ran.Elapsed.TotalSeconds:F1} s; starting it again in {delay.TotalMilliseconds:F0} ms");
            await host.DisposeAsync().ConfigureAwait(false);
            try
            {
                await Task.Delay(delay, stop).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private static string Meaning(int code) => code switch
    {
        3 => "fenced: another node leads",
        4 => "resigned: it stepped down",
        5 => "it could not lead",
        > 128 => "killed by signal " + (code - 128),
        _ => "a failure",
    };

    private static ProcessStartInfo StartInfo(string[] args)
    {
        // The same program again: through its own executable when it has one, else as
        // `dotnet <entry assembly>`.
        var self = Environment.ProcessPath ?? throw new InvalidOperationException("This process has no path to start again.");
        var info = new ProcessStartInfo(self) { UseShellExecute = false };
        if (Path.GetFileNameWithoutExtension(self).Equals("dotnet", StringComparison.OrdinalIgnoreCase))
            info.ArgumentList.Add(Assembly.GetEntryAssembly()?.Location is { Length: > 0 } entry
                ? entry
                : throw new InvalidOperationException("This process has no entry assembly to start again."));
        foreach (var argument in args)
            info.ArgumentList.Add(argument);
        info.Environment[EnvironmentVariable] = "1";
        return info;
    }

    [DllImport("libc", EntryPoint = "kill", SetLastError = true)]
    private static extern int SendSignal(int pid, int signal);

    /// <summary>Asks the process to stop as its platform does it: SIGTERM where there is
    /// one. On Windows there is none to send to another process; the child shares the
    /// console and has had the Ctrl+C, and a stop that came from code ends it.</summary>
    private static void Terminate(Process process)
    {
        try
        {
            if (OperatingSystem.IsWindows())
                process.Kill(entireProcessTree: true);
            else
                _ = SendSignal(process.Id, 15);
        }
        catch (Exception ex) when (ex is InvalidOperationException or System.ComponentModel.Win32Exception)
        {
            // It ended a moment ago.
        }
    }
}
