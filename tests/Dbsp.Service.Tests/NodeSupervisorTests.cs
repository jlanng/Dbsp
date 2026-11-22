using System.Diagnostics;
using System.Text;

namespace Dbsp.Service.Tests;

/// <summary>
/// <see cref="NodeSupervisor"/> by itself: no node, no broker. The supervised program is
/// the failover app with an <c>--exit-plan</c> — a file of exit codes, one taken at each
/// start — so the tests can say how the "node" ends and read what the supervisor did
/// about it. That it brings a real node back as the standby is
/// <c>KafkaServiceProcessTests.A_supervised_node_that_is_fenced_resigns_or_is_killed…</c>.
/// </summary>
public class NodeSupervisorTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(60);

    private sealed class Supervised : IDisposable
    {
        private readonly Process _process;
        private readonly StringBuilder _stderr = new();
        private readonly StringBuilder _stdout = new();
        private readonly string _plan;

        public Supervised(params string[] plan)
        {
            _plan = Path.Combine(Path.GetTempPath(), "dbsp-supervisor-test-" + Guid.NewGuid().ToString("N")[..8] + ".plan");
            File.WriteAllLines(_plan, plan);
            var baseDir = AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar);
            var dll = Path.GetFullPath(Path.Combine(
                baseDir, "..", "..", "..", "..", "Dbsp.Service.FailoverApp", "bin",
                Path.GetFileName(Path.GetDirectoryName(baseDir)!), Path.GetFileName(baseDir), "Dbsp.Service.FailoverApp.dll"));
            Assert.True(File.Exists(dll), $"Service failover app not found at {dll} — build tests/Dbsp.Service.FailoverApp.");
            var info = new ProcessStartInfo("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true, RedirectStandardInput = true };
            foreach (var argument in new[] { dll, "--supervise", "1", "--exit-plan", _plan })
                info.ArgumentList.Add(argument);
            _process = Process.Start(info) ?? throw new InvalidOperationException("The supervisor did not start.");
            _process.ErrorDataReceived += (_, e) =>
            {
                lock (_stderr)
                    _stderr.AppendLine(e.Data);
            };
            _process.OutputDataReceived += (_, e) =>
            {
                lock (_stdout)
                    _stdout.AppendLine(e.Data);
            };
            _process.BeginErrorReadLine();
            _process.BeginOutputReadLine();
        }

        public int Id => _process.Id;

        public string Said
        {
            get
            {
                lock (_stderr)
                    return _stderr.ToString();
            }
        }

        public bool ChildSaid(string text)
        {
            lock (_stdout)
                return _stdout.ToString().Contains(text, StringComparison.Ordinal);
        }

        public int Count(string text) => Said.Split(text).Length - 1;

        public async Task<int> ExitCodeAsync()
        {
            using var limit = new CancellationTokenSource(Patience);
            try
            {
                await _process.WaitForExitAsync(limit.Token);
            }
            catch (OperationCanceledException)
            {
                Assert.Fail($"The supervisor did not end.{Environment.NewLine}{Said}");
            }
            return _process.ExitCode;
        }

        public void Dispose()
        {
            try
            {
                if (!_process.HasExited)
                    _process.Kill(entireProcessTree: true);
            }
            catch (InvalidOperationException)
            {
            }
            _process.Dispose();
            File.Delete(_plan);
        }
    }

    [Fact]
    public async Task A_process_that_ends_fenced_or_resigned_is_started_again_at_once_and_one_asked_to_stop_is_not()
    {
        using var supervised = new Supervised("3", "4");
        Assert.Equal(0, await supervised.ExitCodeAsync());

        // Three starts: fenced, resigned, and the one that was "asked to stop" (exit 0).
        Assert.Equal(3, supervised.Count("node supervisor: started process"));
        Assert.Contains("ended with exit code 3 (fenced: another node leads)", supervised.Said);
        Assert.Contains("ended with exit code 4 (resigned: it stepped down)", supervised.Said);

        // Losing the leadership is how a healthy node ends: no growing delay.
        Assert.Equal(2, supervised.Count("starting it again in 200 ms"));
        Assert.Contains("ended with exit code 0; not starting it again", supervised.Said);
    }

    [Fact]
    public async Task A_process_that_keeps_failing_early_is_started_again_more_and_more_slowly_and_never_given_up_on()
    {
        using var supervised = new Supervised("5", "1", "134", "9");
        Assert.Equal(0, await supervised.ExitCodeAsync());
        Assert.Equal(5, supervised.Count("node supervisor: started process"));
        Assert.Contains("exit code 5 (it could not lead)", supervised.Said);
        Assert.Contains("exit code 134 (killed by signal 6)", supervised.Said);

        // The delay doubles from the first: 400, 800, 1600, 3200 ms.
        var delays = System.Text.RegularExpressions.Regex.Matches(supervised.Said, @"starting it again in (\d+) ms").Select(m => int.Parse(m.Groups[1].Value)).ToList();
        Assert.Equal([400, 800, 1600, 3200], delays);
    }

    [Fact]
    public async Task A_node_whose_own_takeover_failed_ends_with_5_and_is_slowed_down_where_one_that_was_replaced_ends_with_3()
    {
        // The node's side: what it exits with. Replaced by another node, or resigned: how a
        // healthy node ends. Fenced by its own doing — it took the log and then could not
        // lead — is a failure, and the same failure is likely waiting for the next start.
        static NodeLeadershipStatus Status(Dbsp.Kafka.NodeRole role, bool failed = false) => new("a", role, 2, "b", 3) { TakeoverFailed = failed };
        Assert.Equal(3, ServiceNode.ExitCodeFor(Status(Dbsp.Kafka.NodeRole.Fenced)));
        Assert.Equal(4, ServiceNode.ExitCodeFor(Status(Dbsp.Kafka.NodeRole.Resigned)));
        Assert.Equal(5, ServiceNode.ExitCodeFor(Status(Dbsp.Kafka.NodeRole.Fenced, failed: true)));

        // The in-memory leadership says which it was, as the Kafka one does.
        var cluster = new InMemoryServiceCluster();
        var failing = cluster.Node("a");
        var callbacks = new LeadershipCallbacks(
            _ => Task.CompletedTask, (_, _) => throw new InvalidOperationException("no pipelines today"), _ => Task.CompletedTask,
            () => throw new NotSupportedException());
        await Assert.ThrowsAsync<InvalidOperationException>(() => failing.Leadership.StartAsync(callbacks, default));
        Assert.Equal(Dbsp.Kafka.NodeRole.Fenced, failing.Leadership.Status.Role);
        Assert.True(failing.Leadership.Status.TakeoverFailed);
        Assert.Equal(5, ServiceNode.ExitCodeFor(failing.Leadership.Status));

        var leading = cluster.Node("b");
        var taking = cluster.Node("c");
        var fine = new LeadershipCallbacks(_ => Task.CompletedTask, (_, _) => Task.CompletedTask, _ => Task.CompletedTask, () => throw new NotSupportedException());
        await leading.Leadership.StartAsync(fine, default);
        await taking.Leadership.StartAsync(fine, default);
        await taking.Leadership.TakeOverAsync(default);
        Assert.Equal(Dbsp.Kafka.NodeRole.Fenced, leading.Leadership.Status.Role);
        Assert.False(leading.Leadership.Status.TakeoverFailed);
        Assert.Equal(3, ServiceNode.ExitCodeFor(leading.Leadership.Status));

        // The supervisor's side: a node that keeps ending with 5 is started again more
        // and more slowly, where one that keeps ending with 3 is started again at once.
        using var failed = new Supervised("5", "5", "5");
        Assert.Equal(0, await failed.ExitCodeAsync());
        var delays = System.Text.RegularExpressions.Regex.Matches(failed.Said, @"starting it again in (\d+) ms").Select(m => int.Parse(m.Groups[1].Value)).ToList();
        Assert.Equal([400, 800, 1600], delays);
        using var replaced = new Supervised("3", "3", "3");
        Assert.Equal(0, await replaced.ExitCodeAsync());
        Assert.Equal(3, replaced.Count("starting it again in 200 ms"));
    }

    [Fact]
    public async Task A_process_that_refuses_its_arguments_is_not_started_again()
    {
        using var supervised = new Supervised("2", "3");
        Assert.Equal(2, await supervised.ExitCodeAsync());
        Assert.Equal(1, supervised.Count("node supervisor: started process"));
        Assert.Contains("its exit code says so", supervised.Said);
    }

    [Fact]
    public async Task A_stop_request_is_passed_on_to_the_node_and_its_exit_code_is_the_supervisors()
    {
        if (OperatingSystem.IsWindows())
            Assert.Skip("SIGTERM needs a Unix.");
        using var supervised = new Supervised("wait", "3");
        var clock = Stopwatch.StartNew();
        while (!supervised.ChildSaid("waiting"))
        {
            Assert.True(clock.Elapsed < Patience, $"the supervised process did not start.{Environment.NewLine}{supervised.Said}");
            await Task.Delay(20);
        }

        // SIGTERM to the supervisor, as a platform stopping the service sends it.
        using (var kill = Process.Start("kill", ["-TERM", supervised.Id.ToString(System.Globalization.CultureInfo.InvariantCulture)])!)
            await kill.WaitForExitAsync();

        // The node heard it (it ends with 42 on SIGTERM), and it is not started again
        // although 42 is a code that otherwise would be.
        Assert.Equal(42, await supervised.ExitCodeAsync());
        Assert.Equal(1, supervised.Count("node supervisor: started process"));
        Assert.Contains("asked to stop", supervised.Said);
    }
}
