using System.Globalization;

namespace Incremental.Torture;

/// <summary>
/// Per-thread CPU accounting straight from <c>/proc/self/task</c>. The circuit names its
/// worker threads <c>circuit-worker-N</c>, so "how saturated are the workers?" — the question
/// the ramp is looking for — is answerable without instrumenting the step path at all:
/// utime+stime of exactly those threads, over wall time, over the worker count.
/// </summary>
internal static class ThreadCpu
{
    private static readonly double TicksPerSecond = 100.0;   // USER_HZ, fixed at 100 on Linux

    internal readonly record struct Snapshot(double WorkerSeconds, double ProcessSeconds, int WorkerThreads);

    public static Snapshot Take()
    {
        double workers = 0, total = 0;
        var workerThreads = 0;
        foreach (var task in Directory.EnumerateDirectories("/proc/self/task"))
        {
            string stat;
            try
            {
                stat = File.ReadAllText(Path.Combine(task, "stat"));
            }
            catch (IOException)
            {
                continue;                                     // thread exited mid-scan
            }

            // "pid (comm) state ppid ..." — comm can contain spaces and parentheses, so split
            // after the last ')' rather than on whitespace.
            var close = stat.LastIndexOf(')');
            var open = stat.IndexOf('(');
            if (close < 0 || open < 0 || close <= open)
                continue;
            var comm = stat.AsSpan(open + 1, close - open - 1);
            var fields = stat[(close + 2)..].Split(' ');
            // Fields after comm are numbered from 3 (state), so utime is index 11, stime 12.
            if (fields.Length < 13)
                continue;
            var seconds =
                (double.Parse(fields[11], CultureInfo.InvariantCulture) +
                 double.Parse(fields[12], CultureInfo.InvariantCulture)) / TicksPerSecond;

            total += seconds;
            if (comm.StartsWith("circuit-worker"))
            {
                workers += seconds;
                workerThreads++;
            }
        }
        return new Snapshot(workers, total, workerThreads);
    }
}
