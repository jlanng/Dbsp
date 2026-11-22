using System.Diagnostics;

namespace Dbsp.Benchmarks.Contracts;

/// <summary>
/// CI-gateable performance assertions. Absolute wall-clock numbers are far too noisy on
/// shared CI runners to assert on, so these contracts check the library's *scaling* claims
/// instead: a fixed-size delta must cost roughly the same — in allocated bytes and elapsed
/// time — whether operator state is small or large. Each contract runs the identical delta
/// cycle against a small and a large preloaded state (trials interleaved, medians compared)
/// and fails when the large/small ratio exceeds a deliberately generous bound. The state
/// sizes differ by 64x, so a regression from O(delta) to O(state) overshoots the bounds by
/// an order of magnitude while runner noise stays well inside them.
/// </summary>
internal static class PerfContracts
{
    private const double TimeRatioLimit = 3.5;
    private const double AllocRatioLimit = 1.1;
    private const int WarmupTrials = 3;
    private const int MeasuredTrials = 9;
    private const int CyclesPerTrial = 40;

    /// <param name="AllocLimitKb">An absolute ceiling on a trial's allocation, for a
    /// contract whose claim is about bytes per record rather than about scaling: a cost per
    /// delta row is flat across state sizes however large it is, so the ratio cannot see
    /// it. Allocation is deterministic enough to assert on where wall-clock time is not.</param>
    private sealed record Contract(
        string Name, int SmallScale, int LargeScale, Func<int, Task<IDeltaScenario>> Create, double? AllocLimitKb = null);

    // A trial of the lookup cycle is 40 × (256 facts in + 256 out). The same cycle through
    // a stream join allocates about 660 KB, and a remembered lookup has no reason to
    // allocate more: what it keeps per fact lives inline in its store. A structure per
    // remembered fact shows up as several times this.
    private const double LookupTrialAllocLimitKb = 1_000;

    private static readonly Contract[] Contracts =
    [
        new("join: delta cost vs indexed state size", 1_000, 64_000, Scenarios.JoinAsync),
        new("table join: delta cost vs table size and fact state size", 1_000, 64_000, Scenarios.TableJoinAsync),
        new("table index join: delta cost vs table size and fact state size", 1_000, 64_000, Scenarios.TableIndexJoinAsync),
        new("table lookup: delta cost vs table size and remembered state size", 1_000, 64_000, Scenarios.TableLookupAsync,
            LookupTrialAllocLimitKb),
        new("table changes: delta cost after bootstrap vs table size", 1_000, 1_000_000, Scenarios.TableChangesAsync),
        new("derived table: delta cost after bootstrap vs table size", 1_000, 1_000_000, Scenarios.DerivedTableAsync),
        new("changelog provider: commit cost vs state size", 1_000, 64_000, Scenarios.ChangelogCommitAsync),
        new("sum (linear aggregate): delta cost vs group count", 1_000, 64_000, Scenarios.SumAsync),
        new("min (tournament tree): delta cost vs group size", 100, 6_400, Scenarios.MinAsync),
        new("keyed distinct: delta cost vs values per key", 100, 6_400, Scenarios.KeyedDistinctAsync),
    ];

    public static async Task<int> RunAsync(string[] filters)
    {
        var selected = Contracts
            .Where(c => filters.Length == 0
                || filters.Any(f => c.Name.Contains(f, StringComparison.OrdinalIgnoreCase)))
            .ToArray();
        if (selected.Length == 0)
        {
            Console.Error.WriteLine($"No contract matches [{string.Join(", ", filters)}]. Available:");
            foreach (var contract in Contracts)
                Console.Error.WriteLine($"  {contract.Name}");
            return 2;
        }

        var failed = 0;
        foreach (var contract in selected)
        {
            if (!await RunContractAsync(contract))
                failed++;
        }

        Console.WriteLine(failed == 0
            ? $"All {selected.Length} performance contracts hold."
            : $"{failed} of {selected.Length} performance contracts FAILED.");
        return failed == 0 ? 0 : 1;
    }

    private static async Task<bool> RunContractAsync(Contract contract)
    {
        Console.WriteLine();
        Console.WriteLine($"{contract.Name}");
        Console.WriteLine($"  scale {contract.SmallScale:N0} -> {contract.LargeScale:N0} ({contract.LargeScale / contract.SmallScale}x)");

        await using var small = await contract.Create(contract.SmallScale);
        await using var large = await contract.Create(contract.LargeScale);

        for (var i = 0; i < WarmupTrials; i++)
        {
            await MeasureTrialAsync(small);
            await MeasureTrialAsync(large);
        }

        var smallSamples = new List<Sample>(MeasuredTrials);
        var largeSamples = new List<Sample>(MeasuredTrials);
        for (var i = 0; i < MeasuredTrials; i++)
        {
            smallSamples.Add(await MeasureTrialAsync(small));
            largeSamples.Add(await MeasureTrialAsync(large));
        }

        var timeRatio = Report("time ", Median(smallSamples, s => s.Milliseconds), Median(largeSamples, s => s.Milliseconds), "ms");
        var largeAllocKb = Median(largeSamples, s => s.Bytes / 1024.0);
        var allocRatio = Report("alloc", Median(smallSamples, s => s.Bytes / 1024.0), largeAllocKb, "KB");

        var holds = timeRatio <= TimeRatioLimit && allocRatio <= AllocRatioLimit
            && (contract.AllocLimitKb is not { } ceiling || largeAllocKb <= ceiling);
        var limits = $"time {TimeRatioLimit:0.0}x, alloc {AllocRatioLimit:0.0}x"
            + (contract.AllocLimitKb is { } limit ? $", alloc per trial {limit:N0} KB" : "");
        Console.WriteLine($"  {(holds ? "PASS" : "FAIL")}  (limits: {limits})");
        return holds;
    }

    private readonly record struct Sample(double Milliseconds, long Bytes);

    /// <summary>One trial: a burst of identical delta cycles, measured as a unit. Allocated
    /// bytes are read process-wide, which is safe because manual-step single-worker circuits
    /// run entirely on the calling thread and nothing else is running.</summary>
    private static async Task<Sample> MeasureTrialAsync(IDeltaScenario scenario)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        var bytesBefore = GC.GetTotalAllocatedBytes(precise: true);
        var stopwatch = Stopwatch.StartNew();
        for (var i = 0; i < CyclesPerTrial; i++)
            await scenario.RunCycleAsync();
        stopwatch.Stop();
        var bytes = GC.GetTotalAllocatedBytes(precise: true) - bytesBefore;
        return new Sample(stopwatch.Elapsed.TotalMilliseconds, bytes);
    }

    private static double Median(List<Sample> samples, Func<Sample, double> metric)
    {
        var sorted = samples.Select(metric).OrderBy(v => v).ToArray();
        var mid = sorted.Length / 2;
        return sorted.Length % 2 == 1 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
    }

    private static double Report(string label, double smallValue, double largeValue, string unit)
    {
        var ratio = smallValue <= 0 ? 1.0 : largeValue / smallValue;
        Console.WriteLine($"  {label}  {smallValue,10:N1} {unit} -> {largeValue,10:N1} {unit}   ratio {ratio,5:0.00}x");
        return ratio;
    }
}
