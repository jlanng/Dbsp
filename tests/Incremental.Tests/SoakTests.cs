namespace Incremental.Tests;

/// <summary>
/// Opt-in endurance test — not CI-gating. Drives a watermarked, evicting, windowed pipeline
/// through millions of events at Workers = 4 and asserts (a) every finalized window's count
/// is exactly right and announced exactly once, and (b) memory stays bounded — eviction must
/// hold state roughly constant while event time advances, so heap growth after warm-up is
/// capped. Run with:
///
///     INCREMENTAL_SOAK=1 dotnet test --project tests/Incremental.Tests --filter SoakTests
///
/// (takes on the order of a minute; the RNG is fixed-seed, so a failure reproduces exactly).
/// </summary>
public class SoakTests
{
    private const int Steps = 1_000;
    private const int EventsPerStep = 3_000; // 3M events total
    private const int Pages = 16;

    [Fact]
    public async Task Windowed_evicting_pipeline_is_correct_and_memory_bounded_over_millions_of_events()
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable("INCREMENTAL_SOAK") == "1",
            "Soak test is opt-in: set INCREMENTAL_SOAK=1 to run it (~1 minute).");

        var windowSize = TimeSpan.FromSeconds(5);
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = 4 });
        var views = builder.Input<(int Page, DateTimeOffset At)>("views");
        var windowed = views
            .WithEventTime(v => v.At, lateness: TimeSpan.FromSeconds(2))
            .Tumble(windowSize, v => v.At)
            .GroupBy(w => w.WindowStart)
            .EvictWhen(window => window + windowSize);
        var counts = windowed.Count();
        var finalDef = windowed.WindowCloses().Grouped()
            .Join(counts.Grouped(), (window, _, count) => (Window: window, Count: count))
            .ToOutput();

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(views);
        var final = circuit.Handle(finalDef);

        var rng = new Random(12345);
        var at = DateTimeOffset.UnixEpoch;
        var expected = new Dictionary<DateTimeOffset, long>(); // window start -> event count
        var finalized = new Dictionary<DateTimeOffset, long>();
        long baseline = 0;

        for (var step = 0; step < Steps; step++)
        {
            for (var i = 0; i < EventsPerStep; i++)
            {
                // Time advances ~1.5ms/event with small jitter inside the lateness bound, so
                // nothing is dropped and the oracle is exact. That is ~4.5s of event time per
                // step: windows keep closing and their state keeps being evicted.
                at += TimeSpan.FromMicroseconds(1_000 + rng.Next(1_000));
                var window = new DateTimeOffset(at.UtcTicks / windowSize.Ticks * windowSize.Ticks, TimeSpan.Zero);
                expected[window] = expected.GetValueOrDefault(window) + 1;
                await input.InsertAsync((rng.Next(Pages), at));
            }
            await circuit.StepAsync().WaitAsync(TimeSpan.FromSeconds(20));
            while (final.Reader.TryRead(out var delta))
                foreach (var ((window, count), weight) in delta)
                {
                    Assert.Equal(1, weight); // finalized rows are announced once, never retracted
                    Assert.False(finalized.ContainsKey(window), $"window {window:HH:mm:ss} finalized twice");
                    finalized[window] = count;
                }

            if (step == 99)
            {
                baseline = GC.GetTotalMemory(forceFullCollection: true);
            }
            else if (step > 99 && step % 200 == 0)
            {
                var heap = GC.GetTotalMemory(forceFullCollection: true);
                Assert.True(heap < baseline + 96_000_000,
                    $"heap grew from {baseline / 1e6:f0}MB to {heap / 1e6:f0}MB by step {step} — " +
                    "eviction should keep windowed state bounded");
            }
        }

        // The watermark is frozen between steps, so deadlines passed by the final batch's
        // events fire on the next step — run one empty flush step (what AutoStep does).
        await circuit.StepAsync().WaitAsync(TimeSpan.FromSeconds(20));
        while (final.Reader.TryRead(out var delta))
            foreach (var ((window, count), weight) in delta)
            {
                Assert.Equal(1, weight);
                Assert.False(finalized.ContainsKey(window), $"window {window:HH:mm:ss} finalized twice");
                finalized[window] = count;
            }

        // Every window the watermark passed must have been finalized with the exact count.
        var watermark = at - TimeSpan.FromSeconds(2);
        var closed = expected.Where(kv => kv.Key + windowSize <= watermark).ToList();
        Assert.True(closed.Count > 500, $"expected many closed windows, saw {closed.Count}");
        foreach (var (window, count) in closed)
        {
            Assert.True(finalized.TryGetValue(window, out var got),
                $"window {window:HH:mm:ss} closed but was never finalized");
            Assert.Equal(count, got);
        }
        Assert.True(finalized.Count >= closed.Count);
    }
}
