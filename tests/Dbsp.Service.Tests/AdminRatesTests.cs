using System.Text.Json;
using Dbsp.Service.Admin;

namespace Dbsp.Service.Tests;

/// <summary>The rates the admin shows a pipeline: a pipeline that steps every two seconds
/// (a sink's <c>minInterval</c>) has a rate over its intervals, not over one second.</summary>
public class AdminRatesTests
{
    private static MetricSample Second(int i, long steps, long recordsIn, long recordsOut, double stepSeconds = 0) =>
        new(DateTimeOffset.UnixEpoch.AddSeconds(i), 1.0, steps, stepSeconds, stepSeconds, recordsIn, recordsOut, steps, 0, 0, 0, null, null);

    private static JsonElement Rates(IReadOnlyList<MetricSample> seconds) =>
        JsonSerializer.SerializeToElement(AdminEventHub.Rates(seconds[^1], seconds), AdminJson.Options);

    [Fact]
    public void A_pipeline_that_steps_every_two_seconds_reads_its_steady_rate_and_says_it_is_batched()
    {
        // A steady 50 records a second, taken in a step every other second: 100, 0, 100, 0 …
        var seconds = Enumerable.Range(0, 30).Select(i => i % 2 == 0 ? Second(i, 1, 100, 100, 2.0) : Second(i, 0, 0, 0)).ToList();
        foreach (var end in new[] { 29, 30 })
        {
            var rates = Rates(seconds.Take(end).ToList());
            Assert.Equal(50, rates.GetProperty("in").GetDouble());
            Assert.Equal(50, rates.GetProperty("out").GetDouble());
            // The spacing as measured over the last half minute: 15 steps in 29 or 30 seconds.
            Assert.InRange(rates.GetProperty("batchedEveryS").GetDouble(), 1.9, 2.0);
            Assert.True(rates.GetProperty("rateOverS").GetDouble() >= 4, "over at least two intervals");
        }
    }

    [Fact]
    public void A_pipeline_that_steps_every_second_reads_its_last_second()
    {
        var seconds = Enumerable.Range(0, 30).Select(i => Second(i, 10, 40 + i, 40 + i, 0.01)).ToList();
        var rates = Rates(seconds);
        Assert.Equal(69, rates.GetProperty("in").GetDouble());
        Assert.False(rates.TryGetProperty("batchedEveryS", out _));
        Assert.Equal(1, rates.GetProperty("stepMs").GetDouble());
    }

    [Fact]
    public void An_idle_pipeline_is_not_batched()
    {
        var seconds = Enumerable.Range(0, 30).Select(i => Second(i, 0, 0, 0)).ToList();
        var rates = Rates(seconds);
        Assert.Equal(0, rates.GetProperty("in").GetDouble());
        Assert.False(rates.TryGetProperty("batchedEveryS", out _));
    }
}
