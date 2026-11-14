using System.Diagnostics.Metrics;
using Acme.Contracts;
using static Incremental.Service.Tests.SupervisorTests;
using static Incremental.Service.Tests.TestService;

namespace Incremental.Service.Tests;

/// <summary>
/// A pipeline whose state grows without a pause is flagged (<see cref="StateGrowth"/>,
/// <see cref="ServiceOptions.StateGrowthAlarmAfter"/>): on its status, as an event and on
/// the gauge — and a bounded one is not, whether it fills and goes quiet or rises and falls.
/// </summary>
public class StateGrowthTests
{
    // Ahead of the clock: the node's own sampler reads once a second at the real time, and
    // a reading earlier than the last one taken is not a new sample, so it cannot interleave.
    private static readonly DateTimeOffset T0 = new(2100, 1, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Period = TimeSpan.FromMinutes(15);

    /// <summary>Feeds one sample a minute (the period's fifteenth); returns what each did.</summary>
    private static List<int> Feed(StateGrowth growth, IEnumerable<long> entries, int startMinute = 0) =>
        entries.Select((e, i) => growth.Observe(T0.AddMinutes(startMinute + i), e, Period)).ToList();

    [Fact]
    public void Growth_at_every_sample_for_the_period_is_flagged_from_where_it_began()
    {
        var growth = new StateGrowth();
        var results = Feed(growth, Enumerable.Range(1, 16).Select(i => i * 1000L));
        Assert.Equal(1, results[^1]);
        Assert.All(results.SkipLast(1), r => Assert.Equal(0, r));
        Assert.Equal(T0, growth.Since);
        Assert.Equal(1000, growth.OldestEntries);

        // Still growing: flagged once, not again.
        Assert.Equal(0, growth.Observe(T0.AddMinutes(16), 17_000, Period));
        // It falls: the flag clears.
        Assert.Equal(-1, growth.Observe(T0.AddMinutes(17), 16_500, Period));
        Assert.Null(growth.Since);
    }

    [Fact]
    public void A_few_quiet_samples_do_not_hide_growth_but_many_do()
    {
        // Three level steps of fifteen: still a leak with quiet minutes.
        var leak = new StateGrowth();
        long[] quiet = [1, 2, 3, 3, 4, 5, 6, 6, 7, 8, 9, 9, 10, 11, 12, 13];
        Assert.Equal(1, Feed(leak, quiet)[^1]);

        // Filled to its bound in five minutes, then its input went quiet: not a leak.
        var full = new StateGrowth();
        long[] filled = [1, 2, 3, 4, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6];
        Assert.All(Feed(full, filled), r => Assert.Equal(0, r));
    }

    [Fact]
    public void A_bounded_state_that_rises_and_falls_is_never_flagged()
    {
        var bounded = new StateGrowth();
        var plateau = Enumerable.Range(0, 120).Select(i => 10_000L + (i % 4 == 3 ? -50 : i % 4 * 20));
        Assert.All(Feed(bounded, plateau), r => Assert.Equal(0, r));
    }

    [Fact]
    public void Readings_closer_than_a_fifteenth_of_the_period_count_once_and_zero_turns_it_off()
    {
        var growth = new StateGrowth();
        // Once a second for fifteen minutes, growing all the while: sixteen samples a minute apart.
        for (var second = 0; second < 15 * 60; second++)
            Assert.Equal(0, growth.Observe(T0.AddSeconds(second), second, Period));
        Assert.Equal(1, growth.Observe(T0.AddMinutes(15), 15 * 60, Period));

        Assert.Equal(-1, growth.Observe(T0.AddMinutes(16), 10_000, TimeSpan.Zero));
        for (var minute = 0; minute < 40; minute++)
            Assert.Equal(0, growth.Observe(T0.AddMinutes(17 + minute), 20_000 + minute, TimeSpan.Zero));
    }

    [Fact]
    public async Task A_node_flags_the_pipeline_that_keeps_every_record_and_not_the_one_that_counts()
    {
        var node = "sg" + Guid.NewGuid().ToString("N")[..8];
        var gauges = new List<(string Pipeline, long Value)>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == ServiceMeter.Name && instrument.Name == "incremental.service.pipeline.state_growing")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            string? n = null, p = null;
            foreach (var tag in tags)
                (n, p) = tag.Key == "node" ? ((string?)tag.Value, p) : tag.Key == "pipeline" ? (n, (string?)tag.Value) : (n, p);
            if (n == node)
                lock (gauges)
                    gauges.Add((p!, value));
        });
        listener.Start();

        await using var service = await StartAsync(nodeId: node, options: o => o.StateGrowthAlarmAfter = Period);
        await service.DeclareInstrumentsAsync(Instruments(10));
        // "enrich" joins every trade to its instrument and keeps it: one fact per trade, for
        // ever. "counts" keeps a count per trader: five entries however many trades.
        await service.DeployAsync(Spec("keeps-all", "enrich", "g-in", "g-out"));
        await service.DeployAsync(Spec("counts", "counts", "c-in", "c-out"));
        var enriched = service.Memory.UpsertSink<long, EnrichedTrade>("g-out");

        // Sixteen minutes of the sampler's time, a hundred trades between readings.
        for (var minute = 0; minute <= 16; minute++)
        {
            service.Memory.Log<Trade>("g-in").AppendRange(Trades(minute * 100, 100));
            service.Memory.Log<Trade>("c-in").AppendRange(Trades(minute * 100, 100));
            await Eventually(() => enriched.Rows.Count == (minute + 1) * 100, "the trades through");
            await Eventually(() => service.Pipelines.Get("keeps-all").StateSize?.Entries == (minute + 1) * 100, "the state counted");
            service.Node.Metrics.Sample(T0.AddMinutes(minute));
        }

        Assert.Equal(T0, service.Pipelines.Get("keeps-all").StateGrowingSince);
        Assert.Null(service.Pipelines.Get("counts").StateGrowingSince);
        Assert.Contains(service.Node.RecentEvents(), e => e.Subject == "keeps-all" && e.Message.StartsWith("state growing: 100 to 1,600 entries in 15 min"));
        Assert.DoesNotContain(service.Node.RecentEvents(), e => e.Subject == "counts" && e.Message.StartsWith("state growing"));

        service.Node.RefreshNodeGauges();
        listener.RecordObservableInstruments();
        lock (gauges)
        {
            Assert.Contains(("keeps-all", 1L), gauges);
            Assert.Contains(("counts", 0L), gauges);
        }
    }
}
