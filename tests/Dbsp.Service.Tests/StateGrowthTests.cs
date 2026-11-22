using System.Diagnostics.Metrics;
using Dbsp.Storage;
using Acme.Contracts;
using static Dbsp.Service.Tests.SupervisorTests;
using static Dbsp.Service.Tests.TestService;

namespace Dbsp.Service.Tests;

/// <summary>
/// A pipeline with a store that grows without a pause is flagged (<see cref="StateGrowth"/>,
/// <see cref="ServiceOptions.StateGrowthAlarmAfter"/>): on its status, as an event and on
/// the gauge — and a bounded one is not, whether it fills and goes quiet, rises and falls,
/// or is still filling what its definition keeps open on a watermark that moves.
/// </summary>
public class StateGrowthTests
{
    // Ahead of the clock: the node's own sampler reads once a second at the real time, and
    // a reading earlier than the last one taken is not a new sample, so it cannot interleave.
    // Trades stamped then are far beyond ServiceOptions.MaxEventTimeAhead, which these
    // tests therefore turn off.
    private static readonly DateTimeOffset T0 = new(2100, 1, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Period = TimeSpan.FromMinutes(15);

    private const long NoWatermark = long.MinValue;

    private static StateStoreSize Store(string name, long entries) => new(name, "indexed", entries, 0, 0, 0, 0);

    /// <summary>Feeds one store one sample a minute (the period's fifteenth); returns what each did.</summary>
    private static List<int> Feed(StateGrowth growth, IEnumerable<long> entries, int startMinute = 0, string store = "named.joined.tableJoin.facts.w0") =>
        entries.Select((e, i) => growth.Observe(T0.AddMinutes(startMinute + i), [Store(store, e)], NoWatermark, Period)).ToList();

    [Fact]
    public void Growth_at_every_sample_for_the_period_is_flagged_from_where_it_began()
    {
        var growth = new StateGrowth();
        var results = Feed(growth, Enumerable.Range(1, 16).Select(i => i * 1000L));
        Assert.Equal(1, results[^1]);
        Assert.All(results.SkipLast(1), r => Assert.Equal(0, r));
        Assert.Equal(new StateGrowth.Flag("named.joined.tableJoin.facts.w0", T0, 1000, WatermarkStalled: false), growth.Current);
        Assert.Equal(T0, growth.Since);

        // Still growing: flagged once, not again. Gone quiet: still flagged, since what it
        // holds has not gone anywhere.
        Assert.Equal(0, growth.Observe(T0.AddMinutes(16), [Store("named.joined.tableJoin.facts.w0", 17_000)], NoWatermark, Period));
        Assert.Equal(0, growth.Observe(T0.AddMinutes(17), [Store("named.joined.tableJoin.facts.w0", 17_000)], NoWatermark, Period));
        Assert.Equal(T0, growth.Since);
        // It falls: the flag clears.
        Assert.Equal(-1, growth.Observe(T0.AddMinutes(18), [Store("named.joined.tableJoin.facts.w0", 16_500)], NoWatermark, Period));
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

    /// <summary>A store that grew for a period and then stopped for a whole one — a count
    /// per reference key, once every key has been seen — is no longer growing, though it
    /// never falls.</summary>
    [Fact]
    public void A_flag_clears_when_its_store_has_not_risen_for_a_whole_period()
    {
        var growth = new StateGrowth();
        Assert.Equal(1, Feed(growth, Enumerable.Range(1, 16).Select(i => (long)i))[^1]);
        // Fifteen level steps are a period: the sixteen samples then held show no rise.
        var level = Feed(growth, Enumerable.Repeat(16L, 15), startMinute: 16);
        Assert.All(level.SkipLast(1), r => Assert.Equal(0, r));
        Assert.Equal(-1, level[^1]);
        Assert.Null(growth.Current);
        // And it is flagged again only by another period of growth.
        var again = Feed(growth, Enumerable.Range(17, 15).Select(i => (long)i), startMinute: 31);
        Assert.Equal(11, again.IndexOf(1));                      // twelve rises among the fifteen steps held
        Assert.Equal(T0.AddMinutes(27), growth.Since);
    }

    /// <summary>The total hides a leak behind a store that churns: here the join's facts
    /// move by a thousand between samples and a count per record gains two hundred. Store
    /// by store, the count is flagged and named.</summary>
    [Fact]
    public void A_leak_beside_a_store_that_churns_is_flagged_by_its_own_store()
    {
        var growth = new StateGrowth();
        var results = new List<int>();
        var totals = new List<long>();
        for (var minute = 0; minute < 16; minute++)
        {
            var churn = 48_000L + (minute % 2 == 0 ? 1_000 : 0);
            var leak = 200L * (minute + 1);
            totals.Add(2 * churn + leak);
            results.Add(growth.Observe(T0.AddMinutes(minute),
            [
                Store("named.priced.tableJoin.facts.w0", churn), Store("named.priced.tableJoin.deadlines.w0", churn),
                Store("named.seen.aggregateLinear.state.w0", leak),
            ], T0.AddMinutes(minute).UtcTicks, Period));
        }
        Assert.Contains(Enumerable.Range(1, 15), i => totals[i] < totals[i - 1]);      // the total falls: the old rule saw nothing
        Assert.Equal(1, results[^1]);
        Assert.Equal(new StateGrowth.Flag("named.seen.aggregateLinear.state.w0", T0, 200, WatermarkStalled: false), growth.Current);
    }

    /// <summary>A store that closes on the watermark rises at every sample until the
    /// watermark has passed the first record it holds — an hour, with an hour open. It is
    /// not flagged while the watermark moves, however long that is; it is when the
    /// watermark has not moved in the whole period, and no longer once it does.</summary>
    [Fact]
    public void A_store_that_closes_on_the_watermark_is_flagged_only_while_the_watermark_stands_still()
    {
        static StateStoreSize[] At(long entries) =>
        [
            Store("input.trades.upsert.rows.w0", entries), Store("input.trades.upsert.deadlines.w0", entries),
            Store("named.priced.tableJoin.facts.w1", entries), Store("named.priced.tableJoin.deadlines.w1", entries),
        ];

        // Filling for two periods, the watermark a minute further at every sample.
        var filling = new StateGrowth();
        for (var minute = 0; minute < 32; minute++)
            Assert.Equal(0, filling.Observe(T0.AddMinutes(minute), At(1000 * (minute + 1)), T0.AddMinutes(minute).UtcTicks, Period));
        Assert.Null(filling.Current);

        // The same counts with a watermark that never moves: nothing that is held can close.
        var stalled = new StateGrowth();
        for (var minute = 0; minute < 15; minute++)
            Assert.Equal(0, stalled.Observe(T0.AddMinutes(minute), At(1000 * (minute + 1)), T0.UtcTicks, Period));
        Assert.Equal(1, stalled.Observe(T0.AddMinutes(15), At(16_000), T0.UtcTicks, Period));
        Assert.True(stalled.Current!.WatermarkStalled);
        Assert.Equal(T0, stalled.Current.Since);
        // And with no watermark at all — a source that has seen nothing — the same.
        var never = new StateGrowth();
        for (var minute = 0; minute < 15; minute++)
            never.Observe(T0.AddMinutes(minute), At(1000 * (minute + 1)), NoWatermark, Period);
        Assert.Equal(1, never.Observe(T0.AddMinutes(15), At(16_000), NoWatermark, Period));

        // The watermark moves: the flag goes, before anything has been evicted.
        Assert.Equal(-1, stalled.Observe(T0.AddMinutes(16), At(17_000), T0.AddMinutes(1).UtcTicks, Period));
        Assert.Null(stalled.Current);

        // A store with no deadlines beside it, in the same pipeline, is held to the plain rule.
        var mixed = new StateGrowth();
        for (var minute = 0; minute < 16; minute++)
            mixed.Observe(T0.AddMinutes(minute), [.. At(1000 * (minute + 1)), Store("named.seen.aggregateLinear.state.w0", minute + 1)],
                T0.AddMinutes(minute).UtcTicks, Period);
        Assert.Equal("named.seen.aggregateLinear.state.w0", mixed.Current?.Store);
        Assert.False(mixed.Current!.WatermarkStalled);
    }

    [Fact]
    public void A_stores_owner_is_its_name_less_the_purpose_and_the_worker()
    {
        Assert.Equal("named.priced.tableJoin", StateGrowth.OwnerOf("named.priced.tableJoin.facts.w0"));
        Assert.Equal("input.trades.upsert", StateGrowth.OwnerOf("input.trades.upsert.deadlines.w12"));
        Assert.Equal("odd", StateGrowth.OwnerOf("odd"));
        Assert.Equal("odd.wx", StateGrowth.OwnerOf("odd.wx"));
    }

    /// <summary>The rule on the counts a real definition of the reference shape gives: a
    /// keyed input and a join that forget final records, open for an hour, fed steadily
    /// for 32 minutes of the sampler's time. Every store rises at every sample — nothing
    /// is final yet — and nothing is flagged. With a count per trade beside them, that
    /// store is flagged and the others are not.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_definition_that_keeps_trades_open_for_an_hour_is_not_flagged_while_it_fills(bool countPerTrade)
    {
        await using var pipeline = await Testing.PipelineTestHarness.For<OpenTrades>()
            .Settings($$"""{ "openSeconds": 3600, "countPerTrade": {{(countPerTrade ? "true" : "false")}} }""")
            .Table(Acme.Contracts.Tables.Instruments, Instruments(10))
            .MaxEventTimeAhead(null)
            .StartAsync();
        var trades = pipeline.KeyedInput<long, TimedTrade>("trades");
        var growth = new StateGrowth();
        var flags = 0;
        long id = 0;
        for (var minute = 0; minute < 32; minute++)
        {
            for (var i = 0; i < 20; i++, id++)
                await trades.UpsertAsync(new TimedTrade(id, (int)(id % 10) + 1, "t", T0.AddMinutes(minute).AddSeconds(i * 3)));
            await pipeline.StepAsync();
            var size = pipeline.StateSize;
            Assert.Equal(20 * (minute + 1), size.Stores["input.trades.upsert.rows.w0"]);       // rising, every sample
            flags += Math.Max(0, growth.Observe(T0.AddMinutes(minute),
                [.. size.Stores.Select(s => Store(s.Key, s.Value))], pipeline.Watermark?.UtcTicks ?? NoWatermark, Period));
        }
        if (countPerTrade)
        {
            Assert.Equal(1, flags);
            Assert.Equal("named.seen.aggregateLinear.state.w0", growth.Current?.Store);
            Assert.False(growth.Current!.WatermarkStalled);
        }
        else
        {
            Assert.Equal(0, flags);
            Assert.Null(growth.Current);
        }
    }

    [Fact]
    public void Readings_closer_than_a_fifteenth_of_the_period_count_once_and_zero_turns_it_off()
    {
        var growth = new StateGrowth();
        StateStoreSize[] At(long entries) => [Store("s.groups.w0", entries)];
        // Once a second for fifteen minutes, growing all the while: sixteen samples a minute apart.
        for (var second = 0; second < 15 * 60; second++)
        {
            Assert.Equal(second % 60 == 0, growth.Due(T0.AddSeconds(second), Period));
            Assert.Equal(0, growth.Observe(T0.AddSeconds(second), At(second), NoWatermark, Period));
        }
        Assert.Equal(1, growth.Observe(T0.AddMinutes(15), At(15 * 60), NoWatermark, Period));

        Assert.True(growth.Due(T0.AddMinutes(16), TimeSpan.Zero));
        Assert.Equal(-1, growth.Observe(T0.AddMinutes(16), At(10_000), NoWatermark, TimeSpan.Zero));
        Assert.False(growth.Due(T0.AddMinutes(17), TimeSpan.Zero));
        for (var minute = 0; minute < 40; minute++)
            Assert.Equal(0, growth.Observe(T0.AddMinutes(17 + minute), At(20_000 + minute), NoWatermark, TimeSpan.Zero));
    }

    /// <summary>A trade that first arrives behind the watermark is dropped, and so is every
    /// trade behind one that was stamped far ahead. It is no dead letter and no fault: the
    /// pipeline counts it on its status and on the meter, and says so as an event — once,
    /// not per record.</summary>
    [Fact]
    public async Task A_pipeline_counts_the_records_it_drops_as_late_and_says_so_once()
    {
        var node = "lt" + Guid.NewGuid().ToString("N")[..8];
        var counted = new List<long>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == ServiceMeter.Name && instrument.Name == "dbsp.service.pipeline.late_records")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            foreach (var tag in tags)
                if (tag.Key == "node" && (string?)tag.Value == node)
                    lock (counted)
                        counted.Add(value);
        });
        listener.Start();

        await using var service = await StartAsync(nodeId: node, options: o => o.MaxEventTimeAhead = null);
        await service.DeclareInstrumentsAsync(Instruments(10));
        await service.DeployAsync(Spec("open", "open-trades", "lt-in", "lt-out", settings: """{ "openSeconds": 10 }"""));
        var input = service.Memory.KeyedLog<long, TimedTrade>("lt-in");
        var sink = service.Memory.UpsertSink<long, EnrichedTrade>("lt-out");

        // One trade, and one stamped a hundred seconds ahead of it: the watermark goes to 90.
        input.Upsert(new TimedTrade(1, 1, "t", T0));
        input.Upsert(new TimedTrade(2, 1, "t", T0.AddSeconds(100)));
        await Eventually(() => sink.Rows.Count == 2, "the two trades through");
        await Eventually(() => service.Pipelines.Get("open").Watermark == T0.AddSeconds(90), "the watermark frozen");
        Assert.Equal(0, service.Pipelines.Get("open").LateRecords);

        // Two trades never seen before, behind it, and one that is not.
        input.Upsert(new TimedTrade(3, 2, "t", T0.AddSeconds(50)));
        input.Upsert(new TimedTrade(4, 2, "t", T0.AddSeconds(60)));
        input.Upsert(new TimedTrade(5, 2, "t", T0.AddSeconds(95)));
        await Eventually(() => sink.Rows.Count == 3, "the trade that is not late through");
        await Eventually(() => service.Pipelines.Get("open").LateRecords == 2, "the late trades counted");
        Assert.Equal([1L, 2L, 5L], sink.Rows.Keys.Order());
        Assert.Equal(0, service.Pipelines.Get("open").DeadLetters);
        var said = Assert.Single(service.Node.RecentEvents(), e => e.Subject == "open" && e.Message.StartsWith("late records:"));
        Assert.StartsWith("late records: 2 dropped behind the watermark (2100-01-01 12:01:30Z), 2 since this node first ran it", said.Message);

        // More of them: counted, and not said again within the ten minutes.
        input.Upsert(new TimedTrade(6, 2, "t", T0.AddSeconds(70)));
        await Eventually(() => service.Pipelines.Get("open").LateRecords == 3, "the third late trade counted");
        Assert.Single(service.Node.RecentEvents(), e => e.Subject == "open" && e.Message.StartsWith("late records:"));

        service.Node.RefreshNodeGauges();
        listener.RecordObservableInstruments();
        lock (counted)
            Assert.Contains(3L, counted);
    }

    /// <summary>
    /// Round five, K3. A trade stamped far in the future used to move the watermark to
    /// its time: every trade after it late. On a node it is a record in error — more than
    /// <see cref="ServiceOptions.MaxEventTimeAhead"/> ahead of the node's clock — and the
    /// pipeline's error policy deals with it; a spec may set a bound of its own.
    /// </summary>
    [Fact]
    public async Task A_trade_stamped_far_ahead_is_a_dead_letter_on_a_node_and_a_spec_may_allow_it()
    {
        var node = "fa" + Guid.NewGuid().ToString("N")[..8];
        await using var service = await StartAsync(nodeId: node);
        Assert.Equal(TimeSpan.FromDays(1), service.Node.Options.MaxEventTimeAhead);
        await service.DeclareInstrumentsAsync(Instruments(10));
        var now = DateTimeOffset.UtcNow;

        var spec = Spec("bounded", "open-trades", "fa-in", "fa-out", settings: """{ "openSeconds": 10 }""");
        await service.DeployAsync(spec with { Circuit = spec.Circuit with { ErrorPolicy = RecordErrorPolicy.DeadLetter } });
        var input = service.Memory.KeyedLog<long, TimedTrade>("fa-in");
        var sink = service.Memory.UpsertSink<long, EnrichedTrade>("fa-out");
        input.Upsert(new TimedTrade(1, 1, "t", now.AddSeconds(-60)));
        input.Upsert(new TimedTrade(2, 1, "t", T0));                    // the year 2100
        input.Upsert(new TimedTrade(3, 1, "t", now.AddSeconds(-30)));
        await Eventually(() => sink.Rows.Count == 2 && service.Pipelines.Get("bounded").DeadLetters == 1, "two trades through and one refused");
        Assert.Equal([1L, 3L], sink.Rows.Keys.Order());
        Assert.Equal(0, service.Pipelines.Get("bounded").LateRecords);
        Assert.True(service.Pipelines.Get("bounded").Watermark < now, "the watermark went to the refused trade's time");

        // The same feed into a pipeline whose spec says that its time may be that far ahead.
        var wide = Spec("wide", "open-trades", "fa-in", "fa-wide", settings: """{ "openSeconds": 10 }""");
        await service.DeployAsync(wide with { Circuit = wide.Circuit with { MaxEventTimeAhead = TimeSpan.FromDays(365 * 100) } });
        var all = service.Memory.UpsertSink<long, EnrichedTrade>("fa-wide");
        await Eventually(() => all.Rows.ContainsKey(2), "the trade of 2100 through the pipeline that allows it");
        await Eventually(() => service.Pipelines.Get("wide").Watermark > now, "its watermark at that trade");
    }

    [Fact]
    public async Task A_node_flags_the_pipeline_that_keeps_every_record_and_not_the_one_that_counts()
    {
        var node = "sg" + Guid.NewGuid().ToString("N")[..8];
        var gauges = new List<(string Pipeline, long Value)>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == ServiceMeter.Name && instrument.Name == "dbsp.service.pipeline.state_growing")
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

        // Off until the first reading below is about to be taken: the node's own sampler
        // reads once a second at the real time, and a reading of its own before the first
        // one here would be the run's first sample.
        await using var service = await StartAsync(nodeId: node, options: o => (o.StateGrowthAlarmAfter, o.MaxEventTimeAhead) = (TimeSpan.Zero, null));
        await service.DeclareInstrumentsAsync(Instruments(10));
        // "enrich" joins every trade to its instrument and keeps it: one fact per trade, for
        // ever. "counts" keeps a count per trader: five entries however many trades.
        await service.DeployAsync(Spec("keeps-all", "enrich", "g-in", "g-out"));
        await service.DeployAsync(Spec("counts", "counts", "c-in", "c-out"));
        // "fills" keeps trades open for an hour and forgets them then: every store of it
        // rises at every reading here, on a watermark that moves. "leaks" forgets them
        // after thirty seconds and also keeps a count per trade, which nothing closes: its
        // total falls at two readings in three, and its count never does.
        await service.DeployAsync(Spec("fills", "open-trades", "f-in", "f-out", settings: """{ "openSeconds": 3600 }"""));
        await service.DeployAsync(Spec("leaks", "open-trades", "l-in", "l-out", settings: """{ "openSeconds": 30, "countPerTrade": true }""") with
        {
            Sinks = new Dictionary<string, System.Text.Json.JsonElement> { ["out"] = MemoryBinding("l-out"), ["seen"] = MemoryBinding("l-seen") },
        });
        var enriched = service.Memory.UpsertSink<long, EnrichedTrade>("g-out");
        const string Seen = "named.seen.aggregateLinear.state.w0";
        long StoreOf(string pipeline, string store) =>
            service.Pipelines.Get(pipeline).StateSize?.Stores.FirstOrDefault(s => s.Name == store)?.Entries ?? -1;

        // Sixteen minutes of the sampler's time, a hundred trades between readings.
        long timed = 0;
        var previous = 0;
        var leakTotals = new List<long>();
        for (var minute = 0; minute <= 16; minute++)
        {
            service.Memory.Log<Trade>("g-in").AppendRange(Trades(minute * 100, 100));
            service.Memory.Log<Trade>("c-in").AppendRange(Trades(minute * 100, 100));
            // A large batch and two small ones, in turn: what "leaks" holds open swings by
            // far more than its count gains.
            var batch = minute % 3 == 0 ? 200 : 20;
            for (var i = 0; i < batch; i++, timed++)
            {
                var trade = new TimedTrade(timed, (int)(timed % 10) + 1, "t", T0.AddMinutes(minute).AddMilliseconds(i));
                service.Memory.KeyedLog<long, TimedTrade>("f-in").Upsert(trade);
                service.Memory.KeyedLog<long, TimedTrade>("l-in").Upsert(trade);
            }
            await Eventually(() => enriched.Rows.Count == (minute + 1) * 100, "the trades through");
            await Eventually(() => service.Pipelines.Get("keeps-all").StateSize?.Entries == (minute + 1) * 100, "the state counted");
            await Eventually(() => StoreOf("fills", "input.trades.upsert.rows.w0") == timed, "the open trades counted");
            await Eventually(() => StoreOf("leaks", Seen) == timed, "the leak counted");
            // What was final when this batch arrived has gone: this batch is held, and the
            // one before it until the next step (a quiet pipeline forgets at its next record).
            await Eventually(() => StoreOf("leaks", "input.trades.upsert.rows.w0") <= batch + previous, "the final trades forgotten");
            previous = batch;
            leakTotals.Add(service.Pipelines.Get("leaks").StateSize!.Entries);
            service.Node.Options.StateGrowthAlarmAfter = Period;
            service.Node.Metrics.Sample(T0.AddMinutes(minute));
        }

        Assert.Equal(T0, service.Pipelines.Get("keeps-all").StateGrowingSince);
        Assert.Null(service.Pipelines.Get("counts").StateGrowingSince);
        // Still filling, on a watermark that moves: not flagged, though no store of it ever fell.
        Assert.Null(service.Pipelines.Get("fills").StateGrowth);
        // The total fell again and again, and the store that leaks is flagged all the same.
        Assert.True(Enumerable.Range(1, leakTotals.Count - 1).Count(i => leakTotals[i] < leakTotals[i - 1]) >= 4, string.Join(",", leakTotals));
        Assert.Equal(new PipelineStateGrowth(Seen, T0, WatermarkStalled: false), service.Pipelines.Get("leaks").StateGrowth);
        Assert.Contains(service.Node.RecentEvents(), e => e.Subject == "leaks" && e.Message.StartsWith($"state growing: store {Seen} from 200 to "));
        Assert.DoesNotContain(service.Node.RecentEvents(), e => e.Subject == "fills" && e.Message.StartsWith("state growing"));
        Assert.Contains(service.Node.RecentEvents(), e => e.Subject == "keeps-all"
            && e.Message.StartsWith("state growing: store named.joined.tableJoin.facts.w0 from 100 to 1,600 entries in 15 min without falling once"));
        Assert.Equal(new PipelineStateGrowth("named.joined.tableJoin.facts.w0", T0, WatermarkStalled: false), service.Pipelines.Get("keeps-all").StateGrowth);
        Assert.Null(service.Pipelines.Get("counts").StateGrowth);
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
