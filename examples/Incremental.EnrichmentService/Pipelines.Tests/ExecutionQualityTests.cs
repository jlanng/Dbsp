using Enrichment.Contracts;
using Incremental.Service.Testing;

namespace Enrichment.Pipelines.Tests;

public class ExecutionQualityTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 5, 9, 0, 0, TimeSpan.Zero);
    private static readonly Instrument Abc = new(1, "ABC", "GB0000000001", "Equity", 1m);
    private static readonly Instrument Gilt = new(2, "GILT", "GB0000000002", "Govt Bond", 1m);

    private static Trade Execution(long id, int instrument, double second, decimal price) => new(id, instrument, 7, "CP0001", 100, price, T0.AddSeconds(second));

    // One-minute quotes, two-minute windows starting every minute.
    private static Task<PipelineTestHarness> StartAsync() =>
        PipelineTestHarness.For<ExecutionQuality>()
            .Settings("""{ "quoteSeconds": 60, "windowSeconds": 120, "hopSeconds": 60, "latenessSeconds": 5 }""")
            .Table(Reference.Instruments, [Abc, Gilt])
            .StartAsync();

    private static Dictionary<(string AssetClass, DateTimeOffset Start), (long Priced, long Unpriced, decimal Bps)> Windows(PipelineTestHarness pipeline) =>
        pipeline.UpsertSink<string, Quality>("quality").Rows.Values
            .ToDictionary(q => (q.AssetClass, q.WindowStart), q => (q.Priced, q.Unpriced, q.SlippageBps));

    // <snippet:test-two-inputs>
    [Fact]
    public async Task An_execution_meets_the_quote_of_its_interval_whichever_of_the_two_arrives_first()
    {
        await using var pipeline = await StartAsync();
        var executions = pipeline.Input<Trade>("executions");
        var quotes = pipeline.Input<Quote>("quotes");

        // The quote first, then the execution: 101 against a mid of 100 is +100 bp.
        await quotes.InsertAsync(new Quote(1, T0, 100m));
        await executions.InsertAsync(Execution(1, instrument: 1, second: 10, price: 101m));
        // The execution first: no quote yet for the gilt, so it is counted as unpriced.
        await executions.InsertAsync(Execution(2, instrument: 2, second: 20, price: 99m));
        await pipeline.StepAsync();

        // Each execution is in the two windows that cover it.
        Assert.Equal((1, 0, 100m), Windows(pipeline)[("Equity", T0)]);
        Assert.Equal((1, 0, 100m), Windows(pipeline)[("Equity", T0.AddMinutes(-1))]);
        Assert.Equal((0, 1, 0m), Windows(pipeline)[("Govt Bond", T0)]);

        // The gilt's quote arrives: the unpriced row is taken back, the priced one counted.
        await quotes.InsertAsync(new Quote(2, T0, 100m));
        await pipeline.StepAsync();
        Assert.Equal((1, 0, -100m), Windows(pipeline)[("Govt Bond", T0)]);
    }

    [Fact]
    public async Task A_quote_prices_only_its_own_interval_and_a_closed_interval_is_forgotten()
    {
        await using var pipeline = await StartAsync();
        var executions = pipeline.Input<Trade>("executions");
        var quotes = pipeline.Input<Quote>("quotes");

        await quotes.InsertAsync(new Quote(1, T0, 100m));
        await executions.InsertAsync(Execution(1, instrument: 1, second: 70, price: 102m));        // the next interval: no quote there
        await pipeline.StepAsync();
        Assert.Equal((0, 1, 0m), Windows(pipeline)[("Equity", T0.AddMinutes(1))]);

        // Both inputs move on by ten minutes, and the watermark — the lower of the two —
        // passes the end of that interval: the join has let go of it, and the execution's
        // result is final. A quote for it now is late, and dropped.
        await quotes.InsertAsync(new Quote(1, T0.AddMinutes(10), 100m));
        await executions.InsertAsync(Execution(2, instrument: 1, second: 600, price: 100m));
        await pipeline.StepAsync();
        Assert.True(pipeline.Watermark > T0.AddMinutes(2));

        await quotes.InsertAsync(new Quote(1, T0.AddMinutes(1), 100m));
        await pipeline.StepAsync();
        Assert.Equal((0, 1, 0m), Windows(pipeline)[("Equity", T0.AddMinutes(1))]);
    }
    // </snippet:test-two-inputs>

    [Fact]
    public async Task One_input_alone_does_not_move_the_watermark()
    {
        await using var pipeline = await StartAsync();
        await pipeline.Input<Trade>("executions").InsertAsync(Execution(1, instrument: 1, second: 3600, price: 100m));
        await pipeline.StepAsync();
        Assert.Null(pipeline.Watermark);                    // the quotes have said nothing yet

        // So a quote an hour "behind" the executions is not late.
        await pipeline.Input<Quote>("quotes").InsertAsync(new Quote(1, T0, 100m));
        await pipeline.Input<Trade>("executions").InsertAsync(Execution(2, instrument: 1, second: 30, price: 100.5m));
        await pipeline.StepAsync();
        Assert.Equal((1, 0, 50m), Windows(pipeline)[("Equity", T0)]);
    }
}
