using Enrichment.Contracts;
using Incremental.Service.Testing;

namespace Enrichment.Pipelines.Tests;

public class DeskExposureTests
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 5, 9, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Executions_are_summed_per_desk_and_minute_under_the_desk_the_trader_sat_on_then()
    {
        await using var pipeline = await PipelineTestHarness.For<DeskExposure>()
            .Settings("""{ "windowSeconds": 60, "latenessSeconds": 5 }""")
            .Table(Reference.TraderDesks, [new TraderDesk(7, "Ada A.", 3, "Macro", "EMEA")])
            .StartAsync();
        var executions = pipeline.Input<Trade>("executions");
        await executions.InsertAsync(new Trade(1, 1, 7, "CP0001", 100, 10m, T0.AddSeconds(5)));
        await executions.InsertAsync(new Trade(2, 1, 7, "CP0001", 50, 10m, T0.AddSeconds(50)));
        await executions.InsertAsync(new Trade(3, 1, 7, "CP0001", 10, 10m, T0.AddSeconds(65)));
        await pipeline.StepAsync();

        // Ada moves desk; what she did before stays where it was done.
        await pipeline.Table(Reference.TraderDesks).UpsertAsync(new TraderDesk(7, "Ada A.", 4, "Rates", "APAC"));
        await executions.InsertAsync(new Trade(4, 1, 7, "CP0001", 1, 10m, T0.AddSeconds(70)));
        await pipeline.StepAsync();

        var exposure = pipeline.UpsertSink<string, Exposure>("exposure").Rows.Values.ToDictionary(e => (e.Group, e.WindowStart), e => e.Notional);
        Assert.Equal(1500m, exposure[("Macro", T0)]);
        Assert.Equal(100m, exposure[("Macro", T0.AddMinutes(1))]);
        Assert.Equal(10m, exposure[("Rates", T0.AddMinutes(1))]);
    }
}
