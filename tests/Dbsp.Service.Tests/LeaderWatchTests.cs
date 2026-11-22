using Acme.Contracts;
using Dbsp.Kafka;
using static Dbsp.Service.Tests.SupervisorTests;
using static Dbsp.Service.Tests.TestService;

namespace Dbsp.Service.Tests;

/// <summary>
/// The one condition under which a node gives leadership up by itself: it can no longer
/// write the catalog. Heartbeats come from the backend's log writer, not from the catalog
/// or the pipelines, so a leader in that state still looks alive to its standby.
/// </summary>
public class LeaderWatchTests
{
    /// <summary>Faults the catalog's writer the way a deterministic bad row would: an
    /// index whose selector throws on one key, then a write of that key.</summary>
    private static async Task PoisonCatalogAsync(TestService service, string id)
    {
        service.Node.Catalog.Table!.AddIndex(
            "poison-" + id, e => e.Key == "pipeline/" + id ? throw new InvalidOperationException("poisoned catalog row") : e.Key);
        Assert.NotNull(await Record.ExceptionAsync(() => service.DeployAsync(Spec(id, "counts", "trades", id + "-out"))));
    }

    private static bool Published(TestService service, string kind, string fragment) =>
        service.Node.RecentEvents().Any(e => e.Kind == kind && e.Message.Contains(fragment, StringComparison.Ordinal));

    [Fact]
    public async Task A_faulted_catalog_writer_is_reopened_once_and_then_the_node_steps_down_to_its_ready_standby()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        var trades = Trades(0, 100);
        memory.Log<Trade>("trades").AppendRange(trades);
        await using var a = await StartAsync(cluster, "a", memory);
        await a.DeployAsync(Spec("steady", "counts", "trades", "steady-out"));
        await a.Running("steady");
        await using var b = await StartAsync(cluster, "b", memory);
        await Eventually(() => b.Node.Status.Standby is { TakeoverWouldBeWarm: true }, "the standby to be warm");

        // The first fault is mended in place: the catalog is opened from its log again, the
        // node leads on, its pipelines never notice, and the catalog takes writes again.
        await PoisonCatalogAsync(a, "boom");
        await Eventually(() => Published(a, "catalog", "reopened from its log"), "the catalog to be reopened");
        Assert.True(a.Node.IsLeader);
        Assert.Equal(PipelineState.Running, a.Pipelines.Get("steady").State);
        Assert.Equal(0, a.Pipelines.Get("steady").Restarts);
        await a.DeployAsync(Spec("after", "counts", "trades", "after-out"));
        await a.Running("after");
        Assert.Null(a.Node.Catalog.Get("pipeline/boom"));
        await Eventually(() => b.Pipelines.Pipelines.Any(p => p.Id == "after"), "the standby to follow the reopened catalog");

        // A second fault within the minute is not mended again: the node cannot do its
        // job, a ready standby answers, so it steps down and the standby leads.
        await PoisonCatalogAsync(a, "boom-again");
        await Eventually(() => b.Node.Status.Leadership.Role == NodeRole.Leader, "the standby to lead after the hand-over");
        await Eventually(() => a.Node.Status.Leadership.Role == NodeRole.Resigned, "the old leader to have resigned");
        Assert.True(Published(a, "node", "cannot lead (the catalog cannot be written); stepping down to 'b'"));
        await b.Running("steady");
        await b.Running("after");
        await b.DeployAsync(Spec("on-b", "counts", "trades", "on-b-out"));
        var sink = memory.UpsertSink<string, long>("steady-out");
        Assert.True(Counts.Model(trades).All(e => sink.Rows.TryGetValue(e.Key, out var n) && n == e.Value));
    }

    [Fact]
    public async Task With_no_standby_a_node_that_cannot_write_the_catalog_says_so_and_asks_to_exit()
    {
        await using var a = await StartAsync();
        string? why = null;
        a.Node.CannotLead += reason => why = reason;

        await PoisonCatalogAsync(a, "boom");
        await Eventually(() => Published(a, "catalog", "reopened from its log"), "the catalog to be reopened");
        Assert.Null(why);

        await PoisonCatalogAsync(a, "boom-again");
        await Eventually(() => why is not null, "the node to find it cannot lead");
        Assert.Equal("the catalog cannot be written", why);

        // Nothing to hand over to: it does not resign into the void.
        Assert.Equal(NodeRole.Leader, a.Node.Status.Leadership.Role);
        Assert.True(Published(a, "node", "no ready standby answers"));
    }
}
