using Incremental.Storage;

namespace Incremental.Tests;

/// <summary>
/// A circuit whose <c>Start()</c> threw is finished: it runs no step, commits nothing and
/// writes no state, whatever is in its inputs and however it is disposed. Every wait is
/// fenced with a timeout.
/// </summary>
public class RefusedStartTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private sealed record Order(int Id, string Region);

    private static (CircuitBuilder Builder, InputStream<Order> Orders) Define(
        IStorageProvider storage, int workers, bool autoStep, bool changed)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = autoStep,
            Workers = workers,
            Storage = storage,
        });
        var orders = builder.Input<Order>("orders");
        // The changed topology has one more stateful operator ahead of the count, so its
        // state layout is not the checkpoint's.
        (changed ? orders.Distinct() : orders).GroupBy(o => o.Region).Count().ToOutput();
        return (builder, orders);
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(1, true)]
    [InlineData(2, false)]
    [InlineData(2, true)]
    public async Task A_circuit_refused_at_start_runs_no_step_and_commits_nothing_when_disposed(int workers, bool autoStep)
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var (firstBuilder, firstOrders) = Define(storage, workers, autoStep: false, changed: false);
        await using (var first = firstBuilder.Build())
        {
            first.Start();
            for (var n = 0; n < 12; n++)
                await first.Handle(firstOrders).InsertAsync(new Order(n, "emea"), n);
            await first.StepAsync().WaitAsync(Timeout);
            Assert.Equal(11, first.CommittedOffset("orders"));
        }

        // The redeploy that does not match: two records are waiting when it is refused.
        var (changedBuilder, changedOrders) = Define(storage, workers, autoStep, changed: true);
        var refused = changedBuilder.Build();
        await refused.Handle(changedOrders).InsertAsync(new Order(12, "emea"), 12);
        await refused.Handle(changedOrders).InsertAsync(new Order(13, "emea"), 13);
        var error = Assert.Throws<CircuitStateMismatchException>(refused.Start);
        Assert.Contains("does not match the checkpoint", error.Message);

        // It cannot be stepped or started again, and it disposes without running anything.
        if (!autoStep)
            Assert.Contains("refused to start",
                (await Assert.ThrowsAsync<InvalidOperationException>(() => refused.StepAsync())).Message);
        Assert.Contains("refused to start", Assert.Throws<InvalidOperationException>(refused.Start).Message);
        await refused.DisposeAsync().AsTask().WaitAsync(Timeout);
        Assert.True(refused.Completion.IsCompletedSuccessfully);

        // The original topology finds the checkpoint where the first run left it.
        var (againBuilder, againOrders) = Define(storage, workers, autoStep: false, changed: false);
        await using var again = againBuilder.Build();
        again.Start();
        Assert.Equal(11, again.CommittedOffset("orders"));
        _ = againOrders;
    }

    [Fact]
    public async Task A_circuit_refused_on_a_follower_leaves_the_followed_state_alone()
    {
        static (CircuitBuilder Builder, InputStream<Order> Orders) OnPipeline(ChangelogStorage storage) =>
            Define(storage.Pipeline("p"), workers: 1, autoStep: false, changed: false);

        var log = new InMemoryStateChangelog(partitions: 1);
        await using var leader = await ChangelogStorage.OpenAsync(log);
        var (leaderBuilder, leaderOrders) = OnPipeline(leader);
        await using var active = leaderBuilder.Build();
        active.Start();
        var orders = active.Handle(leaderOrders);
        await orders.InsertAsync(new Order(0, "emea"), 0);
        await orders.InsertAsync(new Order(1, "emea"), 1);
        await active.StepAsync().WaitAsync(Timeout);

        await using var standby = ChangelogStorage.Follow(log);
        await standby.WhenCaughtUpAsync().WaitAsync(Timeout);

        // Started too early, with a record already in its input, and disposed as cleanup.
        var (earlyBuilder, earlyOrders) = OnPipeline(standby);
        var early = earlyBuilder.Build();
        await early.Handle(earlyOrders).InsertAsync(new Order(2, "emea"), 2);
        Assert.Contains("follower", Assert.Throws<InvalidOperationException>(early.Start).Message);
        await early.DisposeAsync().AsTask().WaitAsync(Timeout);
        Assert.Null(early.Fault);
        Assert.Equal(0, standby.Pipeline("p").UncommittedEntries);

        // The leader moves on; the standby follows, is promoted, and holds the same state.
        for (var n = 2; n < 5; n++)
            await orders.InsertAsync(new Order(n, "emea"), n);
        await active.StepAsync().WaitAsync(Timeout);
        var expected = leader.Pipeline("p").DescribeState();

        await standby.PromoteAsync().WaitAsync(Timeout);
        var (promotedBuilder, _) = OnPipeline(standby);
        await using var promoted = promotedBuilder.Build();
        promoted.Start();
        Assert.Equal(4, promoted.CommittedOffset("orders"));
        Assert.Equal(expected, standby.Pipeline("p").DescribeState());
    }
}
