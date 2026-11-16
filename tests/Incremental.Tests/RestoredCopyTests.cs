using Incremental.Storage;

namespace Incremental.Tests;

/// <summary>A circuit that starts on restored state and has nothing to read commits once,
/// so that the provider's copy of that state is not kept for as long as it is idle.</summary>
public class RestoredCopyTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    private sealed record Trade(int Id, string Desk, long Quantity);

    private static (Circuit Circuit, InputHandle<Trade> Trades) Build(IStorageProvider storage, bool autoStep = true)
    {
        var builder = new CircuitBuilder(new CircuitOptions { Name = "p", AutoStep = autoStep, Storage = storage });
        var trades = builder.Input<Trade>("trades");
        var totals = trades.GroupBy(t => t.Desk).Sum(t => t.Quantity).ToOutput();
        var circuit = builder.Build();
        _ = Task.Run(async () =>
        {
            await foreach (var _ in circuit.Handle(totals).Reader.ReadAllAsync())
            {
            }
        });
        return (circuit, circuit.Handle(trades));
    }

    private static async Task UntilAsync(Func<bool> condition, string what)
    {
        var clock = System.Diagnostics.Stopwatch.StartNew();
        while (!condition())
        {
            Assert.True(clock.Elapsed < Timeout, $"timed out waiting for {what}");
            await Task.Delay(20);
        }
    }

    private static async Task<InMemoryStateChangelog> LogWithStateAsync(int trades)
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var (circuit, input) = Build(storage.Pipeline("p"));
        circuit.Start();
        for (var i = 0; i < trades; i++)
            await input.InsertAsync(new Trade(i, "desk-" + i % 50, i));
        await UntilAsync(() => storage.Pipeline("p").CommittedOffsets.GetValueOrDefault("trades") == trades - 1, "the first run to commit");
        await circuit.DisposeAsync();
        return log;
    }

    [Fact]
    public async Task An_idle_circuit_started_on_restored_state_commits_once_and_the_copy_goes()
    {
        var log = await LogWithStateAsync(2000);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var before = provider.DescribeState();
        var offsets = provider.CommittedOffsets;
        var sequence = provider.CommitSequence;
        await using var follower = ChangelogStorage.Follow(log);

        var (circuit, input) = Build(provider);
        Assert.True(provider.HoldsRestoredCopy);
        Assert.True(provider.EstimateSize().EntriesHeldTwice > 0);
        circuit.Start();

        // Not at once: a circuit with something to read commits by itself, and that is
        // the commit that counts.
        await Task.Delay(Circuit.RestoredCopyGrace / 4);
        Assert.True(provider.HoldsRestoredCopy);
        Assert.Equal(sequence, provider.CommitSequence);

        await UntilAsync(() => !provider.HoldsRestoredCopy, "the idle circuit to commit");
        Assert.Equal(sequence + 1, provider.CommitSequence);
        Assert.Equal(0, provider.EstimateSize().EntriesHeldTwice);

        // The checkpoint as it was recovered: the same state, the same positions.
        Assert.Equal(before, provider.DescribeState());
        Assert.Equal(offsets.OrderBy(o => o.Key), provider.CommittedOffsets.OrderBy(o => o.Key));

        // Once, not every two seconds.
        await Task.Delay(Circuit.RestoredCopyGrace + TimeSpan.FromMilliseconds(500));
        Assert.Equal(sequence + 1, provider.CommitSequence);

        // And it still is the circuit it was: the next record lands on the restored totals.
        await input.InsertAsync(new Trade(5000, "desk-1", 7));
        await UntilAsync(() => provider.CommittedOffsets.GetValueOrDefault("trades") == 2000, "the next record to commit");
        await follower.WhenCaughtUpAsync().WaitAsync(Timeout);
        Assert.Equal(provider.DescribeState(), follower.Pipeline("p").DescribeState());
        await circuit.DisposeAsync();
    }

    [Fact]
    public async Task A_circuit_with_something_to_read_commits_by_itself_and_no_extra_checkpoint_is_written()
    {
        var log = await LogWithStateAsync(500);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var sequence = provider.CommitSequence;
        var (circuit, input) = Build(provider);
        circuit.Start();
        await input.InsertAsync(new Trade(900, "desk-2", 1));
        await UntilAsync(() => provider.CommittedOffsets.GetValueOrDefault("trades") == 500, "the record to commit");
        Assert.False(provider.HoldsRestoredCopy);
        var after = provider.CommitSequence;
        Assert.InRange(after, sequence + 1, sequence + 2);
        await Task.Delay(Circuit.RestoredCopyGrace + TimeSpan.FromMilliseconds(500));
        Assert.Equal(after, provider.CommitSequence);
        await circuit.DisposeAsync();
    }

    [Fact]
    public async Task A_circuit_on_a_new_state_or_on_a_provider_that_keeps_no_copy_commits_nothing_while_idle()
    {
        var log = new InMemoryStateChangelog(partitions: 1);
        await using var storage = await ChangelogStorage.OpenAsync(log);
        var provider = storage.Pipeline("p");
        var (circuit, _) = Build(provider);
        circuit.Start();
        Assert.False(provider.HoldsRestoredCopy);
        await Task.Delay(Circuit.RestoredCopyGrace + TimeSpan.FromMilliseconds(500));
        Assert.Equal(0, provider.CommitSequence);
        await circuit.DisposeAsync();

        var checkpoints = new InMemoryCheckpointStorageProvider();
        Assert.False(((ICheckpointStorageProvider)checkpoints).HoldsRestoredCopy);
    }
}
