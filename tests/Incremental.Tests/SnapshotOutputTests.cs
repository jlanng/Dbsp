using Incremental.Storage;

namespace Incremental.Tests;

/// <summary>
/// Snapshot outputs (<c>Output(name, snapshot: true)</c>): the integrated state of the view
/// readable via <see cref="OutputHandle{T}.ReadSnapshotAsync"/>, consistent with the delta
/// stream and recovered from a checkpoint after a restart — plus the fault-domain rule that
/// a failed publish faults the circuit instead of silently losing a step's deltas.
/// </summary>
public class SnapshotOutputTests
{
    [Fact]
    public async Task Snapshot_matches_integrated_deltas_including_retractions()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        builder.Input<string>("words").Output("out", snapshot: true);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<string>("words");
        var output = circuit.Output<string>("out");

        await input.InsertAsync("a");
        await input.InsertAsync("b");
        await circuit.StepAsync();
        await input.RemoveAsync("a");
        await input.InsertAsync("b");
        await circuit.StepAsync();

        var snapshot = await output.ReadSnapshotAsync();
        Assert.Equal(0, snapshot.WeightOf("a"));
        Assert.Equal(2, snapshot.WeightOf("b"));
        Assert.Equal(1, snapshot.DistinctCount);
    }

    [Fact]
    public async Task Snapshot_merges_worker_shards()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = 4 });
        builder.Input<int>("numbers")
            .IndexBy(n => n % 3)
            .Count()
            .Output("counts", snapshot: true);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<int>("numbers");
        var output = circuit.Output<KeyedValue<int, long>>("counts");

        for (var n = 0; n < 10; n++)
            await input.InsertAsync(n);
        await circuit.StepAsync();

        var snapshot = await output.ReadSnapshotAsync();
        Assert.Equal(1, snapshot.WeightOf(new KeyedValue<int, long>(0, 4)));
        Assert.Equal(1, snapshot.WeightOf(new KeyedValue<int, long>(1, 3)));
        Assert.Equal(1, snapshot.WeightOf(new KeyedValue<int, long>(2, 3)));
        Assert.Equal(3, snapshot.DistinctCount);
    }

    [Fact]
    public async Task Snapshot_discards_buffered_deltas_it_subsumes()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        builder.Input<int>("numbers").Output("out", snapshot: true);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<int>("numbers");
        var output = circuit.Output<int>("out");

        await input.InsertAsync(1);
        await circuit.StepAsync(); // delta sits unread in the output channel

        var snapshot = await output.ReadSnapshotAsync();
        Assert.Equal(1, snapshot.WeightOf(1));

        // The buffered delta was subsumed by the snapshot and discarded: only deltas from
        // later steps arrive on the channel now.
        Assert.False(output.Reader.TryRead(out _));
        await input.InsertAsync(2);
        await circuit.StepAsync();
        var next = await output.Reader.ReadAsync();
        Assert.Equal(1, next.WeightOf(2));
        Assert.Equal(1, next.DistinctCount);
    }

    [Fact]
    public async Task Snapshot_read_unblocks_a_step_stalled_on_a_full_output_channel()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, OutputChannelCapacity = 1 });
        builder.Input<int>("numbers").Output("out", snapshot: true);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<int>("numbers");
        var output = circuit.Output<int>("out");

        await input.InsertAsync(1);
        await circuit.StepAsync(); // fills the only channel slot

        await input.InsertAsync(2);
        var stalled = circuit.StepAsync(); // blocks publishing, holding the step gate
        Assert.False(stalled.IsCompleted);

        // The read waits for the in-flight step, discarding the buffered deltas it subsumes
        // — which is exactly what lets the stalled publish (and so the step) complete.
        var snapshot = await output.ReadSnapshotAsync();
        await stalled;
        Assert.Equal(1, snapshot.WeightOf(1));
        Assert.Equal(1, snapshot.WeightOf(2));
    }

    [Fact]
    public async Task Snapshot_recovers_from_checkpoint_after_crash()
    {
        var storage = new InMemoryCheckpointStorageProvider();

        static CircuitBuilder Pipeline(IStorageProvider storage)
        {
            var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
            builder.Input<(int Id, string Region)>("orders")
                .IndexBy(o => o.Region)
                .Count()
                .Output("counts", snapshot: true);
            return builder;
        }

        // First run: steps commit (interval 1), but nobody ever reads the published deltas
        // — the crash loses them for good. Abandon without disposing, like a real crash.
        var crashed = Pipeline(storage).Build();
        crashed.Start();
        var input = crashed.Input<(int Id, string Region)>("orders");
        await input.InsertAsync((1, "emea"), offset: 0);
        await input.InsertAsync((2, "emea"), offset: 1);
        await input.InsertAsync((3, "apac"), offset: 2);
        await crashed.StepAsync();

        // Restart against the same provider: the snapshot is the consumer's bootstrap —
        // it reflects the committed checkpoint even though the deltas were never consumed.
        await using var resumed = Pipeline(storage).Build();
        resumed.Start();
        var snapshot = await resumed.Output<KeyedValue<string, long>>("counts").ReadSnapshotAsync();
        Assert.Equal(1, snapshot.WeightOf(new KeyedValue<string, long>("emea", 2)));
        Assert.Equal(1, snapshot.WeightOf(new KeyedValue<string, long>("apac", 1)));
        Assert.Equal(2, snapshot.DistinctCount);
    }

    [Fact]
    public async Task Reading_a_snapshot_of_a_plain_output_throws()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        builder.Input<int>("numbers").Output("out");

        await using var circuit = builder.Build();
        circuit.Start();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => circuit.Output<int>("out").ReadSnapshotAsync());
        Assert.Contains("snapshot: true", ex.Message);
    }

    [Fact]
    public async Task Failed_publish_faults_the_circuit_instead_of_losing_deltas()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, OutputChannelCapacity = 1 });
        builder.Input<int>("numbers").Output("out");

        var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Input<int>("numbers");

        await input.InsertAsync(1);
        await circuit.StepAsync(); // fills the only channel slot

        // The next step's publish blocks on the full channel; cancelling it must not leave
        // a healthy circuit that silently dropped the step's deltas — state has already
        // advanced, so the circuit faults instead.
        await input.InsertAsync(2);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => circuit.StepAsync(cancellation.Token));

        Assert.NotNull(circuit.Fault);
        await Assert.ThrowsAsync<CircuitFaultedException>(async () =>
        {
            // The channel may briefly accept buffered writes; keep sending until the fault
            // completes the input channel and surfaces.
            for (var i = 0; i < 1000; i++)
                await input.InsertAsync(i);
        });
        await circuit.DisposeAsync(); // faulted circuits dispose quietly
    }
}
