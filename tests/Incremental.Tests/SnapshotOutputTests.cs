using Incremental.Storage;

namespace Incremental.Tests;

/// <summary>
/// Table-shaped outputs and their automatic resync: a keyed result's <c>ToOutput()</c>
/// maintains checkpoint-backed snapshot state, and the handle's <c>Changes()</c> begins
/// with one batch asserting the current integrated state — consistent with the live feed,
/// correct across restarts and mid-run subscription. Plus the single-consumer contract,
/// <c>MaterializeInMemory</c>, and the fault-domain rule that a failed publish faults the
/// circuit instead of silently losing a step's batches.
/// </summary>
public class SnapshotOutputTests
{
    [Fact]
    public async Task Bootstrap_batch_matches_integrated_state_including_retractions()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var words = builder.Input<string>("words");
        var countsDef = words.GroupBy(w => w).Count().ToOutput();

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(words);
        var output = circuit.Handle(countsDef);

        await input.InsertAsync("a");
        await input.InsertAsync("b");
        await circuit.StepAsync();
        await input.RemoveAsync("a");
        await input.InsertAsync("b");
        await circuit.StepAsync();

        // Subscribing mid-run: the first batch is the current state, not a replay.
        await using var changes = output.Changes().GetAsyncEnumerator();
        Assert.True(await changes.MoveNextAsync());
        var bootstrap = changes.Current;
        Assert.Equal(0, bootstrap.WeightOf(new KeyedValue<string, long>("a", 1)));
        Assert.Equal(1, bootstrap.WeightOf(new KeyedValue<string, long>("b", 2)));
        Assert.Equal(1, bootstrap.DistinctCount);
    }

    [Fact]
    public async Task Bootstrap_first_batch_is_empty_on_a_fresh_start()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var words = builder.Input<string>("words");
        var countsDef = words.GroupBy(w => w).Count().ToOutput();

        await using var circuit = builder.Build();
        circuit.Start();
        var output = circuit.Handle(countsDef);

        // The reset batch still arrives — a consumer's "treat batch zero as a reset" logic
        // needs no special case for a fresh store.
        await using var changes = output.Changes().GetAsyncEnumerator();
        Assert.True(await changes.MoveNextAsync());
        Assert.True(changes.Current.IsEmpty);
    }

    [Fact]
    public async Task Bootstrap_merges_worker_shards()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Workers = 4 });
        var numbers = builder.Input<int>("numbers");
        var countsDef = numbers.GroupBy(n => n % 3).Count().ToOutput();

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);
        var output = circuit.Handle(countsDef);

        for (var n = 0; n < 10; n++)
            await input.InsertAsync(n);
        await circuit.StepAsync();

        await using var changes = output.Changes().GetAsyncEnumerator();
        Assert.True(await changes.MoveNextAsync());
        var bootstrap = changes.Current;
        Assert.Equal(1, bootstrap.WeightOf(new KeyedValue<int, long>(0, 4)));
        Assert.Equal(1, bootstrap.WeightOf(new KeyedValue<int, long>(1, 3)));
        Assert.Equal(1, bootstrap.WeightOf(new KeyedValue<int, long>(2, 3)));
        Assert.Equal(3, bootstrap.DistinctCount);
    }

    [Fact]
    public async Task Bootstrap_discards_buffered_batches_it_subsumes()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var numbers = builder.Input<int>("numbers");
        var countsDef = numbers.GroupBy(n => n).Count().ToOutput();

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);
        var output = circuit.Handle(countsDef);

        await input.InsertAsync(1);
        await circuit.StepAsync(); // the batch sits unread in the output channel

        await using var changes = output.Changes().GetAsyncEnumerator();
        Assert.True(await changes.MoveNextAsync());
        var bootstrap = changes.Current;
        Assert.Equal(1, bootstrap.WeightOf(new KeyedValue<int, long>(1, 1)));

        // The buffered batch was subsumed by the bootstrap and discarded: integrating the
        // bootstrap plus everything after it never double-counts.
        await input.InsertAsync(2);
        await circuit.StepAsync();
        Assert.True(await changes.MoveNextAsync());
        var next = changes.Current;
        Assert.Equal(1, next.WeightOf(new KeyedValue<int, long>(2, 1)));
        Assert.Equal(1, next.DistinctCount);
    }

    [Fact]
    public async Task Bootstrap_read_unblocks_a_step_stalled_on_a_full_output_channel()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, OutputChannelCapacity = 1 });
        var numbers = builder.Input<int>("numbers");
        var countsDef = numbers.GroupBy(n => n).Count().ToOutput();

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);
        var output = circuit.Handle(countsDef);

        await input.InsertAsync(1);
        await circuit.StepAsync(); // fills the only channel slot

        await input.InsertAsync(2);
        var stalled = circuit.StepAsync(); // blocks publishing, holding the step gate
        Assert.False(stalled.IsCompleted);

        // The bootstrap read waits for the in-flight step, discarding the buffered batches
        // it subsumes — which is exactly what lets the stalled publish (and so the step)
        // complete.
        await using var changes = output.Changes().GetAsyncEnumerator();
        Assert.True(await changes.MoveNextAsync());
        await stalled;
        var bootstrap = changes.Current;
        Assert.Equal(1, bootstrap.WeightOf(new KeyedValue<int, long>(1, 1)));
        Assert.Equal(1, bootstrap.WeightOf(new KeyedValue<int, long>(2, 1)));
    }

    private sealed record OrdersPipeline(
        CircuitBuilder Builder,
        InputStream<(int Id, string Region)> Orders,
        Output<KeyedValue<string, long>> Counts);

    private static OrdersPipeline Pipeline(IStorageProvider storage)
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        var orders = builder.Input<(int Id, string Region)>("orders");
        var counts = orders.GroupBy(o => o.Region).Count().ToOutput();
        return new OrdersPipeline(builder, orders, counts);
    }

    [Fact]
    public async Task Bootstrap_recovers_from_checkpoint_after_crash()
    {
        var storage = new InMemoryCheckpointStorageProvider();

        // First run: steps commit (interval 1), but nobody ever reads the published batches
        // — the crash loses them for good. Abandon without disposing, like a real crash.
        var crashedPipeline = Pipeline(storage);
        var crashed = crashedPipeline.Builder.Build();
        crashed.Start();
        var input = crashed.Handle(crashedPipeline.Orders);
        await input.InsertAsync((1, "emea"), offset: 0);
        await input.InsertAsync((2, "emea"), offset: 1);
        await input.InsertAsync((3, "apac"), offset: 2);
        await crashed.StepAsync();

        // Restart against the same provider: batch zero of the changefeed is the consumer's
        // bootstrap — it reflects the committed checkpoint even though the pre-crash batches
        // were never consumed.
        var resumedPipeline = Pipeline(storage);
        await using var resumed = resumedPipeline.Builder.Build();
        resumed.Start();
        await using var changes = resumed.Handle(resumedPipeline.Counts).Changes().GetAsyncEnumerator();
        Assert.True(await changes.MoveNextAsync());
        var bootstrap = changes.Current;
        Assert.Equal(1, bootstrap.WeightOf(new KeyedValue<string, long>("emea", 2)));
        Assert.Equal(1, bootstrap.WeightOf(new KeyedValue<string, long>("apac", 1)));
        Assert.Equal(2, bootstrap.DistinctCount);
    }

    [Fact]
    public async Task MaterializeInMemory_view_is_correct_across_restart()
    {
        var storage = new InMemoryCheckpointStorageProvider();

        var crashedPipeline = Pipeline(storage);
        var crashed = crashedPipeline.Builder.Build();
        crashed.Start();
        var input = crashed.Handle(crashedPipeline.Orders);
        await input.InsertAsync((1, "emea"), offset: 0);
        await input.InsertAsync((2, "apac"), offset: 1);
        await crashed.StepAsync();

        var resumedPipeline = Pipeline(storage);
        await using var resumed = resumedPipeline.Builder.Build();
        resumed.Start();
        var view = resumed.Handle(resumedPipeline.Counts).MaterializeInMemory();

        // Feed one more row and let the view integrate bootstrap + the live batch.
        var resumedInput = resumed.Handle(resumedPipeline.Orders);
        await resumedInput.InsertAsync((3, "emea"), offset: 2);
        await resumed.StepAsync();
        await resumed.DisposeAsync();
        await view.Completion;

        var rows = view.Current().ToDictionary(e => e.Value, e => e.Weight);
        Assert.Equal(1, rows[new KeyedValue<string, long>("emea", 2)]);
        Assert.Equal(1, rows[new KeyedValue<string, long>("apac", 1)]);
        await view.DisposeAsync();
    }

    [Fact]
    public async Task An_output_has_exactly_one_consumer()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var numbers = builder.Input<int>("numbers");
        var countsDef = numbers.GroupBy(n => n).Count().ToOutput();

        await using var circuit = builder.Build();
        circuit.Start();
        var output = circuit.Handle(countsDef);

        _ = output.Changes();
        Assert.Throws<InvalidOperationException>(() => output.Changes());
        Assert.Throws<InvalidOperationException>(() => output.MaterializeInMemory());
    }

    [Fact]
    public async Task MaterializeInMemory_on_a_plain_stream_output_throws()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var numbers = builder.Input<int>("numbers");
        var plainDef = numbers.ToOutput();

        await using var circuit = builder.Build();
        circuit.Start();

        var ex = Assert.Throws<InvalidOperationException>(() => circuit.Handle(plainDef).MaterializeInMemory());
        Assert.Contains("table-shaped", ex.Message);
    }

    [Fact]
    public async Task ToOutput_without_bootstrap_is_a_pure_changefeed()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var numbers = builder.Input<int>("numbers");
        var countsDef = numbers.GroupBy(n => n).Count().ToOutput(bootstrap: false);

        await using var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);
        var output = circuit.Handle(countsDef);

        await input.InsertAsync(1);
        await circuit.StepAsync();

        // No reset batch: the first thing a consumer sees is the live delta itself.
        await using var changes = output.Changes().GetAsyncEnumerator();
        Assert.True(await changes.MoveNextAsync());
        Assert.Equal(1, changes.Current.WeightOf(new KeyedValue<int, long>(1, 1)));
    }

    [Fact]
    public async Task Failed_publish_faults_the_circuit_instead_of_losing_batches()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, OutputChannelCapacity = 1 });
        var numbers = builder.Input<int>("numbers");
        numbers.ToOutput();

        var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);

        await input.InsertAsync(1);
        await circuit.StepAsync(); // fills the only channel slot

        // The next step's publish blocks on the full channel; cancelling it must not leave
        // a healthy circuit that silently dropped the step's batches — state has already
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
