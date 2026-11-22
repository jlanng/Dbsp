using Dbsp.Storage;

namespace Dbsp.Tests;

/// <summary>
/// <see cref="CircuitOptions.AcknowledgeDeadLetters"/>: the dead-letter output as an
/// acknowledged output. A checkpoint does not pass a dropped record until its reader has
/// said the record is stored, so a crash before that drops and reports it again.
/// </summary>
public class AcknowledgedDeadLetterTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Settle = TimeSpan.FromMilliseconds(100);

    /// <summary>Disposes a circuit at the end of a test, but not for ever: a test that has
    /// failed with a step still held at its commit gate must end as a failure.</summary>
    private sealed class Bounded(Circuit circuit) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync() => await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
    }

    private static (CircuitBuilder Builder, InputStream<int> Numbers) Halves(IStorageProvider storage, bool acknowledge = true)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false,
            Storage = storage,
            ErrorPolicy = RecordErrorPolicy.DeadLetter,
            AcknowledgeDeadLetters = acknowledge,
        });
        var numbers = builder.Input<int>("numbers");
        numbers.Select(n => n % 2 == 0 ? n / 2 : throw new FormatException($"{n} is odd")).ToOutput();
        return (builder, numbers);
    }

    [Fact]
    public async Task Checkpoint_waits_until_the_step_s_dead_letters_are_acknowledged()
    {
        var (builder, numbers) = Halves(new InMemoryCheckpointStorageProvider());
        var circuit = builder.Build();
        await using var cleanup = new Bounded(circuit);
        circuit.Start();
        var input = circuit.Handle(numbers);

        await input.InsertAsync(3, offset: 0);
        await input.InsertAsync(4, offset: 1);
        await input.InsertAsync(5, offset: 2);
        var step = circuit.StepAsync();

        var batch = await circuit.DeadLetterBatches.ReadAsync().AsTask().WaitAsync(Timeout);
        Assert.Equal(1, batch.Sequence);
        Assert.Equal([3, 5], batch.Letters.Select(l => (int)l.Record!).Order());
        Assert.All(batch.Letters, l => Assert.IsType<FormatException>(l.Exception));

        await Task.Delay(Settle);
        Assert.False(step.IsCompleted, "The step committed past dead letters nobody had acknowledged.");
        Assert.Null(circuit.CommittedStep);

        batch.Acknowledge();
        await step.WaitAsync(Timeout);
        Assert.Equal(1L, circuit.CommittedStep);
        Assert.Equal(2L, circuit.CommittedOffset("numbers"));
    }

    [Fact]
    public async Task A_step_that_dropped_nothing_commits_without_an_acknowledgement()
    {
        var (builder, numbers) = Halves(new InMemoryCheckpointStorageProvider());
        var circuit = builder.Build();
        await using var cleanup = new Bounded(circuit);
        circuit.Start();
        await circuit.Handle(numbers).InsertAsync(4, offset: 0);
        await circuit.StepAsync().WaitAsync(Timeout);
        Assert.Equal(1L, circuit.CommittedStep);
        Assert.False(circuit.DeadLetterBatches.TryRead(out _));
    }

    [Fact]
    public async Task Dead_letters_not_acknowledged_before_a_crash_are_reported_again()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        {
            var (builder, numbers) = Halves(storage);
            var crashed = builder.Build();
            crashed.Start();
            var input = crashed.Handle(numbers);
            await input.InsertAsync(1, offset: 0);
            var first = crashed.StepAsync();
            (await crashed.DeadLetterBatches.ReadAsync().AsTask().WaitAsync(Timeout)).Acknowledge();
            await first.WaitAsync(Timeout);

            await input.InsertAsync(7, offset: 1);
            var second = crashed.StepAsync();
            var read = await crashed.DeadLetterBatches.ReadAsync().AsTask().WaitAsync(Timeout);
            Assert.Equal(7, read.Letters.Single().Record);
            // Read, never acknowledged: the reader's store did not confirm it. The process dies here.
            await Task.Delay(Settle);
            Assert.False(second.IsCompleted);
            crashed.AbandonDeadLetters();
            Assert.IsNotType<TimeoutException>(await Assert.ThrowsAnyAsync<Exception>(() => second.WaitAsync(Timeout)));
        }

        var (rebuilt, again) = Halves(storage);
        var circuit = rebuilt.Build();
        await using var cleanup = new Bounded(circuit);
        circuit.Start();
        Assert.Equal(0L, circuit.CommittedOffset("numbers"));   // the checkpoint stands before record 7
        var resumed = circuit.Handle(again);
        await resumed.InsertAsync(1, offset: 0);                 // replayed below the checkpoint: dropped
        await resumed.InsertAsync(7, offset: 1);
        var step = circuit.StepAsync();
        var batch = await circuit.DeadLetterBatches.ReadAsync().AsTask().WaitAsync(Timeout);
        Assert.Equal(7, batch.Letters.Single().Record);
        Assert.Equal(2, batch.Sequence);
        batch.Acknowledge();
        await step.WaitAsync(Timeout);
        Assert.Equal(1L, circuit.CommittedOffset("numbers"));
    }

    [Fact]
    public async Task Abandoning_the_dead_letters_faults_the_circuit_with_the_cause()
    {
        var (builder, numbers) = Halves(new InMemoryCheckpointStorageProvider());
        var circuit = builder.Build();
        await using var cleanup = new Bounded(circuit);
        circuit.Start();
        await circuit.Handle(numbers).InsertAsync(1, offset: 0);
        var step = circuit.StepAsync();
        await circuit.DeadLetterBatches.ReadAsync().AsTask().WaitAsync(Timeout);
        circuit.AbandonDeadLetters(new IOException("the dead-letter store is full"));
        var fault = await Assert.ThrowsAnyAsync<Exception>(() => step.WaitAsync(Timeout));
        Assert.IsNotType<TimeoutException>(fault);
        Assert.Contains("dead letters could not be stored", fault.ToString());
        Assert.Contains("the dead-letter store is full", fault.ToString());
        Assert.Null(circuit.CommittedStep);
    }

    [Fact]
    public async Task The_two_readers_are_exclusive()
    {
        var (acknowledged, _) = Halves(new InMemoryCheckpointStorageProvider());
        await using var a = acknowledged.Build();
        Assert.Throws<InvalidOperationException>(() => a.DeadLetters);
        _ = a.DeadLetterBatches;

        var (plain, _) = Halves(new InMemoryCheckpointStorageProvider(), acknowledge: false);
        await using var b = plain.Build();
        Assert.Throws<InvalidOperationException>(() => b.DeadLetterBatches);
        _ = b.DeadLetters;
    }
}
