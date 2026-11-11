using System.Diagnostics.Metrics;
using Incremental.Cluster;
using Incremental.Storage;
using Incremental.Storage.Redis;
using StackExchange.Redis;

namespace Incremental.Tests;

/// <summary>
/// Commit after delivery: a circuit with an acknowledged output publishes a step's batches
/// first and commits its checkpoint only once their consumer has confirmed them, so a crash
/// re-runs and redelivers instead of losing output. "Crash" is abandoning a circuit without
/// disposing it, as in <see cref="CheckpointTests"/>. Every wait is fenced with a timeout: a
/// gate that never opens shows up as a failure, not a hang.
/// </summary>
public class CommitAfterDeliveryTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Settle = TimeSpan.FromMilliseconds(100);

    private sealed record Pipeline(
        CircuitBuilder Builder,
        InputStream<(int Id, string Region)> Orders,
        AcknowledgedOutput<KeyedValue<string, long>> Counts);

    private static Pipeline CountByRegion(
        IStorageProvider storage, int workers = 1, int interval = 1, bool autoStep = false,
        TimeSpan? maxDelay = null, string? name = null)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            Name = name,
            AutoStep = autoStep,
            Workers = workers,
            Storage = storage,
            CheckpointInterval = interval,
            CheckpointMaxDelay = maxDelay,
        });
        var orders = builder.Input<(int Id, string Region)>("orders");
        var counts = orders.GroupBy(o => o.Region).Count().ToAcknowledgedOutput();
        return new Pipeline(builder, orders, counts);
    }

    private static KeyedValue<string, long> Row(string region, long count) => new(region, count);

    private static Dictionary<KeyedValue<string, long>, long> Contents(ZSet<KeyedValue<string, long>> changes)
    {
        var contents = new Dictionary<KeyedValue<string, long>, long>();
        foreach (var (value, weight) in changes)
            contents[value] = weight;
        return contents;
    }

    /// <summary>A keyed upsert sink, the kind at-least-once delivery is meant for: per key a
    /// batch's assertion replaces the row and a lone retraction deletes it, so applying a
    /// batch twice — or replaying a run of them — converges.</summary>
    private sealed class UpsertSink
    {
        public Dictionary<string, long> Rows { get; } = new();
        public List<(long Sequence, Dictionary<KeyedValue<string, long>, long> Changes)> Log { get; } = [];
        public long HighestApplied { get; private set; }
        public List<long> Redelivered { get; } = [];

        public void Apply(OutputBatch<KeyedValue<string, long>> batch)
        {
            lock (Rows)
            {
                if (batch.Sequence <= HighestApplied)
                    Redelivered.Add(batch.Sequence);
                HighestApplied = Math.Max(HighestApplied, batch.Sequence);
                Log.Add((batch.Sequence, Contents(batch.Changes)));

                foreach (var (row, weight) in batch.Changes)
                    if (weight < 0)
                        Rows.Remove(row.Key);
                foreach (var (row, weight) in batch.Changes)
                    if (weight > 0)
                        Rows[row.Key] = row.Value;
            }
        }

        public int Applied
        {
            get
            {
                lock (Rows)
                    return Log.Count;
            }
        }
    }

    /// <summary>Disposes a circuit at the end of a test, but not for ever: a test that has
    /// already failed with a step still held at its commit gate must end as a failure, and
    /// disposal queues behind that step.</summary>
    private sealed class BoundedDisposal(Circuit circuit) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync() => await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
    }

    private static async Task Eventually(Func<bool> done)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!done())
        {
            Assert.True(DateTime.UtcNow < deadline, "Timed out waiting for the expected state.");
            await Task.Delay(5);
        }
    }

    // ---- the gate ----

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task Checkpoint_waits_for_the_acknowledgement(int workers)
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var pipeline = CountByRegion(storage, workers);
        var circuit = pipeline.Builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();
        var input = circuit.Handle(pipeline.Orders);
        var output = circuit.Handle(pipeline.Counts);

        await input.SendAsync((1, "emea"), 1, offset: 0);
        var step = circuit.StepAsync();

        // Delivered: the batch is readable while the step is still held at its commit.
        var batch = await output.Reader.ReadAsync().AsTask().WaitAsync(Timeout);
        Assert.Equal(1, batch.Sequence);
        Assert.Equal(1, batch.Changes.WeightOf(Row("emea", 1)));

        await Task.Delay(Settle);
        Assert.False(step.IsCompleted, "The step committed without its batch being acknowledged.");
        Assert.Null(circuit.CommittedOffset("orders"));
        Assert.Null(circuit.CommittedStep);
        Assert.Equal(0, output.AcknowledgedSequence);

        batch.Acknowledge();
        await step.WaitAsync(Timeout);
        Assert.Equal(0L, circuit.CommittedOffset("orders"));
        Assert.Equal(1L, circuit.CommittedStep);
        Assert.Equal(1, output.AcknowledgedSequence);
    }

    [Fact]
    public async Task Step_that_published_nothing_commits_without_an_acknowledgement()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        var numbers = builder.Input<int>("numbers");
        var evens = numbers.Where(n => n % 2 == 0).ToAcknowledgedOutput();
        var circuit = builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();
        var input = circuit.Handle(numbers);
        var output = circuit.Handle(evens);

        await input.InsertAsync(1); // filtered out: the step has no batch to wait for
        await circuit.StepAsync().WaitAsync(Timeout);
        Assert.Equal(0L, circuit.CommittedOffset("numbers"));
        Assert.False(output.Reader.TryRead(out _));

        // Sequences are step numbers, so the empty step leaves a gap.
        await input.InsertAsync(2);
        var step = circuit.StepAsync();
        var batch = await output.Reader.ReadAsync().AsTask().WaitAsync(Timeout);
        Assert.Equal(2, batch.Sequence);
        batch.Acknowledge();
        await step.WaitAsync(Timeout);
    }

    [Fact]
    public async Task Commit_point_needs_every_earlier_batch_acknowledged_and_an_acknowledgement_is_cumulative()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var pipeline = CountByRegion(storage, interval: 3);
        var circuit = pipeline.Builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();
        var input = circuit.Handle(pipeline.Orders);
        var output = circuit.Handle(pipeline.Counts);

        // Steps between commit points do not wait: nothing becomes durable at them.
        for (var i = 0; i < 2; i++)
        {
            await input.SendAsync((i, "emea"), 1, offset: i);
            await circuit.StepAsync().WaitAsync(Timeout);
        }
        Assert.Null(circuit.CommittedOffset("orders"));

        await input.SendAsync((2, "emea"), 1, offset: 2);
        var third = circuit.StepAsync();
        var batches = new List<OutputBatch<KeyedValue<string, long>>>();
        for (var i = 0; i < 3; i++)
            batches.Add(await output.Reader.ReadAsync().AsTask().WaitAsync(Timeout));
        Assert.Equal([1L, 2L, 3L], batches.Select(b => b.Sequence));

        // Acknowledging the first two is not enough: step 3's own batch is outstanding.
        batches[1].Acknowledge();
        await Task.Delay(Settle);
        Assert.False(third.IsCompleted);
        Assert.Null(circuit.CommittedOffset("orders"));

        batches[2].Acknowledge();
        await third.WaitAsync(Timeout);
        Assert.Equal(2L, circuit.CommittedOffset("orders"));

        // Late and repeated acknowledgements are harmless; one for a batch that was never
        // published is a consumer bug.
        batches[0].Acknowledge();
        output.Acknowledge(3);
        Assert.Equal(3, output.AcknowledgedSequence);
        Assert.Throws<ArgumentOutOfRangeException>(() => output.Acknowledge(4));
        Assert.Throws<ArgumentOutOfRangeException>(() => output.Acknowledge(0));
    }

    [Fact]
    public async Task Only_the_acknowledged_output_holds_the_commit()
    {
        // A plain output beside the acknowledged one: nobody reads it, and nothing waits
        // for anybody to (within its channel's capacity, as ever).
        var storage = new InMemoryCheckpointStorageProvider();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        var numbers = builder.Input<int>("numbers");
        var plain = numbers.ToOutput();
        var acknowledged = numbers.ToAcknowledgedOutput();
        var circuit = builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();

        await circuit.Handle(numbers).InsertAsync(7);
        var step = circuit.StepAsync();
        var batch = await circuit.Handle(acknowledged).Reader.ReadAsync().AsTask().WaitAsync(Timeout);
        batch.Acknowledge();
        await step.WaitAsync(Timeout);
        Assert.Equal(0L, circuit.CommittedOffset("numbers"));
        Assert.True(circuit.Handle(plain).Reader.TryRead(out var unread));
        Assert.Equal(1, unread!.WeightOf(7));
    }

    [Fact]
    public async Task Plain_outputs_alone_still_commit_without_any_consumer()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        var numbers = builder.Input<int>("numbers");
        numbers.ToOutput();
        var circuit = builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();

        await circuit.Handle(numbers).InsertAsync(7);
        await circuit.StepAsync().WaitAsync(Timeout);
        Assert.Equal(0L, circuit.CommittedOffset("numbers"));
    }

    [Fact]
    public async Task Without_a_checkpointing_provider_nothing_waits()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var numbers = builder.Input<int>("numbers");
        var result = numbers.ToAcknowledgedOutput();
        var circuit = builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();

        await circuit.Handle(numbers).InsertAsync(7);
        await circuit.StepAsync().WaitAsync(Timeout);
        Assert.True(circuit.Handle(result).Reader.TryRead(out var batch));
        Assert.Equal(1, batch.Sequence);
        Assert.Null(circuit.CommittedStep);
    }

    // ---- crash at each point ----

    public enum CrashPoint
    {
        BeforeDelivery,
        AfterDeliveryBeforeAcknowledgement,
        AfterAcknowledgementBeforeCommit,
        AfterCommit,
    }

    /// <summary>The source: three steps' worth of offset-stamped orders.</summary>
    private static readonly (int Id, string Region)[][] Steps =
    [
        [(0, "emea"), (1, "apac")],
        [(2, "emea"), (3, "emea"), (4, "amer")],
        [(5, "apac"), (6, "amer")],
    ];

    private static async Task<bool> SendStepAsync(
        InputHandle<(int Id, string Region)> input, (int Id, string Region)[] orders, long? committed)
    {
        var sent = false;
        foreach (var order in orders)
        {
            if (order.Id <= (committed ?? -1))
                continue; // the producer resumes its source just above the committed offset
            await input.SendAsync(order, 1, offset: order.Id);
            sent = true;
        }
        return sent;
    }

    [Theory]
    [MemberData(nameof(CrashCases))]
    public async Task Crash_at_any_point_loses_no_output_and_redelivery_is_identifiable(CrashPoint crashPoint, int workers)
    {
        // Reference: the same source through a run that is never interrupted.
        var reference = new UpsertSink();
        {
            var pipeline = CountByRegion(new InMemoryCheckpointStorageProvider(), workers);
            var circuit = pipeline.Builder.Build();
            circuit.Start();
            var consumer = circuit.Handle(pipeline.Counts).ConsumeAsync((batch, _) =>
            {
                reference.Apply(batch);
                return ValueTask.CompletedTask;
            });
            foreach (var step in Steps)
            {
                await SendStepAsync(circuit.Handle(pipeline.Orders), step, committed: null);
                await circuit.StepAsync().WaitAsync(Timeout);
            }
            await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
            await consumer.WaitAsync(Timeout);
        }
        Assert.Equal(new Dictionary<string, long> { ["emea"] = 3, ["apac"] = 2, ["amer"] = 2 }, reference.Rows);
        Assert.Equal([1L, 2L, 3L], reference.Log.Select(entry => entry.Sequence));

        var storage = new FlakyCommitProvider(new InMemoryCheckpointStorageProvider());
        var sink = new UpsertSink();

        // The run that crashes during step 2. Step 1 is delivered, acknowledged, committed.
        {
            var pipeline = CountByRegion(storage, workers);
            var crashed = pipeline.Builder.Build();
            crashed.Start();
            var input = crashed.Handle(pipeline.Orders);
            var output = crashed.Handle(pipeline.Counts);

            await SendStepAsync(input, Steps[0], committed: null);
            var first = crashed.StepAsync();
            var batch = await output.Reader.ReadAsync().AsTask().WaitAsync(Timeout);
            sink.Apply(batch);
            batch.Acknowledge();
            await first.WaitAsync(Timeout);
            Assert.Equal(1L, crashed.CommittedOffset("orders"));

            await SendStepAsync(input, Steps[1], committed: null);
            storage.FailCommits = crashPoint == CrashPoint.AfterAcknowledgementBeforeCommit;
            var second = crashed.StepAsync();
            Assert.True(await output.Reader.WaitToReadAsync().AsTask().WaitAsync(Timeout));

            switch (crashPoint)
            {
                case CrashPoint.BeforeDelivery:
                    // Published into the channel; the process dies before the sink reads it.
                    break;

                case CrashPoint.AfterDeliveryBeforeAcknowledgement:
                    Assert.True(output.Reader.TryRead(out batch));
                    sink.Apply(batch);
                    break;

                case CrashPoint.AfterAcknowledgementBeforeCommit:
                    Assert.True(output.Reader.TryRead(out batch));
                    sink.Apply(batch);
                    batch.Acknowledge();
                    await Assert.ThrowsAsync<IOException>(() => second).WaitAsync(Timeout);
                    Assert.NotNull(crashed.Fault);
                    break;

                case CrashPoint.AfterCommit:
                    Assert.True(output.Reader.TryRead(out batch));
                    sink.Apply(batch);
                    batch.Acknowledge();
                    await second.WaitAsync(Timeout);
                    break;
            }

            if (crashPoint == CrashPoint.AfterCommit)
            {
                Assert.Equal(4L, crashed.CommittedOffset("orders"));
            }
            else
            {
                // Whatever the sink has or has not seen, the checkpoint is still step 1's.
                await Task.Delay(Settle);
                Assert.Equal(crashPoint == CrashPoint.AfterAcknowledgementBeforeCommit, second.IsCompleted);
                Assert.Equal(1L, crashed.CommittedOffset("orders"));
                Assert.Equal(1L, crashed.CommittedStep);
            }
            storage.FailCommits = false;
        }

        // The restart: same provider, the producer replays from the committed offset.
        {
            var pipeline = CountByRegion(storage, workers);
            var resumed = pipeline.Builder.Build();
            resumed.Start();
            var committed = resumed.CommittedOffset("orders");
            Assert.Equal(crashPoint == CrashPoint.AfterCommit ? 4L : 1L, committed);
            Assert.Equal(crashPoint == CrashPoint.AfterCommit ? 2L : 1L, resumed.CommittedStep);

            var consumer = resumed.Handle(pipeline.Counts).ConsumeAsync((batch, _) =>
            {
                sink.Apply(batch);
                return ValueTask.CompletedTask;
            });
            foreach (var step in Steps)
                if (await SendStepAsync(resumed.Handle(pipeline.Orders), step, committed))
                    await resumed.StepAsync().WaitAsync(Timeout);
            await resumed.DisposeAsync().AsTask().WaitAsync(Timeout);
            await consumer.WaitAsync(Timeout);
            Assert.Equal(6L, resumed.CommittedOffset("orders"));
        }

        // Nothing lost: the sink ends where the uninterrupted run's did.
        Assert.Equal(reference.Rows, sink.Rows);

        // Every step's batch reached the sink, under the sequence the reference run gave
        // it and with the same contents, however many times.
        foreach (var (sequence, changes) in sink.Log)
            Assert.Equal(reference.Log.Single(entry => entry.Sequence == sequence).Changes, changes);
        Assert.Equal([1L, 2L, 3L], sink.Log.Select(entry => entry.Sequence).Distinct());

        // And a batch delivered twice is recognisable as such by its sequence alone.
        var deliveredTwice = crashPoint is CrashPoint.AfterDeliveryBeforeAcknowledgement or CrashPoint.AfterAcknowledgementBeforeCommit;
        Assert.Equal(deliveredTwice ? [2L] : [], sink.Redelivered);
        Assert.Equal(deliveredTwice ? 4 : 3, sink.Log.Count);
    }

    public static TheoryData<CrashPoint, int> CrashCases()
    {
        var cases = new TheoryData<CrashPoint, int>();
        foreach (var crashPoint in Enum.GetValues<CrashPoint>())
            foreach (var workers in new[] { 1, 4 })
                cases.Add(crashPoint, workers);
        return cases;
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(4, 1)]
    [InlineData(1, 5)]
    public async Task Auto_stepping_crash_with_withheld_acknowledgements_replays_to_the_same_sink_contents(int workers, int interval)
    {
        const int orders = 400;
        string[] regions = ["emea", "apac", "amer"];
        var expected = regions.ToDictionary(r => r, r => (long)Enumerable.Range(0, orders).Count(i => regions[i % 3] == r));

        var storage = new InMemoryCheckpointStorageProvider();
        var sink = new UpsertSink();

        // The sink confirms its first three batches and then goes quiet: it still receives
        // (and applies) what the circuit publishes, but the checkpoint must stop moving.
        const int acknowledgedBatches = 3;
        long? stalledAt;
        {
            var pipeline = CountByRegion(storage, workers, interval, autoStep: true);
            var crashed = pipeline.Builder.Build();
            crashed.Start();
            var input = crashed.Handle(pipeline.Orders);
            var output = crashed.Handle(pipeline.Counts);
            var consumer = Task.Run(async () =>
            {
                await foreach (var batch in output.Reader.ReadAllAsync())
                {
                    sink.Apply(batch);
                    if (sink.Applied <= acknowledgedBatches)
                        batch.Acknowledge();
                }
            });

            for (var i = 0; i < orders; i++)
            {
                // Several steps, not one: while the sink is still confirming, wait for each
                // of these orders to come out as a batch of its own before sending on. (A
                // pause instead leaves it to the scheduler whether a slow start swallows
                // everything in three steps, and then there is no fourth batch to withhold.)
                var applied = sink.Applied;
                await input.SendAsync((i, regions[i % 3]), 1, offset: i).AsTask().WaitAsync(Timeout);
                if (i % 40 == 0 && applied <= acknowledgedBatches)
                    await Eventually(() => sink.Applied > applied);
            }

            await Eventually(() => sink.Applied > acknowledgedBatches);
            await Task.Delay(Settle);
            stalledAt = crashed.CommittedOffset("orders");
            await Task.Delay(Settle);
            Assert.Equal(stalledAt, crashed.CommittedOffset("orders"));
            Assert.True((stalledAt ?? -1) < orders - 1, "The checkpoint reached the end with acknowledgements withheld.");

            // Never further than the acknowledged batches account for: the unconfirmed
            // batch the sink is holding is not under the checkpoint.
            var confirmed = sink.Log.Take(acknowledgedBatches).Sum(entry => entry.Changes.Where(c => c.Value > 0).Sum(c => c.Key.Value))
                - sink.Log.Take(acknowledgedBatches).Sum(entry => entry.Changes.Where(c => c.Value < 0).Sum(c => c.Key.Value));
            Assert.True((stalledAt ?? -1) + 1 <= confirmed,
                $"Committed through offset {stalledAt}, but the acknowledged batches cover only {confirmed} orders.");
        }

        {
            var pipeline = CountByRegion(storage, workers, interval, autoStep: true);
            var resumed = pipeline.Builder.Build();
            resumed.Start();
            Assert.Equal(stalledAt, resumed.CommittedOffset("orders"));
            var consumer = resumed.Handle(pipeline.Counts).ConsumeAsync((batch, _) =>
            {
                sink.Apply(batch);
                return ValueTask.CompletedTask;
            });

            // The first order past the checkpoint goes in first, so that the first step the
            // restart runs has output: a step that drained only replayed orders would be
            // empty, take the withheld batch's sequence, and leave nothing to recognise as
            // its redelivery. Then the whole source again: what the checkpoint covers is
            // dropped as replay, and so is that first order when it comes round.
            var input = resumed.Handle(pipeline.Orders);
            var next = (int)((stalledAt ?? -1) + 1);
            await input.SendAsync((next, regions[next % 3]), 1, offset: next).AsTask().WaitAsync(Timeout);
            for (var i = 0; i < orders; i++)
                await input.SendAsync((i, regions[i % 3]), 1, offset: i).AsTask().WaitAsync(Timeout);
            await resumed.DisposeAsync().AsTask().WaitAsync(Timeout);
            await consumer.WaitAsync(Timeout);
            Assert.Equal(orders - 1L, resumed.CommittedOffset("orders"));
        }

        Assert.Equal(expected, sink.Rows);
        Assert.NotEmpty(sink.Redelivered); // the withheld batch came again, under a sequence already seen
    }

    [Fact]
    public async Task Redis_crash_after_delivery_redelivers_under_the_same_sequence()
    {
        ConnectionMultiplexer redis;
        try
        {
            redis = ConnectionMultiplexer.Connect("localhost:6379,connectTimeout=1000,abortConnect=true");
        }
        catch (RedisConnectionException)
        {
            throw Xunit.Sdk.SkipException.ForSkip("Redis is not reachable on localhost:6379 — run `docker compose up -d`.");
        }
        using var connection = redis;
        var options = new RedisStorageOptions { KeyPrefix = $"sp-test:{Guid.NewGuid():N}" };
        var sink = new UpsertSink();

        // Step 1 is delivered, acknowledged and committed; step 2 is delivered and the
        // process dies before the sink confirms it.
        {
            var pipeline = CountByRegion(new RedisStorageProvider(redis, options));
            var crashed = pipeline.Builder.Build();
            crashed.Start();
            var input = crashed.Handle(pipeline.Orders);
            var output = crashed.Handle(pipeline.Counts);

            await SendStepAsync(input, Steps[0], committed: null);
            var first = crashed.StepAsync();
            var batch = await output.Reader.ReadAsync().AsTask().WaitAsync(Timeout);
            sink.Apply(batch);
            batch.Acknowledge();
            await first.WaitAsync(Timeout);

            await SendStepAsync(input, Steps[1], committed: null);
            var second = crashed.StepAsync();
            sink.Apply(await output.Reader.ReadAsync().AsTask().WaitAsync(Timeout));
            await Task.Delay(Settle);
            Assert.False(second.IsCompleted);
        }

        // A fresh provider over the same keys: Redis holds step 1's checkpoint, sequence included.
        {
            var pipeline = CountByRegion(new RedisStorageProvider(redis, options));
            var resumed = pipeline.Builder.Build();
            resumed.Start();
            var committed = resumed.CommittedOffset("orders");
            Assert.Equal(1L, committed);
            Assert.Equal(1L, resumed.CommittedStep);

            var consumer = resumed.Handle(pipeline.Counts).ConsumeAsync((batch, _) =>
            {
                sink.Apply(batch);
                return ValueTask.CompletedTask;
            });
            foreach (var step in Steps)
                if (await SendStepAsync(resumed.Handle(pipeline.Orders), step, committed))
                    await resumed.StepAsync().WaitAsync(Timeout);
            await resumed.DisposeAsync().AsTask().WaitAsync(Timeout);
            await consumer.WaitAsync(Timeout);
            Assert.Equal(6L, resumed.CommittedOffset("orders"));
            Assert.Equal(3L, resumed.CommittedStep);
        }

        Assert.Equal(new Dictionary<string, long> { ["emea"] = 3, ["apac"] = 2, ["amer"] = 2 }, sink.Rows);
        Assert.Equal([1L, 2L, 2L, 3L], sink.Log.Select(entry => entry.Sequence));
        Assert.Equal(sink.Log[1].Changes, sink.Log[2].Changes);
        Assert.Equal([2L], sink.Redelivered);
    }

    [Fact]
    public async Task Sequence_continues_across_a_clean_restart()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        for (var run = 0; run < 3; run++)
        {
            var pipeline = CountByRegion(storage);
            var circuit = pipeline.Builder.Build();
            circuit.Start();
            Assert.Equal(run == 0 ? null : run * 2L, circuit.CommittedStep);

            var sequences = new List<long>();
            var consumer = circuit.Handle(pipeline.Counts).ConsumeAsync((batch, _) =>
            {
                sequences.Add(batch.Sequence);
                return ValueTask.CompletedTask;
            });
            for (var step = 0; step < 2; step++)
            {
                await circuit.Handle(pipeline.Orders).InsertAsync((step, "emea"));
                await circuit.StepAsync().WaitAsync(Timeout);
            }
            await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
            await consumer.WaitAsync(Timeout);
            Assert.Equal([run * 2L + 1, run * 2L + 2], sequences);
        }
    }

    // ---- backpressure, shutdown, a consumer that goes away ----

    [Fact]
    public async Task Withheld_acknowledgement_backpressures_the_producer_through_bounded_channels()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var builder = new CircuitBuilder(new CircuitOptions
        {
            Storage = storage,
            InputChannelCapacity = 4,
            MaxStepItemsPerInput = 4,
            OutputChannelCapacity = 2,
        });
        var numbers = builder.Input<int>("numbers");
        var result = numbers.ToAcknowledgedOutput();
        var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);
        var output = circuit.Handle(result);

        var produced = 0;
        var producer = Task.Run(async () =>
        {
            for (var i = 0; i < 1000; i++)
            {
                await input.InsertAsync(i);
                Volatile.Write(ref produced, i + 1);
            }
        });

        // Nothing acknowledges: one step runs, its commit waits, and the producer is held
        // by the input channel — a handful of records in, not a thousand.
        await Task.Delay(Settle * 3);
        Assert.False(producer.IsCompleted);
        Assert.InRange(Volatile.Read(ref produced), 1, 4 + 4 + 1);

        var consumer = output.ConsumeAsync((_, _) => ValueTask.CompletedTask);
        await producer.WaitAsync(Timeout);
        await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
        await consumer.WaitAsync(Timeout);
        Assert.Equal(999L, circuit.CommittedOffset("numbers"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Final_commit_at_disposal_waits_for_the_last_acknowledgements(bool autoStep)
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var pipeline = CountByRegion(storage, interval: 100, autoStep: autoStep);
        var circuit = pipeline.Builder.Build();
        circuit.Start();
        var output = circuit.Handle(pipeline.Counts);

        await circuit.Handle(pipeline.Orders).InsertAsync((1, "emea"));
        if (!autoStep)
            await circuit.StepAsync().WaitAsync(Timeout);
        var batch = await output.Reader.ReadAsync().AsTask().WaitAsync(Timeout);

        var disposal = circuit.DisposeAsync().AsTask();
        await Task.Delay(Settle);
        Assert.False(disposal.IsCompleted, "Disposal committed the final checkpoint past an unacknowledged batch.");
        Assert.Null(circuit.CommittedOffset("orders"));

        batch.Acknowledge();
        await disposal.WaitAsync(Timeout);
        Assert.Equal(0L, circuit.CommittedOffset("orders"));
    }

    [Fact]
    public async Task Consumer_that_stops_reading_faults_the_circuit_instead_of_stalling_it()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var pipeline = CountByRegion(storage);
        var circuit = pipeline.Builder.Build();
        circuit.Start();
        var output = circuit.Handle(pipeline.Counts);

        await circuit.Handle(pipeline.Orders).InsertAsync((1, "emea"));
        var step = circuit.StepAsync();
        await foreach (var _ in output.Batches())
            break; // read one batch, acknowledge nothing, walk away

        var fault = await Assert.ThrowsAsync<InvalidOperationException>(() => step).WaitAsync(Timeout);
        Assert.Contains("stopped reading", fault.Message);
        Assert.Same(fault, circuit.Fault);
        Assert.Null(circuit.CommittedOffset("orders"));
        await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
        Assert.Null(circuit.CommittedOffset("orders")); // and disposal does not commit a faulted circuit

        // One consumer, as on a plain output.
        Assert.Throws<InvalidOperationException>(() => output.Batches());
    }

    [Fact]
    public async Task Consumer_that_goes_away_during_shutdown_skips_the_final_commit()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var pipeline = CountByRegion(storage, interval: 100);
        var circuit = pipeline.Builder.Build();
        circuit.Start();
        var output = circuit.Handle(pipeline.Counts);

        await circuit.Handle(pipeline.Orders).InsertAsync((1, "emea"));
        await circuit.StepAsync().WaitAsync(Timeout);

        var received = 0;
        var consumer = Task.Run(async () =>
        {
            await foreach (var _ in output.Batches())
                received++; // received, never acknowledged
        });

        var disposal = circuit.DisposeAsync().AsTask();
        await consumer.WaitAsync(Timeout); // the output completed and was read to its end
        Assert.Equal(1, received);
        await Task.Delay(Settle);
        Assert.False(disposal.IsCompleted);

        // The shutdown timeout, in effect: the consumer is declared gone, the wait ends,
        // and the steps stay uncommitted for the restart to deliver again.
        output.Abandon();
        await disposal.WaitAsync(Timeout);
        Assert.Null(circuit.CommittedOffset("orders"));
    }

    [Fact]
    public async Task Cancelling_the_consumer_mid_feed_faults_the_circuit()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var pipeline = CountByRegion(storage, autoStep: true);
        var circuit = pipeline.Builder.Build();
        circuit.Start();
        var output = circuit.Handle(pipeline.Counts);

        using var giveUp = new CancellationTokenSource();
        var consumer = Task.Run(async () =>
        {
            await foreach (var _ in output.Batches(giveUp.Token))
            {
            }
        });

        await circuit.Handle(pipeline.Orders).InsertAsync((1, "emea"));
        await Task.Delay(Settle);
        Assert.Null(circuit.Fault);
        giveUp.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => consumer).WaitAsync(Timeout);
        await Assert.ThrowsAsync<InvalidOperationException>(() => circuit.Completion).WaitAsync(Timeout);
        Assert.Null(circuit.CommittedOffset("orders"));
        await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
    }

    [Fact]
    public async Task ConsumeAsync_acknowledges_on_return_and_a_throwing_sink_faults_with_its_exception()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var pipeline = CountByRegion(storage);
        var circuit = pipeline.Builder.Build();
        circuit.Start();
        var input = circuit.Handle(pipeline.Orders);
        var output = circuit.Handle(pipeline.Counts);

        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        var consumer = output.ConsumeAsync(async (_, _) =>
        {
            if (++calls == 1)
                await release.Task; // "delivering"
            else
                throw new IOException("sink is down");
        });

        await input.InsertAsync((1, "emea"));
        var first = circuit.StepAsync();
        await Task.Delay(Settle);
        Assert.False(first.IsCompleted, "The batch was acknowledged before the sink returned.");
        release.SetResult();
        await first.WaitAsync(Timeout);
        Assert.Equal(0L, circuit.CommittedOffset("orders"));

        await input.InsertAsync((2, "emea"));
        var fault = await Assert.ThrowsAsync<InvalidOperationException>(() => circuit.StepAsync()).WaitAsync(Timeout);
        Assert.IsType<IOException>(fault.InnerException);
        await Assert.ThrowsAsync<IOException>(() => consumer).WaitAsync(Timeout);
        Assert.Equal(0L, circuit.CommittedOffset("orders")); // not past the batch the sink refused
        await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
    }

    // ---- CheckpointMaxDelay ----

    [Fact]
    public async Task Max_delay_commits_an_idle_auto_stepping_circuit()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var maxDelay = TimeSpan.FromMilliseconds(150);
        var pipeline = CountByRegion(storage, interval: 1_000_000, autoStep: true, maxDelay: maxDelay);
        var circuit = pipeline.Builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();
        var consumer = circuit.Handle(pipeline.Counts).ConsumeAsync((_, _) => ValueTask.CompletedTask);

        var before = System.Diagnostics.Stopwatch.GetTimestamp();
        await circuit.Handle(pipeline.Orders).InsertAsync((1, "emea"));

        // No further input: the delay alone brings the commit — and nothing brings it
        // sooner, the interval being nowhere near. The bound is on when the commit is seen,
        // measured from before the record went in, so a slow machine cannot break it: it
        // can only see the commit later still.
        await Eventually(() => circuit.CommittedOffset("orders") == 0L);
        var elapsed = System.Diagnostics.Stopwatch.GetElapsedTime(before);
        Assert.True(elapsed >= maxDelay, $"committed {elapsed.TotalMilliseconds:F0} ms after the record, before the {maxDelay.TotalMilliseconds:F0} ms delay was up");
    }

    [Fact]
    public async Task Max_delay_idle_commit_still_waits_for_the_acknowledgement()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var pipeline = CountByRegion(storage, interval: 1_000_000, autoStep: true, maxDelay: TimeSpan.FromMilliseconds(50));
        var circuit = pipeline.Builder.Build();
        circuit.Start();
        var output = circuit.Handle(pipeline.Counts);

        await circuit.Handle(pipeline.Orders).InsertAsync((1, "emea"));
        var batch = await output.Reader.ReadAsync().AsTask().WaitAsync(Timeout);
        await Task.Delay(Settle * 3);
        Assert.Null(circuit.CommittedOffset("orders"));

        batch.Acknowledge();
        await Eventually(() => circuit.CommittedOffset("orders") == 0L);
        var consumer = output.ConsumeAsync((_, _) => ValueTask.CompletedTask);
        await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
    }

    [Fact]
    public async Task Max_delay_makes_a_manual_step_commit_before_the_interval()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false,
            Storage = storage,
            CheckpointInterval = 1_000_000,
            CheckpointMaxDelay = TimeSpan.FromMilliseconds(100),
        });
        var numbers = builder.Input<int>("numbers");
        numbers.ToOutput();
        var circuit = builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();
        var input = circuit.Handle(numbers);

        await input.InsertAsync(1);
        await circuit.StepAsync();
        Assert.Null(circuit.CommittedOffset("numbers"));

        await Task.Delay(150);
        await input.InsertAsync(2);
        await circuit.StepAsync();
        Assert.Equal(1L, circuit.CommittedOffset("numbers"));

        // The clock restarts at the commit.
        await input.InsertAsync(3);
        await circuit.StepAsync();
        Assert.Equal(1L, circuit.CommittedOffset("numbers"));
    }

    // ---- the provider seam, the metric, what is refused ----

    /// <summary>Delegates to an in-memory provider; commits can be made to fail, and are
    /// counted by the path they came in on.</summary>
    private sealed class FlakyCommitProvider(InMemoryCheckpointStorageProvider inner) : ICheckpointStorageProvider
    {
        public bool FailCommits { get; set; }
        public int AsyncCommits { get; private set; }
        public int SyncCommits { get; private set; }

        public IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull => inner.CreateWeightStore<T>(name);

        public IIndexedWeightStore<TKey, TValue> CreateIndexedWeightStore<TKey, TValue>(string name)
            where TKey : notnull
            where TValue : notnull => inner.CreateIndexedWeightStore<TKey, TValue>(name);

        public IValueStore<TKey, TValue> CreateValueStore<TKey, TValue>(string name) where TKey : notnull =>
            inner.CreateValueStore<TKey, TValue>(name);

        public IReadOnlyDictionary<string, long> Recover() => inner.Recover();

        public void Commit(IReadOnlyDictionary<string, long> inputOffsets)
        {
            SyncCommits++;
            inner.Commit(inputOffsets);
        }

        public async ValueTask CommitAsync(IReadOnlyDictionary<string, long> inputOffsets, CancellationToken cancellationToken = default)
        {
            await Task.Yield(); // a commit that really is asynchronous
            if (FailCommits)
                throw new IOException("commit failed");
            AsyncCommits++;
            inner.Commit(inputOffsets);
        }

        public string? CommittedManifest(string scope) => inner.CommittedManifest(scope);

        public void StageManifest(string scope, string manifest) => inner.StageManifest(scope, manifest);
    }

    /// <summary>A provider written before <c>CommitAsync</c> existed: only the synchronous
    /// commit, picked up through the interface's default.</summary>
    private sealed class SyncOnlyProvider(InMemoryCheckpointStorageProvider inner) : ICheckpointStorageProvider
    {
        public int Commits { get; private set; }

        public IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull => inner.CreateWeightStore<T>(name);

        public IIndexedWeightStore<TKey, TValue> CreateIndexedWeightStore<TKey, TValue>(string name)
            where TKey : notnull
            where TValue : notnull => inner.CreateIndexedWeightStore<TKey, TValue>(name);

        public IValueStore<TKey, TValue> CreateValueStore<TKey, TValue>(string name) where TKey : notnull =>
            inner.CreateValueStore<TKey, TValue>(name);

        public IReadOnlyDictionary<string, long> Recover() => inner.Recover();

        public void Commit(IReadOnlyDictionary<string, long> inputOffsets)
        {
            Commits++;
            inner.Commit(inputOffsets);
        }

        public string? CommittedManifest(string scope) => inner.CommittedManifest(scope);

        public void StageManifest(string scope, string manifest) => inner.StageManifest(scope, manifest);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Circuit_commits_through_CommitAsync(bool autoStep)
    {
        var storage = new FlakyCommitProvider(new InMemoryCheckpointStorageProvider());
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = autoStep, Storage = storage });
        var numbers = builder.Input<int>("numbers");
        var doubled = numbers.Select(n => n * 2).ToOutput();
        var circuit = builder.Build();
        circuit.Start();

        await circuit.Handle(numbers).InsertAsync(1);
        if (!autoStep)
            await circuit.StepAsync().WaitAsync(Timeout);
        await circuit.Handle(doubled).Reader.ReadAsync().AsTask().WaitAsync(Timeout);
        await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);

        Assert.Equal(0L, circuit.CommittedOffset("numbers"));
        Assert.True(storage.AsyncCommits >= 1);
        Assert.Equal(0, storage.SyncCommits);
    }

    [Fact]
    public async Task Provider_with_only_the_synchronous_commit_keeps_working()
    {
        var storage = new SyncOnlyProvider(new InMemoryCheckpointStorageProvider());
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        var numbers = builder.Input<int>("numbers");
        numbers.ToOutput();
        var circuit = builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();

        await circuit.Handle(numbers).InsertAsync(1);
        await circuit.StepAsync();
        Assert.Equal(1, storage.Commits);
        Assert.Equal(0L, circuit.CommittedOffset("numbers"));
    }

    [Fact]
    public async Task Failed_asynchronous_commit_faults_the_circuit_and_leaves_the_previous_checkpoint()
    {
        var storage = new FlakyCommitProvider(new InMemoryCheckpointStorageProvider());
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = storage });
        var numbers = builder.Input<int>("numbers");
        numbers.ToOutput();
        var circuit = builder.Build();
        circuit.Start();
        var input = circuit.Handle(numbers);

        await input.InsertAsync(1);
        await circuit.StepAsync();
        storage.FailCommits = true;
        await input.InsertAsync(2);
        await Assert.ThrowsAsync<IOException>(() => circuit.StepAsync());
        Assert.IsType<IOException>(circuit.Fault);
        Assert.Equal(0L, circuit.CommittedOffset("numbers"));
        await circuit.DisposeAsync();
    }

    [Fact]
    public async Task Time_spent_waiting_for_acknowledgements_is_a_metric()
    {
        const string name = "ack-wait-metric";
        var waits = new List<double>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "Incremental" && instrument.Name == "incremental.circuit.sink_ack_wait")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
        {
            Assert.Equal("s", instrument.Unit);
            foreach (var tag in tags)
                if (tag.Key == "circuit" && Equals(tag.Value, name))
                    lock (waits)
                        waits.Add(value);
        });
        listener.Start();

        var pipeline = CountByRegion(new InMemoryCheckpointStorageProvider(), name: name);
        var circuit = pipeline.Builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();
        var output = circuit.Handle(pipeline.Counts);

        await circuit.Handle(pipeline.Orders).InsertAsync((1, "emea"));
        var step = circuit.StepAsync();
        var batch = await output.Reader.ReadAsync().AsTask().WaitAsync(Timeout);
        await Task.Delay(200);
        batch.Acknowledge();
        await step.WaitAsync(Timeout);

        lock (waits)
        {
            var wait = Assert.Single(waits);
            Assert.InRange(wait, 0.15, 5.0);
        }
    }

    [Fact]
    public void Clustered_circuits_refuse_acknowledged_outputs_and_the_time_bound()
    {
        var mesh = InMemoryExchangeTransport.CreateMesh(2);
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false,
            Cluster = new ClusterOptions { Transport = mesh[0] },
        });
        var numbers = builder.Input<int>("numbers");
        Assert.Throws<NotSupportedException>(() => numbers.ToAcknowledgedOutput());

        Assert.Throws<NotSupportedException>(() => new CircuitBuilder(new CircuitOptions
        {
            AutoStep = false,
            Cluster = new ClusterOptions { Transport = mesh[1] },
            CheckpointMaxDelay = TimeSpan.FromSeconds(1),
        }));
    }

    [Fact]
    public void Options_and_names_are_validated()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new CircuitBuilder(new CircuitOptions { CheckpointMaxDelay = TimeSpan.Zero }));

        // The step sequence commits beside the offsets under this key.
        var builder = new CircuitBuilder();
        builder.Input<int>("$step");
        Assert.Throws<ArgumentException>(() => builder.Build());
    }

    [Fact]
    public async Task Checkpoint_without_a_step_sequence_is_refused()
    {
        // A format-5 checkpoint: no `$step`, so resuming it would restart the sequence
        // below what the sinks have already seen.
        var storage = new InMemoryCheckpointStorageProvider();
        storage.StageManifest("local", "format=5\nworkers=1\nprocesses=1\n");
        storage.Commit(new Dictionary<string, long> { ["orders"] = 41 });

        var pipeline = CountByRegion(storage);
        var circuit = pipeline.Builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        var refusal = Assert.Throws<CircuitStateMismatchException>(circuit.Start);
        Assert.Contains("format", refusal.Message);
    }
}
