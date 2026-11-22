using System.Diagnostics;
using Dbsp.Storage;

namespace Dbsp.Tests;

/// <summary>
/// Sinks: an acknowledged output delivered to an <see cref="IUpsertSink{TKey,TRow}"/> as at
/// most one upsert or delete per key per step, or to an <see cref="IAppendSink{T}"/> with the
/// step's sequence, at-least-once. "Crash" is abandoning a circuit without disposing it and
/// cancelling its delivery, as in <see cref="CommitAfterDeliveryTests"/>. Every wait is
/// fenced with a timeout.
/// </summary>
public class SinkDeliveryTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Settle = TimeSpan.FromMilliseconds(100);

    private sealed record Order(int Id, string Region, long Amount);

    private sealed record Pipeline(
        CircuitBuilder Builder,
        KeyedInputStream<int, Order> Orders,
        AcknowledgedOutput<KeyedValue<string, long>> Totals);

    /// <summary>Orders upserted and deleted by id, totalled per region: a table-shaped
    /// result in which a region's row changes, appears, and — when its last order goes —
    /// disappears.</summary>
    private static Pipeline TotalsByRegion(
        IStorageProvider? storage = null, int workers = 1, bool autoStep = false, int interval = 1,
        int inputCapacity = 8192, int outputCapacity = 256, int maxStepItems = 8192)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = autoStep,
            Workers = workers,
            Storage = storage ?? new InMemoryStorageProvider(),
            CheckpointInterval = interval,
            InputChannelCapacity = inputCapacity,
            OutputChannelCapacity = outputCapacity,
            MaxStepItemsPerInput = maxStepItems,
        });
        var orders = builder.KeyedInput<int, Order>("orders", o => o.Id);
        var totals = orders.GroupBy(o => o.Region).Sum(o => o.Amount).ToAcknowledgedOutput();
        return new Pipeline(builder, orders, totals);
    }

    /// <summary>One source command: an upsert of <see cref="Row"/>, or a delete of
    /// <see cref="Id"/> when it is null. Its offset is its index in the source.</summary>
    private readonly record struct Command(int Id, Order? Row)
    {
        public static Command Upsert(int id, string region, long amount) => new(id, new Order(id, region, amount));
        public static Command Delete(int id) => new(id, null);
    }

    /// <summary>The source, in steps. Regions appear, change, move and vanish: "amer" exists
    /// only in the middle, so the sink has to be told to delete it.</summary>
    private static readonly Command[][] Steps =
    [
        [Command.Upsert(1, "emea", 10), Command.Upsert(2, "apac", 5)],
        [Command.Upsert(3, "amer", 7), Command.Upsert(1, "emea", 12), Command.Upsert(4, "emea", 1)],
        [Command.Delete(3), Command.Upsert(2, "emea", 5)],
        [Command.Upsert(5, "apac", 9), Command.Delete(4)],
    ];

    /// <summary>What the sink must hold after the whole source, worked out with no circuit.</summary>
    private static Dictionary<string, long> Model(IEnumerable<Command> commands)
    {
        var orders = new Dictionary<int, Order>();
        foreach (var command in commands)
        {
            if (command.Row is { } row)
                orders[command.Id] = row;
            else
                orders.Remove(command.Id);
        }
        return orders.Values.GroupBy(o => o.Region).ToDictionary(g => g.Key, g => g.Sum(o => o.Amount));
    }

    /// <summary>Sends one step's commands, stamped with their offsets, skipping what the
    /// committed offset says is already in the checkpoint. Returns whether anything was sent.</summary>
    private static async Task<bool> SendStepAsync(KeyedInputHandle<int, Order> input, int step, long? committed)
    {
        var offset = Steps.Take(step).Sum(s => s.Length);
        var sent = false;
        foreach (var command in Steps[step])
        {
            if (offset > (committed ?? -1))
            {
                if (command.Row is { } row)
                    await input.UpsertAsync(row, offset);
                else
                    await input.DeleteAsync(command.Id, offset);
                sent = true;
            }
            offset++;
        }
        return sent;
    }

    /// <summary>Wraps a sink: records what each write was handed, and can hold a write
    /// before or after it reaches the inner sink, or fail it.</summary>
    private sealed class ControlledSink<TKey, TRow>(IUpsertSink<TKey, TRow> inner) : IUpsertSink<TKey, TRow>
        where TKey : notnull
    {
        public List<(long Sequence, Dictionary<TKey, TRow> Upserts, HashSet<TKey> Deletes)> Writes { get; } = [];
        public Func<long, bool>? HoldBefore { get; set; }
        public Func<long, bool>? HoldAfter { get; set; }
        public Func<int, Exception?>? Fail { get; set; } // by attempt number, 1-based
        public TaskCompletionSource Held { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _attempts;

        public int Attempts => Volatile.Read(ref _attempts);

        public async ValueTask WriteAsync(UpsertBatch<TKey, TRow> batch, CancellationToken cancellationToken)
        {
            var attempt = Interlocked.Increment(ref _attempts);
            if (Fail?.Invoke(attempt) is { } failure)
                throw failure;
            if (HoldBefore?.Invoke(batch.Sequence) == true)
            {
                Held.TrySetResult();
                await Release.Task.WaitAsync(cancellationToken);
            }
            lock (Writes)
                Writes.Add((batch.Sequence, new Dictionary<TKey, TRow>(batch.Upserts), [.. batch.Deletes]));
            await inner.WriteAsync(batch, cancellationToken);
            if (HoldAfter?.Invoke(batch.Sequence) == true)
            {
                Held.TrySetResult();
                await Release.Task.WaitAsync(cancellationToken);
            }
        }
    }

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

    private sealed class FlakyCommitProvider(InMemoryCheckpointStorageProvider inner) : ICheckpointStorageProvider
    {
        public bool FailCommits { get; set; }

        public IWeightStore<T> CreateWeightStore<T>(string name) where T : notnull => inner.CreateWeightStore<T>(name);

        public IIndexedWeightStore<TKey, TValue> CreateIndexedWeightStore<TKey, TValue>(string name)
            where TKey : notnull
            where TValue : notnull => inner.CreateIndexedWeightStore<TKey, TValue>(name);

        public IValueStore<TKey, TValue> CreateValueStore<TKey, TValue>(string name) where TKey : notnull =>
            inner.CreateValueStore<TKey, TValue>(name);

        public IReadOnlyDictionary<string, long> Recover() => inner.Recover();

        public void Commit(IReadOnlyDictionary<string, long> inputOffsets)
        {
            if (FailCommits)
                throw new IOException("commit failed");
            inner.Commit(inputOffsets);
        }

        public string? CommittedManifest(string scope) => inner.CommittedManifest(scope);

        public void StageManifest(string scope, string manifest) => inner.StageManifest(scope, manifest);
    }

    // ---- the collapse ----

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task Each_step_is_delivered_as_at_most_one_upsert_or_delete_per_key(int workers)
    {
        var pipeline = TotalsByRegion(workers: workers);
        var circuit = pipeline.Builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();
        var input = circuit.Handle(pipeline.Orders);
        var memory = new InMemoryUpsertSink<string, long>();
        var sink = new ControlledSink<string, long>(memory);
        var delivery = circuit.Handle(pipeline.Totals).DeliverToAsync(sink);

        for (var step = 0; step < Steps.Length; step++)
        {
            await SendStepAsync(input, step, committed: null);
            await circuit.StepAsync().WaitAsync(Timeout);
        }
        await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
        await delivery.WaitAsync(Timeout);

        Assert.Equal(4, sink.Writes.Count);
        Assert.Equal([1L, 2L, 3L, 4L], sink.Writes.Select(w => w.Sequence));

        // Step 1: two regions appear.
        Assert.Equal(new Dictionary<string, long> { ["emea"] = 10, ["apac"] = 5 }, sink.Writes[0].Upserts);
        Assert.Empty(sink.Writes[0].Deletes);

        // Step 2: emea's row is retracted and re-asserted by the aggregate (10 → 13) — one
        // upsert, not a delete and an upsert — and amer appears.
        Assert.Equal(new Dictionary<string, long> { ["emea"] = 13, ["amer"] = 7 }, sink.Writes[1].Upserts);
        Assert.Empty(sink.Writes[1].Deletes);

        // Step 3: amer's only order goes, so the region is deleted; apac's order moves to
        // emea, which deletes apac too.
        Assert.Equal(new Dictionary<string, long> { ["emea"] = 18 }, sink.Writes[2].Upserts);
        Assert.Equal(["amer", "apac"], sink.Writes[2].Deletes.Order());

        // Step 4: apac is back; emea loses an order.
        Assert.Equal(new Dictionary<string, long> { ["apac"] = 9, ["emea"] = 17 }, sink.Writes[3].Upserts);
        Assert.Empty(sink.Writes[3].Deletes);

        Assert.Equal(Model(Steps.SelectMany(s => s)), memory.Rows);
        Assert.Equal(4, memory.Batches);
        Assert.Equal(4, memory.LastSequence);
        Assert.Equal(0, memory.Redeliveries);
    }

    [Fact]
    public async Task Key_selector_overload_keys_a_stream_of_plain_rows()
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false });
        var orders = builder.KeyedInput<int, Order>("orders", o => o.Id);
        var output = orders.ToAcknowledgedOutput();
        var circuit = builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();
        var input = circuit.Handle(orders);
        var sink = new InMemoryUpsertSink<int, Order>();
        var delivery = circuit.Handle(output).DeliverToAsync(sink, o => o.Id);

        await input.UpsertAsync(new Order(1, "emea", 10));
        await input.UpsertAsync(new Order(2, "apac", 5));
        await circuit.StepAsync().WaitAsync(Timeout);
        await input.UpsertAsync(new Order(1, "emea", 11)); // old row retracted, new one asserted
        await input.DeleteAsync(2);
        await circuit.StepAsync().WaitAsync(Timeout);
        await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
        await delivery.WaitAsync(Timeout);

        Assert.Equal(new Dictionary<int, Order> { [1] = new(1, "emea", 11) }, sink.Rows);
    }

    [Theory]
    [InlineData(false)] // two different rows under one key
    [InlineData(true)]  // the same row twice
    public async Task Key_left_with_more_than_one_live_row_faults_the_circuit(bool sameRow)
    {
        var builder = new CircuitBuilder(new CircuitOptions { AutoStep = false, Storage = new InMemoryCheckpointStorageProvider() });
        var orders = builder.Input<Order>("orders");
        var output = orders.ToAcknowledgedOutput();
        var circuit = builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();
        var sink = new InMemoryUpsertSink<int, Order>();
        var delivery = circuit.Handle(output).DeliverToAsync(sink, o => o.Id);

        await circuit.Handle(orders).InsertAsync(new Order(1, "emea", 10));
        await circuit.Handle(orders).InsertAsync(sameRow ? new Order(1, "emea", 10) : new Order(1, "apac", 3));
        var step = circuit.StepAsync();

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => delivery).WaitAsync(Timeout);
        Assert.Contains("more than one live row", failure.Message);
        await Assert.ThrowsAnyAsync<Exception>(() => step).WaitAsync(Timeout);
        Assert.NotNull(circuit.Fault);
        Assert.Empty(sink.Rows);
        Assert.Null(circuit.CommittedStep);
    }

    private sealed class LastBatchSink<TKey, TRow> : IUpsertSink<TKey, TRow> where TKey : notnull
    {
        public Dictionary<TKey, TRow> Upserts { get; private set; } = [];
        public List<TKey> Deletes { get; private set; } = [];

        public ValueTask WriteAsync(UpsertBatch<TKey, TRow> batch, CancellationToken cancellationToken)
        {
            Upserts = new Dictionary<TKey, TRow>(batch.Upserts);
            Deletes = [.. batch.Deletes];
            return ValueTask.CompletedTask;
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Collapse_takes_the_asserted_row_wherever_the_retraction_falls_in_the_delta(bool retractionFirst)
    {
        // A Z-set has no order, so the collapse must not depend on which of a key's two
        // entries it meets last.
        var sink = new LastBatchSink<int, Order>();
        var deliver = SinkDelivery.Upserts<Order, int>(sink, o => o.Id);
        var changes = new ZSet<Order>();
        if (retractionFirst)
            changes.Add(new Order(1, "emea", 10), -1);
        changes.Add(new Order(1, "emea", 11), 1);
        changes.Add(new Order(2, "apac", 5), -1);
        if (!retractionFirst)
            changes.Add(new Order(1, "emea", 10), -1);

        await deliver(new OutputBatch<Order>(null!, 7, changes), default);

        Assert.Equal(new Dictionary<int, Order> { [1] = new(1, "emea", 11) }, sink.Upserts);
        Assert.Equal([2], sink.Deletes);
    }

    [Fact]
    public async Task Null_key_from_the_key_selector_is_refused()
    {
        var sink = new LastBatchSink<string, Order>();
        var deliver = SinkDelivery.Upserts<Order, string>(sink, o => o.Region == "none" ? null! : o.Region);
        var changes = new ZSet<Order> { { new Order(1, "none", 10), 1 } };

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await deliver(new OutputBatch<Order>(null!, 1, changes), default));
        Assert.Contains("returned null", failure.Message);
        Assert.Empty(sink.Upserts);
    }

    // ---- crash and restart ----

    public enum CrashPoint
    {
        BeforeDelivery,
        AfterDeliveryBeforeAcknowledgement,
        AfterAcknowledgementBeforeCommit,
        AfterCommit,
    }

    public static TheoryData<CrashPoint, int, int> CrashCases()
    {
        var cases = new TheoryData<CrashPoint, int, int>();
        foreach (var crashPoint in Enum.GetValues<CrashPoint>())
            foreach (var workers in new[] { 1, 4 })
                foreach (var crashStep in new[] { 2, 3 })
                    cases.Add(crashPoint, workers, crashStep);
        return cases;
    }

    [Theory]
    [MemberData(nameof(CrashCases))]
    public async Task Crash_at_any_point_leaves_the_sink_equal_to_the_model(CrashPoint crashPoint, int workers, int crashStep)
    {
        var model = Model(Steps.SelectMany(s => s));
        var storage = new FlakyCommitProvider(new InMemoryCheckpointStorageProvider());
        var memory = new InMemoryUpsertSink<string, long>();

        // The run that crashes during step `crashStep` (1-based). Earlier steps are
        // delivered, acknowledged and committed.
        {
            var pipeline = TotalsByRegion(storage, workers);
            var crashed = pipeline.Builder.Build();
            crashed.Start();
            var input = crashed.Handle(pipeline.Orders);
            var sink = new ControlledSink<string, long>(memory)
            {
                HoldBefore = sequence => crashPoint == CrashPoint.BeforeDelivery && sequence == crashStep,
                HoldAfter = sequence => crashPoint == CrashPoint.AfterDeliveryBeforeAcknowledgement && sequence == crashStep,
            };
            using var death = new CancellationTokenSource();
            var delivery = crashed.Handle(pipeline.Totals).DeliverToAsync(sink, cancellationToken: death.Token);

            for (var step = 0; step < crashStep - 1; step++)
            {
                await SendStepAsync(input, step, committed: null);
                await crashed.StepAsync().WaitAsync(Timeout);
            }
            var committedBefore = crashed.CommittedOffset("orders");
            Assert.Equal(crashStep - 1L, crashed.CommittedStep);

            await SendStepAsync(input, crashStep - 1, committed: null);
            storage.FailCommits = crashPoint == CrashPoint.AfterAcknowledgementBeforeCommit;
            var last = crashed.StepAsync();
            switch (crashPoint)
            {
                case CrashPoint.BeforeDelivery:
                case CrashPoint.AfterDeliveryBeforeAcknowledgement:
                    await sink.Held.Task.WaitAsync(Timeout);
                    await Task.Delay(Settle);
                    Assert.False(last.IsCompleted, "The step committed while its batch was still with the sink.");
                    break;
                case CrashPoint.AfterAcknowledgementBeforeCommit:
                    await Assert.ThrowsAsync<IOException>(() => last).WaitAsync(Timeout);
                    break;
                case CrashPoint.AfterCommit:
                    await last.WaitAsync(Timeout);
                    break;
            }

            // What the sink holds at the moment of the crash, and what the checkpoint says.
            Assert.Equal(crashPoint == CrashPoint.BeforeDelivery ? crashStep - 1 : crashStep, memory.Batches);
            if (crashPoint == CrashPoint.AfterCommit)
                Assert.Equal((long)crashStep, crashed.CommittedStep);
            else
            {
                Assert.Equal(crashStep - 1L, crashed.CommittedStep);
                Assert.Equal(committedBefore, crashed.CommittedOffset("orders"));
            }

            death.Cancel(); // the process is gone: its delivery with it
            await Assert.ThrowsAnyAsync<Exception>(() => delivery).WaitAsync(Timeout);
            storage.FailCommits = false;
        }

        // The restart: same provider, same sink, the producer replays from the committed offset.
        {
            var pipeline = TotalsByRegion(storage, workers);
            var resumed = pipeline.Builder.Build();
            resumed.Start();
            var committed = resumed.CommittedOffset("orders");
            var delivery = resumed.Handle(pipeline.Totals).DeliverToAsync(memory);
            for (var step = 0; step < Steps.Length; step++)
                if (await SendStepAsync(resumed.Handle(pipeline.Orders), step, committed))
                    await resumed.StepAsync().WaitAsync(Timeout);
            await resumed.DisposeAsync().AsTask().WaitAsync(Timeout);
            await delivery.WaitAsync(Timeout);
            Assert.Equal(Steps.Sum(s => s.Length) - 1L, resumed.CommittedOffset("orders"));
        }

        Assert.Equal(model, memory.Rows);

        // The crashed step's batch reached the sink twice exactly when the sink had it and
        // the checkpoint did not; applying it again changed nothing.
        var deliveredTwice = crashPoint is CrashPoint.AfterDeliveryBeforeAcknowledgement or CrashPoint.AfterAcknowledgementBeforeCommit;
        Assert.Equal(deliveredTwice ? 1 : 0, memory.Redeliveries);
        Assert.Equal(Steps.Length + (deliveredTwice ? 1 : 0), memory.Batches);
        Assert.Equal(Steps.Length, memory.LastSequence);
    }

    [Fact]
    public async Task Redelivering_every_batch_is_harmless()
    {
        // One run's batches, captured, then applied to a second sink twice over and out of
        // step with themselves: first all of them, then all of them again.
        var pipeline = TotalsByRegion();
        var circuit = pipeline.Builder.Build();
        circuit.Start();
        var input = circuit.Handle(pipeline.Orders);
        var once = new InMemoryUpsertSink<string, long>();
        var twice = new InMemoryUpsertSink<string, long>();
        var batches = new List<UpsertBatch<string, long>>();
        var capture = new ControlledSink<string, long>(once);
        var delivery = circuit.Handle(pipeline.Totals).DeliverToAsync(capture);
        for (var step = 0; step < Steps.Length; step++)
        {
            await SendStepAsync(input, step, committed: null);
            await circuit.StepAsync().WaitAsync(Timeout);
        }
        await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
        await delivery.WaitAsync(Timeout);

        for (var pass = 0; pass < 2; pass++)
            foreach (var (sequence, upserts, deletes) in capture.Writes)
                await twice.WriteAsync(new UpsertBatch<string, long>(sequence, [.. upserts], [.. deletes]), default);

        Assert.Equal(once.Rows, twice.Rows);
        Assert.Equal(Model(Steps.SelectMany(s => s)), twice.Rows);
        Assert.Equal(Steps.Length, twice.Redeliveries);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task Auto_stepping_crash_with_a_stalled_sink_replays_to_the_model(int workers)
    {
        // Keys here persist once they appear, which is the case an auto-stepping replay
        // converges on whatever steps it cuts (see SinkDelivery for the case it does not).
        const int orders = 400;
        Order OrderAt(int i) => new(i % 40, $"r{i % 7}", i);
        var commands = Enumerable.Range(0, orders).Select(i => new Command(i % 40, OrderAt(i))).ToArray();
        var model = Model(commands);

        var storage = new InMemoryCheckpointStorageProvider();
        var memory = new InMemoryUpsertSink<string, long>();
        {
            var pipeline = TotalsByRegion(storage, workers, autoStep: true, maxStepItems: 16);
            var crashed = pipeline.Builder.Build();
            crashed.Start();
            var sink = new ControlledSink<string, long>(memory) { HoldAfter = sequence => sequence >= 5 };
            using var death = new CancellationTokenSource();
            var delivery = crashed.Handle(pipeline.Totals).DeliverToAsync(sink, cancellationToken: death.Token);
            var input = crashed.Handle(pipeline.Orders);
            for (var i = 0; i < 200; i++)
                await input.UpsertAsync(OrderAt(i), offset: i).AsTask().WaitAsync(Timeout);
            await sink.Held.Task.WaitAsync(Timeout);
            await Task.Delay(Settle);
            Assert.True((crashed.CommittedStep ?? 0) < 5, "A step committed while the sink still held its batch.");
            death.Cancel();
            await Assert.ThrowsAnyAsync<Exception>(() => delivery).WaitAsync(Timeout);
        }
        {
            var pipeline = TotalsByRegion(storage, workers, autoStep: true, maxStepItems: 64);
            var resumed = pipeline.Builder.Build();
            resumed.Start();
            var committed = resumed.CommittedOffset("orders") ?? -1;
            Assert.InRange(committed, 0, 198);
            var delivery = resumed.Handle(pipeline.Totals).DeliverToAsync(memory);
            var input = resumed.Handle(pipeline.Orders);
            for (var i = (int)committed + 1; i < orders; i++)
                await input.UpsertAsync(OrderAt(i), offset: i).AsTask().WaitAsync(Timeout);
            await resumed.DisposeAsync().AsTask().WaitAsync(Timeout);
            await delivery.WaitAsync(Timeout);
            Assert.Equal(orders - 1L, resumed.CommittedOffset("orders"));
        }
        Assert.Equal(model, memory.Rows);
    }

    // ---- failure policy ----

    [Fact]
    public async Task Failed_write_is_retried_with_the_same_batch_until_it_succeeds()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var pipeline = TotalsByRegion(storage);
        var circuit = pipeline.Builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();
        var memory = new InMemoryUpsertSink<string, long>();
        var sink = new ControlledSink<string, long>(memory)
        {
            Fail = attempt => attempt <= 3 ? new IOException($"sink down {attempt}") : null,
        };
        var failures = new List<(string Message, int Attempt)>();
        var delivery = circuit.Handle(pipeline.Totals).DeliverToAsync(sink, new SinkDeliveryOptions
        {
            MaxRetries = 3,
            InitialBackoff = TimeSpan.FromMilliseconds(1),
            MaxBackoff = TimeSpan.FromMilliseconds(2),
            OnFailedAttempt = (ex, attempt) => failures.Add((ex.Message, attempt)),
        });

        await SendStepAsync(circuit.Handle(pipeline.Orders), 0, committed: null);
        await circuit.StepAsync().WaitAsync(Timeout);

        Assert.Equal(4, sink.Attempts);
        Assert.Equal([("sink down 1", 1), ("sink down 2", 2), ("sink down 3", 3)], failures);
        Assert.Equal(new Dictionary<string, long> { ["emea"] = 10, ["apac"] = 5 }, memory.Rows);
        Assert.Equal(1, memory.Batches);
        Assert.Equal(1L, circuit.CommittedStep);
        Assert.Null(circuit.Fault);
        Assert.False(delivery.IsCompleted);
    }

    [Fact]
    public async Task Checkpoint_does_not_pass_a_batch_the_sink_is_still_failing()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var pipeline = TotalsByRegion(storage);
        var circuit = pipeline.Builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();
        var healthy = false;
        var sink = new ControlledSink<string, long>(new InMemoryUpsertSink<string, long>())
        {
            Fail = _ => Volatile.Read(ref healthy) ? null : new IOException("sink down"),
        };
        var delivery = circuit.Handle(pipeline.Totals).DeliverToAsync(sink, new SinkDeliveryOptions
        {
            MaxRetries = null, // a sink that is down stalls the pipeline; it does not fault it
            InitialBackoff = TimeSpan.FromMilliseconds(1),
            MaxBackoff = TimeSpan.FromMilliseconds(1),
        });

        await SendStepAsync(circuit.Handle(pipeline.Orders), 0, committed: null);
        var step = circuit.StepAsync();
        await Eventually(() => sink.Attempts >= 25);
        Assert.False(step.IsCompleted);
        Assert.Null(circuit.CommittedStep);
        Assert.Null(circuit.Fault);

        Volatile.Write(ref healthy, true);
        await step.WaitAsync(Timeout);
        Assert.Equal(1L, circuit.CommittedStep);
        Assert.False(delivery.IsCompleted);
    }

    [Fact]
    public async Task Backoff_doubles_and_is_capped()
    {
        var pipeline = TotalsByRegion();
        var circuit = pipeline.Builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();
        var attempts = new List<long>();
        var sink = new ControlledSink<string, long>(new InMemoryUpsertSink<string, long>())
        {
            Fail = attempt =>
            {
                lock (attempts)
                    attempts.Add(Stopwatch.GetTimestamp());
                return attempt <= 6 ? new IOException("sink down") : null;
            },
        };
        _ = circuit.Handle(pipeline.Totals).DeliverToAsync(sink, new SinkDeliveryOptions
        {
            MaxRetries = 6,
            InitialBackoff = TimeSpan.FromMilliseconds(20),
            MaxBackoff = TimeSpan.FromMilliseconds(40),
        });

        await SendStepAsync(circuit.Handle(pipeline.Orders), 0, committed: null);
        await circuit.StepAsync().WaitAsync(Timeout);
        await Eventually(() => sink.Attempts == 7);

        // Waits of 20, 40, 40, 40, 40, 40 ms: 220 in all. Uncapped doubling would be 1260,
        // and no backoff at all would be nothing.
        Assert.Equal(7, attempts.Count);
        var gaps = attempts.Zip(attempts.Skip(1), (a, b) => Stopwatch.GetElapsedTime(a, b).TotalMilliseconds).ToArray();
        Assert.All(gaps, gap => Assert.InRange(gap, 15, 600));
        Assert.True(gaps[1] >= 35, $"The second wait should have doubled: {gaps[1]} ms.");
        Assert.InRange(gaps.Sum(), 200, 900);
    }

    [Fact]
    public async Task Retries_exhausted_fault_the_circuit_without_committing_and_the_restart_redelivers()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var memory = new InMemoryUpsertSink<string, long>();
        {
            var pipeline = TotalsByRegion(storage);
            var circuit = pipeline.Builder.Build();
            circuit.Start();
            var input = circuit.Handle(pipeline.Orders);
            // Healthy for step 1 (attempt 1), then down for good.
            var sink = new ControlledSink<string, long>(memory)
            {
                Fail = attempt => attempt > 1 ? new IOException("sink down") : null,
            };
            var delivery = circuit.Handle(pipeline.Totals).DeliverToAsync(sink, new SinkDeliveryOptions
            {
                MaxRetries = 2,
                InitialBackoff = TimeSpan.FromMilliseconds(1),
                MaxBackoff = TimeSpan.FromMilliseconds(1),
            });

            await SendStepAsync(input, 0, committed: null);
            await circuit.StepAsync().WaitAsync(Timeout);
            await SendStepAsync(input, 1, committed: null);
            var step = circuit.StepAsync();

            var failure = await Assert.ThrowsAsync<SinkDeliveryException>(() => delivery).WaitAsync(Timeout);
            Assert.Equal(3, failure.Attempts);
            Assert.IsType<IOException>(failure.InnerException);
            Assert.Equal(4, sink.Attempts); // one for step 1, three for step 2

            await Assert.ThrowsAnyAsync<Exception>(() => step).WaitAsync(Timeout);
            Assert.NotNull(circuit.Fault);
            Assert.Same(failure, circuit.Fault!.InnerException);
            Assert.Equal(1L, circuit.CommittedStep);
            await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
        }
        {
            var pipeline = TotalsByRegion(storage);
            var resumed = pipeline.Builder.Build();
            resumed.Start();
            Assert.Equal(1L, resumed.CommittedStep);
            var committed = resumed.CommittedOffset("orders");
            var delivery = resumed.Handle(pipeline.Totals).DeliverToAsync(memory);
            for (var step = 0; step < Steps.Length; step++)
                if (await SendStepAsync(resumed.Handle(pipeline.Orders), step, committed))
                    await resumed.StepAsync().WaitAsync(Timeout);
            await resumed.DisposeAsync().AsTask().WaitAsync(Timeout);
            await delivery.WaitAsync(Timeout);
        }
        Assert.Equal(Model(Steps.SelectMany(s => s)), memory.Rows);
        Assert.Equal(0, memory.Redeliveries); // step 2 never reached the sink the first time
    }

    [Fact]
    public async Task Cancellation_is_not_retried()
    {
        var pipeline = TotalsByRegion();
        var circuit = pipeline.Builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();
        var sink = new ControlledSink<string, long>(new InMemoryUpsertSink<string, long>()) { HoldBefore = _ => true };
        using var stop = new CancellationTokenSource();
        var failed = 0;
        var delivery = circuit.Handle(pipeline.Totals).DeliverToAsync(
            sink, new SinkDeliveryOptions { MaxRetries = null, OnFailedAttempt = (_, _) => failed++ }, stop.Token);

        await SendStepAsync(circuit.Handle(pipeline.Orders), 0, committed: null);
        var step = circuit.StepAsync();
        await sink.Held.Task.WaitAsync(Timeout);
        stop.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => delivery).WaitAsync(Timeout);
        Assert.Equal(1, sink.Attempts);
        Assert.Equal(0, failed); // a cancelled write is not a failed attempt
        await step.WaitAsync(Timeout); // no checkpointing provider: nothing waits on the sink
    }

    [Fact]
    public void Options_are_validated()
    {
        var sink = new InMemoryUpsertSink<string, long>();
        Assert.Throws<ArgumentOutOfRangeException>(() => SinkDelivery.Upserts(sink, new SinkDeliveryOptions { MaxRetries = -1 }));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            SinkDelivery.Upserts(sink, new SinkDeliveryOptions { InitialBackoff = TimeSpan.FromSeconds(-1) }));
        Assert.Throws<ArgumentOutOfRangeException>(() => SinkDelivery.Upserts(
            sink, new SinkDeliveryOptions { InitialBackoff = TimeSpan.FromSeconds(2), MaxBackoff = TimeSpan.FromSeconds(1) }));
        Assert.Throws<ArgumentNullException>(() => SinkDelivery.Upserts<string, long>(null!));
        Assert.Throws<ArgumentNullException>(() => SinkDelivery.Appends<string>(null!));
    }

    // ---- backpressure ----

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Slow_sink_backpressures_the_producer_and_buffers_nothing_beyond_the_channels(bool checkpointing)
    {
        // One order per step, one batch in the output channel, eight records in the input
        // channel. With the sink stuck on its first batch, at most: one batch with the sink,
        // one in the channel, one staged behind it, one step's record in the workers, and
        // the input channel's eight.
        var pipeline = TotalsByRegion(
            checkpointing ? new InMemoryCheckpointStorageProvider() : null,
            autoStep: true, inputCapacity: 8, outputCapacity: 1, maxStepItems: 1);
        var circuit = pipeline.Builder.Build();
        circuit.Start();
        var input = circuit.Handle(pipeline.Orders);
        var memory = new InMemoryUpsertSink<string, long>();
        var sink = new ControlledSink<string, long>(memory) { HoldBefore = _ => true };
        var delivery = circuit.Handle(pipeline.Totals).DeliverToAsync(sink);

        const int total = 200;
        var sent = 0;
        var producer = Task.Run(async () =>
        {
            for (var i = 0; i < total; i++)
            {
                await input.UpsertAsync(new Order(i, $"r{i % 5}", 1));
                Volatile.Write(ref sent, i + 1);
            }
        });

        await sink.Held.Task.WaitAsync(Timeout);
        await Task.Delay(Settle * 3);
        var stalledAt = Volatile.Read(ref sent);
        Assert.False(producer.IsCompleted, "The producer ran to the end past a sink that never returned.");
        Assert.InRange(stalledAt, 1, 8 + 5);
        Assert.Equal(1, sink.Attempts); // the delivery holds one batch and reads no further
        await Task.Delay(Settle);
        Assert.Equal(stalledAt, Volatile.Read(ref sent));

        sink.HoldBefore = null;
        sink.Release.SetResult();
        await producer.WaitAsync(Timeout);
        await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
        await delivery.WaitAsync(Timeout);
        Assert.Equal(Enumerable.Range(0, 5).ToDictionary(r => $"r{r}", _ => (long)(total / 5)), memory.Rows);
    }

    // ---- append sinks ----

    private sealed record Event(int Id, string Kind);

    private static (CircuitBuilder Builder, InputStream<Event> Events, AcknowledgedOutput<string> Lines) Lines(
        IStorageProvider? storage = null, bool autoStep = false, int workers = 1, int maxStepItems = 8192)
    {
        var builder = new CircuitBuilder(new CircuitOptions
        {
            AutoStep = autoStep,
            Workers = workers,
            Storage = storage ?? new InMemoryStorageProvider(),
            MaxStepItemsPerInput = maxStepItems,
        });
        var events = builder.Input<Event>("events");
        var lines = events.Where(e => e.Kind != "noise").Select(e => $"{e.Id}:{e.Kind}").ToAcknowledgedOutput();
        return (builder, events, lines);
    }

    private sealed class RecordingAppendSink<T>(IAppendSink<T> inner) : IAppendSink<T>
    {
        public List<(long Sequence, List<T> Records)> Writes { get; } = [];
        public Func<long, bool>? HoldAfter { get; set; }
        public TaskCompletionSource Held { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async ValueTask WriteAsync(AppendBatch<T> batch, CancellationToken cancellationToken)
        {
            lock (Writes)
                Writes.Add((batch.Sequence, [.. batch.Records]));
            await inner.WriteAsync(batch, cancellationToken);
            if (HoldAfter?.Invoke(batch.Sequence) == true)
            {
                Held.TrySetResult();
                await Task.Delay(System.Threading.Timeout.Infinite, cancellationToken);
            }
        }
    }

    [Fact]
    public async Task Append_sink_receives_each_steps_records_with_its_sequence()
    {
        var (builder, eventsDef, linesDef) = Lines();
        var circuit = builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();
        var events = circuit.Handle(eventsDef);
        var memory = new InMemoryAppendSink<string>();
        var sink = new RecordingAppendSink<string>(memory);
        var delivery = circuit.Handle(linesDef).DeliverToAsync(sink);

        await events.InsertAsync(new Event(1, "open"));
        await events.SendAsync(new Event(2, "tick"), 2); // inserted twice: appended twice
        await circuit.StepAsync().WaitAsync(Timeout);
        await events.InsertAsync(new Event(3, "noise"));  // filtered: the step delivers nothing
        await circuit.StepAsync().WaitAsync(Timeout);
        await events.InsertAsync(new Event(4, "close"));
        await circuit.StepAsync().WaitAsync(Timeout);
        await circuit.DisposeAsync().AsTask().WaitAsync(Timeout);
        await delivery.WaitAsync(Timeout);

        Assert.Equal([1L, 3L], sink.Writes.Select(w => w.Sequence));
        Assert.Equal(["1:open", "2:tick", "2:tick"], sink.Writes[0].Records.Order());
        Assert.Equal(["4:close"], sink.Writes[1].Records);
        Assert.Equal(["1:open", "2:tick", "2:tick", "4:close"], memory.Records.Order());
        Assert.Equal(3, memory.LastSequence);
        Assert.Equal(2, memory.Batches);
    }

    [Fact]
    public async Task Retraction_on_an_append_sink_faults_the_circuit()
    {
        var (builder, eventsDef, linesDef) = Lines(new InMemoryCheckpointStorageProvider());
        var circuit = builder.Build();
        await using var cleanup = new BoundedDisposal(circuit);
        circuit.Start();
        var memory = new InMemoryAppendSink<string>();
        var delivery = circuit.Handle(linesDef).DeliverToAsync(memory);

        await circuit.Handle(eventsDef).RemoveAsync(new Event(1, "open"));
        var step = circuit.StepAsync();

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(() => delivery).WaitAsync(Timeout);
        Assert.Contains("retraction", failure.Message);
        await Assert.ThrowsAnyAsync<Exception>(() => step).WaitAsync(Timeout);
        Assert.NotNull(circuit.Fault);
        Assert.Empty(memory.Records);
    }

    [Fact]
    public async Task Redelivered_append_batch_is_recognised_by_its_sequence_and_not_appended_twice()
    {
        var storage = new InMemoryCheckpointStorageProvider();
        var memory = new InMemoryAppendSink<string>();
        Event[][] steps = [[new(1, "open"), new(2, "tick")], [new(3, "tick"), new(4, "close")], [new(5, "open")]];
        {
            var (builder, eventsDef, linesDef) = Lines(storage);
            var crashed = builder.Build();
            crashed.Start();
            var sink = new RecordingAppendSink<string>(memory) { HoldAfter = sequence => sequence == 2 };
            using var death = new CancellationTokenSource();
            var delivery = crashed.Handle(linesDef).DeliverToAsync(sink, cancellationToken: death.Token);

            foreach (var e in steps[0])
                await crashed.Handle(eventsDef).InsertAsync(e, offset: e.Id);
            await crashed.StepAsync().WaitAsync(Timeout);
            foreach (var e in steps[1])
                await crashed.Handle(eventsDef).InsertAsync(e, offset: e.Id);
            _ = crashed.StepAsync();
            await sink.Held.Task.WaitAsync(Timeout); // written, never acknowledged
            Assert.Equal(1L, crashed.CommittedStep);
            death.Cancel();
            await Assert.ThrowsAnyAsync<Exception>(() => delivery).WaitAsync(Timeout);
        }
        Assert.Equal(["1:open", "2:tick", "3:tick", "4:close"], memory.Records.Order());
        {
            var (builder, eventsDef, linesDef) = Lines(storage);
            var resumed = builder.Build();
            resumed.Start();
            var committed = resumed.CommittedOffset("events");
            Assert.Equal(2L, committed);
            var delivery = resumed.Handle(linesDef).DeliverToAsync(memory);
            foreach (var step in steps)
            {
                var replayed = step.Where(e => e.Id > committed).ToArray();
                foreach (var e in replayed)
                    await resumed.Handle(eventsDef).InsertAsync(e, offset: e.Id);
                if (replayed.Length > 0)
                    await resumed.StepAsync().WaitAsync(Timeout);
            }
            await resumed.DisposeAsync().AsTask().WaitAsync(Timeout);
            await delivery.WaitAsync(Timeout);
        }

        Assert.Equal(["1:open", "2:tick", "3:tick", "4:close", "5:open"], memory.Records.Order());
        Assert.Equal(1, memory.Redeliveries);
        Assert.Equal(4, memory.Batches);
    }

    [Fact]
    public async Task In_memory_append_sink_truncates_to_a_sequence()
    {
        var sink = new InMemoryAppendSink<string>();
        await sink.WriteAsync(new AppendBatch<string>(1, ["a"]), default);
        await sink.WriteAsync(new AppendBatch<string>(2, ["b", "c"]), default);
        await sink.WriteAsync(new AppendBatch<string>(4, ["d"]), default);

        sink.TruncateAfter(2);
        Assert.Equal(["a", "b", "c"], sink.Records);
        Assert.Equal(2, sink.LastSequence);

        // What comes next is not a redelivery, whatever sequence the truncated tail had.
        await sink.WriteAsync(new AppendBatch<string>(3, ["e"]), default);
        Assert.Equal(["a", "b", "c", "e"], sink.Records);
        Assert.Equal(0, sink.Redeliveries);

        sink.TruncateAfter(0);
        Assert.Empty(sink.Records);
        Assert.Equal(0, sink.LastSequence);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task Auto_stepping_crash_appends_every_record_once_when_the_sink_truncates_to_the_committed_step(int workers)
    {
        // An auto-stepping replay cuts its steps where it likes, so a redelivered record
        // need not come back under the sequence it first had. The exact rule is the one the
        // checkpoint gives: on restart, drop what the sink holds above the committed step.
        const int total = 500;
        var storage = new InMemoryCheckpointStorageProvider();
        var memory = new InMemoryAppendSink<string>();
        {
            var (builder, eventsDef, linesDef) = Lines(storage, autoStep: true, workers, maxStepItems: 8);
            var crashed = builder.Build();
            crashed.Start();
            var sink = new RecordingAppendSink<string>(memory) { HoldAfter = sequence => sequence >= 7 };
            using var death = new CancellationTokenSource();
            var delivery = crashed.Handle(linesDef).DeliverToAsync(sink, cancellationToken: death.Token);
            for (var i = 0; i < 250; i++)
                await crashed.Handle(eventsDef).InsertAsync(new Event(i, i % 3 == 0 ? "noise" : "tick"), offset: i)
                    .AsTask().WaitAsync(Timeout);
            await sink.Held.Task.WaitAsync(Timeout);
            await Task.Delay(Settle);
            death.Cancel();
            await Assert.ThrowsAnyAsync<Exception>(() => delivery).WaitAsync(Timeout);
        }
        Assert.NotEmpty(memory.Records);
        {
            var (builder, eventsDef, linesDef) = Lines(storage, autoStep: true, workers, maxStepItems: 50);
            var resumed = builder.Build();
            resumed.Start();
            var committed = resumed.CommittedOffset("events") ?? -1;
            Assert.True(memory.LastSequence > (resumed.CommittedStep ?? 0), "The sink should be ahead of the checkpoint.");
            memory.TruncateAfter(resumed.CommittedStep ?? 0);
            var delivery = resumed.Handle(linesDef).DeliverToAsync(memory);
            for (var i = (int)committed + 1; i < total; i++)
                await resumed.Handle(eventsDef).InsertAsync(new Event(i, i % 3 == 0 ? "noise" : "tick"), offset: i)
                    .AsTask().WaitAsync(Timeout);
            await resumed.DisposeAsync().AsTask().WaitAsync(Timeout);
            await delivery.WaitAsync(Timeout);
        }

        var expected = Enumerable.Range(0, total).Where(i => i % 3 != 0).Select(i => $"{i}:tick").Order().ToArray();
        Assert.Equal(expected, memory.Records.Order());
    }
}
