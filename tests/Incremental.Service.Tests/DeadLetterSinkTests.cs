using System.Text.Json;
using Acme.Contracts;
using static Incremental.Service.Tests.SupervisorTests;
using static Incremental.Service.Tests.TestService;

namespace Incremental.Service.Tests;

/// <summary>An append sink a test can hold or break. It hands the records on to the memory
/// connector's sink of the same name.</summary>
internal sealed class HeldAppendConnector(MemoryConnector memory) : IAppendSinkConnector
{
    /// <summary>Awaited inside every write, before anything is stored.</summary>
    public Func<Task>? OnWrite;
    public int Writes;

    public ConnectorDescription Describe(JsonElement config) => new("a held sink", ["password"]);

    public static JsonElement Binding(string name, string? password = null) =>
        Json(password is null
            ? $$"""{ "connector": "held", "name": "{{name}}" }"""
            : $$"""{ "connector": "held", "name": "{{name}}", "password": "{{password}}" }""");

    public IAppendSink<T> Create<T>(SinkContext context) => new Sink<T>(this, ((IAppendSinkConnector)memory).Create<T>(context));

    private sealed class Sink<T>(HeldAppendConnector owner, IAppendSink<T> inner) : IAppendSink<T>
    {
        public async ValueTask WriteAsync(AppendBatch<T> batch, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref owner.Writes);
            if (owner.OnWrite is { } hook)
                await hook().WaitAsync(cancellationToken);
            await inner.WriteAsync(batch, cancellationToken);
        }
    }
}

/// <summary>
/// The durable dead-letter sink (<see cref="PipelineSpec.DeadLetters"/>): every dropped
/// record reaches it before the checkpoint passes it, across a takeover too, and a sink
/// that will not take them stops the pipeline and says so.
/// </summary>
public class DeadLetterSinkTests
{
    private static readonly PipelineCircuitOptions Lenient = new() { ErrorPolicy = RecordErrorPolicy.DeadLetter };

    private static PipelineSpec Fragile(string id, JsonElement deadLetters) =>
        Spec(id, "fragile", id + "-in", id + "-out") with { Circuit = Lenient, DeadLetters = deadLetters };

    private static async Task<(TestService Service, HeldAppendConnector Held)> StartHeldAsync(
        InMemoryServiceCluster? cluster = null, string nodeId = "a", MemoryConnector? memory = null, HeldAppendConnector? held = null)
    {
        memory ??= new MemoryConnector();
        held ??= new HeldAppendConnector(memory);
        var service = await StartAsync(cluster, nodeId, memory,
            o =>
            {
                o.StallAfter = TimeSpan.FromMilliseconds(50);
                o.SinkDelivery = new SinkDeliveryOptions { MaxRetries = 1, InitialBackoff = TimeSpan.FromMilliseconds(10), MaxBackoff = TimeSpan.FromMilliseconds(10) };
            },
            b => b.AddSinkConnector("held", _ => held));
        return (service, held);
    }

    private static List<Trade> WithPoison(int from, int count, int every)
    {
        var trades = Trades(from, count);
        for (var i = every - 1; i < trades.Count; i += every)
            trades[i] = trades[i] with { Quantity = -1 };
        return trades;
    }

    private static bool Same<TKey, TRow>(IReadOnlyDictionary<TKey, TRow> actual, IReadOnlyDictionary<TKey, TRow> expected) where TKey : notnull =>
        actual.Count == expected.Count && expected.All(e => actual.TryGetValue(e.Key, out var row) && Equals(row, e.Value));

    private static long TradeId(DeadLetterEnvelope envelope) => JsonDocument.Parse(envelope.Record!).RootElement.GetProperty("id").GetInt64();

    [Fact]
    public async Task Every_dropped_record_reaches_the_sink_with_what_is_needed_to_find_and_judge_it()
    {
        await using var service = await StartAsync();
        var trades = WithPoison(0, 200, every: 9);
        var poison = trades.Where(t => t.Quantity < 0).ToList();
        var deployed = await service.DeployAsync(Fragile("p", MemoryBinding("dlq")));
        service.Memory.Log<Trade>("p-in").AppendRange(trades);

        var sink = service.Memory.AppendSink<DeadLetterEnvelope>("dlq");
        await Eventually(() => sink.Records.Count == poison.Count, "every dropped record in the dead-letter sink");
        var good = trades.Where(t => t.Quantity >= 0).ToList();
        await Eventually(() => Same(service.Memory.UpsertSink<string, long>("p-out").Rows, Counts.Model(good)), "the rest counted");

        Assert.Equal(poison.Select(t => t.Id).Order(), sink.Records.Select(TradeId).Order());
        Assert.All(sink.Records, e =>
        {
            Assert.Equal("p", e.Pipeline);
            Assert.Equal(deployed.Spec.Generation, e.Generation);
            Assert.InRange(e.Step, 1, long.MaxValue);
            Assert.Equal(1, e.Weight);
            Assert.Equal("InvalidOperationException", e.ExceptionType);
            Assert.Equal($"negative quantity on trade {TradeId(e)}", e.Message);
            Assert.EndsWith(".select", e.Operator);
            Assert.Null(e.Source);
            Assert.InRange(e.Time, DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow);
        });
        // The steps are the circuit's: in order, and never past what is committed.
        Assert.Equal(sink.Records.Select(e => e.Step).Order(), sink.Records.Select(e => e.Step));

        // The ring is still filled, for the admin UI; and the status says where they went.
        var status = service.Pipelines.Get("p");
        Assert.Equal(poison.Count, status.DeadLetters);
        Assert.Equal(poison.Count, service.Pipelines.DeadLetters("p").Count);
        Assert.Equal(new DeadLetterSinkStatus("memory", poison.Count, status.DeadLetterSink!.Batches, null, null, null, 0), status.DeadLetterSink);
        Assert.InRange(status.DeadLetterSink.Batches, 1, poison.Count);
        Assert.Null(service.Pipelines.Get("p").BlockedOn);
    }

    [Fact]
    public async Task A_dead_letter_s_positions_are_the_inputs_and_not_the_circuit_s_clock()
    {
        // An event-time pipeline's checkpoint also carries its watermark, under keys of
        // the circuit's own. They say nothing about where a dropped record came from.
        await using var service = await StartAsync();
        await service.DeployAsync(Spec("w", "windowed") with
        {
            Circuit = Lenient,
            DeadLetters = MemoryBinding("dlq"),
            Inputs = new Dictionary<string, JsonElement> { ["ticks"] = MemoryBinding("w-in") },
            Sinks = new Dictionary<string, JsonElement> { ["live"] = MemoryBinding("w-live"), ["final"] = MemoryBinding("w-final") },
        });
        service.Memory.Log<Tick>("w-in").AppendRange([new Tick(1, "t", 1_000, 1), new Tick(2, "t", 2_000, 1)]);
        await Eventually(() => service.Pipelines.Get("w").CommittedOffsets.ContainsKey("$watermark"), "a checkpoint with the watermark in it");

        service.Memory.Log<Tick>("w-in").AppendRange([new Tick(3, "t", 3_000, -1)]);
        var sink = service.Memory.AppendSink<DeadLetterEnvelope>("dlq");
        await Eventually(() => sink.Records.Count == 1, "the dropped tick in the dead-letter sink");
        var after = sink.Records[0].After;
        Assert.NotNull(after);
        Assert.NotEmpty(after);
        Assert.All(after.Keys, key => Assert.StartsWith("ticks", key));
    }

    [Fact]
    public async Task The_checkpoint_waits_for_the_sink_so_a_takeover_delivers_what_the_old_leader_had_not_stored()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        var (a, heldA) = await StartHeldAsync(cluster, "a", memory);
        await using var first = a;
        var (b, _) = await StartHeldAsync(cluster, "b", memory, new HeldAppendConnector(memory));
        await using var second = b;

        var log = memory.Log<Trade>("p-in");
        var sink = memory.AppendSink<DeadLetterEnvelope>("dlq");
        await a.DeployAsync(Fragile("p", HeldAppendConnector.Binding("dlq")));
        log.AppendRange(WithPoison(0, 40, every: 10));                          // ids 9, 19, 29, 39
        await Eventually(() => sink.Records.Count == 4, "four dead letters under the first leader");
        await Eventually(() => a.Pipelines.Get("p").CommittedOffsets.GetValueOrDefault("trades#p0", -1) == 39, "everything so far committed");
        await Eventually(() => b.Node.Status.Standby is { TakeoverWouldBeWarm: true }, "the standby being warm");

        // The sink stops answering. The next poison record is dropped by a step whose
        // dead letter never reaches it — so that step must not be committed either.
        var hold = new TaskCompletionSource();
        heldA.OnWrite = () => hold.Task;
        var writesBefore = heldA.Writes;
        log.AppendRange(WithPoison(40, 10, every: 10));                         // id 49
        await Eventually(() => heldA.Writes > writesBefore, "the write that is held");
        await Eventually(() => a.Pipelines.Get("p").BlockedOn == "sink-ack:deadLetters", "the pipeline saying what it waits on");
        var status = a.Pipelines.Get("p");
        Assert.Equal(PipelineState.Running, status.State);
        Assert.NotNull(status.DeadLetterSink!.WriteInFlightSince);
        Assert.True(status.CommittedOffsets["trades#p0"] < 49, "the checkpoint passed a record whose dead letter is not stored");
        Assert.Equal(4, sink.Records.Count);

        // The leader is replaced with the write still held: the record is in no sink.
        await b.Node.TakeOverAsync(new NodeActionOptions(), default);
        await b.Running("p");
        await Eventually(() => sink.Records.Select(TradeId).Contains(49), "the new leader delivering the dead letter the old one had not");
        Assert.Equal([9L, 19L, 29L, 39L, 49L], sink.Records.Select(TradeId).Distinct().Order());
        var good = WithPoison(0, 50, every: 10).Where(t => t.Quantity >= 0).ToList();
        await Eventually(() => Same(memory.UpsertSink<string, long>("p-out").Rows, Counts.Model(good)), "the counts under the new leader");
        hold.TrySetCanceled();
    }

    [Fact]
    public async Task A_sink_that_will_not_take_dead_letters_stops_the_pipeline_says_why_and_loses_nothing()
    {
        var (service, held) = await StartHeldAsync();
        await using var _ = service;
        var broken = true;
        held.OnWrite = () => Volatile.Read(ref broken) ? Task.FromException(new IOException("the dead-letter topic is full")) : Task.CompletedTask;
        await service.DeployAsync(Fragile("p", HeldAppendConnector.Binding("dlq")));
        var log = service.Memory.Log<Trade>("p-in");
        var sink = service.Memory.AppendSink<DeadLetterEnvelope>("dlq");
        log.AppendRange(WithPoison(0, 30, every: 10));

        await Eventually(() => service.Pipelines.Get("p").State == PipelineState.Failed, "the pipeline failing rather than dropping or waiting");
        var failed = service.Pipelines.Get("p");
        Assert.Contains("dead letters could not be stored (IOException: the dead-letter topic is full)", failed.Faults[^1].Message);
        Assert.Equal("IOException: the dead-letter topic is full", failed.DeadLetterSink!.LastError);
        Assert.True(failed.DeadLetterSink.FailedWrites >= 2);
        Assert.NotNull(failed.DeadLetterSink.LastErrorTime);
        Assert.Empty(sink.Records);
        // Nothing was committed past a record whose dead letter is not stored.
        Assert.True(failed.CommittedOffsets.GetValueOrDefault("trades#p0", -1) < 9);
        Assert.Contains("pipeline-failed", service.Node.Health.Reasons);

        // The sink is mended and the pipeline restarted: the same records are dropped and stored.
        Volatile.Write(ref broken, false);
        await service.Pipelines.RestartAsync("p", new ActionOptions(), default);
        await service.Running("p");
        await Eventually(() => sink.Records.Select(TradeId).Order().SequenceEqual([9L, 19L, 29L]), "every dead letter stored after the sink was mended");
    }

    [Fact]
    public async Task The_binding_is_checked_at_the_dry_run_and_its_secrets_are_hidden()
    {
        var (service, _) = await StartHeldAsync();
        await using var __ = service;
        service.Memory.Log<Trade>("p-in");

        // An upsert-only connector cannot hold dead letters.
        var probe = await Assert.ThrowsAsync<ServiceException>(() => service.Pipelines.ValidateAsync(
            Fragile("p", Json("""{ "connector": "nowhere" }""")), default));
        Assert.Contains(probe.Details, d => d.StartsWith("dead-letter slot 'deadLetters': no connector named 'nowhere'"));

        var misplaced = await Assert.ThrowsAsync<ServiceException>(() => service.Pipelines.ValidateAsync(
            Spec("p", "fragile", "p-in", "p-out") with
            {
                Sinks = new Dictionary<string, JsonElement> { ["out"] = MemoryBinding("p-out"), ["deadLetters"] = MemoryBinding("dlq") },
            }, default));
        Assert.Contains(misplaced.Details, d => d.Contains("bound by the spec's own \"deadLetters\" member"));

        await service.DeployAsync(Fragile("p", HeldAppendConnector.Binding("dlq", password: "hunter2")));
        var shown = service.Pipelines.RedactedSpec("p");
        Assert.Equal("***", shown.DeadLetters!.Value.GetProperty("password").GetString());
        Assert.Equal("hunter2", service.Pipelines.Spec("p").DeadLetters!.Value.GetProperty("password").GetString());

        // Sent back as it was shown, the stored secret is kept.
        await service.DeployAsync(shown with { Generation = 0 }, StateChoice.Keep);
        Assert.Equal("hunter2", service.Pipelines.Spec("p").DeadLetters!.Value.GetProperty("password").GetString());
    }

    [Fact]
    public void A_definition_cannot_declare_the_dead_letter_slot()
    {
        var harness = Assert.Throws<InvalidOperationException>(() =>
        {
            var builder = new CircuitBuilder(new CircuitOptions());
            var pipeline = new PipelineBuilder(
                new PipelineSpec { Id = "p", Plugin = "x", Definition = "y" }, builder, null!,
                Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, null!);
            pipeline.AppendSink("deadLetters", pipeline.Input<Trade>("trades"));
        });
        Assert.Contains("dead-letter sink", harness.Message);
    }
}
