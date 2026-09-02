using System.Net;
using System.Net.Sockets;
using Incremental.Cluster;

namespace Incremental.Tests;

/// <summary>
/// The fault-instead-of-hang guarantees for clusters: a stuck (not dead)
/// peer trips <see cref="CircuitOptions.ExchangeTimeout"/> and faults the circuit with a
/// descriptive error; a merely slow peer under the timeout still completes; oversized
/// frames are rejected before allocation; and barrier waiters that outlive the spin budget
/// park and are correctly woken — by the late arrival or by a poison.
/// </summary>
public class ClusterResilienceTests
{
    private static readonly TimeSpan Outer = TimeSpan.FromSeconds(20);

    private static CircuitOptions Clustered(IExchangeTransport transport, TimeSpan? exchangeTimeout) => new()
    {
        Workers = 2,
        AutoStep = false,
        Cluster = new ClusterOptions { Transport = transport },
        ExchangeTimeout = exchangeTimeout,
    };

    private static (CircuitBuilder Builder, InputStream<int> Numbers, Output<KeyedValue<int, long>> Counts)
        CountPipeline(CircuitOptions options)
    {
        var builder = new CircuitBuilder(options);
        var numbers = builder.Input<int>("numbers");
        var counts = numbers.GroupBy(n => n % 4).Count().ToOutput(bootstrap: false);
        return (builder, numbers, counts);
    }

    [Fact]
    public async Task Stuck_peer_trips_the_exchange_timeout_and_faults_instead_of_hanging()
    {
        var mesh = InMemoryExchangeTransport.CreateMesh(2);
        var defs0 = CountPipeline(Clustered(mesh[0], TimeSpan.FromMilliseconds(500)));
        var defs1 = CountPipeline(Clustered(mesh[1], TimeSpan.FromMilliseconds(500)));
        await using var process0 = defs0.Builder.Build();
        await using var process1 = defs1.Builder.Build();
        process0.Start();
        process1.Start();

        for (var n = 0; n < 8; n++)
            await process0.Handle(defs0.Numbers).InsertAsync(n);

        // Process 1 never steps — alive as far as the transport can tell, but stuck. The
        // designated worker's wait for its shards must time out and fault, never hang.
        var ex = await Assert.ThrowsAsync<TimeoutException>(() => process0.StepAsync()).WaitAsync(Outer);
        Assert.Contains("timed out", ex.Message);
        Assert.Contains("process 1", ex.Message);
        Assert.Contains("ExchangeTimeout", ex.Message);
        Assert.NotNull(process0.Fault);

        // The poison told process 1 (fault frame, backstopped by the disconnect): its own
        // first step faults promptly instead of waiting for a peer that already gave up.
        await Assert.ThrowsAnyAsync<Exception>(() => process1.StepAsync()).WaitAsync(Outer);
        Assert.NotNull(process1.Fault);
    }

    [Fact]
    public async Task Slow_but_alive_peer_under_the_timeout_still_completes()
    {
        var mesh = InMemoryExchangeTransport.CreateMesh(2);
        var defs0 = CountPipeline(Clustered(mesh[0], TimeSpan.FromSeconds(10)));
        var defs1 = CountPipeline(Clustered(mesh[1], TimeSpan.FromSeconds(10)));
        await using var process0 = defs0.Builder.Build();
        await using var process1 = defs1.Builder.Build();
        process0.Start();
        process1.Start();

        for (var n = 0; n < 8; n++)
            await process0.Handle(defs0.Numbers).InsertAsync(n);

        var step0 = process0.StepAsync();
        await Task.Delay(300); // slower than a healthy step, well under the timeout
        await Task.WhenAll(step0, process1.StepAsync()).WaitAsync(Outer);

        Assert.Null(process0.Fault);
        Assert.Null(process1.Fault);
        var counts = new ZSet<KeyedValue<int, long>>();
        foreach (var output in new[] { process0.Handle(defs0.Counts), process1.Handle(defs1.Counts) })
            while (output.Reader.TryRead(out var delta))
                counts.AddAll(delta);
        Assert.Equal(4, counts.DistinctCount); // 8 numbers over 4 key groups, 2 each
        foreach (var (keyed, weight) in counts)
        {
            Assert.Equal(2, keyed.Value);
            Assert.Equal(1, weight);
        }
    }

    [Fact]
    public void ExchangeTimeout_must_be_positive()
    {
        var mesh = InMemoryExchangeTransport.CreateMesh(2);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            CountPipeline(Clustered(mesh[0], TimeSpan.Zero)).Builder.Build());
    }

    [Fact]
    public async Task Frames_beyond_the_cap_are_rejected_on_both_sides()
    {
        var ports = FreePorts(2);
        string[] endpoints = [$"127.0.0.1:{ports[0]}", $"127.0.0.1:{ports[1]}"];
        // Asymmetric caps (a misconfiguration) let one frame test both directions: small
        // enough for the sender's cap, beyond the receiver's.
        await using var sender = new TcpExchangeTransport(0, endpoints, maxFrameBytes: 64 << 10);
        await using var receiver = new TcpExchangeTransport(1, endpoints, maxFrameBytes: 2048);
        await Task.WhenAll(sender.ConnectAsync(default), receiver.ConnectAsync(default)).WaitAsync(Outer);

        // Send side: a frame beyond the local cap is refused before it is queued.
        var tooBigForSender = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            sender.SendAsync(1, new ExchangeFrame(1, 0, new byte[128 << 10]), default).AsTask()).WaitAsync(Outer);
        Assert.Contains("maxFrameBytes", tooBigForSender.Message);

        // Receive side: a length prefix beyond the local cap is rejected before allocation.
        await sender.SendAsync(1, new ExchangeFrame(1, 0, new byte[4096]), default).AsTask().WaitAsync(Outer);
        var tooBigForReceiver = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            receiver.ReceiveAsync(0, default).AsTask().WaitAsync(Outer));
        Assert.Contains("maxFrameBytes", tooBigForReceiver.Message);
    }

    [Fact]
    public async Task Barrier_waiter_that_outlives_the_spin_budget_parks_and_wakes_correctly()
    {
        // One worker's delegate stalls far past the spin budget, so its peer parks on the
        // barrier's blocking path; the late arrival must wake it and the step must produce
        // the same counts as ever.
        var options = new CircuitOptions { Workers = 2, AutoStep = false };
        var builder = new CircuitBuilder(options);
        var numbers = builder.Input<int>("numbers");
        var countsDef = numbers
            .Select(n =>
            {
                if (n == 0)
                    Thread.Sleep(400);
                return n;
            })
            .GroupBy(n => n % 4)
            .Count()
            .ToOutput(bootstrap: false);
        await using var circuit = builder.Build();
        circuit.Start();

        var input = circuit.Handle(numbers);
        for (var n = 0; n < 8; n++)
            await input.InsertAsync(n);
        await circuit.StepAsync().WaitAsync(Outer);

        Assert.Null(circuit.Fault);
        var counts = new ZSet<KeyedValue<int, long>>();
        while (circuit.Handle(countsDef).Reader.TryRead(out var delta))
            counts.AddAll(delta);
        Assert.Equal(4, counts.DistinctCount);
    }

    [Fact]
    public async Task Poison_wakes_a_parked_barrier_waiter()
    {
        // The faulting worker stalls past its peer's spin budget before throwing, so the
        // poison must reach a waiter on the barrier's blocking path — the step has to fault
        // with the original error, not hang.
        var options = new CircuitOptions { Workers = 2, AutoStep = false };
        var builder = new CircuitBuilder(options);
        var numbers = builder.Input<int>("numbers");
        numbers
            .Select(n =>
            {
                if (n != 0)
                    return n;
                Thread.Sleep(400);
                throw new InvalidOperationException("boom 0");
            })
            .GroupBy(n => n % 4)
            .Count()
            .ToOutput(bootstrap: false);
        await using var circuit = builder.Build();
        circuit.Start();

        var input = circuit.Handle(numbers);
        for (var n = 0; n < 8; n++)
            await input.InsertAsync(n);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => circuit.StepAsync()).WaitAsync(Outer);
        Assert.Contains("boom 0", ex.Message);
        Assert.NotNull(circuit.Fault);
    }

    private static int[] FreePorts(int count)
    {
        var listeners = Enumerable.Range(0, count)
            .Select(_ =>
            {
                var listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start();
                return listener;
            })
            .ToArray();
        var ports = listeners.Select(l => ((IPEndPoint)l.LocalEndpoint).Port).ToArray();
        foreach (var listener in listeners)
            listener.Stop();
        return ports;
    }
}
