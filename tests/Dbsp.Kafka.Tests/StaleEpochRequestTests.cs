using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text;
using Confluent.Kafka;

namespace Dbsp.Kafka.Tests;

/// <summary>
/// How two nodes once both ended fenced, reproduced on purpose.
/// <para>
/// A transactional producer that has lost track of its sequence numbers — which a leader's
/// client does when partitions change leaders under an open transaction — asks the
/// transaction coordinator for a new epoch: <c>InitProducerId</c> carrying the producer id
/// and epoch it has (KIP-360). If another process has taken the transactional id since,
/// that request is stale and must be refused. The coordinator of Kafka 3.9 refuses it only
/// when no transaction is open. When one is — the new owner's — it checks the producer id
/// and not the epoch, takes the request for the usual "a new producer wants this id",
/// aborts the open transaction and raises the epoch: the new owner is fenced by the node it
/// replaced. The stale request is answered "try again", and the retry is then refused, so
/// the old node ends too. In the recorded run the open transaction was the standby's first
/// one, the one that commits its term (<c>docs/design/durability-and-standby.md</c>,
/// <i>As built: broker failures</i>).
/// </para>
/// <para>
/// The client cannot be made to send that request at a chosen moment, so the test speaks
/// the two requests it needs of the Kafka protocol itself.
/// </para>
/// </summary>
public class StaleEpochRequestTests
{
    private static readonly TimeSpan Timeout = KafkaEnv.Timeout;

    private const short ConcurrentTransactions = 51;
    private const short InvalidProducerEpoch = 47;
    private const short ProducerFenced = 90;

    [Fact]
    public async Task A_replaced_writers_request_for_a_new_epoch_fences_its_successor_mid_transaction_and_the_successor_acquires_again()
    {
        await using var env = KafkaEnv.Create();
        var options = env.Options(nodeId: "b", heartbeat: TimeSpan.FromMilliseconds(100));
        var transactionalId = env.Prefix + ".writer";
        var log = await env.ConnectAsync(options);

        // The old writer: it holds the transactional id under some producer id and epoch.
        var coordinator = await CoordinatorAsync(transactionalId);
        var old = await InitProducerIdAsync(coordinator, transactionalId, producerId: -1, epoch: -1);
        Assert.Equal(0, old.Error);

        // The successor takes the id (that fences the old writer) and opens its first
        // transaction, the one that commits its term. With its records at the brokers and
        // the commit not yet asked for, the old writer's request for a new epoch arrives.
        (short Error, long ProducerId, short Epoch)? stale = null;
        log.FailTransaction = _ =>
        {
            stale ??= InitProducerIdAsync(coordinator, transactionalId, old.ProducerId, old.Epoch).GetAwaiter().GetResult();
            return false;
        };
        var lost = await Assert.ThrowsAsync<KafkaLogNotAcquiredException>(() => log.AcquireWriterAsync()).WaitAsync(Timeout);

        // The stale request was not refused: it was taken for a newcomer's, and the open
        // transaction — the successor's — was aborted under a raised epoch.
        Assert.Equal(ConcurrentTransactions, stale!.Value.Error);
        Assert.IsType<KafkaLogFencedException>(lost.InnerException);

        // The old writer gains nothing by it: asked again, the coordinator refuses.
        var again = await InitProducerIdAsync(coordinator, transactionalId, old.ProducerId, old.Epoch);
        Assert.Contains(again.Error, new[] { ProducerFenced, InvalidProducerEpoch });

        // So nobody holds the id. Before the fix the successor ended here as Fenced, and
        // the deployment had no node. Now it is a reader with a new client, its term was
        // never committed, and it takes the id again.
        Assert.False(log.IsFenced);
        Assert.False(log.IsWriter);
        Assert.Equal(1, log.WriterResets);
        log.FailTransaction = null;
        await log.AcquireWriterAsync().WaitAsync(Timeout);
        Assert.True(log.IsWriter);
        Assert.Equal(1, log.Term);
        using var transaction = (await log.State.AcquireWriterAsync()).BeginTransaction();
        transaction.Append(0, Encoding.UTF8.GetBytes("p\00\0store\0k"), [1, (byte)'1']);
        await transaction.CommitAsync().AsTask().WaitAsync(Timeout);
    }

    // ── just enough of the protocol ────────────────────────────────────────────────────

    /// <summary>FindCoordinator v2 for a transactional id: the broker to ask.</summary>
    private static async Task<(string Host, int Port)> CoordinatorAsync(string transactionalId)
    {
        var first = KafkaEnv.Bootstrap.Split(',')[0];
        var colon = first.LastIndexOf(':');
        var bootstrap = (first[..colon], int.Parse(first[(colon + 1)..]));
        for (var attempt = 0; ; attempt++)
        {
            var body = new MemoryStream();
            WriteString(body, transactionalId);
            body.WriteByte(1);                                   // key type: transaction
            var response = await RequestAsync(bootstrap, apiKey: 10, version: 2, flexible: false, body.ToArray());
            var at = 4;                                          // throttle time
            var error = BinaryPrimitives.ReadInt16BigEndian(response.AsSpan(at));
            at += 2;
            var messageLength = BinaryPrimitives.ReadInt16BigEndian(response.AsSpan(at));
            at += 2 + Math.Max((int)messageLength, 0);
            at += 4;                                             // node id
            var hostLength = BinaryPrimitives.ReadInt16BigEndian(response.AsSpan(at));
            var host = Encoding.UTF8.GetString(response, at + 2, hostLength);
            var port = BinaryPrimitives.ReadInt32BigEndian(response.AsSpan(at + 2 + hostLength));
            if (error == 0)
                return (host, port);
            Assert.True(attempt < 50, $"FindCoordinator kept answering error {error}");
            await Task.Delay(200);                               // the coordinator's topic is being created
        }
    }

    /// <summary>InitProducerId v3. With a producer id and epoch it is the request a
    /// producer sends to have its epoch raised; with −1 for both, a newcomer's.</summary>
    private static async Task<(short Error, long ProducerId, short Epoch)> InitProducerIdAsync(
        (string Host, int Port) coordinator, string transactionalId, long producerId, short epoch)
    {
        for (var attempt = 0; ; attempt++)
        {
            var body = new MemoryStream();
            var id = Encoding.UTF8.GetBytes(transactionalId);
            body.WriteByte((byte)(id.Length + 1));               // compact string: length + 1 (short ids only)
            body.Write(id);
            WriteInt32(body, 60_000);                            // transaction timeout
            var number = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(number, producerId);
            body.Write(number);
            BinaryPrimitives.WriteInt16BigEndian(number, epoch);
            body.Write(number, 0, 2);
            body.WriteByte(0);                                   // no tagged fields
            Assert.True(id.Length < 127);

            var response = await RequestAsync(coordinator, apiKey: 22, version: 3, flexible: true, body.ToArray());
            var error = BinaryPrimitives.ReadInt16BigEndian(response.AsSpan(4));
            var result = (error, BinaryPrimitives.ReadInt64BigEndian(response.AsSpan(6)), BinaryPrimitives.ReadInt16BigEndian(response.AsSpan(14)));

            // A newcomer is told to wait while an earlier transaction is settled, or while
            // the coordinator loads; the stale request's own answer is the test's to read.
            if (producerId >= 0 || error is not (ConcurrentTransactions or 14 or 15 or 16) || attempt > 50)
                return result;
            await Task.Delay(200);
        }
    }

    private static async Task<byte[]> RequestAsync((string Host, int Port) broker, short apiKey, short version, bool flexible, byte[] body)
    {
        using var client = new TcpClient { NoDelay = true };
        await client.ConnectAsync(broker.Host, broker.Port).WaitAsync(Timeout);
        var stream = client.GetStream();

        var request = new MemoryStream();
        WriteInt32(request, 0);                                  // size, filled in below
        WriteInt16(request, apiKey);
        WriteInt16(request, version);
        WriteInt32(request, 7);                                  // correlation id
        WriteString(request, "stale-epoch-test");
        if (flexible)
            request.WriteByte(0);                                // header tagged fields
        request.Write(body);
        var bytes = request.ToArray();
        BinaryPrimitives.WriteInt32BigEndian(bytes, bytes.Length - 4);
        await stream.WriteAsync(bytes).AsTask().WaitAsync(Timeout);

        var size = new byte[4];
        await stream.ReadExactlyAsync(size).AsTask().WaitAsync(Timeout);
        var response = new byte[BinaryPrimitives.ReadInt32BigEndian(size)];
        await stream.ReadExactlyAsync(response).AsTask().WaitAsync(Timeout);
        Assert.Equal(7, BinaryPrimitives.ReadInt32BigEndian(response));

        // What follows the correlation id: a flexible response's header has tagged fields.
        var header = flexible ? 5 : 4;
        return response[header..];
    }

    private static void WriteInt16(Stream stream, short value)
    {
        Span<byte> bytes = stackalloc byte[2];
        BinaryPrimitives.WriteInt16BigEndian(bytes, value);
        stream.Write(bytes);
    }

    private static void WriteInt32(Stream stream, int value)
    {
        Span<byte> bytes = stackalloc byte[4];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        stream.Write(bytes);
    }

    private static void WriteString(Stream stream, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteInt16(stream, (short)bytes.Length);
        stream.Write(bytes);
    }
}
