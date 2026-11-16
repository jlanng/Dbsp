using System.Collections.Concurrent;
using System.Text.Json;
using Incremental.Storage;

namespace Incremental.Kafka.Tests;

/// <summary>
/// Source resumption under crashes. Two topics are produced to while a circuit reads them
/// through <see cref="KafkaSource"/>s — facts into a stream input, accounts into a keyed
/// input — with its state on the Kafka changelog and its results in Kafka sink topics. The
/// process is cut down at arbitrary moments, several times, and each successor restores
/// from the changelog and starts its sources from the offsets in the checkpoint. At the
/// end the sink topics are compared with a model computed from what was produced.
/// <para>
/// What must hold: every successor's sources begin exactly one record after the committed
/// offsets; sums and counts — where a record lost or applied twice shows — are exact; the
/// append topic holds every fact at least once; the keyed topic holds every surviving
/// account, with its last row. What need not hold is the gap the design records
/// (durability-and-standby.md, <i>A gap in "keyed upserts converge"</i>): an account
/// upserted before a crash and deleted in the replay can stay in the sink, because the
/// auto-stepping replay cuts different steps. The test counts those and requires each to
/// be a key the model deleted.
/// </para>
/// </summary>
public class KafkaSourceResumptionTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Timeout = KafkaEnv.Timeout;
    private const int FactPartitions = 3, AccountPartitions = 2, Facts = 6000, AccountKeys = 40;

    private sealed record Fact(int Id, string Key, int Amount);
    private sealed record Account(int Id, string Owner, int Limit);

    private static Fact FactAt(int i) => new(i, "k" + i % 17, i % 23 + 1);

    private sealed class Node
    {
        public required Circuit Circuit;
        public required KafkaSource FactSource;
        public required KafkaSource AccountSource;
        public required Task Deliveries;
        public required List<IAsyncDisposable> Sinks;
        public readonly CancellationTokenSource Kill = new();
        public readonly ConcurrentDictionary<(string Topic, int Partition), long> FirstOffsetRead = new();
        public (IReadOnlyDictionary<int, long> Facts, IReadOnlyDictionary<int, long> Accounts) Committed;

        /// <summary>The process dies: deliveries stop where they are, the sources stop,
        /// nothing is shut down in order and nothing more is committed.</summary>
        public async Task CrashAsync()
        {
            Kill.Cancel();
            await FactSource.DisposeAsync();
            await AccountSource.DisposeAsync();
            try
            {
                await Deliveries.WaitAsync(Timeout);
            }
            catch (Exception)
            {
                // However a killed delivery ends.
            }
            foreach (var sink in Sinks)
                await sink.DisposeAsync();
        }
    }

    private sealed record Topics(string Facts, string Accounts, string Sums, string Counts, string AccountsOut, string FactsOut);

    private async Task<Node> StartAsync(KafkaEnv env, Topics topics, int incarnation)
    {
        // A new process: its own connection, which fences the last one's, and a state
        // store restored from the changelog.
        var log = await env.ConnectWriterAsync(env.Options(nodeId: "node-" + incarnation));
        var storage = await ChangelogStorage.OpenAsync(log.State).WaitAsync(Timeout);
        var builder = new CircuitBuilder(new CircuitOptions
        {
            Name = "resume",
            Storage = storage.Pipeline("resume"),
            CheckpointInterval = 3,
            CheckpointMaxDelay = TimeSpan.FromMilliseconds(200),
            MaxStepItemsPerInput = 40,
            InputChannelCapacity = 128,
        });
        var facts = builder.Input<Fact>("facts");
        var accounts = builder.KeyedInput<int, Account>("accounts", a => a.Id);
        var sums = facts.GroupBy(f => f.Key).Sum(f => (long)f.Amount).ToAcknowledgedOutput();
        var counts = facts.GroupBy(f => f.Key).Sum(_ => 1L).ToAcknowledgedOutput();
        var accountRows = accounts.ToAcknowledgedOutput();
        var factRows = facts.ToAcknowledgedOutput();
        var circuit = builder.Build();
        circuit.Start();

        KafkaSinkOptions Sink(string topic) => new() { Topic = topic, BootstrapServers = KafkaEnv.Bootstrap };
        var sumSink = new KafkaUpsertSink<string, long>(Sink(topics.Sums));
        var countSink = new KafkaUpsertSink<string, long>(Sink(topics.Counts));
        var accountSink = new KafkaUpsertSink<int, Account>(Sink(topics.AccountsOut));
        var factSink = new KafkaAppendSink<Fact>(Sink(topics.FactsOut));
        var node = new Node
        {
            Circuit = circuit,
            Sinks = [sumSink, countSink, accountSink, factSink],
            Deliveries = Task.CompletedTask,
            FactSource = null!,
            AccountSource = null!,
        };
        node.Deliveries = Task.WhenAll(
            circuit.Handle(sums).DeliverToAsync(sumSink, cancellationToken: node.Kill.Token),
            circuit.Handle(counts).DeliverToAsync(countSink, cancellationToken: node.Kill.Token),
            circuit.Handle(accountRows).DeliverToAsync(accountSink, a => a.Id, cancellationToken: node.Kill.Token),
            circuit.Handle(factRows).DeliverToAsync(factSink, cancellationToken: node.Kill.Token));
        _ = node.Deliveries.ContinueWith(t => t.Exception, TaskScheduler.Default);

        // Where the checkpoint says each partition stands; the sources must begin one
        // record later, and the decoders note where they actually began.
        var committedFacts = circuit.CommittedOffsets("facts");
        var committedAccounts = circuit.CommittedOffsets("accounts");
        void Note(in KafkaRecord record) => node.FirstOffsetRead.TryAdd((record.Topic, record.Partition), record.Offset);
        var factJson = KafkaDeserializers.Json<Fact>();
        var accountJson = KafkaDeserializers.Keyed<int, Account>();
        node.FactSource = await KafkaSource.StartAsync(circuit, facts, env.Source(topics.Facts), (ReadOnlySpan<byte> data, in KafkaRecord record) =>
        {
            Note(in record);
            return factJson(data, in record);
        }).WaitAsync(Timeout);
        node.AccountSource = await KafkaSource.StartAsync(circuit, accounts, env.Source(topics.Accounts), (in KafkaRecord record) =>
        {
            Note(in record);
            return accountJson(in record);
        }).WaitAsync(Timeout);
        node.Committed = (committedFacts, committedAccounts);
        return node;
    }

    [Fact]
    public async Task SourcesResumedAfterCrashesAtArbitraryPoints_LoseNothing_AndTheSinksEqualTheModel()
    {
        await using var env = KafkaEnv.Create();
        var topics = new Topics(
            await env.CreateTopicAsync("facts", FactPartitions),
            await env.CreateTopicAsync("accounts", AccountPartitions, compacted: true),
            await env.CreateTopicAsync("sums", 2, compacted: true),
            await env.CreateTopicAsync("counts", 2, compacted: true),
            await env.CreateTopicAsync("accounts-out", 2, compacted: true),
            await env.CreateTopicAsync("facts-out", 1));

        // The model, and the producing of it: steadily, so that every crash lands mid-stream.
        var accountModel = new Dictionary<int, Account>();
        var deletedAccounts = new HashSet<int>();
        var accountRecords = new int[AccountPartitions];
        var random = new Random(23);
        var producing = Task.Run(async () =>
        {
            for (var i = 0; i < Facts; i++)
            {
                env.ProduceJson(topics.Facts, i % FactPartitions, i, FactAt(i));
                if (i % 5 == 0)
                {
                    var id = random.Next(AccountKeys);
                    if (random.Next(3) == 0)
                    {
                        accountModel.Remove(id);
                        deletedAccounts.Add(id);
                        env.ProduceJson<int, Account>(topics.Accounts, id % AccountPartitions, id, null);
                    }
                    else
                    {
                        accountModel[id] = new Account(id, "owner-" + random.Next(9), i);
                        deletedAccounts.Remove(id);
                        env.ProduceJson(topics.Accounts, id % AccountPartitions, id, accountModel[id]);
                    }
                    accountRecords[id % AccountPartitions]++;
                }
                if (i % 40 == 39)
                {
                    await env.FlushAsync();
                    await Task.Delay(45);
                }
            }
            await env.FlushAsync();
        });

        const int Crashes = 6;
        var timing = new Random();
        long read = 0;
        var resumedFromACheckpoint = 0;
        var node = await StartAsync(env, topics, 0);
        for (var crash = 1; crash <= Crashes; crash++)
        {
            await Task.Delay(timing.Next(250, 1100));
            await node.CrashAsync();
            read += node.FactSource.RecordsRead + node.AccountSource.RecordsRead;

            node = await StartAsync(env, topics, crash);
            // It read from one record after the checkpoint, on every partition it read at all.
            await KafkaEnv.EventuallyAsync(
                () => Task.FromResult(node.FirstOffsetRead.Count == FactPartitions + AccountPartitions || producing.IsCompleted),
                "the successor has read from every partition");
            foreach (var ((topic, partition), first) in node.FirstOffsetRead)
            {
                var committed = topic == topics.Facts ? node.Committed.Facts : node.Committed.Accounts;
                Assert.Equal(committed.GetValueOrDefault(partition, -1) + 1, first);
            }
            if (node.Committed.Facts.Count == FactPartitions)
                resumedFromACheckpoint++;
        }

        await producing.WaitAsync(Timeout);
        Assert.True(resumedFromACheckpoint >= Crashes / 2, "most crashes must land after a checkpoint, or they prove little");
        var last = node;
        // Done when the checkpoint covers the last record of every partition: with commit
        // after delivery, that is also when the sinks have everything.
        await KafkaEnv.EventuallyAsync(() =>
        {
            var facts = last.Circuit.CommittedOffsets("facts");
            var accounts = last.Circuit.CommittedOffsets("accounts");
            return Task.FromResult(
                Enumerable.Range(0, FactPartitions).All(p => facts.GetValueOrDefault(p, -1) == Facts / FactPartitions - 1)
                && Enumerable.Range(0, AccountPartitions).All(p => accounts.GetValueOrDefault(p, -1) == accountRecords[p] - 1));
        }, "the last incarnation has checkpointed the end of both topics");
        read += last.FactSource.RecordsRead + last.AccountSource.RecordsRead;
        Assert.Null(last.Circuit.Fault);

        // Sums and counts: exact. A record skipped or applied twice by any of the seven
        // incarnations would show here.
        var allFacts = Enumerable.Range(0, Facts).Select(FactAt).ToList();
        Assert.Equal(
            allFacts.GroupBy(f => f.Key).ToDictionary(g => g.Key, g => g.Sum(f => (long)f.Amount)),
            KafkaEnv.ReadTable<string, long>(topics.Sums));
        Assert.Equal(
            allFacts.GroupBy(f => f.Key).ToDictionary(g => g.Key, g => (long)g.Count()),
            KafkaEnv.ReadTable<string, long>(topics.Counts));

        // The append topic: every fact, at least once.
        var appended = KafkaEnv.ReadTopic(topics.FactsOut)
            .Select(r => JsonSerializer.Deserialize<Fact>(r.Message.Value, JsonSerializerOptions.Web)!).ToList();
        Assert.Equal(allFacts.ToHashSet(), appended.ToHashSet());
        var duplicates = appended.Count - Facts;

        // The keyed topic: every surviving account with its last row; anything more is a
        // key the model deleted (the recorded gap).
        var accountsOut = KafkaEnv.ReadTable<int, Account>(topics.AccountsOut);
        foreach (var (id, account) in accountModel)
            Assert.Equal(account, accountsOut.GetValueOrDefault(id));
        var stale = accountsOut.Keys.Except(accountModel.Keys).ToList();
        Assert.All(stale, id => Assert.Contains(id, deletedAccounts));

        var produced = Facts + accountRecords.Sum();
        Assert.True(read > produced, "every crash should have made its successor read something again");
        KafkaEnv.Measured(output,
            $"source resumption: {produced} records, {Crashes} crashes; {read - produced} records read again, " +
            $"{duplicates} facts appended more than once, {stale.Count} deleted account(s) left in the keyed sink");

        await last.CrashAsync();
    }
}
