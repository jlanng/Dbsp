using System.Text;
using System.Text.Json;
using Acme.Contracts;
using static Incremental.Service.Tests.SupervisorTests;
using static Incremental.Service.Tests.TestService;

namespace Incremental.Service.Tests;

/// <summary>
/// The <c>files</c> connector over a local object store: objects read in name order with
/// the position in the checkpoint, one object written per step, and both across a restart
/// and a takeover — always compared with a model of what the pipeline should hold.
/// </summary>
public class FileConnectorTests : IDisposable
{
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);
    private readonly string _root = Path.Combine(Path.GetTempPath(), "incremental-file-connector-tests", Guid.NewGuid().ToString("N"));
    private readonly LocalObjectStore _store;

    public FileConnectorTests() => _store = new LocalObjectStore(_root);

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    private Task<TestService> StartAsync(InMemoryServiceCluster? cluster = null, string nodeId = "a", MemoryConnector? memory = null) =>
        TestService.StartAsync(cluster, nodeId, memory, configure: b => b.AddObjectStore("landing", _ => _store));

    private static JsonElement Files(string prefix, string? more = null) =>
        Json($$"""{ "connector": "files", "store": "landing", "prefix": "{{prefix}}"{{(more is null ? "" : ", " + more)}} }""");

    /// <summary>An object, written whole and then moved into place.</summary>
    private async Task PutAsync(string name, IEnumerable<string> lines)
    {
        await using var stream = await _store.OpenWriteAsync(name, default);
        await stream.WriteAsync(Encoding.UTF8.GetBytes(string.Join("\n", lines) + "\n"));
    }

    private Task PutAsync(string name, IEnumerable<Trade> trades) => PutAsync(name, trades.Select(t => JsonSerializer.Serialize(t, Web)));

    private static bool Same<TKey, TRow>(IReadOnlyDictionary<TKey, TRow> actual, IReadOnlyDictionary<TKey, TRow> expected) where TKey : notnull =>
        actual.Count == expected.Count && expected.All(e => actual.TryGetValue(e.Key, out var row) && Equals(row, e.Value));

    private static PipelineSpec CountsFromFiles(string id, string prefix, string sink) =>
        Spec(id, "counts", null, sink) with
        {
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Files(prefix, "\"pollInterval\": \"00:00:00.05\"") },
        };

    private async Task<List<T>> AppendsAsync<T>(string prefix)
    {
        var records = new List<T>();
        await foreach (var record in FileSinkFormat.ReadAppendsAsync<T>(_store, prefix))
            records.Add(record);
        return records;
    }

    // ── the position ───────────────────────────────────────────────────────────────────

    [Fact]
    public void Whatever_prefix_of_its_advances_a_checkpoint_holds_the_position_names_a_whole_object()
    {
        // The source's sends, in order, for three objects with names of different lengths;
        // a checkpoint may land after any of them. Each partition keeps its highest offset.
        var names = new[] { "in/2026-01-05/part-0001.jsonl", "in/b.jsonl", "in/2026-01-06/a-much-longer-name-than-the-others-ünïcode.jsonl" };
        var sends = new List<(int Partition, long Offset, string Object, long Line)>();
        for (var ordinal = 1; ordinal <= names.Length; ordinal++)
        {
            foreach (var (partition, offset) in FilePosition.Name(ordinal, names[ordinal - 1]))
                sends.Add((partition, offset, names[ordinal - 1], -1));
            for (var line = 0; line < 3; line++)
                sends.Add((0, FilePosition.Line(ordinal, line), names[ordinal - 1], line));
        }

        var committed = new Dictionary<int, long>();
        Assert.Null(FilePosition.Decode(committed));
        string? lastWhole = null;
        long lastLine = -1;
        foreach (var (partition, offset, name, line) in sends)
        {
            Assert.True(!committed.TryGetValue(partition, out var held) || offset > held, "every send must advance its partition, or the input drops it");
            committed[partition] = offset;
            if (partition == 1)
                (lastWhole, lastLine) = (name, -1);     // the header: from here the checkpoint names this object
            else if (partition == 0)
                lastLine = line;
            var decoded = FilePosition.Decode(committed);
            if (lastWhole is null)
                Assert.Null(decoded);
            else
            {
                Assert.Equal(lastWhole, decoded!.Value.Name);
                Assert.Equal(lastLine, decoded.Value.Line);
            }
        }
    }

    [Fact]
    public void A_name_too_long_to_checkpoint_is_refused_by_name()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => FilePosition.Name(1, new string('x', 300)).ToList());
        Assert.Contains("300 bytes", ex.Message);
    }

    // ── the source ─────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Objects_are_read_in_name_order_new_ones_are_picked_up_and_one_that_sorts_before_the_position_is_not()
    {
        await using var service = await StartAsync();
        var trades = Trades(0, 300);
        await PutAsync("in/0002.jsonl", trades.Skip(100).Take(100));
        await PutAsync("in/0001.jsonl", trades.Take(100));
        await PutAsync("elsewhere/0000.jsonl", Trades(900, 50));                // another prefix
        await service.DeployAsync(CountsFromFiles("counts", "in/", "counts"));
        var sink = service.Memory.UpsertSink<string, long>("counts");
        await Eventually(() => Same(sink.Rows, Counts.Model(trades.Take(200))), "the two objects counted");

        await PutAsync("in/0003.jsonl", trades.Skip(200));
        await Eventually(() => Same(sink.Rows, Counts.Model(trades)), "the object that arrived later counted");

        await PutAsync("in/0000-late.jsonl", Trades(500, 40));                 // sorts before 0003: never read
        await PutAsync("in/0004.jsonl", Array.Empty<Trade>());
        await PutAsync("in/0005.jsonl", Trades(600, 10));
        trades.AddRange(Trades(600, 10));
        await Eventually(() => Same(sink.Rows, Counts.Model(trades)), "the next object counted and the late one left out");

        var source = service.Pipelines.Get("counts").Sources.Single().Stats;
        Assert.Equal(310, source.Records);
        Assert.Equal("in/0005.jsonl:10", source.At);
        Assert.Equal(0, source.Lag);
    }

    [Fact]
    public async Task A_restart_resumes_at_the_next_line_and_reads_nothing_twice()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        var trades = Trades(0, 250);
        await PutAsync("in/a.jsonl", trades.Take(100));
        await PutAsync("in/b.jsonl", trades.Skip(100).Take(150));
        var sink = memory.UpsertSink<string, long>("counts");
        await using (var first = await StartAsync(cluster, "a", memory))
        {
            await first.DeployAsync(CountsFromFiles("counts", "in/", "counts"));
            await Eventually(() => Same(sink.Rows, Counts.Model(trades)), "the first run's counts");
            await Eventually(() => first.Pipelines.Get("counts").CommittedOffsets.Count > 0, "a checkpoint with the position");
        }

        // While it is down: the object it stood in is deleted, and two more arrive.
        await _store.DeleteAsync("in/a.jsonl", default);
        await _store.DeleteAsync("in/b.jsonl", default);
        var later = Trades(250, 120);
        await PutAsync("in/c.jsonl", later.Take(50));
        await PutAsync("in/d.jsonl", later.Skip(50));
        trades.AddRange(later);

        await using var second = await StartAsync(cluster, "a", memory);
        await second.Running("counts");
        await Eventually(() => Same(sink.Rows, Counts.Model(trades)), "the counts after the restart");
        // Counts are not idempotent: a line read twice would show. And the source says so.
        Assert.Equal(later.Count, second.Pipelines.Get("counts").Sources.Single().Stats.Records);
    }

    [Fact]
    public async Task A_restart_in_the_middle_of_an_object_carries_on_inside_it()
    {
        // The position is committed with 60 lines of an object read; the object then
        // "grows" (a local file rewritten whole with more lines), which a restart reads on from line 61.
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        var trades = Trades(0, 100);
        await PutAsync("in/a.jsonl", trades.Take(60));
        var sink = memory.UpsertSink<string, long>("counts");
        await using (var first = await StartAsync(cluster, "a", memory))
        {
            await first.DeployAsync(CountsFromFiles("counts", "in/", "counts"));
            await Eventually(() => Same(sink.Rows, Counts.Model(trades.Take(60))), "sixty lines counted");
        }
        await PutAsync("in/a.jsonl", trades);
        await using var second = await StartAsync(cluster, "a", memory);
        await Eventually(() => Same(sink.Rows, Counts.Model(trades)), "the rest of the object counted, once");
        Assert.Equal(40, second.Pipelines.Get("counts").Sources.Single().Stats.Records);
    }

    [Fact]
    public async Task A_line_that_is_not_a_record_is_a_dead_letter_with_its_object_and_line_and_is_not_met_again()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        var trades = Trades(0, 20);
        var lines = trades.Select(t => JsonSerializer.Serialize(t, Web)).ToList();
        lines.Insert(7, "{ this is not json");
        await PutAsync("in/a.jsonl", lines);
        var spec = CountsFromFiles("counts", "in/", "counts") with { Circuit = new PipelineCircuitOptions { ErrorPolicy = RecordErrorPolicy.DeadLetter } };
        var sink = memory.UpsertSink<string, long>("counts");
        await using (var first = await StartAsync(cluster, "a", memory))
        {
            await first.DeployAsync(spec);
            await Eventually(() => Same(sink.Rows, Counts.Model(trades)), "the readable lines counted");
            var letter = Assert.Single(first.Pipelines.DeadLetters("counts"));
            Assert.Equal("input[trades]", letter.Operator);
            Assert.StartsWith("in/a.jsonl:8: { this is not json", letter.Record);
            Assert.Equal(1, first.Pipelines.Get("counts").Sources.Single().Stats.Unreadable);
        }
        await PutAsync("in/b.jsonl", Trades(20, 5));
        trades.AddRange(Trades(20, 5));
        await using var second = await StartAsync(cluster, "a", memory);
        await Eventually(() => Same(sink.Rows, Counts.Model(trades)), "the counts after the restart");
        Assert.Equal(0, second.Pipelines.Get("counts").DeadLetters);

        // Under the default policy the same line stops the pipeline at it.
        await PutAsync("strict/a.jsonl", lines);
        await second.DeployAsync(CountsFromFiles("strict", "strict/", "strict"));
        await Eventually(() => second.Pipelines.Get("strict").State == PipelineState.Failed, "the strict pipeline failing");
        Assert.Contains("strict/a.jsonl:8", second.Pipelines.Get("strict").Faults[^1].Message);
    }

    [Fact]
    public async Task A_keyed_input_takes_each_line_as_an_upsert()
    {
        await using var service = await StartAsync();
        await PutAsync("in/1.jsonl", [new Trade(1, 1, 5, "a"), new Trade(2, 1, 6, "b")]);
        await service.DeployAsync(Spec("latest", "latest-trades", null, "latest") with
        {
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Files("in/", "\"pollInterval\": \"00:00:00.05\", \"suffix\": \".jsonl\"") },
        });
        var sink = service.Memory.UpsertSink<long, Trade>("latest");
        await Eventually(() => sink.Rows.Count == 2, "two trades");
        await PutAsync("in/2.jsonl", [new Trade(1, 1, 50, "a")]);
        await PutAsync("in/3.ignored", [new Trade(2, 1, 60, "b")]);
        await Eventually(() => sink.Rows[1].Quantity == 50, "the later line for trade 1 replacing the earlier");
        Assert.Equal(6, sink.Rows[2].Quantity);
    }

    // ── the sinks ──────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task The_append_sink_holds_each_record_once_across_a_restart_and_drops_what_lies_above_the_checkpoint()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        var trades = Trades(0, 200);
        var log = memory.Log<Trade>("in");
        var spec = Spec("big", "big-trades", "in") with { Sinks = new Dictionary<string, JsonElement> { ["out"] = Files("out/big-") } };

        // Left by an earlier life of the prefix: a step far above anything committed.
        await PutAsync(FileSinkFormat.ObjectName("out/big-", 9_000, 1), [new Trade(-1, 1, 99, "ghost")]);
        await PutAsync("out/big-notes.txt", ["not a step object: left alone"]);

        long committed;
        await using (var first = await StartAsync(cluster, "a", memory))
        {
            await first.DeployAsync(spec);
            log.AppendRange(trades.Take(120));
            await EventuallyAsync(async () => (await AppendsAsync<Trade>("out/big-")).Order(ById).SequenceEqual(BigTrades.Model(trades.Take(120))), "the first run's records, and no ghost");
            committed = first.Pipelines.Get("big").CommittedStep!.Value;
            Assert.All(await FileSinkFormat.StepObjectsAsync(_store, "out/big-"), name =>
            {
                Assert.True(FileSinkFormat.TryParse("out/big-", name, out var step, out var term));
                Assert.InRange(step, 1, committed);
                Assert.Equal(first.Node.Status.Leadership.Term, term);
            });
        }

        // What a generation that died after writing and before committing leaves behind.
        await PutAsync(FileSinkFormat.ObjectName("out/big-", committed + 1, 1), [new Trade(-2, 1, 99, "uncommitted")]);
        await using var second = await StartAsync(cluster, "a", memory);
        await second.Running("big");

        // The new generation first commits steps that give the sink nothing (trades below
        // the threshold): the uncommitted object now lies under the committed step, and
        // must go all the same when the sink first writes.
        for (var i = 0; i < 3; i++)
        {
            var stepBefore = second.Pipelines.Get("big").CommittedStep ?? 0;
            log.Append(new Trade(10_000 + i, 1, 1, "small"));
            await Eventually(() => (second.Pipelines.Get("big").CommittedStep ?? 0) > stepBefore, "a step with no output committed");
        }
        Assert.True(second.Pipelines.Get("big").CommittedStep > committed + 1);
        log.AppendRange(trades.Skip(120));
        await EventuallyAsync(async () => (await AppendsAsync<Trade>("out/big-")).Order(ById).SequenceEqual(BigTrades.Model(trades)), "every record once after the restart");
        Assert.NotNull(await _store.StatAsync("out/big-notes.txt", default));
    }

    private static readonly IComparer<Trade> ById = Comparer<Trade>.Create((a, b) => a.Id.CompareTo(b.Id));

    private static async Task EventuallyAsync(Func<Task<bool>> condition, string what)
    {
        var deadline = DateTime.UtcNow + TestService.Timeout;
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
                Assert.Fail($"Timed out waiting for: {what}");
            await Task.Delay(20);
        }
    }

    [Fact]
    public async Task The_upsert_sink_s_objects_add_up_to_the_latest_row_per_key_deletes_included_and_a_reset_clears_them()
    {
        await using var service = await StartAsync();
        var instruments = Instruments(10);
        var table = await service.DeclareInstrumentsAsync(instruments);
        var trades = Trades(0, 150);
        var log = service.Memory.Log<Trade>("in");
        await service.DeployAsync(Spec("enrich", "enrich", "in") with { Sinks = new Dictionary<string, JsonElement> { ["out"] = Files("out/enriched/") } });
        log.AppendRange(trades);
        await EventuallyAsync(async () => Same(await FileSinkFormat.ReadUpsertsAsync<long, EnrichedTrade>(_store, "out/enriched/"), Enrich.Model(trades, instruments)), "the enriched rows");

        // An instrument goes: its trades leave the join, which the sink records as deletes.
        await table.Writer.DeleteAsync(3);
        await table.Writer.UpsertAsync(instruments[4] with { Multiplier = 9 });
        await table.Writer.CommitAsync();
        instruments[4] = instruments[4] with { Multiplier = 9 };
        instruments.RemoveAt(2);
        await EventuallyAsync(async () => Same(await FileSinkFormat.ReadUpsertsAsync<long, EnrichedTrade>(_store, "out/enriched/"), Enrich.Model(trades, instruments)), "the rows after the table changed");
        var text = string.Concat((await FileSinkFormat.StepObjectsAsync(_store, "out/enriched/")).Select(n => File.ReadAllText(Path.Combine(_root, n))));
        Assert.Contains("\"deleted\":true", text);

        var objectsBefore = (await FileSinkFormat.StepObjectsAsync(_store, "out/enriched/")).Count;
        await service.Pipelines.ResetAsync("enrich", new ActionOptions(), default);
        await service.Running("enrich");
        await EventuallyAsync(async () => Same(await FileSinkFormat.ReadUpsertsAsync<long, EnrichedTrade>(_store, "out/enriched/"), Enrich.Model(trades, instruments)), "the rows after the reset");
        Assert.True((await FileSinkFormat.StepObjectsAsync(_store, "out/enriched/")).Count < objectsBefore, "the reset left the old run's objects in place");
    }

    [Fact]
    public async Task A_minimum_interval_between_objects_makes_fewer_larger_objects_of_the_same_records()
    {
        await using var service = await StartAsync();
        var trades = Trades(0, 400);
        var log = service.Memory.Log<Trade>("in");
        await service.DeployAsync(Spec("big", "big-trades", "in") with
        {
            Sinks = new Dictionary<string, JsonElement> { ["out"] = Files("out/spaced-", "\"minInterval\": \"00:00:00.25\"") },
        });
        // Forty appends, 25 ms apart: unspaced, that is about forty steps and forty objects.
        for (var i = 0; i < 40; i++)
        {
            log.AppendRange(trades.Skip(i * 10).Take(10));
            await Task.Delay(25);
        }
        await EventuallyAsync(async () => (await AppendsAsync<Trade>("out/spaced-")).Order(ById).SequenceEqual(BigTrades.Model(trades)), "every record, in fewer objects");
        Assert.InRange((await FileSinkFormat.StepObjectsAsync(_store, "out/spaced-")).Count, 2, 12);
    }

    [Fact]
    public async Task Where_two_terms_wrote_the_same_step_the_reader_takes_the_higher_term()
    {
        await PutAsync(FileSinkFormat.ObjectName("out/", 1, 1), ["{\"key\":1,\"row\":\"first\"}"]);
        await PutAsync(FileSinkFormat.ObjectName("out/", 2, 1), ["{\"key\":1,\"row\":\"from the replaced leader\"}", "{\"key\":2,\"row\":\"ghost\"}"]);
        await PutAsync(FileSinkFormat.ObjectName("out/", 2, 2), ["{\"key\":1,\"row\":\"second\"}"]);
        var rows = await FileSinkFormat.ReadUpsertsAsync<int, string>(_store, "out/");
        Assert.Equal(new Dictionary<int, string> { [1] = "second" }, rows);
    }

    // ── failover ───────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task A_takeover_carries_the_file_position_and_the_sink_s_steps_to_the_new_leader()
    {
        var cluster = new InMemoryServiceCluster();
        var memory = new MemoryConnector();
        var trades = Trades(0, 600);
        for (var i = 0; i < 3; i++)
            await PutAsync($"in/{i:D4}.jsonl", trades.Skip(i * 100).Take(100));
        var counts = memory.UpsertSink<string, long>("counts");
        var big = Spec("big", "big-trades", null) with
        {
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Files("in/", "\"pollInterval\": \"00:00:00.05\"") },
            Sinks = new Dictionary<string, JsonElement> { ["out"] = Files("out/big-") },
        };

        await using var a = await StartAsync(cluster, "a", memory);
        await using var b = await StartAsync(cluster, "b", memory);
        await a.DeployAsync(CountsFromFiles("counts", "in/", "counts"));
        await a.DeployAsync(big);
        await Eventually(() => Same(counts.Rows, Counts.Model(trades.Take(300))), "the counts under the first leader");
        await EventuallyAsync(async () => (await AppendsAsync<Trade>("out/big-")).Order(ById).SequenceEqual(BigTrades.Model(trades.Take(300))), "the file sink under the first leader");
        await Eventually(() => b.Node.Status.Standby is { TakeoverWouldBeWarm: true }, "the standby being warm");

        // More objects land while leadership moves.
        var landing = Task.Run(async () =>
        {
            for (var i = 3; i < 6; i++)
            {
                await PutAsync($"in/{i:D4}.jsonl", trades.Skip(i * 100).Take(100));
                await Task.Delay(15);
            }
        });
        await b.Node.TakeOverAsync(new NodeActionOptions(), default);
        await landing;
        await b.Running("counts");
        await b.Running("big");

        await Eventually(() => Same(counts.Rows, Counts.Model(trades)), "the counts under the second leader: every line once");
        await EventuallyAsync(async () => (await AppendsAsync<Trade>("out/big-")).Order(ById).SequenceEqual(BigTrades.Model(trades)), "the file sink under the second leader: every record once");
        var read = b.Pipelines.Get("counts").Sources.Single().Stats.Records;
        Assert.InRange(read, 300, 599);                                         // resumed, not read again from the first object
        var terms = (await FileSinkFormat.StepObjectsAsync(_store, "out/big-"))
            .Select(n => FileSinkFormat.TryParse("out/big-", n, out _, out var term) ? term : -1).Distinct().Order().ToList();
        Assert.Equal([a.Node.Status.Leadership.Term, b.Node.Status.Leadership.Term], terms);
    }

    // ── config ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task The_config_is_closed_and_names_a_registered_store()
    {
        await using var service = await StartAsync();
        async Task<string> Refused(JsonElement input, JsonElement? sink = null)
        {
            var spec = Spec("p", "big-trades", null) with
            {
                Inputs = new Dictionary<string, JsonElement> { ["trades"] = input },
                Sinks = new Dictionary<string, JsonElement> { ["out"] = sink ?? Files("out/") },
            };
            var ex = await Assert.ThrowsAsync<ServiceException>(() => service.Pipelines.ValidateAsync(spec, default));
            return string.Join(" ", ex.Details);
        }

        Assert.Contains("where the store is belongs to the host", await Refused(Files("in/", "\"root\": \"/etc\"")));
        Assert.Contains("needs a \"prefix\"", await Refused(Json("""{ "connector": "files", "store": "landing" }""")));
        Assert.Contains("must be relative", await Refused(Files("/etc/")));
        Assert.Contains("must be relative", await Refused(Files("in/../../etc/")));
        Assert.Contains("no object store named 'nowhere' is registered (registered: landing)",
            await Refused(Json("""{ "connector": "files", "store": "nowhere", "prefix": "in/" }""")));
        Assert.Contains("pollInterval is a positive duration", await Refused(Files("in/", "\"pollInterval\": \"soon\"")));
        Assert.Contains("are a source's settings", await Refused(Files("in/"), Files("out/", "\"pollInterval\": \"00:00:01\"")));
        Assert.Contains("is a sink's setting", await Refused(Files("in/", "\"minInterval\": \"00:00:01\"")));

        var connector = Assert.Single(service.Node.Discovery.Connectors, c => c.Name == "files");
        Assert.Equal([SlotKind.AppendInput, SlotKind.KeyedInput, SlotKind.UpsertSink, SlotKind.AppendSink], connector.Kinds);
        Assert.True(connector.Resettable);
        Assert.False(connector.Config!.Open);
    }
}
