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
        var bytes = Encoding.UTF8.GetBytes(string.Join("\n", lines) + "\n");
        await _store.WriteAsync(name, (stream, ct) => stream.WriteAsync(bytes, ct), default);
    }

    private Task PutAsync(string name, IEnumerable<Trade> trades) => PutAsync(name, trades.Select(t => JsonSerializer.Serialize(t, Web)));

    private static bool Same<TKey, TRow>(IReadOnlyDictionary<TKey, TRow> actual, IReadOnlyDictionary<TKey, TRow> expected) where TKey : notnull =>
        actual.Count == expected.Count && expected.All(e => actual.TryGetValue(e.Key, out var row) && Equals(row, e.Value));

    private static PipelineSpec CountsFromFiles(string id, string prefix, string sink) =>
        Spec(id, "counts", null, sink) with
        {
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Files(prefix, "\"pollInterval\": \"00:00:00.05\", \"settle\": \"00:00:00\"") },
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
            // The letter reaches the node's ring by a consumer of its own: the sink can
            // have the last count a moment before it.
            await Eventually(() => first.Pipelines.DeadLetters("counts").Count > 0, "the dead letter");
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
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Files("in/", "\"pollInterval\": \"00:00:00.05\", \"settle\": \"00:00:00\", \"suffix\": \".jsonl\"") },
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
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Files("in/", "\"pollInterval\": \"00:00:00.05\", \"settle\": \"00:00:00\"") },
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
        var objects = (await FileSinkFormat.StepObjectsAsync(_store, "out/big-"))
            .Select(n => FileSinkFormat.TryParse("out/big-", n, out FileSinkFormat.StepObject parsed) ? parsed : default).ToList();
        Assert.Equal([a.Node.Status.Leadership.Term, b.Node.Status.Leadership.Term], objects.Select(o => o.Term).Distinct().Order());
        // Each object's name carries the checkpoint its writer started from: nothing for
        // the first leader, and for the second a step the first had committed — which is
        // what voids anything the first might still write above it.
        Assert.All(objects.Where(o => o.Term == a.Node.Status.Leadership.Term), o => Assert.Equal(0, o.Base));
        var second = objects.Where(o => o.Term == b.Node.Status.Leadership.Term).ToList();
        Assert.Single(second.Select(o => o.Base).Distinct());
        Assert.InRange(second[0].Base, 1, second.Min(o => o.Step) - 1);
    }

    // ── the reader on its own: what a checkpoint may hold, and what a listing may miss ──

    /// <summary>An input as the reader sees it: a position per partition that admits only
    /// what is above it (<c>InputPosition.Admit</c>), and the lines that got through.</summary>
    private sealed class FakeInput
    {
        public readonly Dictionary<int, long> Position = [];
        public readonly List<string> Lines = [];
        public readonly List<(FileRecord Record, string Message)> Rejected = [];
        public Func<string, Task>? OnLine;
        public int StopAfterAdvances = int.MaxValue;
        private int _advances;

        private bool Admit(int partition, long offset)
        {
            lock (Position)
            {
                if (Position.TryGetValue(partition, out var held) && offset <= held)
                    return false;
                Position[partition] = offset;
                return true;
            }
        }

        public Dictionary<int, long> Checkpoint()
        {
            lock (Position)
                return new Dictionary<int, long>(Position);
        }

        public FileSourceReader Read(LocalObjectStore store, string prefix, IReadOnlyDictionary<int, long> committed, string more = "") =>
            new(store, FilesSlotConfig.Parse(Files(prefix, "\"settle\": \"00:00:00\", \"pollInterval\": \"00:00:00.02\"" + more)), committed,
                async (text, offset, stop) =>
                {
                    if (OnLine is { } hook)
                        await hook(text).WaitAsync(stop);
                    if (Admit(0, offset))
                        lock (Lines)
                            Lines.Add(text);
                },
                (record, error, offset, _) =>
                {
                    if (Admit(0, offset))
                        lock (Rejected)
                            Rejected.Add(((FileRecord)record!, error.Message));
                    return ValueTask.CompletedTask;
                },
                async (partition, offset, _) =>
                {
                    if (Interlocked.Increment(ref _advances) > StopAfterAdvances)
                        await Task.Delay(System.Threading.Timeout.Infinite, _);          // the node dies here, as far as the checkpoint knows
                    Admit(partition, offset);
                });

        public int Count
        {
            get
            {
                lock (Lines)
                    return Lines.Count;
            }
        }
    }

    private static IEnumerable<string> Numbered(string name, int count) => Enumerable.Range(0, count).Select(i => $"\"{name} {i}\"");

    [Fact]
    public async Task A_checkpoint_holding_half_of_a_name_does_not_spoil_the_position_when_another_object_is_read_next()
    {
        // The position is 05. The source moves to 07 and a checkpoint takes the first
        // four parts of that name — then the node goes, and 06 arrives while it is away.
        await PutAsync("quotes/00000005.jsonl", Numbered("05", 3));
        var input = new FakeInput();
        await using (var first = input.Read(_store, "quotes/", new Dictionary<int, long>()))
            await Eventually(() => input.Count == 3, "the first object read");
        await PutAsync("quotes/00000007.jsonl", Numbered("07", 80));
        var before = input.Checkpoint();
        input = new FakeInput { StopAfterAdvances = 4 };
        foreach (var (partition, offset) in before)
            input.Position[partition] = offset;
        await using (var dying = input.Read(_store, "quotes/", before))
            await Eventually(() => input.Checkpoint().Count >= before.Count + 1 && input.Checkpoint().Values.Max() >> 32 == 2, "half of the next name committed");
        var half = input.Checkpoint();
        Assert.Equal("quotes/00000005.jsonl", FilePosition.Decode(half)!.Value.Name);

        // The new generation reads 06 — not the object whose name is half committed — and
        // stops 40 lines into it.
        await PutAsync("quotes/00000006.jsonl", Numbered("06", 80));
        var second = new FakeInput();
        foreach (var (partition, offset) in half)
            second.Position[partition] = offset;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        second.OnLine = text => text == "\"06 40\"" ? gate.Task : Task.CompletedTask;
        Dictionary<int, long> inside;
        await using (var reader = second.Read(_store, "quotes/", half))
        {
            await Eventually(() => second.Count == 40, "forty lines of 06 read");
            inside = second.Checkpoint();
        }
        Assert.Equal(("quotes/00000006.jsonl", 39L), (FilePosition.Decode(inside)!.Value.Name, FilePosition.Decode(inside)!.Value.Line));

        // And a third carries on inside 06, then reads 07 whole: every line once.
        var third = new FakeInput();
        foreach (var (partition, offset) in inside)
            third.Position[partition] = offset;
        await using (var reader = third.Read(_store, "quotes/", inside))
            await Eventually(() => third.Count == 40 + 80, "the rest of 06 and all of 07");
        Assert.Equal(Numbered("06", 80).Skip(40).Concat(Numbered("07", 80)), third.Lines);
        Assert.Equal("quotes/00000007.jsonl:80", FilePosition.Describe(third.Checkpoint()));
    }

    [Fact]
    public void Ordinals_stay_above_everything_sent_and_alternate_banks()
    {
        Assert.Equal(1, FilePosition.Next(0, 0));
        Assert.Equal(2, FilePosition.Next(1, 1));
        Assert.Equal(4, FilePosition.Next(1, 2));       // 2 was half sent: 3 would overwrite the bank of 1
        Assert.Equal(3, FilePosition.Next(0, 1));       // nothing whole yet, half of 1 sent
        Assert.Equal(7, FilePosition.Next(4, 6));
        Assert.Equal(0, FilePosition.Highest(new Dictionary<int, long>()));
        Assert.Equal(5, FilePosition.Highest(new Dictionary<int, long> { [1] = (4L << 32) | 9, [70] = (5L << 32) | 123, [0] = (4L << 32) | 2 }));
    }

    [Fact]
    public async Task An_object_that_arrives_among_the_names_already_listed_is_still_read()
    {
        for (var i = 1; i <= 5; i++)
            await PutAsync($"in/{i:D4}.jsonl", Numbered($"{i:D4}", 2));
        var input = new FakeInput();
        var arrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // The source stands in 0001, held up by its input, with 0002 … 0005 listed.
        input.OnLine = text => text == "\"0001 1\"" ? arrived.Task : Task.CompletedTask;
        await using var reader = input.Read(_store, "in/", new Dictionary<int, long>());
        await Eventually(() => input.Count == 1, "the source inside the first object");
        await PutAsync("in/0003a.jsonl", Numbered("0003a", 2));         // sorts after the position, inside what was listed
        await Task.Delay(60);                                            // more than a poll interval
        arrived.SetResult();
        await Eventually(() => input.Count == 12, "all six objects read");
        Assert.Equal(
            new[] { "0001", "0002", "0003", "0003a", "0004", "0005" }.SelectMany(n => Numbered(n, 2)),
            input.Lines);
    }

    [Fact]
    public async Task A_line_longer_than_the_limit_is_rejected_in_its_place_without_being_held()
    {
        // 40 MB with no line end, between two records: a binary, an export that is one JSON array.
        var path = Path.Combine(_root, "in");
        Directory.CreateDirectory(path);
        await using (var file = File.Create(Path.Combine(path, "a.jsonl")))
        {
            await file.WriteAsync(Encoding.UTF8.GetBytes("\"before\"\n"));
            var block = Encoding.UTF8.GetBytes(new string('x', 1 << 20));
            for (var i = 0; i < 40; i++)
                await file.WriteAsync(block);
            await file.WriteAsync(Encoding.UTF8.GetBytes("\r\n\"after\"\n"));
        }
        var input = new FakeInput();
        await using (var reader = input.Read(_store, "in/", new Dictionary<int, long>(), ", \"maxLineLength\": 1000"))
            await Eventually(() => input.Count == 2, "the records on both sides of the long line");
        Assert.Equal(["\"before\"", "\"after\""], input.Lines);
        var (record, message) = Assert.Single(input.Rejected);
        Assert.Equal(("in/a.jsonl", 1L, BoundedLineReader.Kept), (record.Object, record.Line, record.Text.Length));
        Assert.Contains("in/a.jsonl:2 is a line of 41943040 characters", message);
        Assert.Contains("maxLineLength", message);
    }

    /// <summary>A text of one character repeated, never held anywhere.</summary>
    private sealed class Repeated(char character, long count, string then) : TextReader
    {
        private long _left = count;
        private int _tail;

        public override ValueTask<int> ReadAsync(Memory<char> buffer, CancellationToken cancellationToken = default)
        {
            if (_left > 0)
            {
                var n = (int)Math.Min(_left, buffer.Length);
                buffer.Span[..n].Fill(character);
                _left -= n;
                return new(n);
            }
            var rest = Math.Min(then.Length - _tail, buffer.Length);
            then.AsSpan(_tail, rest).CopyTo(buffer.Span);
            _tail += rest;
            return new(rest);
        }
    }

    [Fact]
    public async Task A_line_of_any_length_is_read_past_holding_no_more_than_the_limit()
    {
        // Half a billion characters and no line end: as a string it could not exist.
        const int limit = 100_000;
        var reader = new BoundedLineReader(new Repeated('x', 500_000_000, "\nnext\n"), limit);
        var huge = (await reader.ReadLineAsync(default))!.Value;
        Assert.Equal((500_000_000L, true, BoundedLineReader.Kept), (huge.Length, huge.TooLong, huge.Text.Length));
        Assert.InRange(reader.Held, 0, 2 * limit);
        Assert.Equal(new LineRead("next", 4, false), await reader.ReadLineAsync(default));
        Assert.InRange(reader.Held, 0, 2 * limit);
        Assert.Null(await reader.ReadLineAsync(default));
    }

    [Fact]
    public async Task Lines_end_as_a_text_reader_s_do_and_a_limit_counts_characters()
    {
        var text = "one\ntwo\r\nthree\rfour\n\n" + new string('y', 5000) + "\nlast";
        var expected = new List<string>();
        using (var plain = new StringReader(text))
            while (plain.ReadLine() is { } line)
                expected.Add(line);
        var reader = new BoundedLineReader(new StringReader(text), 4999);
        var read = new List<LineRead>();
        while (await reader.ReadLineAsync(default) is { } line)
            read.Add(line);
        Assert.Equal(expected.Count, read.Count);
        Assert.Equal(expected.Select(l => l.Length > 4999 ? l[..BoundedLineReader.Kept] : l), read.Select(l => l.Text));
        Assert.Equal(expected.Select(l => (long)l.Length), read.Select(l => l.Length));
        Assert.Equal(expected.Select(l => l.Length > 4999), read.Select(l => l.TooLong));

        // At the limit exactly: whole.
        var exact = new BoundedLineReader(new StringReader(new string('z', 5000)), 5000);
        Assert.Equal(new LineRead(new string('z', 5000), 5000, false), await exact.ReadLineAsync(default));
        Assert.Null(await exact.ReadLineAsync(default));
    }

    // ── the store ──────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task A_write_that_fails_or_is_cancelled_part_way_leaves_nothing_under_the_name()
    {
        await _store.WriteAsync("out/a.jsonl", (stream, ct) => stream.WriteAsync("whole\n"u8.ToArray(), ct), default);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await _store.WriteAsync("out/a.jsonl", async (stream, ct) =>
        {
            await stream.WriteAsync("half of a li"u8.ToArray(), ct);
            throw new InvalidOperationException("a row that does not serialize");
        }, default));
        using var cancel = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await _store.WriteAsync("out/b.jsonl", async (stream, ct) =>
        {
            await stream.WriteAsync("half"u8.ToArray(), ct);
            cancel.Cancel();
            ct.ThrowIfCancellationRequested();
        }, cancel.Token));

        Assert.Equal("whole\n", File.ReadAllText(Path.Combine(_root, "out", "a.jsonl")));
        Assert.Null(await _store.StatAsync("out/b.jsonl", default));
        Assert.Equal(["a.jsonl"], Directory.GetFiles(Path.Combine(_root, "out")).Select(Path.GetFileName));      // no temporary left either
    }

    [Fact]
    public async Task A_listing_walks_the_directory_its_prefix_names_and_matches_by_prefix()
    {
        foreach (var name in new[] { "in/0002.jsonl", "in/0001.jsonl", "in/2026/01/a.jsonl", "inside/x.jsonl", "out/1.jsonl", "top.jsonl" })
            await PutAsync(name, ["{}"]);
        async Task<List<string>> List(string prefix)
        {
            var names = new List<string>();
            await foreach (var info in _store.ListAsync(prefix, default))
                names.Add(info.Path);
            return names;
        }
        Assert.Equal(["in/0001.jsonl", "in/0002.jsonl", "in/2026/01/a.jsonl"], await List("in/"));
        Assert.Equal(["in/0001.jsonl", "in/0002.jsonl"], await List("in/000"));
        Assert.Equal(["in/0001.jsonl", "in/0002.jsonl", "in/2026/01/a.jsonl", "inside/x.jsonl"], await List("in"));
        Assert.Equal(["in/2026/01/a.jsonl"], await List("in/2026/"));
        Assert.Empty(await List("nowhere/"));
        Assert.Equal(6, (await List("")).Count);
        await Assert.ThrowsAsync<ArgumentException>(() => List("../x/"));

        // A file the store could not even stat lies outside the prefix: it is not looked at.
        Directory.CreateDirectory(Path.Combine(_root, "other"));
        File.CreateSymbolicLink(Path.Combine(_root, "other", "loop"), Path.Combine(_root, "other"));
        Assert.Equal(["out/1.jsonl"], await List("out/"));
    }

    // ── one prefix, one sink ───────────────────────────────────────────────────────────

    [Fact]
    public async Task A_prefix_another_sink_writes_is_refused_at_the_dry_run_and_the_deploy()
    {
        await using var service = await StartAsync();
        var log = service.Memory.Log<Trade>("in");
        PipelineSpec Big(string id, string prefix) =>
            Spec(id, "big-trades", "in") with { Sinks = new Dictionary<string, JsonElement> { ["out"] = Files(prefix) } };
        await service.DeployAsync(Big("a", "out/a/"));
        log.AppendRange(Trades(0, 100));
        await EventuallyAsync(async () => (await AppendsAsync<Trade>("out/a/")).Count == BigTrades.Model(Trades(0, 100)).Count, "a's output");
        var objects = await FileSinkFormat.StepObjectsAsync(_store, "out/a/");

        // Another pipeline onto a's prefix: its first write would delete a's objects.
        foreach (var dryRun in new[] { true, false })
        {
            var refused = await Assert.ThrowsAsync<ServiceException>(() => service.Pipelines.DeployAsync(Big("b", "out/a/"), new DeployOptions(DryRun: dryRun), default));
            Assert.Contains(refused.Details, d => d.Contains("sink slot 'out'") && d.Contains("pipeline 'a' writes from its sink slot 'out'") && d.Contains("files:landing:out/a/"));
        }
        // Its dead letters there, likewise; and one spec's two sinks onto one prefix.
        var letters = await Assert.ThrowsAsync<ServiceException>(() => service.Pipelines.ValidateAsync(Big("b", "out/b/") with { DeadLetters = Files("out/a/") }, default));
        Assert.Contains(letters.Details, d => d.StartsWith("dead-letter slot") && d.Contains("pipeline 'a'"));
        var twice = await Assert.ThrowsAsync<ServiceException>(() => service.Pipelines.ValidateAsync(Big("b", "out/b/") with { DeadLetters = Files("out/b/") }, default));
        Assert.Contains(twice.Details, d => d.Contains("this spec's sink slot 'out'"));

        Assert.Equal(objects, await FileSinkFormat.StepObjectsAsync(_store, "out/a/"));
        Assert.DoesNotContain(service.Pipelines.Pipelines, p => p.Id == "b");

        // Its own prefix is its own across a replace; a neighbouring prefix is free.
        await service.DeployAsync(Big("a", "out/a/") with { Circuit = new PipelineCircuitOptions { CheckpointInterval = 2 } });
        await service.DeployAsync(Big("b", "out/a-2/"));
        await service.Running("b");
    }

    [Fact]
    public async Task An_input_onto_what_a_files_sink_writes_is_refused_and_so_is_a_sink_onto_what_an_input_reads()
    {
        // A sink's objects are not a log of names: a step run again is written under the
        // name it had, compactAfter folds objects away and leaves deleted keys out of the
        // snapshot, retain prunes. A source reads on by name and would see none of it.
        await using var service = await StartAsync();
        const string fast = "\"pollInterval\": \"00:00:00.05\", \"settle\": \"00:00:00\"";
        PipelineSpec Big(string id, string sink, JsonElement? input = null) => Spec(id, "big-trades", "in") with
        {
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = input ?? MemoryBinding("in") },
            Sinks = new Dictionary<string, JsonElement> { ["out"] = Files(sink) },
        };
        await service.DeployAsync(Big("a", "lake/a/"));

        // The sink's own prefix, and a prefix its objects lie under.
        foreach (var prefix in new[] { "lake/a/", "lake/", "lake/a" })
        foreach (var dryRun in new[] { true, false })
        {
            var refused = await Assert.ThrowsAsync<ServiceException>(
                () => service.Pipelines.DeployAsync(Big("b", "lake/b/", Files(prefix, fast)), new DeployOptions(DryRun: dryRun), default));
            Assert.Contains(refused.Details, d => d.StartsWith("input slot 'trades'") && d.Contains("pipeline 'a' writes from its sink slot 'out'")
                && d.Contains("files:landing:lake/a/") && d.Contains("Kafka topic or a table"));
        }
        Assert.DoesNotContain(service.Pipelines.Pipelines, p => p.Id == "b");

        // One spec reading what it writes itself.
        var own = await Assert.ThrowsAsync<ServiceException>(() => service.Pipelines.ValidateAsync(Big("c", "loop/", Files("loop/", fast)), default));
        Assert.Contains(own.Details, d => d.StartsWith("sink slot 'out'") && d.Contains("this spec's input slot 'trades' reads it"));

        // The other order: the reader is there first, and a sink onto what it reads is refused.
        await service.DeployAsync(Big("reader", "lake/reader-out/", Files("landing/", fast)));
        foreach (var dryRun in new[] { true, false })
        {
            var refused = await Assert.ThrowsAsync<ServiceException>(
                () => service.Pipelines.DeployAsync(Big("d", "landing/d/"), new DeployOptions(DryRun: dryRun), default));
            Assert.Contains(refused.Details, d => d.StartsWith("sink slot 'out'") && d.Contains("pipeline 'reader' reads it as its input slot 'trades'") && d.Contains("files:landing:landing/"));
        }
        var letters = await Assert.ThrowsAsync<ServiceException>(() => service.Pipelines.ValidateAsync(Big("d", "lake/d/") with { DeadLetters = Files("landing/dead/") }, default));
        Assert.Contains(letters.Details, d => d.StartsWith("dead-letter slot") && d.Contains("pipeline 'reader' reads it"));

        // What is not a sink's: a neighbouring prefix, a prefix below the sink's (its
        // objects are not under it), and a source whose suffix no sink object can end in.
        await service.Pipelines.ValidateAsync(Big("e", "lake/e/", Files("lake/a-in/", fast)), default);
        await service.Pipelines.ValidateAsync(Big("e", "lake/e/", Files("lake/a/in/", fast)), default);
        await service.Pipelines.ValidateAsync(Big("e", "lake/e/", Files("lake/a/", fast + ", \"suffix\": \".csv\"")), default);
        foreach (var suffix in new[] { ".jsonl", "l", "0.jsonl", ".snapshot.jsonl" })
            await Assert.ThrowsAsync<ServiceException>(() => service.Pipelines.ValidateAsync(Big("e", "lake/e/", Files("lake/a/", fast + $", \"suffix\": \"{suffix}\"")), default));
    }

    [Fact]
    public async Task A_dead_letter_names_a_files_input_s_position_as_an_object_and_a_line()
    {
        await using var service = await StartAsync();
        var trades = Trades(0, 40);
        for (var i = 9; i < trades.Count; i += 10)
            trades[i] = trades[i] with { Quantity = -1 };
        await PutAsync("in/2026-11-11/part-0001.jsonl", trades.Take(20));
        await service.DeployAsync(Spec("p", "fragile", null, "out") with
        {
            Circuit = new PipelineCircuitOptions { ErrorPolicy = RecordErrorPolicy.DeadLetter },
            DeadLetters = MemoryBinding("dlq"),
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Files("in/", "\"pollInterval\": \"00:00:00.05\", \"settle\": \"00:00:00\"") },
        });
        var sink = service.Memory.AppendSink<DeadLetterEnvelope>("dlq");
        await Eventually(() => sink.Records.Count == 2, "the first object's dropped records");
        await Eventually(() => service.Pipelines.Get("p").CommittedOffsets.Count > 0, "a checkpoint");
        await PutAsync("in/2026-11-11/part-0002.jsonl", trades.Skip(20));
        await Eventually(() => sink.Records.Count == 4, "the second object's dropped records");

        // The later ones were dropped after a checkpoint: it is named as a person reads it,
        // and none of the partitions the name is spread over is shown.
        var later = sink.Records[^1];
        Assert.Matches(@"^in/2026-11-11/part-000[12]\.jsonl:\d+$", later.AfterAt!["trades"]);
        Assert.True(later.After is null || !later.After.Keys.Any(k => k.StartsWith("trades#")), "the files input's raw partitions are in the envelope");
        Assert.All(sink.Records, e => Assert.True(e.After is null || e.After.Count < 4));
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
        Assert.Contains("maxLineLength is a number of characters from 1", await Refused(Files("in/", "\"maxLineLength\": 0")));
        Assert.Contains("are a source's settings", await Refused(Files("in/"), Files("out/", "\"maxLineLength\": 100")));

        var connector = Assert.Single(service.Node.Discovery.Connectors, c => c.Name == "files");
        Assert.Equal([SlotKind.AppendInput, SlotKind.KeyedInput, SlotKind.UpsertSink, SlotKind.AppendSink], connector.Kinds);
        Assert.True(connector.Resettable);
        Assert.False(connector.Config!.Open);
    }

    // ── listing: a page after the position, not the prefix ─────────────────────────────

    /// <summary>A store that records how it is listed, and can run something just before
    /// an object is opened. <paramref name="paged"/> false: a store written before
    /// <see cref="IObjectStore.ListPageAsync"/>, which has only the whole listing.</summary>
    private class RecordingStore(LocalObjectStore inner) : IObjectStore
    {
        public readonly List<string?> PagesAfter = [];
        public int WholeListings;
        public Func<string, Task>? BeforeOpen;

        public ValueTask<ObjectInfo?> StatAsync(string path, CancellationToken cancellationToken) => inner.StatAsync(path, cancellationToken);

        public async ValueTask<Stream> OpenReadAsync(string path, CancellationToken cancellationToken)
        {
            if (BeforeOpen is { } hook)
                await hook(path);
            return await inner.OpenReadAsync(path, cancellationToken);
        }

        public async ValueTask WriteAsync(string path, Func<Stream, CancellationToken, ValueTask> write, CancellationToken cancellationToken)
        {
            await inner.WriteAsync(path, write, cancellationToken);
        }

        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken) => inner.DeleteAsync(path, cancellationToken);

        public IAsyncEnumerable<ObjectInfo> ListAsync(string prefix, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref WholeListings);
            return inner.ListAsync(prefix, cancellationToken);
        }

        protected ValueTask<ObjectPage> Page(string prefix, string? after, int limit, CancellationToken cancellationToken)
        {
            lock (PagesAfter)
                PagesAfter.Add(after);
            return inner.ListPageAsync(prefix, after, limit, cancellationToken);
        }
    }

    private sealed class PagingStore(LocalObjectStore inner) : RecordingStore(inner), IObjectStore
    {
        public ValueTask<ObjectPage> ListPageAsync(string prefix, string? after, int limit, CancellationToken cancellationToken) => Page(prefix, after, limit, cancellationToken);
    }

    private FileSourceReader Read(IObjectStore store, FakeInput input, string prefix, string more) =>
        new(store, FilesSlotConfig.Parse(Files(prefix, "\"pollInterval\": \"00:00:00.02\"" + more)), new Dictionary<int, long>(),
            (text, offset, _) =>
            {
                lock (input.Lines)
                    input.Lines.Add(text);
                return ValueTask.CompletedTask;
            },
            (_, _, _, _) => ValueTask.CompletedTask,
            (_, _, _) => ValueTask.CompletedTask);

    [Fact]
    public async Task A_poll_asks_for_the_names_after_the_one_it_is_at_and_never_for_the_whole_prefix()
    {
        for (var i = 1; i <= 4; i++)
            await PutAsync($"in/{i:D4}.jsonl", Numbered($"{i:D4}", 2));
        var store = new PagingStore(_store);
        var input = new FakeInput();
        await using var reader = Read(store, input, "in/", ", \"settle\": \"00:00:00\"");
        await Eventually(() => input.Count == 8, "the four objects read");
        int polls;
        lock (store.PagesAfter)
            polls = store.PagesAfter.Count;
        await Eventually(() => { lock (store.PagesAfter) return store.PagesAfter.Count >= polls + 3; }, "three more polls");
        lock (store.PagesAfter)
        {
            Assert.Null(store.PagesAfter[0]);                                   // the first: from the start of the prefix
            Assert.All(store.PagesAfter.Skip(polls), after => Assert.Equal("in/0004.jsonl", after));
        }
        Assert.Equal(0, store.WholeListings);

        await PutAsync("in/0005.jsonl", Numbered("0005", 2));
        await Eventually(() => input.Count == 10, "the object that landed after the position");
    }

    [Fact]
    public async Task A_store_without_a_paged_listing_is_paged_over_its_whole_listing()
    {
        for (var i = 1; i <= 10; i++)
            await PutAsync($"in/{i:D4}.jsonl", Numbered($"{i:D4}", 1));
        await PutAsync("other/0001.jsonl", Numbered("other", 1));
        IObjectStore store = new RecordingStore(_store);                        // implements ListAsync only

        var first = await store.ListPageAsync("in/", null, 4, default);
        Assert.Equal(["in/0001.jsonl", "in/0002.jsonl", "in/0003.jsonl", "in/0004.jsonl"], first.Objects.Select(o => o.Path));
        Assert.True(first.More);
        var second = await store.ListPageAsync("in/", "in/0004.jsonl", 4, default);
        Assert.Equal(["in/0005.jsonl", "in/0006.jsonl", "in/0007.jsonl", "in/0008.jsonl"], second.Objects.Select(o => o.Path));
        Assert.True(second.More);
        var last = await store.ListPageAsync("in/", "in/0008.jsonl", 4, default);
        Assert.Equal(["in/0009.jsonl", "in/0010.jsonl"], last.Objects.Select(o => o.Path));
        Assert.False(last.More);
        Assert.Empty((await store.ListPageAsync("in/", "in/0010.jsonl", 4, default)).Objects);

        // The local store's own paging says the same.
        var local = await _store.ListPageAsync("in/", "in/0004.jsonl", 4, default);
        Assert.Equal(second.Objects.Select(o => o.Path), local.Objects.Select(o => o.Path));
        Assert.True(local.More);
        Assert.False((await _store.ListPageAsync("in/", "in/0006.jsonl", 4, default)).More);

        // And the source reads through it, in order.
        var input = new FakeInput();
        await using var reader = Read(store, input, "in/", ", \"settle\": \"00:00:00\"");
        await Eventually(() => input.Count == 10, "every object read through the whole listing");
        Assert.Equal(Enumerable.Range(1, 10).SelectMany(i => Numbered($"{i:D4}", 1)), input.Lines);
    }

    // ── settle: how far behind a greater name an object may arrive ─────────────────────

    [Fact]
    public async Task An_object_that_arrives_behind_a_greater_name_within_the_settle_time_is_read_in_its_place()
    {
        var input = new FakeInput();
        await using var reader = Read(_store, input, "in/", ", \"settle\": \"00:00:01.5\"");
        await PutAsync("in/0002.jsonl", Numbered("0002", 2));
        await Task.Delay(400);
        Assert.Equal(0, input.Count);                                           // seen, and left to settle
        await PutAsync("in/0001.jsonl", Numbered("0001", 2));                   // behind it in name order, 0.4 s late
        await Eventually(() => input.Count == 4, "both objects read");
        Assert.Equal(Numbered("0001", 2).Concat(Numbered("0002", 2)), input.Lines);
    }

    [Fact]
    public async Task Nothing_is_read_past_an_object_that_has_not_settled()
    {
        // 0003 has been there for a minute; 0001 has just landed. Reading 0003 now would
        // put the position past 0001's place before 0001 may be read.
        await PutAsync("in/0003.jsonl", Numbered("0003", 1));
        File.SetLastWriteTimeUtc(Path.Combine(_root, "in", "0003.jsonl"), DateTime.UtcNow.AddMinutes(-1));
        await PutAsync("in/0001.jsonl", Numbered("0001", 1));
        var input = new FakeInput();
        await using var reader = Read(_store, input, "in/", ", \"settle\": \"00:00:01\"");
        await Task.Delay(300);
        Assert.Equal(0, input.Count);
        Assert.Equal(2, reader.Stats.Lag);                                      // both seen, neither read
        await Eventually(() => input.Count == 2, "both read once the first has settled");
        Assert.Equal(Numbered("0001", 1).Concat(Numbered("0003", 1)), input.Lines);
    }

    /// <summary>A store whose clock is not the node's: every modification time it
    /// reports is shifted, as a file server's or a bucket's would be.</summary>
    private sealed class SkewedStore(LocalObjectStore inner, TimeSpan skew) : IObjectStore
    {
        private ObjectInfo Shift(ObjectInfo info) => info with { Modified = info.Modified + skew };

        public async ValueTask<ObjectInfo?> StatAsync(string path, CancellationToken cancellationToken) =>
            await inner.StatAsync(path, cancellationToken) is { } info ? Shift(info) : null;

        public ValueTask<Stream> OpenReadAsync(string path, CancellationToken cancellationToken) => inner.OpenReadAsync(path, cancellationToken);
        public ValueTask WriteAsync(string path, Func<Stream, CancellationToken, ValueTask> write, CancellationToken cancellationToken) => inner.WriteAsync(path, write, cancellationToken);
        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken) => inner.DeleteAsync(path, cancellationToken);

        public async IAsyncEnumerable<ObjectInfo> ListAsync(string prefix, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var info in inner.ListAsync(prefix, cancellationToken))
                yield return Shift(info);
        }

        public async ValueTask<ObjectPage> ListPageAsync(string prefix, string? after, int limit, CancellationToken cancellationToken)
        {
            var page = await inner.ListPageAsync(prefix, after, limit, cancellationToken);
            return new ObjectPage([.. page.Objects.Select(Shift)], page.More);
        }
    }

    [Theory]
    [InlineData(-6)]    // the store's clock six seconds behind the node's: every new object "is six seconds old"
    [InlineData(-3600)]
    [InlineData(90)]    // and ahead of it: every new object "was written in the future"
    public async Task The_settle_time_does_not_depend_on_the_nodes_clock_agreeing_with_the_stores(int skewSeconds)
    {
        var store = new SkewedStore(_store, TimeSpan.FromSeconds(skewSeconds));
        var input = new FakeInput();
        await using var reader = Read(store, input, "in/", ", \"settle\": \"00:00:01.5\"");
        var clock = System.Diagnostics.Stopwatch.StartNew();
        await PutAsync("in/0002.jsonl", Numbered("0002", 2));
        await Task.Delay(400);
        // Behind the node's clock, the object must not pass for settled; it is 0.4 s old.
        Assert.Equal(0, input.Count);
        await PutAsync("in/0001.jsonl", Numbered("0001", 2));                   // behind it in name order, 0.4 s late
        await Eventually(() => input.Count == 4, "both objects read");
        Assert.Equal(Numbered("0001", 2).Concat(Numbered("0002", 2)), input.Lines);
        // Ahead of it, the objects must not wait out the difference as well.
        Assert.InRange(clock.Elapsed.TotalSeconds, 1.5, 6);

        // And a backlog is not made to wait object by object: these are old by the
        // store's own stamps the moment one newer than them is listed.
        for (var i = 3; i <= 40; i++)
            await PutAsync($"in/{i:D4}.jsonl", Numbered($"{i:D4}", 1));
        await Task.Delay(1600);
        clock.Restart();
        await PutAsync("in/0041.jsonl", Numbered("0041", 1));
        await Eventually(() => input.Count == 4 + 38, "the backlog read");
        Assert.InRange(clock.Elapsed.TotalSeconds, 0, 1.2);
        await Eventually(() => input.Count == 4 + 39, "and the newest once it has settled");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-3600)]
    public async Task A_backlog_that_is_old_by_the_stores_own_stamps_is_read_at_once_by_a_reader_that_has_only_just_seen_it(int skewSeconds)
    {
        // A reader that starts on a prefix has seen nothing for any time at all. What
        // was written a minute before the newest object is old by the store's own
        // account, and is not made to wait out a settle time of this reader's: with
        // only the reader's own sightings to go by, a backlog would wait once per
        // listing of it.
        for (var i = 1; i <= 3; i++)
        {
            await PutAsync($"in/{i:D4}.jsonl", Numbered($"{i:D4}", 1));
            File.SetLastWriteTimeUtc(Path.Combine(_root, "in", $"{i:D4}.jsonl"), DateTime.UtcNow.AddMinutes(-1));
        }
        await PutAsync("in/0004.jsonl", Numbered("0004", 1));
        var input = new FakeInput();
        var clock = System.Diagnostics.Stopwatch.StartNew();
        await using var reader = Read(new SkewedStore(_store, TimeSpan.FromSeconds(skewSeconds)), input, "in/", ", \"settle\": \"00:00:03\"");
        await Eventually(() => input.Count == 3, "the backlog read");
        Assert.InRange(clock.Elapsed.TotalSeconds, 0, 1.5);                     // not after the three seconds
        await Task.Delay(300);
        Assert.Equal(3, input.Count);                                           // the newest waits out its own
        await Eventually(() => input.Count == 4, "the newest once it has settled");
        Assert.InRange(clock.Elapsed.TotalSeconds, 3, 9);
        Assert.Equal(Enumerable.Range(1, 4).SelectMany(i => Numbered($"{i:D4}", 1)), input.Lines);
    }

    [Fact]
    public async Task An_object_that_changes_while_it_settles_waits_from_the_change()
    {
        var input = new FakeInput();
        await using var reader = Read(_store, input, "in/", ", \"settle\": \"00:00:01\"");
        await PutAsync("in/0001.jsonl", Numbered("0001", 1));
        await Task.Delay(700);
        await PutAsync("in/0001.jsonl", Numbered("0001", 3));                   // written again: another object under the name
        await Task.Delay(600);
        Assert.Equal(0, input.Count);                                           // 1.3 s after the first write, 0.6 s after the second
        await Eventually(() => input.Count == 3, "the object as it was last written");
    }

    [Fact]
    public async Task With_no_settle_time_an_object_that_arrives_behind_the_position_is_not_read()
    {
        // The bound, stated as a test: "settle": 0 is for names that only ever increase.
        var input = new FakeInput();
        await using var reader = Read(_store, input, "in/", ", \"settle\": \"00:00:00\"");
        await PutAsync("in/0002.jsonl", Numbered("0002", 2));
        await Eventually(() => input.Count == 2, "the first object read at once");
        await PutAsync("in/0001.jsonl", Numbered("0001", 2));
        await Task.Delay(300);
        Assert.Equal(Numbered("0002", 2), input.Lines);
    }

    [Fact]
    public async Task A_source_whose_prefix_holds_nothing_says_what_it_is_waiting_for_until_something_is_there()
    {
        var input = new FakeInput();
        await using var reader = Read(_store, input, "typo/", ", \"settle\": \"00:00:00\"");
        Assert.Equal("objects under typo/ in store landing", reader.Stats.WaitingFor);
        await Task.Delay(100);
        Assert.Equal("objects under typo/ in store landing", reader.Stats.WaitingFor);
        await PutAsync("typo/0001.jsonl", Numbered("0001", 1));
        await Eventually(() => input.Count == 1, "the object read");
        Assert.Null(reader.Stats.WaitingFor);
    }

    // ── a keyed input's deletes ────────────────────────────────────────────────────────

    [Fact]
    public async Task A_keyed_input_in_the_changes_format_upserts_and_deletes_and_reads_the_lines_an_upsert_sink_writes()
    {
        await using var service = await StartAsync();
        var keyed = "\"pollInterval\": \"00:00:00.05\", \"settle\": \"00:00:00\", \"format\": \"changes\"";
        string Row(Trade trade) => $"{{\"key\":{trade.Id},\"row\":{JsonSerializer.Serialize(trade, Web)}}}";

        // First pipeline: changes in, the latest trade per id out — to files, as an upsert sink.
        await service.DeployAsync(Spec("first", "latest-trades") with
        {
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Files("in/", keyed) },
            Sinks = new Dictionary<string, JsonElement> { ["out"] = Files("mid/") },
        });
        // Second pipeline: a keyed input in the same format, fed with the first one's
        // objects handed on under names of their own. (Its input is not the sink's
        // prefix itself: that is refused, see the test above.)
        var rows = service.Memory.UpsertSink<long, Trade>("latest");
        await service.DeployAsync(Spec("second", "latest-trades", null, "latest") with
        {
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Files("handed/", keyed) },
        });
        var handed = 0;
        async Task HandOnAsync()
        {
            foreach (var name in (await FileSinkFormat.StepObjectsAsync(_store, "mid/")).Skip(handed))
                await PutAsync($"handed/{++handed:D6}.jsonl", await File.ReadAllLinesAsync(Path.Combine(_root, name)));
        }

        var trades = Trades(0, 6);
        await PutAsync("in/0001.jsonl", trades.Select(Row));
        await EventuallyAsync(async () => Same(await FileSinkFormat.ReadUpsertsAsync<long, Trade>(_store, "mid/"), trades.ToDictionary(t => t.Id)), "six rows in the first pipeline's sink");
        await HandOnAsync();
        await Eventually(() => Same(rows.Rows, trades.ToDictionary(t => t.Id)), "six rows through both pipelines");

        var changed = trades[1] with { Quantity = 777 };
        await PutAsync("in/0002.jsonl", [Row(changed), $"{{\"key\":{trades[3].Id},\"deleted\":true}}", $"{{\"key\":{trades[4].Id},\"deleted\":true}}"]);
        var expected = trades.ToDictionary(t => t.Id);
        expected[changed.Id] = changed;
        expected.Remove(trades[3].Id);
        expected.Remove(trades[4].Id);
        await EventuallyAsync(async () => Same(await FileSinkFormat.ReadUpsertsAsync<long, Trade>(_store, "mid/"), expected), "an upsert and two deletes in the first pipeline's sink");
        await HandOnAsync();
        await Eventually(() => Same(rows.Rows, expected), "an upsert and two deletes through both pipelines");

        // A line that is neither is rejected in its place, like any line that is not a record.
        await PutAsync("in/0003.jsonl", ["{\"row\":{\"id\":1}}", "{\"key\":99}", Row(trades[3])]);
        await Eventually(() => service.Pipelines.Get("first").Faults.Any(f => f.Message.Contains("in/0003.jsonl:1 is not a record") && f.Message.Contains("key")), "the line without a key faulting the pipeline");
    }

    [Fact]
    public async Task The_settings_of_the_other_kinds_of_slot_are_refused_and_the_schema_says_which_kind_each_is_for()
    {
        await using var service = await StartAsync();
        async Task<string> Refused(string definition, JsonElement input, JsonElement sink)
        {
            var spec = Spec("p", definition, null) with
            {
                Inputs = new Dictionary<string, JsonElement> { ["trades"] = input },
                Sinks = new Dictionary<string, JsonElement> { ["out"] = sink },
            };
            var ex = await Assert.ThrowsAsync<ServiceException>(() => service.Pipelines.ValidateAsync(spec, default));
            return string.Join(" ", ex.Details);
        }

        // big-trades: an append input and an append sink. latest-trades: keyed, and an upsert sink.
        Assert.Contains("\"format\" is a keyed input's setting", await Refused("big-trades", Files("in/", "\"format\": \"changes\""), Files("out/")));
        Assert.Contains("\"compactAfter\" is an upsert sink's setting", await Refused("big-trades", Files("in/"), Files("out/", "\"compactAfter\": 10")));
        Assert.Contains("\"retain\" is an append sink's setting", await Refused("latest-trades", Files("in/"), Files("out/", "\"retain\": 10")));
        Assert.Contains("are a source's settings", await Refused("big-trades", Files("in/"), Files("out/", "\"settle\": \"00:00:01\"")));
        Assert.Contains("settle is a duration", await Refused("big-trades", Files("in/", "\"settle\": \"-00:00:01\""), Files("out/")));
        Assert.Contains("format is \"rows\" or \"changes\"", await Refused("latest-trades", Files("in/", "\"format\": \"csv\""), Files("out/")));
        Assert.Contains("compactAfter is a number of objects from 2", await Refused("latest-trades", Files("in/"), Files("out/", "\"compactAfter\": 1")));
        Assert.Contains("retain is a number of objects from 1", await Refused("big-trades", Files("in/"), Files("out/", "\"retain\": 0")));

        var properties = Assert.Single(service.Node.Discovery.Connectors, c => c.Name == "files").Config!.Properties.ToDictionary(p => p.Name, p => p.AppliesTo);
        Assert.Null(properties["store"]);
        Assert.Null(properties["prefix"]);
        foreach (var source in new[] { "suffix", "pollInterval", "settle", "maxLineLength" })
            Assert.Equal([SlotKind.AppendInput, SlotKind.KeyedInput], properties[source]);
        Assert.Equal([SlotKind.KeyedInput], properties["format"]);
        Assert.Equal([SlotKind.UpsertSink, SlotKind.AppendSink], properties["minInterval"]);
        Assert.Equal([SlotKind.UpsertSink], properties["compactAfter"]);
        Assert.Equal([SlotKind.AppendSink], properties["retain"]);
        // Every property a kind is refused has that kind left out of its AppliesTo, and the
        // other way round: the schema and RequireKind are one rule said twice.
        var kafka = Assert.Single(service.Node.Discovery.Connectors, c => c.Name == "kafka").Config!.Properties.ToDictionary(p => p.Name, p => p.AppliesTo);
        Assert.Equal([SlotKind.AppendInput, SlotKind.KeyedInput], kafka["startFrom"]);
        Assert.Equal([SlotKind.KeyedInput], kafka["format"]);
        Assert.Equal([SlotKind.UpsertSink, SlotKind.AppendSink], kafka["producer"]);
    }

    // ── sinks: retention, snapshots, and the object of a leader that was replaced ──────

    [Fact]
    public async Task An_append_sink_told_to_retain_keeps_that_many_of_its_newest_objects_across_a_restart()
    {
        await using var service = await StartAsync();
        var log = service.Memory.Log<Trade>("in");
        await service.DeployAsync(Spec("big", "big-trades", "in") with
        {
            Sinks = new Dictionary<string, JsonElement> { ["out"] = Files("out/kept-", "\"retain\": 3") },
        });
        var trades = Trades(0, 400).Where(t => t.Quantity >= BigTrades.Threshold).ToList();
        async Task<List<long>> StepsAsync() =>
            [.. (await FileSinkFormat.StepObjectsAsync(_store, "out/kept-")).Select(n => FileSinkFormat.TryParse("out/kept-", n, out var step, out _) ? step : -1)];

        for (var i = 0; i < 8; i++)
        {
            log.Append(trades[i]);
            var seen = i + 1;
            await EventuallyAsync(async () => (await AppendsAsync<Trade>("out/kept-")).LastOrDefault() == trades[seen - 1], $"record {seen} written");
        }
        var before = await StepsAsync();
        Assert.Equal(3, before.Count);
        Assert.Equal(trades.Skip(5).Take(3), await AppendsAsync<Trade>("out/kept-"));

        // A new generation finds the three and goes on counting from them.
        await service.Pipelines.StopAsync("big", new ActionOptions(), default);
        await service.Pipelines.StartAsync("big", new ActionOptions(), default);
        await service.Running("big");
        log.Append(trades[8]);
        await EventuallyAsync(async () => (await AppendsAsync<Trade>("out/kept-")).LastOrDefault() == trades[8], "a record after the restart");
        Assert.Equal(trades.Skip(6).Take(3), await AppendsAsync<Trade>("out/kept-"));
        Assert.Equal(3, Directory.GetFiles(Path.Combine(_root, "out")).Length);
    }

    [Fact]
    public async Task An_upsert_sink_told_to_compact_folds_its_objects_into_a_snapshot_that_reads_the_same()
    {
        await using var service = await StartAsync();
        var keyed = "\"pollInterval\": \"00:00:00.02\", \"settle\": \"00:00:00\", \"format\": \"changes\"";
        await service.DeployAsync(Spec("latest", "latest-trades") with
        {
            Inputs = new Dictionary<string, JsonElement> { ["trades"] = Files("in/", keyed) },
            Sinks = new Dictionary<string, JsonElement> { ["out"] = Files("out/latest/", "\"compactAfter\": 4") },
        });
        string Row(Trade trade) => $"{{\"key\":{trade.Id},\"row\":{JsonSerializer.Serialize(trade, Web)}}}";
        var expected = new Dictionary<long, Trade>();
        var trades = Trades(0, 40);
        var files = 0;
        string[] Names() => Directory.Exists(Path.Combine(_root, "out", "latest")) ? [.. Directory.GetFiles(Path.Combine(_root, "out", "latest")).Select(Path.GetFileName).Order()!] : [];

        // Thirty steps, one object in at a time so that each is a step: upserts, changes
        // of rows already written, and deletes.
        for (var round = 0; round < 30; round++)
        {
            var lines = new List<string>();
            var fresh = trades[round];
            expected[fresh.Id] = fresh;
            lines.Add(Row(fresh));
            if (round >= 2)
            {
                var changed = trades[round - 2] with { Quantity = 1000 + round };
                if (expected.ContainsKey(changed.Id))
                {
                    expected[changed.Id] = changed;
                    lines.Add(Row(changed));
                }
            }
            if (round % 3 == 2)
            {
                var gone = trades[round - 1].Id;
                expected.Remove(gone);
                lines.Add($"{{\"key\":{gone},\"deleted\":true}}");
            }
            await PutAsync($"in/{files++:D4}.jsonl", lines);
            await EventuallyAsync(async () => Same(await FileSinkFormat.ReadUpsertsAsync<long, Trade>(_store, "out/latest/"), expected), $"the sink after round {round}");
            // Never more than the trigger's worth of step objects, one snapshot, and the
            // step that set the fold off (it is above the checkpoint while it is written).
            Assert.InRange(Names().Length, 1, 4 + 2);
        }
        var names = Names();
        Assert.Single(names, n => n.EndsWith(".snapshot.jsonl"));
        // The snapshot holds each live key once and no deleted one.
        var snapshot = File.ReadAllLines(Path.Combine(_root, "out", "latest", names.Single(n => n.EndsWith(".snapshot.jsonl"))));
        Assert.DoesNotContain(snapshot, line => line.Contains("\"deleted\""));
        Assert.Equal(snapshot.Length, snapshot.Select(line => JsonDocument.Parse(line).RootElement.GetProperty("key").GetInt64()).Distinct().Count());

        // A restart counts what is there and goes on; a reset takes the snapshot too.
        await service.Pipelines.StopAsync("latest", new ActionOptions(), default);
        await service.Pipelines.StartAsync("latest", new ActionOptions(), default);
        await service.Running("latest");
        expected[trades[35].Id] = trades[35];
        await PutAsync($"in/{files++:D4}.jsonl", [Row(trades[35])]);
        await EventuallyAsync(async () => Same(await FileSinkFormat.ReadUpsertsAsync<long, Trade>(_store, "out/latest/"), expected), "the sink after a restart");
        await service.Pipelines.ResetAsync("latest", new ActionOptions(), default);
        await service.Running("latest");
        await EventuallyAsync(async () => Same(await FileSinkFormat.ReadUpsertsAsync<long, Trade>(_store, "out/latest/"), expected), "the sink after a reset: built again from the input");
    }

    [Fact]
    public async Task A_reader_that_listed_before_a_fold_and_reads_after_it_starts_over_from_the_snapshot()
    {
        for (var step = 1; step <= 5; step++)
            await PutAsync(FileSinkFormat.ObjectName("out/", step, 1, 0), [$"{{\"key\":{step % 3},\"row\":\"step {step}\"}}", $"{{\"key\":{10 + step},\"row\":\"only {step}\"}}"]);
        await PutAsync(FileSinkFormat.ObjectName("out/", 6, 1, 0), ["{\"key\":11,\"deleted\":true}", "{\"key\":0,\"row\":\"step 6\"}"]);
        var expected = new Dictionary<int, string> { [0] = "step 6", [1] = "step 4", [2] = "step 5", [12] = "only 2", [13] = "only 3", [14] = "only 4", [15] = "only 5" };
        Assert.Equal(expected, await FileSinkFormat.ReadUpsertsAsync<int, string>(_store, "out/"));

        // The fold happens between the reader's listing and its first read.
        var store = new PagingStore(_store);
        var folded = false;
        store.BeforeOpen = async _ =>
        {
            if (folded)
                return;
            folded = true;
            Assert.Equal(1, await FileSinkFormat.CompactAsync<int>(_store, "out/", upTo: 5, term: 1, Web, default));
        };
        Assert.Equal(expected, await FileSinkFormat.ReadUpsertsAsync<int, string>(store, "out/"));
        Assert.True(folded);
        Assert.Equal(
            [FileSinkFormat.SnapshotName("out/", 5, 1), FileSinkFormat.ObjectName("out/", 6, 1, 0)],
            await FileSinkFormat.StepObjectsAsync(_store, "out/"));
        Assert.Equal(2, Directory.GetFiles(Path.Combine(_root, "out")).Length);

        // Folding again with one step above the snapshot: the two become one, and a
        // second fold of nothing writes nothing.
        Assert.Equal(0, await FileSinkFormat.CompactAsync<int>(_store, "out/", upTo: 6, term: 2, Web, default));
        Assert.Equal([FileSinkFormat.SnapshotName("out/", 6, 2)], await FileSinkFormat.StepObjectsAsync(_store, "out/"));
        Assert.Equal(expected, await FileSinkFormat.ReadUpsertsAsync<int, string>(_store, "out/"));
        Assert.Equal(0, await FileSinkFormat.CompactAsync<int>(_store, "out/", upTo: 6, term: 2, Web, default));
        Assert.Equal(expected, await FileSinkFormat.ReadUpsertsAsync<int, string>(_store, "out/"));
    }

    [Fact]
    public async Task A_fold_that_loses_an_object_to_another_writer_writes_nothing()
    {
        for (var step = 1; step <= 4; step++)
            await PutAsync(FileSinkFormat.ObjectName("out/", step, 1, 0), [$"{{\"key\":{step},\"row\":\"step {step}\"}}"]);
        var store = new PagingStore(_store);
        store.BeforeOpen = name =>
        {
            File.Delete(Path.Combine(_root, FileSinkFormat.ObjectName("out/", 2, 1, 0)));
            return Task.CompletedTask;
        };
        Assert.Equal(4, await FileSinkFormat.CompactAsync<int>(store, "out/", upTo: 4, term: 1, Web, default));
        Assert.DoesNotContain(Directory.GetFiles(Path.Combine(_root, "out")), n => n.Contains("snapshot") || n.EndsWith(".tmp"));
        Assert.Equal(3, Directory.GetFiles(Path.Combine(_root, "out")).Length);
    }

    [Fact]
    public async Task The_late_object_of_a_replaced_leader_is_void_once_its_successor_has_written()
    {
        // Leader one (term 1) started from nothing and wrote steps 1 to 4; the checkpoint
        // covers 2. It is replaced while step 4's object is still on its way.
        await PutAsync(FileSinkFormat.ObjectName("out/", 1, 1, 0), ["{\"key\":1,\"row\":\"a1\"}"]);
        await PutAsync(FileSinkFormat.ObjectName("out/", 2, 1, 0), ["{\"key\":2,\"row\":\"a2\"}"]);
        // Leader two (term 2) starts from the checkpoint at 2, deletes what is above it,
        // and runs the same input again cut into one step, 3 — and its step 4 gives this
        // sink nothing, so it writes no object for it.
        await PutAsync(FileSinkFormat.ObjectName("out/", 3, 2, 2), ["{\"key\":1,\"row\":\"b3: the latest\"}", "{\"key\":3,\"row\":\"b3\"}"]);
        // Then leader one's write of step 4 lands: rows as of the middle of what leader
        // two has since finished.
        await PutAsync(FileSinkFormat.ObjectName("out/", 4, 1, 0), ["{\"key\":1,\"row\":\"a4: stale\"}", "{\"key\":9,\"row\":\"a4: never produced by leader two\"}"]);

        Assert.Equal(
            new Dictionary<int, string> { [1] = "b3: the latest", [2] = "a2", [3] = "b3" },
            await FileSinkFormat.ReadUpsertsAsync<int, string>(_store, "out/"));
        Assert.DoesNotContain(FileSinkFormat.ObjectName("out/", 4, 1, 0), await FileSinkFormat.StepObjectsAsync(_store, "out/"));

        // The same object at a step the successor's base covers is not void: it was committed.
        Assert.Contains(FileSinkFormat.ObjectName("out/", 2, 1, 0), await FileSinkFormat.StepObjectsAsync(_store, "out/"));
        // A third leader from the checkpoint at 3 leaves leader two's step 3 alone and
        // would void its step 5.
        await PutAsync(FileSinkFormat.ObjectName("out/", 5, 2, 2), ["{\"key\":3,\"row\":\"b5: stale\"}"]);
        await PutAsync(FileSinkFormat.ObjectName("out/", 4, 3, 3), ["{\"key\":4,\"row\":\"c4\"}"]);
        Assert.Equal(
            new Dictionary<int, string> { [1] = "b3: the latest", [2] = "a2", [3] = "b3", [4] = "c4" },
            await FileSinkFormat.ReadUpsertsAsync<int, string>(_store, "out/"));

        // A fold takes the void objects away with the rest and leaves the same rows.
        await FileSinkFormat.CompactAsync<int>(_store, "out/", upTo: 5, term: 3, Web, default);
        Assert.Equal([FileSinkFormat.SnapshotName("out/", 4, 3)], await FileSinkFormat.StepObjectsAsync(_store, "out/"));
        Assert.Equal(
            new Dictionary<int, string> { [1] = "b3: the latest", [2] = "a2", [3] = "b3", [4] = "c4" },
            await FileSinkFormat.ReadUpsertsAsync<int, string>(_store, "out/"));
        Assert.Single(Directory.GetFiles(Path.Combine(_root, "out")));
    }

    [Fact]
    public void An_object_s_name_says_its_step_its_term_and_the_checkpoint_its_writer_started_from()
    {
        var name = FileSinkFormat.ObjectName("out/a-", 12, 3, 9);
        Assert.Equal("out/a-0000000000000000012-000003-0000000000000000009.jsonl", name);
        Assert.True(FileSinkFormat.TryParse("out/a-", name, out FileSinkFormat.StepObject parsed));
        Assert.Equal(new FileSinkFormat.StepObject(name, 12, 3, 9, false), parsed);
        Assert.True(FileSinkFormat.TryParse("out/a-", FileSinkFormat.SnapshotName("out/a-", 12, 3), out parsed));
        Assert.Equal((12, 3, true), (parsed.Step, parsed.Term, parsed.Snapshot));
        // The older form, without the base: read, and it voids nothing.
        Assert.True(FileSinkFormat.TryParse("out/a-", FileSinkFormat.ObjectName("out/a-", 12, 3), out parsed));
        Assert.Equal((12, 3, long.MaxValue, false), (parsed.Step, parsed.Term, parsed.Base, parsed.Snapshot));
        foreach (var other in new[] { "out/a-12-3-9.jsonl", "out/a-0000000000000000012-000003-9.jsonl", "out/a-0000000000000000012-000003.snapshot.jsonl", "out/b-0000000000000000012-000003.jsonl", "out/a-0000000000000000012-000003.json", "out/a-readme.jsonl" })
            Assert.False(FileSinkFormat.TryParse("out/a-", other, out parsed), other);
    }

    [Fact]
    public async Task A_pipeline_whose_prefix_holds_nothing_says_so_at_the_dry_run_and_degrades_the_node_until_an_object_lands()
    {
        await using var service = await TestService.StartAsync(
            options: o => o.MissingInputGrace = TimeSpan.FromMilliseconds(300),
            configure: b => b.AddObjectStore("landing", _ => _store));
        var spec = CountsFromFiles("counts", "tradse/", "counts");             // the typing mistake

        var dry = await service.Pipelines.DeployAsync(spec, new DeployOptions(DryRun: true), default);
        var warning = Assert.Single(dry.Plan.Warnings);
        Assert.Contains("input 'trades': no object under tradse/ in store landing", warning);

        await service.DeployAsync(spec);
        await service.Running("counts");
        await Eventually(() => service.Pipelines.Get("counts").Sources.Single() is { Stats.WaitingFor: "objects under tradse/ in store landing", WaitingSince: not null }, "the source waiting");
        await Eventually(() => service.Node.Health.Reasons.Contains("source-waiting"), "the node saying so", TimeSpan.FromSeconds(10));
        Assert.Equal(HealthLevel.Degraded, service.Node.Health.Level);
        await Eventually(() => service.Node.RecentEvents().Any(e => e.Kind == "pipeline" && e.Message == "input 'trades' waiting for objects under tradse/ in store landing"), "the event");

        await PutAsync("tradse/0001.jsonl", Trades(0, 10));
        var sink = service.Memory.UpsertSink<string, long>("counts");
        await Eventually(() => Same(sink.Rows, Counts.Model(Trades(0, 10))), "the object read");
        await Eventually(() => service.Node.Health.Level == HealthLevel.Healthy, "the node healthy again", TimeSpan.FromSeconds(10));
        Assert.Null(service.Pipelines.Get("counts").Sources.Single().WaitingSince);
        Assert.Empty((await service.Pipelines.DeployAsync(spec with { Id = "again", Sinks = new Dictionary<string, JsonElement> { ["out"] = TestService.MemoryBinding("again") } }, new DeployOptions(DryRun: true), default)).Plan.Warnings);
    }
}
