using System.Text;

namespace Incremental.Service.Tests;

/// <summary>
/// The files connector's tests (<see cref="FileConnectorTests"/>) on a local directory, and
/// what is the directory's alone or needs no store: the position's encoding, the line
/// reader, the sink's object names.
/// </summary>
public sealed class LocalFileConnectorTests : FileConnectorTests
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "incremental-file-connector-tests", Guid.NewGuid().ToString("N"));

    protected override ValueTask<IObjectStore> OpenStoreAsync() => new(new LocalObjectStore(_root));

    protected override ValueTask CloseStoreAsync()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
        return ValueTask.CompletedTask;
    }

    protected override ValueTask<List<string>> NamesUnderAsync(string prefix)
    {
        var names = Directory.Exists(_root)
            ? Directory.GetFiles(_root, "*", SearchOption.AllDirectories)
                .Select(f => Path.GetRelativePath(_root, f).Replace(Path.DirectorySeparatorChar, '/'))
                .Where(n => n.StartsWith(prefix, StringComparison.Ordinal))
                .Order(StringComparer.Ordinal).ToList()
            : [];
        return new(names);
    }

    protected override ValueTask AgeAsync(TimeSpan age, params string[] names)
    {
        var then = DateTime.UtcNow - (age > TimeSpan.FromMinutes(1) ? age : TimeSpan.FromMinutes(1));
        foreach (var name in names)
            File.SetLastWriteTimeUtc(Path.Combine(_root, name), then);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task A_listing_walks_only_the_directory_its_prefix_names()
    {
        await PutAsync("out/1.jsonl", ["{}"]);
        // A file the store could not even stat lies outside the prefix: it is not looked at.
        Directory.CreateDirectory(Path.Combine(_root, "other"));
        File.CreateSymbolicLink(Path.Combine(_root, "other", "loop"), Path.Combine(_root, "other"));
        var names = new List<string>();
        await foreach (var info in Store.ListAsync("out/", default))
            names.Add(info.Path);
        Assert.Equal(["out/1.jsonl"], names);
    }

    // ── no store: the position, the line reader, the names ─────────────────────────────

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
}
