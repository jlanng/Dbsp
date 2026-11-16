using System.Text.RegularExpressions;

namespace Incremental.Service.Tests;

/// <summary>
/// The service's guide and the example's README show C# that must compile. They do it by
/// quoting regions of the example's sources: a block in the document is preceded by
/// <c>&lt;!-- snippet: path#name --&gt;</c>, and the source marks the region with
/// <c>// &lt;snippet:name&gt;</c> … <c>// &lt;/snippet:name&gt;</c>. These tests hold the
/// two together — the example is built with the solution, so a quoted block is compiled code.
/// </summary>
public partial class DocSnippetTests
{
    private static readonly string[] Documents =
    [
        "docs/enrichment-service.md",
        "examples/Incremental.EnrichmentService/README.md",
    ];

    // A block's body is every line up to the first line that starts with a fence.
    [GeneratedRegex(@"<!--\s*snippet:\s*(?<path>[^#\s]+)#(?<name>[\w-]+)\s*-->\s*\n```csharp\n(?<code>(?:(?!```)[^\n]*\n)*)```")]
    private static partial Regex Quoted();

    [GeneratedRegex(@"```csharp\n")]
    private static partial Regex CSharpBlock();

    private static string Root()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "Incremental.slnx")))
            directory = directory.Parent;
        return directory?.FullName ?? throw new InvalidOperationException("The repository root was not found above the test output.");
    }

    /// <summary>The region of that name, without its markers, its common indentation or
    /// the blank lines at its ends.</summary>
    internal static string Region(string source, string name)
    {
        var lines = source.Replace("\r\n", "\n").Split('\n');
        var start = Array.FindIndex(lines, l => l.Trim() == $"// <snippet:{name}>");
        var end = Array.FindIndex(lines, l => l.Trim() == $"// </snippet:{name}>");
        if (start < 0 || end < start)
            throw new InvalidOperationException($"No region '{name}'.");
        var body = lines[(start + 1)..end].Where(l => !l.TrimStart().StartsWith("// <snippet:", StringComparison.Ordinal)
                                                     && !l.TrimStart().StartsWith("// </snippet:", StringComparison.Ordinal)).ToList();
        while (body.Count > 0 && body[0].Trim().Length == 0)
            body.RemoveAt(0);
        while (body.Count > 0 && body[^1].Trim().Length == 0)
            body.RemoveAt(body.Count - 1);
        var indent = body.Where(l => l.Trim().Length > 0).Min(l => l.Length - l.TrimStart().Length);
        return string.Join('\n', body.Select(l => l.Length >= indent ? l[indent..] : l.TrimStart()));
    }

    public static TheoryData<string> AllDocuments() => [.. Documents];

    [Theory]
    [MemberData(nameof(AllDocuments))]
    public void Every_quoted_block_is_the_region_of_the_example_source_it_names(string document)
    {
        var root = Root();
        var text = File.ReadAllText(Path.Combine(root, document)).Replace("\r\n", "\n");
        var quoted = Quoted().Matches(text);
        Assert.NotEmpty(quoted);
        foreach (Match match in quoted)
        {
            var path = Path.Combine(root, match.Groups["path"].Value);
            Assert.True(File.Exists(path), $"{document} quotes {match.Groups["path"].Value}, which does not exist.");
            var region = Region(File.ReadAllText(path), match.Groups["name"].Value);
            Assert.True(region == match.Groups["code"].Value.TrimEnd('\n'),
                $"{document}: the block quoting {match.Groups["path"].Value}#{match.Groups["name"].Value} is not that region any more. " +
                "Copy the region into the document again.");
        }
    }

    [Theory]
    [MemberData(nameof(AllDocuments))]
    public void No_csharp_block_stands_without_a_source(string document)
    {
        var text = File.ReadAllText(Path.Combine(Root(), document)).Replace("\r\n", "\n");
        Assert.Equal(Quoted().Matches(text).Count, CSharpBlock().Matches(text).Count);
    }

    [Fact]
    public void A_region_is_cut_at_its_markers_and_dedented()
    {
        const string source = "class C\n{\n    // <snippet:x>\n    void M()\n    {\n        // <snippet:inner>\n        N();\n        // </snippet:inner>\n    }\n\n    // </snippet:x>\n}\n";
        Assert.Equal("void M()\n{\n    N();\n}", Region(source, "x"));
        Assert.Equal("N();", Region(source, "inner"));
        Assert.Throws<InvalidOperationException>(() => Region(source, "missing"));
    }
}
