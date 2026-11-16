using System.Net;
using System.Text.RegularExpressions;
using Incremental.Service.AdminHost;

namespace Incremental.Service.Tests;

/// <summary>
/// Every list the admin UI draws, on a leader and on a standby with data in them, in a
/// real browser: <c>Ui/tables-check.js</c> visits each page and checks that every row has
/// as many cells as its table has headers and that each cell's label (what the phone
/// layout shows beside it) is its header's text. The views build their tables through
/// one helper (<c>grid</c> in <c>app.js</c>) that takes each column once, which is what
/// makes the two agree; this is the check that nothing went round it. Skipped where no
/// Chrome or Chromium is installed, as <see cref="AdminUiMorphTests"/> is; GitHub's Ubuntu
/// runners have Chrome, and CI runs it.
/// </summary>
public partial class AdminUiTablesTests
{
    [Fact]
    public async Task Every_list_on_a_leader_and_a_standby_has_a_cell_under_each_header_and_each_cell_is_labelled_as_its_header()
    {
        var chrome = AdminUiMorphTests.FindChrome();
        if (chrome is null)
            Assert.Skip("No Chrome or Chromium on this machine (set INCREMENTAL_CHROME to one).");

        // The UI's own files, and the check loaded after them.
        var root = AdminUiMorphTests.Root();
        var ui = Directory.CreateTempSubdirectory("incremental-tables").FullName;
        try
        {
            foreach (var file in Directory.EnumerateFiles(Path.Combine(root, "src", "Incremental.Service.Admin", "wwwroot")))
                File.Copy(file, Path.Combine(ui, Path.GetFileName(file)));
            foreach (var name in new[] { "tables-check.js", "tables-stub.js" })
                File.Copy(Path.Combine(root, "tests", "Incremental.Service.Tests", "Ui", name), Path.Combine(ui, name));
            var index = File.ReadAllText(Path.Combine(ui, "index.html"));
            Assert.Contains("<script src=\"app.js\" defer></script>", index);
            Assert.Contains("<script src=\"shell.js\" defer></script>", index);
            index = index
                .Replace("<script src=\"app.js\" defer></script>", "<script src=\"tables-stub.js\" defer></script>\n<script src=\"app.js\" defer></script>", StringComparison.Ordinal)
                .Replace("<script src=\"shell.js\" defer></script>", "<script src=\"shell.js\" defer></script>\n<script src=\"tables-check.js\" defer></script>", StringComparison.Ordinal);
            File.WriteAllText(Path.Combine(ui, "index.html"), index);

            // Two nodes with something in every list: pipelines running and built, tables
            // with rows and readers, a dead letter, state, an audit trail.
            await using var admin = await AdminFixture.StartAsync(o =>
            {
                o.Nodes = 2;
                o.Rate = 60;
                o.UiDirectory = ui;
            });
            var leader = admin.Host.Nodes[0];
            var standby = admin.Host.Nodes[1];
            await TestService.Eventually(() => leader.Node.Pipelines.Get("lenient-orders").DeadLetters > 0, "a dead letter");
            await TestService.Eventually(() => leader.Node.Tables.Stats("positions").Rows > 0, "positions");
            await TestService.Eventually(() => standby.Node.Pipelines.Pipelines.Any(p => p.State == PipelineState.Built), "a pipeline built on the standby");

            foreach (var (node, role) in new[] { (leader, "Leader"), (standby, "Standby") })
            {
                var dom = await AdminUiMorphTests.DumpDomAsync(chrome, node.Admin, Path.Combine(ui, "profile-" + role), virtualMilliseconds: 120_000);
                var result = Result().Match(dom);
                Assert.True(result.Success, $"The {role}'s page has no result; the browser printed:\n{dom}");
                var lines = WebUtility.HtmlDecode(result.Groups[1].Value).Split('\n', StringSplitOptions.RemoveEmptyEntries);
                var failed = lines.Where(l => l.Contains("FAIL", StringComparison.Ordinal)).ToList();
                Assert.True(failed.Count == 0, $"On the {role}:\n{string.Join('\n', failed)}\n\nall:\n{string.Join('\n', lines)}");
                Assert.StartsWith($"done {role} ", lines[^1]);

                // The lists an operator reads first are among those checked, each with its columns.
                string[] expected = role == "Leader"
                    ? ["ok #/ table 1 (Table | State | Rows | Memory | Version | Readers)",
                       "ok #/tables table 1 (Table | State | Rows | Memory | Version | Versions /s | Written by | Loader | Readers | Floor held by)"]
                    : ["ok #/ table 1 (Table | State | Rows | Memory | Version | Behind)",
                       "ok #/tables table 1 (Table | State | Rows | Memory | Version | Behind | Written by | Loader | Readers | Floor held by)"];
                foreach (var line in expected)
                    Assert.Contains(line, lines);
                foreach (var page in new[] { "#/pipelines table", "#/cluster table", "#/audit table", "#/dead-letters table" }.Append(role == "Leader" ? "#/pipelines/" : "#/plugins table"))
                    Assert.Contains(lines, l => l.StartsWith("ok " + page, StringComparison.Ordinal));
                Assert.Contains(lines, l => l.StartsWith("ok #/tables/", StringComparison.Ordinal) && l.Contains("(Pipeline | Use | Committed version", StringComparison.Ordinal));
            }
        }
        finally
        {
            try
            {
                Directory.Delete(ui, recursive: true);
            }
            catch (IOException)
            {
                // The browser's profile can still be closing.
            }
        }
    }

    [GeneratedRegex("<pre id=\"result\">(.*?)</pre>", RegexOptions.Singleline)]
    private static partial Regex Result();
}
