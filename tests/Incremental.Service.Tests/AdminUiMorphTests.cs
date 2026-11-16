using System.Diagnostics;
using System.Net;
using System.Text.RegularExpressions;

namespace Incremental.Service.Tests;

/// <summary>
/// The admin UI's <c>morph</c> in a real browser: <c>Ui/morph-tests.js</c> uses form
/// controls as a person would, redraws from a model that says something else, and checks
/// what each control shows. It runs in a headless Chrome or Chromium with the UI's own
/// <c>app.js</c> (<c>--dump-dom</c>: no driver, no package), and is skipped where neither is
/// installed — GitHub's Ubuntu runners have Chrome. Set <c>INCREMENTAL_CHROME</c> to a
/// browser's path to choose one.
/// </summary>
public partial class AdminUiMorphTests
{
    [Fact]
    public async Task Morph_makes_every_form_control_show_what_the_model_says_in_a_real_browser()
    {
        var chrome = FindChrome();
        if (chrome is null)
            Assert.Skip("No Chrome or Chromium on this machine (set INCREMENTAL_CHROME to one).");

        var root = Root();
        var page = Directory.CreateTempSubdirectory("incremental-morph").FullName;
        try
        {
            File.Copy(Path.Combine(root, "src", "Incremental.Service.Admin", "wwwroot", "app.js"), Path.Combine(page, "app.js"));
            foreach (var name in new[] { "morph.html", "morph-tests.js" })
                File.Copy(Path.Combine(root, "tests", "Incremental.Service.Tests", "Ui", name), Path.Combine(page, name));

            var dom = await DumpDomAsync(chrome, Path.Combine(page, "morph.html"), Path.Combine(page, "profile"));
            var result = Result().Match(dom);
            Assert.True(result.Success, $"The page has no result; the browser printed:\n{dom}");
            var lines = WebUtility.HtmlDecode(result.Groups[1].Value).Split('\n', StringSplitOptions.RemoveEmptyEntries);
            Assert.All(lines, line => Assert.StartsWith("ok ", line));
            Assert.Equal(10, lines.Length);
        }
        finally
        {
            try
            {
                Directory.Delete(page, recursive: true);
            }
            catch (IOException)
            {
                // The browser's profile can still be closing.
            }
        }
    }

    internal static async Task<string> DumpDomAsync(string chrome, string page, string profile, int virtualMilliseconds = 5000)
    {
        var start = new ProcessStartInfo(chrome)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var argument in new[]
                 {
                     "--headless=new", "--disable-gpu", "--no-sandbox", "--no-first-run", "--no-default-browser-check",
                     "--disable-extensions", $"--user-data-dir={profile}", $"--virtual-time-budget={virtualMilliseconds}",
                     "--dump-dom", page.Contains("://", StringComparison.Ordinal) ? page : new Uri(page).AbsoluteUri,
                 })
            start.ArgumentList.Add(argument);
        using var process = Process.Start(start)!;
        var output = process.StandardOutput.ReadToEndAsync();
        var errors = process.StandardError.ReadToEndAsync();
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        try
        {
            await process.WaitForExitAsync(limit.Token);
        }
        catch (OperationCanceledException)
        {
            process.Kill(entireProcessTree: true);
            throw new TimeoutException($"The browser did not finish in a minute: {await errors}");
        }
        return await output;
    }

    internal static string? FindChrome()
    {
        if (Environment.GetEnvironmentVariable("INCREMENTAL_CHROME") is { Length: > 0 } chosen)
            return chosen;
        string[] names = OperatingSystem.IsWindows()
            ? [@"C:\Program Files\Google\Chrome\Application\chrome.exe", @"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"]
            : OperatingSystem.IsMacOS()
                ? ["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", "/Applications/Chromium.app/Contents/MacOS/Chromium"]
                : ["google-chrome", "google-chrome-stable", "chromium", "chromium-browser"];
        foreach (var name in names)
        {
            if (Path.IsPathRooted(name))
            {
                if (File.Exists(name))
                    return name;
                continue;
            }
            foreach (var directory in (Environment.GetEnvironmentVariable("PATH") ?? "").Split(Path.PathSeparator))
            {
                var path = Path.Combine(directory, name);
                if (File.Exists(path))
                    return path;
            }
        }
        return null;
    }

    internal static string Root()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "Incremental.slnx")))
            directory = directory.Parent;
        return directory?.FullName ?? throw new InvalidOperationException("The repository root was not found above the test output.");
    }

    [GeneratedRegex("<pre id=\"result\">(.*?)</pre>", RegexOptions.Singleline)]
    private static partial Regex Result();
}
