using Incremental.Service.AdminHost;
using Incremental.Tools;

// dotnet run --project tests/Incremental.Service.AdminHost [-- --port 5180 --nodes 2 --read-only --no-rows --rate 60 --ui-dir src/Incremental.Service.Admin/wwwroot]
Option[] known =
[
    new("--port", "N", "The first node's port (5180); a second node takes the next."),
    new("--nodes", "N", "1 (the default) or 2: a leader and a standby."),
    new("--rate", "N", "Generated orders per second (60)."),
    new("--instruments", "N", "Rows in the instruments table (5000)."),
    new("--read-only", null, "The admin API refuses every mutation."),
    new("--no-rows", null, "ExposeRowData off: no rows, records or messages leave the API."),
    new("--no-upload", null, "Plugin upload off."),
    new("--ui-dir", "path", "Serve the UI's files from this directory (src/Incremental.Service.Admin/wwwroot)."),
    new("--verbose", null, "Log at Information."),
];
if (CommandLine.Check("Incremental.Service.AdminHost [options]", known, args) is { } exit)
    return exit;

string? Arg(string name)
{
    var i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
}

var options = new DemoOptions
{
    Port = int.Parse(Arg("--port") ?? "5180"),
    Nodes = int.Parse(Arg("--nodes") ?? "1"),
    Rate = int.Parse(Arg("--rate") ?? "60"),
    Instruments = int.Parse(Arg("--instruments") ?? "5000"),
    ReadOnly = args.Contains("--read-only"),
    ExposeRowData = !args.Contains("--no-rows"),
    AllowPluginUpload = !args.Contains("--no-upload"),
    UiDirectory = Arg("--ui-dir"),
    LogLevel = args.Contains("--verbose") ? LogLevel.Information : LogLevel.Warning,
};

await using var host = await DemoHost.StartAsync(options);
foreach (var node in host.Nodes)
    Console.WriteLine($"{node.Node.NodeId}: {node.Admin}");
Console.WriteLine("Ctrl+C to stop.");

var stopped = new TaskCompletionSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    stopped.TrySetResult();
};
AppDomain.CurrentDomain.ProcessExit += (_, _) => stopped.TrySetResult();
await Task.WhenAny(stopped.Task, Task.WhenAny(host.Nodes.Select(n => n.App.WaitForShutdownAsync())));
return 0;
