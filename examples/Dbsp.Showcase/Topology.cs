namespace Dbsp.Showcase;

/// <summary>
/// The showcase's running configuration: workers per node and nodes.
/// One value flows from the command line into the live circuit, the header, the scaling
/// measurement, and the cluster act, and comes back from the browser's "apply" as the
/// target of a relaunch — so the page can always show what is running and let every axis
/// change in one restart. Nodes above one arm the cluster act (that many OS processes over
/// TCP, barrier-synchronized); the live pipeline itself stays in this process because
/// <c>Recurse</c> is rejected in clustered circuits.
/// </summary>
public sealed record Topology(int Workers, int Nodes)
{
    public const int MaxWorkers = 32;
    public const int MaxNodes = 4;

    public static readonly Topology Default = new(2, 1);

    public static Topology Normalize(int workers, int nodes) =>
        new(Math.Clamp(workers, 1, MaxWorkers), Math.Clamp(nodes, 1, MaxNodes));

    /// <summary>Parses the process arguments. <c>--cluster</c> remains an alias for
    /// <c>--nodes 2</c>.</summary>
    public static Topology FromArgs(string[] args)
    {
        string? Arg(string name)
        {
            var i = Array.IndexOf(args, name);
            return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
        }
        var workers = Arg("--workers") is { } w ? int.Parse(w) : Default.Workers;
        var nodes = Arg("--nodes") is { } n ? int.Parse(n) : args.Contains("--cluster") ? 2 : Default.Nodes;
        return Normalize(workers, nodes);
    }

    /// <summary>The arguments that reproduce this topology on a relaunch.</summary>
    public IEnumerable<string> ToArgs() =>
        ["--workers", Workers.ToString(), "--nodes", Nodes.ToString()];

    public string Label =>
        $"{Workers} worker{(Workers == 1 ? "" : "s")} × {Nodes} node{(Nodes == 1 ? "" : "s")}, barrier exchanges";
}
