namespace Incremental.Showcase;

/// <summary>How the exchanges of the live pipeline synchronize — the third axis of the
/// showcase's topology next to workers and nodes.</summary>
public enum ExchangeMode
{
    /// <summary>Every worker rendezvous at every exchange (the default engine).</summary>
    Barrier,
    /// <summary>Workers wait only for the publications they are about to read (experimental).</summary>
    PointToPoint,
    /// <summary>Point-to-point plus fragment streaming through fused linear chains (experimental).</summary>
    Streaming,
}

/// <summary>
/// The showcase's running configuration: workers per node, nodes, and the exchange mode.
/// One value flows from the command line into the live circuit, the header, the scaling
/// measurement, and the cluster act, and comes back from the browser's "apply" as the
/// target of a relaunch — so the page can always show what is running and let every axis
/// change in one restart. Nodes above one arm the cluster act (that many OS processes over
/// TCP, barrier-synchronized); the live pipeline itself stays in this process because
/// <c>Recurse</c> is rejected in clustered circuits.
/// </summary>
public sealed record Topology(int Workers, int Nodes, ExchangeMode Exchange)
{
    public const int MaxWorkers = 32;
    public const int MaxNodes = 4;

    public static readonly Topology Default = new(2, 1, ExchangeMode.Barrier);

    public static Topology Normalize(int workers, int nodes, ExchangeMode exchange) =>
        new(Math.Clamp(workers, 1, MaxWorkers), Math.Clamp(nodes, 1, MaxNodes), exchange);

    /// <summary>Parses the process arguments. <c>--cluster</c> and <c>--p2p</c> remain as
    /// aliases for <c>--nodes 2</c> and <c>--exchange p2p</c>.</summary>
    public static Topology FromArgs(string[] args)
    {
        string? Arg(string name)
        {
            var i = Array.IndexOf(args, name);
            return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
        }
        var workers = Arg("--workers") is { } w ? int.Parse(w) : Default.Workers;
        var nodes = Arg("--nodes") is { } n ? int.Parse(n) : args.Contains("--cluster") ? 2 : Default.Nodes;
        var exchange = Arg("--exchange") is { } e ? ParseExchange(e)
            : args.Contains("--streaming") ? ExchangeMode.Streaming
            : args.Contains("--p2p") ? ExchangeMode.PointToPoint
            : Default.Exchange;
        return Normalize(workers, nodes, exchange);
    }

    public static ExchangeMode ParseExchange(string text) => text.ToLowerInvariant() switch
    {
        "barrier" => ExchangeMode.Barrier,
        "p2p" or "point-to-point" or "pointtopoint" => ExchangeMode.PointToPoint,
        "streaming" => ExchangeMode.Streaming,
        _ => throw new ArgumentException($"unknown exchange mode '{text}' (barrier | p2p | streaming)"),
    };

    public static string ExchangeArg(ExchangeMode mode) => mode switch
    {
        ExchangeMode.Barrier => "barrier",
        ExchangeMode.PointToPoint => "p2p",
        _ => "streaming",
    };

    public static string ExchangeLabel(ExchangeMode mode) => mode switch
    {
        ExchangeMode.Barrier => "barrier",
        ExchangeMode.PointToPoint => "point-to-point",
        _ => "streaming",
    };

    /// <summary>The arguments that reproduce this topology on a relaunch.</summary>
    public IEnumerable<string> ToArgs() =>
        ["--workers", Workers.ToString(), "--nodes", Nodes.ToString(), "--exchange", ExchangeArg(Exchange)];

    public string Label =>
        $"{Workers} worker{(Workers == 1 ? "" : "s")} × {Nodes} node{(Nodes == 1 ? "" : "s")}, {ExchangeLabel(Exchange)} exchanges";

    public bool PointToPoint => Exchange != ExchangeMode.Barrier;
    public bool Streaming => Exchange == ExchangeMode.Streaming;
}
