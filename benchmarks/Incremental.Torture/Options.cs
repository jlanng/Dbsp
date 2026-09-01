using System.Globalization;

namespace Incremental.Torture;

internal sealed record Options
{
    public int Workers { get; init; } = 4;
    public int Merchants { get; init; } = 2_000;
    public int Customers { get; init; } = 50_000;
    public int Categories { get; init; } = 32;
    public int FeeTiers { get; init; } = 24;
    public int Regions { get; init; } = 12;
    public int Rings { get; init; } = 200;

    /// <summary>Size of the live order population: the circuit's steady-state working set.</summary>
    public int LiveOrders { get; init; } = 150_000;

    /// <summary>Virtual seconds advance once per this many orders, so window churn is a
    /// property of the data, not of how fast the harness happens to be running.</summary>
    public int OrdersPerVirtualSecond { get; init; } = 2_000;

    /// <summary>Target order rate (orders/second). Each order carries a retraction and up to
    /// two payment rows, so the record rate through the inputs is roughly 3.8x this. Zero
    /// means unpaced: feed as fast as backpressure allows.</summary>
    public int Rate { get; init; }

    public double Seconds { get; init; } = 20;
    public double WarmupSeconds { get; init; } = 5;
    public int Seed { get; init; } = 42;
    public bool PointToPoint { get; init; }

    /// <summary>Fragment streaming (implies point-to-point, which it builds on).</summary>
    public bool Streaming { get; init; }

    /// <summary>Point-to-point spin budget before a wait parks (CircuitOptions.ExchangeSpinBudget); null keeps the default.</summary>
    public int? SpinBudget { get; init; }
    public int StepBatch { get; init; } = 8_192;
    public int InputCapacity { get; init; } = 32_768;

    /// <summary>Emit the summary as a single JSON line (for the ramp driver) as well as text.</summary>
    public bool Json { get; init; }

    /// <summary>Print a byte-weighted allocation histogram by type for the measured window.</summary>
    public bool AllocationTypes { get; init; }

    /// <summary>When &gt; 1, run the cluster-mode graph across this many co-hosted processes
    /// stepped in lockstep, which is what puts the exchange wire format on the measured path.</summary>
    public int Processes { get; init; } = 1;

    public static Options Parse(string[] args)
    {
        var options = new Options();
        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            if (!arg.StartsWith("--", StringComparison.Ordinal))
                throw new ArgumentException($"Unexpected argument '{arg}'.");
            var name = arg[2..];
            if (name == "p2p") { options = options with { PointToPoint = true }; continue; }
            if (name == "streaming") { options = options with { Streaming = true, PointToPoint = true }; continue; }
            if (name == "json") { options = options with { Json = true }; continue; }
            if (name == "alloc-types") { options = options with { AllocationTypes = true }; continue; }

            var value = i + 1 < args.Length ? args[++i] : throw new ArgumentException($"'{arg}' needs a value.");
            int Int() => int.Parse(value, CultureInfo.InvariantCulture);
            double Real() => double.Parse(value, CultureInfo.InvariantCulture);
            options = name switch
            {
                "workers" => options with { Workers = Int() },
                "spin-budget" => options with { SpinBudget = Int() },
                "merchants" => options with { Merchants = Int() },
                "customers" => options with { Customers = Int() },
                "categories" => options with { Categories = Int() },
                "fee-tiers" => options with { FeeTiers = Int() },
                "regions" => options with { Regions = Int() },
                "rings" => options with { Rings = Int() },
                "live-orders" => options with { LiveOrders = Int() },
                "orders-per-vsec" => options with { OrdersPerVirtualSecond = Int() },
                "rate" => options with { Rate = Int() },
                "seconds" => options with { Seconds = Real() },
                "warmup" => options with { WarmupSeconds = Real() },
                "seed" => options with { Seed = Int() },
                "batch" => options with { StepBatch = Int() },
                "input-capacity" => options with { InputCapacity = Int() },
                "processes" => options with { Processes = Int() },
                _ => throw new ArgumentException($"Unknown option '{arg}'."),
            };
        }
        return options;
    }
}
