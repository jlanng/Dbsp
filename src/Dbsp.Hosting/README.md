# Dbsp.Hosting

Generic Host and dependency-injection integration for
[Dbsp](https://www.nuget.org/packages/Dbsp), the incremental stream
processing library for .NET. Register a circuit definition and the host owns its
lifecycle: start and checkpoint recovery, a draining shutdown, output consumers, the fault
policy, configuration binding and a health check.

Put the dataflow in a definition class. Its constructor takes the `CircuitBuilder` (any
further parameters come from the container), and the inputs and outputs you want reachable
are its members:

```csharp
public sealed class PageAnalytics
{
    public InputStream<PageView> Views { get; }
    public Output<WindowedValue<string, long>> TopPages { get; }

    public PageAnalytics(CircuitBuilder builder)
    {
        Views = builder.Input<PageView>("views");
        TopPages = Views
            .WithEventTime(v => v.At, lateness: TimeSpan.FromSeconds(2))
            .GroupBy(v => v.Page)
            .TumblingWindow(TimeSpan.FromSeconds(5))
            .Count()
            .TopK(3, r => r.Value)
            .ToOutput();
    }
}

builder.Services
    .AddDbsp<PageAnalytics>()
    .BindConfiguration("Dbsp:PageAnalytics")   // Workers, capacities, CheckpointInterval, …
    .AddOutputConsumer<LeaderboardPublisher, WindowedValue<string, long>>(d => d.TopPages)
    .AddHealthCheck();
```

The class is the circuit's key in the container. Anything that feeds or reads the circuit
injects `CircuitHost<PageAnalytics>` and redeems members for handles:

```csharp
app.MapPost("/views", (PageView view, CircuitHost<PageAnalytics> analytics) =>
    analytics.Handle(d => d.Views).InsertAsync(view));
```

A circuit that faults stops the application by default. Clustered (multi-process) circuits
are not hostable yet.

Targets net8.0 and net10.0. Use the same version of this package as of `Dbsp`.

## Documentation

- [Introduction, *Hosting it*](https://github.com/jlanng/Dbsp/blob/main/docs/intro.md#hosting-it)
- [README](https://github.com/jlanng/Dbsp/blob/main/README.md)
