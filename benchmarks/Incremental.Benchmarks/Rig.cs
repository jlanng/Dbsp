namespace Incremental.Benchmarks;

/// <summary>
/// Drives a manually-stepped circuit for measurement. Outputs registered via
/// <see cref="Watch{T}"/> are drained after every step, so the bounded output channels can
/// never stall a long measurement loop; <see cref="FeedAsync{T}"/> preloads state in chunks
/// with a step between each, so preloads larger than the input channel capacity cannot
/// deadlock a circuit that only advances on explicit steps.
/// </summary>
internal sealed class Rig : IAsyncDisposable
{
    private readonly List<Action> _drains = [];

    public Rig(Circuit circuit) => Circuit = circuit;

    public Circuit Circuit { get; }

    /// <summary>Options sized so a whole measured delta always fits in one step.</summary>
    public static CircuitOptions Options(int stepCapacity = 1 << 15) => new()
    {
        AutoStep = false,
        InputChannelCapacity = stepCapacity,
        MaxStepItemsPerInput = stepCapacity,
    };

    public OutputHandle<T> Watch<T>(string name) where T : notnull
    {
        var handle = Circuit.Output<T>(name);
        _drains.Add(() => { while (handle.Reader.TryRead(out _)) { } });
        return handle;
    }

    public async Task StepAsync()
    {
        await Circuit.StepAsync();
        foreach (var drain in _drains)
            drain();
    }

    public async Task FeedAsync<T>(InputHandle<T> input, IEnumerable<WeightedValue<T>> rows, int chunk = 8192)
        where T : notnull
    {
        var pending = 0;
        foreach (var row in rows)
        {
            await input.SendAsync(row.Value, row.Weight);
            if (++pending == chunk)
            {
                await StepAsync();
                pending = 0;
            }
        }
        if (pending > 0)
            await StepAsync();
    }

    public ValueTask DisposeAsync() => Circuit.DisposeAsync();
}
