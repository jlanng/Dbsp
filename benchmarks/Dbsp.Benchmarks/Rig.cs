namespace Dbsp.Benchmarks;

/// <summary>
/// Drives a manually-stepped circuit for measurement. Outputs registered via
/// <see cref="Watch{T}"/> get a discarding consumer, so the bounded output channels can
/// never stall a long measurement loop; <see cref="FeedAsync{T}"/> preloads state in chunks
/// with a step between each, so preloads larger than the input channel capacity cannot
/// deadlock a circuit that only advances on explicit steps.
/// </summary>
internal sealed class Rig : IAsyncDisposable
{
    private readonly List<Task> _consumers = [];

    public Rig(Circuit circuit) => Circuit = circuit;

    public Circuit Circuit { get; }

    /// <summary>Options sized so a whole measured delta always fits in one step.</summary>
    public static CircuitOptions Options(int stepCapacity = 1 << 15, Dbsp.Storage.IStorageProvider? storage = null) => new()
    {
        AutoStep = false,
        InputChannelCapacity = stepCapacity,
        MaxStepItemsPerInput = stepCapacity,
        Storage = storage ?? new Dbsp.Storage.InMemoryStorageProvider(),
    };

    /// <summary>Redeems <paramref name="output"/> and discards its changefeed for the life
    /// of the rig, keeping the output channel drained. Use <c>ToOutput(bootstrap: false)</c>
    /// on keyed/windowed results so measurements don't absorb snapshot-maintenance cost.</summary>
    public void Watch<T>(Output<T> output) where T : notnull
    {
        var handle = Circuit.Handle(output);
        _consumers.Add(Task.Run(async () =>
        {
            await foreach (var _ in handle.Changes())
            {
            }
        }));
    }

    public Task StepAsync() => Circuit.StepAsync();

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

    public async ValueTask DisposeAsync()
    {
        await Circuit.DisposeAsync();
        await Task.WhenAll(_consumers);
    }
}
