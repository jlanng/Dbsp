using System.Globalization;

namespace Incremental.EnrichmentService;

/// <summary>
/// Keeps the world moving while the example runs for a person: trades at a steady rate
/// and, every few seconds, one change of reference data — an instrument's multiplier, a
/// trader's desk, a counterparty's name. In memory it moves the feeds the adapters read;
/// on Kafka it produces to the trades topic and updates rows in Postgres, and only while
/// one of this process's nodes leads, so that two processes do not both generate.
/// </summary>
public sealed class Generator(World world, IReadOnlyList<ExampleNode> nodes, int rate)
{
    public async Task RunAsync(CancellationToken stop)
    {
        var shape = world.Shape;
        var random = new Random(5);
        var revision = 0;
        var tick = 0;
        long produced = -1;        // Kafka: the next feed position; read from the topic when this process starts leading
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
        try
        {
            while (await timer.WaitForNextTickAsync(stop))
            {
                tick++;
                var perTick = Math.Max(1, rate / 10);
                var changeReferenceData = tick % 50 == 0;
                try
                {
                    if (world.Kafka is { } kafka)
                    {
                        if (!nodes.Any(n => n.Node.IsLeader))
                        {
                            produced = -1;
                            continue;
                        }
                        if (produced < 0)
                            produced = await Task.Run(kafka.TradesPublished, stop);
                        await kafka.ProduceTradesAsync(shape, produced, perTick);
                        produced += perTick;
                        if (changeReferenceData)
                        {
                            if (++revision % 2 == 0)
                                await kafka.SqlAsync($"update {kafka.Table("traders")} set desk_id = {1 + random.Next(shape.Desks)} where id = {1 + random.Next(shape.Traders)}");
                            else
                                await kafka.SqlAsync($"update {kafka.Table("instruments")} set multiplier = {(1 + random.Next(8) * 0.25m).ToString(CultureInfo.InvariantCulture)} where id = {1 + random.Next(shape.Instruments)}");
                        }
                    }
                    else
                    {
                        world.Trades.Publish(perTick);
                        if (changeReferenceData)
                        {
                            switch (++revision % 3)
                            {
                                case 0:
                                    world.Instruments.Publish();                                // the feed's next record revises one instrument
                                    break;
                                case 1:
                                    var trader = world.Traders.Get(1 + random.Next(shape.Traders))!;
                                    world.Traders.Upsert(trader with { DeskId = 1 + random.Next(shape.Desks) });
                                    break;
                                default:
                                    world.Counterparties.Replace(shape.Counterparty(1 + random.Next(shape.Counterparties), revision));
                                    break;
                            }
                        }
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    // The broker or the database is away for a moment: the next tick tries again.
                    if (tick % 50 == 1)
                        Console.WriteLine($"generator: {ex.Message}");
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }
}
