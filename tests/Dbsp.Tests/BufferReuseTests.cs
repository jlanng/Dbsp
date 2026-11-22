using Dbsp.Operators;

namespace Dbsp.Tests;

/// <summary>
/// Reused Z-set buffers keep their capacity so that a steady workload stops allocating —
/// but emptying a dictionary costs time proportional to its capacity, so a buffer that one
/// enormous delta passed through (a table's bootstrap, a catch-up after a long stop) must
/// not charge every later step for it.
/// </summary>
public class BufferReuseTests
{
    private const int Huge = 100_000;

    private static void Fill(ZSet<int> set, int count)
    {
        for (var i = 0; i < count; i++)
            set.Add(i);
    }

    [Fact]
    public void A_reused_set_gives_back_capacity_it_has_stopped_filling()
    {
        var set = new ZSet<int>();
        Fill(set, Huge);
        Assert.True(set.Capacity >= Huge);

        for (var step = 0; step < 200; step++)
        {
            set.Clear();
            Fill(set, 3);
        }
        Assert.InRange(set.Capacity, 3, 1024);
        Assert.Equal(3, set.DistinctCount);
    }

    [Fact]
    public void A_reused_set_that_is_filled_again_now_and_then_keeps_its_capacity()
    {
        // Giving capacity back the moment a step leaves it unused would turn a bursty
        // workload's buffers back into the per-burst garbage that reuse exists to avoid.
        var set = new ZSet<int>();
        for (var step = 0; step < 500; step++)
        {
            set.Clear();
            Fill(set, step % 50 == 0 ? Huge : 3);
            Assert.True(set.Capacity >= Huge, $"capacity fell to {set.Capacity} at step {step}");
        }
    }

    [Fact]
    public void An_operators_output_buffers_shrink_after_one_table_sized_delta()
    {
        var output = new OutputBuffer<int>();
        Fill(output.Next(Huge), Huge);
        Fill(output.Next(Huge), Huge);

        var latest = output.Next(1);
        for (var step = 0; step < 400; step++)
        {
            latest = output.Next(1);
            latest.Add(step);
        }
        Assert.InRange(latest.Capacity, 1, 1024);
        Assert.InRange(output.Next(1).Capacity, 1, 1024);
    }
}
