namespace StreamProcessor.Tests;

public class ZSetTests
{
    [Fact]
    public void Weights_compose_additively_and_zero_entries_vanish()
    {
        var zset = new ZSet<string>();
        zset.Add("a");
        zset.Add("a", 2);
        zset.Add("b", -1);

        Assert.Equal(3, zset.WeightOf("a"));
        Assert.Equal(-1, zset.WeightOf("b"));
        Assert.Equal(2, zset.DistinctCount);

        zset.Add("a", -3);
        Assert.Equal(0, zset.WeightOf("a"));
        Assert.Equal(1, zset.DistinctCount);
    }

    [Fact]
    public void Negated_flips_every_weight()
    {
        var zset = new ZSet<int>();
        zset.Add(1, 2);
        zset.Add(2, -5);

        var negated = zset.Negated();
        Assert.Equal(-2, negated.WeightOf(1));
        Assert.Equal(5, negated.WeightOf(2));
    }

    [Fact]
    public void AddAll_is_zset_addition()
    {
        var left = new ZSet<int>();
        left.Add(1);
        left.Add(2);

        var right = new ZSet<int>();
        right.Add(2, -1);
        right.Add(3);

        left.AddAll(right);
        Assert.Equal(1, left.WeightOf(1));
        Assert.Equal(0, left.WeightOf(2));
        Assert.Equal(1, left.WeightOf(3));
    }
}
