namespace Incremental.Tests;

public class OptionsValidationTests
{
    [Fact]
    public void Non_positive_capacities_are_rejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new CircuitBuilder(new CircuitOptions { InputChannelCapacity = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new CircuitBuilder(new CircuitOptions { OutputChannelCapacity = -1 }));
        // Zero would send the auto-step pump into a hot loop: input pending, nothing drained.
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new CircuitBuilder(new CircuitOptions { MaxStepItemsPerInput = 0 }));
    }

}
