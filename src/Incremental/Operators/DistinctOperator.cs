using Incremental.Storage;

namespace Incremental.Operators;

/// <summary>
/// Incremental DISTINCT: maintains the integrated weight of every value and emits a delta
/// only when a value crosses between "present" (integrated weight &gt; 0) and "absent".
/// Requires all occurrences of a value to be routed to the same worker; the builder inserts
/// an exchange on the value hash upstream when the circuit runs with multiple workers.
/// </summary>
internal sealed class DistinctOperator<T> : IStepOperator where T : notnull
{
    private readonly IWeightStore<T> _integrated;

    public DistinctOperator(OperatorContext context) =>
        _integrated = context.Storage.CreateWeightStore<T>(context.StateName("integrated"));

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<T>)inputs[0]!;
        var output = new ZSet<T>();
        foreach (var (value, weight) in delta)
        {
            var before = _integrated.WeightOf(value);
            var after = _integrated.Update(value, weight);
            var change = (after > 0 ? 1 : 0) - (before > 0 ? 1 : 0);
            if (change != 0)
                output.Add(value, change);
        }
        return output;
    }
}
