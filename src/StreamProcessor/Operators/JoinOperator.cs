using StreamProcessor.Storage;

namespace StreamProcessor.Operators;

/// <summary>
/// Incremental equi-join. A join is bilinear, so its incremental form is
/// Δ(L ⋈ R) = ΔL ⋈ R + L ⋈ ΔR + ΔL ⋈ ΔR: each side keeps its integrated state in an
/// indexed store, and each step only probes the state with the new deltas — no
/// recomputation over historical data. Both inputs must be co-partitioned by key, which
/// <c>IndexBy</c> guarantees via its exchange.
/// </summary>
internal sealed class JoinOperator<TKey, TLeft, TRight, TOut> : IStepOperator
    where TKey : notnull
    where TLeft : notnull
    where TRight : notnull
    where TOut : notnull
{
    private readonly IIndexedWeightStore<TKey, TLeft> _left;
    private readonly IIndexedWeightStore<TKey, TRight> _right;
    private readonly Func<TKey, TLeft, TRight, TOut> _resultSelector;

    public JoinOperator(OperatorContext context, Func<TKey, TLeft, TRight, TOut> resultSelector)
    {
        _left = context.Storage.CreateIndexedWeightStore<TKey, TLeft>(context.StateName("left"));
        _right = context.Storage.CreateIndexedWeightStore<TKey, TRight>(context.StateName("right"));
        _resultSelector = resultSelector;
    }

    public object Step(object?[] inputs)
    {
        var deltaLeft = (ZSet<KeyedValue<TKey, TLeft>>)inputs[0]!;
        var deltaRight = (ZSet<KeyedValue<TKey, TRight>>)inputs[1]!;
        var output = new ZSet<TOut>();

        // ΔL ⋈ R (right state not yet updated with ΔR)
        foreach (var ((key, left), leftWeight) in deltaLeft)
            foreach (var (right, rightWeight) in _right.GetGroup(key))
                output.Add(_resultSelector(key, left, right), checked(leftWeight * rightWeight));

        // Integrate ΔL, so the pass below also covers the ΔL ⋈ ΔR cross term.
        foreach (var ((key, left), leftWeight) in deltaLeft)
            _left.Update(key, left, leftWeight);

        // (L + ΔL) ⋈ ΔR
        foreach (var ((key, right), rightWeight) in deltaRight)
            foreach (var (left, leftWeight) in _left.GetGroup(key))
                output.Add(_resultSelector(key, left, right), checked(leftWeight * rightWeight));

        foreach (var ((key, right), rightWeight) in deltaRight)
            _right.Update(key, right, rightWeight);

        return output;
    }
}
