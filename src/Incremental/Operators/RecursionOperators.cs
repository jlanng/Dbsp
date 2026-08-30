using Incremental.Storage;

namespace Incremental.Operators;

// A Recurse loop region computes X = Distinct(seed + body(X)) to a fixed point inside every
// outer circuit step, and emits the net change of the fixed point versus the previous outer
// step. Following DBSP's recursive-circuit construction, the region runs in a nested time
// domain (outer step t, inner iteration k): the seed and every imported outer stream enter
// at k = 0 only (the δ0 operator), a feedback edge (z⁻¹ in k) circulates the loop delta,
// and the exit integrates the per-iteration deltas back into a single outer delta.
//
// Stateful operators inside the loop must be incremental in BOTH time dimensions. Their
// state therefore remembers not just how much weight each row accumulated, but at which
// inner iteration it did — the per-iteration "profile" — because the correct output at
// (t, k) is the rectangle difference
//
//     out[t][k] = f(A(t,k)) − f(A(t−1,k)) − f(A(t,k−1)) + f(A(t−1,k−1))
//
// where A(t,k) is the input accumulated over steps ≤ t and iterations ≤ k. Collapsing the
// iteration dimension (keeping only total weights) computes the wrong fixed point under
// deletion whenever derivations are cyclic: a self-supporting row (a path that derives
// itself around a cycle) would keep itself alive after its real support is retracted. The
// profile is what makes support well-founded — a row's weight at iteration k only counts
// derivations of depth k, so retractions unwind in derivation order and self-support
// cancels to zero.

/// <summary>
/// A node inside a Recurse loop region that participates in the iteration protocol. The
/// scheduler calls <see cref="BeginOuterStep"/> once per outer step before iterating,
/// <see cref="BeginIteration"/> before each pass over the region, and
/// <see cref="EndOuterStep"/> after convergence. <see cref="HasPendingWork"/> feeds the
/// convergence decision: the loop keeps iterating while any worker's operators still owe
/// emissions at a later iteration (a historical profile breakpoint not yet reached), even
/// if no delta is currently circulating.
/// </summary>
internal interface ILoopOperator
{
    void BeginOuterStep();
    void BeginIteration(int iteration);
    bool HasPendingWork(int iteration);
    void EndOuterStep();
}

/// <summary>The loop's back-edge (z⁻¹ over inner iterations): emits the stabilized delta
/// the region produced in the previous iteration, empty at iteration 0. The scheduler
/// hands it each iteration's result via <see cref="SetPending"/>; whether that delta is
/// empty on every worker is the other half of the convergence decision.</summary>
internal interface ILoopFeedbackOperator : ILoopOperator
{
    void SetPending(object? delta);
    bool PendingIsNonEmpty { get; }
}

internal sealed class LoopFeedbackOperator<T> : IStepOperator, ILoopFeedbackOperator where T : notnull
{
    private ZSet<T>? _pending;

    public void BeginOuterStep() => _pending = null;
    public void BeginIteration(int iteration) { }
    public bool HasPendingWork(int iteration) => false;
    public void EndOuterStep() => _pending = null;

    public void SetPending(object? delta) => _pending = (ZSet<T>?)delta;
    public bool PendingIsNonEmpty => _pending is { IsEmpty: false };

    public object Step(object?[] inputs) => _pending ?? new ZSet<T>();
}

/// <summary>
/// Brings an outer stream into a loop region (DBSP's δ0): the outer step's delta enters the
/// iteration domain at iteration 0 and contributes nothing afterwards — re-emitting it every
/// iteration would integrate it into loop state once per iteration. Untyped because the
/// builder inserts imports while rewriting node inputs; the empty Z-set for later iterations
/// is created from the runtime type of the iteration-0 delta.
/// </summary>
internal sealed class LoopImportOperator : IStepOperator, ILoopOperator
{
    private int _iteration;
    private object? _empty;

    public void BeginOuterStep() { }
    public void BeginIteration(int iteration) => _iteration = iteration;
    public bool HasPendingWork(int iteration) => false;
    public void EndOuterStep() { }

    public object Step(object?[] inputs)
    {
        var outer = inputs[0]!;
        return _iteration == 0
            ? outer
            : _empty ??= Activator.CreateInstance(outer.GetType())!;
    }
}

/// <summary>The loop's exit (∫ over inner iterations): sums the stabilized per-iteration
/// deltas into the single outer delta downstream operators see — the net change of this
/// step's fixed point versus the previous step's.</summary>
internal sealed class LoopExitOperator<T> : IStepOperator, ILoopOperator where T : notnull
{
    private ZSet<T> _outerDelta = new();

    public void BeginOuterStep() => _outerDelta = new ZSet<T>();
    public void BeginIteration(int iteration) { }
    public bool HasPendingWork(int iteration) => false;
    public void EndOuterStep() { }

    public object Step(object?[] inputs)
    {
        _outerDelta.AddAll((ZSet<T>)inputs[0]!);
        return _outerDelta;
    }
}

/// <summary>
/// DISTINCT inside a Recurse loop, incremental in both time dimensions. State is each
/// value's per-iteration weight profile (how much weight it gained at inner iteration k,
/// summed over all previous outer steps); the output at (step, iteration) is the rectangle
/// difference of presence over the accumulated profile. Values are tracked for the rest of
/// the outer step from the first iteration that touches them, because a change at an early
/// iteration can owe an emission at a later one — exactly where the value's historical
/// profile has its next breakpoint (see <see cref="ILoopOperator.HasPendingWork"/>).
/// </summary>
internal sealed class LoopDistinctOperator<T> : IStepOperator, ILoopOperator where T : notnull
{
    private sealed class ValueState
    {
        /// <summary>Historical weight gained at each iteration ≥ the activation iteration.</summary>
        public Dictionary<int, long>? History;

        /// <summary>Historical weight accumulated over iterations already consumed (&lt; current).</summary>
        public long HistoryBelow;

        /// <summary>This outer step's weight accumulated over iterations already consumed.</summary>
        public long CurrentBelow;

        public int MaxHistoryIteration = -1;
    }

    private readonly IIndexedWeightStore<T, int> _history;
    private readonly Dictionary<T, ValueState> _active = new();
    private readonly List<(T Value, int Iteration, long Delta)> _stepDeltas = [];
    private int _iteration;
    private int _maxPendingIteration = -1;

    public LoopDistinctOperator(OperatorContext context) =>
        _history = context.Storage.CreateIndexedWeightStore<T, int>(context.StateName("history"));

    public void BeginOuterStep() => Reset();
    public void BeginIteration(int iteration) => _iteration = iteration;
    public bool HasPendingWork(int iteration) => _maxPendingIteration > iteration;

    public void EndOuterStep()
    {
        // Fold this step's per-iteration deltas into the durable profiles, one batched
        // store call per outer step. Retractions cancel against the insertions they undo
        // at the same iteration, so fully retracted values leave no state behind.
        if (_stepDeltas.Count > 0)
            _history.ApplyAll(_stepDeltas);
        Reset();
    }

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<T>)inputs[0]!;
        Activate(delta);

        var output = new ZSet<T>();
        foreach (var (value, state) in _active)
        {
            var historyAt = state.History?.GetValueOrDefault(_iteration) ?? 0;
            var currentAt = delta.WeightOf(value);

            var before = state.HistoryBelow > 0;                                                    // (t−1, k−1)
            var beforeThisIteration = state.HistoryBelow + historyAt > 0;                           // (t−1, k)
            var nowPreviousIteration = state.HistoryBelow + state.CurrentBelow > 0;                 // (t,   k−1)
            var now = state.HistoryBelow + historyAt + state.CurrentBelow + currentAt > 0;          // (t,   k)

            var change = ((now ? 1 : 0) - (beforeThisIteration ? 1 : 0))
                       - ((nowPreviousIteration ? 1 : 0) - (before ? 1 : 0));
            if (change != 0)
                output.Add(value, change);

            state.HistoryBelow += historyAt;
            state.CurrentBelow += currentAt;
            if (currentAt != 0)
                _stepDeltas.Add((value, _iteration, currentAt));
        }
        return output;
    }

    /// <summary>Starts tracking values first touched at this iteration: one batched fetch of
    /// their profiles, with iterations already behind the loop folded into the running sum.</summary>
    private void Activate(ZSet<T> delta)
    {
        List<T>? fresh = null;
        foreach (var (value, _) in delta)
            if (!_active.ContainsKey(value))
            {
                _active.Add(value, new ValueState());
                (fresh ??= []).Add(value);
            }
        if (fresh is null)
            return;

        var groups = _history.GetGroups(fresh);
        for (var i = 0; i < fresh.Count; i++)
        {
            var state = _active[fresh[i]];
            foreach (var (iteration, weight) in groups[i])
            {
                if (iteration < _iteration)
                    state.HistoryBelow += weight;
                else
                    (state.History ??= [])[iteration] = weight;
                state.MaxHistoryIteration = Math.Max(state.MaxHistoryIteration, iteration);
            }
            _maxPendingIteration = Math.Max(_maxPendingIteration, state.MaxHistoryIteration);
        }
    }

    private void Reset()
    {
        _active.Clear();
        _stepDeltas.Clear();
        _maxPendingIteration = -1;
    }
}

/// <summary>
/// Equi-join inside a Recurse loop, incremental in both time dimensions. Each side's state
/// is its rows tagged with the inner iteration at which they arrived (per key, summed over
/// all previous outer steps). The output at (step t, iteration k) is the rectangle
/// difference of the bilinear join, which works out to four probe passes per touched key:
///
///   ΔL[t][k] ⋈ (right accumulated through (t, k))            — the familiar two, as in
///   ΔR[t][k] ⋈ (left accumulated through (t, k) minus ΔL)      the flat incremental join
///   H_L[k]   ⋈ (this step's right changes below k)           — historical rows at exactly
///   H_R[k]   ⋈ (this step's left changes below k)              iteration k meet the new
///                                                              changes from earlier ones
/// The last two are what the collapsed-time join gets wrong: they place each historical
/// row's contribution at the iteration the row originally arrived, keeping derivation
/// depths aligned across outer steps so cyclic support unwinds correctly under deletion.
/// Both inputs must be co-partitioned by key, as with the flat join.
/// </summary>
internal sealed class LoopJoinOperator<TKey, TLeft, TRight, TOut> : IStepOperator, ILoopOperator
    where TKey : notnull
    where TLeft : notnull
    where TRight : notnull
    where TOut : notnull
{
    private sealed class Side<TValue> where TValue : notnull
    {
        /// <summary>Historical rows that arrived at each iteration ≥ the activation iteration.</summary>
        public Dictionary<int, List<WeightedValue<TValue>>>? History;

        /// <summary>Historical rows accumulated over iterations already consumed (&lt; current).</summary>
        public readonly ZSet<TValue> HistoryBelow = new();

        /// <summary>This outer step's rows accumulated over iterations already consumed.</summary>
        public readonly ZSet<TValue> CurrentBelow = new();

        public int MaxHistoryIteration = -1;
    }

    private sealed class KeyState
    {
        public readonly Side<TLeft> Left = new();
        public readonly Side<TRight> Right = new();
    }

    private readonly IIndexedWeightStore<TKey, (TLeft Value, int Iteration)> _left;
    private readonly IIndexedWeightStore<TKey, (TRight Value, int Iteration)> _right;
    private readonly Func<TKey, TLeft, TRight, TOut> _resultSelector;
    private readonly Dictionary<TKey, KeyState> _active = new();
    private readonly List<(TKey Key, (TLeft Value, int Iteration) Entry, long Delta)> _leftFold = [];
    private readonly List<(TKey Key, (TRight Value, int Iteration) Entry, long Delta)> _rightFold = [];
    private int _iteration;
    private int _maxPendingIteration = -1;

    public LoopJoinOperator(OperatorContext context, Func<TKey, TLeft, TRight, TOut> resultSelector)
    {
        _left = context.Storage.CreateIndexedWeightStore<TKey, (TLeft, int)>(context.StateName("left"));
        _right = context.Storage.CreateIndexedWeightStore<TKey, (TRight, int)>(context.StateName("right"));
        _resultSelector = resultSelector;
    }

    public void BeginOuterStep() => Reset();
    public void BeginIteration(int iteration) => _iteration = iteration;
    public bool HasPendingWork(int iteration) => _maxPendingIteration > iteration;

    public void EndOuterStep()
    {
        if (_leftFold.Count > 0)
            _left.ApplyAll(_leftFold);
        if (_rightFold.Count > 0)
            _right.ApplyAll(_rightFold);
        Reset();
    }

    public object Step(object?[] inputs)
    {
        var deltaLeft = (ZSet<KeyedValue<TKey, TLeft>>)inputs[0]!;
        var deltaRight = (ZSet<KeyedValue<TKey, TRight>>)inputs[1]!;
        Activate(deltaLeft, deltaRight);

        var nowLeft = Bucket(deltaLeft, _leftFold);
        var nowRight = Bucket(deltaRight, _rightFold);

        var output = new ZSet<TOut>();
        foreach (var (key, state) in _active)
        {
            var historyLeft = state.Left.History?.GetValueOrDefault(_iteration);
            var historyRight = state.Right.History?.GetValueOrDefault(_iteration);

            // Historical rows at exactly this iteration ⋈ the current step's changes from
            // earlier iterations (which they never met: those changes did not exist when
            // these rows last passed through iteration k).
            if (historyLeft is not null)
                foreach (var (leftValue, leftWeight) in historyLeft)
                    foreach (var (rightValue, rightWeight) in state.Right.CurrentBelow)
                        output.Add(_resultSelector(key, leftValue, rightValue), checked(leftWeight * rightWeight));
            if (historyRight is not null)
                foreach (var (rightValue, rightWeight) in historyRight)
                    foreach (var (leftValue, leftWeight) in state.Left.CurrentBelow)
                        output.Add(_resultSelector(key, leftValue, rightValue), checked(leftWeight * rightWeight));

            Consume(state.Left.HistoryBelow, historyLeft);
            Consume(state.Right.HistoryBelow, historyRight);

            // ΔR ⋈ left accumulated through (t, k), ΔL[k] not yet included.
            if (nowRight?.GetValueOrDefault(key) is { } rightRows)
                foreach (var (rightValue, rightWeight) in rightRows)
                {
                    foreach (var (leftValue, leftWeight) in state.Left.HistoryBelow)
                        output.Add(_resultSelector(key, leftValue, rightValue), checked(leftWeight * rightWeight));
                    foreach (var (leftValue, leftWeight) in state.Left.CurrentBelow)
                        output.Add(_resultSelector(key, leftValue, rightValue), checked(leftWeight * rightWeight));
                    state.Right.CurrentBelow.Add(rightValue, rightWeight);
                }

            // ΔL ⋈ right accumulated through (t, k), ΔR[k] included — covers the ΔL ⋈ ΔR cross term.
            if (nowLeft?.GetValueOrDefault(key) is { } leftRows)
                foreach (var (leftValue, leftWeight) in leftRows)
                {
                    foreach (var (rightValue, rightWeight) in state.Right.HistoryBelow)
                        output.Add(_resultSelector(key, leftValue, rightValue), checked(leftWeight * rightWeight));
                    foreach (var (rightValue, rightWeight) in state.Right.CurrentBelow)
                        output.Add(_resultSelector(key, leftValue, rightValue), checked(leftWeight * rightWeight));
                    state.Left.CurrentBelow.Add(leftValue, leftWeight);
                }
        }
        return output;
    }

    /// <summary>Starts tracking keys first touched at this iteration: one batched fetch per
    /// side of their iteration-tagged rows, with iterations already behind the loop folded
    /// into the running accumulations.</summary>
    private void Activate(ZSet<KeyedValue<TKey, TLeft>> deltaLeft, ZSet<KeyedValue<TKey, TRight>> deltaRight)
    {
        List<TKey>? fresh = null;
        foreach (var ((key, _), _) in deltaLeft)
            if (!_active.ContainsKey(key))
            {
                _active.Add(key, new KeyState());
                (fresh ??= []).Add(key);
            }
        foreach (var ((key, _), _) in deltaRight)
            if (!_active.ContainsKey(key))
            {
                _active.Add(key, new KeyState());
                (fresh ??= []).Add(key);
            }
        if (fresh is null)
            return;

        var leftGroups = _left.GetGroups(fresh);
        var rightGroups = _right.GetGroups(fresh);
        for (var i = 0; i < fresh.Count; i++)
        {
            var state = _active[fresh[i]];
            Load(state.Left, leftGroups[i]);
            Load(state.Right, rightGroups[i]);
            _maxPendingIteration = Math.Max(
                _maxPendingIteration,
                Math.Max(state.Left.MaxHistoryIteration, state.Right.MaxHistoryIteration));
        }
    }

    private void Load<TValue>(Side<TValue> side, IReadOnlyList<WeightedValue<(TValue Value, int Iteration)>> entries)
        where TValue : notnull
    {
        foreach (var ((value, iteration), weight) in entries)
        {
            if (iteration < _iteration)
            {
                side.HistoryBelow.Add(value, weight);
            }
            else
            {
                side.History ??= [];
                if (!side.History.TryGetValue(iteration, out var rows))
                    side.History[iteration] = rows = [];
                rows.Add(new WeightedValue<TValue>(value, weight));
            }
            side.MaxHistoryIteration = Math.Max(side.MaxHistoryIteration, iteration);
        }
    }

    private static void Consume<TValue>(ZSet<TValue> accumulated, List<WeightedValue<TValue>>? rows)
        where TValue : notnull
    {
        if (rows is null)
            return;
        foreach (var (value, weight) in rows)
            accumulated.Add(value, weight);
    }

    private Dictionary<TKey, List<WeightedValue<TValue>>>? Bucket<TValue>(
        ZSet<KeyedValue<TKey, TValue>> delta,
        List<(TKey Key, (TValue Value, int Iteration) Entry, long Delta)> fold)
        where TValue : notnull
    {
        if (delta.IsEmpty)
            return null;
        var buckets = new Dictionary<TKey, List<WeightedValue<TValue>>>();
        foreach (var ((key, value), weight) in delta)
        {
            if (!buckets.TryGetValue(key, out var rows))
                buckets[key] = rows = [];
            rows.Add(new WeightedValue<TValue>(value, weight));
            fold.Add((key, (value, _iteration), weight));
        }
        return buckets;
    }

    private void Reset()
    {
        _active.Clear();
        _leftFold.Clear();
        _rightFold.Clear();
        _maxPendingIteration = -1;
    }
}
