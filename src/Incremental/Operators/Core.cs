using Incremental.Cluster;
using Incremental.Storage;

namespace Incremental.Operators;

/// <summary>Per-worker, per-node context handed to operator factories.</summary>
public sealed class OperatorContext
{
    public required int WorkerId { get; init; }
    public required int WorkerCount { get; init; }
    public required IStorageProvider Storage { get; init; }
    public required string NodeName { get; init; }

    /// <summary>The worker's id in the global worker set: equal to <see cref="WorkerId"/> on
    /// a single process, offset by the process's slice in a clustered circuit — which keeps
    /// state store names distinct when processes share one physical store.</summary>
    public int GlobalWorkerId { get; init; }

    /// <summary>Collects every store name the circuit's operators mint (all creation goes
    /// through <see cref="StateName"/>, in operator constructors), so the circuit can commit
    /// a state manifest with each checkpoint and refuse a mismatched restore.</summary>
    internal Action<string>? ManifestObserver { get; init; }

    /// <summary>A store name unique to this node, worker, and purpose (cluster-wide).</summary>
    public string StateName(string suffix)
    {
        var name = $"{NodeName}.{suffix}.w{GlobalWorkerId}";
        ManifestObserver?.Invoke(name);
        return name;
    }
}

/// <summary>
/// A circuit operator. Once per step, <see cref="Step"/> receives the Z-set deltas produced
/// by its input nodes this step (boxed <see cref="ZSet{T}"/> instances) and returns the
/// delta it produces. Operators are instantiated once per worker; stateful operators keep
/// worker-local state keyed by the exchange partitioning upstream of them.
/// </summary>
internal interface IStepOperator
{
    object Step(object?[] inputs);

    /// <summary>
    /// The delta to publish when every input delta this step is empty, letting the scheduler
    /// skip <see cref="Step"/> altogether — for a keyed operator downstream of an exchange
    /// that is the common case, since a small delta touches one worker's shard and leaves the
    /// others with nothing to do.
    /// <para>
    /// Null (the default) means "run me anyway", which is the safe answer and the required one
    /// for any operator that does work a delta cannot account for: reading the staged input
    /// batch, reacting to a watermark, evicting closed keys, or carrying loop state between
    /// iterations. Only return a value when <c>Step</c> over empty inputs provably changes no
    /// state and produces nothing.
    /// </para>
    /// <para>
    /// The instance is reused across steps and shared with every downstream operator that
    /// receives it, so it must stay empty. That is safe because operators treat their inputs
    /// as read-only — the same property that already lets pass-through operators return their
    /// input unchanged.
    /// </para>
    /// </summary>
    object? EmptyStepResult => null;

    /// <summary>
    /// Whether this operator is linear: running <see cref="Step"/> on the fragments of a
    /// delta and summing the outputs equals running it on the summed delta — including any
    /// state effects. Only stateless record-wise operators qualify; the fragment-streaming
    /// scheduler fuses a chain of them into the exchange they feed and runs them per chunk
    /// (see docs/design/fragment-streaming.md). Declare true only when the property holds
    /// for any partition of any input, retractions included.
    /// </summary>
    bool IsLinear => false;
}

/// <summary>
/// A stateful operator that can consume its exchanged input in fragments, as producers
/// publish them, instead of as one merged delta: <see cref="OnFragment"/> is called from the
/// exchange's sweep position with each non-empty fragment addressed to this worker (in
/// arrival order — any order is correct, which is what qualifies an operator for this
/// interface: bilinear probe-then-integrate for the join, a homomorphism fold for linear
/// aggregates), and the operator's ordinary <see cref="IStepOperator.Step"/> then runs at
/// its own sweep position as the completion call — the streamed inputs' slots hold empty
/// deltas by then, so <c>Step</c> processes whatever was not streamed and emits the step's
/// single output delta. Output content and timing are exactly the unstreamed operator's.
/// An implementation must copy what it keeps: a fragment is a producer-owned reused buffer,
/// dead after the call returns.
/// </summary>
internal interface IFragmentSinkOperator : IStepOperator
{
    void OnFragment(int input, object fragment);
}

/// <summary>
/// A fragment sink whose per-fragment output is <em>final</em> — the fragment's contribution
/// to the step's output delta, never revised by later fragments — so the runtime may take
/// it as it is produced and forward it into a downstream streamed exchange instead of
/// waiting for the operator's <see cref="IStepOperator.Step"/>. The join qualifies (each
/// fragment's probe results are exactly its share of ΔL ⋈ R + L ⋈ ΔR + ΔL ⋈ ΔR); an
/// aggregate whose output depends on the whole step's delta per key does not. When the
/// runtime forwards, every fed fragment goes through <see cref="OnFragmentEmitting"/> and
/// <c>Step</c> emits only what the non-streamed inputs produce. The returned delta is a
/// reused buffer the caller consumes before calling the operator again.
/// </summary>
internal interface IEmittingFragmentSinkOperator : IFragmentSinkOperator
{
    object OnFragmentEmitting(int input, object fragment);
}

/// <summary>
/// An exchange (shuffle) operator: repartitions its input delta across workers by hash so
/// that all records with the same routing key land on the same worker. The runtime calls
/// <see cref="Partition"/>, synchronizes all workers on a barrier, then calls
/// <see cref="Merge"/> with the fragments addressed to this worker. With a single worker
/// the runtime skips the shuffle and calls <see cref="IStepOperator.Step"/> (identity).
/// </summary>
internal interface IExchangeStepOperator : IStepOperator
{
    object[] Partition(object? input, int parts);
    object Merge(object?[] parts);

    /// <summary>Fragment streaming (point-to-point mode only; see
    /// docs/design/fragment-streaming.md). A producer partitions each chunk of its input
    /// into a ring slot via <see cref="PartitionFragment"/> — clearing the slot first, or
    /// appending when coalescing several forwarded chunks into one fragment — and
    /// release-publishes a cumulative fragment counter; <see cref="FragmentRing"/> is the
    /// ring the runtime hands consumers (its elements are written before the counter, so the
    /// counter's release/acquire pair publishes them). A consumer folds arriving fragments
    /// with <see cref="AccumulateFragment"/> and takes the step's merged delta — value-equal
    /// to <see cref="Merge"/> over the same parts — from <see cref="CompleteMerge"/>. The
    /// ring is single-buffered for the same reason the mailboxes are: every consumer
    /// finishes the execution that reads a fragment before its producer executes the node
    /// again.</summary>
    object?[][] FragmentRing { get; }

    /// <inheritdoc cref="FragmentRing"/>
    void PartitionFragment(object chunk, int parts, int ringIndex, bool append);

    /// <inheritdoc cref="FragmentRing"/>
    void AccumulateFragment(object fragment);

    /// <inheritdoc cref="FragmentRing"/>
    object CompleteMerge();

    /// <summary>Ring depth — the most fragments one execution may publish. Chunk sizing
    /// scales up to respect it, so an execution never laps its own ring.</summary>
    public const int FragmentRingSize = 8;

    /// <summary>Cluster path: merges every local sender's fragments for the
    /// <paramref name="count"/> partitions starting at <paramref name="firstShard"/> (a peer
    /// process's contiguous global-worker range) and serializes them for the wire, one shard
    /// per local worker of the receiving process.</summary>
    byte[] SerializeShards(object[][] senderFragments, int firstShard, int count);

    /// <summary>Inverse of <see cref="SerializeShards"/>: the received shards as Z-sets,
    /// indexed by the receiving process's local worker id. <paramref name="peer"/> is the
    /// sending process, so the decoder can reuse per-peer buffers.</summary>
    object[] DeserializeShards(byte[] payload, int peer);
}

/// <summary>Entry point of a circuit: emits whatever batch the coordinator staged for this worker.</summary>
internal sealed class InputOperator<T> : IStepOperator where T : notnull
{
    // Double-buffered like an OutputBuffer, with the filling split across threads: the
    // coordinator fills Pending between steps (InputPort.DrainInto — steps are gated, so a
    // fill never overlaps a running step), and Step hands the filled buffer downstream and
    // flips. The buffer handed out is cleared one step later, once its consumers —
    // downstream operators within the step, the output ports' between-step copies — are
    // done with it. An idle input keeps returning the same empty buffer without flipping,
    // which costs nothing and stays correct: the next fill lands in it between steps.
    private readonly ZSet<T> _a = new(), _b = new();
    private bool _useB;

    public InputOperator() => Pending = _a;

    /// <summary>The batch being staged for the next step. Only the coordinator writes it,
    /// between steps.</summary>
    public ZSet<T> Pending { get; private set; }

    public object Step(object?[] inputs)
    {
        var batch = Pending;
        if (!batch.IsEmpty)
        {
            Pending = (_useB = !_useB) ? _b : _a;
            Pending.Clear();
        }
        return batch;
    }
}

internal sealed class MapOperator<TIn, TOut>(Func<TIn, TOut> selector) : IStepOperator
    where TIn : notnull
    where TOut : notnull
{
    private readonly ZSet<TOut> _empty = new();
    private readonly OutputBuffer<TOut> _output = new();

    public object? EmptyStepResult => _empty;

    public bool IsLinear => true;

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<TIn>)inputs[0]!;
        var output = _output.Next(delta.DistinctCount);
        foreach (var (value, weight) in delta)
            output.Add(selector(value), weight);
        return output;
    }
}

internal sealed class FilterOperator<T>(Func<T, bool> predicate) : IStepOperator where T : notnull
{
    private readonly ZSet<T> _empty = new();
    private readonly OutputBuffer<T> _output = new();

    public object? EmptyStepResult => _empty;

    public bool IsLinear => true;

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<T>)inputs[0]!;
        var output = _output.Next(delta.DistinctCount);
        foreach (var (value, weight) in delta)
            if (predicate(value))
                output.Add(value, weight);
        return output;
    }
}

internal sealed class FlatMapOperator<TIn, TOut>(Func<TIn, IEnumerable<TOut>> selector) : IStepOperator
    where TIn : notnull
    where TOut : notnull
{
    private readonly ZSet<TOut> _empty = new();
    private readonly OutputBuffer<TOut> _output = new();

    public object? EmptyStepResult => _empty;

    public bool IsLinear => true;

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<TIn>)inputs[0]!;
        var output = _output.Next(delta.DistinctCount);
        foreach (var (value, weight) in delta)
            foreach (var expanded in selector(value))
                output.Add(expanded, weight);
        return output;
    }
}

/// <summary>
/// Hopping/tumbling window assignment: each record joins every window of <c>size</c> whose
/// start is a multiple of <c>hop</c> and which covers its event time. This is a flat map with
/// a known, constant fan-out — <c>ceil(size/hop)</c> windows per record — which is exactly what
/// the general <see cref="FlatMapOperator{TIn,TOut}"/> cannot exploit: it would size its output
/// for one row per input and then grow through it, and pay an iterator allocation per record
/// for the assignment sequence. A 30-second window hopping every 5 seconds sextuples its
/// input, so on a busy stream that is the largest Z-set the graph builds.
/// </summary>
internal sealed class WindowAssignOperator<TKey, TValue>(
    Func<TValue, long> eventTicks, long sizeTicks, long hopTicks) : IStepOperator
    where TKey : notnull
    where TValue : notnull
{
    private readonly ZSet<KeyedValue<WindowKey<TKey>, TValue>> _empty = new();
    private readonly OutputBuffer<KeyedValue<WindowKey<TKey>, TValue>> _output = new();
    private readonly int _windowsPerRecord = (int)((sizeTicks + hopTicks - 1) / hopTicks);

    public object? EmptyStepResult => _empty;

    public bool IsLinear => true;

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<KeyedValue<TKey, TValue>>)inputs[0]!;
        var output = _output.Next(delta.DistinctCount * _windowsPerRecord);
        foreach (var ((key, value), weight) in delta)
        {
            var ticks = eventTicks(value);
            var latestStart = ticks / hopTicks * hopTicks;
            for (var start = latestStart; start > ticks - sizeTicks; start -= hopTicks)
                output.Add(
                    new KeyedValue<WindowKey<TKey>, TValue>(
                        new WindowKey<TKey>(
                            new DateTimeOffset(start, TimeSpan.Zero),
                            new DateTimeOffset(start + sizeTicks, TimeSpan.Zero),
                            key),
                        value),
                    weight);
        }
        return output;
    }
}

internal sealed class NegateOperator<T> : IStepOperator where T : notnull
{
    private readonly ZSet<T> _empty = new();
    private readonly OutputBuffer<T> _output = new();

    public object? EmptyStepResult => _empty;

    public bool IsLinear => true;

    public object Step(object?[] inputs)
    {
        var delta = (ZSet<T>)inputs[0]!;
        var output = _output.Next(delta.DistinctCount);
        foreach (var (value, weight) in delta)
            output.Add(value, -weight);
        return output;
    }
}

/// <summary>Z-set addition (bag union) of any number of input streams.</summary>
internal sealed class PlusOperator<T> : IStepOperator where T : notnull
{
    private readonly ZSet<T> _empty = new();
    private readonly OutputBuffer<T> _output = new();

    public object? EmptyStepResult => _empty;

    public bool IsLinear => true;

    public object Step(object?[] inputs)
    {
        var total = 0;
        foreach (var input in inputs)
            total += ((ZSet<T>)input!).DistinctCount;
        var output = _output.Next(total);
        foreach (var input in inputs)
            output.AddAll((ZSet<T>)input!);
        return output;
    }
}

/// <summary>The routing hash must be process- and machine-stable (see <see cref="StableHash"/>):
/// cluster peers partition independently and must agree, and a restart against checkpointed
/// state must route every key to the worker whose shard holds it.</summary>
internal sealed class ExchangeOperator<T>(Func<T, uint> routingHash) : IExchangeStepOperator where T : notnull
{
    // Both the fragment array and the Z-sets in it are reused across steps rather than
    // reallocated. The array is fixed-size (the global worker count cannot change mid-run),
    // and the previous step's fragments are dead by the time this worker repartitions: every
    // worker runs every node in the same order, so between one step's Merge (which reads
    // peers' arrays, after the exchange barrier) and the next Partition on this node there is
    // always at least that barrier plus the step boundary — and, for an exchange inside a
    // Recurse region, the loop's own flag barriers. Reusing the Z-sets matters more than
    // reusing the array: a shuffle refills the same shards every step, so after a few steps
    // they are the right size and stop re-walking the dictionary's doubling ladder. Merge
    // therefore copies rather than adopting a fragment — the fragment is about to be cleared.
    private ZSet<T>[]? _fragments;

    // Single-worker fast path: no shuffle, pass the delta through untouched.
    public object Step(object?[] inputs) => inputs[0]!;

    public object[] Partition(object? input, int parts)
    {
        var fragments = _fragments;
        if (fragments is null || fragments.Length != parts)
        {
            fragments = _fragments = new ZSet<T>[parts];
            for (var i = 0; i < parts; i++)
                fragments[i] = new ZSet<T>();
        }
        else
        {
            // Clearing an already-empty dictionary is free, so the sparse case — most shards
            // empty, most executions routing nothing — costs a handful of length checks.
            foreach (var fragment in fragments)
                fragment.Clear();
        }

        var delta = (ZSet<T>)input!;
        if (delta.IsEmpty)
            return fragments;

        foreach (var (value, weight) in delta)
            fragments[(int)(routingHash(value) % (uint)parts)].Add(value, weight);
        return fragments;
    }

    /// <summary>Merged output for a worker that was sent nothing. Kept apart from the reused
    /// fragments: this one does travel downstream as the node's delta and must stay empty
    /// forever, whereas the fragments are cleared and refilled every step.</summary>
    private readonly ZSet<T> _emptyMerge = new();

    /// <summary>An exchange over all-empty inputs shuffles nothing and changes no state, so
    /// the scheduler may skip it wholesale — single-worker via the ordinary empty-input skip,
    /// point-to-point via the empty bit each producer publishes with its sequence — as long
    /// as it still publishes/consumes sequences where peers expect them.</summary>
    public object? EmptyStepResult => _emptyMerge;

    private readonly OutputBuffer<T> _merge = new();

    public object Merge(object?[] parts)
    {
        // An exchange cannot be skipped the way a plain operator can — its barrier keeps the
        // workers aligned — but a worker that was sent nothing this step still need not
        // allocate a Z-set to say so. Sizing the merge up front is the point of the counting
        // pass: the shards are disjoint by key, so their entry counts sum to the merged size
        // exactly, and the reused buffer is filled once at its final size.
        var total = 0;
        foreach (var part in parts)
            if (part is ZSet<T> fragment)
                total += fragment.DistinctCount;
        if (total == 0)
            return _emptyMerge;

        var merged = _merge.Next(total);
        foreach (var part in parts)
            if (part is ZSet<T> { IsEmpty: false } fragment)
                merged.AddAll(fragment);
        return merged;
    }

    // Fragment streaming (see the interface doc and docs/design/fragment-streaming.md).
    // The ring is the chunked analogue of _fragments: per ring slot, one reused Z-set per
    // target plus the boxed array the runtime publishes. Single-buffered like the mailboxes —
    // every consumer finishes the execution that reads a fragment before this worker executes
    // the node again — and rarely more than one slot deep in practice: only a delta larger
    // than the chunk target ever advances past slot 0.
    private readonly object?[][] _ring = new object?[IExchangeStepOperator.FragmentRingSize][];
    private readonly ZSet<T>[]?[] _ringFragments = new ZSet<T>[]?[IExchangeStepOperator.FragmentRingSize];

    public object?[][] FragmentRing => _ring;

    public void PartitionFragment(object chunk, int parts, int ringIndex, bool append)
    {
        var fragments = _ringFragments[ringIndex];
        if (fragments is null || fragments.Length != parts)
        {
            fragments = _ringFragments[ringIndex] = new ZSet<T>[parts];
            var boxed = new object[parts];
            for (var i = 0; i < parts; i++)
                boxed[i] = fragments[i] = new ZSet<T>();
            _ring[ringIndex] = boxed;
        }
        else if (!append)
        {
            foreach (var fragment in fragments)
                fragment.Clear();
        }

        foreach (var (value, weight) in (ZSet<T>)chunk)
            fragments[(int)(routingHash(value) % (uint)parts)].Add(value, weight);
    }

    /// <summary>The partial-merge accumulator: the same double-buffered <see cref="_merge"/>
    /// the batch <see cref="Merge"/> fills — streaming fills it earlier, not longer.</summary>
    private ZSet<T>? _accumulating;

    public void AccumulateFragment(object fragment) =>
        (_accumulating ??= _merge.Next(0)).AddAll((ZSet<T>)fragment);

    public object CompleteMerge()
    {
        var merged = _accumulating;
        _accumulating = null;
        return merged is { IsEmpty: false } ? merged : _emptyMerge;
    }

    // Wire encoding: a frame is one shard per target worker, each a run of (value, weight)
    // pairs. Values go through the dense codec when T has one — field values in order, no
    // names, LEB128 integers — and fall back to the storage serializer's JSON otherwise. The
    // decision is a property of T, so both ends of a mesh (same build, same types) make it
    // identically; the format byte on the frame catches the case where they somehow do not.
    //
    // Scratch is per instance and only the designated worker serializes, so the buffers below
    // are single-threaded like everything else here.
    private WireWriter? _wireWriter;
    private WireReader? _wireReader;
    private ZSet<T>? _shard;

    private const byte JsonFrame = 0;

    public byte[] SerializeShards(object[][] senderFragments, int firstShard, int count)
    {
        var shard = _shard ??= new ZSet<T>();
        if (WireCodec<T>.Write is not { } write)
        {
            var shards = new WeightedValue<T>[count][];
            for (var k = 0; k < count; k++)
            {
                shard.Clear();
                MergeShard(senderFragments, firstShard + k, shard);
                shards[k] = [.. shard];
            }
            var json = System.Text.Encoding.UTF8.GetBytes(JsonStateSerializer.Instance.Serialize(shards));
            var framed = new byte[json.Length + 1];
            framed[0] = JsonFrame;
            json.CopyTo(framed, 1);
            return framed;
        }

        var writer = _wireWriter ??= new WireWriter();
        writer.Reset();
        writer.WriteByte(WireCodec.FormatVersion);
        writer.WriteUInt32((uint)count);
        for (var k = 0; k < count; k++)
        {
            shard.Clear();
            MergeShard(senderFragments, firstShard + k, shard);
            writer.WriteUInt32((uint)shard.DistinctCount);
            foreach (var (value, weight) in shard)
            {
                write(writer, value);
                writer.WriteInt64(weight);
            }
        }
        return writer.ToArray();
    }

    private static void MergeShard(object[][] senderFragments, int shard, ZSet<T> into)
    {
        foreach (var fragments in senderFragments)
            if (fragments[shard] is ZSet<T> { IsEmpty: false } fragment)
                into.AddAll(fragment);
    }

    // Received shards decode into per-peer reused Z-sets: by the time a peer's next frame
    // for this node is deserialized, every local worker has consumed the previous one — a
    // worker must finish the execution that read it before it can arrive at (and, in
    // point-to-point mode, publish for) this node again — and Merge copies rather than
    // adopting. Keyed by peer because all peers' shards are alive at once during the merge.
    // The JSON fallback keeps allocating: it is the rare path for types with no dense codec.
    private Dictionary<int, (object[] Boxed, ZSet<T>[] Shards)>? _decoded;

    public object[] DeserializeShards(byte[] payload, int peer)
    {
        if (payload.Length == 0)
            throw new InvalidDataException("Received an empty exchange frame.");
        if (payload[0] == JsonFrame)
        {
            var shards = JsonStateSerializer.Instance.Deserialize<WeightedValue<T>[][]>(
                System.Text.Encoding.UTF8.GetString(payload, 1, payload.Length - 1));
            var decoded = new object[shards.Length];
            for (var k = 0; k < shards.Length; k++)
                decoded[k] = new ZSet<T>(shards[k]);
            return decoded;
        }
        if (payload[0] != WireCodec.FormatVersion)
            throw new InvalidDataException(
                $"Exchange frame carries wire format {payload[0]}, but this process speaks {WireCodec.FormatVersion}. " +
                "Every process of a cluster must run the same build.");

        var read = WireCodec<T>.Read
            ?? throw new InvalidDataException(
                $"Received a dense exchange frame for '{typeof(T)}', which this process has no dense codec for. " +
                "Every process of a cluster must run the same build.");
        var reader = _wireReader ??= new WireReader();
        reader.Reset(payload);
        reader.ReadByte(); // format, already checked
        var count = (int)reader.ReadUInt32();

        var cache = _decoded ??= [];
        if (!cache.TryGetValue(peer, out var buffers) || buffers.Shards.Length != count)
        {
            buffers = (new object[count], new ZSet<T>[count]);
            for (var k = 0; k < count; k++)
                buffers.Boxed[k] = buffers.Shards[k] = new ZSet<T>();
            cache[peer] = buffers;
        }

        for (var k = 0; k < count; k++)
        {
            var entries = (int)reader.ReadUInt32();
            var zset = buffers.Shards[k];
            zset.Clear();
            zset.EnsureCapacity(entries);
            for (var i = 0; i < entries; i++)
            {
                var value = read(reader);
                zset.Add(value, reader.ReadInt64());
            }
        }
        return buffers.Boxed;
    }
}
