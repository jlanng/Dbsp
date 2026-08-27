using Incremental.Operators;
using Incremental.Runtime;

namespace Incremental;

internal sealed class NodeDefinition
{
    public required int Id { get; init; }
    public required string Name { get; init; }
    public required int[] Inputs { get; init; }
    public bool IsExchange { get; init; }
    public required Func<OperatorContext, IStepOperator> Factory { get; init; }
}

/// <summary>
/// Declares the dataflow graph of a circuit: inputs, operators, and outputs. Streams are
/// handles into the graph under construction; <see cref="Build"/> freezes it into a runnable
/// <see cref="Circuit"/>.
/// </summary>
public sealed class CircuitBuilder
{
    private readonly List<NodeDefinition> _nodes = [];
    private readonly List<InputRegistration> _inputs = [];
    private readonly List<OutputRegistration> _outputs = [];
    private bool _built;

    public CircuitBuilder(CircuitOptions? options = null)
    {
        Options = options ?? new CircuitOptions();
        ArgumentOutOfRangeException.ThrowIfLessThan(Options.Workers, 1, nameof(Options.Workers));
        ArgumentOutOfRangeException.ThrowIfLessThan(Options.CheckpointInterval, 1, nameof(Options.CheckpointInterval));
    }

    public CircuitOptions Options { get; }

    /// <summary>The circuit's shared event-time clock; created by the first <c>WithWatermark</c>.</summary>
    internal WatermarkClock? WatermarkClock { get; private set; }

    internal WatermarkClock GetOrCreateWatermarkClock() => WatermarkClock ??= new WatermarkClock(Options.Workers);

    /// <summary>Declares a named input. Feed it after <see cref="Build"/> via <see cref="Circuit.Input{T}"/>.</summary>
    public ZStream<T> Input<T>(string name) where T : notnull
    {
        ThrowIfBuilt();
        if (_inputs.Any(i => i.Name == name))
            throw new ArgumentException($"An input named '{name}' already exists.", nameof(name));

        var nodeId = AddNode($"input[{name}]", [], _ => new InputOperator<T>());
        _inputs.Add(new InputRegistration<T> { Name = name, NodeId = nodeId });
        return new ZStream<T>(this, nodeId);
    }

    /// <summary>Declares a named input with upsert/delete-by-key semantics: after
    /// <see cref="Build"/>, <see cref="Circuit.KeyedInput{TKey,TValue}"/> returns a handle
    /// whose <c>UpsertAsync</c> retracts the previous row for the key automatically.</summary>
    public ZStream<TValue> KeyedInput<TKey, TValue>(string name, Func<TValue, TKey> keySelector)
        where TKey : notnull
        where TValue : notnull
    {
        ThrowIfBuilt();
        if (_inputs.Any(i => i.Name == name))
            throw new ArgumentException($"An input named '{name}' already exists.", nameof(name));

        var nodeId = AddNode($"input[{name}]", [], _ => new InputOperator<TValue>());
        _inputs.Add(new KeyedInputRegistration<TKey, TValue> { Name = name, NodeId = nodeId, KeySelector = keySelector });
        return new ZStream<TValue>(this, nodeId);
    }

    internal int AddNode(string kind, int[] inputs, Func<OperatorContext, IStepOperator> factory, bool isExchange = false)
    {
        ThrowIfBuilt();
        var id = _nodes.Count;
        _nodes.Add(new NodeDefinition
        {
            Id = id,
            Name = $"n{id}.{kind}",
            Inputs = inputs,
            IsExchange = isExchange,
            Factory = factory,
        });
        return id;
    }

    internal void AddOutput<T>(string name, int nodeId) where T : notnull
    {
        ThrowIfBuilt();
        if (_outputs.Any(o => o.Name == name))
            throw new ArgumentException($"An output named '{name}' already exists.", nameof(name));
        _outputs.Add(new OutputRegistration<T> { Name = name, NodeId = nodeId });
    }

    public Circuit Build()
    {
        ThrowIfBuilt();
        _built = true;
        return new Circuit([.. _nodes], _inputs, _outputs, Options, WatermarkClock);
    }

    private void ThrowIfBuilt()
    {
        if (_built)
            throw new InvalidOperationException("The circuit has already been built; the builder can no longer be modified.");
    }
}
