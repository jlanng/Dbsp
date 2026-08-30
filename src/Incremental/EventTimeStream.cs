using Incremental.Operators;

namespace Incremental;

/// <summary>
/// A stream whose records carry a declared event time (see
/// <see cref="ZStream{T}.WithEventTime"/>). The timestamp selector travels in the type:
/// groupings of this stream can be windowed, and operations that preserve the record shape
/// (<see cref="Where"/>, <see cref="Distinct()"/>) keep the event time, while reshaping ones
/// (<c>Select</c>, <c>SelectMany</c>) return a plain stream — the selector no longer applies
/// to the new shape, so re-declare event time after reshaping.
/// </summary>
public sealed class EventTimeStream<T> : ZStream<T> where T : notnull
{
    private readonly Func<T, DateTimeOffset> _eventTime;

    internal EventTimeStream(CircuitBuilder builder, int nodeId, Func<T, DateTimeOffset> eventTime)
        : base(builder, nodeId)
    {
        _eventTime = eventTime;
    }

    /// <summary>Names the operator this stream carries, keeping the event time; see
    /// <see cref="ZStream{T}.Named"/>.</summary>
    public new EventTimeStream<T> Named(string name)
    {
        Builder.SetStateId(NodeId, name);
        return this;
    }

    /// <summary>Keeps the records matching <paramref name="predicate"/>, keeping the event time.</summary>
    public new EventTimeStream<T> Where(Func<T, bool> predicate) =>
        new(Builder,
            Builder.AddNode("where", [NodeId], _ => new FilterOperator<T>(predicate)),
            _eventTime);

    /// <summary>Collapses the multiset to a set, keeping the event time; see
    /// <see cref="ZStream{T}.Distinct()"/>.</summary>
    public new EventTimeStream<T> Distinct() =>
        new(Builder, DistinctCore(Builder.Options.IsSharded ? StableHash.RoutingHasherFor<T>("Distinct") : null).NodeId, _eventTime);

    /// <summary>Collapses the multiset to a set with a caller-supplied routing hash, keeping
    /// the event time; see <see cref="ZStream{T}.Distinct(Func{T,uint})"/>.</summary>
    public new EventTimeStream<T> Distinct(Func<T, uint> stableHash)
    {
        ArgumentNullException.ThrowIfNull(stableHash);
        return new(Builder, DistinctCore(stableHash).NodeId, _eventTime);
    }

    /// <summary>Groups the event-timed stream by <paramref name="keySelector"/>; the result
    /// offers everything a plain grouping does, plus windows. See
    /// <see cref="ZStream{T}.GroupBy{TKey}(Func{T,TKey})"/> for the routing contract.</summary>
    public new GroupedEventTimeStream<TKey, T> GroupBy<TKey>(Func<T, TKey> keySelector) where TKey : notnull =>
        GroupByCore(keySelector, Builder.Options.IsSharded ? StableHash.RoutingHasherFor<TKey>("GroupBy") : null);

    /// <summary>Groups the event-timed stream with a caller-supplied routing hash; see
    /// <see cref="ZStream{T}.GroupBy{TKey}(Func{T,TKey},Func{TKey,uint})"/>.</summary>
    public new GroupedEventTimeStream<TKey, T> GroupBy<TKey>(Func<T, TKey> keySelector, Func<TKey, uint> stableHash)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(stableHash);
        return GroupByCore(keySelector, stableHash);
    }

    private GroupedEventTimeStream<TKey, T> GroupByCore<TKey>(Func<T, TKey> keySelector, Func<TKey, uint>? keyHash)
        where TKey : notnull
    {
        var keyed = Builder.AddNode(
            "groupBy",
            [NodeId],
            _ => new MapOperator<T, KeyedValue<TKey, T>>(v => new KeyedValue<TKey, T>(keySelector(v), v)));

        var node = keyed;
        if (Builder.Options.IsSharded)
        {
            node = Builder.AddNode(
                "exchange",
                [keyed],
                _ => new ExchangeOperator<KeyedValue<TKey, T>>(kv => keyHash!(kv.Key)),
                isExchange: true);
        }
        return new GroupedEventTimeStream<TKey, T>(Builder, node, _eventTime);
    }
}
