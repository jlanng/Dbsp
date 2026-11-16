using System.Reflection;
using System.Runtime.CompilerServices;

namespace Incremental.Storage;

/// <summary>
/// Whether two equal values of a type always serialize to the same bytes. A changelog
/// record is found again by its key's bytes — by the log's compaction, and by whoever
/// restores from it — while a store finds an entry by <c>Equals</c>. Where the two can
/// disagree (<c>10.0m</c> equals <c>10.00m</c>; one instant under two offsets; <c>0.0</c>
/// and <c>-0.0</c>; any hand-written equality) a store has to remember which instance it
/// wrote an entry under: see <see cref="HeldInstances{T}"/>.
/// <para>
/// The answer is structural and errs towards "no", which costs memory, never correctness:
/// yes for the integral primitives, <c>bool</c>, <c>char</c>, <c>string</c>, <c>Guid</c>,
/// enums, <c>TimeSpan</c>, <c>DateOnly</c> and <c>TimeOnly</c>; for tuples, records and
/// structs whose equality is the compiler's or the runtime's field-by-field one and whose
/// fields all qualify; and for the engine's own window types, whose bounds it always
/// builds in UTC. No for everything else.
/// </para>
/// </summary>
internal static class ByteStableEquality
{
    public static bool Holds<T>() => Cached<T>.Value;

    private static class Cached<T>
    {
        public static readonly bool Value = Holds(typeof(T));
    }

    public static bool Holds(Type type) => Holds(type, []);

    private static bool Holds(Type type, HashSet<Type> visiting)
    {
        if (type.IsEnum)
            return true;
        if (type == typeof(float) || type == typeof(double) || type == typeof(Half))
            return false; // 0.0 equals -0.0
        if (type.IsPrimitive || type == typeof(string) || type == typeof(Guid) || type == typeof(TimeSpan)
            || type == typeof(DateOnly) || type == typeof(TimeOnly) || type == typeof(Int128) || type == typeof(UInt128))
            return true;
        if (type == typeof(decimal) || type == typeof(DateTime) || type == typeof(DateTimeOffset))
            return false; // scale; kind; offset
        if (Nullable.GetUnderlyingType(type) is { } underlying)
            return Holds(underlying, visiting);
        if (type.IsArray || type.IsPointer || type.IsInterface || type.IsAbstract || type == typeof(object)
            || typeof(Delegate).IsAssignableFrom(type))
            return false;

        // The engine's window types: their bounds are DateTimeOffsets, and the engine only
        // ever builds them at offset zero.
        if (type.IsGenericType && type.GetGenericTypeDefinition() is var definition
            && (definition == typeof(WindowKey<>) || definition == typeof(Windowed<>) || definition == typeof(WindowedValue<,>)))
            return type.GetGenericArguments().All(argument => Holds(argument, visiting));
        if (type == typeof(WindowBounds))
            return true;

        if (!visiting.Add(type))
            return false; // a type that contains itself: not worth following
        try
        {
            for (var level = type; level is not null && level != typeof(object) && level != typeof(ValueType); level = level.BaseType)
            {
                if (!ComparesItsFields(level))
                    return false;
                foreach (var field in level.GetFields(
                             BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
                    if (!Holds(field.FieldType, visiting))
                        return false;
            }
            return true;
        }
        finally
        {
            visiting.Remove(type);
        }
    }

    /// <summary>Whether <paramref name="type"/>'s own equality is field by field, each
    /// field by its own equality: a tuple, a record whose <c>Equals</c> the compiler wrote,
    /// or a struct that left equality to the runtime.</summary>
    private static bool ComparesItsFields(Type type)
    {
        if (type.FullName is { } name && (name.StartsWith("System.ValueTuple`", StringComparison.Ordinal)
                                          || name.StartsWith("System.Tuple`", StringComparison.Ordinal)))
            return true;

        const BindingFlags declared = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;
        if (type.GetMethod(nameof(Equals), declared, [type]) is { } typed)
            return typed.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false);
        return type.IsValueType && type.GetMethod(nameof(Equals), declared, [typeof(object)]) is null;
    }
}

/// <summary>
/// For a store on the changelog: the instance each live entry's record was written under.
/// A commit writes an entry's record — and its tombstone — under that instance and no
/// other, however many equal instances have touched the entry since, so the log holds one
/// record per entry and removing the entry removes it. Holds nothing, and costs nothing,
/// for a type whose equal values serialize alike (<see cref="ByteStableEquality"/>).
/// </summary>
internal sealed class HeldInstances<T> where T : notnull
{
    private readonly HashSet<T>? _held = ByteStableEquality.Holds<T>() ? null : [];

    /// <summary>False when there is nothing to track: every method is then a no-op that
    /// returns its argument.</summary>
    public bool Active => _held is not null;

    public int Count => _held?.Count ?? 0;

    /// <summary>The instance held for <paramref name="value"/>; <paramref name="value"/>
    /// itself, now held, if there was none.</summary>
    public T Hold(T value)
    {
        if (_held is null)
            return value;
        if (_held.TryGetValue(value, out var held))
            return held;
        _held.Add(value);
        return value;
    }

    /// <summary>The instance held for <paramref name="value"/>, or <paramref name="value"/>.</summary>
    public T Of(T value) => _held is not null && _held.TryGetValue(value, out var held) ? held : value;

    public void Release(T value) => _held?.Remove(value);

    public void Clear() => _held?.Clear();
}
