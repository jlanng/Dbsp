using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;

namespace Incremental;

/// <summary>
/// Deterministic structural hashing: the hash of a value is the same in every process, on
/// every machine, across restarts — unlike <see cref="object.GetHashCode"/>, which .NET
/// randomizes per process for strings (and therefore for every composite containing one).
/// Exchange operators route records to workers by this hash, and the MIN/MAX tournament
/// trees derive their persistent bucket slots from it, so sharded state stays addressable
/// after a restart against a persistent storage provider and agrees across the processes of
/// a cluster.
///
/// Supported types: the primitive numerics, <c>bool</c>, <c>char</c>, enums, <c>string</c>,
/// <c>decimal</c>, <c>Guid</c>, the temporal types (<c>DateTime</c>, <c>DateTimeOffset</c>,
/// <c>TimeSpan</c>, <c>DateOnly</c>, <c>TimeOnly</c>), <c>Nullable&lt;T&gt;</c>,
/// single-dimensional arrays and <c>IReadOnlyList&lt;T&gt;</c> collections of supported
/// types, and compositions of the above — tuples, records, and plain types, hashed over
/// their public readable properties and fields (compiled once per type). Equal values hash
/// equally as long as equality agrees with the public structure; a type with custom
/// equality semantics (say, case-insensitive keys) needs a caller-supplied hash instead —
/// see the <c>IndexBy</c>/<c>Distinct</c> overloads taking a <c>stableHash</c> delegate.
/// Unsupported types are rejected when the circuit is defined, never mid-step.
/// </summary>
public static class StableHash
{
    private const ulong FnvOffset = 14695981039346656037ul;
    private const ulong FnvPrime = 1099511628211ul;

    /// <summary>Contribution of a null member/element, chosen to collide with nothing likely.</summary>
    private const ulong NullSentinel = 0x9E3779B97F4A7C15ul;

    /// <summary>The stable hash of <paramref name="value"/>. Deterministic across processes
    /// and machines for the supported types; throws <see cref="NotSupportedException"/> for
    /// an unsupported <typeparamref name="T"/>.</summary>
    public static ulong Of<T>(T value) where T : notnull => OfCore(value);

    /// <summary>The compiled hasher for <typeparamref name="T"/>; resolving it validates the
    /// whole type graph, so unsupported types fail here — at definition time — with a
    /// descriptive <see cref="NotSupportedException"/>.</summary>
    internal static Func<T, ulong> HasherFor<T>() => Cache<T>.Hasher ??= BuildHasher<T>();

    /// <summary>Resolves the exchange-routing hash for a sharded circuit, translating an
    /// unsupported key type into an error that names the operator and the escape hatch.</summary>
    internal static Func<TKey, uint> RoutingHasherFor<TKey>(string operation) where TKey : notnull
    {
        Func<TKey, ulong> hash;
        try
        {
            hash = HasherFor<TKey>();
        }
        catch (NotSupportedException e)
        {
            throw new InvalidOperationException(
                $"{operation} on a sharded circuit (Workers > 1 or clustered) routes records to workers by a " +
                $"process-stable hash of '{typeof(TKey)}', which could not be derived. {e.Message} " +
                $"Supply one explicitly via the {operation} overload taking a stableHash delegate — it must return " +
                "the same value for equal keys in every process.", e);
        }
        return v => (uint)hash(v);
    }

    // One compiled hasher per closed type. The null-then-assign race is benign: BuildHasher
    // is pure and deterministic, so concurrent resolvers produce identical delegates.
    private static class Cache<T>
    {
        internal static Func<T, ulong>? Hasher;
    }

    private static ulong OfCore<T>(T value) => (Cache<T>.Hasher ??= BuildHasher<T>())(value);

    private static Func<T, ulong> BuildHasher<T>()
    {
        Validate(typeof(T), typeof(T), path: null, seen: []);
        var value = Expression.Parameter(typeof(T), "value");
        var body = Expression.Call(Method(nameof(Mix)), RawHash(typeof(T), value));
        return Expression.Lambda<Func<T, ulong>>(body, value).Compile();
    }

    // ---- classification ----------------------------------------------------------------

    private static bool IsIntegralLeaf(Type t) =>
        t == typeof(byte) || t == typeof(sbyte) || t == typeof(short) || t == typeof(ushort) ||
        t == typeof(int) || t == typeof(uint) || t == typeof(long) || t == typeof(ulong) ||
        t == typeof(char) || t == typeof(bool);

    private static bool IsLeaf(Type t) =>
        IsIntegralLeaf(t) || t.IsEnum ||
        t == typeof(string) || t == typeof(float) || t == typeof(double) || t == typeof(decimal) ||
        t == typeof(Guid) || t == typeof(DateTime) || t == typeof(DateTimeOffset) ||
        t == typeof(TimeSpan) || t == typeof(DateOnly) || t == typeof(TimeOnly);

    private static Type? ListElementType(Type t)
    {
        if (t.IsArray)
            return t.GetArrayRank() == 1 ? t.GetElementType() : null;
        if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IReadOnlyList<>))
            return t.GetGenericArguments()[0];
        foreach (var itf in t.GetInterfaces())
            if (itf.IsGenericType && itf.GetGenericTypeDefinition() == typeof(IReadOnlyList<>))
                return itf.GetGenericArguments()[0];
        return null;
    }

    /// <summary>Public readable non-indexer properties plus public fields, sorted by name so
    /// the hash does not depend on reflection or declaration order.</summary>
    private static List<(string Name, Type Type)> Members(Type t)
    {
        var members = new List<(string, Type)>();
        foreach (var p in t.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            if (p.CanRead && p.GetIndexParameters().Length == 0)
                members.Add((p.Name, p.PropertyType));
        foreach (var f in t.GetFields(BindingFlags.Public | BindingFlags.Instance))
            members.Add((f.Name, f.FieldType));
        members.Sort(static (a, b) => string.CompareOrdinal(a.Item1, b.Item1));
        return members;
    }

    // ---- validation (eager, so unsupported types fail at circuit definition time) --------

    private static void Validate(Type type, Type root, string? path, HashSet<Type> seen)
    {
        if (!seen.Add(type))
            return; // already checked, or a recursive type — its own resolution re-validates

        if (IsLeaf(type))
            return;

        var underlying = Nullable.GetUnderlyingType(type);
        if (underlying is not null)
        {
            Validate(underlying, root, path, seen);
            return;
        }

        if (type.IsArray && type.GetArrayRank() > 1)
            throw Unsupported(type, root, path, "is a multi-dimensional array");
        if (ListElementType(type) is { } element)
        {
            Validate(element, root, path is null ? "[]" : path + "[]", seen);
            return;
        }

        if (type == typeof(object) || type.IsInterface || type.IsAbstract)
            throw Unsupported(type, root, path, "is not a concrete type, so its runtime representation cannot be hashed deterministically");
        if (typeof(Delegate).IsAssignableFrom(type))
            throw Unsupported(type, root, path, "is a delegate");
        if (type.IsPointer || type == typeof(IntPtr) || type == typeof(UIntPtr))
            throw Unsupported(type, root, path, "is a native pointer");
        if (typeof(System.Collections.IEnumerable).IsAssignableFrom(type))
            throw Unsupported(type, root, path, "is a collection without a stable indexed order (arrays and IReadOnlyList<T> are supported)");

        var members = Members(type);
        if (members.Count == 0)
            throw Unsupported(type, root, path, "has no public readable properties or fields, so every instance would hash identically");
        foreach (var (name, memberType) in members)
            Validate(memberType, root, path is null ? name : $"{path}.{name}", seen);
    }

    private static NotSupportedException Unsupported(Type type, Type root, string? path, string reason) =>
        new(path is null
            ? $"Type '{type}' cannot be stably hashed: it {reason}."
            : $"Type '{root}' cannot be stably hashed: member '{path}' of type '{type}' {reason}.");

    // ---- expression construction ---------------------------------------------------------

    /// <summary>The pre-mix hash of <paramref name="value"/> (an expression of exactly
    /// <paramref name="type"/>, never null). Composite members defer to <see cref="OfCore{T}"/>
    /// so every type's hasher is compiled and cached once.</summary>
    private static Expression RawHash(Type type, Expression value)
    {
        if (type == typeof(string))
            return Expression.Call(Method(nameof(HashString)), value);
        if (type == typeof(bool))
            return Expression.Condition(value, Expression.Constant(1ul), Expression.Constant(0ul));
        if (type.IsEnum)
            return RawHash(type.GetEnumUnderlyingType(), Expression.Convert(value, type.GetEnumUnderlyingType()));
        if (IsIntegralLeaf(type))
            return IntegralToULong(type, value);
        if (type == typeof(float) || type == typeof(double))
            return Expression.Call(Method(nameof(HashDouble)),
                type == typeof(double) ? value : Expression.Convert(value, typeof(double)));
        if (type == typeof(decimal))
            return Expression.Call(Method(nameof(HashDecimal)), value);
        if (type == typeof(Guid))
            return Expression.Call(Method(nameof(HashGuid)), value);
        if (type == typeof(DateTime))
            return IntegralToULong(typeof(long), Expression.Property(value, nameof(DateTime.Ticks)));
        if (type == typeof(DateTimeOffset))
            return IntegralToULong(typeof(long), Expression.Property(value, nameof(DateTimeOffset.UtcTicks)));
        if (type == typeof(TimeSpan))
            return IntegralToULong(typeof(long), Expression.Property(value, nameof(TimeSpan.Ticks)));
        if (type == typeof(DateOnly))
            return IntegralToULong(typeof(int), Expression.Property(value, nameof(DateOnly.DayNumber)));
        if (type == typeof(TimeOnly))
            return IntegralToULong(typeof(long), Expression.Property(value, nameof(TimeOnly.Ticks)));

        if (Nullable.GetUnderlyingType(type) is { } underlying)
            return Expression.Condition(
                Expression.Property(value, "HasValue"),
                RawHash(underlying, Expression.Property(value, "Value")),
                Expression.Constant(NullSentinel));

        if (ListElementType(type) is { } element)
            return Expression.Call(
                Method(nameof(HashList)).MakeGenericMethod(element),
                Expression.Convert(value, typeof(IReadOnlyList<>).MakeGenericType(element)));

        return CompositeHash(type, value);
    }

    /// <summary>FNV-style fold over the (mixed) hashes of the value's public members.</summary>
    private static Expression CompositeHash(Type type, Expression value)
    {
        var h = Expression.Variable(typeof(ulong), "h");
        var body = new List<Expression> { Expression.Assign(h, Expression.Constant(FnvOffset)) };
        var locals = new List<ParameterExpression> { h };

        foreach (var (name, memberType) in Members(type))
        {
            var member = Expression.Variable(memberType, name);
            locals.Add(member);
            body.Add(Expression.Assign(member, Expression.PropertyOrField(value, name)));

            var memberHash = Expression.Call(Method(nameof(OfCore)).MakeGenericMethod(memberType), member);
            var contribution = memberType.IsValueType
                ? (Expression)memberHash
                : Expression.Condition(
                    Expression.ReferenceEqual(member, Expression.Constant(null, memberType)),
                    Expression.Constant(NullSentinel),
                    memberHash);
            body.Add(Expression.Assign(h,
                Expression.Multiply(Expression.ExclusiveOr(h, contribution), Expression.Constant(FnvPrime))));
        }

        body.Add(h);
        return Expression.Block(typeof(ulong), locals, body);
    }

    /// <summary>Sign-extending for the signed integrals, so the expression matches C#'s
    /// unchecked cast semantics.</summary>
    private static Expression IntegralToULong(Type type, Expression value) =>
        type == typeof(sbyte) || type == typeof(short) || type == typeof(int) || type == typeof(long)
            ? Expression.Convert(Expression.Convert(value, typeof(long)), typeof(ulong))
            : Expression.Convert(value, typeof(ulong));

    private static MethodInfo Method(string name) =>
        typeof(StableHash).GetMethod(name, BindingFlags.NonPublic | BindingFlags.Static)!;

    // ---- leaf hashes ---------------------------------------------------------------------

    private static ulong HashString(string value)
    {
        // FNV-1a over the chars — endianness-free, matching the historical tournament-tree
        // string hash so persisted trees keep their slots.
        var hash = FnvOffset;
        foreach (var c in value)
            hash = (hash ^ c) * FnvPrime;
        return hash;
    }

    private static ulong HashDouble(double value) =>
        value == 0 ? 0 // +0.0 and -0.0 compare equal but differ bitwise
        : double.IsNaN(value) ? 0x7FF8000000000000ul // canonicalize NaN payloads
        : unchecked((ulong)BitConverter.DoubleToInt64Bits(value));

    private static ulong HashDecimal(decimal value)
    {
        // 1.0m and 1.00m are equal but differ in scale; "G29" formatting drops trailing
        // zeros, giving a canonical text that is culture- and process-independent.
        return HashString(value.ToString("G29", CultureInfo.InvariantCulture));
    }

    private static ulong HashGuid(Guid value)
    {
        Span<byte> bytes = stackalloc byte[16];
        value.TryWriteBytes(bytes); // defined byte order on every platform
        var hash = FnvOffset;
        foreach (var b in bytes)
            hash = (hash ^ b) * FnvPrime;
        return hash;
    }

    private static ulong HashList<T>(IReadOnlyList<T> items)
    {
        var hash = (FnvOffset ^ (ulong)items.Count) * FnvPrime;
        for (var i = 0; i < items.Count; i++)
        {
            var item = items[i];
            hash = (hash ^ (item is null ? NullSentinel : OfCore(item))) * FnvPrime;
        }
        return hash;
    }

    /// <summary>splitmix64 finalizer: spreads clustered inputs (sequential ints, timestamps)
    /// over the low bits that routing and slot selection use.</summary>
    private static ulong Mix(ulong x)
    {
        x ^= x >> 30;
        x *= 0xBF58476D1CE4E5B9ul;
        x ^= x >> 27;
        x *= 0x94D049BB133111EBul;
        x ^= x >> 31;
        return x;
    }
}
