using System.Linq.Expressions;
using System.Reflection;

namespace Incremental.Cluster;

/// <summary>
/// A compiled dense binary codec for the values an exchange ships between processes.
///
/// The cross-process shuffle used to encode shards as JSON text and then UTF-8 it, which cost
/// two large intermediates per frame (the string and the bytes) on top of the format's own
/// verbosity — at a few hundred kilobytes a frame that is large-object-heap traffic on every
/// step, and the collector, not the shuffle, ends up being the expensive part of clustering.
/// This writes the values straight into a reused byte buffer: field values in a fixed order,
/// no names, LEB128 integers.
///
/// The codec is built once per type from the same structural rules
/// <see cref="StableHash"/> uses — public readable properties and fields, ordered by name, so
/// both ends agree without exchanging a schema — and compiled to a delegate. A type it cannot
/// round-trip (no constructor that can rebuild it, an unsupported member) simply has no codec,
/// and the exchange falls back to JSON for it. Both ends run the same build and therefore make
/// the same decision.
/// </summary>
internal static class WireCodec<T>
{
    /// <summary>Null when <typeparamref name="T"/> has no dense encoding (JSON fallback).</summary>
    internal static readonly Action<WireWriter, T>? Write;
    internal static readonly Func<WireReader, T>? Read;

    static WireCodec()
    {
        try
        {
            WireCodec.Validate(typeof(T), typeof(T), path: null, seen: []);
            Write = WireCodec.BuildWriter<T>();
            Read = WireCodec.BuildReader<T>();
        }
        catch (NotSupportedException)
        {
            (Write, Read) = (null, null);
        }
    }
}

internal static class WireCodec
{
    /// <summary>Bumped whenever the encoding of any supported type changes. Frames carry it,
    /// so a mismatched peer fails loudly instead of decoding garbage.</summary>
    internal const byte FormatVersion = 1;

    // ---- entry points used by compiled composites ---------------------------------------

    internal static void WriteValue<TValue>(WireWriter writer, TValue value) =>
        WireCodec<TValue>.Write!(writer, value);

    internal static TValue ReadValue<TValue>(WireReader reader) => WireCodec<TValue>.Read!(reader);

    // ---- classification ------------------------------------------------------------------

    private static bool IsLeaf(Type t) =>
        t.IsEnum || t == typeof(bool) || t == typeof(char) ||
        t == typeof(byte) || t == typeof(sbyte) || t == typeof(short) || t == typeof(ushort) ||
        t == typeof(int) || t == typeof(uint) || t == typeof(long) || t == typeof(ulong) ||
        t == typeof(float) || t == typeof(double) || t == typeof(decimal) ||
        t == typeof(string) || t == typeof(Guid) ||
        t == typeof(DateTime) || t == typeof(DateTimeOffset) || t == typeof(TimeSpan) ||
        t == typeof(DateOnly) || t == typeof(TimeOnly);

    /// <summary>The element type of a sequence this codec can rebuild: arrays, and the
    /// interfaces an array satisfies. A member typed as a concrete collection is not
    /// supported — the codec would have to guess how to construct it.</summary>
    private static Type? SequenceElement(Type t)
    {
        if (t.IsArray)
            return t.GetArrayRank() == 1 ? t.GetElementType() : null;
        if (!t.IsGenericType)
            return null;
        var definition = t.GetGenericTypeDefinition();
        return definition == typeof(IReadOnlyList<>) || definition == typeof(IList<>) ||
               definition == typeof(IReadOnlyCollection<>) || definition == typeof(ICollection<>) ||
               definition == typeof(IEnumerable<>)
            ? t.GetGenericArguments()[0]
            : null;
    }

    /// <summary>The encoded members of a composite, in the order both ends will use: public
    /// readable properties and public fields, sorted by ordinal name — the same rule
    /// <see cref="StableHash"/> applies, so the two stay in step.</summary>
    private static List<MemberInfo> Members(Type t)
    {
        var members = new List<MemberInfo>();
        foreach (var property in t.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            if (property.CanRead && property.GetIndexParameters().Length == 0)
                members.Add(property);
        foreach (var field in t.GetFields(BindingFlags.Public | BindingFlags.Instance))
            members.Add(field);
        members.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));
        return members;
    }

    private static Type MemberType(MemberInfo member) =>
        member is PropertyInfo property ? property.PropertyType : ((FieldInfo)member).FieldType;

    private static bool IsSettable(MemberInfo member) => member switch
    {
        PropertyInfo property => property.CanWrite && property.SetMethod?.IsPublic == true,
        FieldInfo field => !field.IsInitOnly,
        _ => false,
    };

    /// <summary>The constructor used to rebuild a composite: the one taking the most
    /// parameters whose names all name encoded members. Records and tuples land on their
    /// primary constructor; a plain class lands on its parameterless one and is rebuilt
    /// member by member.</summary>
    private static ConstructorInfo? Constructor(Type t, List<MemberInfo> members)
    {
        ConstructorInfo? best = null;
        foreach (var candidate in t.GetConstructors(BindingFlags.Public | BindingFlags.Instance))
        {
            var parameters = candidate.GetParameters();
            if (parameters.Any(p => p.Name is null ||
                    !members.Any(m => string.Equals(m.Name, p.Name, StringComparison.OrdinalIgnoreCase) &&
                                      MemberType(m) == p.ParameterType)))
                continue;
            if (best is null || parameters.Length > best.GetParameters().Length)
                best = candidate;
        }
        // A value type always has an implicit parameterless constructor; GetConstructors does
        // not report it, so null here still means "member-init from default" for a struct.
        return best;
    }

    private static MemberInfo? ParameterMember(List<MemberInfo> members, ParameterInfo parameter) =>
        members.Find(m => string.Equals(m.Name, parameter.Name, StringComparison.OrdinalIgnoreCase) &&
                          MemberType(m) == parameter.ParameterType);

    // ---- validation ----------------------------------------------------------------------

    internal static void Validate(Type type, Type root, string? path, HashSet<Type> seen)
    {
        if (!seen.Add(type))
            return;

        if (IsLeaf(type))
            return;

        if (Nullable.GetUnderlyingType(type) is { } underlying)
        {
            Validate(underlying, root, path, seen);
            return;
        }

        if (SequenceElement(type) is { } element)
        {
            Validate(element, root, path is null ? "[]" : path + "[]", seen);
            return;
        }

        if (type.IsArray || type == typeof(object) || type.IsInterface || type.IsAbstract ||
            type.IsPointer || typeof(Delegate).IsAssignableFrom(type) ||
            typeof(System.Collections.IEnumerable).IsAssignableFrom(type))
            throw Unsupported(type, root, path);

        var members = Members(type);
        if (members.Count == 0)
            throw Unsupported(type, root, path);

        var constructor = Constructor(type, members);
        var byConstructor = constructor is null
            ? new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            : [.. constructor.GetParameters().Select(p => p.Name!)];
        if (constructor is null && !type.IsValueType)
            throw Unsupported(type, root, path);

        foreach (var member in members)
        {
            // Every member must be rebuildable, or the value would not survive the round trip.
            if (!byConstructor.Contains(member.Name) && !IsSettable(member))
                throw Unsupported(type, root, path);
            Validate(MemberType(member), root, path is null ? member.Name : $"{path}.{member.Name}", seen);
        }
    }

    private static NotSupportedException Unsupported(Type type, Type root, string? path) =>
        new(path is null
            ? $"'{type}' has no dense wire encoding."
            : $"'{root}' has no dense wire encoding: member '{path}' of type '{type}'.");

    // ---- writer ---------------------------------------------------------------------------

    internal static Action<WireWriter, TValue> BuildWriter<TValue>()
    {
        var writer = Expression.Parameter(typeof(WireWriter), "writer");
        var value = Expression.Parameter(typeof(TValue), "value");
        return Expression.Lambda<Action<WireWriter, TValue>>(
            WriteExpression(typeof(TValue), writer, value), writer, value).Compile();
    }

    private static Expression WriteExpression(Type type, Expression writer, Expression value)
    {
        Expression Call(string method, Expression argument) =>
            Expression.Call(writer, typeof(WireWriter).GetMethod(method)!, argument);

        if (type.IsEnum)
            return WriteExpression(type.GetEnumUnderlyingType(),
                writer, Expression.Convert(value, type.GetEnumUnderlyingType()));
        if (type == typeof(bool)) return Call(nameof(WireWriter.WriteBool), value);
        if (type == typeof(byte) || type == typeof(ushort) || type == typeof(char) || type == typeof(uint))
            return Call(nameof(WireWriter.WriteUInt32), Expression.Convert(value, typeof(uint)));
        if (type == typeof(ulong)) return Call(nameof(WireWriter.WriteUInt64), value);
        if (type == typeof(sbyte) || type == typeof(short) || type == typeof(int))
            return Call(nameof(WireWriter.WriteInt32), Expression.Convert(value, typeof(int)));
        if (type == typeof(long)) return Call(nameof(WireWriter.WriteInt64), value);
        if (type == typeof(float)) return Call(nameof(WireWriter.WriteSingle), value);
        if (type == typeof(double)) return Call(nameof(WireWriter.WriteDouble), value);
        if (type == typeof(decimal)) return Call(nameof(WireWriter.WriteDecimal), value);
        if (type == typeof(Guid)) return Call(nameof(WireWriter.WriteGuid), value);
        if (type == typeof(string)) return Call(nameof(WireWriter.WriteString), value);
        if (type == typeof(TimeSpan))
            return Call(nameof(WireWriter.WriteInt64), Expression.Property(value, nameof(TimeSpan.Ticks)));
        if (type == typeof(TimeOnly))
            return Call(nameof(WireWriter.WriteInt64), Expression.Property(value, nameof(TimeOnly.Ticks)));
        if (type == typeof(DateOnly))
            return Call(nameof(WireWriter.WriteInt32), Expression.Property(value, nameof(DateOnly.DayNumber)));
        if (type == typeof(DateTime))
            return Expression.Block(
                Call(nameof(WireWriter.WriteInt64), Expression.Property(value, nameof(DateTime.Ticks))),
                Call(nameof(WireWriter.WriteUInt32),
                    Expression.Convert(Expression.Convert(Expression.Property(value, nameof(DateTime.Kind)), typeof(int)), typeof(uint))));
        if (type == typeof(DateTimeOffset))
            return Expression.Block(
                Call(nameof(WireWriter.WriteInt64), Expression.Property(value, nameof(DateTimeOffset.UtcTicks))),
                Call(nameof(WireWriter.WriteInt32),
                    Expression.Convert(
                        Expression.Property(Expression.Property(value, nameof(DateTimeOffset.Offset)), nameof(TimeSpan.TotalMinutes)),
                        typeof(int))));

        if (Nullable.GetUnderlyingType(type) is { } underlying)
            return Expression.Block(
                Call(nameof(WireWriter.WriteBool), Expression.Property(value, "HasValue")),
                Expression.IfThen(
                    Expression.Property(value, "HasValue"),
                    WriteMember(underlying, writer, Expression.Property(value, "Value"))));

        if (SequenceElement(type) is { } element)
            return WriteSequence(type, element, writer, value);

        return WriteComposite(type, writer, value);
    }

    /// <summary>A member (or element) write: the value itself, preceded by a presence byte
    /// when its type can be null.</summary>
    private static Expression WriteMember(Type type, Expression writer, Expression value)
    {
        var write = Expression.Call(
            typeof(WireCodec).GetMethod(nameof(WriteValue), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(type),
            writer, value);
        if (type.IsValueType)
            return write;
        return Expression.Block(
            Expression.Call(writer, typeof(WireWriter).GetMethod(nameof(WireWriter.WriteBool))!,
                Expression.ReferenceNotEqual(value, Expression.Constant(null, type))),
            Expression.IfThen(Expression.ReferenceNotEqual(value, Expression.Constant(null, type)), write));
    }

    private static Expression WriteSequence(Type type, Type element, Expression writer, Expression value)
    {
        var list = Expression.Variable(typeof(IReadOnlyList<>).MakeGenericType(element), "list");
        var index = Expression.Variable(typeof(int), "i");
        // Count is declared on IReadOnlyCollection<T>, not on IReadOnlyList<T>.
        var count = Expression.Property(
            list, typeof(IReadOnlyCollection<>).MakeGenericType(element).GetProperty(nameof(IReadOnlyCollection<int>.Count))!);
        var breakTo = Expression.Label("done");
        return Expression.Block(
            [list, index],
            Expression.Assign(list, Expression.Convert(value, list.Type)),
            Expression.Call(writer, typeof(WireWriter).GetMethod(nameof(WireWriter.WriteUInt32))!,
                Expression.Convert(count, typeof(uint))),
            Expression.Assign(index, Expression.Constant(0)),
            Expression.Loop(
                Expression.IfThenElse(
                    Expression.LessThan(index, count),
                    Expression.Block(
                        WriteMember(element, writer,
                            Expression.MakeIndex(list, list.Type.GetProperty("Item"), [index])),
                        Expression.PostIncrementAssign(index)),
                    Expression.Break(breakTo)),
                breakTo));
    }

    private static Expression WriteComposite(Type type, Expression writer, Expression value)
    {
        var body = new List<Expression>();
        foreach (var member in Members(type))
            body.Add(WriteMember(MemberType(member), writer, Expression.MakeMemberAccess(value, member)));
        return body.Count == 1 ? body[0] : Expression.Block(body);
    }

    // ---- reader ---------------------------------------------------------------------------

    internal static Func<WireReader, TValue> BuildReader<TValue>()
    {
        var reader = Expression.Parameter(typeof(WireReader), "reader");
        return Expression.Lambda<Func<WireReader, TValue>>(
            ReadExpression(typeof(TValue), reader), reader).Compile();
    }

    private static Expression ReadExpression(Type type, Expression reader)
    {
        Expression Call(string method) => Expression.Call(reader, typeof(WireReader).GetMethod(method)!);

        if (type.IsEnum)
            return Expression.Convert(ReadExpression(type.GetEnumUnderlyingType(), reader), type);
        if (type == typeof(bool)) return Call(nameof(WireReader.ReadBool));
        if (type == typeof(byte) || type == typeof(ushort) || type == typeof(char) || type == typeof(uint))
            return Expression.Convert(Call(nameof(WireReader.ReadUInt32)), type);
        if (type == typeof(ulong)) return Call(nameof(WireReader.ReadUInt64));
        if (type == typeof(sbyte) || type == typeof(short) || type == typeof(int))
            return Expression.Convert(Call(nameof(WireReader.ReadInt32)), type);
        if (type == typeof(long)) return Call(nameof(WireReader.ReadInt64));
        if (type == typeof(float)) return Call(nameof(WireReader.ReadSingle));
        if (type == typeof(double)) return Call(nameof(WireReader.ReadDouble));
        if (type == typeof(decimal)) return Call(nameof(WireReader.ReadDecimal));
        if (type == typeof(Guid)) return Call(nameof(WireReader.ReadGuid));
        if (type == typeof(string)) return Call(nameof(WireReader.ReadString));
        if (type == typeof(TimeSpan))
            return Expression.New(typeof(TimeSpan).GetConstructor([typeof(long)])!, Call(nameof(WireReader.ReadInt64)));
        if (type == typeof(TimeOnly))
            return Expression.New(typeof(TimeOnly).GetConstructor([typeof(long)])!, Call(nameof(WireReader.ReadInt64)));
        if (type == typeof(DateOnly))
            return Expression.Call(typeof(DateOnly).GetMethod(nameof(DateOnly.FromDayNumber))!, Call(nameof(WireReader.ReadInt32)));
        if (type == typeof(DateTime))
            return Expression.New(
                typeof(DateTime).GetConstructor([typeof(long), typeof(DateTimeKind)])!,
                Call(nameof(WireReader.ReadInt64)),
                Expression.Convert(Expression.Convert(Call(nameof(WireReader.ReadUInt32)), typeof(int)), typeof(DateTimeKind)));
        if (type == typeof(DateTimeOffset))
        {
            // UtcTicks then the offset: the value is rebuilt as an instant plus its offset, so
            // it round-trips exactly rather than through a local-time reinterpretation.
            var ticks = Expression.Variable(typeof(long), "ticks");
            var minutes = Expression.Variable(typeof(int), "minutes");
            return Expression.Block(
                [ticks, minutes],
                Expression.Assign(ticks, Call(nameof(WireReader.ReadInt64))),
                Expression.Assign(minutes, Call(nameof(WireReader.ReadInt32))),
                Expression.Call(
                    Expression.New(
                        typeof(DateTimeOffset).GetConstructor([typeof(long), typeof(TimeSpan)])!,
                        ticks, Expression.Constant(TimeSpan.Zero)),
                    typeof(DateTimeOffset).GetMethod(nameof(DateTimeOffset.ToOffset))!,
                    Expression.Call(typeof(TimeSpan).GetMethod(nameof(TimeSpan.FromMinutes), [typeof(double)])!,
                        Expression.Convert(minutes, typeof(double)))));
        }

        if (Nullable.GetUnderlyingType(type) is { } underlying)
            return Expression.Condition(
                Expression.Call(reader, typeof(WireReader).GetMethod(nameof(WireReader.ReadBool))!),
                Expression.Convert(ReadMember(underlying, reader), type),
                Expression.Default(type));

        if (SequenceElement(type) is { } element)
            return ReadSequence(type, element, reader);

        return ReadComposite(type, reader);
    }

    private static Expression ReadMember(Type type, Expression reader)
    {
        var read = Expression.Call(
            typeof(WireCodec).GetMethod(nameof(ReadValue), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(type),
            reader);
        if (type.IsValueType)
            return read;
        return Expression.Condition(
            Expression.Call(reader, typeof(WireReader).GetMethod(nameof(WireReader.ReadBool))!),
            read,
            Expression.Constant(null, type));
    }

    private static Expression ReadSequence(Type type, Type element, Expression reader)
    {
        var array = Expression.Variable(element.MakeArrayType(), "items");
        var count = Expression.Variable(typeof(int), "count");
        var index = Expression.Variable(typeof(int), "i");
        var breakTo = Expression.Label("done");
        return Expression.Block(
            type,
            [array, count, index],
            Expression.Assign(count,
                Expression.Convert(Expression.Call(reader, typeof(WireReader).GetMethod(nameof(WireReader.ReadUInt32))!), typeof(int))),
            Expression.Assign(array, Expression.NewArrayBounds(element, count)),
            Expression.Assign(index, Expression.Constant(0)),
            Expression.Loop(
                Expression.IfThenElse(
                    Expression.LessThan(index, count),
                    Expression.Block(
                        Expression.Assign(Expression.ArrayAccess(array, index), ReadMember(element, reader)),
                        Expression.PostIncrementAssign(index)),
                    Expression.Break(breakTo)),
                breakTo),
            Expression.Convert(array, type));
    }

    private static Expression ReadComposite(Type type, Expression reader)
    {
        var members = Members(type);
        var constructor = Constructor(type, members);

        // Members are read in the writer's order into locals first, then handed to the
        // constructor in its own parameter order — the two orders are unrelated.
        var locals = new List<ParameterExpression>();
        var body = new List<Expression>();
        var byName = new Dictionary<string, ParameterExpression>(StringComparer.OrdinalIgnoreCase);
        foreach (var member in members)
        {
            var local = Expression.Variable(MemberType(member), member.Name);
            locals.Add(local);
            byName[member.Name] = local;
            body.Add(Expression.Assign(local, ReadMember(MemberType(member), reader)));
        }

        var byConstructor = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        Expression created;
        if (constructor is null)
        {
            created = Expression.New(type);
        }
        else
        {
            var arguments = new List<Expression>();
            foreach (var parameter in constructor.GetParameters())
            {
                byConstructor.Add(parameter.Name!);
                arguments.Add(byName[ParameterMember(members, parameter)!.Name]);
            }
            created = Expression.New(constructor, arguments);
        }

        var bindings = members
            .Where(m => !byConstructor.Contains(m.Name))
            .Select(m => (MemberBinding)Expression.Bind(m, byName[m.Name]))
            .ToList();
        body.Add(bindings.Count == 0 ? created : Expression.MemberInit((NewExpression)created, bindings));

        return Expression.Block(type, locals, body);
    }
}
