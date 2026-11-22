using System.Reflection;
using System.Runtime.CompilerServices;

namespace Dbsp.Diagnostics;

/// <summary>
/// Estimates what an object costs on the managed heap, everything it alone keeps alive
/// included: the object, its strings, its arrays and the objects its fields refer to, each
/// with its header and rounded up as the runtime rounds them (64-bit).
/// <para>
/// It is an estimate made by reading the type's fields, not a measurement: the runtime may
/// pad a class it lays out by a few bytes more than the sum of its fields, and an object
/// reachable from two places is counted once per walk — so a string shared by many rows
/// (an interned code, a cached name) is counted in each row that is sized; only the empty
/// string and a collection's comparer are known to be shared and left out. For records of
/// primitives, strings and small collections, which is what rows and state entries are,
/// it is within a few per cent of what the collector reports (<c>ObjectSizeTests</c>).
/// </para>
/// <para>
/// One instance belongs to one owner — a table, a state store — and is used from one
/// thread at a time. It remembers each type's layout itself and in nothing static, so an
/// owner that is dropped takes the types with it: a plugin's types must stay unloadable.
/// </para>
/// </summary>
internal sealed class ObjectSize
{
    private const int HeaderBytes = 16;     // object header and method table pointer
    private const int MinimumBytes = 24;    // the smallest object the runtime allocates
    private const int ReferenceBytes = 8;
    private const int MaxDepth = 12;

    // How many objects one walk visits before it stops and answers with what it has: a
    // row that leads to something vast (a cache, the world) is not sized to the end.
    private const int MaxObjects = 4096;

    // How many elements of one array of references or of structs are walked. A longer
    // array is sized from these: the same mean per element for the rest.
    private const int MaxElements = 256;

    private sealed record Layout(long InstanceBytes, FieldInfo[] References, int ElementBytes, bool ElementsHoldReferences);

    private readonly Dictionary<Type, Layout> _layouts = [];
    private readonly Dictionary<Type, int> _valueSizes = [];
    private readonly Dictionary<Type, bool> _holds = [];
    private readonly HashSet<object> _seen = new(ReferenceEqualityComparer.Instance);

    /// <summary>The bytes <paramref name="value"/> keeps on the heap; 0 for null and for a
    /// value type that holds no reference.</summary>
    public long Of<T>(T value)
    {
        if (value is null)
            return 0;
        _seen.Clear();
        if (typeof(T).IsValueType)
        {
            // The value itself lies in whatever holds it (a dictionary's entry, a field);
            // only what its fields refer to is on the heap on its account.
            return RuntimeHelpers.IsReferenceOrContainsReferences<T>() ? ReferencedFrom(value, typeof(T), 0) : 0;
        }
        return Walk(value, 0);
    }

    private long Walk(object value, int depth)
    {
        if (depth > MaxDepth || _seen.Count >= MaxObjects || !_seen.Add(value))
            return 0;
        switch (value)
        {
            case string text:
                // The empty string is the runtime's one instance, whoever refers to it.
                return text.Length == 0 ? 0 : Align(22 + 2L * text.Length);
            case Type or Delegate or Pointer or MemberInfo or Module or Assembly:
                // The runtime's own; not this object's to count.
                return 0;
            case Array array:
                return ArrayBytes(array, depth);
        }

        var type = value.GetType();
        var layout = LayoutOf(type);
        var bytes = layout.InstanceBytes;
        foreach (var field in layout.References)
            bytes += FieldBytes(field.GetValue(value), field.FieldType, depth);
        return bytes;
    }

    /// <summary>What a field's value keeps beyond the field itself.</summary>
    private long FieldBytes(object? value, Type declared, int depth)
    {
        if (value is null)
            return 0;
        if (declared.IsValueType)
            return ReferencedFrom(value, declared, depth);
        return Walk(value, depth + 1);
    }

    /// <summary>What the reference fields of a (boxed) value type keep.</summary>
    private long ReferencedFrom(object boxed, Type type, int depth)
    {
        if (depth > MaxDepth)
            return 0;
        long bytes = 0;
        foreach (var field in LayoutOf(type).References)
            bytes += FieldBytes(field.GetValue(boxed), field.FieldType, depth + 1);
        return bytes;
    }

    private long ArrayBytes(Array array, int depth)
    {
        var element = array.GetType().GetElementType()!;
        var layout = LayoutOf(array.GetType());
        var bytes = Align(24 + (long)array.Length * layout.ElementBytes);
        if (!layout.ElementsHoldReferences || array.Length == 0 || array.Rank != 1)
            return bytes;

        var walked = Math.Min(array.Length, MaxElements);
        long held = 0;
        for (var i = 0; i < walked; i++)
            held += FieldBytes(array.GetValue(i), element, depth);
        return bytes + (walked == array.Length ? held : (long)((double)held / walked * array.Length));
    }

    private Layout LayoutOf(Type type)
    {
        if (_layouts.TryGetValue(type, out var layout))
            return layout;

        if (type.IsArray)
        {
            var element = type.GetElementType()!;
            layout = new Layout(0, [], element.IsValueType ? ValueBytes(element) : ReferenceBytes, !element.IsValueType || HoldsReferences(element));
        }
        else
        {
            var references = new List<FieldInfo>();
            long fields = 0;
            for (var current = type; current is not null && current != typeof(object) && current != typeof(ValueType); current = current.BaseType)
            {
                foreach (var field in current.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
                {
                    var fieldType = field.FieldType;
                    if (fieldType.IsPointer || fieldType.IsByRef || fieldType.IsByRefLike)
                    {
                        fields += ReferenceBytes;
                        continue;
                    }
                    fields += fieldType.IsValueType ? ValueBytes(fieldType) : ReferenceBytes;

                    // A comparer is shared by every collection that uses it: the field is
                    // counted, what it refers to is nobody's in particular.
                    if (IsComparer(fieldType))
                        continue;
                    if (!fieldType.IsValueType || HoldsReferences(fieldType))
                        references.Add(field);
                }
            }
            layout = new Layout(Math.Max(MinimumBytes, Align(HeaderBytes + fields)), [.. references], 0, false);
        }
        return _layouts[type] = layout;
    }

    /// <summary>The bytes a value of <paramref name="type"/> takes where it lies: in a
    /// field, in an array element. Structs are the sum of their fields, each rounded to
    /// its own alignment, which is how the runtime lays out the ones it does not reorder.</summary>
    private int ValueBytes(Type type)
    {
        if (type.IsEnum)
            type = Enum.GetUnderlyingType(type);
        if (type.IsPrimitive)
        {
            return Type.GetTypeCode(type) switch
            {
                TypeCode.Boolean or TypeCode.Byte or TypeCode.SByte => 1,
                TypeCode.Char or TypeCode.Int16 or TypeCode.UInt16 => 2,
                TypeCode.Int32 or TypeCode.UInt32 or TypeCode.Single => 4,
                _ => 8,
            };
        }
        if (type == typeof(decimal) || type == typeof(Guid))
            return 16;
        if (_valueSizes.TryGetValue(type, out var known))
            return known;

        int size = 0, alignment = 1;
        foreach (var field in type.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
        {
            var fieldType = field.FieldType;
            var fieldSize = fieldType.IsValueType && !fieldType.IsPointer ? ValueBytes(fieldType) : ReferenceBytes;
            var fieldAlignment = Math.Min(8, fieldType.IsValueType && !fieldType.IsPrimitive && !fieldType.IsEnum ? 8 : fieldSize);
            fieldAlignment = Math.Max(1, fieldAlignment);
            size = (size + fieldAlignment - 1) / fieldAlignment * fieldAlignment + fieldSize;
            alignment = Math.Max(alignment, fieldAlignment);
        }
        size = Math.Max(1, (size + alignment - 1) / alignment * alignment);
        return _valueSizes[type] = size;
    }

    private static bool IsComparer(Type type) =>
        type.IsInterface && type.IsGenericType
        && (type.GetGenericTypeDefinition() == typeof(IEqualityComparer<>) || type.GetGenericTypeDefinition() == typeof(IComparer<>));

    private bool HoldsReferences(Type type)
    {
        if (type.IsPrimitive || type.IsEnum || type.IsPointer)
            return false;
        if (!type.IsValueType)
            return true;
        if (_holds.TryGetValue(type, out var known))
            return known;
        var holds = false;
        foreach (var field in type.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
            holds |= field.FieldType != type && HoldsReferences(field.FieldType);
        return _holds[type] = holds;
    }

    private static long Align(long bytes) => (bytes + 7) & ~7L;

    /// <summary>The bytes one entry of a <see cref="Dictionary{TKey,TValue}"/> takes in the
    /// dictionary's own arrays: its hash code and chain link, the key and the value as
    /// they lie in the entry, and one bucket.</summary>
    public static int DictionaryEntryBytes<TKey, TValue>() => Unsafe.SizeOf<(int Hash, int Next, TKey Key, TValue Value)>() + sizeof(int);
}

/// <summary>
/// A running mean of what one item costs on the heap, kept by sizing some of the items as
/// they pass: each of the first <see cref="Eager"/>, then one in <see cref="Stride"/>, so
/// that a store of a hundred entries and a table of a hundred million are both known from
/// a few hundred to a few hundred thousand measurements and the cost per item written is a
/// counter. Written by one thread at a time; read by any.
/// </summary>
internal sealed class SizeSampler
{
    private const int Eager = 256;
    private const int Stride = 512;

    private readonly ObjectSize _size = new();
    private long _offered;
    private long _sampled;
    private long _bytes;

    public void Offer<T>(T item)
    {
        if (!_gaveUp && (_offered++ < Eager || (_offered - 1) % Stride == 0))
            Add(item, default(object));
    }

    /// <summary>Two parts of one item (a key and its value).</summary>
    public void Offer<TKey, TValue>(TKey key, TValue value)
    {
        if (!_gaveUp && (_offered++ < Eager || (_offered - 1) % Stride == 0))
            Add(key, value);
    }

    // Sizing reads fields by reflection, and this is called where a table publishes a
    // version and where a state store commits: a type it cannot read (a field that
    // throws when read) must cost the estimate, never the write. One failure and this
    // sampler sizes nothing more; what it has stands.
    private bool _gaveUp;

    private void Add<TFirst, TSecond>(TFirst first, TSecond second)
    {
        try
        {
            Add(_size.Of(first) + _size.Of(second));
        }
        catch (Exception)
        {
            _gaveUp = true;
        }
    }

    private void Add(long bytes)
    {
        Volatile.Write(ref _bytes, _bytes + bytes);
        Volatile.Write(ref _sampled, _sampled + 1);
    }

    /// <summary>Items sized so far.</summary>
    public long Sampled => Volatile.Read(ref _sampled);

    /// <summary>Mean bytes of the items sized; 0 before the first.</summary>
    public double Mean
    {
        get
        {
            var sampled = Volatile.Read(ref _sampled);
            return sampled == 0 ? 0 : (double)Volatile.Read(ref _bytes) / sampled;
        }
    }
}
