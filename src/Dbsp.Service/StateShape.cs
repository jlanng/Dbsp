using System.Reflection;

namespace Dbsp.Service;

/// <summary>One state store of a pipeline's circuit: what kind it is and the types it
/// holds, as text. <c>KeyReach</c> and <c>ValueReach</c> name every described type the
/// key and the value are made of.</summary>
internal sealed record StoreShape(
    CircuitStateStoreKind Kind, string? Key, string Value, IReadOnlyList<string> KeyReach, IReadOnlyList<string> ValueReach);

/// <summary>
/// What a pipeline's state is made of, as text and nothing else — no <see cref="Type"/> is
/// kept, so a shape outlives the plugin version it was read from. <c>Stores</c> is every
/// state store of the circuit (worker 0's: the others hold the same types) by its name
/// without the worker; <c>Tables</c> is the key and row type of every table the
/// definition reads or writes; <c>Types</c> is every type those are made of, with its
/// members as the state serializer sees them (public properties and fields; an enum's
/// members with their numbers).
/// <para>
/// This is what a keep-or-reset choice is informed by: two shapes that are equal hold the
/// same types with the same members in the same stores, and where they differ
/// <see cref="StateShapes.Differences"/> says which store, which type and which member.
/// A type that is not in any store — a sink's row, a settings class, a type used only
/// inside a delegate — is not part of it, because no state depends on it.
/// </para>
/// </summary>
internal sealed record StateShape(
    IReadOnlyDictionary<string, StoreShape> Stores,
    IReadOnlyDictionary<string, (string Key, string Row)> Tables,
    IReadOnlyDictionary<string, IReadOnlyList<string>> Types);

internal static class StateShapes
{
    public static StateShape Describe(IReadOnlyList<CircuitStateStore> stores, IReadOnlyDictionary<string, (Type Key, Type Row)> tables)
    {
        var types = new SortedDictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
        var shapes = new SortedDictionary<string, StoreShape>(StringComparer.Ordinal);
        foreach (var store in stores)
        {
            // One worker's stores stand for all: `{node}.{purpose}.w{worker}`.
            if (!store.Name.EndsWith(".w0", StringComparison.Ordinal))
                continue;
            var keyReach = new SortedSet<string>(StringComparer.Ordinal);
            var valueReach = new SortedSet<string>(StringComparer.Ordinal);
            if (store.KeyType is not null)
                Walk(store.KeyType, types, keyReach);
            Walk(store.ValueType, types, valueReach);
            shapes[store.Name[..^3]] = new StoreShape(
                store.Kind, store.KeyType is null ? null : Name(store.KeyType), Name(store.ValueType), [.. keyReach], [.. valueReach]);
        }
        var tableShapes = new SortedDictionary<string, (string, string)>(StringComparer.Ordinal);
        foreach (var (table, (key, row)) in tables)
        {
            var reach = new SortedSet<string>(StringComparer.Ordinal);
            Walk(key, types, reach);
            Walk(row, types, reach);
            tableShapes[table] = (Name(key), Name(row));
        }
        return new StateShape(shapes, tableShapes, types);
    }

    /// <summary>A type as text: namespace-qualified unless it is the runtime's own, with
    /// its generic arguments spelled out. Two loads of one assembly give the same text.</summary>
    internal static string Name(Type type)
    {
        if (type.IsArray)
            return Name(type.GetElementType()!) + "[]";
        if (Nullable.GetUnderlyingType(type) is { } underlying)
            return Name(underlying) + "?";
        var name = type.Namespace is null or "System" || type.Namespace.StartsWith("System.", StringComparison.Ordinal)
            ? type.Name
            : (type.DeclaringType is { } outer ? Name(outer) + "." : type.Namespace + ".") + type.Name;
        if (!type.IsGenericType)
            return name;
        var tick = name.IndexOf('`');
        if (tick >= 0)
            name = name[..tick];
        return $"{name}<{string.Join(", ", type.GetGenericArguments().Select(Name))}>";
    }

    /// <summary>A record type spelled out for a reader: one line per described type it
    /// is made of, the type itself first — <c>Name { member: Type; … }</c>. Empty for a
    /// type that is the runtime's own all the way down.</summary>
    internal static IReadOnlyList<string> Members(Type type)
    {
        var types = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
        Walk(type, types, new HashSet<string>(StringComparer.Ordinal));
        var first = Name(type);
        return [.. types.OrderBy(t => t.Key == first ? 0 : 1).ThenBy(t => t.Key, StringComparer.Ordinal)
            .Select(t => $"{t.Key} {{ {string.Join("; ", t.Value)} }}")];
    }

    private static bool IsRuntimeType(Type type) =>
        type.Namespace is "System" || (type.Namespace?.StartsWith("System.", StringComparison.Ordinal) ?? false);

    private static void Walk(Type type, IDictionary<string, IReadOnlyList<string>> types, ISet<string> reach)
    {
        if (type.IsArray || type.IsByRef || type.IsPointer)
        {
            if (type.GetElementType() is { } element)
                Walk(element, types, reach);
            return;
        }
        if (type.IsGenericParameter)
            return;
        if (type.IsEnum)
        {
            var name = Name(type);
            if (reach.Add(name) && !types.ContainsKey(name))
                types[name] = [.. type.GetFields(BindingFlags.Public | BindingFlags.Static)
                    .Select(f => $"{f.Name} = {Convert.ToString(f.GetRawConstantValue(), System.Globalization.CultureInfo.InvariantCulture)}")
                    .Order(StringComparer.Ordinal)];
            return;
        }
        if (IsRuntimeType(type))
        {
            // int, string, DateTimeOffset: a leaf. A tuple, a nullable, a list: what it is
            // made of is its arguments.
            if (type.IsGenericType)
                foreach (var argument in type.GetGenericArguments())
                    Walk(argument, types, reach);
            return;
        }

        var described = Name(type);
        if (!reach.Add(described))
            return;
        var members = new List<(string Name, Type Type)>();
        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            if (property.GetMethod is not null && property.GetIndexParameters().Length == 0)
                members.Add((property.Name, property.PropertyType));
        foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.Instance))
            members.Add((field.Name, field.FieldType));
        if (!types.ContainsKey(described))
            types[described] = [.. members.Select(m => $"{m.Name}: {Name(m.Type)}").Order(StringComparer.Ordinal)];
        foreach (var (_, memberType) in members)
            Walk(memberType, types, reach);
    }

    /// <summary>
    /// What differs between the state of two specs of one pipeline, for a person to judge
    /// by: one line per store that is new, gone or of another type, and one per type whose
    /// members differ, naming the stores that hold it and whether it is part of what
    /// identifies an entry. Empty when the shapes are the same.
    /// </summary>
    public static List<string> Differences(StateShape old, StateShape next)
    {
        var lines = new List<string>();
        foreach (var name in old.Stores.Keys.Union(next.Stores.Keys).Order(StringComparer.Ordinal))
        {
            var was = old.Stores.GetValueOrDefault(name);
            var now = next.Stores.GetValueOrDefault(name);
            if (was is null)
                lines.Add($"store '{name}' is new ({Holds(now!)}): it starts empty");
            else if (now is null)
                lines.Add($"store '{name}' ({Holds(was)}) has no place in the new circuit: its entries cannot be kept");
            else if (was.Kind != now.Kind || was.Key != now.Key || was.Value != now.Value)
                lines.Add($"store '{name}' held {Holds(was)} and would hold {Holds(now)}: its entries are read as the new types, or not at all");
        }
        foreach (var name in old.Tables.Keys.Intersect(next.Tables.Keys).Order(StringComparer.Ordinal))
            if (old.Tables[name] != next.Tables[name])
                lines.Add($"table '{name}' was read as {old.Tables[name].Key} → {old.Tables[name].Row} and would be read as {next.Tables[name].Key} → {next.Tables[name].Row}");

        foreach (var type in old.Types.Keys.Intersect(next.Types.Keys).Order(StringComparer.Ordinal))
        {
            var was = old.Types[type];
            var now = next.Types[type];
            if (was.SequenceEqual(now))
                continue;
            static string MemberName(string member) => member[..member.IndexOfAny([':', '='])].TrimEnd();
            var before = was.ToDictionary(MemberName, StringComparer.Ordinal);
            var after = now.ToDictionary(MemberName, StringComparer.Ordinal);
            var changes = new List<string>();
            foreach (var member in after.Keys.Except(before.Keys).Order(StringComparer.Ordinal))
                changes.Add($"adds {after[member]}");
            foreach (var member in before.Keys.Except(after.Keys).Order(StringComparer.Ordinal))
                changes.Add($"drops {before[member]}");
            foreach (var member in before.Keys.Intersect(after.Keys).Order(StringComparer.Ordinal))
                if (before[member] != after[member])
                    changes.Add($"changes {before[member]} to {after[member][(member.Length)..].TrimStart(':', '=', ' ')}");

            // Where the type sits decides what the change does to entries already stored.
            var identity = new List<string>();
            var value = new List<string>();
            foreach (var (store, shape) in next.Stores)
            {
                if (!old.Stores.ContainsKey(store))
                    continue;
                var inKey = shape.KeyReach.Contains(type);
                var inValue = shape.ValueReach.Contains(type);
                if (inKey || (inValue && shape.Kind != CircuitStateStoreKind.Value))
                    identity.Add(store);
                else if (inValue)
                    value.Add(store);
            }
            var tables = next.Tables.Where(t => old.Tables.ContainsKey(t.Key)).Select(t => t.Key).ToList();
            var where = identity.Count > 0
                ? $"it identifies entries of {Stores(identity)}: an entry written before the change is not found again under its new form, so keeping this state is not safe"
                : value.Count > 0
                    ? $"it is a value in {Stores(value)}: a member the stored entries lack reads as its default, and a member that was dropped is lost when the entry is next written"
                    : tables.Count > 0 ? "it is a table's key or row type, which the tables hold, not this state" : "no kept store holds it";
            lines.Add($"type {type} {string.Join(", ", changes)} — {where}");
        }
        return lines;

        static string Holds(StoreShape shape) => shape.Key is null ? shape.Value : $"{shape.Key} → {shape.Value}";
        static string Stores(List<string> names) => string.Join(", ", names.Order(StringComparer.Ordinal).Select(n => $"'{n}'"));
    }

    /// <summary>The line that says the shapes are the same, and of what.</summary>
    public static string Unchanged(StateShape shape)
    {
        var types = shape.Types.Keys.ToList();
        var named = types.Count == 0 ? "no types of their own"
            : types.Count <= 6 ? string.Join(", ", types)
            : string.Join(", ", types.Take(6)) + $" and {types.Count - 6} more";
        // Types covers the tables' rows too: a definition with no store of its own still
        // has types to name, and they are the tables', not "state stores'".
        var tables = shape.Tables.Count == 0 ? null : $"the {shape.Tables.Count} table(s) it reads or writes";
        var what = shape.Stores.Count == 0
            ? tables is null
                ? "the definition keeps no state stores and uses no tables, before or after"
                : $"the definition keeps no state stores, before or after; {tables} hold the same types with the same members ({named})"
            : $"the {shape.Stores.Count} state store(s){(tables is null ? "" : " and " + tables)} are the same and hold the same types with the same members ({named})";
        return $"state shapes: unchanged — {what}; what the definition computes from them is not compared";
    }
}
