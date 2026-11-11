using System.Collections.Concurrent;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.IO.Compression;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;

namespace Incremental.Service;

/// <summary>
/// Marks a contract assembly: it holds <see cref="TableDefinition{TKey,TRow}"/>s and the
/// row and key types of shared tables. It is loaded once per process and never unloaded,
/// and every pipeline plugin that references it gets that one copy — even if it ships its
/// own — so two plugins agree on a row type.
/// </summary>
[AttributeUsage(AttributeTargets.Assembly)]
public sealed class IncrementalContractAttribute : Attribute;

/// <summary>Marks a settings property that changes no stored row — a label, a batch size —
/// so that changing it keeps the pipeline's state without being asked.</summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class StateNeutralAttribute : Attribute;

public enum PluginKind { Pipeline, Contract }

public sealed record PluginFile(string Path, string Sha256, long Size);

/// <summary>One setting of a definition. <c>Default</c> is what the settings class holds
/// when a spec leaves the setting out (null when that is null, or not a plain value);
/// <c>Values</c> are an enum's member names; <c>Minimum</c> and <c>Maximum</c> come from a
/// <c>[Range]</c>. The last five are absent in a manifest stored before they existed.</summary>
public sealed record SettingInfo(
    string Name, string JsonType, bool Required, bool StateNeutral, string? Description,
    JsonElement? Default = null, IReadOnlyList<string>? Values = null, double? Minimum = null, double? Maximum = null,
    bool Nullable = false);

public sealed record PluginDefinitionInfo(
    string Name, string Type, string? Description, string? SettingsType, IReadOnlyList<SettingInfo> Settings);

/// <summary>A stored plugin version, as the catalog holds it under
/// <c>plugin/{name}/{version}</c>. The version is a hash of the files, so the same bytes
/// are the same version on every node.</summary>
/// <remarks><c>MainAssembly</c> is the main assembly's file, as a path within the package;
/// <c>AssemblyName</c> is that assembly's simple name, which need not be the file's
/// (null in a manifest stored before the two were told apart: the file's name then).
/// <c>Sequence</c> orders the versions of one name by when they were imported — it is the
/// audit sequence of the import, which only grows, whoever leads — and is what decides
/// which stored version of a contract a starting process loads: the newest.
/// <c>ImportedAt</c> and <c>ImportedBy</c> are when the version was stored and the caller
/// the import was audited under; <c>InformationalVersion</c> is the main assembly's
/// <c>AssemblyInformationalVersion</c> — what its author called the build — when it has
/// one. All three are null in a manifest stored before they existed.</remarks>
public sealed record PluginManifest(
    string Name, string Version, PluginKind Kind, string MainAssembly, IReadOnlyList<PluginFile> Files,
    IReadOnlyList<PluginDefinitionInfo> Definitions, IReadOnlyList<string> TableDefinitions, string Fingerprint,
    string? AssemblyName = null, long Sequence = 0, DateTimeOffset? ImportedAt = null, string? ImportedBy = null,
    string? InformationalVersion = null)
{
    public long Bytes => Files.Sum(f => f.Size);

    /// <summary>The simple name of the main assembly.</summary>
    [JsonIgnore]
    public string MainAssemblyName => AssemblyName ?? Path.GetFileNameWithoutExtension(MainAssembly);
}

public sealed record PluginInfo(
    string Name, string Version, PluginKind Kind, bool BuiltIn, IReadOnlyList<PluginDefinitionInfo> Definitions,
    IReadOnlyList<string> TableDefinitions, string LoadState, int Users);

public interface IPluginCatalog
{
    IReadOnlyList<PluginInfo> Plugins { get; }

    /// <summary>Load contexts alive in this process, counting those asked to unload and
    /// not yet collected.</summary>
    int LiveContexts { get; }

    /// <summary>Leader: imports a plugin from a directory (the main assembly, its
    /// <c>.deps.json</c>, its private dependencies). Importing the same bytes again
    /// returns the manifest already stored.</summary>
    Task<PluginManifest> ImportDirectoryAsync(string directory, CancellationToken cancellationToken, string? caller = null);

    /// <summary>Leader: as <see cref="ImportDirectoryAsync"/>, from a zip, which is
    /// validated while it is unpacked. A stream that cannot seek is first copied to a
    /// file in the cache directory, and refused past <see cref="ServiceOptions.MaxPluginBytes"/>.</summary>
    Task<PluginManifest> ImportZipAsync(Stream zip, CancellationToken cancellationToken, string? caller = null);

    /// <summary>Leader: imports every package in <see cref="ServiceOptions.PluginDirectory"/>
    /// the catalog lacks, as the first lead does. A package that cannot be imported is
    /// logged and skipped; the result is what is stored afterwards.</summary>
    Task<IReadOnlyList<PluginInfo>> RescanAsync(CancellationToken cancellationToken);

    /// <summary>The stored manifest of a plugin version: its files and definitions.</summary>
    PluginManifest? Manifest(string name, string version);

    /// <summary>Leader: refused while a pipeline spec names the version, and for a
    /// contract while a declared table's definition comes from it and from no other stored
    /// version.</summary>
    Task DeleteAsync(string name, string version, CancellationToken cancellationToken, string? caller = null);
}

internal static class ServiceNames
{
    private static readonly Regex Name = new("^[a-z0-9][a-z0-9._-]{0,62}$", RegexOptions.CultureInvariant);

    public static bool IsValid(string? name) => name is not null && Name.IsMatch(name);

    public static void Validate(string? name, string what)
    {
        if (!IsValid(name))
            throw ServiceException.Invalid($"'{name}' is not a valid {what}: it must match [a-z0-9][a-z0-9._-]{{0,62}}.");
    }
}

/// <summary>
/// What the default load context provides: the host's own assemblies. Two rules decide
/// whether a plugin's reference goes there.
/// <para>
/// The assemblies whose types cross between the host and a plugin are always the host's,
/// whatever the plugin ships: <c>Incremental*</c>, <c>Microsoft.Extensions.*</c>,
/// <c>Confluent.Kafka</c> and the framework (<c>System.*</c> and its kin). Two copies of
/// one of those would be two sets of types, and a definition could not be handed a builder.
/// </para>
/// <para>
/// Any other assembly the host happens to carry is shared when the host's version
/// satisfies the reference (it is at least the version asked for). When the plugin was
/// built against a newer one and ships it, the plugin gets its own copy: the default
/// context would refuse the higher version, and the package holds one that works.
/// </para>
/// </summary>
internal static class HostAssemblies
{
    private static readonly Dictionary<string, string> Trusted = TrustedPaths();
    private static readonly ConcurrentDictionary<string, Version?> TrustedVersions = new(StringComparer.OrdinalIgnoreCase);

    private static Dictionary<string, string> TrustedPaths()
    {
        var paths = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var path in ((AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") as string) ?? "").Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
            paths.TryAdd(Path.GetFileNameWithoutExtension(path), path);
        return paths;
    }

    public static bool Contains(string name) => TryVersion(name, out _);

    /// <summary>Whether the host carries an assembly of that name, and at what version
    /// (null when it cannot be read).</summary>
    public static bool TryVersion(string name, out Version? version)
    {
        if (AssemblyLoadContext.Default.Assemblies.FirstOrDefault(a => string.Equals(a.GetName().Name, name, StringComparison.OrdinalIgnoreCase)) is { } loaded)
        {
            version = loaded.GetName().Version;
            return true;
        }
        if (Trusted.TryGetValue(name, out var path))
        {
            version = TrustedVersions.GetOrAdd(name, static (_, file) =>
            {
                try
                {
                    return AssemblyName.GetAssemblyName(file).Version;
                }
                catch
                {
                    return null;
                }
            }, path);
            return true;
        }
        version = null;
        return false;
    }

    /// <summary>The assemblies whose types cross the boundary between host and plugin.</summary>
    public static bool AlwaysShared(string name) =>
        name.Equals("Incremental", StringComparison.OrdinalIgnoreCase)
        || name.StartsWith("Incremental.", StringComparison.OrdinalIgnoreCase) && Contains(name)
        || name.StartsWith("Microsoft.Extensions.", StringComparison.OrdinalIgnoreCase)
        || name.Equals("Confluent.Kafka", StringComparison.OrdinalIgnoreCase)
        || name.Equals("System", StringComparison.OrdinalIgnoreCase)
        || name.StartsWith("System.", StringComparison.OrdinalIgnoreCase)
        || name is "mscorlib" or "netstandard" or "WindowsBase" or "Microsoft.CSharp" or "Microsoft.VisualBasic" or "Microsoft.VisualBasic.Core"
        || name.StartsWith("Microsoft.Win32.", StringComparison.OrdinalIgnoreCase);

    /// <summary>Whether a reference goes to the host rather than to the plugin's own files.</summary>
    public static bool Provides(AssemblyName name, bool pluginShipsIt)
    {
        if (name.Name is not { } simple || !TryVersion(simple, out var hosted))
            return false;
        return AlwaysShared(simple) || !pluginShipsIt || name.Version is null || hosted is null || hosted >= name.Version;
    }
}

/// <summary>
/// The contract assemblies of this process, by simple name. Process-wide on purpose: a row
/// type has to be one type for every table, plugin and node object in the process. An
/// assembly registered by the host (<c>AddContracts</c>) lives in the default context; one
/// that arrived through the catalog is loaded into one non-collectible context.
/// </summary>
internal static class ContractAssemblies
{
    private sealed class Context() : AssemblyLoadContext("incremental-contracts", isCollectible: false)
    {
        protected override Assembly? Load(AssemblyName name)
        {
            if (name.Name is null || HostAssemblies.Contains(name.Name))
                return null;
            if (Find(name.Name) is { } contract)
                return contract;
            lock (Loaded)
            {
                foreach (var directory in Directories)
                {
                    var path = Path.Combine(directory, name.Name + ".dll");
                    if (File.Exists(path))
                        return LoadFromAssemblyPath(path);
                }
            }
            return null;
        }
    }

    private static readonly Context Shared = new();
    private static readonly Dictionary<string, (Assembly Assembly, string Version)> Loaded = new(StringComparer.OrdinalIgnoreCase);
    private static readonly List<string> Directories = [];

    /// <summary>Whether a loaded contract came from that directory: its files are in use
    /// for the life of the process.</summary>
    public static bool Uses(string directory)
    {
        lock (Loaded)
            return Directories.Any(d => d.StartsWith(directory, StringComparison.Ordinal));
    }

    public static Assembly? Find(string name)
    {
        lock (Loaded)
            return Loaded.TryGetValue(name, out var loaded) ? loaded.Assembly : null;
    }

    public static string? VersionOf(string name)
    {
        lock (Loaded)
            return Loaded.TryGetValue(name, out var loaded) ? loaded.Version : null;
    }

    public static IReadOnlyList<Assembly> All
    {
        get
        {
            lock (Loaded)
                return Loaded.Values.Select(l => l.Assembly).ToList();
        }
    }

    public static void RegisterBuiltIn(Assembly assembly)
    {
        lock (Loaded)
            Loaded.TryAdd(assembly.GetName().Name!, (assembly, assembly.ManifestModule.ModuleVersionId.ToString("N")[..16]));
    }

    /// <summary>Loads the contract unless one of that name is already loaded; returns
    /// whether this version is the one in use. <paramref name="mainAssembly"/> is the
    /// file within <paramref name="directory"/>; <paramref name="name"/> the assembly's
    /// simple name, which the file's need not be.</summary>
    public static bool Load(string directory, string mainAssembly, string name, string version)
    {
        lock (Loaded)
        {
            if (Loaded.TryGetValue(name, out var loaded))
                return loaded.Version == version;
            var path = Path.Combine(directory, mainAssembly.Replace('/', Path.DirectorySeparatorChar));
            Directories.Add(Path.GetDirectoryName(path)!);
            if (!Directories.Contains(directory))
                Directories.Add(directory);
            Loaded[name] = (Shared.LoadFromAssemblyPath(path), version);
            return true;
        }
    }
}

/// <summary>A pipeline plugin's load context. An assembly name resolves, in order, to the
/// host's own (default context) by the rules of <see cref="HostAssemblies"/>, to a loaded
/// contract assembly even if the plugin ships its own copy, and otherwise privately from
/// the plugin's files: beside its main assembly, then the package's root.</summary>
internal sealed class PluginLoadContext : AssemblyLoadContext
{
    private readonly string[] _directories;
    private readonly AssemblyDependencyResolver? _resolver;
    private readonly HashSet<string> _private = new(StringComparer.OrdinalIgnoreCase);

    public PluginLoadContext(string name, string directory, string mainAssemblyPath)
        : base(name, isCollectible: true)
    {
        var beside = Path.GetDirectoryName(mainAssemblyPath)!;
        _directories = string.Equals(beside, directory, StringComparison.Ordinal) ? [directory] : [beside, directory];
        if (File.Exists(Path.ChangeExtension(mainAssemblyPath, ".deps.json")))
            _resolver = new AssemblyDependencyResolver(mainAssemblyPath);
    }

    /// <summary>Whether this context resolved that name to a file of the plugin's own.</summary>
    public bool LoadedPrivately(string name)
    {
        lock (_private)
            return _private.Contains(name);
    }

    protected override Assembly? Load(AssemblyName name)
    {
        if (name.Name is null)
            return null;
        var path = _resolver?.ResolveAssemblyToPath(name);
        if (path is null)
        {
            foreach (var directory in _directories)
            {
                var probe = Path.Combine(directory, name.Name + ".dll");
                if (File.Exists(probe))
                {
                    path = probe;
                    break;
                }
            }
        }
        if (ContractAssemblies.Find(name.Name) is { } contract)
            return contract;
        if (HostAssemblies.Provides(name, pluginShipsIt: path is not null))
            return null;
        if (path is null)
            return null;
        lock (_private)
            _private.Add(name.Name);
        return LoadFromAssemblyPath(path);
    }

    protected override IntPtr LoadUnmanagedDll(string unmanagedDllName) =>
        _resolver?.ResolveUnmanagedDllToPath(unmanagedDllName) is { } path ? LoadUnmanagedDllFromPath(path) : IntPtr.Zero;
}

/// <summary>
/// One use of a loaded plugin version: a pipeline generation, or a dry run. It gives the
/// definitions and the <see cref="JsonSerializerOptions"/> everything of the plugin's must
/// be serialized with — per version, so that their metadata caches go when the version is
/// unloaded. Disposing the last lease of a version unloads its context.
/// </summary>
internal sealed class PluginLease : IDisposable
{
    private readonly LoadedPlugin _plugin;
    private int _disposed;

    internal PluginLease(LoadedPlugin plugin) => _plugin = plugin;

    public string Name => _plugin.Name;
    public string Version => _plugin.Version;
    public JsonSerializerOptions Json => _plugin.Json;
    public JsonSerializerOptions StateJson => _plugin.StateJson;
    public PluginLease Again() => _plugin.Owner.Lease(_plugin);

    public DefinitionBinding Definition(string name) =>
        _plugin.Definitions.TryGetValue(name, out var definition)
            ? definition
            : throw ServiceException.Invalid(
                $"Plugin '{Name}' version {Version} has no pipeline definition '{name}' (it has: {string.Join(", ", _plugin.Definitions.Keys.Order())}).");

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
            _plugin.Owner.Release(_plugin);
    }
}

/// <summary>A definition class found in a plugin assembly, and how to run it.</summary>
internal sealed class DefinitionBinding(string name, Type type, Type? settingsType, string? description)
{
    public string Name => name;
    public Type Type => type;
    public Type? SettingsType => settingsType;
    public string? Description => description;

    /// <summary>Binds the spec's settings — camelCase, unknown members refused, string
    /// enums — and validates their DataAnnotations. Null for a definition with none.</summary>
    public object? BindSettings(JsonElement? settings, JsonSerializerOptions json)
    {
        if (settingsType is null)
        {
            if (settings is { ValueKind: JsonValueKind.Object } given && given.EnumerateObject().Any())
                throw ServiceException.Invalid($"Definition '{name}' takes no settings.");
            return null;
        }

        object bound;
        try
        {
            var element = settings is { ValueKind: JsonValueKind.Object } s ? s : JsonDocument.Parse("{}").RootElement;
            bound = element.Deserialize(settingsType, json) ?? throw new JsonException("The settings are null.");
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException)
        {
            throw ServiceException.Invalid($"The settings of definition '{name}' do not bind: {ex.Message}");
        }

        // Validated attribute by attribute rather than through Validator: its store of
        // type metadata is a process-wide static and would pin the plugin's context.
        var errors = new List<string>();
        foreach (var property in settingsType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var value = property.GetValue(bound);
            foreach (var attribute in property.GetCustomAttributes<ValidationAttribute>(inherit: true))
                if (!attribute.IsValid(value))
                    errors.Add(attribute.FormatErrorMessage(JsonNamingPolicy.CamelCase.ConvertName(property.Name)));
        }
        if (errors.Count > 0)
            throw ServiceException.Invalid($"The settings of definition '{name}' are not valid.", errors);
        return bound;
    }

    /// <summary>What a reflective call threw, without the wrappers reflection puts round
    /// it: a constructor's or a static initializer's own exception is the one to report.</summary>
    public static Exception Cause(Exception ex)
    {
        while (ex is TargetInvocationException or TypeInitializationException && ex.InnerException is { } inner)
            ex = inner;
        return ex;
    }

    public void Build(PipelineBuilder pipeline, object? settings)
    {
        try
        {
            var instance = Activator.CreateInstance(type)
                ?? throw new InvalidOperationException($"Definition class {type} could not be constructed.");
            if (settingsType is null)
                ((IPipelineDefinition)instance).Build(pipeline);
            else
                typeof(IPipelineDefinition<>).MakeGenericType(settingsType).GetMethod("Build")!.Invoke(instance, [pipeline, settings]);
        }
        catch (Exception ex) when (ex is TargetInvocationException or TypeInitializationException && ex.InnerException is not null)
        {
            // The constructor's, the static initializer's or the Build method's own
            // exception, not reflection's wrapper round it.
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(Cause(ex)).Throw();
        }
    }

    public PluginDefinitionInfo Describe()
    {
        if (settingsType is null)
            return new PluginDefinitionInfo(name, type.FullName!, description, null, []);

        // The settings as a spec that gives none leaves them: where the defaults are read.
        object? defaults = null;
        try
        {
            defaults = Activator.CreateInstance(settingsType);
        }
        catch
        {
            // No parameterless constructor, or one that throws: no defaults to show.
        }
        return new PluginDefinitionInfo(
            name, type.FullName!, description, settingsType.FullName,
            settingsType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanWrite && p.Name != "EqualityContract")
                .Select(p =>
                {
                    var plain = System.Nullable.GetUnderlyingType(p.PropertyType) ?? p.PropertyType;
                    var range = p.GetCustomAttribute<RangeAttribute>();
                    return new SettingInfo(
                        JsonNamingPolicy.CamelCase.ConvertName(p.Name), JsonTypeOf(p.PropertyType),
                        p.GetCustomAttribute<RequiredAttribute>() is not null || p.GetCustomAttribute<RequiredMemberAttribute>() is not null,
                        p.GetCustomAttribute<StateNeutralAttribute>() is not null,
                        p.GetCustomAttribute<DescriptionAttribute>()?.Description,
                        defaults is null ? null : DefaultOf(p, defaults),
                        plain.IsEnum ? Enum.GetNames(plain) : null,
                        Number(range?.Minimum), Number(range?.Maximum),
                        !p.PropertyType.IsValueType || System.Nullable.GetUnderlyingType(p.PropertyType) is not null);
                })
                .ToList());
    }

    private static double? Number(object? bound)
    {
        try
        {
            return bound is null ? null : Convert.ToDouble(bound, System.Globalization.CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
        {
            return null;
        }
    }

    /// <summary>A setting's default as JSON, for the values JSON has a plain form for:
    /// numbers, booleans, strings (durations, dates and enums among them) and lists of
    /// those. Written by hand: a serializer would cache metadata for the plugin's types.</summary>
    private static JsonElement? DefaultOf(PropertyInfo property, object defaults)
    {
        object? value;
        try
        {
            value = property.GetValue(defaults);
        }
        catch
        {
            return null;
        }
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            if (!Write(writer, value, depth: 0))
                return null;
        }
        return value is null ? null : JsonDocument.Parse(buffer.ToArray()).RootElement.Clone();

        static bool Write(Utf8JsonWriter writer, object? value, int depth)
        {
            switch (value)
            {
                case null: writer.WriteNullValue(); return true;
                case bool b: writer.WriteBooleanValue(b); return true;
                case string s: writer.WriteStringValue(s); return true;
                case Enum e: writer.WriteStringValue(e.ToString()); return true;
                case decimal d: writer.WriteNumberValue(d); return true;
                case double d: writer.WriteNumberValue(d); return true;
                case float f: writer.WriteNumberValue(f); return true;
                case sbyte or byte or short or ushort or int or uint or long:
                    writer.WriteNumberValue(Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture)); return true;
                case ulong u: writer.WriteNumberValue(u); return true;
                case TimeSpan t: writer.WriteStringValue(t.ToString("c")); return true;
                case DateTimeOffset t: writer.WriteStringValue(t); return true;
                case DateTime t: writer.WriteStringValue(t); return true;
                case Guid g: writer.WriteStringValue(g); return true;
                case System.Collections.IEnumerable items when depth == 0 && value is not System.Collections.IDictionary:
                    writer.WriteStartArray();
                    foreach (var item in items)
                        if (!Write(writer, item, depth + 1))
                            return false;
                    writer.WriteEndArray();
                    return true;
                default: return false;
            }
        }
    }

    private static string JsonTypeOf(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        if (type == typeof(bool))
            return "boolean";
        if (type.IsEnum || type == typeof(string) || type == typeof(TimeSpan) || type == typeof(DateTimeOffset) || type == typeof(DateTime) || type == typeof(Guid))
            return "string";
        if (type.IsPrimitive || type == typeof(decimal))
            return "number";
        if (type != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(type))
            return "array";
        return "object";
    }

    public static Dictionary<string, DefinitionBinding> Discover(Assembly assembly)
    {
        var found = new Dictionary<string, DefinitionBinding>(StringComparer.Ordinal);
        foreach (var type in assembly.GetTypes())
        {
            if (ForType(type) is not { } binding)
                continue;
            if (!found.TryAdd(binding.Name, binding))
                throw ServiceException.Invalid($"The plugin has two pipeline definitions named '{binding.Name}' ({found[binding.Name].Type.FullName} and {type.FullName}).");
        }
        return found;
    }

    /// <summary>The binding of one class; null for a class that is not a definition.</summary>
    public static DefinitionBinding? ForType(Type type)
    {
        if (!type.IsClass || type.IsAbstract)
            return null;
        var generic = type.GetInterfaces().Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IPipelineDefinition<>)).ToList();
        var plain = typeof(IPipelineDefinition).IsAssignableFrom(type);
        if (generic.Count == 0 && !plain)
            return null;
        if (generic.Count + (plain ? 1 : 0) > 1)
            throw ServiceException.Invalid($"Definition class {type.FullName} implements more than one definition interface; it has one settings type or none.");
        if (type.GetConstructor(Type.EmptyTypes) is null)
            throw ServiceException.Invalid($"Definition class {type.FullName} has no public parameterless constructor.");
        var attribute = type.GetCustomAttribute<PipelineAttribute>();
        return new DefinitionBinding(attribute?.Name ?? type.Name, type, generic.FirstOrDefault()?.GetGenericArguments()[0], attribute?.Description);
    }
}

internal sealed class LoadedPlugin(
    PluginCatalog owner, string name, string version, PluginLoadContext? context, Dictionary<string, DefinitionBinding> definitions)
{
    public PluginCatalog Owner => owner;
    public string Name => name;
    public string Version => version;

    /// <summary>Null for a plugin compiled into the host: it lives in the default context
    /// and is never unloaded.</summary>
    public PluginLoadContext? Context { get; set; } = context;
    public Dictionary<string, DefinitionBinding> Definitions { get; set; } = definitions;
    public int Users;

    /// <summary>Set when a contract arrived that this context had already resolved to a
    /// copy of its own: no new use is given this context, and it unloads with its last.</summary>
    public bool Superseded;

    public JsonSerializerOptions Json { get; set; } = NewJson();
    public JsonSerializerOptions StateJson { get; set; } = NewStateJson();

    // Each instance gets a resolver of its own. System.Text.Json shares one metadata
    // cache between options instances it finds equivalent, and two that differ in nothing
    // would share theirs: one version's types would then live as long as any other
    // version's options — or, for the state options, as long as the process's defaults.
    public static JsonSerializerOptions NewJson() => new(JsonSerializerDefaults.Web)
    {
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
        Converters = { new JsonStringEnumConverter() },
        TypeInfoResolver = new DefaultJsonTypeInfoResolver(),
    };

    /// <summary>The changelog's format: what <c>JsonStateSerializer</c> writes by default.</summary>
    public static JsonSerializerOptions NewStateJson() => new()
    {
        IncludeFields = true,
        TypeInfoResolver = new DefaultJsonTypeInfoResolver(),
    };
}

internal sealed class PluginCatalog(ServiceContext context) : IPluginCatalog
{
    private readonly ILogger _logger = context.LoggerFactory.CreateLogger("Incremental.Service.Plugins");
    private readonly object _lock = new();
    private readonly Dictionary<(string Name, string Version), LoadedPlugin> _loaded = [];
    private readonly Dictionary<string, PluginManifest> _builtIn = new(StringComparer.Ordinal);
    private readonly List<WeakReference> _contexts = [];
    private readonly List<WeakReference> _inspections = [];
    private readonly SemaphoreSlim _import = new(1, 1);
    private readonly ConcurrentDictionary<string, string> _pendingContracts = new(StringComparer.Ordinal);

    private static string DefaultCacheBase => Path.Combine(Path.GetTempPath(), "incremental-service");

    private string CacheRoot =>
        context.Options.CacheDirectory ?? Path.Combine(DefaultCacheBase, Environment.ProcessId.ToString(), context.NodeId);

    // ── the cache directory ────────────────────────────────────────────────────────────

    /// <summary>At start: what earlier runs left. Under the default location that is the
    /// whole directory of every process that no longer runs; under any location, imports
    /// that were cut short.</summary>
    private void CleanCacheAtStart()
    {
        if (context.Options.CacheDirectory is null && Directory.Exists(DefaultCacheBase))
        {
            foreach (var directory in Directory.EnumerateDirectories(DefaultCacheBase))
            {
                if (!int.TryParse(Path.GetFileName(directory), out var pid) || pid == Environment.ProcessId)
                    continue;
                try
                {
                    using var process = System.Diagnostics.Process.GetProcessById(pid);
                    if (!process.HasExited)
                        continue; // a live process: its own to delete
                }
                catch (Exception ex) when (ex is ArgumentException or InvalidOperationException)
                {
                    // No such process.
                }
                TryDeleteDirectory(directory);
            }
        }
        TryDeleteDirectory(Path.Combine(CacheRoot, "import"));
    }

    /// <summary>At dispose: the default location belongs to this process and goes with it.
    /// A configured directory is kept, so that the next run loads from disk.</summary>
    internal void DeleteOwnCache()
    {
        if (context.Options.CacheDirectory is not null)
            return;
        TryDeleteDirectory(CacheRoot);
        try
        {
            var parent = Path.GetDirectoryName(CacheRoot)!;
            if (Directory.Exists(parent) && !Directory.EnumerateFileSystemEntries(parent).Any())
                Directory.Delete(parent);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
        }
    }

    /// <summary>Best effort: a loaded assembly's file cannot be deleted on Windows, and what
    /// stays is deleted by a later prune or the next start.</summary>
    private static void TryDeleteDirectory(string directory)
    {
        try
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
        }
    }

    /// <summary>
    /// Deletes the cached files of plugin versions the catalog no longer holds and this
    /// process has not loaded. With no <paramref name="catalog"/> (the leader) the catalog
    /// is read under the import lock, so no version can be stored between that read and
    /// the deletes; a standby passes the entries it has just fetched from.
    /// </summary>
    internal async Task PruneCacheAsync(IReadOnlyDictionary<string, string>? catalog, CancellationToken cancellationToken)
    {
        var root = Path.Combine(CacheRoot, "plugins");
        if (!Directory.Exists(root))
            return;
        await _import.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            catalog ??= context.Catalog.Read();
            var stored = catalog.Keys.Where(k => k.StartsWith("plugin/", StringComparison.Ordinal)).ToHashSet(StringComparer.Ordinal);
            foreach (var plugin in Directory.EnumerateDirectories(root))
            {
                foreach (var version in Directory.EnumerateDirectories(plugin))
                {
                    var key = (Name: Path.GetFileName(plugin), Version: Path.GetFileName(version));
                    if (stored.Contains($"plugin/{key.Name}/{key.Version}") || ContractAssemblies.Uses(version))
                        continue;
                    lock (_lock)
                    {
                        if (_loaded.ContainsKey(key))
                            continue;
                        _prefetched.Remove(key);
                    }
                    TryDeleteDirectory(version);
                }
                try
                {
                    if (!Directory.EnumerateFileSystemEntries(plugin).Any())
                        Directory.Delete(plugin);
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                }
            }
        }
        finally
        {
            _import.Release();
        }
    }

    // ── built in ───────────────────────────────────────────────────────────────────────

    internal void LoadBuiltIns()
    {
        CleanCacheAtStart();
        foreach (var assembly in context.Registrations.Contracts)
        {
            ContractAssemblies.RegisterBuiltIn(assembly);
            var manifest = Describe(assembly, PluginKind.Contract, [], assembly.ManifestModule.ModuleVersionId.ToString("N")[..16]);
            _builtIn[manifest.Name] = manifest;
        }
        foreach (var assembly in context.Registrations.Plugins)
        {
            var version = assembly.ManifestModule.ModuleVersionId.ToString("N")[..16];
            var definitions = DefinitionBinding.Discover(assembly);
            var manifest = Describe(assembly, PluginKind.Pipeline, definitions.Values, version);
            _builtIn[manifest.Name] = manifest;
            lock (_lock)
                _loaded[(manifest.Name, version)] = new LoadedPlugin(this, manifest.Name, version, null, definitions) { Users = 1 };
        }
    }

    // mainFile: the main assembly's file within the package; null for an assembly
    // compiled into the host, which has no package.
    private static PluginManifest Describe(
        Assembly assembly, PluginKind kind, IEnumerable<DefinitionBinding> definitions, string version,
        IReadOnlyList<PluginFile>? files = null, string? mainFile = null)
    {
        var name = assembly.GetName().Name!;
        return new PluginManifest(
            name.ToLowerInvariant(), version, kind, mainFile ?? name + ".dll", files ?? [],
            definitions.Select(d => d.Describe()).OrderBy(d => d.Name, StringComparer.Ordinal).ToList(),
            assembly.GetTypes().Where(t => !t.IsAbstract && typeof(ITableDefinition).IsAssignableFrom(t)).Select(t => t.FullName!).Order().ToList(),
            Fingerprint(assembly), name)
        {
            InformationalVersion = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion is { Length: > 0 } text
                ? (text.Length > 128 ? text[..128] : text)
                : null,
        };
    }

    /// <summary>
    /// A hash over the data shapes the assembly declares: the public fields and
    /// properties of its types, the members and values of its enums (state stores an enum
    /// as its number), and the members of its anonymous types (the natural key of a
    /// <c>Distinct</c> or a <c>GroupBy</c>). It informs a keep-or-reset choice and proves
    /// nothing: a shape built only inside a delegate from types of other assemblies — a
    /// tuple, a type of a private dependency — is not in it.
    /// </summary>
    internal static string Fingerprint(Assembly assembly)
    {
        var text = new StringBuilder();
        static bool Anonymous(Type type) => type.Name.Contains("AnonymousType", StringComparison.Ordinal);
        foreach (var type in assembly.GetTypes()
                     .Where(t => t.GetCustomAttribute<CompilerGeneratedAttribute>() is null || Anonymous(t))
                     .OrderBy(t => t.FullName, StringComparer.Ordinal))
        {
            text.Append(type.FullName).Append('{');
            if (type.IsEnum)
            {
                foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.Static).OrderBy(f => f.Name, StringComparer.Ordinal))
                    text.Append(field.Name).Append('=').Append(Convert.ToString(field.GetRawConstantValue(), System.Globalization.CultureInfo.InvariantCulture)).Append(';');
            }
            foreach (var member in type.GetMembers(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                         .Where(m => m is PropertyInfo or FieldInfo).OrderBy(m => m.Name, StringComparer.Ordinal))
            {
                var memberType = (member as PropertyInfo)?.PropertyType ?? ((FieldInfo)member).FieldType;
                text.Append(member.Name).Append(':').Append(memberType.FullName ?? memberType.Name).Append(';');
            }
            text.Append('}');
        }
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text.ToString())))[..16].ToLowerInvariant();
    }

    // ── what is stored ─────────────────────────────────────────────────────────────────

    private IEnumerable<PluginManifest> Stored() =>
        context.Catalog.Read().Where(e => e.Key.StartsWith("plugin/", StringComparison.Ordinal)).Select(e => ServiceJson.Read<PluginManifest>(e.Value));

    public async Task<IReadOnlyList<PluginInfo>> RescanAsync(CancellationToken cancellationToken)
    {
        context.RequireLeader();
        if (context.Options.PluginDirectory is null)
            throw ServiceException.Conflict("This node has no plugin directory configured.");
        await ImportPluginDirectoryAsync(cancellationToken).ConfigureAwait(false);
        return Plugins;
    }

    public PluginManifest? Manifest(string name, string version) =>
        _builtIn.TryGetValue(name, out var builtIn) && builtIn.Version == version
            ? builtIn
            : context.Catalog.Get<PluginManifest>($"plugin/{name}/{version}");

    /// <summary>The version a spec means when it names none: the only one there is.</summary>
    internal string ResolveVersion(string name, string? version)
    {
        if (!string.IsNullOrEmpty(version))
            return version;
        var versions = Stored().Where(m => m.Name == name).Select(m => m.Version).ToList();
        if (_builtIn.TryGetValue(name, out var builtIn))
            versions.Add(builtIn.Version);
        return versions.Count switch
        {
            1 => versions[0],
            0 => throw ServiceException.Invalid($"No plugin named '{name}' is stored."),
            _ => throw ServiceException.Invalid($"Plugin '{name}' has {versions.Count} versions ({string.Join(", ", versions.Order())}); the spec must name one."),
        };
    }

    public int LiveContexts
    {
        get
        {
            lock (_lock)
            {
                _contexts.RemoveAll(c => !c.IsAlive);
                _inspections.RemoveAll(c => !c.IsAlive);
                return _contexts.Count + _inspections.Count;
            }
        }
    }

    public IReadOnlyList<PluginInfo> Plugins
    {
        get
        {
            var users = context.PipelineSpecs().GroupBy(p => (p.Plugin, p.Version)).ToDictionary(g => g.Key, g => g.Count());
            var infos = new List<PluginInfo>();
            foreach (var manifest in _builtIn.Values)
                infos.Add(new PluginInfo(manifest.Name, manifest.Version, manifest.Kind, true, manifest.Definitions, manifest.TableDefinitions,
                    "built-in", users.GetValueOrDefault((manifest.Name, manifest.Version))));
            var stored = Stored().ToList();
            foreach (var manifest in stored)
            {
                string state;
                if (manifest.Kind == PluginKind.Contract)
                {
                    // One version of a contract is loaded per process. A stored version
                    // imported after it is pending (a restart loads it); one imported
                    // before it is superseded (no restart will).
                    var inUse = ContractAssemblies.VersionOf(manifest.MainAssemblyName);
                    state = inUse == manifest.Version ? "loaded"
                        : inUse is not null && stored.FirstOrDefault(m => m.Kind == PluginKind.Contract && m.Version == inUse
                            && string.Equals(m.MainAssemblyName, manifest.MainAssemblyName, StringComparison.OrdinalIgnoreCase)) is { } loaded
                            && loaded.Sequence > manifest.Sequence ? "superseded"
                        : "pending";
                }
                else
                    lock (_lock)
                        state = _loaded.ContainsKey((manifest.Name, manifest.Version)) ? "loaded" : "stored";
                infos.Add(new PluginInfo(manifest.Name, manifest.Version, manifest.Kind, false, manifest.Definitions, manifest.TableDefinitions,
                    state, users.GetValueOrDefault((manifest.Name, manifest.Version))));
            }
            return infos.OrderBy(i => i.Name, StringComparer.Ordinal).ThenBy(i => i.Version, StringComparer.Ordinal).ToList();
        }
    }

    // ── contracts and table definitions ────────────────────────────────────────────────

    /// <summary>Loads every stored contract this process has not loaded: of the versions
    /// of one assembly, the one imported last (<see cref="PluginManifest.Sequence"/>), so a
    /// restart always picks the newest. A version imported after the loaded one stays
    /// pending until the process restarts.</summary>
    internal async Task LoadContractsAsync(IReadOnlyDictionary<string, string> catalog, CancellationToken cancellationToken)
    {
        var contracts = ContractLoadOrder(
            catalog.Where(e => e.Key.StartsWith("plugin/", StringComparison.Ordinal)).Select(e => ServiceJson.Read<PluginManifest>(e.Value)));
        foreach (var manifest in contracts)
        {
            try
            {
                var assemblyName = manifest.MainAssemblyName;
                if (ContractAssemblies.VersionOf(assemblyName) is { } loaded)
                {
                    var inUse = contracts.FirstOrDefault(m => m.Version == loaded && string.Equals(m.MainAssemblyName, assemblyName, StringComparison.OrdinalIgnoreCase));
                    if (loaded != manifest.Version && manifest.Sequence > (inUse?.Sequence ?? long.MaxValue)
                        && _pendingContracts.TryAdd(manifest.Name, manifest.Version))
                        _logger.LogWarning(
                            "Contract {Contract} version {Version} is stored but version {Loaded} is loaded; it takes effect when this process restarts",
                            manifest.Name, manifest.Version, loaded);
                    continue;
                }
                var directory = await MaterializeAsync(manifest, cancellationToken).ConfigureAwait(false);
                ContractAssemblies.Load(directory, manifest.MainAssembly, assemblyName, manifest.Version);
                _logger.LogInformation("Contract {Contract} version {Version} loaded", manifest.Name, manifest.Version);
                SupersedePrivateCopies(assemblyName);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Contract {Contract} version {Version} could not be loaded", manifest.Name, manifest.Version);
            }
        }
    }

    /// <summary>
    /// A contract has just been loaded. A plugin context that resolved that name earlier
    /// did so to the plugin's own copy, and keeps it for its life: its row types are not
    /// the contract's. Such a context is given to no new use — the next acquire of the
    /// version loads a fresh one, which gets the contract — and unloads with its last
    /// lease. The pipelines that run on it keep their private copy until they are restarted.
    /// </summary>
    internal void SupersedePrivateCopies(string assemblyName)
    {
        lock (_lock)
        {
            foreach (var (key, plugin) in _loaded.ToList())
            {
                if (plugin.Context is not { } loadContext || !loadContext.LoadedPrivately(assemblyName))
                    continue;
                plugin.Superseded = true;
                _loaded.Remove(key);
                _logger.LogWarning(
                    "Plugin {Plugin} version {Version} was loaded before contract {Contract} and holds its own copy of it. Pipelines already running " +
                    "on it keep that copy until they are restarted; anything loaded from now on gets the contract",
                    plugin.Name, plugin.Version, assemblyName);
            }
        }
    }

    /// <summary>The stored contracts in the order they are tried: the version imported
    /// last first, so that it is the one a process loads, whatever order the catalog's
    /// rows come in.</summary>
    internal static List<PluginManifest> ContractLoadOrder(IEnumerable<PluginManifest> stored) =>
        stored.Where(m => m.Kind == PluginKind.Contract)
            .OrderByDescending(m => m.Sequence).ThenBy(m => m.Version, StringComparer.Ordinal).ToList();

    internal ITableDefinition ResolveTableDefinition(TableSpec spec)
    {
        foreach (var assembly in ContractAssemblies.All)
        {
            if (spec.Contract is not null && !string.Equals(assembly.GetName().Name, spec.Contract, StringComparison.OrdinalIgnoreCase))
                continue;
            if (assembly.GetType(spec.Definition) is { } type && typeof(ITableDefinition).IsAssignableFrom(type))
                return (ITableDefinition)(Activator.CreateInstance(type)
                    ?? throw new InvalidOperationException($"Table definition {type} could not be constructed."));
        }
        throw new InvalidOperationException(
            $"Table definition '{spec.Definition}' was not found in {(spec.Contract is null ? "any loaded contract assembly" : $"contract '{spec.Contract}'")} " +
            $"(loaded: {string.Join(", ", ContractAssemblies.All.Select(a => a.GetName().Name).Order())}).");
    }

    /// <summary>
    /// A standby's part in plugins: the files of every pipeline plugin version a deployed
    /// pipeline names are fetched from the blob store into the cache directory and checked
    /// against their hashes. No code is loaded. Returns how many versions are still
    /// missing — a manifest the catalog does not hold yet, or a blob not yet readable.
    /// </summary>
    internal async Task<int> PrefetchAsync(IReadOnlyDictionary<string, string> catalog, CancellationToken cancellationToken)
    {
        var pending = 0;
        var wanted = catalog.Where(e => e.Key.StartsWith("pipeline/", StringComparison.Ordinal))
            .Select(e => ServiceJson.Read<PipelineSpec>(e.Value))
            .Where(p => p.Desired != DesiredState.Removed)
            .Select(p => (p.Plugin, p.Version)).Distinct().ToList();
        foreach (var (name, version) in wanted)
        {
            if (_builtIn.TryGetValue(name, out var builtIn) && builtIn.Version == version)
                continue;
            lock (_lock)
                if (_loaded.ContainsKey((name, version)) || _prefetched.Contains((name, version)))
                    continue;
            try
            {
                if (!catalog.TryGetValue($"plugin/{name}/{version}", out var json))
                {
                    pending++;
                    continue;
                }
                await MaterializeAsync(ServiceJson.Read<PluginManifest>(json), cancellationToken).ConfigureAwait(false);
                lock (_lock)
                    _prefetched.Add((name, version));
                _logger.LogInformation("Plugin {Plugin} version {Version} fetched from the blob store", name, version);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                pending++;
                _logger.LogDebug(ex, "Plugin {Plugin} version {Version} is not fetched yet", name, version);
            }
        }
        return pending;
    }

    private readonly HashSet<(string Name, string Version)> _prefetched = [];

    // ── leases ─────────────────────────────────────────────────────────────────────────

    /// <summary>Loads the version if this node has not, and counts one more user of it.</summary>
    internal async Task<PluginLease> AcquireAsync(string name, string version, CancellationToken cancellationToken)
    {
        lock (_lock)
            if (_loaded.TryGetValue((name, version), out var loaded))
                return Lease(loaded);

        var manifest = Manifest(name, version)
            ?? throw ServiceException.Invalid($"Plugin '{name}' version {version} is not stored.");
        if (manifest.Kind != PluginKind.Pipeline)
            throw ServiceException.Invalid($"Plugin '{name}' is a contract assembly; it holds no pipeline definitions.");
        var directory = await MaterializeAsync(manifest, cancellationToken).ConfigureAwait(false);

        lock (_lock)
        {
            if (_loaded.TryGetValue((name, version), out var raced))
                return Lease(raced);
            _contexts.RemoveAll(c => !c.IsAlive);
            if (_contexts.Count >= context.Options.MaxLoadedPluginVersions)
                throw new ServiceException(
                    "too-many-plugin-versions",
                    $"{_contexts.Count} plugin load contexts are alive on this node (the cap is {context.Options.MaxLoadedPluginVersions}), counting " +
                    "versions asked to unload and not yet collected. Restart this node to reclaim them.", 503);
            var plugin = Load(name, version, directory, manifest.MainAssembly.Replace('/', Path.DirectorySeparatorChar));
            _loaded[(name, version)] = plugin;
            _logger.LogInformation("Plugin {Plugin} version {Version} loaded", name, version);
            return Lease(plugin);
        }
    }

    // Kept out of line so that no local of the caller's frame holds the assembly.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private LoadedPlugin Load(string name, string version, string directory, string mainAssembly)
    {
        var path = Path.Combine(directory, mainAssembly);
        var loadContext = new PluginLoadContext($"{name}@{version}", directory, path);
        _contexts.Add(new WeakReference(loadContext));
        try
        {
            return new LoadedPlugin(this, name, version, loadContext, DefinitionBinding.Discover(loadContext.LoadFromAssemblyPath(path)));
        }
        catch
        {
            loadContext.Unload();
            throw;
        }
    }

    internal PluginLease Lease(LoadedPlugin plugin)
    {
        lock (_lock)
            plugin.Users++;
        return new PluginLease(plugin);
    }

    internal void Release(LoadedPlugin plugin)
    {
        lock (_lock)
        {
            if (--plugin.Users > 0 || plugin.Context is not { } loadContext)
                return;
            if (_loaded.TryGetValue((plugin.Name, plugin.Version), out var held) && ReferenceEquals(held, plugin))
                _loaded.Remove((plugin.Name, plugin.Version));

            // Nothing of the plugin's may stay reachable from here: the definitions and
            // the serializer options hold its types.
            plugin.Context = null;
            plugin.Definitions = [];
            plugin.Json = LoadedPlugin.NewJson();
            plugin.StateJson = LoadedPlugin.NewStateJson();
            loadContext.Unload();
            JsonAccessorCache.EvictSoon();
            _logger.LogInformation("Plugin {Plugin} version {Version} unloaded: nothing uses it", plugin.Name, plugin.Version);
        }
    }

    /// <summary>
    /// Per-version <see cref="JsonSerializerOptions"/> with a resolver of their own keep a
    /// plugin's types out of every metadata cache but one: System.Text.Json compiles the
    /// accessors of a type's constructor and members and keeps them in a process-wide
    /// cache, keyed by the type. An entry there is dropped about a second after its last
    /// use — but only when the cache is next used, by anyone. On a quiet node nothing uses
    /// it, and the unloaded context stays alive, counted against
    /// <see cref="ServiceOptions.MaxLoadedPluginVersions"/>, until something does. So after
    /// an unload the cache is used on purpose, a few times over the next seconds, with a
    /// type of the service's own.
    /// </summary>
    private static class JsonAccessorCache
    {
        private sealed class Probe
        {
            public int Value { get; set; }
        }

        private static int _pending;

        public static void EvictSoon()
        {
            if (Interlocked.Exchange(ref _pending, 1) != 0)
                return;
            Task.Run(async () =>
            {
                try
                {
                    for (var pass = 0; pass < 4; pass++)
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(1200)).ConfigureAwait(false);
                        if (pass == 0)
                            Interlocked.Exchange(ref _pending, 0); // an unload from here on asks again
                        Touch();
                    }
                }
                catch
                {
                    // Best effort: the cap on live contexts is what catches a context that stays.
                }
            }).Forget();
        }

        // New options with a new resolver each time: metadata for Probe is built again,
        // which is a use of the accessor cache and so a pass of its eviction.
        private static void Touch() =>
            JsonSerializer.Serialize(new Probe(), new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() });
    }

    /// <summary>The plugin's files, from the blob store, in the cache directory, each
    /// checked against its hash.</summary>
    private async Task<string> MaterializeAsync(PluginManifest manifest, CancellationToken cancellationToken)
    {
        var directory = Path.Combine(CacheRoot, "plugins", manifest.Name, manifest.Version);
        foreach (var file in manifest.Files)
        {
            var target = Path.Combine(directory, file.Path.Replace('/', Path.DirectorySeparatorChar));
            if (File.Exists(target) && await HashAsync(target, cancellationToken).ConfigureAwait(false) == file.Sha256)
                continue;
            Directory.CreateDirectory(Path.GetDirectoryName(target)!);
            var temporary = target + "." + Guid.NewGuid().ToString("N") + ".tmp";
            await using (var stream = File.Create(temporary))
            {
                if (!await context.Backend.Blobs.CopyToAsync(file.Sha256, stream, cancellationToken).ConfigureAwait(false))
                {
                    stream.Close();
                    File.Delete(temporary);
                    throw new InvalidOperationException(
                        $"Plugin '{manifest.Name}' version {manifest.Version}: the blob of '{file.Path}' ({file.Sha256}) is not in the blob store yet.");
                }
            }
            if (await HashAsync(temporary, cancellationToken).ConfigureAwait(false) != file.Sha256)
            {
                File.Delete(temporary);
                throw new InvalidDataException($"Plugin '{manifest.Name}' version {manifest.Version}: '{file.Path}' does not match its hash.");
            }
            File.Move(temporary, target, overwrite: true);
        }
        return directory;
    }

    private static async Task<string> HashAsync(string path, CancellationToken cancellationToken)
    {
        await using var stream = File.OpenRead(path);
        return Convert.ToHexString(await SHA256.HashDataAsync(stream, cancellationToken).ConfigureAwait(false)).ToLowerInvariant();
    }

    // ── import ─────────────────────────────────────────────────────────────────────────

    private static readonly Regex SafeEntry = new("^[A-Za-z0-9._/-]+$", RegexOptions.CultureInvariant);

    private static void RequireSafe(string entry)
    {
        if (!SafeEntry.IsMatch(entry) || Path.IsPathRooted(entry) || entry.StartsWith('/') || entry.Split('/').Any(part => part is ".." or "." or ""))
            throw ServiceException.Invalid($"Plugin file name '{entry}' is not a safe relative name (allowed: [A-Za-z0-9._/-], no '..', not rooted).");
    }

    public async Task<PluginManifest> ImportZipAsync(Stream zip, CancellationToken cancellationToken, string? caller = null)
    {
        ArgumentNullException.ThrowIfNull(zip);
        context.RequireLeader();
        var temporary = Path.Combine(CacheRoot, "import", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(temporary);
        FileStream? spooled = null;
        try
        {
            // ZipArchive reads a stream that cannot seek by copying all of it into memory
            // first, before any limit here applies. Such a stream is copied to a file
            // instead, and refused once it is larger than a plugin may inflate to.
            if (!zip.CanSeek)
            {
                spooled = new FileStream(
                    temporary + ".zip", FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 81920, FileOptions.DeleteOnClose | FileOptions.Asynchronous);
                var chunk = new byte[81920];
                int got;
                while ((got = await zip.ReadAsync(chunk, cancellationToken).ConfigureAwait(false)) > 0)
                {
                    if (spooled.Length + got > context.Options.MaxPluginBytes)
                        throw ServiceException.Invalid($"The zip is larger than {context.Options.MaxPluginBytes} bytes.");
                    await spooled.WriteAsync(chunk.AsMemory(0, got), cancellationToken).ConfigureAwait(false);
                }
                spooled.Position = 0;
                zip = spooled;
            }
            using (var archive = new ZipArchive(zip, ZipArchiveMode.Read, leaveOpen: true))
            {
                var files = archive.Entries.Where(e => !e.FullName.EndsWith('/')).ToList();
                if (files.Count > context.Options.MaxPluginFiles)
                    throw ServiceException.Invalid($"The zip has {files.Count} files; a plugin may have at most {context.Options.MaxPluginFiles}.");

                // Counted while inflating: the sizes in the headers are the sender's claim.
                var total = 0L;
                var buffer = new byte[81920];
                foreach (var entry in files)
                {
                    var name = entry.FullName.Replace('\\', '/');
                    RequireSafe(name);
                    var target = Path.Combine(temporary, name.Replace('/', Path.DirectorySeparatorChar));
                    Directory.CreateDirectory(Path.GetDirectoryName(target)!);
                    await using var source = entry.Open();
                    await using var destination = File.Create(target);
                    int read;
                    while ((read = await source.ReadAsync(buffer, cancellationToken).ConfigureAwait(false)) > 0)
                    {
                        total += read;
                        if (total > context.Options.MaxPluginBytes)
                            throw ServiceException.Invalid($"The zip inflates to more than {context.Options.MaxPluginBytes} bytes.");
                        await destination.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            return await ImportDirectoryAsync(temporary, cancellationToken, caller).ConfigureAwait(false);
        }
        finally
        {
            if (spooled is not null)
                await spooled.DisposeAsync().ConfigureAwait(false);

            // Best effort, and never the import's outcome: on Windows a file of the
            // package that the inspection loaded is still mapped until its context is
            // collected, and deleting it is refused. What stays is deleted at the next start.
            TryDeleteDirectory(temporary);
        }
    }

    public async Task<PluginManifest> ImportDirectoryAsync(string directory, CancellationToken cancellationToken, string? caller = null)
    {
        context.RequireLeader();
        directory = Path.GetFullPath(directory);
        if (!Directory.Exists(directory))
            throw ServiceException.Invalid($"Plugin directory '{directory}' does not exist.");

        var files = new List<PluginFile>();
        var total = 0L;
        foreach (var path in Directory.EnumerateFiles(directory, "*", SearchOption.AllDirectories).Order(StringComparer.Ordinal))
        {
            var relative = Path.GetRelativePath(directory, path).Replace(Path.DirectorySeparatorChar, '/');
            if (relative.EndsWith(".pdb", StringComparison.OrdinalIgnoreCase))
                continue;
            RequireSafe(relative);
            var size = new FileInfo(path).Length;
            total += size;
            if (files.Count >= context.Options.MaxPluginFiles)
                throw ServiceException.Invalid($"The plugin has more than {context.Options.MaxPluginFiles} files.");
            if (total > context.Options.MaxPluginBytes)
                throw ServiceException.Invalid($"The plugin is larger than {context.Options.MaxPluginBytes} bytes.");
            files.Add(new PluginFile(relative, await HashAsync(path, cancellationToken).ConfigureAwait(false), size));
        }

        var version = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(
            string.Join('\n', files.OrderBy(f => f.Path, StringComparer.Ordinal).Select(f => f.Path + "\0" + f.Sha256)))))[..16].ToLowerInvariant();
        var main = MainAssemblyOf(directory, files);
        var manifest = Inspect(directory, main, version, files);
        ServiceNames.Validate(manifest.Name, "plugin name");

        async Task PutMissingBlobsAsync()
        {
            foreach (var file in files)
            {
                if (await context.Backend.Blobs.ExistsAsync(file.Sha256, cancellationToken).ConfigureAwait(false))
                    continue;
                await using var stream = File.OpenRead(Path.Combine(directory, file.Path.Replace('/', Path.DirectorySeparatorChar)));
                await context.Backend.Blobs.PutAsync(file.Sha256, stream, cancellationToken).ConfigureAwait(false);
            }
        }

        // The lock a delete holds too: a delete works out which blobs no stored version
        // needs, and must not do that between this import's look at a blob and its manifest.
        await _import.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (Manifest(manifest.Name, version) is { } existing)
            {
                // The same bytes again. Any blob the store has lost is put back, so
                // importing a version again is what repairs it.
                if (existing.Files.Count > 0)
                    await PutMissingBlobsAsync().ConfigureAwait(false);
                return existing;
            }
            if (_builtIn.ContainsKey(manifest.Name))
                throw ServiceException.Conflict($"Plugin '{manifest.Name}' is compiled into this host; a stored plugin cannot take its name.");
            var stored = Stored().ToList();
            if (stored.Sum(m => m.Bytes) + manifest.Bytes > context.Options.MaxStoredPluginBytes)
                throw new ServiceException("too-large", $"Storing this plugin would pass the cap of {context.Options.MaxStoredPluginBytes} bytes of stored plugins.", 413);
            if (stored.FirstOrDefault(m => m.Name == manifest.Name && m.Kind != manifest.Kind) is { } other)
                throw ServiceException.Conflict($"Plugin '{manifest.Name}' is stored as a {other.Kind} plugin; this one is a {manifest.Kind} plugin.");

            // Blobs before the manifest: content-addressed, harmless if interrupted.
            await PutMissingBlobsAsync().ConfigureAwait(false);

            using (var write = await context.Catalog.BeginAsync(cancellationToken).ConfigureAwait(false))
            {
                // The audit entry's sequence is the version's place among the versions of
                // its name: later imports sort after it on every node.
                var sequence = write.Audit(context.NodeId, context.Term, caller, "plugin.import", $"{manifest.Name}/{version}", null, null);
                manifest = manifest with { Sequence = sequence, ImportedAt = DateTimeOffset.UtcNow, ImportedBy = caller ?? "local" };
                write.Put($"plugin/{manifest.Name}/{version}", manifest);
                await write.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            _logger.LogInformation("Plugin {Plugin} version {Version} imported ({Kind}, {Files} files)", manifest.Name, version, manifest.Kind, files.Count);
            context.Events.Publish("plugin", manifest.Name, $"version {version} imported");
        }
        finally
        {
            _import.Release();
        }

        if (manifest.Kind == PluginKind.Contract)
            await LoadContractsAsync(context.Catalog.Read(), cancellationToken).ConfigureAwait(false);
        return manifest;
    }

    /// <summary>The assembly with a <c>.deps.json</c> beside it; with none or several, the
    /// only assembly, or the one <c>plugin.json</c> names.</summary>
    private static string MainAssemblyOf(string directory, List<PluginFile> files)
    {
        var named = Path.Combine(directory, "plugin.json");
        if (File.Exists(named))
        {
            using var document = JsonDocument.Parse(File.ReadAllText(named));
            if (document.RootElement.TryGetProperty("assembly", out var assembly) && assembly.GetString() is { } name && files.Any(f => f.Path == name))
                return name;
            throw ServiceException.Invalid("plugin.json must name the main assembly: { \"assembly\": \"My.Plugin.dll\" }.");
        }
        var withDeps = files.Where(f => f.Path.EndsWith(".deps.json", StringComparison.Ordinal))
            .Select(f => f.Path[..^".deps.json".Length] + ".dll").Where(dll => files.Any(f => f.Path == dll)).ToList();
        if (withDeps.Count == 1)
            return withDeps[0];
        var assemblies = files.Where(f => f.Path.EndsWith(".dll", StringComparison.Ordinal) && !f.Path.Contains('/')).ToList();
        if (assemblies.Count == 1)
            return assemblies[0].Path;
        throw ServiceException.Invalid(
            "The plugin's main assembly cannot be told: it is the one with a .deps.json beside it, or the only .dll, or the one plugin.json names.");
    }

    /// <summary>Loads the plugin into a throwaway context, reads what it defines, unloads.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private PluginManifest Inspect(string directory, string main, string version, List<PluginFile> files)
    {
        var path = Path.Combine(directory, main.Replace('/', Path.DirectorySeparatorChar));
        var throwaway = new PluginLoadContext("inspect", directory, path);
        lock (_lock)
            _inspections.Add(new WeakReference(throwaway));
        try
        {
            // From a stream, so the file is not held and a contract inspected here is not
            // the copy the contract context later loads from the cache.
            using var bytes = new MemoryStream(File.ReadAllBytes(path));
            var assembly = throwaway.LoadFromStream(bytes);
            var kind = assembly.GetCustomAttribute<IncrementalContractAttribute>() is not null ? PluginKind.Contract : PluginKind.Pipeline;
            var definitions = DefinitionBinding.Discover(assembly);
            if (kind == PluginKind.Contract && definitions.Count > 0)
                throw ServiceException.Invalid("A contract assembly cannot hold pipeline definitions: it is never unloaded.");
            return Describe(assembly, kind, definitions.Values, version, files, main);
        }
        catch (ReflectionTypeLoadException ex)
        {
            throw ServiceException.Invalid(
                $"The plugin's types could not be loaded: {ex.LoaderExceptions.FirstOrDefault()?.Message ?? ex.Message}");
        }
        catch (BadImageFormatException ex)
        {
            throw ServiceException.Invalid($"'{main}' is not a .NET assembly: {ex.Message}");
        }
        finally
        {
            throwaway.Unload();
        }
    }

    /// <summary>Imports every package in <see cref="ServiceOptions.PluginDirectory"/> the
    /// catalog lacks. A package is a subdirectory or a zip.</summary>
    internal async Task ImportPluginDirectoryAsync(CancellationToken cancellationToken)
    {
        if (context.Options.PluginDirectory is not { } root || !Directory.Exists(root))
            return;

        // Contracts first, so that the pipeline plugins that follow resolve against them.
        var packages = Directory.EnumerateDirectories(root).Concat(Directory.EnumerateFiles(root, "*.zip")).Order(StringComparer.Ordinal).ToList();
        var deferred = new List<string>();
        foreach (var pass in new[] { true, false })
        {
            foreach (var package in pass ? packages : deferred)
            {
                try
                {
                    PluginManifest manifest;
                    if (Directory.Exists(package))
                        manifest = await ImportDirectoryAsync(package, cancellationToken, "plugin-directory").ConfigureAwait(false);
                    else
                    {
                        await using var zip = File.OpenRead(package);
                        manifest = await ImportZipAsync(zip, cancellationToken, "plugin-directory").ConfigureAwait(false);
                    }
                    _ = manifest;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    if (pass)
                        deferred.Add(package);
                    else
                        _logger.LogError(ex, "Plugin package {Package} could not be imported", package);
                }
            }
        }
    }

    public async Task DeleteAsync(string name, string version, CancellationToken cancellationToken, string? caller = null)
    {
        context.RequireLeader();

        // Two locks, in this order. The import lock: which blobs a delete may take is
        // worked out from the stored manifests, and an import must not be between its look
        // at a shared blob and its manifest while that happens. The references lock: a
        // deploy commits a spec naming a version under it, so from the check of who names
        // this one to the end of the delete no pipeline can come to name it.
        await _import.WaitAsync(cancellationToken).ConfigureAwait(false);
        IDisposable? references = null;
        try
        {
            references = await context.LockReferencesAsync(cancellationToken).ConfigureAwait(false);
            var manifest = context.Catalog.Get<PluginManifest>($"plugin/{name}/{version}")
                ?? throw ServiceException.NotFound($"Plugin '{name}' version {version}");
            RequireUnused(manifest);

            using (var write = await context.Catalog.BeginAsync(cancellationToken).ConfigureAwait(false))
            {
                write.Delete($"plugin/{name}/{version}");
                write.Audit(context.NodeId, context.Term, caller, "plugin.delete", $"{name}/{version}", null, null);
                await write.CommitAsync(cancellationToken).ConfigureAwait(false);
            }

            // The blob store's writes are not fenced. A node that has been replaced must
            // not go on to delete what the new leader may have stored again, so each
            // deletion is preceded by a look at whether this node still leads. That narrows
            // the window to one call; a blob lost in it is put back by importing the
            // version again.
            var kept = Stored().SelectMany(m => m.Files).Select(f => f.Sha256).ToHashSet();
            foreach (var file in manifest.Files.Where(f => !kept.Contains(f.Sha256)))
            {
                if (!context.IsLeader)
                    break;
                await context.Backend.Blobs.DeleteAsync(file.Sha256, cancellationToken).ConfigureAwait(false);
            }

            var cached = Path.Combine(CacheRoot, "plugins", manifest.Name, manifest.Version);
            bool loaded;
            lock (_lock)
            {
                loaded = _loaded.ContainsKey((name, version));
                _prefetched.Remove((name, version));
            }
            if (!loaded && !ContractAssemblies.Uses(cached))
                TryDeleteDirectory(cached);
            _pendingContracts.TryRemove(new KeyValuePair<string, string>(name, version));
            context.Events.Publish("plugin", name, $"version {version} deleted");
        }
        finally
        {
            references?.Dispose();
            _import.Release();
        }
    }

    /// <summary>Why a stored version cannot be deleted, as a refusal; nothing when it can.
    /// A pipeline names its plugin version. A table names a definition, which a contract
    /// provides: a contract is in use while a declared table's definition is in it and in
    /// no other stored version of the same assembly, because a process that starts later
    /// could not create that table.</summary>
    internal void RequireUnused(PluginManifest manifest)
    {
        if (context.PipelineSpecs().FirstOrDefault(p => p.Plugin == manifest.Name && p.Version == manifest.Version) is { } user)
            throw ServiceException.Conflict($"Plugin '{manifest.Name}' version {manifest.Version} is named by pipeline '{user.Id}'.");

        // The version a replace would be rolled back to is still needed until that
        // replace has run.
        if (context.PipelineSpecs().FirstOrDefault(p => p.Previous is { } previous && previous.Plugin == manifest.Name && previous.Version == manifest.Version) is { } unconfirmed)
            throw ServiceException.Conflict(
                $"Plugin '{manifest.Name}' version {manifest.Version} is what pipeline '{unconfirmed.Id}' goes back to if its replacement cannot start; " +
                "it can be deleted once the pipeline has run.");
        if (manifest.Kind != PluginKind.Contract)
            return;
        var assembly = manifest.MainAssemblyName;
        var elsewhere = Stored()
            .Where(m => m.Kind == PluginKind.Contract && m.Version != manifest.Version
                && string.Equals(m.MainAssemblyName, assembly, StringComparison.OrdinalIgnoreCase))
            .SelectMany(m => m.TableDefinitions).ToHashSet(StringComparer.Ordinal);
        var tables = context.Tables.Handles.Select(h => h.Spec)
            .Where(t => (t.Contract is null || string.Equals(t.Contract, assembly, StringComparison.OrdinalIgnoreCase))
                && manifest.TableDefinitions.Contains(t.Definition) && !elsewhere.Contains(t.Definition))
            .Select(t => t.Name).Order(StringComparer.Ordinal).ToList();
        if (tables.Count > 0)
            throw ServiceException.Conflict(
                $"Contract '{manifest.Name}' version {manifest.Version} defines table(s) {string.Join(", ", tables)} and no other stored version does; " +
                "a node that starts without it could not create them. Remove the tables first, or import the version that replaces it.");
    }

}
