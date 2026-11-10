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

public sealed record SettingInfo(string Name, string JsonType, bool Required, bool StateNeutral, string? Description);

public sealed record PluginDefinitionInfo(
    string Name, string Type, string? Description, string? SettingsType, IReadOnlyList<SettingInfo> Settings);

/// <summary>A stored plugin version, as the catalog holds it under
/// <c>plugin/{name}/{version}</c>. The version is a hash of the files, so the same bytes
/// are the same version on every node.</summary>
public sealed record PluginManifest(
    string Name, string Version, PluginKind Kind, string MainAssembly, IReadOnlyList<PluginFile> Files,
    IReadOnlyList<PluginDefinitionInfo> Definitions, IReadOnlyList<string> TableDefinitions, string Fingerprint)
{
    public long Bytes => Files.Sum(f => f.Size);
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
    /// validated while it is unpacked.</summary>
    Task<PluginManifest> ImportZipAsync(Stream zip, CancellationToken cancellationToken, string? caller = null);

    /// <summary>Leader: refused while a pipeline spec names the version.</summary>
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

/// <summary>What the default load context provides: the host's own assemblies. A plugin
/// never gets a private copy of one of these.</summary>
internal static class HostAssemblies
{
    private static readonly HashSet<string> Trusted = new(
        ((AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") as string) ?? "")
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
            .Select(p => Path.GetFileNameWithoutExtension(p)!),
        StringComparer.OrdinalIgnoreCase);

    public static bool Contains(string name) =>
        Trusted.Contains(name)
        || AssemblyLoadContext.Default.Assemblies.Any(a => string.Equals(a.GetName().Name, name, StringComparison.OrdinalIgnoreCase));
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
    /// whether this version is the one in use.</summary>
    public static bool Load(string directory, string mainAssembly, string version)
    {
        var name = Path.GetFileNameWithoutExtension(mainAssembly);
        lock (Loaded)
        {
            if (Loaded.TryGetValue(name, out var loaded))
                return loaded.Version == version;
            Directories.Add(directory);
            Loaded[name] = (Shared.LoadFromAssemblyPath(Path.Combine(directory, mainAssembly)), version);
            return true;
        }
    }
}

/// <summary>A pipeline plugin's load context. An assembly name resolves, in order, to the
/// host's own (default context), to a loaded contract assembly even if the plugin ships
/// its own copy, and otherwise privately from the plugin's directory.</summary>
internal sealed class PluginLoadContext : AssemblyLoadContext
{
    private readonly string _directory;
    private readonly AssemblyDependencyResolver? _resolver;

    public PluginLoadContext(string name, string directory, string mainAssemblyPath)
        : base(name, isCollectible: true)
    {
        _directory = directory;
        if (File.Exists(Path.ChangeExtension(mainAssemblyPath, ".deps.json")))
            _resolver = new AssemblyDependencyResolver(mainAssemblyPath);
    }

    protected override Assembly? Load(AssemblyName name)
    {
        if (name.Name is null || HostAssemblies.Contains(name.Name))
            return null;
        if (ContractAssemblies.Find(name.Name) is { } contract)
            return contract;
        var path = _resolver?.ResolveAssemblyToPath(name);
        if (path is null)
        {
            var probe = Path.Combine(_directory, name.Name + ".dll");
            path = File.Exists(probe) ? probe : null;
        }
        return path is null ? null : LoadFromAssemblyPath(path);
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

    public void Build(PipelineBuilder pipeline, object? settings)
    {
        var instance = Activator.CreateInstance(type)
            ?? throw new InvalidOperationException($"Definition class {type} could not be constructed.");
        try
        {
            if (settingsType is null)
                ((IPipelineDefinition)instance).Build(pipeline);
            else
                typeof(IPipelineDefinition<>).MakeGenericType(settingsType).GetMethod("Build")!.Invoke(instance, [pipeline, settings]);
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
        }
    }

    public PluginDefinitionInfo Describe() => new(
        name, type.FullName!, description, settingsType?.FullName,
        settingsType is null ? [] : settingsType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && p.Name != "EqualityContract")
            .Select(p => new SettingInfo(
                JsonNamingPolicy.CamelCase.ConvertName(p.Name), JsonTypeOf(p.PropertyType),
                p.GetCustomAttribute<RequiredAttribute>() is not null || p.GetCustomAttribute<RequiredMemberAttribute>() is not null,
                p.GetCustomAttribute<StateNeutralAttribute>() is not null,
                p.GetCustomAttribute<DescriptionAttribute>()?.Description))
            .ToList());

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
            if (!type.IsClass || type.IsAbstract)
                continue;
            var generic = type.GetInterfaces().Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IPipelineDefinition<>)).ToList();
            var plain = typeof(IPipelineDefinition).IsAssignableFrom(type);
            if (generic.Count == 0 && !plain)
                continue;
            if (generic.Count + (plain ? 1 : 0) > 1)
                throw ServiceException.Invalid($"Definition class {type.FullName} implements more than one definition interface; it has one settings type or none.");
            if (type.GetConstructor(Type.EmptyTypes) is null)
                throw ServiceException.Invalid($"Definition class {type.FullName} has no public parameterless constructor.");
            var attribute = type.GetCustomAttribute<PipelineAttribute>();
            var name = attribute?.Name ?? type.Name;
            if (!found.TryAdd(name, new DefinitionBinding(name, type, generic.FirstOrDefault()?.GetGenericArguments()[0], attribute?.Description)))
                throw ServiceException.Invalid($"The plugin has two pipeline definitions named '{name}' ({found[name].Type.FullName} and {type.FullName}).");
        }
        return found;
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
    private readonly SemaphoreSlim _import = new(1, 1);
    private readonly ConcurrentDictionary<string, string> _pendingContracts = new(StringComparer.Ordinal);

    private string CacheRoot =>
        context.Options.CacheDirectory ?? Path.Combine(Path.GetTempPath(), "incremental-service", Environment.ProcessId.ToString(), context.NodeId);

    // ── built in ───────────────────────────────────────────────────────────────────────

    internal void LoadBuiltIns()
    {
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

    private static PluginManifest Describe(Assembly assembly, PluginKind kind, IEnumerable<DefinitionBinding> definitions, string version, IReadOnlyList<PluginFile>? files = null)
    {
        var name = assembly.GetName().Name!;
        return new PluginManifest(
            name.ToLowerInvariant(), version, kind, name + ".dll", files ?? [],
            definitions.Select(d => d.Describe()).OrderBy(d => d.Name, StringComparer.Ordinal).ToList(),
            assembly.GetTypes().Where(t => !t.IsAbstract && typeof(ITableDefinition).IsAssignableFrom(t)).Select(t => t.FullName!).Order().ToList(),
            Fingerprint(assembly));
    }

    /// <summary>A hash over the public fields and properties of every type the assembly
    /// defines that is not compiler-generated: it informs a keep-or-reset choice.</summary>
    private static string Fingerprint(Assembly assembly)
    {
        var text = new StringBuilder();
        foreach (var type in assembly.GetTypes().Where(t => t.GetCustomAttribute<CompilerGeneratedAttribute>() is null).OrderBy(t => t.FullName, StringComparer.Ordinal))
        {
            text.Append(type.FullName).Append('{');
            foreach (var member in type.GetMembers(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                         .Where(m => m is PropertyInfo or FieldInfo).OrderBy(m => m.Name, StringComparer.Ordinal))
                text.Append(member.Name).Append(':').Append((member as PropertyInfo)?.PropertyType.FullName ?? ((FieldInfo)member).FieldType.FullName).Append(';');
            text.Append('}');
        }
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text.ToString())))[..16].ToLowerInvariant();
    }

    // ── what is stored ─────────────────────────────────────────────────────────────────

    private IEnumerable<PluginManifest> Stored() =>
        context.Catalog.Read().Where(e => e.Key.StartsWith("plugin/", StringComparison.Ordinal)).Select(e => ServiceJson.Read<PluginManifest>(e.Value));

    internal PluginManifest? Manifest(string name, string version) =>
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
                return _contexts.Count;
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
            foreach (var manifest in Stored())
            {
                string state;
                if (manifest.Kind == PluginKind.Contract)
                    state = ContractAssemblies.VersionOf(Path.GetFileNameWithoutExtension(manifest.MainAssembly)) == manifest.Version ? "loaded" : "pending";
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

    /// <summary>Loads every stored contract this process has not loaded. A second version
    /// of a loaded contract stays pending until the process restarts.</summary>
    internal async Task LoadContractsAsync(IReadOnlyDictionary<string, string> catalog, CancellationToken cancellationToken)
    {
        foreach (var manifest in catalog.Where(e => e.Key.StartsWith("plugin/", StringComparison.Ordinal))
                     .Select(e => ServiceJson.Read<PluginManifest>(e.Value)).Where(m => m.Kind == PluginKind.Contract))
        {
            try
            {
                var assemblyName = Path.GetFileNameWithoutExtension(manifest.MainAssembly);
                if (ContractAssemblies.VersionOf(assemblyName) is { } loaded)
                {
                    if (loaded != manifest.Version && _pendingContracts.TryAdd(manifest.Name, manifest.Version))
                        _logger.LogWarning(
                            "Contract {Contract} version {Version} is stored but version {Loaded} is loaded; it takes effect when this process restarts",
                            manifest.Name, manifest.Version, loaded);
                    continue;
                }
                var directory = await MaterializeAsync(manifest, cancellationToken).ConfigureAwait(false);
                ContractAssemblies.Load(directory, manifest.MainAssembly, manifest.Version);
                _logger.LogInformation("Contract {Contract} version {Version} loaded", manifest.Name, manifest.Version);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Contract {Contract} version {Version} could not be loaded", manifest.Name, manifest.Version);
            }
        }
    }

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
            var plugin = Load(name, version, directory, manifest.MainAssembly);
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
            _loaded.Remove((plugin.Name, plugin.Version));

            // Nothing of the plugin's may stay reachable from here: the definitions and
            // the serializer options hold its types.
            plugin.Context = null;
            plugin.Definitions = [];
            plugin.Json = LoadedPlugin.NewJson();
            plugin.StateJson = LoadedPlugin.NewStateJson();
            loadContext.Unload();
            _logger.LogInformation("Plugin {Plugin} version {Version} unloaded: nothing uses it", plugin.Name, plugin.Version);
        }
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
        context.RequireLeader();
        var temporary = Path.Combine(CacheRoot, "import", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(temporary);
        try
        {
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
            try
            {
                Directory.Delete(temporary, recursive: true);
            }
            catch (IOException)
            {
            }
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

        await _import.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (Manifest(manifest.Name, version) is { } existing)
                return existing;
            if (_builtIn.ContainsKey(manifest.Name))
                throw ServiceException.Conflict($"Plugin '{manifest.Name}' is compiled into this host; a stored plugin cannot take its name.");
            var stored = Stored().ToList();
            if (stored.Sum(m => m.Bytes) + manifest.Bytes > context.Options.MaxStoredPluginBytes)
                throw new ServiceException("too-large", $"Storing this plugin would pass the cap of {context.Options.MaxStoredPluginBytes} bytes of stored plugins.", 413);
            if (stored.FirstOrDefault(m => m.Name == manifest.Name && m.Kind != manifest.Kind) is { } other)
                throw ServiceException.Conflict($"Plugin '{manifest.Name}' is stored as a {other.Kind} plugin; this one is a {manifest.Kind} plugin.");

            // Blobs before the manifest: content-addressed, harmless if interrupted.
            foreach (var file in files)
            {
                if (await context.Backend.Blobs.ExistsAsync(file.Sha256, cancellationToken).ConfigureAwait(false))
                    continue;
                await using var stream = File.OpenRead(Path.Combine(directory, file.Path.Replace('/', Path.DirectorySeparatorChar)));
                await context.Backend.Blobs.PutAsync(file.Sha256, stream, cancellationToken).ConfigureAwait(false);
            }

            using (var write = await context.Catalog.BeginAsync(cancellationToken).ConfigureAwait(false))
            {
                write.Put($"plugin/{manifest.Name}/{version}", manifest);
                write.Audit(context.NodeId, context.Term, caller, "plugin.import", $"{manifest.Name}/{version}", null, null);
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
    private static PluginManifest Inspect(string directory, string main, string version, List<PluginFile> files)
    {
        var path = Path.Combine(directory, main);
        var throwaway = new PluginLoadContext("inspect", directory, path);
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
            return Describe(assembly, kind, definitions.Values, version, files);
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
        var manifest = context.Catalog.Get<PluginManifest>($"plugin/{name}/{version}")
            ?? throw ServiceException.NotFound($"Plugin '{name}' version {version}");
        if (context.PipelineSpecs().FirstOrDefault(p => p.Plugin == name && p.Version == version) is { } user)
            throw ServiceException.Conflict($"Plugin '{name}' version {version} is named by pipeline '{user.Id}'.");

        using (var write = await context.Catalog.BeginAsync(cancellationToken).ConfigureAwait(false))
        {
            write.Delete($"plugin/{name}/{version}");
            write.Audit(context.NodeId, context.Term, caller, "plugin.delete", $"{name}/{version}", null, null);
            await write.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        var kept = Stored().SelectMany(m => m.Files).Select(f => f.Sha256).ToHashSet();
        foreach (var file in manifest.Files.Where(f => !kept.Contains(f.Sha256)))
            await context.Backend.Blobs.DeleteAsync(file.Sha256, cancellationToken).ConfigureAwait(false);
        context.Events.Publish("plugin", name, $"version {version} deleted");
    }
}
