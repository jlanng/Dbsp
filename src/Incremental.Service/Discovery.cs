using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Incremental.Service;

/// <summary>One property of a connector's or a table source's config. <c>JsonType</c> is
/// <c>string</c>, <c>number</c>, <c>boolean</c>, <c>object</c> or <c>array</c>;
/// <c>Values</c> are the values it may take, when they are a closed set; <c>Default</c> is
/// what it is when left out, as text; a <c>Secret</c> one is returned by the admin API as
/// <c>***</c>.</summary>
public sealed record ConfigProperty(
    string Name, string JsonType, bool Required = false, string? Description = null, IReadOnlyList<string>? Values = null,
    string? Default = null, bool Secret = false);

/// <summary>What a config may say. <c>Open</c>: properties not listed are accepted (and
/// are the connector's own business); otherwise they are refused.</summary>
public sealed record ConfigSchema(IReadOnlyList<ConfigProperty> Properties, string? Description = null, bool Open = false);

/// <summary>Optional on a connector (<see cref="IConnector"/>) and on a table source
/// factory (<see cref="ITableSourceFactory"/>): says what its config accepts, so that an
/// operator composing a spec — or a form generated for one — does not have to read the
/// host's source. Without it the kind is listed with no schema.</summary>
public interface IConfigDescribed
{
    ConfigSchema ConfigSchema { get; }
}

/// <summary>A registered connector: its name, what kinds of slot it can be bound to, and
/// its config if it says.</summary>
public sealed record ConnectorInfo(string Name, IReadOnlyList<SlotKind> Kinds, bool Resettable, ConfigSchema? Config, string Type);

/// <summary>A named connection a spec may refer to. Names and kinds only: where it points
/// and how it authenticates stay in the host.</summary>
public sealed record ConnectionInfo(string Name, string Kind);

/// <summary>A table loader kind: built in (<c>file</c>, <c>kafka</c>, <c>debezium</c>) or
/// registered by the host. <c>Connection</c> is the kind of connection its
/// <see cref="TableLoaderSpec.Connection"/> names, null when it takes none.</summary>
public sealed record TableSourceInfo(string Kind, bool BuiltIn, string? Connection, ConfigSchema? Config);

/// <summary>A table a definition reads or writes: the slot (which a spec's <c>tables</c>
/// can bind to a declared table), the table it resolves to, and the types.</summary>
public sealed record TableSlotShape(string Slot, string Table, string Access, string KeyType, string RowType, string Definition);

/// <summary>
/// Everything a deploy of one definition needs to know: its settings (with defaults,
/// ranges and enum values), its input and sink slots with their kinds and element types,
/// and the tables it reads and writes. Slots are found by running the definition's
/// <c>Build</c> against nothing — no table need be declared, no connector bound — with the
/// settings given, or with the settings class's defaults. <c>Notes</c> says when that was
/// not possible and why.
/// </summary>
public sealed record DefinitionDescription(
    string Plugin, string Version, string Definition, string? Description, IReadOnlyList<SettingInfo> Settings,
    IReadOnlyList<SlotShape> Inputs, IReadOnlyList<SlotShape> Sinks, IReadOnlyList<TableSlotShape> Tables,
    IReadOnlyList<string> Notes);

/// <summary>A starting spec for a definition, with its real slot names bound to
/// connectors this host has registered. <c>Valid</c>: it passed the dry run as it is;
/// otherwise <c>Problems</c> lists what the dry run refused and <c>Notes</c> what to fill in.</summary>
public sealed record SpecTemplate(
    PipelineSpec Spec, bool Valid, IReadOnlyList<string> Problems, IReadOnlyList<string> Notes,
    IReadOnlyDictionary<string, IReadOnlyList<string>> ConnectorsForSlot);

/// <summary>A table definition a declared table can name: a <see cref="TableDefinition{TKey,TRow}"/>
/// class in a contract assembly this process has loaded. <c>DefaultName</c> is the name
/// the definition gives itself (and the slot a pipeline reads it under);
/// <c>DeclaredAs</c> are the tables declared from it so far.</summary>
public sealed record TableDefinitionInfo(
    string Definition, string Contract, string DefaultName, string KeyType, string RowType, IReadOnlyList<string> DeclaredAs);

/// <summary>One difference between two versions of a plugin. <c>Kind</c> is
/// <c>setting</c>, <c>input</c>, <c>sink</c>, <c>table</c> or <c>table-definition</c>;
/// <c>Change</c> is <c>added</c>, <c>removed</c> or <c>changed</c>; <c>From</c> and
/// <c>To</c> describe the member on each side in words.</summary>
public sealed record MemberChange(string Kind, string Name, string Change, string? From, string? To);

/// <summary>One definition across two versions of its plugin. <c>Change</c> is
/// <c>added</c>, <c>removed</c>, <c>changed</c> or <c>unchanged</c> — unchanged in what is
/// compared: its settings and, where they could be described, its slots and tables. What
/// the definition computes is not compared.</summary>
public sealed record DefinitionChange(string Definition, string Change, IReadOnlyList<MemberChange> Members, IReadOnlyList<string> Notes);

/// <summary>What differs between two stored versions of one plugin, as far as it is
/// declared: definitions, their settings, and — when <c>ShapesCompared</c> — their input,
/// sink and table slots, which are found by running each definition's <c>Build</c> on the
/// leader. State shapes are compared when a replace is planned, not here.</summary>
public sealed record PluginDiff(
    string Plugin, string From, string To, bool ShapesCompared, IReadOnlyList<DefinitionChange> Definitions,
    IReadOnlyList<MemberChange> TableDefinitions, IReadOnlyList<string> Notes);

/// <summary>What exists to deploy with, read from the host's registrations and the stored
/// plugins. The lists answer on any node; describing a definition runs its <c>Build</c>,
/// which only the leader does.</summary>
public interface IServiceDiscovery
{
    IReadOnlyList<ConnectorInfo> Connectors { get; }
    IReadOnlyList<ConnectionInfo> Connections { get; }
    IReadOnlyList<TableSourceInfo> TableSources { get; }

    /// <summary>The table definitions in the contract assemblies loaded in this process.</summary>
    IReadOnlyList<TableDefinitionInfo> TableDefinitions { get; }

    /// <summary>What differs between two versions of a plugin. Any node answers with the
    /// definitions and settings; the slots are compared on the leader only.</summary>
    Task<PluginDiff> DiffAsync(string plugin, string from, string to, CancellationToken cancellationToken);

    /// <summary>The definition's settings, slots and tables. <paramref name="version"/>
    /// may be empty while the plugin has one stored version; <paramref name="settings"/>
    /// are the settings to describe it under, null for its defaults. Leader only.</summary>
    Task<DefinitionDescription> DescribeDefinitionAsync(
        string plugin, string? version, string definition, JsonElement? settings, CancellationToken cancellationToken);

    /// <summary>A starting spec for the definition; its pipeline id is
    /// <paramref name="id"/>, or the definition's name. Leader only.</summary>
    Task<SpecTemplate> TemplateAsync(string plugin, string? version, string definition, string? id, CancellationToken cancellationToken);
}

internal sealed class ServiceDiscovery(ServiceContext context) : IServiceDiscovery
{
    public IReadOnlyList<ConnectorInfo> Connectors =>
        [.. context.Registrations.Connectors.Keys.Order(StringComparer.Ordinal).Select(Connector)];

    private ConnectorInfo Connector(string name)
    {
        IConnector connector;
        try
        {
            connector = context.Connector(JsonSerializer.SerializeToElement(new Dictionary<string, string> { ["connector"] = name }));
        }
        catch (Exception ex)
        {
            // A connector the host registered and cannot construct is still a name in use.
            return new ConnectorInfo(name, [], false, null, $"(could not be created: {ex.GetType().Name})");
        }
        var kinds = new List<SlotKind>();
        if (connector is IAppendSourceConnector)
            kinds.Add(SlotKind.AppendInput);
        if (connector is IKeyedSourceConnector)
            kinds.Add(SlotKind.KeyedInput);
        if (connector is IUpsertSinkConnector)
            kinds.Add(SlotKind.UpsertSink);
        if (connector is IAppendSinkConnector)
            kinds.Add(SlotKind.AppendSink);
        var schema = (connector as IConfigDescribed)?.ConfigSchema;
        if (connector is KafkaConnector)
            schema = WithConnections(schema!, "connection", context.KafkaConnections.Names);
        if (connector is FileConnector)
            schema = WithConnections(schema!, "store", context.Registrations.ObjectStores.Keys);
        return new ConnectorInfo(name, kinds, connector is IResettableSinkConnector, schema, connector.GetType().Name);
    }

    /// <summary>The schema with the property that names a connection listing the names registered.</summary>
    private static ConfigSchema WithConnections(ConfigSchema schema, string property, IEnumerable<string> names) =>
        schema with { Properties = [.. schema.Properties.Select(p => p.Name == property ? p with { Values = [.. names.Order(StringComparer.Ordinal)] } : p)] };

    public IReadOnlyList<ConnectionInfo> Connections =>
    [
        .. context.KafkaConnections.Names.Order(StringComparer.Ordinal).Select(n => new ConnectionInfo(n, "kafka")),
        .. context.Registrations.ObjectStores.Keys.Order(StringComparer.Ordinal).Select(n => new ConnectionInfo(n, "object-store")),
    ];

    public IReadOnlyList<TableSourceInfo> TableSources
    {
        get
        {
            var kafka = new ConfigSchema(
            [
                new("topic", "string", Required: true, Description: "The topic the table is loaded from."),
                new("startFrom", "string", Description: "Where to start with no committed position.", Values: ["earliest", "latest"], Default: "earliest"),
                new("reloadWhenPositionLost", "boolean", Description: "Reload the table from the start of the topic when its committed position has been deleted.", Default: "false"),
            ]);
            var sources = new List<TableSourceInfo>
            {
                new("file", true, "object-store", new ConfigSchema(
                [
                    new("path", "string", Required: true, Description: "A JSON-lines file, relative to the object store named by the loader's connection."),
                    new("pollInterval", "string", Description: "How often the file is looked at for a change, as a duration (hh:mm:ss).", Default: "00:00:05"),
                ], "Makes the table equal to the file whenever the file changes.")),
                new("kafka", true, "kafka", kafka with { Description = "A topic of rows as JSON: a value upserts by the table's key, a tombstone deletes." }),
                new("debezium", true, "kafka", kafka with { Description = "A topic of Debezium change events: snapshot, inserts, updates and deletes." }),
            };
            foreach (var (kind, factory) in context.Registrations.TableSources.OrderBy(s => s.Key, StringComparer.Ordinal))
            {
                ConfigSchema? schema = null;
                try
                {
                    schema = (factory(context.Services) as IConfigDescribed)?.ConfigSchema;
                }
                catch
                {
                    // Listed without a schema: a factory that cannot be made now says so when used.
                }
                // A registered kind takes the place of a built-in one of the same name.
                sources.RemoveAll(s => s.Kind == kind);
                sources.Add(new TableSourceInfo(kind, false, null, schema));
            }
            return sources;
        }
    }

    public IReadOnlyList<TableDefinitionInfo> TableDefinitions
    {
        get
        {
            var declared = context.Tables.Tables.ToLookup(t => t.Definition, t => t.Name, StringComparer.Ordinal);
            var found = new List<TableDefinitionInfo>();
            foreach (var assembly in ContractAssemblies.All)
            {
                Type[] types;
                try
                {
                    types = assembly.GetTypes();
                }
                catch (System.Reflection.ReflectionTypeLoadException ex)
                {
                    types = [.. ex.Types.Where(t => t is not null)!];
                }
                foreach (var type in types.Where(t => !t.IsAbstract && typeof(ITableDefinition).IsAssignableFrom(t) && t.FullName is not null))
                {
                    try
                    {
                        if (Activator.CreateInstance(type) is ITableDefinition definition)
                            found.Add(new TableDefinitionInfo(
                                type.FullName!, assembly.GetName().Name!, definition.Name, definition.KeyType.Name, definition.RowType.Name,
                                [.. declared[type.FullName!].Order(StringComparer.Ordinal)]));
                    }
                    catch
                    {
                        // A definition that cannot be constructed cannot be declared either.
                    }
                }
            }
            return [.. found.OrderBy(d => d.Definition, StringComparer.Ordinal).ThenBy(d => d.Contract, StringComparer.Ordinal)];
        }
    }

    internal static string Text(SettingInfo setting)
    {
        var parts = new List<string> { setting.JsonType + (setting.Nullable ? "?" : "") };
        if (setting.Required)
            parts.Add("required");
        if (setting.Default is { } value)
            parts.Add("default " + value.GetRawText());
        if (setting.Values is { Count: > 0 } values)
            parts.Add("one of " + string.Join(" | ", values));
        if (setting.Minimum is not null || setting.Maximum is not null)
            parts.Add(string.Create(System.Globalization.CultureInfo.InvariantCulture, $"range {setting.Minimum}..{setting.Maximum}"));
        if (setting.StateNeutral)
            parts.Add("state-neutral");
        return string.Join(", ", parts);
    }

    internal static List<MemberChange> Compare(string kind, IReadOnlyDictionary<string, string> from, IReadOnlyDictionary<string, string> to)
    {
        var changes = new List<MemberChange>();
        foreach (var name in from.Keys.Union(to.Keys).Order(StringComparer.Ordinal))
        {
            var before = from.GetValueOrDefault(name);
            var after = to.GetValueOrDefault(name);
            if (before is null)
                changes.Add(new MemberChange(kind, name, "added", null, after));
            else if (after is null)
                changes.Add(new MemberChange(kind, name, "removed", before, null));
            else if (before != after)
                changes.Add(new MemberChange(kind, name, "changed", before, after));
        }
        return changes;
    }

    public async Task<PluginDiff> DiffAsync(string plugin, string from, string to, CancellationToken cancellationToken)
    {
        ServiceNames.Validate(plugin, "plugin name");
        PluginInfo Version(string version) =>
            context.Plugins.Plugins.FirstOrDefault(p => p.Name == plugin && p.Version == version)
            ?? throw ServiceException.NotFound($"Plugin '{plugin}' version {version}");
        var before = Version(from);
        var after = Version(to);
        var notes = new List<string>();
        var shapes = context.IsLeader && before.Kind == PluginKind.Pipeline;
        if (!context.IsLeader && before.Kind == PluginKind.Pipeline)
            notes.Add("Slots and tables are not compared here: finding them runs each definition's Build, which only the leader does.");

        var definitions = new List<DefinitionChange>();
        var old = before.Definitions.ToDictionary(d => d.Name, StringComparer.Ordinal);
        var @new = after.Definitions.ToDictionary(d => d.Name, StringComparer.Ordinal);
        foreach (var name in old.Keys.Union(@new.Keys).Order(StringComparer.Ordinal))
        {
            if (!old.TryGetValue(name, out var a))
            {
                definitions.Add(new DefinitionChange(name, "added", [], []));
                continue;
            }
            if (!@new.TryGetValue(name, out var b))
            {
                definitions.Add(new DefinitionChange(name, "removed", [], []));
                continue;
            }
            var members = Compare("setting", a.Settings.ToDictionary(s => s.Name, Text, StringComparer.Ordinal), b.Settings.ToDictionary(s => s.Name, Text, StringComparer.Ordinal));
            var definitionNotes = new List<string>();
            if (shapes)
            {
                try
                {
                    var x = await DescribeDefinitionAsync(plugin, from, name, null, cancellationToken).ConfigureAwait(false);
                    var y = await DescribeDefinitionAsync(plugin, to, name, null, cancellationToken).ConfigureAwait(false);
                    static string Slot(SlotShape s) =>
                        $"{s.Kind}<{(s.KeyType is null ? "" : s.KeyType + ", ")}{s.Type}>" + (s.Members is { Count: > 0 } members ? "\n" + string.Join("\n", members) : "");
                    static string Table(TableSlotShape t) => $"{t.Access} {t.KeyType} → {t.RowType} ({t.Definition})";
                    members.AddRange(Compare("input", x.Inputs.ToDictionary(s => s.Name, Slot, StringComparer.Ordinal), y.Inputs.ToDictionary(s => s.Name, Slot, StringComparer.Ordinal)));
                    members.AddRange(Compare("sink", x.Sinks.ToDictionary(s => s.Name, Slot, StringComparer.Ordinal), y.Sinks.ToDictionary(s => s.Name, Slot, StringComparer.Ordinal)));
                    members.AddRange(Compare("table",
                        x.Tables.GroupBy(t => t.Slot, StringComparer.Ordinal).ToDictionary(g => g.Key, g => string.Join("; ", g.Select(Table)), StringComparer.Ordinal),
                        y.Tables.GroupBy(t => t.Slot, StringComparer.Ordinal).ToDictionary(g => g.Key, g => string.Join("; ", g.Select(Table)), StringComparer.Ordinal)));
                    definitionNotes.AddRange(x.Notes.Select(n => $"{Short(from)}: {n}"));
                    definitionNotes.AddRange(y.Notes.Select(n => $"{Short(to)}: {n}"));
                }
                catch (ServiceException ex)
                {
                    definitionNotes.Add($"Its slots could not be compared: {ex.Message}");
                }
            }
            definitions.Add(new DefinitionChange(name, members.Count > 0 ? "changed" : "unchanged", members, definitionNotes));
        }

        var tables = Compare("table-definition",
            before.TableDefinitions.ToDictionary(t => t, _ => "defined", StringComparer.Ordinal),
            after.TableDefinitions.ToDictionary(t => t, _ => "defined", StringComparer.Ordinal));
        return new PluginDiff(plugin, from, to, shapes, definitions, tables, notes);

        static string Short(string version) => version.Length > 8 ? version[..8] : version;
    }

    public async Task<DefinitionDescription> DescribeDefinitionAsync(
        string plugin, string? version, string definition, JsonElement? settings, CancellationToken cancellationToken)
    {
        context.RequireLeader();
        ServiceNames.Validate(plugin, "plugin name");
        var resolved = context.Plugins.ResolveVersion(plugin, version);
        using var lease = await context.Plugins.AcquireAsync(plugin, resolved, cancellationToken).ConfigureAwait(false);
        return Describe(plugin, resolved, definition, settings, lease);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private DefinitionDescription Describe(string plugin, string version, string name, JsonElement? settings, PluginLease lease)
    {
        var definition = lease.Definition(name);
        var info = definition.Describe();
        var notes = new List<string>();

        // The settings to build under: the ones given, else the ones a spec with none
        // would bind, else the settings class as its constructor leaves it.
        object? bound = null;
        var built = true;
        try
        {
            bound = definition.BindSettings(settings, lease.Json);
        }
        catch (ServiceException ex) when (settings is null && definition.SettingsType is { } type)
        {
            try
            {
                bound = Activator.CreateInstance(type);
                notes.Add($"The slots were found under the settings class's defaults, which a spec could not deploy as they are: {ex.Message} {string.Join(" ", ex.Details)}".TrimEnd());
            }
            catch (Exception inner)
            {
                built = false;
                notes.Add($"The slots are not known: the settings have required members and no defaults to try ({DefinitionBinding.Cause(inner).Message}). Describe the definition with settings.");
            }
        }

        PipelineBuilder? pipeline = null;
        if (built)
        {
            var spec = new PipelineSpec { Id = "describe", Plugin = plugin, Version = version, Definition = name };
            var builder = new CircuitBuilder(new CircuitOptions { Name = $"{name} (described)" });
            pipeline = new PipelineBuilder(spec, builder, context.Tables, context.LoggerFactory.CreateLogger("Incremental.Service.Pipelines"), context.Services, detached: true);
            try
            {
                definition.Build(pipeline, bound);
            }
            catch (Exception ex)
            {
                ex = DefinitionBinding.Cause(ex);
                notes.Add($"The slots may be incomplete: the definition threw while describing its circuit ({ex.GetType().Name}: {ex.Message}).");
            }
        }

        return new DefinitionDescription(
            plugin, version, info.Name, info.Description, info.Settings,
            pipeline?.Shape.Inputs ?? [], pipeline?.Shape.Sinks ?? [], pipeline?.Shape.TableSlots ?? [], notes);
    }

    public async Task<SpecTemplate> TemplateAsync(string plugin, string? version, string definition, string? id, CancellationToken cancellationToken)
    {
        var described = await DescribeDefinitionAsync(plugin, version, definition, null, cancellationToken).ConfigureAwait(false);
        id ??= ServiceNames.IsValid(described.Definition) ? described.Definition : "pipeline";
        ServiceNames.Validate(id, "pipeline id");
        var notes = new List<string>(described.Notes);
        var connectors = Connectors;
        var forSlot = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);

        // Settings: every one with a default as that default; one that is required and has
        // none is left for the caller, and named.
        var settings = new JsonObject();
        foreach (var setting in described.Settings)
        {
            if (setting.Default is { } value)
                settings[setting.Name] = JsonNode.Parse(value.GetRawText());
            else if (setting.Required)
                notes.Add($"Setting '{setting.Name}' ({setting.JsonType}) is required and has no default: give it a value.");
        }

        JsonElement Binding(SlotShape slot)
        {
            // A connector the host registered for this kind of slot, the host's own before
            // the built-in ones, and of those the first whose config needs nothing more.
            var able = connectors.Where(c => c.Kinds.Contains(slot.Kind)).Select(c => c.Name).ToList();
            forSlot[slot.Name] = able;
            var ordered = able.OrderBy(n => n is "memory" ? 1 : n is "kafka" ? 2 : n is "files" ? 3 : 0).ThenBy(n => n, StringComparer.Ordinal).ToList();
            foreach (var name in ordered)
            {
                var config = new JsonObject { ["connector"] = name };
                if (name == "memory")
                    config["name"] = $"{id}/{slot.Name}";
                var element = JsonSerializer.SerializeToElement(config);
                try
                {
                    context.Connector(element).Describe(element);
                    return element;
                }
                catch
                {
                    // It needs more than its name: not a config to offer unasked.
                }
            }
            var first = ordered.FirstOrDefault();
            notes.Add(first is null
                ? $"No registered connector can be bound to the {slot.Kind} slot '{slot.Name}'."
                : $"Slot '{slot.Name}' ({slot.Kind}) is bound to '{first}' with its required config left to fill in.");
            var placeholder = new JsonObject { ["connector"] = first ?? "" };
            if (first is not null && connectors.First(c => c.Name == first).Config is { } schema)
                foreach (var property in schema.Properties.Where(p => p.Required))
                    placeholder[property.Name] = property.Values is { Count: > 0 } values ? values[0] : "";
            return JsonSerializer.SerializeToElement(placeholder);
        }

        // Tables: a slot binds to the declared table of its own name; where there is none
        // of that name, to a declared table of the same definition if there is exactly one.
        var tables = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var slot in described.Tables)
        {
            var declared = context.Tables.Tables;
            if (declared.Any(t => t.Name == slot.Slot))
                continue;
            var sameDefinition = declared.Where(t => t.Definition == slot.Definition).Select(t => t.Name).ToList();
            if (sameDefinition.Count == 1)
                tables[slot.Slot] = sameDefinition[0];
            else
                notes.Add(sameDefinition.Count == 0
                    ? $"No table is declared for the slot '{slot.Slot}' ({slot.Definition}): declare one, or bind the slot under \"tables\"."
                    : $"The table slot '{slot.Slot}' could be any of {string.Join(", ", sameDefinition)}: bind it under \"tables\".");
        }

        var spec = new PipelineSpec
        {
            Id = id,
            Plugin = described.Plugin,
            Version = described.Version,
            Definition = described.Definition,
            Settings = settings.Count > 0 ? JsonSerializer.SerializeToElement(settings) : null,
            Tables = tables,
            Inputs = described.Inputs.ToDictionary(s => s.Name, Binding, StringComparer.Ordinal),
            Sinks = described.Sinks.ToDictionary(s => s.Name, Binding, StringComparer.Ordinal),
        };

        var problems = new List<string>();
        try
        {
            await context.Supervisor.ValidateAsync(spec, cancellationToken).ConfigureAwait(false);
        }
        catch (ServiceException ex)
        {
            problems.Add(ex.Message);
            problems.AddRange(ex.Details);
        }
        return new SpecTemplate(spec, problems.Count == 0, problems, notes, forSlot);
    }
}
