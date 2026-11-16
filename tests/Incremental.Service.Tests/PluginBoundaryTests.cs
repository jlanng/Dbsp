using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using Incremental.Service.Testing;

namespace Incremental.Service.Tests;

/// <summary>
/// What a plugin compiles against is <c>Incremental</c> and
/// <c>Incremental.Service.Abstractions</c>, and its tests <c>Incremental.Service.Testing</c>.
/// None of what those hand a plugin may name a type of the service's runtime, its hosting or
/// Kafka: a type that did would make every plugin compile against those assemblies again,
/// and put their types on the plugin boundary (<c>HostAssemblies.Provides</c>).
/// </summary>
public class PluginBoundaryTests
{
    /// <summary>Assemblies a plugin-facing type may name: the core, the two packages, the
    /// framework, and the logging abstractions the core's own surface carries.</summary>
    private static bool Allowed(string name) =>
        name is "Incremental" or "Incremental.Service.Abstractions" or "Incremental.Service.Testing"
            or "Microsoft.Extensions.Logging.Abstractions" or "mscorlib" or "netstandard"
        || name.StartsWith("System", StringComparison.Ordinal)
        || name.StartsWith("Microsoft.Win32.", StringComparison.Ordinal);

    /// <summary>The assemblies that must never reach a plugin by any path.</summary>
    private static bool Forbidden(string name) =>
        name is "Incremental.Service" or "Incremental.Service.Admin" or "Incremental.Kafka" or "Incremental.Hosting"
            or "Confluent.Kafka"
        || name.StartsWith("Microsoft.Extensions.Hosting", StringComparison.Ordinal)
        || name.StartsWith("Microsoft.Extensions.Diagnostics", StringComparison.Ordinal)
        || name.StartsWith("Microsoft.AspNetCore", StringComparison.Ordinal);

    private static readonly Assembly Abstractions = typeof(PipelineBuilder).Assembly;
    private static readonly Assembly Testing = typeof(PipelineTestHarness).Assembly;

    [Fact]
    public void The_plugin_facing_types_are_in_the_abstractions_package()
    {
        Assert.Equal("Incremental.Service.Abstractions", Abstractions.GetName().Name);
        foreach (var type in new[]
                 {
                     typeof(IPipelineDefinition), typeof(IPipelineDefinition<>), typeof(PipelineAttribute), typeof(PipelineBuilder),
                     typeof(TableDefinition<,>), typeof(ITableDefinition), typeof(IndexDefinition<,>), typeof(TableBuilder<,>),
                     typeof(TableDefinitionExtensions), typeof(IncrementalContractAttribute), typeof(StateNeutralAttribute),
                     typeof(ServiceException), typeof(PipelineShape), typeof(IRecordFormat), typeof(IRecordCodec<>),
                 })
            Assert.Same(Abstractions, type.Assembly);
    }

    [Fact]
    public void Nothing_the_abstractions_or_the_harness_show_names_the_services_runtime_its_hosting_or_kafka()
    {
        var problems = new List<string>();
        foreach (var assembly in new[] { Abstractions, Testing })
            foreach (var (where, type) in Surface(assembly))
                if (type.Assembly.GetName().Name is { } name && !Allowed(name))
                    problems.Add($"{where} names {type.FullName ?? type.Name} from {name}");
        Assert.True(problems.Count == 0, string.Join("\n", problems.Distinct()));
    }

    [Fact]
    public void The_abstractions_load_nothing_of_the_services_runtime_its_hosting_or_kafka()
    {
        // Everything the abstractions need to load, all the way down.
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var queue = new Queue<AssemblyName>([Abstractions.GetName()]);
        while (queue.TryDequeue(out var next))
        {
            if (next.Name is not { } name || !seen.Add(name))
                continue;
            Assert.False(Forbidden(name), $"The abstractions load {name} ({string.Join(", ", seen)})");
            if (name.StartsWith("System", StringComparison.Ordinal) || name is "mscorlib" or "netstandard")
                continue;
            foreach (var reference in Assembly.Load(next).GetReferencedAssemblies())
                queue.Enqueue(reference);
        }
        Assert.Contains("Incremental", seen);
    }

    public static TheoryData<string> PluginAssemblies() =>
    [
        "tests/Incremental.Service.TestPlugins/Acme.Contracts/bin/{c}/net10.0/Acme.Contracts.dll",
        "tests/Incremental.Service.TestPlugins/Acme.RefData/bin/{c}/net10.0/Acme.RefData.dll",
        "tests/Incremental.Service.TestPlugins/Acme.Venues/bin/{c}/net10.0/Acme.Venues.dll",
        "tests/Incremental.Service.TestPlugins/Acme.Pipelines/bin/{c}/net10.0/Acme.Pipelines.dll",
        "tests/Incremental.Service.TestPlugins/Acme.Pipelines.V2/bin/{c}/net10.0/Acme.Pipelines.dll",
        "tests/Incremental.Service.TestPlugins/Acme.Desks/bin/{c}/net10.0/Acme.Desks.dll",
        "examples/Incremental.EnrichmentService/Contracts/bin/{c}/net10.0/Enrichment.Contracts.dll",
        "examples/Incremental.EnrichmentService/Pipelines/bin/{c}/net10.0/Enrichment.Pipelines.dll",
        "examples/Incremental.EnrichmentService/Pipelines.V2/bin/{c}/net10.0/Enrichment.Pipelines.dll",
        "examples/Incremental.EnrichmentService/Pipelines.Tests/bin/{c}/net10.0/Enrichment.Pipelines.Tests.dll",
        "benchmarks/Incremental.Service.Scale/Contracts/bin/{c}/net10.0/Scale.Contracts.dll",
        "benchmarks/Incremental.Service.Scale/Pipelines/bin/{c}/net10.0/Scale.Pipelines.dll",
    ];

    [Theory]
    [MemberData(nameof(PluginAssemblies))]
    public void A_plugin_and_the_example_s_definition_tests_reference_none_of_the_services_runtime_its_hosting_or_kafka(string path)
    {
        var configuration = new DirectoryInfo(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar)).Parent!.Name;
        var file = Path.Combine(Root(), path.Replace("{c}", configuration));
        Assert.True(File.Exists(file), $"not built: {file}");

        // Read from the file's metadata, not loaded: what the compiler recorded is what
        // the assembly needs at runtime.
        using var stream = File.OpenRead(file);
        using var pe = new PEReader(stream);
        var metadata = pe.GetMetadataReader();
        var references = metadata.AssemblyReferences.Select(h => metadata.GetString(metadata.GetAssemblyReference(h).Name)).ToList();
        Assert.DoesNotContain(references, Forbidden);
        if (!file.EndsWith(".Tests.dll", StringComparison.Ordinal))
            Assert.Contains("Incremental.Service.Abstractions", references);
    }

    [Fact]
    public void The_example_s_definition_tests_reference_the_testing_package_and_not_the_service()
    {
        var project = File.ReadAllText(Path.Combine(Root(), "examples/Incremental.EnrichmentService/Pipelines.Tests/Enrichment.Pipelines.Tests.csproj"));
        Assert.Contains(@"src\Incremental.Service.Testing\Incremental.Service.Testing.csproj", project);
        Assert.DoesNotContain(@"src\Incremental.Service\Incremental.Service.csproj", project);
        foreach (var props in new[]
                 {
                     "examples/Incremental.EnrichmentService/Plugin.props", "tests/Incremental.Service.TestPlugins/Directory.Build.props",
                     "benchmarks/Incremental.Service.Scale/Plugin.props",
                 })
        {
            var text = File.ReadAllText(Path.Combine(Root(), props));
            Assert.Contains(@"src\Incremental.Service.Abstractions\Incremental.Service.Abstractions.csproj", text);
            Assert.DoesNotContain(@"src\Incremental.Service\Incremental.Service.csproj", text);
        }
    }

    // ── the surface, by reflection ─────────────────────────────────────────────────────

    /// <summary>Every type a public or protected member of a public type names: base
    /// types, interfaces, fields, properties, events, method and constructor signatures,
    /// generic arguments and constraints, and attributes' types.</summary>
    private static IEnumerable<(string Where, Type Type)> Surface(Assembly assembly)
    {
        const BindingFlags members = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly;
        foreach (var type in assembly.GetExportedTypes())
        {
            var where = type.FullName!;
            foreach (var named in Names(type.BaseType))
                yield return (where, named);
            foreach (var named in type.GetInterfaces().SelectMany(Names))
                yield return (where, named);
            foreach (var named in type.GetGenericArguments().SelectMany(a => a.GetGenericParameterConstraints()).SelectMany(Names))
                yield return (where, named);
            foreach (var member in type.GetMembers(members))
            {
                var at = $"{where}.{member.Name}";
                IEnumerable<Type> types = member switch
                {
                    FieldInfo f when f.IsPublic || f.IsFamily || f.IsFamilyOrAssembly => [f.FieldType],
                    PropertyInfo p when Visible(p.GetMethod) || Visible(p.SetMethod) => [p.PropertyType, .. p.GetIndexParameters().Select(i => i.ParameterType)],
                    EventInfo e when Visible(e.AddMethod) => [e.EventHandlerType!],
                    MethodInfo m when Visible(m) =>
                        [m.ReturnType, .. m.GetParameters().Select(p => p.ParameterType),
                         .. m.GetGenericArguments().SelectMany(a => a.GetGenericParameterConstraints())],
                    ConstructorInfo c when Visible(c) => c.GetParameters().Select(p => p.ParameterType),
                    _ => [],
                };
                foreach (var named in types.SelectMany(Names))
                    yield return (at, named);
            }
        }
    }

    private static bool Visible(MethodBase? method) => method is { IsPublic: true } or { IsFamily: true } or { IsFamilyOrAssembly: true };

    private static IEnumerable<Type> Names(Type? type)
    {
        if (type is null || type.IsGenericParameter)
            yield break;
        if (type.HasElementType)
        {
            foreach (var inner in Names(type.GetElementType()))
                yield return inner;
            yield break;
        }
        yield return type.IsGenericType ? type.GetGenericTypeDefinition() : type;
        if (type.IsGenericType)
            foreach (var argument in type.GetGenericArguments().SelectMany(Names))
                yield return argument;
    }

    private static string Root()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "Incremental.slnx")))
            directory = directory.Parent;
        return directory?.FullName ?? throw new InvalidOperationException("The repository root was not found above the test output.");
    }
}
