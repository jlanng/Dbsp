// Shared by the programs under tests/ and benchmarks/ that take --name value options
// (linked into each project, not a library): usage for --help, and exit code 2 for an
// option the program does not have. A program that silently ignores a misspelt option
// runs with a default nobody asked for.
namespace Incremental.Tools;

/// <summary>One option: its name, the name of its value (null for a switch) and one line of help.</summary>
internal sealed record Option(string Name, string? Value, string Help);

internal static class CommandLine
{
    /// <summary>
    /// Null when every argument is an option the program has and every option that takes
    /// a value has one. Otherwise the exit code to end with: 0 after printing usage for
    /// <c>--help</c>, 2 after naming what was wrong on standard error.
    /// </summary>
    public static int? Check(string synopsis, IReadOnlyList<Option> options, IReadOnlyList<string> args, TextWriter? output = null, TextWriter? error = null)
    {
        output ??= Console.Out;
        error ??= Console.Error;
        if (args.Any(a => a is "--help" or "-h" or "-?" or "/?"))
        {
            output.Write(Usage(synopsis, options));
            return 0;
        }
        for (var i = 0; i < args.Count; i++)
        {
            var option = options.FirstOrDefault(o => o.Name == args[i]);
            if (option is null)
            {
                error.WriteLine($"Unknown option '{args[i]}'. --help lists the options.");
                return 2;
            }
            if (option.Value is null)
                continue;
            if (i + 1 >= args.Count)
            {
                error.WriteLine($"{option.Name} needs a value: {option.Name} {option.Value}. --help lists the options.");
                return 2;
            }
            i++;
        }
        return null;
    }

    public static string Usage(string synopsis, IReadOnlyList<Option> options)
    {
        var text = new System.Text.StringBuilder();
        text.Append("usage: ").AppendLine(synopsis).AppendLine();
        var width = options.Max(o => o.Name.Length + (o.Value is null ? 0 : o.Value.Length + 1));
        foreach (var option in options)
        {
            var left = option.Value is null ? option.Name : option.Name + " " + option.Value;
            text.Append("  ").Append(left.PadRight(width + 2)).AppendLine(option.Help);
        }
        text.Append("  ").Append("--help".PadRight(width + 2)).AppendLine("Print this and exit.");
        return text.ToString();
    }
}
