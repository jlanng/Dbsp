using Incremental.Tools;

namespace Incremental.Service.Tests;

/// <summary>
/// The option check the programs under tests/ and benchmarks/ share
/// (tests/Shared/CommandLine.cs): --help prints usage and ends with 0, anything the
/// program does not have ends with 2.
/// </summary>
public class CommandLineTests
{
    private static readonly Option[] Options = [new("--port", "N", "The port."), new("--read-only", null, "No mutations.")];

    private static (int? Exit, string Out, string Error) Check(params string[] args)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        return (CommandLine.Check("program [options]", Options, args, output, error), output.ToString(), error.ToString());
    }

    [Fact]
    public void Known_options_pass_and_print_nothing()
    {
        Assert.Equal((null, "", ""), Check());
        Assert.Equal((null, "", ""), Check("--port", "5180", "--read-only"));
        // A value is taken whole, even one that looks like an option or is empty.
        Assert.Equal((null, "", ""), Check("--port", "--read-only"));
        Assert.Equal((null, "", ""), Check("--read-only", "--port", ""));
    }

    [Fact]
    public void Help_prints_every_option_and_ends_with_zero()
    {
        foreach (var flag in new[] { "--help", "-h", "-?" })
        {
            var (exit, output, error) = Check("--port", "1", flag);
            Assert.Equal(0, exit);
            Assert.Equal("", error);
            Assert.Contains("usage: program [options]", output);
            Assert.Contains("--port N", output);
            Assert.Contains("--read-only", output);
        }
    }

    [Theory]
    [InlineData("--prot", "5180")]
    [InlineData("--read-only", "yes")]
    [InlineData("5180")]
    [InlineData("--port")]
    [InlineData("--read-only", "--port")]
    public void An_unknown_option_a_stray_value_or_a_missing_value_ends_with_two(params string[] args)
    {
        var (exit, output, error) = Check(args);
        Assert.Equal(2, exit);
        Assert.Equal("", output);
        Assert.Contains("--help", error);
    }
}
