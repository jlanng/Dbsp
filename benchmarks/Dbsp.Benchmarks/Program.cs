using BenchmarkDotNet.Running;
using Dbsp.Benchmarks.Contracts;

// `--assert [name-filter...]` runs the CI performance contracts (scaling assertions with a
// nonzero exit code on failure) instead of the BenchmarkDotNet suite.
if (args.Length > 0 && args[0] == "--assert")
    return await PerfContracts.RunAsync(args[1..]);

// `--log-sizes` prints bytes per row of the durable logs' record format, raw and compressed.
if (args.Length > 0 && args[0] == "--log-sizes")
    return Dbsp.Benchmarks.LogSerializationBenchmarks.PrintSizes();

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
return 0;
