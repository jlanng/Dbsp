// The service's scale rig: `run` is the measured scenario (it starts the nodes itself);
// `node` is one of those nodes. See README.md.
using Incremental.Service.Scale;

if (args.Length == 0 || args[0] is not ("run" or "node"))
{
    Console.Error.WriteLine(
        "usage: Incremental.Service.Scale run [--rows N] [--state-keys N] [--pipelines 20] [--joins 2] [--aggregates 2] [--rate 2000] " +
        "[--churn 500] [--steady-seconds 30] [--bootstrap host:port] [--replication 1] [--broker-container name] [--out file.json]");
    return 2;
}
var arguments = new Arguments(args.Skip(1));
return args[0] == "node" ? await NodeHost.RunAsync(arguments) : await new Rig(arguments).RunAsync();
