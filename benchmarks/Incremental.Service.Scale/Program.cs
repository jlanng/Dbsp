// The service's scale rig: `run` is the measured scenario (it starts the nodes itself);
// `node` is one of those nodes. See README.md.
using Incremental.Service.Scale;
using Incremental.Tools;

Option[] shared =
[
    new("--bootstrap", "host:port", "The Kafka bootstrap servers (localhost:9092)."),
    new("--prefix", "name", "The topic prefix (run: a fresh one each time)."),
    new("--replication", "N", "Replication factor of every topic (1)."),
    new("--takeover-after-ms", "N", "Silence after which the standby takes over (3000)."),
    new("--oauth", "principal", "Every client authenticates with SASL/OAUTHBEARER as this principal."),
    new("--oauth-lifetime-seconds", "N", "The life of one token (20)."),
    new("--log-level", "level", "The nodes' log level on stderr (Warning)."),
    new("--gc", "name", "The nodes' GC settings: node (the default, what the guide recommends), stock, or name=value pairs."),
];
Option[] run =
[
    new("--rows", "N", "Rows of the shared table (1,000,000)."),
    new("--state-keys", "N", "Orders of state in the stateful pipelines (1,000,000)."),
    new("--pipelines", "N", "Pipelines in all (20)."),
    new("--joins", "N", "Of them, keyed-order joins (2)."),
    new("--aggregates", "N", "Of them, per-party totals (2)."),
    new("--rate", "N", "Orders per second under steady load (2000)."),
    new("--churn", "N", "Changed table rows per second under steady load (500)."),
    new("--steady-seconds", "N", "Length of the steady load (30)."),
    new("--load-batch", "N", "Rows per table write while loading."),
    new("--single", null, "One node only: no standby, no failover."),
    new("--memory-only", null, "Stop after the steady load: memory figures, no failover."),
    new("--oauth-rig", "principal", "The rig's own clients use this principal instead of the nodes'."),
    new("--broker-container", "name", "The broker's container, to measure its topics' size on disk."),
    new("--broker-data", "dir", "The broker's log directory inside it (/tmp/kafka-logs)."),
    new("--keep-topics", null, "Leave the run's topics on the broker."),
    new("--out", "file.json", "Write the results here."),
];
Option[] node =
[
    new("--node", "id", "This node's id."),
    new("--probe-port", "N", "Where this node answers its peer's probe."),
    new("--peer-port", "N", "Where its peer answers."),
    new("--plugins", "dir", "The plugin directory."),
    new("--cache", "dir", "The plugin cache directory."),
    new("--heartbeat-ms", "N", "The leader's heartbeat interval (1000)."),
    new("--state-partitions", "N", "Partitions of the state log (12)."),
    new("--table-partitions", "N", "Partitions of a table's rows (4)."),
    new("--supervise", null, "Run the node as a child and start it again when it ends."),
];
const string Synopsis = "Incremental.Service.Scale run [options]      the measured scenario; it starts the nodes itself\n       Incremental.Service.Scale node [options]     one of those nodes (the rig starts these)";
if (args.Length == 0 || args[0] is not ("run" or "node"))
{
    var asked = args.Any(a => a is "--help" or "-h" or "-?");
    (asked ? Console.Out : Console.Error).Write(
        CommandLine.Usage(Synopsis, [.. shared, .. run]) + "\nnode:\n" + string.Join('\n', node.Select(o => $"  {o.Name}{(o.Value is null ? "" : " " + o.Value)}  {o.Help}")) + "\n");
    return asked ? 0 : 2;
}
if (CommandLine.Check(Synopsis, args[0] == "run" ? [.. shared, .. run] : [.. shared, .. node], args.Skip(1).ToList()) is { } exit)
    return exit;
var arguments = new Arguments(args.Skip(1));

// A node the rig starts is supervised, as a node in production is by its platform: this
// process starts the node as a child and starts it again when it ends fenced, resigned or
// killed (Incremental.Service.NodeSupervisor). The rig's stdin and stdout are the child's.
if (args[0] == "node" && arguments.Has("--supervise") && !Incremental.Service.NodeSupervisor.IsSupervised)
    return await Incremental.Service.NodeSupervisor.RunAsync(args);
return args[0] == "node" ? await NodeHost.RunAsync(arguments) : await new Rig(arguments).RunAsync();
