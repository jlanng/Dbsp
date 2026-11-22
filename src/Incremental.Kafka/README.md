# Incremental.Kafka

Kafka for [Incremental](https://www.nuget.org/packages/Incremental), the incremental
stream processing library for .NET:

- **Durable logs.** The state changelog and shared-table logs on compacted topics, written
  through one transactional producer per process (group commit, `transactional.id`
  fencing).
- **Warm standby.** A second process follows the logs and takes over when the leader goes
  silent (`KafkaLeadership`).
- **Sources and sinks.** `KafkaSource`, `KafkaUpsertSink`, `KafkaAppendSink`, and a decoder
  for Debezium change events. A source's position is part of the circuit's checkpoint, not
  a consumer group's.

```csharp
await using var log = await KafkaLog.ConnectAsync(new KafkaLogOptions
{
    BootstrapServers = "localhost:9092",
    TopicPrefix = "enrichment",
    ReplicationFactor = 1,                       // 3 by default, for a real cluster
});
await log.AcquireWriterAsync();                  // the active; a standby does not call this until takeover

await using var storage = await ChangelogStorage.OpenAsync(log.State);
var builder = new CircuitBuilder(new CircuitOptions { Storage = storage.Pipeline("orders-by-region") });
var ordersInput = builder.Input<Order>("orders");
// … the dataflow …
await using var circuit = builder.Build();
circuit.Start();

await using var orders = await KafkaSource.StartAsync(circuit, ordersInput,
    new KafkaSourceOptions { BootstrapServers = "localhost:9092", Topic = "orders" });
```

`KafkaLogOptions.ClientConfig` passes any librdkafka setting to every client (TLS, SASL).

Targets net8.0 and net10.0; depends on `Confluent.Kafka`. Use the same version of this
package as of `Incremental`.

## Documentation

- [Introduction, *Running a circuit*](https://github.com/jlanng/Incremental/blob/main/docs/intro.md#running-a-circuit):
  the logs, a standby, sources and sinks
- [Durability and warm standby](https://github.com/jlanng/Incremental/blob/main/docs/design/durability-and-standby.md):
  the design, what a failover guarantees and what it does not
- [README](https://github.com/jlanng/Incremental/blob/main/README.md): known limitations
