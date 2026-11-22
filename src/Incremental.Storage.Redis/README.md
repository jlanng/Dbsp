# Incremental.Storage.Redis

A Redis storage provider for [Incremental](https://www.nuget.org/packages/Incremental),
the incremental stream processing library for .NET. Operator state lives in Redis hashes,
so it survives restarts and can exceed the heap, and each checkpoint commits the state
changes and the input offsets they came from in one `MULTI`/`EXEC`: after a crash a
circuit resumes exactly where its last checkpoint left it.

```csharp
var redis = await ConnectionMultiplexer.ConnectAsync("localhost");

var builder = new CircuitBuilder(new CircuitOptions
{
    Storage = new RedisStorageProvider(redis, new RedisStorageOptions { KeyPrefix = "orders-prod" }),
});
```

`KeyPrefix` is required and must be unique per circuit sharing the Redis instance: two
circuits under one prefix would interleave their state.

State is serialized as JSON, and that is a persisted format. Changing the shape of a type
held in state invalidates what is stored for it: use a new key prefix and rebuild from
source, or keep state types stable.

Targets net8.0 and net10.0; depends on `StackExchange.Redis`. Use the same version of this
package as of `Incremental`.

## Documentation

- [Introduction, *Running a circuit*](https://github.com/jlanng/Incremental/blob/main/docs/intro.md#running-a-circuit)
- [README](https://github.com/jlanng/Incremental/blob/main/README.md): storage,
  checkpointing and exactly-once restart
