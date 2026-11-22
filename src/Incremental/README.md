# Incremental

An incremental stream processing library for .NET, built on the ideas of
[DBSP](https://arxiv.org/abs/2203.16684): every collection is a **Z-set** (values with
signed weights), every stream carries **deltas**, and every operator — joins and
aggregations included — processes only the changes, never the full state. An update that
touches one row costs work proportional to that row, not to the size of your data.

```csharp
var builder = new CircuitBuilder(new CircuitOptions { Workers = 4 });

var orders    = builder.Input<Order>("orders");
var customers = builder.Input<Customer>("customers");

var revenueDef = orders
    .Where(o => o.Amount > 0)
    .GroupBy(o => o.CustomerId)
    .Join(customers.GroupBy(c => c.Id),
          (_, order, customer) => new RegionAmount(customer.Region, order.Amount))
    .GroupBy(x => x.Region)
    .Sum(x => x.Amount)
    .ToOutput();

await using var circuit = builder.Build();
circuit.Start();

var ordersIn = circuit.Handle(orders);      // definitions redeem for typed handles
var revenue  = circuit.Handle(revenueDef);

await ordersIn.InsertAsync(new Order(101, custId: 1, amount: 60m));
await ordersIn.RemoveAsync(new Order(101, custId: 1, amount: 60m)); // cancelled: retracts downstream

await foreach (var batch in revenue.Changes())
    Console.WriteLine(batch); // e.g. {(emea, 40) => +1, (emea, 100) => -1}
```

The API is a fluent builder over delegates. It borrows LINQ's names where the semantics
match (`Select`, `Where`, `GroupBy`, `Distinct`) but is deliberately not LINQ: no
expression trees, no `IQueryable`.

## What is in the package

- Joins (inner, left, full, anti), `Distinct`, `TopK`, `Count`/`Sum`/`Min`/`Max` and custom
  aggregates, all incremental and all correct under retraction.
- Event time: watermarks, tumbling, hopping and session windows, emit-on-close, and
  eviction of state that is final.
- Recursive circuits (fixed points) for transitive-closure-style queries.
- Multi-worker execution in one process, and the same exchange across processes over TCP.
- Shared tables: reference data held once per process and joined by many circuits.
- Checkpoints of state together with input offsets, for exactly-once restart; acknowledged
  outputs and sinks for at-least-once delivery.
- Backpressure from bounded channels throughout, `ILogger` and `Meter` observability, and a
  per-record error policy with a dead-letter output.

Targets net8.0 and net10.0. Its one dependency is
`Microsoft.Extensions.Logging.Abstractions`.

## Related packages

| Package | What it adds |
| --- | --- |
| `Incremental.Storage.Redis` | Operator state and atomic checkpoints in Redis |
| `Incremental.Hosting` | Generic Host and dependency-injection integration |
| `Incremental.Kafka` | Durable logs, warm standby, sources, sinks and Debezium CDC on Kafka |

The packages are released together: use the same version of each.

## Documentation

- [Introduction](https://github.com/jlanng/Incremental/blob/main/docs/intro.md): the whole
  surface, from a first circuit to running one in production.
- [README](https://github.com/jlanng/Incremental/blob/main/README.md): concepts, the
  examples, benchmarks, and the known limitations.

This is a 0.x release: the API may change between minor versions.
