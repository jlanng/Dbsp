using Incremental;

// Live order analytics: join orders to customers, then keep per-region revenue and
// per-region max order size incrementally up to date. Both inputs are keyed: cancelling an
// order is DeleteAsync(id), and moving a customer to another region is one UpsertAsync —
// the handle retracts the old row itself, and every joined and aggregated result follows.

var builder = new CircuitBuilder(new CircuitOptions { Workers = 2 });

var orders = builder.KeyedInput<int, Order>("orders", o => o.OrderId);
var customers = builder.KeyedInput<int, Customer>("customers", c => c.Id);

var enriched = orders
    .Filter(o => o.Amount > 0)
    .IndexBy(o => o.CustomerId)
    .Join(customers.IndexBy(c => c.Id), (_, order, customer) => new RegionAmount(customer.Region, order.Amount));

var byRegion = enriched.IndexBy(x => x.Region);
byRegion.Sum(x => x.Amount).Output("revenue");
byRegion.Max(x => x.Amount).Output("largest-order");

await using var circuit = builder.Build();
circuit.Start();

var ordersIn = circuit.KeyedInput<int, Order>("orders");
var customersIn = circuit.KeyedInput<int, Customer>("customers");
var revenue = circuit.Output<KeyedValue<string, decimal>>("revenue");
var largest = circuit.Output<KeyedValue<string, decimal>>("largest-order");

var printers = Task.WhenAll(
    PrintDeltasAsync("revenue", revenue),
    PrintDeltasAsync("largest-order", largest));

await customersIn.UpsertAsync(new Customer(1, "emea"));
await customersIn.UpsertAsync(new Customer(2, "apac"));

await ordersIn.UpsertAsync(new Order(100, 1, 40m));
await ordersIn.UpsertAsync(new Order(101, 1, 60m));
await ordersIn.UpsertAsync(new Order(102, 2, 25m));

// The pump batches whatever is buffered into each step; pausing between phases keeps them
// in separate steps so their individual deltas are visible below.
await Task.Delay(100);

// Order 101 is cancelled: revenue drops and the max falls back to 40.
await ordersIn.DeleteAsync(101);
await Task.Delay(100);

// Customer 1 relocates to apac: their remaining order re-attributes — one upsert retracts
// the emea row and re-asserts it under apac, and both aggregates follow.
await customersIn.UpsertAsync(new Customer(1, "apac"));
await Task.Delay(100);

ordersIn.Complete();
customersIn.Complete();
await printers;
await circuit.DisposeAsync();

static async Task PrintDeltasAsync<T>(string name, OutputHandle<T> output) where T : notnull
{
    await foreach (var delta in output.ReadAllAsync())
        foreach (var (value, weight) in delta)
            Console.WriteLine($"[{name}] {value} {weight:+#;-#;0}");
}

record Order(int OrderId, int CustomerId, decimal Amount);

record Customer(int Id, string Region);

record RegionAmount(string Region, decimal Amount);
