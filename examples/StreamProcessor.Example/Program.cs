using StreamProcessor;

// Live order analytics: join orders to customers, then keep per-region revenue and
// per-region max order size incrementally up to date — including under deletions
// (cancelled orders emit retractions that flow through join and aggregates).

var builder = new CircuitBuilder(new CircuitOptions { Workers = 2 });

var orders = builder.Input<Order>("orders");
var customers = builder.Input<Customer>("customers");

var enriched = orders
    .Filter(o => o.Amount > 0)
    .IndexBy(o => o.CustomerId)
    .Join(customers.IndexBy(c => c.Id), (_, order, customer) => new RegionAmount(customer.Region, order.Amount));

var byRegion = enriched.IndexBy(x => x.Region);
byRegion.Sum(x => x.Amount).Output("revenue");
byRegion.Max(x => x.Amount).Output("largest-order");

await using var circuit = builder.Build();
circuit.Start();

var ordersIn = circuit.Input<Order>("orders");
var customersIn = circuit.Input<Customer>("customers");
var revenue = circuit.Output<KeyedValue<string, decimal>>("revenue");
var largest = circuit.Output<KeyedValue<string, decimal>>("largest-order");

var printers = Task.WhenAll(
    PrintDeltasAsync("revenue", revenue),
    PrintDeltasAsync("largest-order", largest));

await customersIn.InsertAsync(new Customer(1, "emea"));
await customersIn.InsertAsync(new Customer(2, "apac"));

await ordersIn.InsertAsync(new Order(100, 1, 40m));
await ordersIn.InsertAsync(new Order(101, 1, 60m));
await ordersIn.InsertAsync(new Order(102, 2, 25m));

// Order 101 is cancelled: revenue drops and the max falls back to 40.
await ordersIn.RemoveAsync(new Order(101, 1, 60m));

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
