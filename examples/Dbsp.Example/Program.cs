using Dbsp;

// Live order analytics: join orders to customers, then keep per-region revenue and
// per-region max order size incrementally up to date. Both inputs are keyed: cancelling an
// order is DeleteAsync(id), and moving a customer to another region is one UpsertAsync —
// the circuit retracts the old row itself, and every joined and aggregated result follows.

var builder = new CircuitBuilder(new CircuitOptions { Workers = 2 });

var orders = builder.KeyedInput<int, Order>("orders", o => o.OrderId);
var customers = builder.KeyedInput<int, Customer>("customers", c => c.Id);

var enriched = orders
    .Where(o => o.Amount > 0)
    .GroupBy(o => o.CustomerId)
    .Join(customers.GroupBy(c => c.Id), (_, order, customer) => new RegionAmount(customer.Region, order.Amount));

var byRegion = enriched.GroupBy(x => x.Region);
// bootstrap: false — these consumers print raw deltas rather than maintaining state, so
// they want the pure changefeed, not a resync batch first.
var revenue = byRegion.Sum(x => x.Amount).ToOutput(bootstrap: false);
var largest = byRegion.Max(x => x.Amount).ToOutput(bootstrap: false);

await using var circuit = builder.Build();
circuit.Start();

var ordersIn = circuit.Handle(orders);
var customersIn = circuit.Handle(customers);
var revenueOut = circuit.Handle(revenue);
var largestOut = circuit.Handle(largest);

var printers = Task.WhenAll(
    PrintDeltasAsync("revenue", revenueOut),
    PrintDeltasAsync("largest-order", largestOut));

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
    await foreach (var delta in output.Changes())
        foreach (var (value, weight) in delta)
            Console.WriteLine($"[{name}] {value} {weight:+#;-#;0}");
}

record Order(int OrderId, int CustomerId, decimal Amount);

record Customer(int Id, string Region);

record RegionAmount(string Region, decimal Amount);
