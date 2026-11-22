using Dbsp.Tables;

namespace Dbsp.Tests;

/// <summary>
/// <see cref="CircuitBuilder.Describe"/> — the graph of a circuit read from its builder
/// (enrichment-service.md, C3, the graph half) — and
/// <see cref="TableSnapshot{TKey,TRow}.RowsBy"/> (C4): rows read through a declared index
/// at a pinned version.
/// </summary>
public class DescribeTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    private sealed record Listing(int Id, string Symbol, string Venue);

    [Fact]
    public async Task Describe_reports_nodes_in_order_with_their_inputs_names_and_outputs()
    {
        var table = new SharedTable<int, Listing>("listings", l => l.Id);
        var derived = new SharedTable<int, Listing>("copies", l => l.Id);
        var builder = new CircuitBuilder(new CircuitOptions { Workers = 2 });
        var quotes = builder.Input<(int Id, int Listing)>("quotes");
        var joined = quotes.GroupBy(q => q.Listing).Join(builder.Table(table), (_, q, l) => new Listing(q.Id, l.Symbol, l.Venue));
        var counted = quotes.GroupBy(q => q.Listing).Count().Named("per-listing");
        joined.ToOutput();
        counted.ToAcknowledgedOutput();
        joined.ToTable(derived);

        var graph = builder.Describe();

        Assert.Equal(Enumerable.Range(0, graph.Nodes.Count), graph.Nodes.Select(n => n.Id));
        Assert.All(graph.Nodes, n => Assert.All(n.Inputs, i => Assert.InRange(i, 0, n.Id - 1)));
        var input = Assert.Single(graph.Nodes, n => n.Kind == "input[quotes]");
        Assert.Empty(input.Inputs);
        var tableNode = Assert.Single(graph.Nodes, n => n.Kind == "table[listings]");
        Assert.Empty(tableNode.Inputs);
        // Two groupings on a sharded circuit: each sits behind an exchange.
        Assert.True(graph.Nodes.Count(n => n.IsExchange) >= 2);
        var named = Assert.Single(graph.Nodes, n => n.Name == "per-listing");
        Assert.StartsWith("named.per-listing.", named.StateName);
        Assert.All(graph.Nodes.Where(n => n.Name is null), n => Assert.StartsWith($"n{n.Id}.", n.StateName));

        Assert.Equal(
            [CircuitOutputKind.Stream, CircuitOutputKind.Acknowledged, CircuitOutputKind.Table],
            graph.Outputs.Select(o => o.Kind));
        Assert.Equal("copies", graph.Outputs[2].Table);
        Assert.Equal(named.Id, graph.Outputs[1].NodeId);
        Assert.Equal(graph.Outputs[0].NodeId, graph.Outputs[2].NodeId);
        Assert.Empty(graph.Loops);

        // Describing claims nothing: another builder can still take the derived table.
        var other = new CircuitBuilder();
        other.Input<Listing>("in").ToTable(derived);
        await using var circuit = other.Build();
        Assert.Equal(graph.Nodes.Count, builder.Describe().Nodes.Count);
    }

    [Fact]
    public void Describe_connects_a_lookup_to_the_table_it_probes()
    {
        var listings = new SharedTable<int, Listing>("listings", l => l.Id);
        var venues = new SharedTable<int, Listing>("venues", l => l.Id);
        var builder = new CircuitBuilder(new CircuitOptions { Workers = 2 });
        var quotes = builder.Input<(int Id, int Listing)>("quotes");
        var listingsRef = builder.Table(listings);
        var remembered = quotes.Lookup(listingsRef, q => q.Listing, (q, l) => l.Symbol);
        var appendOnly = quotes.LookupAppendOnly(builder.Table(venues), q => q.Id, (q, l) => l.Venue);
        var byIndex = quotes.LookupAppendOnly(listingsRef.By("symbol", l => l.Symbol), q => "A", (q, l) => l.Venue);
        var joined = quotes.GroupBy(q => q.Listing).Join(listingsRef, (_, q, l) => l.Symbol);
        remembered.ToOutput();
        appendOnly.ToOutput();
        byIndex.ToOutput();
        joined.ToOutput();

        var graph = builder.Describe();

        var listingsNode = Assert.Single(graph.Nodes, n => n.Kind == "table[listings]").Id;
        var venuesNode = Assert.Single(graph.Nodes, n => n.Kind == "table[venues]").Id;
        // A lookup probes: the table is no input of its execution, and is named as probed.
        var lookup = Assert.Single(graph.Nodes, n => n.Kind == "tableLookup");
        Assert.Equal([listingsNode], lookup.Probes);
        Assert.DoesNotContain(listingsNode, lookup.Inputs);
        var appends = graph.Nodes.Where(n => n.Kind == "tableLookupAppendOnly").ToList();
        Assert.Equal([venuesNode], appends[0].Probes);
        // Through an index it is still that table.
        Assert.Equal([listingsNode], appends[1].Probes);
        // A join consumes the table's changes: an input edge, and nothing probed.
        Assert.All(graph.Nodes.Where(n => !n.Kind.StartsWith("tableLookup", StringComparison.Ordinal)), n => Assert.Empty(n.Probes));
        Assert.Equal(graph.Nodes[graph.Outputs[0].NodeId].Kind, lookup.Kind);
    }

    [Fact]
    public void Describe_reports_a_recursion_region_and_the_circuit_error_policy()
    {
        var builder = new CircuitBuilder(new CircuitOptions { ErrorPolicy = RecordErrorPolicy.DeadLetter });
        var edges = builder.Input<(int From, int To)>("edges");
        var reach = edges.Recurse(r => r.GroupBy(e => e.To).Join(edges.GroupBy(e => e.From), (_, a, b) => (a.From, b.To)));
        var strict = reach.Select(e => e.From).OnError(RecordErrorPolicy.Fault);
        strict.ToOutput();

        var graph = builder.Describe();

        var loop = Assert.Single(graph.Loops);
        Assert.InRange(loop.ResultNode, loop.Start, loop.End);
        Assert.Equal(RecordErrorPolicy.Fault, graph.Nodes[graph.Outputs[0].NodeId].ErrorPolicy);
        Assert.Contains(graph.Nodes, n => n.ErrorPolicy == RecordErrorPolicy.DeadLetter);
    }

    [Fact]
    public async Task RowsBy_reads_the_rows_under_an_index_key_as_of_the_snapshot()
    {
        var table = new SharedTable<int, Listing>("listings", l => l.Id);
        table.AddIndex("symbol", l => l.Symbol);
        foreach (var row in new[] { new Listing(1, "A", "X"), new Listing(2, "A", "Y"), new Listing(3, "B", "X") })
            await table.Writer.UpsertAsync(row);
        await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);

        using var before = table.Snapshot();
        await table.Writer.UpsertAsync(new Listing(2, "B", "Y"));
        await table.Writer.DeleteAsync(1);
        await table.Writer.CommitAsync().AsTask().WaitAsync(Timeout);
        using var after = table.Snapshot();

        Assert.Equal([1, 2], before.RowsBy("symbol", "A").Select(r => r.Key).Order());
        Assert.Empty(after.RowsBy("symbol", "A"));
        Assert.Equal([2, 3], after.RowsBy("symbol", "B").Select(r => r.Key).Order());
        Assert.Equal("Y", after.RowsBy("symbol", "B").Single(r => r.Key == 2).Value.Venue);
        Assert.Empty(after.RowsBy("symbol", "none"));

        // A page of the rows under a key: the keys passed over are not read, and the
        // total is what the index holds under the key.
        var first = after.RowsBy("symbol", "B", 0, 1, out var total);
        var second = after.RowsBy("symbol", "B", 1, 1, out _);
        Assert.Equal(2, total);
        Assert.Equal([2, 3], first.Concat(second).Select(r => r.Key).Order());
        Assert.Empty(after.RowsBy("symbol", "B", 2, 5, out total));
        Assert.Equal(2, total);
        Assert.Empty(after.RowsBy("symbol", "none", 0, 5, out total));
        Assert.Equal(0, total);
        Assert.Throws<ArgumentOutOfRangeException>(() => after.RowsBy("symbol", "B", -1, 1, out _));

        Assert.Throws<ArgumentException>(() => after.RowsBy("venue", "X"));
        Assert.Throws<ArgumentException>(() => after.RowsBy("symbol", 1));
    }
}
