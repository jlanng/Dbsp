using Dbsp.Storage;

namespace Dbsp.Tests;

/// <summary>
/// The default provider's indexed weight store. State keyed by something row-like — a
/// remembered lookup's facts, a join against a unique key — is all groups of one value, so
/// what such a group costs is what an upserted record costs.
/// </summary>
public class InMemoryIndexedWeightStoreTests
{
    private static IIndexedWeightStore<int, (int Id, int Rev)> NewStore() =>
        new InMemoryStorageProvider().CreateIndexedWeightStore<int, (int Id, int Rev)>("store");

    [Fact]
    public void A_group_of_one_value_allocates_nothing_of_its_own()
    {
        const int keys = 256;
        var store = NewStore();
        var assert = Enumerable.Range(0, keys).Select(key => (key, (key, 1), 1L)).ToList();
        var retract = Enumerable.Range(0, keys).Select(key => (key, (key, 1), -1L)).ToList();
        var read = Enumerable.Range(0, keys).ToList();
        var into = new List<WeightedValue<(int Id, int Rev)>>(keys);
        var bounds = new List<int>(keys);

        void Cycle()
        {
            // The order an upsert-shaped stream produces across steps: every group of one
            // step created before any of them is emptied again.
            store.ApplyAll(assert);
            into.Clear();
            bounds.Clear();
            store.GetGroupsInto(read, into, bounds);
            store.ApplyAll(retract);
        }

        // Once to size the map and settle the JIT, then measured.
        for (var round = 0; round < 3; round++)
            Cycle();
        // The least of a few tries: a collection started by another test's threads while
        // this one measures retires this thread's allocation buffer, and the unused
        // remainder is then counted as allocated. A store that really allocates does so
        // on every try.
        var least = long.MaxValue;
        for (var attempt = 0; attempt < 5 && least != 0; attempt++)
        {
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var round = 0; round < 50; round++)
                Cycle();
            least = Math.Min(least, GC.GetAllocatedBytesForCurrentThread() - before);
        }
        Assert.Equal(0, least);
        Assert.Equal(keys, into.Count);
    }

    [Fact]
    public void Groups_read_back_exactly_as_written_however_they_grow_and_shrink()
    {
        // Against a flat model, over few enough keys and values that groups keep passing
        // through empty, one value and several.
        var random = new Random(7);
        var store = NewStore();
        var model = new Dictionary<(int Key, (int Id, int Rev) Value), long>();
        var batch = new List<(int Key, (int Id, int Rev) Value, long Delta)>();
        var into = new List<WeightedValue<(int Id, int Rev)>>();
        var bounds = new List<int>();
        var keys = Enumerable.Range(0, 6).ToList();

        void Apply(int key, (int Id, int Rev) value, long delta, long? returned)
        {
            var updated = model.GetValueOrDefault((key, value)) + delta;
            if (updated == 0)
                model.Remove((key, value));
            else
                model[(key, value)] = updated;
            if (returned is not null)
                Assert.Equal(updated, returned);
        }

        for (var round = 0; round < 4_000; round++)
        {
            var key = random.Next(keys.Count);
            var value = (random.Next(3), 0);
            var delta = random.Next(-2, 3);
            switch (random.Next(10))
            {
                case 0:
                    store.RemoveGroup(key);
                    foreach (var held in model.Keys.Where(k => k.Key == key).ToList())
                        model.Remove(held);
                    break;
                case < 5:
                    Apply(key, value, delta, store.Update(key, value, delta));
                    break;
                default:
                    batch.Clear();
                    for (var i = random.Next(1, 6); i > 0; i--)
                        batch.Add((random.Next(2) == 0 ? key : random.Next(keys.Count), (random.Next(3), 0), random.Next(-2, 3)));
                    store.ApplyAll(batch);
                    foreach (var (k, v, d) in batch)
                        Apply(k, v, d, null);
                    break;
            }

            into.Clear();
            bounds.Clear();
            store.GetGroupsInto(keys, into, bounds);
            for (var k = 0; k < keys.Count; k++)
            {
                var expected = model.Where(e => e.Key.Key == k).ToDictionary(e => e.Key.Value, e => e.Value);
                var start = k == 0 ? 0 : bounds[k - 1];
                Assert.Equal(expected, into.Skip(start).Take(bounds[k] - start).ToDictionary(e => e.Value, e => e.Weight));
                Assert.Equal(expected, store.GetGroup(k).ToDictionary(e => e.Value, e => e.Weight));
            }
        }
    }
}
