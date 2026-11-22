namespace Dbsp.Kafka.Tests;

/// <summary>
/// The rule that makes a table restore one version rather than a mixture, against simulated
/// partitions: every interleaving a consumer could produce, including records that arrive
/// from a partition after it was paused. No broker needed.
/// </summary>
public class SnapshotCutTests
{
    private readonly record struct Record(long Offset, long Version, bool IsHead);

    /// <summary>One rows partition: for each version some rows, then the head, then an
    /// offset with nothing readable behind it (the transaction's marker).</summary>
    private static List<Record> Partition(Random random, int versions)
    {
        var records = new List<Record>();
        long offset = 0;
        for (var version = 0; version <= versions; version++)
        {
            for (var rows = version == 0 ? 0 : random.Next(4); rows > 0; rows--)
                records.Add(new Record(offset++, version, IsHead: false));
            records.Add(new Record(offset++, version, IsHead: true));
            offset++;
        }
        return records;
    }

    private static long EndAfter(List<Record> partition, long version) =>
        partition.First(r => r.IsHead && r.Version == version).Offset + 2;

    [Fact]
    public void Every_interleaving_ends_with_all_partitions_at_one_version_at_or_past_their_ends()
    {
        for (var seed = 0; seed < 20_000; seed++)
        {
            var random = new Random(seed);
            var partitionCount = 1 + random.Next(4);
            var versions = 1 + random.Next(12);
            var logs = Enumerable.Range(0, partitionCount).Select(_ => Partition(random, versions)).ToArray();

            // The ends are asked for one partition at a time, so they need not agree.
            var endVersions = logs.Select(_ => (long)random.Next(versions + 1)).ToArray();
            var cut = new SnapshotCut(logs.Select((log, p) => EndAfter(log, endVersions[p])).ToArray());

            var cursor = new int[partitionCount];
            var paused = new bool[partitionCount];
            var stragglers = new int[partitionCount];   // already fetched when the pause took effect
            var endReported = new bool[partitionCount];
            var applied = new long[partitionCount];     // the version of the last record applied
            var afterHead = new bool[partitionCount];

            while (!cut.Done)
            {
                var runnable = Enumerable.Range(0, partitionCount)
                    .Where(p => cursor[p] < logs[p].Count && (!paused[p] || stragglers[p] > 0)).ToList();
                if (runnable.Count > 0)
                {
                    var p = runnable[random.Next(runnable.Count)];
                    if (paused[p])
                        stragglers[p]--;
                    var record = logs[p][cursor[p]++];
                    applied[p] = record.Version;
                    afterHead[p] = record.IsHead;
                    if (record.IsHead)
                        cut.Head(p, record.Offset, record.Version);
                    else
                        cut.Row(p, record.Offset);
                }
                else
                {
                    // Nothing more can arrive: the writer has stopped, and every running
                    // partition is at its end, which the client now reports.
                    var atEnd = Enumerable.Range(0, partitionCount).Where(p => !paused[p] && !endReported[p]).ToList();
                    Assert.True(atEnd.Count > 0, $"seed {seed}: the cut never completed and nothing is left to read");
                    foreach (var p in atEnd)
                    {
                        endReported[p] = true;
                        cut.End(p);
                    }
                }

                for (var p = 0; p < partitionCount; p++)
                {
                    if (cut.ShouldPause(p) && !paused[p])
                        stragglers[p] = random.Next(3);
                    paused[p] = cut.ShouldPause(p);
                }
            }

            // One version everywhere, each partition exactly at that version's head, and
            // nothing committed before the restore began left out.
            Assert.True(afterHead.All(h => h), $"seed {seed}: a partition stopped inside a version");
            Assert.True(applied.All(v => v == cut.Target), $"seed {seed}: versions {string.Join(",", applied)} at target {cut.Target}");
            Assert.True(cut.Target >= endVersions.Max(), $"seed {seed}: stopped at {cut.Target}, before an end at {endVersions.Max()}");
        }
    }

    [Fact]
    public void A_partition_whose_tail_is_an_aborted_transaction_is_settled_by_the_end_event()
    {
        // The end is past records that will never be seen: only the client's report of the
        // end of the partition can say the partition has been read.
        var cut = new SnapshotCut([2, 10]);
        cut.Head(0, 0, 0);
        cut.Head(1, 0, 0);
        Assert.True(cut.ShouldPause(0));
        Assert.False(cut.ShouldPause(1));
        Assert.False(cut.Done);
        cut.End(1);
        Assert.True(cut.Done);
        Assert.Equal(0, cut.Target);
    }
}
