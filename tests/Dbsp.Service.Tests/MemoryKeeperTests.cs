using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging.Abstractions;
using Ageing = Dbsp.Service.MemoryKeeper.CollectorAgeing;

namespace Dbsp.Service.Tests;

/// <summary>The node gives back what it holds and does not use: the C allocator's free
/// memory, and — by asking a quiet process for young collections — what the collector
/// kept from a burst. When it asks, and when it stops asking, is decided apart from any
/// heap, and tested so.</summary>
public class MemoryKeeperTests
{
    private const long Gb = 1L << 30, Mb = 1L << 20;
    private static readonly DateTimeOffset T0 = new(2026, 11, 12, 9, 0, 0, TimeSpan.Zero);

    [Fact]
    public void The_collector_is_asked_only_while_it_holds_well_over_what_is_live_and_collects_seldom()
    {
        // Under a quarter over what is live, or under 256 MB over: its own business.
        Assert.Equal(0, new Ageing().Decide(committedBytes: 4 * Gb + 900 * Mb, heapBytes: 4 * Gb, collectionsSince: 0, T0));
        Assert.Equal(0, new Ageing().Decide(300 * Mb, 100 * Mb, 0, T0));
        Assert.Equal(0, new Ageing().Decide(100 * Mb, 300 * Mb, 0, T0));

        // Well over, and quiet: enough collections for a region to be let go.
        Assert.Equal(Ageing.CollectionsToRelease, new Ageing().Decide(5 * Gb, 2 * Gb, 0, T0));
        Assert.Equal(25, new Ageing().Decide(5 * Gb, 2 * Gb, Ageing.CollectionsToRelease - 1, T0));
        Assert.Equal(25, new Ageing().Decide(2 * Gb + 600 * Mb, 2 * Gb, 3, T0));

        // Well over, and collecting by itself: the collector has had its chances.
        Assert.Equal(0, new Ageing().Decide(5 * Gb, 2 * Gb, Ageing.CollectionsToRelease, T0));
        Assert.Equal(0, new Ageing().Decide(5 * Gb, 2 * Gb, 400, T0));
    }

    [Fact]
    public void Asking_that_does_not_help_is_rested_longer_each_time_and_begins_afresh_once_the_excess_is_gone()
    {
        var ageing = new Ageing();
        var now = T0;
        Assert.Equal(25, ageing.Decide(5 * Gb, 2 * Gb, 0, now));

        // It helped (a tenth of the excess or more went): ask again while there is excess.
        now += TimeSpan.FromSeconds(30);
        Assert.Equal(25, ageing.Decide(5 * Gb - 300 * Mb, 2 * Gb, 0, now));

        // It did not: a minute's rest, and nothing asked within it.
        now += TimeSpan.FromSeconds(30);
        Assert.Equal(0, ageing.Decide(5 * Gb - 300 * Mb - 10 * Mb, 2 * Gb, 0, now));
        Assert.Equal(0, ageing.Decide(5 * Gb - 310 * Mb, 2 * Gb, 0, now + Ageing.FirstRest - TimeSpan.FromSeconds(1)));
        now += Ageing.FirstRest;
        Assert.Equal(25, ageing.Decide(5 * Gb - 310 * Mb, 2 * Gb, 0, now));

        // Nor the second time: two minutes; then four, eight, and never more than ten.
        var rests = new List<TimeSpan>();
        for (var round = 0; round < 6; round++)
        {
            Assert.Equal(0, ageing.Decide(5 * Gb - 310 * Mb, 2 * Gb, 0, now));
            var rested = TimeSpan.Zero;
            while (ageing.Decide(5 * Gb - 310 * Mb, 2 * Gb, 0, now + rested) == 0)
                rested += TimeSpan.FromSeconds(30);
            rests.Add(rested);
            now += rested;
        }
        Assert.Equal([2, 4, 8, 10, 10, 10], rests.Select(r => (int)r.TotalMinutes));

        // The excess goes (the heap grew into it): the next burst is a first time again.
        now += TimeSpan.FromSeconds(30);
        Assert.Equal(0, ageing.Decide(5 * Gb, 4 * Gb + 500 * Mb, 0, now));
        Assert.Equal(25, ageing.Decide(9 * Gb, 4 * Gb, 0, now + TimeSpan.FromSeconds(30)));
        Assert.Equal(25, ageing.Decide(8 * Gb, 4 * Gb, 0, now + TimeSpan.FromSeconds(60)));
    }

    [DllImport("libc.so.6")]
    private static extern nint malloc(nuint size);

    [DllImport("libc.so.6")]
    private static extern void free(nint block);

    [Fact]
    public async Task What_the_c_allocator_holds_free_goes_back_to_the_operating_system()
    {
        Assert.SkipUnless(OperatingSystem.IsLinux() && File.Exists("/proc/self/status"), "glibc's allocator is what is trimmed");
        try
        {
            free(malloc(16));
        }
        catch (DllNotFoundException)
        {
            Assert.Skip("not glibc");
        }

        // What the Kafka client does at a restore: a great many blocks under the size the
        // allocator maps on its own, written to, then freed — all but a few, so that the
        // arena cannot simply shrink from its top.
        const int blocks = 4000, size = 64 * 1024;
        var held = new nint[blocks];
        for (var i = 0; i < blocks; i++)
        {
            held[i] = malloc(size);
            for (var offset = 0; offset < size; offset += 4096)
                Marshal.WriteByte(held[i], offset, 1);
        }
        for (var i = 0; i < blocks; i++)
        {
            if (i % 200 != 0)
                free(held[i]);
        }

        var keeper = new MemoryKeeper();
        var before = ProcessMemory.AnonymousResidentBytes()!.Value;
        await keeper.RunAsync(NullLogger.Instance, "test", default);
        var after = ProcessMemory.AnonymousResidentBytes()!.Value;
        for (var i = 0; i < blocks; i += 200)
            free(held[i]);

        // 250 MB freed; other tests allocate beside this one, so most of it is asked for.
        Assert.NotNull(keeper.Last);
        Assert.True(keeper.Last.NativeReturnedBytes > 150 * Mb, $"returned {keeper.Last.NativeReturnedBytes / Mb} MB; resident {before / Mb} MB before, {after / Mb} MB after");
        Assert.Equal(keeper.Last.NativeReturnedBytes, keeper.Last.NativeReturnedBytesTotal);

        // Nothing more to return: nothing is asked of the allocator, and the total stands.
        await keeper.RunAsync(NullLogger.Instance, "test", default);
        Assert.InRange(keeper.Last.NativeReturnedBytes, 0, 20 * Mb);
        Assert.True(keeper.Last.NativeReturnedBytesTotal >= 150 * Mb);
    }

    [Fact]
    public async Task The_keeper_runs_no_more_often_than_asked_one_run_at_a_time_and_not_at_all_when_turned_off()
    {
        var keeper = new MemoryKeeper();
        await keeper.RunIfDueAsync(TimeSpan.Zero, NullLogger.Instance, "test", default);
        Assert.Null(keeper.Last);

        await keeper.RunIfDueAsync(TimeSpan.FromMilliseconds(1), NullLogger.Instance, "test", default);
        var first = keeper.Last;
        Assert.NotNull(first);

        // Within the interval: not again.
        await keeper.RunIfDueAsync(TimeSpan.FromMinutes(5), NullLogger.Instance, "test", default);
        Assert.Same(first, keeper.Last);

        await Task.Delay(50);
        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => keeper.RunIfDueAsync(TimeSpan.FromMilliseconds(20), NullLogger.Instance, "test", default)));
        Assert.NotSame(first, keeper.Last);
    }

    [Fact]
    public async Task A_node_looks_after_its_memory_by_itself_and_says_what_it_did()
    {
        await using var service = await TestService.StartAsync(options: o => o.ReleaseUnusedMemoryEvery = TimeSpan.FromMilliseconds(200));
        await TestService.Eventually(() => service.Node.MemoryRelease is not null, "the node's first look at its memory");
        var first = service.Node.MemoryRelease!;
        await TestService.Eventually(() => service.Node.MemoryRelease!.At > first.At, "its next look");
    }
}
