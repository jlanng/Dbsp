namespace Incremental.Tests;

/// <summary>Manual tumbling-window assignment — the window-as-data escape hatch (windows
/// are just records produced by <c>SelectMany</c>) — shared by the tests that exercise the
/// generic keyed eviction/close mechanics rather than the paved windowed-grouping road.</summary>
internal static class TestWindows
{
    public static ZStream<Windowed<T>> Tumble<T>(this ZStream<T> stream, TimeSpan size, Func<T, DateTimeOffset> eventTime)
        where T : notnull =>
        stream.SelectMany(v =>
        {
            var start = eventTime(v).UtcTicks / size.Ticks * size.Ticks;
            return new[]
            {
                new Windowed<T>(
                    new DateTimeOffset(start, TimeSpan.Zero),
                    new DateTimeOffset(start + size.Ticks, TimeSpan.Zero),
                    v),
            };
        });
}
