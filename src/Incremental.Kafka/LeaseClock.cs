using System.Runtime.InteropServices;

namespace Incremental.Kafka;

/// <summary>
/// The clock the delivery lease is measured on: elapsed milliseconds that keep counting
/// while the machine is suspended. A lease is only worth something if the time its holder
/// spends frozen counts against it. <see cref="Environment.TickCount64"/> is
/// <c>CLOCK_MONOTONIC</c> on Linux, which stops during a suspend, so a leader that slept
/// past its lease would wake still holding it; <c>CLOCK_BOOTTIME</c> does not stop. On
/// Windows <c>TickCount64</c> already counts sleep. Elsewhere, and if the call fails, this
/// falls back to <c>TickCount64</c>.
/// <para>
/// What no local clock covers is a hypervisor that stops the guest's clocks while the
/// guest is paused: there the lease is no protection, and only the term on the delivery
/// is (durability-and-standby.md, <i>The sink gap</i>).
/// </para>
/// </summary>
internal static class LeaseClock
{
    private const int ClockBoottime = 7;

    private static readonly bool Boottime = OperatingSystem.IsLinux() && TryBoottime(out _);

    /// <summary>Whether the clock counts time spent suspended.</summary>
    public static bool CountsSuspendedTime => Boottime || OperatingSystem.IsWindows();

    public static long Milliseconds => Boottime && TryBoottime(out var now) ? now : Environment.TickCount64;

    [StructLayout(LayoutKind.Sequential)]
    private struct Timespec
    {
        public nint Seconds;
        public nint Nanoseconds;
    }

    [DllImport("libc", EntryPoint = "clock_gettime")]
    private static extern int ClockGettime(int clock, out Timespec time);

    private static bool TryBoottime(out long milliseconds)
    {
        try
        {
            if (ClockGettime(ClockBoottime, out var time) == 0)
            {
                milliseconds = (long)time.Seconds * 1000 + (long)time.Nanoseconds / 1_000_000;
                return true;
            }
        }
        catch (Exception ex) when (ex is DllNotFoundException or EntryPointNotFoundException)
        {
        }
        milliseconds = 0;
        return false;
    }
}
