namespace Incremental.Torture;

/// <summary>Bounded, one-microsecond histogram of completed steps in the measured window.
/// The lock only protects measurement boundaries against the circuit's meter callback.
/// Percentiles are upper bin edges; durations above one second are counted separately.</summary>
internal sealed class StepLatency
{
    private readonly object _gate = new();
    private readonly long[] _bins = new long[1_000_001];
    private bool _active;
    private long _count, _overflow;
    private double _seconds;

    public void Begin()
    {
        lock (_gate)
        {
            Array.Clear(_bins);
            _count = _overflow = 0;
            _seconds = 0;
            _active = true;
        }
    }

    public void Record(double seconds)
    {
        lock (_gate)
        {
            if (!_active) return;
            var us = Math.Ceiling(seconds * 1e6);
            if (us >= _bins.Length) _overflow++;
            _bins[(int)Math.Clamp(us, 0, _bins.Length - 1)]++;
            _count++;
            _seconds += seconds;
        }
    }

    public Snapshot End()
    {
        lock (_gate)
        {
            _active = false;
            double Quantile(double fraction)
            {
                var target = (long)Math.Ceiling(_count * fraction);
                long seen = 0;
                for (var i = 0; i < _bins.Length; i++)
                    if ((seen += _bins[i]) >= target) return i;
                return _bins.Length - 1;
            }
            return new(_count, _seconds, _seconds * 1e6 / Math.Max(1, _count),
                Quantile(0.5), Quantile(0.95), Quantile(0.99), _overflow);
        }
    }

    internal readonly record struct Snapshot(long Count, double Seconds, double MeanUs,
        double P50Us, double P95Us, double P99Us, long OverOneSecond);
}
