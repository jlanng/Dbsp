using System.Buffers;
using System.Diagnostics;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using Incremental.Cluster;
using K4os.Compression.LZ4;
using ZstdSharp;

namespace Incremental.Benchmarks;

/// <summary>
/// What the durable logs' record format costs: JSON behind a format byte, compressed by the
/// producer (docs/design/durability-and-standby.md, <em>Decided (1)</em>), against the dense
/// codec the cluster's exchange uses (<c>ShardCodec</c>) — the measurement that decision
/// asked for before <c>Incremental.Kafka</c> is called stable.
/// <para>
/// Two row shapes: a trade (a fact: ids, numbers, an instant) and a wide reference row
/// (an instrument: mostly short strings). The BenchmarkDotNet methods measure encode and
/// decode per row. <c>-- --log-sizes</c> prints bytes per row, raw and compressed in
/// batches as a producer would, with a quick timing of each step.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class LogSerializationBenchmarks
{
    public enum Side { Buy, Sell }

    public sealed record Trade(
        long TradeId, int InstrumentId, string Account, string Trader, Side Side, long Quantity, decimal Price,
        string Currency, string Venue, DateTimeOffset ExecutedAt, DateOnly SettleDate, string OrderRef);

    public sealed record Instrument(
        int Id, string Symbol, string Isin, string Name, string Currency, string Exchange, string Sector, string Country,
        decimal TickSize, decimal LotSize, decimal Multiplier, DateOnly Listed, DateOnly? Expiry, bool Active,
        string Issuer, string Category, int RiskBucket, long SharesOutstanding, string Notes);

    public static Trade MakeTrade(int i) => new(
        9_000_000_000L + i, i % 5000, $"ACC-{i % 800:D5}", $"trader{i % 120}", i % 2 == 0 ? Side.Buy : Side.Sell,
        100 * (1 + i % 50), 100m + (i % 9973) / 100m, (i % 3) switch { 0 => "USD", 1 => "EUR", _ => "GBP" },
        (i % 4) switch { 0 => "XNYS", 1 => "XNAS", 2 => "XLON", _ => "XETR" },
        new DateTimeOffset(2026, 11, 10, 9, 30, 0, TimeSpan.Zero).AddMilliseconds(i * 37L), new DateOnly(2026, 11, 12),
        $"ORD-{i * 7919L % 100_000_000:D8}");

    public static Instrument MakeInstrument(int id) => new(
        id, $"SYM{id:D6}", $"US{id:D10}", $"Instrument number {id} common stock", (id % 3) switch { 0 => "USD", 1 => "EUR", _ => "GBP" },
        (id % 4) switch { 0 => "XNYS", 1 => "XNAS", 2 => "XLON", _ => "XETR" }, $"sector-{id % 11}", $"C{id % 40:D2}",
        0.01m, 100m, 1m, new DateOnly(1990 + id % 30, 1 + id % 12, 1 + id % 28), id % 7 == 0 ? new DateOnly(2030, 12, 31) : null,
        id % 13 != 0, $"Issuer {id % 500} Holdings plc", $"category-{id % 17}", id % 9, 1_000_000L * (1 + id % 97), "revision 0");

    // The state serializer's options: fields included, so tuples round-trip.
    private static readonly JsonSerializerOptions Json = new() { IncludeFields = true };

    private const int Rows = 1000;
    private Trade[] _trades = null!;
    private Instrument[] _instruments = null!;
    private byte[][] _tradeJson = null!;
    private byte[][] _instrumentJson = null!;
    private byte[][] _tradeDense = null!;
    private byte[][] _instrumentDense = null!;
    private readonly ArrayBufferWriter<byte> _buffer = new(4096);
    private Utf8JsonWriter _writer = null!;
    private readonly WireWriter _wire = new();
    private readonly WireReader _wireReader = new();

    [GlobalSetup]
    public void Setup()
    {
        _trades = Enumerable.Range(0, Rows).Select(MakeTrade).ToArray();
        _instruments = Enumerable.Range(0, Rows).Select(MakeInstrument).ToArray();
        _tradeJson = _trades.Select(t => JsonSerializer.SerializeToUtf8Bytes(t, Json)).ToArray();
        _instrumentJson = _instruments.Select(i => JsonSerializer.SerializeToUtf8Bytes(i, Json)).ToArray();
        _tradeDense = _trades.Select(Dense).ToArray();
        _instrumentDense = _instruments.Select(Dense).ToArray();
        _writer = new Utf8JsonWriter(_buffer);
    }

    internal static byte[] Dense<T>(T value)
    {
        var writer = new WireWriter();
        WireCodec<T>.Write!(writer, value);
        return writer.ToArray();
    }

    // Each method handles 1000 rows, so a reported time in microseconds reads as
    // nanoseconds per row.

    [Benchmark]
    public int Json_encode_trade()
    {
        var bytes = 0;
        foreach (var trade in _trades)
        {
            _buffer.ResetWrittenCount();
            _writer.Reset(_buffer);
            JsonSerializer.Serialize(_writer, trade, Json);
            bytes += _buffer.WrittenCount;
        }
        return bytes;
    }

    [Benchmark]
    public int Json_decode_trade()
    {
        var sum = 0;
        foreach (var json in _tradeJson)
            sum += JsonSerializer.Deserialize<Trade>(json, Json)!.InstrumentId;
        return sum;
    }

    [Benchmark]
    public int Dense_encode_trade()
    {
        var bytes = 0;
        foreach (var trade in _trades)
        {
            _wire.Reset();
            WireCodec<Trade>.Write!(_wire, trade);
            bytes += _wire.Length;
        }
        return bytes;
    }

    [Benchmark]
    public int Dense_decode_trade()
    {
        var sum = 0;
        foreach (var dense in _tradeDense)
        {
            _wireReader.Reset(dense);
            sum += WireCodec<Trade>.Read!(_wireReader).InstrumentId;
        }
        return sum;
    }

    [Benchmark]
    public int Json_encode_instrument()
    {
        var bytes = 0;
        foreach (var instrument in _instruments)
        {
            _buffer.ResetWrittenCount();
            _writer.Reset(_buffer);
            JsonSerializer.Serialize(_writer, instrument, Json);
            bytes += _buffer.WrittenCount;
        }
        return bytes;
    }

    [Benchmark]
    public int Json_decode_instrument()
    {
        var sum = 0;
        foreach (var json in _instrumentJson)
            sum += JsonSerializer.Deserialize<Instrument>(json, Json)!.Id;
        return sum;
    }

    [Benchmark]
    public int Dense_encode_instrument()
    {
        var bytes = 0;
        foreach (var instrument in _instruments)
        {
            _wire.Reset();
            WireCodec<Instrument>.Write!(_wire, instrument);
            bytes += _wire.Length;
        }
        return bytes;
    }

    [Benchmark]
    public int Dense_decode_instrument()
    {
        var sum = 0;
        foreach (var dense in _instrumentDense)
        {
            _wireReader.Reset(dense);
            sum += WireCodec<Instrument>.Read!(_wireReader).Id;
        }
        return sum;
    }

    // ── bytes per row ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Prints, for each row shape and encoding, the bytes per row as written (key and
    /// value of the table's row record) and as a producer's compressed batch would hold
    /// them, with the time the compressor took. A batch here is the records' keys and
    /// values laid end to end, 1000 rows at a time, compressed whole — close to what
    /// librdkafka does with a record batch, without the per-record framing (about 7 bytes a
    /// record before compression). zstd at level 3 and lz4 at its fast level are the
    /// client's defaults; the compressors here are managed ports, so their times are a
    /// guide, not librdkafka's. Encode and decode cost is the BenchmarkDotNet methods' job.
    /// The rows are synthetic and regular (sequential ids, a few repeated strings), which
    /// flatters every compressed number.
    /// </summary>
    public static int PrintSizes()
    {
        const int Count = 100_000, Batch = 1000;
        Console.WriteLine($"Durable log record format: bytes per row ({Count:N0} rows, compressed in batches of {Batch:N0}).");
        Console.WriteLine();
        Console.WriteLine($"{"shape",-12}{"encoding",-8}{"raw B/row",10}{"zstd B/row",12}{"lz4 B/row",11}{"zstd ns/row",13}{"lz4 ns/row",12}");

        Report("trade", Enumerable.Range(0, Count).Select(MakeTrade).ToArray(), t => t.TradeId);
        Report("instrument", Enumerable.Range(0, Count).Select(MakeInstrument).ToArray(), i => i.Id);
        return 0;

        static void Report<T, TKey>(string shape, T[] rows, Func<T, TKey> key) where T : class
        {
            // JSON, as KafkaTableLog writes a row record: key = the key's JSON,
            // value = format byte + {"v":version,"r":row}.
            var buffer = new ArrayBufferWriter<byte>(4096);
            using var writer = new Utf8JsonWriter(buffer);
            byte[] JsonRecord(T row, long version)
            {
                buffer.ResetWrittenCount();
                buffer.GetSpan(1)[0] = 1;
                buffer.Advance(1);
                writer.Reset(buffer);
                writer.WriteStartObject();
                writer.WriteNumber("v"u8, version);
                writer.WritePropertyName("r"u8);
                JsonSerializer.Serialize(writer, row, Json);
                writer.WriteEndObject();
                writer.Flush();
                return buffer.WrittenSpan.ToArray();
            }
            T JsonRow(byte[] record)
            {
                var reader = new Utf8JsonReader(record.AsSpan(1));
                reader.Read();
                reader.Read();
                reader.Read();
                reader.Read();
                reader.Read();
                return JsonSerializer.Deserialize<T>(ref reader, Json)!;
            }

            // Dense: the same record with the row in the exchange's codec — what a second
            // persisted format would hold.
            var wire = new WireWriter();
            var wireReader = new WireReader();
            byte[] DenseRecord(T row, long version)
            {
                wire.Reset();
                wire.WriteByte(2);
                WireCodec<long>.Write!(wire, version);
                WireCodec<T>.Write!(wire, row);
                return wire.ToArray();
            }
            T DenseRow(byte[] record)
            {
                wireReader.Reset(record);
                WireCodec<byte>.Read!(wireReader);
                WireCodec<long>.Read!(wireReader);
                return WireCodec<T>.Read!(wireReader);
            }

            var keys = rows.Select(r => JsonSerializer.SerializeToUtf8Bytes(key(r), Json)).ToArray();
            Line(shape, "json", rows, keys, JsonRecord, JsonRow);
            if (WireCodec<T>.Write is null)
                Console.WriteLine($"{shape,-12}{"dense",-8}  (no dense codec for this type)");
            else
                Line(shape, "dense", rows, keys, DenseRecord, DenseRow);
        }

        static void Line<T>(string shape, string encoding, T[] rows, byte[][] keys, Func<T, long, byte[]> encode, Func<byte[], T> decode)
        {
            var records = new byte[rows.Length][];
            for (var i = 0; i < rows.Length; i++)
            {
                records[i] = encode(rows[i], i);
                if (i % 1000 == 0 && !Equals(decode(records[i]), rows[i]))
                    throw new InvalidOperationException($"{shape}/{encoding}: a row did not round-trip.");
            }

            var clock = new Stopwatch();
            long raw = 0, zstd = 0, lz4 = 0;
            double zstdNs = 0, lz4Ns = 0;
            using var compressor = new Compressor(3);
            var batch = new MemoryStream();
            for (var start = 0; start < rows.Length; start += Batch)
            {
                batch.SetLength(0);
                for (var i = start; i < start + Batch; i++)
                {
                    batch.Write(keys[i]);
                    batch.Write(records[i]);
                }
                var bytes = batch.GetBuffer().AsSpan(0, (int)batch.Length);
                raw += bytes.Length;

                clock.Restart();
                zstd += compressor.Wrap(bytes).Length;
                zstdNs += clock.Elapsed.TotalNanoseconds;

                var target = new byte[LZ4Codec.MaximumOutputSize(bytes.Length)];
                clock.Restart();
                lz4 += LZ4Codec.Encode(bytes, target, LZ4Level.L00_FAST);
                lz4Ns += clock.Elapsed.TotalNanoseconds;
            }

            double PerRow(double total) => total / rows.Length;
            Console.WriteLine(
                $"{shape,-12}{encoding,-8}{PerRow(raw),10:F0}{PerRow(zstd),12:F0}{PerRow(lz4),11:F0}{PerRow(zstdNs),13:F0}{PerRow(lz4Ns),12:F0}");
        }
    }
}
