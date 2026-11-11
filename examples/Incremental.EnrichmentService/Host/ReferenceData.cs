using System.Text;
using System.Text.Json;
using Enrichment.Contracts;

namespace Incremental.EnrichmentService;

/// <summary>How much reference data there is, and how the trade feed is shaped. Everything
/// the example generates is a pure function of these numbers and a position, so a model of
/// what the pipelines should produce can be computed beside them.</summary>
public sealed record WorldShape
{
    public int Instruments { get; init; } = 2000;
    public int Counterparties { get; init; } = 300;
    public int Traders { get; init; } = 24;

    /// <summary>Trade ids run over this many values and then come round again: the feed's
    /// later records for an id amend the trade, so a host left running holds bounded state.</summary>
    public long TradeKeySpace { get; init; } = 20_000;

    /// <summary>Event time between one trade and the next.</summary>
    public TimeSpan TradeInterval { get; init; } = TimeSpan.FromMilliseconds(20);

    /// <summary>One trade in this many has no quantity: a record one of the definitions
    /// (watchlist-alerts) throws on and the others merely carry. 0: none.</summary>
    public int PoisonEvery { get; init; }

    /// <summary>Feed positions whose execution carries the time of the feed's first trade:
    /// a record far behind the watermark of any pipeline that has read what came before
    /// it. The smoke test puts one right behind a takeover.</summary>
    public HashSet<long> Stragglers { get; } = [];

    /// <summary>The length of the interval a reference quote is for.</summary>
    public TimeSpan QuoteInterval { get; init; } = TimeSpan.FromMinutes(1);

    public static readonly DateTimeOffset Epoch = new(2026, 1, 5, 9, 0, 0, TimeSpan.Zero);

    private static readonly (string Name, string Region)[] DeskSeed =
    [
        ("Equity Cash", "EMEA"), ("Equity Derivs", "AMER"), ("Macro", "EMEA"), ("Credit", "AMER"),
        ("Rates", "APAC"), ("FX Spot", "EMEA"), ("Commodities", "AMER"), ("Convertibles", "APAC"),
    ];
    private static readonly string[] FirstNames = ["Ada", "Grace", "Alan", "Edsger", "Barbara", "Donald", "Tony", "Leslie", "Radia", "Ken", "Dennis", "Margaret"];
    private static readonly string[] AssetClasses = ["Equity", "Govt Bond", "Corp Bond", "FX", "Commodity"];
    private static readonly string[] Countries = ["GB", "US", "DE", "FR", "JP", "SG", "CH", "NL"];
    private static readonly string[] Ratings = ["AAA", "AA", "A", "BBB", "BB"];
    private static readonly string[] Houses = ["Holdings", "Partners", "Capital", "Asset Management", "Securities"];

    public int Desks => DeskSeed.Length;

    public Desk Desk(int id) => new(id, DeskSeed[id - 1].Name, DeskSeed[id - 1].Region);

    public Trader Trader(int id) => new(id, $"{FirstNames[id % FirstNames.Length]} {(char)('A' + id % 26)}.", 1 + id % Desks);

    public Instrument Instrument(int id, long revision = 0) =>
        new(id, $"SYM{id:D5}", $"GB{id:D10}", AssetClasses[id % AssetClasses.Length], 1 + (id + revision) % 8 * 0.25m);

    public static string CounterpartyId(int number) => $"CP{number:D4}";

    public Counterparty Counterparty(int number, int revision = 0) => new(
        CounterpartyId(number),
        $"Counterparty {number:D4} {Houses[number % Houses.Length]}" + (revision == 0 ? "" : $" (renamed {revision})"),
        Countries[number % Countries.Length], Ratings[(number + revision) % Ratings.Length]);

    /// <summary>The feed's record at a position. About one trade in twenty names a
    /// counterparty that is in no table.</summary>
    public Trade TradeAt(long offset)
    {
        var r = Mix((ulong)offset);
        var instrument = 1 + (int)(r % (ulong)Instruments);
        r = Mix(r);
        var trader = 1 + (int)(r % (ulong)Traders);
        r = Mix(r);
        var counterparty = 1 + (int)(r % (ulong)(Counterparties + Counterparties / 20 + 1));
        r = Mix(r);
        var quantity = PoisonEvery > 0 && offset % PoisonEvery == PoisonEvery - 1 ? 0 : 100 * (1 + (long)(r % 500));
        r = Mix(r);
        // Within half a percent of the instrument's own level, so that a price can be
        // compared with the instrument's reference quote.
        var price = Math.Round(Level(instrument) * (1 + ((long)(r % 1001) - 500) / 100_000m), 2);
        return new Trade(
            offset % TradeKeySpace, instrument, trader, CounterpartyId(counterparty), quantity, price,
            Stragglers.Contains(offset) ? Epoch : Epoch + TimeSpan.FromTicks(TradeInterval.Ticks * offset));
    }

    /// <summary>The instrument feed's record at a position: first every instrument once,
    /// then revisions of one instrument at a time.</summary>
    public Instrument InstrumentChangeAt(long position) =>
        position < Instruments
            ? Instrument((int)position + 1)
            : Instrument(1 + (int)(Mix((ulong)position) % (ulong)Instruments), position);

    // ── the quote feed ─────────────────────────────────────────────────────────────────

    /// <summary>Three instruments in four are quoted in an interval, a different quarter
    /// left out each time: so some executions never meet a reference price.</summary>
    public int QuotesPerInterval => Instruments - Instruments / 4;

    private int Rotation => Instruments / 4 + 1;

    /// <summary>The quote feed's record at a position: interval after interval, each
    /// interval's quotes together and stamped with the interval's start.</summary>
    public Quote QuoteAt(long position)
    {
        var interval = position / QuotesPerInterval;
        var instrument = 1 + (int)((interval * Rotation + position % QuotesPerInterval) % Instruments);
        return new Quote(instrument, Epoch + QuoteInterval * interval, Mid(instrument, interval));
    }

    public bool Quoted(int instrument, long interval) =>
        ((instrument - 1 - interval * Rotation) % Instruments + Instruments) % Instruments < QuotesPerInterval;

    /// <summary>The reference price: within a fifth of a percent of the instrument's level.</summary>
    public decimal Mid(int instrument, long interval) =>
        Math.Round(Level(instrument) * (1 + ((long)(Mix((ulong)(instrument * 1_000_003L + interval)) % 401) - 200) / 100_000m), 4);

    /// <summary>What the instrument trades around: between 5 and 505.</summary>
    private static decimal Level(int instrument) => 5m + (long)(Mix((ulong)instrument * 7919) % 50_000) / 100m;

    /// <summary>Which quote interval an instant falls in.</summary>
    public long IntervalOf(DateTimeOffset at) => (at - Epoch).Ticks / QuoteInterval.Ticks;

    // ── the watchlists ─────────────────────────────────────────────────────────────────

    public IEnumerable<WatchlistEntry> Sanctions =>
        Enumerable.Range(1, Counterparties).Where(n => n % 11 == 0).Select(n => new WatchlistEntry(CounterpartyId(n), "sanctioned"));

    public IEnumerable<WatchlistEntry> CreditWatch =>
        Enumerable.Range(1, Counterparties).Where(n => n % 7 == 3).Select(n => new WatchlistEntry(CounterpartyId(n), "downgrade review"));

    // splitmix64: a position in, well-spread bits out, the same in every process.
    private static ulong Mix(ulong x)
    {
        x += 0x9E3779B97F4A7C15;
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EB;
        return x ^ (x >> 31);
    }
}

/// <summary>
/// A feed with positions: record <c>n</c> is a function of <c>n</c>, and <see cref="Published"/>
/// is how far the feed has got. It stands in for a topic or a vendor's sequenced stream —
/// a reader asks for what lies above the position it has, and nothing is buffered here.
/// </summary>
public sealed class PositionedFeed<T>(Func<long, T> at)
{
    private readonly object _lock = new();
    private long _published;
    private TaskCompletionSource _more = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public long Published => Volatile.Read(ref _published);

    public T At(long position) => at(position);

    public void Publish(long count = 1)
    {
        TaskCompletionSource signal;
        lock (_lock)
        {
            _published += count;
            signal = _more;
            _more = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        signal.TrySetResult();
    }

    /// <summary>Completes when there is a record at <paramref name="position"/>.</summary>
    public async Task WaitForAsync(long position, CancellationToken cancellationToken)
    {
        while (true)
        {
            Task more;
            lock (_lock)
            {
                if (_published > position)
                    return;
                more = _more.Task;
            }
            await more.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

/// <summary>
/// A source of truth with no positions: the current rows and a way to hear that some
/// changed. It stands in for a directory service or a database read through an API. It
/// keeps one row per key and a change counter, so a reader that falls behind catches up
/// from the rows themselves — there is no queue to overflow.
/// </summary>
public sealed class LiveDirectory<TKey, TRow>(Func<TRow, TKey> keyOf)
    where TKey : notnull
    where TRow : class
{
    private readonly object _lock = new();
    private readonly Dictionary<TKey, (TRow Row, long Change)> _rows = [];
    private long _change;
    private TaskCompletionSource _changed = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Upsert(TRow row)
    {
        TaskCompletionSource signal;
        lock (_lock)
        {
            _rows[keyOf(row)] = (row, ++_change);
            signal = _changed;
            _changed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        signal.TrySetResult();
    }

    public TRow? Get(TKey key)
    {
        lock (_lock)
            return _rows.TryGetValue(key, out var entry) ? entry.Row : null;
    }

    /// <summary>Every row, and the change counter they are as of.</summary>
    public (IReadOnlyList<TRow> Rows, long Change) Snapshot()
    {
        lock (_lock)
            return (_rows.Values.Select(e => e.Row).ToList(), _change);
    }

    /// <summary>The rows changed after <paramref name="change"/>, the counter they are as
    /// of, and a task that completes at the next change after that.</summary>
    public (IReadOnlyList<TRow> Rows, long Change, Task Next) Since(long change)
    {
        lock (_lock)
            return (_rows.Values.Where(e => e.Change > change).Select(e => e.Row).ToList(), _change, _changed.Task);
    }
}

/// <summary>The counterparties, as a JSON-lines file in a directory the <c>file</c> table
/// loader reads through an object store. Rewriting the file is how a counterparty changes:
/// the loader sees a new file and makes the table equal to it.</summary>
public sealed class CounterpartyFile
{
    public const string FileName = "counterparties.jsonl";
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly string _path;

    public CounterpartyFile(string directory, WorldShape shape, bool overwrite)
    {
        Directory.CreateDirectory(directory);
        _path = Path.Combine(directory, FileName);
        if (!overwrite && File.Exists(_path))
        {
            foreach (var line in File.ReadLines(_path).Where(l => l.Length > 0))
            {
                var row = JsonSerializer.Deserialize<Counterparty>(line, Json)!;
                Rows[row.Id] = row;
            }
            return;
        }
        for (var i = 1; i <= shape.Counterparties; i++)
            Rows[WorldShape.CounterpartyId(i)] = shape.Counterparty(i);
        Write();
    }

    public Dictionary<string, Counterparty> Rows { get; } = new(StringComparer.Ordinal);

    public void Replace(Counterparty row)
    {
        Rows[row.Id] = row;
        Write();
    }

    // Written beside and moved into place, so the loader never reads half a file.
    private void Write()
    {
        var text = new StringBuilder();
        foreach (var row in Rows.Values.OrderBy(r => r.Id, StringComparer.Ordinal))
            text.AppendLine(JsonSerializer.Serialize(row, Json));
        var temporary = _path + ".writing";
        File.WriteAllText(temporary, text.ToString());
        File.Move(temporary, _path, overwrite: true);
    }
}

/// <summary>A watchlist, as a JSON-lines file the <c>file</c> table loader reads. Like the
/// counterparties, it changes by being rewritten whole.</summary>
public sealed class WatchlistFile
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly string _path;

    public WatchlistFile(string directory, string fileName, IEnumerable<WatchlistEntry> entries, bool overwrite)
    {
        FileName = fileName;
        _path = Path.Combine(directory, fileName);
        if (!overwrite && File.Exists(_path))
        {
            foreach (var line in File.ReadLines(_path).Where(l => l.Length > 0))
            {
                var row = JsonSerializer.Deserialize<WatchlistEntry>(line, Json)!;
                Rows[row.CounterpartyId] = row;
            }
            return;
        }
        foreach (var entry in entries)
            Rows[entry.CounterpartyId] = entry;
        Write();
    }

    public string FileName { get; }
    public Dictionary<string, WatchlistEntry> Rows { get; } = new(StringComparer.Ordinal);

    public void Add(WatchlistEntry entry)
    {
        Rows[entry.CounterpartyId] = entry;
        Write();
    }

    private void Write()
    {
        var temporary = _path + ".writing";
        File.WriteAllLines(temporary, Rows.Values.OrderBy(r => r.CounterpartyId, StringComparer.Ordinal).Select(r => JsonSerializer.Serialize(r, Json)));
        File.Move(temporary, _path, overwrite: true);
    }
}
