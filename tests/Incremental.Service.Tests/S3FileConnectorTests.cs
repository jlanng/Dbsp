using System.Net.Sockets;
using System.Text;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Acme.Contracts;
using Incremental.Service.S3;
using static Incremental.Service.Tests.SupervisorTests;

namespace Incremental.Service.Tests;

/// <summary>
/// The files connector's tests (<see cref="FileConnectorTests"/>) on <see cref="S3ObjectStore"/>
/// against the connector stack's MinIO (<c>docker compose --profile connectors up -d</c>;
/// <c>INCREMENTAL_S3_ENDPOINT</c> points them elsewhere), each test under a key prefix of
/// its own in one bucket. They skip when MinIO is not reachable. And what is S3's alone: a
/// write of several parts, the page S3 lists, the identity of an object rewritten.
/// MinIO is S3's API, not Amazon S3: nothing here checks AWS itself.
/// </summary>
public sealed class S3FileConnectorTests : FileConnectorTests
{
    private const string Bucket = "incremental-tests";
    private static readonly string Endpoint = Environment.GetEnvironmentVariable("INCREMENTAL_S3_ENDPOINT") ?? "http://localhost:9000";
    private static readonly Lazy<bool> Reachable = new(() =>
    {
        var uri = new Uri(Endpoint);
        using var socket = new TcpClient();
        try
        {
            return socket.ConnectAsync(uri.Host, uri.Port).Wait(TimeSpan.FromSeconds(2)) && socket.Connected;
        }
        catch (AggregateException)
        {
            return false;
        }
    });

    private readonly string _prefix = $"files/{Guid.NewGuid():N}/";
    private AmazonS3Client? _client;
    private S3ObjectStore? _store;

    private static AmazonS3Client Client() => new(
        new BasicAWSCredentials(
            Environment.GetEnvironmentVariable("INCREMENTAL_S3_ACCESS_KEY") ?? "incremental",
            Environment.GetEnvironmentVariable("INCREMENTAL_S3_SECRET_KEY") ?? "incremental-secret"),
        new AmazonS3Config { ServiceURL = Endpoint, ForcePathStyle = true, AuthenticationRegion = "us-east-1" });

    private S3ObjectStoreOptions Options => new() { Bucket = Bucket, Prefix = _prefix, PartSize = 5 * 1024 * 1024 };

    protected override async ValueTask<IObjectStore> OpenStoreAsync()
    {
        if (!Reachable.Value)
            Assert.Skip($"MinIO is not reachable on {Endpoint} — run `docker compose --profile connectors up -d`.");
        _client = Client();
        try
        {
            await _client.PutBucketAsync(new PutBucketRequest { BucketName = Bucket });
        }
        catch (AmazonS3Exception ex) when (ex.ErrorCode is "BucketAlreadyOwnedByYou" or "BucketAlreadyExists")
        {
        }
        return _store = new S3ObjectStore(_client, Options);
    }

    protected override async ValueTask CloseStoreAsync()
    {
        if (_client is null)
            return;
        try
        {
            var keys = new List<KeyVersion>();
            await foreach (var info in _store!.ListAsync("", default))
                keys.Add(new KeyVersion { Key = _prefix + info.Path });
            foreach (var chunk in keys.Chunk(1000))
                await _client.DeleteObjectsAsync(new DeleteObjectsRequest { BucketName = Bucket, Objects = [.. chunk] });
        }
        catch (AmazonServiceException)
        {
        }
        _client.Dispose();
    }

    protected override async ValueTask<List<string>> NamesUnderAsync(string prefix)
    {
        var names = new List<string>();
        var request = new ListObjectsV2Request { BucketName = Bucket, Prefix = _prefix + prefix };
        ListObjectsV2Response response;
        do
        {
            response = await _client!.ListObjectsV2Async(request);
            names.AddRange((response.S3Objects ?? []).Select(o => o.Key[_prefix.Length..]));
            request.ContinuationToken = response.NextContinuationToken;
        }
        while (response.IsTruncated == true);
        // An upload begun and neither completed nor aborted is what a bucket holds of a
        // write that did not finish: a name here, for a test that asks what is left.
        // MinIO lists uploads only for a prefix that is a whole key, so each name this
        // test class has written under the prefix is asked for by itself.
        foreach (var path in _written.Where(p => p.StartsWith(prefix, StringComparison.Ordinal)).Distinct())
        {
            if (await UploadsOfAsync(path) > 0)
                names.Add(path + " (upload not completed)");
        }
        names.Sort(StringComparer.Ordinal);
        return names;
    }

    private readonly List<string> _written = [];

    /// <summary>The multipart uploads begun under the path's own key and not finished.</summary>
    private async Task<int> UploadsOfAsync(string path)
    {
        var uploads = await _client!.ListMultipartUploadsAsync(new ListMultipartUploadsRequest { BucketName = Bucket, Prefix = _prefix + path });
        return (uploads.MultipartUploads ?? []).Count(u => u.Key == _prefix + path);
    }

    protected override async ValueTask AgeAsync(TimeSpan age, params string[] names) => await Task.Delay(age);

    // ── what is S3's alone ─────────────────────────────────────────────────────────────

    private static byte[] Block(int size, byte fill)
    {
        var block = new byte[size];
        Array.Fill(block, fill);
        return block;
    }

    [Fact]
    public async Task A_write_of_several_parts_is_one_object_when_it_returns_and_nothing_before()
    {
        const int mib = 1024 * 1024;
        _written.Add("out/big.bin");
        await Store.WriteAsync("out/big.bin", async (stream, ct) =>
        {
            for (var i = 0; i < 12; i++)
            {
                await stream.WriteAsync(Block(mib, (byte)('a' + i)), ct);
                if (i == 10)
                {
                    // Two parts sent: S3 holds an upload, and no object.
                    Assert.Null(await Store.StatAsync("out/big.bin", ct));
                    Assert.Empty((await Store.ListPageAsync("out/", null, 10, ct)).Objects);
                    Assert.Equal(["out/big.bin (upload not completed)"], await NamesUnderAsync("out/"));
                }
            }
        }, default);

        var info = await Store.StatAsync("out/big.bin", default);
        Assert.Equal(12L * mib, info!.Length);
        await using var read = await Store.OpenReadAsync("out/big.bin", default);
        var buffer = new byte[mib];
        for (var i = 0; i < 12; i++)
        {
            await read.ReadExactlyAsync(buffer);
            Assert.All(buffer, b => Assert.Equal((byte)('a' + i), b));
        }
        Assert.Equal(0, await read.ReadAsync(buffer));
        Assert.Equal(["out/big.bin"], await NamesUnderAsync("out/"));
    }

    [Fact]
    public async Task A_write_of_several_parts_that_fails_leaves_the_name_as_it_was_and_no_upload_behind()
    {
        _written.AddRange(["out/a.jsonl", "out/b.jsonl"]);
        await PutAsync("out/a.jsonl", ["whole"]);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await Store.WriteAsync("out/a.jsonl", async (stream, ct) =>
        {
            for (var i = 0; i < 11; i++)
                await stream.WriteAsync(Block(1024 * 1024, (byte)'x'), ct);
            throw new InvalidOperationException("a row that does not serialize");
        }, default));
        using var cancel = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await Store.WriteAsync("out/b.jsonl", async (stream, ct) =>
        {
            await stream.WriteAsync(Block(6 * 1024 * 1024, (byte)'y'), ct);
            cancel.Cancel();
            ct.ThrowIfCancellationRequested();
        }, cancel.Token));

        Assert.Equal("whole\n", await ReadTextAsync("out/a.jsonl"));
        Assert.Null(await Store.StatAsync("out/b.jsonl", default));
        Assert.Equal(["out/a.jsonl"], await NamesUnderAsync("out/"));
    }

    [Fact]
    public async Task A_page_is_one_listing_after_a_name_and_says_whether_more_follow()
    {
        for (var i = 1; i <= 25; i++)
            await PutAsync($"in/{i:D4}.jsonl", ["{}"]);
        await PutAsync("inside/x.jsonl", ["{}"]);
        var first = await Store.ListPageAsync("in/", null, 10, default);
        Assert.Equal(Enumerable.Range(1, 10).Select(i => $"in/{i:D4}.jsonl"), first.Objects.Select(o => o.Path));
        Assert.True(first.More);
        var last = await Store.ListPageAsync("in/", "in/0020.jsonl", 5, default);
        Assert.Equal(Enumerable.Range(21, 5).Select(i => $"in/{i:D4}.jsonl"), last.Objects.Select(o => o.Path));
        Assert.False(last.More);
        Assert.Empty((await Store.ListPageAsync("in/", "in/0025.jsonl", 5, default)).Objects);
        // After a name that is not there, and with more asked for than S3 gives in one listing.
        var between = await Store.ListPageAsync("in/", "in/0012.jsonl.zzz", 5000, default);
        Assert.Equal(Enumerable.Range(13, 13).Select(i => $"in/{i:D4}.jsonl"), between.Objects.Select(o => o.Path));
        Assert.False(between.More);
    }

    [Fact]
    public async Task An_object_written_again_with_the_same_length_is_another_object()
    {
        // The source's settle rule tells an object changed under its name by its identity:
        // a directory's is the length and the time, a bucket's the ETag.
        await PutAsync("in/a.jsonl", ["one"]);
        var before = await Store.StatAsync("in/a.jsonl", default);
        await PutAsync("in/a.jsonl", ["two"]);
        var after = await Store.StatAsync("in/a.jsonl", default);
        Assert.Equal(before!.Length, after!.Length);
        Assert.NotEqual(before.Identity, after.Identity);
        var listed = Assert.Single((await Store.ListPageAsync("in/", null, 10, default)).Objects);
        Assert.Equal(after.Identity, listed.Identity);
        Assert.Equal(TimeSpan.Zero, listed.Modified.Offset);
    }

    [Fact]
    public async Task Two_stores_on_one_bucket_under_different_prefixes_see_only_their_own()
    {
        await PutAsync("in/a.jsonl", ["mine"]);
        using var other = new S3ObjectStore(_client!, new S3ObjectStoreOptions { Bucket = Bucket, Prefix = _prefix + "neighbour/" });
        await other.WriteAsync("in/a.jsonl", (s, ct) => s.WriteAsync(Encoding.UTF8.GetBytes("theirs\n"), ct), default);
        Assert.Equal("mine\n", await ReadTextAsync("in/a.jsonl"));
        Assert.Equal(["in/a.jsonl"], (await other.ListPageAsync("", null, 10, default)).Objects.Select(o => o.Path));
        Assert.Throws<ArgumentException>(() => new S3ObjectStore(_client!, new S3ObjectStoreOptions { Bucket = Bucket, Prefix = "no-slash" }));
        await Assert.ThrowsAsync<FileNotFoundException>(async () => await Store.OpenReadAsync("in/missing.jsonl", default));
    }

    // ── the host's registration ────────────────────────────────────────────────────────

    [Fact]
    public async Task A_store_registered_by_its_options_is_what_a_files_slot_reads_from()
    {
        // The registration a host writes, pointed at MinIO: the client is the store's own.
        await PutAsync("in/0001.jsonl", Trades(0, 50));
        await using var service = await TestService.StartAsync(configure: b => b.AddS3ObjectStore("bucket", new S3ObjectStoreOptions
        {
            Bucket = Bucket,
            Prefix = _prefix,
            ServiceUrl = Endpoint,
            ForcePathStyle = true,
            Credentials = new BasicAWSCredentials(
                Environment.GetEnvironmentVariable("INCREMENTAL_S3_ACCESS_KEY") ?? "incremental",
                Environment.GetEnvironmentVariable("INCREMENTAL_S3_SECRET_KEY") ?? "incremental-secret"),
        }));
        await service.DeployAsync(TestService.Spec("counts", "counts", null, "counts") with
        {
            Inputs = new Dictionary<string, System.Text.Json.JsonElement>
            {
                ["trades"] = TestService.Json("""{ "connector": "files", "store": "bucket", "prefix": "in/", "pollInterval": "00:00:00.05", "settle": "00:00:00" }"""),
            },
        });
        var sink = service.Memory.UpsertSink<string, long>("counts");
        var expected = Counts.Model(Trades(0, 50));
        await TestService.Eventually(() => sink.Rows.Count == expected.Count && expected.All(e => sink.Rows.TryGetValue(e.Key, out var n) && n == e.Value), "the object counted");
        Assert.Contains(service.Node.Discovery.Connections, c => c is { Name: "bucket", Kind: "object-store" });
    }

    [Fact]
    public async Task The_guide_s_registration_makes_a_store_without_asking_AWS_anything()
    {
        // What the guide shows for production. Nothing here reaches AWS: the store and its
        // client are made when a slot first names the store, and making them sends nothing.
        await using var service = await TestService.StartAsync(configure: builder =>
        {
            // <snippet:s3-store>
            builder.AddS3ObjectStore("lake", new S3ObjectStoreOptions
            {
                Bucket = "acme-lake",
                Prefix = "enrichment/",        // the store's root: a spec's "quotes/" is the key enrichment/quotes/…
                Region = "eu-west-2",
                // Credentials left out: the SDK's default chain, which on EKS is the pod's role.
            });
            // </snippet:s3-store>
        });
        var store = Assert.IsType<S3ObjectStore>(service.Node.Context.ObjectStore("lake"));
        Assert.Equal(("acme-lake", "enrichment/"), (store.Bucket, store.Prefix));
    }
}
