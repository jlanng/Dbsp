using System.Net;
using System.Runtime.CompilerServices;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

namespace Incremental.Service.S3;

/// <summary>Where an <see cref="S3ObjectStore"/> keeps its objects and how it reaches
/// them. Set by the host; a spec names only the store's registered name and a path.</summary>
public sealed record S3ObjectStoreOptions
{
    /// <summary>The bucket. It must exist: the store creates nothing.</summary>
    public required string Bucket { get; init; }

    /// <summary>A key prefix every path is put under, the store's root: <c>landing/</c>
    /// makes the path <c>in/a.jsonl</c> the key <c>landing/in/a.jsonl</c>. Empty: the
    /// bucket's top. A non-empty prefix ends with <c>/</c>.</summary>
    public string Prefix { get; init; } = "";

    /// <summary>An endpoint other than AWS's (MinIO, a VPC endpoint). Null: AWS's endpoint
    /// for <see cref="Region"/>.</summary>
    public string? ServiceUrl { get; init; }

    /// <summary>The bucket's region. Null: the SDK's own resolution (environment,
    /// profile, instance metadata), or <c>us-east-1</c> beside a <see cref="ServiceUrl"/>.</summary>
    public string? Region { get; init; }

    /// <summary>Path-style addressing (<c>host/bucket/key</c>), which MinIO needs.</summary>
    public bool ForcePathStyle { get; init; }

    /// <summary>Null: the SDK's default chain — on EKS the pod's role (IRSA or Pod
    /// Identity), which is how production is meant to authenticate. Given: these.</summary>
    public AWSCredentials? Credentials { get; init; }

    /// <summary>How much of a write is held before it goes as one part of a multipart
    /// upload. An object no larger is one <c>PutObject</c>. At least 5 MiB, S3's least part.</summary>
    public int PartSize { get; init; } = 8 * 1024 * 1024;
}

/// <summary>
/// An <see cref="IObjectStore"/> over an S3 bucket, for the <c>files</c> connector
/// (<c>AddS3ObjectStore</c>). What the connector needs of a store, and how S3 gives it:
/// <list type="bullet">
/// <item>A page of names after a name: one <c>ListObjectsV2</c> with <c>StartAfter</c> and
/// <c>MaxKeys</c>. S3 lists in the order of the keys' UTF-8 bytes, which is ordinal order
/// for every name without characters beyond U+FFFF beside ones from U+E000; the page is
/// put in ordinal order whatever comes back.</item>
/// <item>A write that is whole or absent: S3 shows an object only once its <c>PutObject</c>
/// or <c>CompleteMultipartUpload</c> has finished. A write that throws sends neither, and a
/// multipart upload begun for it is aborted.</item>
/// <item>The settle rule's stamps: an object's <c>LastModified</c> as the store's listing
/// reports it, compared only with other objects' stamps. An object's stamp may be when its
/// upload began rather than when it could be listed (S3 is understood to stamp a multipart
/// upload with its start), so a writer whose uploads take a long time needs a
/// <c>settle</c> that covers them.</item>
/// </list>
/// Tested against MinIO (the connector stack's <c>minio</c> service), never against
/// Amazon S3: authentication by IAM, S3's own consistency and its stamps' precision there
/// are not checked (decision 7 of the round-five plan).
/// </summary>
public sealed class S3ObjectStore : IObjectStore, IDisposable
{
    private const int LeastPart = 5 * 1024 * 1024;
    private readonly IAmazonS3 _client;
    private readonly bool _ownsClient;
    private readonly string _bucket;
    private readonly string _root;
    private readonly int _partSize;

    public S3ObjectStore(S3ObjectStoreOptions options)
        : this(CreateClient(options ?? throw new ArgumentNullException(nameof(options))), options, ownsClient: true)
    {
    }

    /// <summary>A store over a client the host made and keeps: the store does not dispose it.</summary>
    public S3ObjectStore(IAmazonS3 client, S3ObjectStoreOptions options)
        : this(client, options, ownsClient: false)
    {
    }

    private S3ObjectStore(IAmazonS3 client, S3ObjectStoreOptions options, bool ownsClient)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrEmpty(options.Bucket);
        if (options.Prefix.Length > 0 && (!options.Prefix.EndsWith('/') || options.Prefix.StartsWith('/')))
            throw new ArgumentException($"An S3 store's prefix ends with '/' and does not start with one, not '{options.Prefix}'.", nameof(options));
        if (options.PartSize < LeastPart)
            throw new ArgumentOutOfRangeException(nameof(options), $"PartSize is at least {LeastPart} bytes, S3's least part.");
        _client = client;
        _ownsClient = ownsClient;
        _bucket = options.Bucket;
        _root = options.Prefix;
        _partSize = options.PartSize;
    }

    private static AmazonS3Client CreateClient(S3ObjectStoreOptions options)
    {
        var config = new AmazonS3Config { ForcePathStyle = options.ForcePathStyle };
        if (options.ServiceUrl is { } url)
        {
            config.ServiceURL = url;
            config.AuthenticationRegion = options.Region ?? "us-east-1";
        }
        else if (options.Region is { } region)
            config.RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(region);
        return options.Credentials is { } credentials ? new AmazonS3Client(credentials, config) : new AmazonS3Client(config);
    }

    public string Bucket => _bucket;
    public string Prefix => _root;

    /// <summary>The key of a path, refused as the local store refuses it: absolute, or with
    /// a segment that climbs (<c>..</c>) — a key has no directories to climb out of, but a
    /// path that tries means the same mistake on either store.</summary>
    private string Key(string path, bool prefix = false)
    {
        if (!prefix)
            ArgumentException.ThrowIfNullOrEmpty(path);
        ArgumentNullException.ThrowIfNull(path);
        if (path.StartsWith('/') || path.Contains('\\'))
            throw new ArgumentException($"Path '{path}' is absolute; an object store takes paths relative to its root.", nameof(path));
        foreach (var segment in path.Split('/'))
        {
            if (segment is "..")
                throw new ArgumentException($"Path '{path}' resolves outside the object store's root.", nameof(path));
        }
        return _root + path;
    }

    private string PathOf(string key) => key[_root.Length..];

    private static bool NotFound(AmazonS3Exception ex) =>
        ex.StatusCode == HttpStatusCode.NotFound || ex.ErrorCode is "NoSuchKey" or "NotFound";

    public async ValueTask<ObjectInfo?> StatAsync(string path, CancellationToken cancellationToken)
    {
        var key = Key(path);
        try
        {
            var head = await _client.GetObjectMetadataAsync(new GetObjectMetadataRequest { BucketName = _bucket, Key = key }, cancellationToken).ConfigureAwait(false);
            return new ObjectInfo(path, head.ContentLength, Stamp(head.LastModified), head.ETag);
        }
        catch (AmazonS3Exception ex) when (NotFound(ex))
        {
            return null;
        }
    }

    private static DateTimeOffset Stamp(DateTime? modified) =>
        modified is { } m ? new DateTimeOffset(m.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(m, DateTimeKind.Utc) : m.ToUniversalTime()) : DateTimeOffset.MinValue;

    public async ValueTask<Stream> OpenReadAsync(string path, CancellationToken cancellationToken)
    {
        var key = Key(path);
        GetObjectResponse response;
        try
        {
            response = await _client.GetObjectAsync(new GetObjectRequest { BucketName = _bucket, Key = key }, cancellationToken).ConfigureAwait(false);
        }
        catch (AmazonS3Exception ex) when (NotFound(ex))
        {
            // What the connector takes for an object gone between its listing and its read.
            throw new FileNotFoundException($"No object '{path}' in bucket '{_bucket}'.", path, ex);
        }
        return new ResponseStream(response);
    }

    public async ValueTask WriteAsync(string path, Func<Stream, CancellationToken, ValueTask> write, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(write);
        var upload = new UploadStream(_client, _bucket, Key(path), _partSize);
        try
        {
            await write(upload, cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            await upload.CompleteAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Half an object is no object: nothing was put, and parts sent are dropped.
            await upload.AbortAsync().ConfigureAwait(false);
            throw;
        }
    }

    public async ValueTask DeleteAsync(string path, CancellationToken cancellationToken) =>
        await _client.DeleteObjectAsync(new DeleteObjectRequest { BucketName = _bucket, Key = Key(path) }, cancellationToken).ConfigureAwait(false);

    public async IAsyncEnumerable<ObjectInfo> ListAsync(string prefix, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var request = new ListObjectsV2Request { BucketName = _bucket, Prefix = Key(prefix, prefix: true) };
        while (true)
        {
            var response = await _client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);
            foreach (var entry in response.S3Objects ?? [])
                yield return Info(entry);
            if (response.IsTruncated != true || string.IsNullOrEmpty(response.NextContinuationToken))
                yield break;
            request.ContinuationToken = response.NextContinuationToken;
        }
    }

    /// <summary>One <c>ListObjectsV2</c>: the keys after <paramref name="after"/>, at most
    /// <paramref name="limit"/> of them, and whether S3 has more.</summary>
    public async ValueTask<ObjectPage> ListPageAsync(string prefix, string? after, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var request = new ListObjectsV2Request
        {
            BucketName = _bucket,
            Prefix = Key(prefix, prefix: true),
            MaxKeys = Math.Min(limit, 1000),
            StartAfter = after is null ? null : Key(after),
        };
        var response = await _client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);
        var objects = new List<ObjectInfo>(response.S3Objects?.Count ?? 0);
        foreach (var entry in response.S3Objects ?? [])
            objects.Add(Info(entry));
        objects.Sort(static (a, b) => string.CompareOrdinal(a.Path, b.Path));
        return new ObjectPage(objects, response.IsTruncated == true);
    }

    private ObjectInfo Info(S3Object entry) => new(PathOf(entry.Key), entry.Size ?? 0, Stamp(entry.LastModified), entry.ETag);

    public void Dispose()
    {
        if (_ownsClient)
            _client.Dispose();
    }

    /// <summary>The body of a <c>GetObject</c>, which goes with its response.</summary>
    private sealed class ResponseStream(GetObjectResponse response) : Stream
    {
        private readonly Stream _inner = response.ResponseStream;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => response.ContentLength;
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
        public override int Read(Span<byte> buffer) => _inner.Read(buffer);
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => _inner.ReadAsync(buffer, offset, count, cancellationToken);
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => _inner.ReadAsync(buffer, cancellationToken);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _inner.Dispose();
                response.Dispose();
            }
            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// What a write fills. It holds up to a part; an object that never grows past one is
    /// one <c>PutObject</c> when the write returns, and a larger one becomes a multipart
    /// upload whose parts go as they fill and which is completed only when the write
    /// returns. Disposing it (a <c>StreamWriter</c> over it may) sends nothing and keeps
    /// what it holds: only <see cref="CompleteAsync"/> makes the object.
    /// </summary>
    internal sealed class UploadStream(IAmazonS3 client, string bucket, string key, int partSize) : Stream
    {
        private MemoryStream _buffer = new();
        private string? _uploadId;
        private readonly List<PartETag> _parts = [];
        private long _written;
        private bool _finished;

        public bool Multipart => _uploadId is not null;

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => !_finished;
        public override long Length => _written;
        public override long Position { get => _written; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            // A writer that does not await: it waits for the part it fills here.
            while (buffer.Length > 0)
            {
                var take = Append(buffer);
                buffer = buffer[take..];
                if (_buffer.Length >= partSize)
                    SendPartAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();
            }
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            while (buffer.Length > 0)
            {
                var take = Append(buffer.Span);
                buffer = buffer[take..];
                if (_buffer.Length >= partSize)
                    await SendPartAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        private int Append(ReadOnlySpan<byte> buffer)
        {
            if (_finished)
                throw new ObjectDisposedException(nameof(UploadStream), "The object has been written or given up.");
            var take = (int)Math.Min(buffer.Length, partSize - _buffer.Length);
            _buffer.Write(buffer[..take]);
            _written += take;
            return take;
        }

        private async ValueTask SendPartAsync(CancellationToken cancellationToken)
        {
            _uploadId ??= (await client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest { BucketName = bucket, Key = key }, cancellationToken).ConfigureAwait(false)).UploadId;
            var number = _parts.Count + 1;
            _buffer.Position = 0;
            var response = await client.UploadPartAsync(new UploadPartRequest
            {
                BucketName = bucket,
                Key = key,
                UploadId = _uploadId,
                PartNumber = number,
                PartSize = _buffer.Length,
                InputStream = _buffer,
            }, cancellationToken).ConfigureAwait(false);
            _parts.Add(new PartETag(number, response.ETag));
            _buffer = new MemoryStream();
        }

        /// <summary>Makes the object: one put, or the last part and the completion.</summary>
        public async ValueTask CompleteAsync(CancellationToken cancellationToken)
        {
            if (_finished)
                throw new InvalidOperationException("The object has been written or given up.");
            if (_uploadId is null)
            {
                _buffer.Position = 0;
                await client.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucket,
                    Key = key,
                    InputStream = _buffer,
                    AutoCloseStream = false,
                }, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                if (_buffer.Length > 0)
                    await SendPartAsync(cancellationToken).ConfigureAwait(false);
                await client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
                {
                    BucketName = bucket,
                    Key = key,
                    UploadId = _uploadId,
                    PartETags = _parts,
                }, cancellationToken).ConfigureAwait(false);
            }
            _finished = true;
            _buffer = new MemoryStream();
        }

        /// <summary>Gives the object up: parts already sent are dropped. Never throws — a
        /// failed abort leaves an incomplete upload, which no listing shows and a
        /// bucket's lifecycle rule removes.</summary>
        public async ValueTask AbortAsync()
        {
            _finished = true;
            _buffer = new MemoryStream();
            if (_uploadId is null)
                return;
            try
            {
                using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                await client.AbortMultipartUploadAsync(new AbortMultipartUploadRequest { BucketName = bucket, Key = key, UploadId = _uploadId }, bound.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is AmazonServiceException or OperationCanceledException or HttpRequestException or IOException)
            {
            }
        }
    }
}
