using Dbsp.Service.S3;

namespace Dbsp.Service;

public static class S3ServiceBuilderExtensions
{
    /// <summary>
    /// An S3 bucket (under a key prefix) that a <c>files</c> slot or a <c>file</c> table
    /// loader may name as its <c>store</c>. The store is made once, when first named, and
    /// lives as long as the service. Tested against MinIO only: Amazon S3 itself, and IAM
    /// authentication to it, are not checked (see <see cref="S3ObjectStore"/>).
    /// </summary>
    public static DbspServiceBuilder AddS3ObjectStore(this DbspServiceBuilder builder, string name, S3ObjectStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrEmpty(options.Bucket, nameof(options));
        return builder.AddObjectStore(name, _ => new S3ObjectStore(options));
    }
}
