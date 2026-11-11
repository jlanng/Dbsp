using System.Runtime.CompilerServices;

namespace Incremental.Service;

/// <summary>An object as a store reports it. Two reads with the same
/// <see cref="Identity"/> are the same content.</summary>
public sealed record ObjectInfo(string Path, long Length, DateTimeOffset Modified, string? ETag = null)
{
    public string Identity => ETag ?? $"{Length}:{Modified.UtcTicks}";
}

/// <summary>
/// Files or objects under a root the host registered by name
/// (<c>AddObjectStore("refdata", …)</c>). Specs name the store and a relative path; they
/// cannot name a host or an absolute path. Local files are built in; anything else (S3) is
/// the host's implementation.
/// </summary>
public interface IObjectStore
{
    ValueTask<ObjectInfo?> StatAsync(string path, CancellationToken cancellationToken);
    ValueTask<Stream> OpenReadAsync(string path, CancellationToken cancellationToken);

    /// <summary>Writes an object whole. <paramref name="write"/> fills the stream it is
    /// given, and what it wrote replaces the object when it returns (write-then-rename, or
    /// one put): a reader never sees half of it. When <paramref name="write"/> throws or is
    /// cancelled, nothing changes under the name.</summary>
    ValueTask WriteAsync(string path, Func<Stream, CancellationToken, ValueTask> write, CancellationToken cancellationToken);
    ValueTask DeleteAsync(string path, CancellationToken cancellationToken);
    IAsyncEnumerable<ObjectInfo> ListAsync(string prefix, CancellationToken cancellationToken);
}

/// <summary>An <see cref="IObjectStore"/> over a local directory. It is rooted: a path
/// that is absolute or resolves outside the root is refused.</summary>
public sealed class LocalObjectStore : IObjectStore
{
    private readonly string _root;

    public LocalObjectStore(string root)
    {
        ArgumentException.ThrowIfNullOrEmpty(root);
        _root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(root));
    }

    public string Root => _root;

    private string Resolve(string path)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        if (Path.IsPathRooted(path))
            throw new ArgumentException($"Path '{path}' is absolute; an object store takes paths relative to its root.", nameof(path));
        var full = Path.GetFullPath(Path.Combine(_root, path));
        if (!full.StartsWith(_root + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            throw new ArgumentException($"Path '{path}' resolves outside the object store's root.", nameof(path));
        return full;
    }

    private ObjectInfo Info(FileInfo file) =>
        new(Path.GetRelativePath(_root, file.FullName).Replace(Path.DirectorySeparatorChar, '/'), file.Length, file.LastWriteTimeUtc);

    public ValueTask<ObjectInfo?> StatAsync(string path, CancellationToken cancellationToken)
    {
        var file = new FileInfo(Resolve(path));
        return new ValueTask<ObjectInfo?>(file.Exists ? Info(file) : null);
    }

    public ValueTask<Stream> OpenReadAsync(string path, CancellationToken cancellationToken) =>
        new(new FileStream(Resolve(path), FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, 64 * 1024, useAsync: true));

    public async ValueTask WriteAsync(string path, Func<Stream, CancellationToken, ValueTask> write, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(write);
        var target = Resolve(path);
        Directory.CreateDirectory(Path.GetDirectoryName(target)!);
        var temporary = target + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            var stream = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None, 64 * 1024, useAsync: true);
            await using (stream.ConfigureAwait(false))
                await write(stream, cancellationToken).ConfigureAwait(false);
            File.Move(temporary, target, overwrite: true);
        }
        catch
        {
            // Half an object is no object: the name keeps what it had.
            try
            {
                File.Delete(temporary);
            }
            catch (IOException)
            {
            }
            throw;
        }
    }

    public ValueTask DeleteAsync(string path, CancellationToken cancellationToken)
    {
        File.Delete(Resolve(path));
        return ValueTask.CompletedTask;
    }

    /// <summary>The objects whose path starts with <paramref name="prefix"/>, in ordinal
    /// order of their paths. Only the directory the prefix names is walked — the part of
    /// it up to its last <c>/</c> — so a listing costs what is under the prefix, not what
    /// is in the store.</summary>
    public async IAsyncEnumerable<ObjectInfo> ListAsync(string prefix, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await Task.CompletedTask.ConfigureAwait(false);
        ArgumentNullException.ThrowIfNull(prefix);
        var slash = prefix.LastIndexOf('/');
        var directory = slash <= 0 ? _root : Resolve(prefix[..slash]);
        if (!Directory.Exists(directory))
            yield break;
        var files = new List<(string Path, FileInfo File)>();
        foreach (var file in new DirectoryInfo(directory).EnumerateFiles("*", SearchOption.AllDirectories))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var path = Path.GetRelativePath(_root, file.FullName).Replace(Path.DirectorySeparatorChar, '/');
            if (path.StartsWith(prefix, StringComparison.Ordinal) && !path.EndsWith(".tmp", StringComparison.Ordinal))
                files.Add((path, file));
        }
        files.Sort(static (a, b) => string.CompareOrdinal(a.Path, b.Path));
        foreach (var (path, file) in files)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ObjectInfo info;
            try
            {
                info = new ObjectInfo(path, file.Length, file.LastWriteTimeUtc);
            }
            catch (FileNotFoundException)
            {
                continue;           // deleted while the directory was being listed
            }
            yield return info;
        }
    }
}
