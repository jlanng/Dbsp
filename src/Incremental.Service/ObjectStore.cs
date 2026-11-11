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

    /// <summary>A stream whose content replaces the object when it is disposed
    /// (write-then-rename): a reader never sees half of it.</summary>
    ValueTask<Stream> OpenWriteAsync(string path, CancellationToken cancellationToken);
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

    public ValueTask<Stream> OpenWriteAsync(string path, CancellationToken cancellationToken)
    {
        var target = Resolve(path);
        Directory.CreateDirectory(Path.GetDirectoryName(target)!);
        return new ValueTask<Stream>(new RenamingStream(target));
    }

    public ValueTask DeleteAsync(string path, CancellationToken cancellationToken)
    {
        File.Delete(Resolve(path));
        return ValueTask.CompletedTask;
    }

    public async IAsyncEnumerable<ObjectInfo> ListAsync(string prefix, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await Task.CompletedTask.ConfigureAwait(false);
        if (!Directory.Exists(_root))
            yield break;
        foreach (var file in new DirectoryInfo(_root).EnumerateFiles("*", SearchOption.AllDirectories).OrderBy(f => f.FullName, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            ObjectInfo info;
            try
            {
                info = Info(file);
            }
            catch (FileNotFoundException)
            {
                continue;           // deleted while the directory was being listed
            }
            if (info.Path.StartsWith(prefix, StringComparison.Ordinal) && !info.Path.EndsWith(".tmp", StringComparison.Ordinal))
                yield return info;
        }
    }

    private sealed class RenamingStream(string target) : FileStream(
        target + "." + Guid.NewGuid().ToString("N") + ".tmp", FileMode.CreateNew, FileAccess.Write, FileShare.None, 64 * 1024, useAsync: true)
    {
        private bool _moved;

        public override async ValueTask DisposeAsync()
        {
            var temporary = Name;
            await base.DisposeAsync().ConfigureAwait(false);
            Move(temporary);
        }

        protected override void Dispose(bool disposing)
        {
            var temporary = Name;
            base.Dispose(disposing);
            if (disposing)
                Move(temporary);
        }

        private void Move(string temporary)
        {
            if (_moved)
                return;
            _moved = true;
            File.Move(temporary, target, overwrite: true);
        }
    }
}
