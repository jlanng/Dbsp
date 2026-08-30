using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;

namespace Incremental.Cluster;

/// <summary>
/// Append-only byte buffer behind the dense shard encoding. Reused across frames by the
/// worker that ships them, so a steady stream of shuffles settles on one buffer instead of
/// allocating a fresh one per frame. Integers are LEB128 (zig-zag for the signed ones), which
/// is what makes the encoding dense where it matters: Z-set weights are almost always +1 or
/// -1, and identifiers and counts are small.
/// </summary>
internal sealed class WireWriter
{
    private byte[] _buffer = new byte[4096];
    private int _length;

    public int Length => _length;

    public void Reset() => _length = 0;

    /// <summary>A right-sized copy of what has been written — the frame handed to the
    /// transport, which owns it from then on.</summary>
    public byte[] ToArray() => _buffer.AsSpan(0, _length).ToArray();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Span<byte> Take(int count)
    {
        if (_length + count > _buffer.Length)
            Grow(count);
        var span = _buffer.AsSpan(_length, count);
        _length += count;
        return span;
    }

    private void Grow(int count)
    {
        var capacity = Math.Max(_buffer.Length * 2, _length + count);
        Array.Resize(ref _buffer, capacity);
    }

    public void WriteByte(byte value) => Take(1)[0] = value;

    public void WriteBool(bool value) => WriteByte(value ? (byte)1 : (byte)0);

    public void WriteUInt64(ulong value)
    {
        while (value >= 0x80)
        {
            WriteByte((byte)(value | 0x80));
            value >>= 7;
        }
        WriteByte((byte)value);
    }

    public void WriteInt64(long value) => WriteUInt64((ulong)(value << 1 ^ value >> 63));

    public void WriteUInt32(uint value) => WriteUInt64(value);

    public void WriteInt32(int value) => WriteInt64(value);

    public void WriteSingle(float value) => BinaryPrimitives.WriteSingleLittleEndian(Take(4), value);

    public void WriteDouble(double value) => BinaryPrimitives.WriteDoubleLittleEndian(Take(8), value);

    public void WriteDecimal(decimal value)
    {
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(value, bits);
        var span = Take(16);
        for (var i = 0; i < 4; i++)
            BinaryPrimitives.WriteInt32LittleEndian(span[(i * 4)..], bits[i]);
    }

    public void WriteGuid(Guid value) => value.TryWriteBytes(Take(16));

    public void WriteString(string value)
    {
        var bytes = Encoding.UTF8.GetByteCount(value);
        WriteUInt32((uint)bytes);
        Encoding.UTF8.GetBytes(value, Take(bytes));
    }
}

/// <summary>Reading counterpart of <see cref="WireWriter"/>; reused per frame the same way.</summary>
internal sealed class WireReader
{
    private byte[] _buffer = [];
    private int _position;

    public void Reset(byte[] buffer)
    {
        _buffer = buffer;
        _position = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ReadOnlySpan<byte> Take(int count)
    {
        if (_position + count > _buffer.Length)
            throw Truncated();
        var span = _buffer.AsSpan(_position, count);
        _position += count;
        return span;
    }

    public byte ReadByte()
    {
        if (_position >= _buffer.Length)
            throw Truncated();
        return _buffer[_position++];
    }

    public bool ReadBool() => ReadByte() != 0;

    public ulong ReadUInt64()
    {
        ulong value = 0;
        var shift = 0;
        while (true)
        {
            var b = ReadByte();
            value |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return value;
            shift += 7;
            if (shift > 63)
                throw new InvalidDataException("Exchange frame contains a malformed variable-length integer.");
        }
    }

    public long ReadInt64()
    {
        var raw = ReadUInt64();
        return (long)(raw >> 1) ^ -(long)(raw & 1);
    }

    public uint ReadUInt32() => (uint)ReadUInt64();

    public int ReadInt32() => (int)ReadInt64();

    public float ReadSingle() => BinaryPrimitives.ReadSingleLittleEndian(Take(4));

    public double ReadDouble() => BinaryPrimitives.ReadDoubleLittleEndian(Take(8));

    public decimal ReadDecimal()
    {
        var span = Take(16);
        Span<int> bits = stackalloc int[4];
        for (var i = 0; i < 4; i++)
            bits[i] = BinaryPrimitives.ReadInt32LittleEndian(span[(i * 4)..]);
        return new decimal(bits);
    }

    public Guid ReadGuid() => new(Take(16));

    public string ReadString() => Encoding.UTF8.GetString(Take((int)ReadUInt32()));

    private static InvalidDataException Truncated() =>
        new("Exchange frame ended mid-value: the peer's encoding does not match this process's.");
}
