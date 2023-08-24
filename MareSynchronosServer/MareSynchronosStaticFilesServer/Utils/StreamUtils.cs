namespace MareSynchronosStaticFilesServer.Utils;

public class CountedStream : Stream
{
    private readonly Stream _stream;
    public ulong BytesRead { get; private set; }
    public ulong BytesWritten { get; private set; }

    public CountedStream(Stream underlyingStream)
    {
        _stream = underlyingStream;
    }

    public override bool CanRead => _stream.CanRead;

    public override bool CanSeek => _stream.CanSeek;

    public override bool CanWrite => _stream.CanWrite;

    public override long Length => _stream.Length;

    public override long Position { get => _stream.Position; set => _stream.Position = value; }

    public override void Flush()
    {
        _stream.Flush();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        int n = _stream.Read(buffer, offset, count);
        BytesRead += (ulong)n;
        return n;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        return _stream.Seek(offset, origin);
    }

    public override void SetLength(long value)
    {
        _stream.SetLength(value);
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        BytesWritten += (ulong)count;
        _stream.Write(buffer, offset, count);
    }
}

public class ConcatenatedStreamReader : Stream
{
    private IEnumerable<Stream> _streams;
    private IEnumerator<Stream> _iter;
    private bool _finished;

    public ConcatenatedStreamReader(IEnumerable<Stream> streams)
    {
        _streams = streams;
        _iter = streams.GetEnumerator();
        _finished = !_iter.MoveNext();
    }

    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => throw new NotSupportedException();

    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

    public override void Flush()
    {
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        int n = 0;

        while (n == 0 && !_finished)
        {
            n = _iter.Current.Read(buffer, offset, count);

            if (n == 0)
                _finished = !_iter.MoveNext();
        }

        return n;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }
}
