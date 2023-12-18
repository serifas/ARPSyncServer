namespace MareSynchronosStaticFilesServer.Utils;

// Concatenates the content of multiple readable streams
public class ConcatenatedStreamReader : Stream
{
    private IEnumerable<Stream> _streams;
    private IEnumerator<Stream> _iter;
    private bool _finished;
    public bool DisposeUnderlying = true;

    public ConcatenatedStreamReader(IEnumerable<Stream> streams)
    {
        _streams = streams;
        _iter = streams.GetEnumerator();
        _finished = !_iter.MoveNext();
    }

    protected override void Dispose(bool disposing)
    {
        if (!DisposeUnderlying)
            return;
        foreach (var stream in _streams)
            stream.Dispose();
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

    public async override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        int n = 0;

        while (n == 0 && !_finished)
        {
            n = await _iter.Current.ReadAsync(buffer, offset, count, cancellationToken);

            if (cancellationToken.IsCancellationRequested)
                break;

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
