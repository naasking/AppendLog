using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.Remoting;

namespace AppendLog
{
    /// <summary>
    /// A stream that limits access to a subset of an underlying stream type.
    /// </summary>
    public sealed class BoundedStream : Stream
    {
        long start;
        int length;
        FileStream underlying;

        /// <summary>
        /// Construct a bounded stream.
        /// </summary>
        /// <param name="underlying"></param>
        /// <param name="start"></param>
        /// <param name="length"></param>
        public BoundedStream(FileStream underlying, long start, int length)
        {
            this.length = length;
            this.underlying = underlying;
            underlying.Position = this.start = start;
        }

        ~BoundedStream()
        {
            Dispose();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            var pos = origin == SeekOrigin.Begin ? start + offset :
                      origin == SeekOrigin.Current ? Position + offset :
                                                     Length + offset;
            if (pos < start) throw new ArgumentException("Cannot seek before the beginning of the log.", "offset");
            if (pos > start + length) throw new ArgumentException("Cannot seek past the end of the log.", "offset");
            return underlying.Seek(pos, SeekOrigin.Begin);
        }

        public override long Position
        {
            get { return underlying.Position - start; }
            set { Seek(value, SeekOrigin.Begin); }
        }

        public override long Length
        {
            get { return Math.Min(length, Math.Max(0, underlying.Length - start)); }
        }

        public override void SetLength(long value)
        {
            if (length < 0) throw new ArgumentOutOfRangeException("value", "Attempted to set the value parameter to less than 0.");
            underlying.SetLength(value + start);
        }

        public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
        {
            return underlying.BeginRead(buffer, offset, Math.Min(count, (int)(length - Position)), callback, state);
        }
        public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
        {
            return underlying.BeginWrite(buffer, offset, Math.Min(count, (int)(length - Position)), callback, state);
        }
        public override int Read(byte[] buffer, int offset, int count)
        {
            return underlying.Read(buffer, offset, Math.Min(count, (int)(length - Position)));
        }
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return underlying.ReadAsync(buffer, offset, Math.Min(count, (int)(length - Position)), cancellationToken);
        }
        public override void Write(byte[] buffer, int offset, int count)
        {
            underlying.Write(buffer, offset, Math.Min(count, (int)(length - Position)));
        }
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return underlying.WriteAsync(buffer, offset, Math.Min(count, (int)(length - Position)), cancellationToken);
        }

        #region Delegated operations
        public override bool CanRead
        {
            get { return underlying.CanRead; }
        }
        public override bool CanWrite
        {
            get { return underlying.CanWrite; }
        }
        public override bool CanSeek
        {
            get { return underlying.CanSeek; }
        }
        public override bool CanTimeout
        {
            get { return underlying.CanTimeout; }
        }
        public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
        {
            return underlying.CopyToAsync(destination, bufferSize, cancellationToken);
        }
        public override ObjRef CreateObjRef(Type requestedType)
        {
            return underlying.CreateObjRef(requestedType);
        }
        public override int EndRead(IAsyncResult asyncResult)
        {
            return base.EndRead(asyncResult);
        }
        public override void EndWrite(IAsyncResult asyncResult)
        {
            underlying.EndWrite(asyncResult);
        }
        public override void Flush()
        {
            underlying.Flush();
        }
        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            return underlying.FlushAsync(cancellationToken);
        }
        public override object InitializeLifetimeService()
        {
            return underlying.InitializeLifetimeService();
        }
        public override int ReadByte()
        {
            return underlying.ReadByte();
        }
        public override int ReadTimeout
        {
            get { return underlying.ReadTimeout; }
            set { underlying.ReadTimeout = value; }
        }
        public override void WriteByte(byte value)
        {
            underlying.WriteByte(value);
        }
        public override int WriteTimeout
        {
            get { return underlying.WriteTimeout; }
            set { underlying.WriteTimeout = value; }
        }
        #endregion
    }
}
