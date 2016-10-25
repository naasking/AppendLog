using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.Remoting;
using System.Diagnostics.Contracts;

namespace AppendLog
{
    /// <summary>
    /// A stream that limits access to a subset of an underlying stream type.
    /// </summary>
    public sealed class BoundedStream : Stream
    {
        readonly long start;
        readonly int length;
        readonly FileStream underlying;

        /// <summary>
        /// Construct a bounded stream.
        /// </summary>
        /// <param name="underlying"></param>
        /// <param name="start"></param>
        /// <param name="length"></param>
        public BoundedStream(FileStream underlying, long start, int length)
        {
            Contract.Requires(underlying != null);
            Contract.Requires(start >= 0);
            Contract.Requires(length >= 0);
            this.length = length;
            this.underlying = underlying;
            underlying.Seek(this.start = start, SeekOrigin.Begin);
            Contract.Assume(underlying.Position == start);
        }

        ~BoundedStream()
        {
            Dispose();
        }

        [ContractInvariantMethod]
        void Invaraints()
        {
            Contract.Invariant(underlying != null);
            Contract.Invariant(start >= 0);
            Contract.Invariant(length >= 0);
            Contract.Invariant(underlying.Position >= start);
            //Contract.Invariant(underlying.Position < start + length);
            //Contract.Invariant(length >= Position);
            //Contract.Invariant(Position >= 0);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            //Contract.Ensures(Contract.Result<long>() <= length);
            var pos = origin == SeekOrigin.Begin   ? start + offset:
                      origin == SeekOrigin.Current ? underlying.Position + offset:
                                                     underlying.Length + offset;
            if (pos < start) throw new ArgumentException("Cannot seek before the beginning of the log.", "offset");
            if (pos >= start + length) throw new ArgumentException("Cannot seek past the end of the log.", "offset");
            var npos = underlying.Seek(pos, SeekOrigin.Begin) - start;
            Contract.Assume(underlying.Position == pos);
            return npos;
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
            if (value < 0) throw new ArgumentOutOfRangeException("value", "Attempted to set the value parameter to less than 0.");
            underlying.SetLength(value + start);
        }

        public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
        {
            count = Remainder(count);
            return underlying.BeginRead(buffer, offset, count, callback, state);
        }
        public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
        {
            return underlying.BeginWrite(buffer, offset, Remainder(count), callback, state);
        }
        public override int Read(byte[] buffer, int offset, int count)
        {
            return underlying.Read(buffer, offset, Remainder(count));
        }
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return underlying.ReadAsync(buffer, offset, Remainder(count), cancellationToken);
        }
        public override void Write(byte[] buffer, int offset, int count)
        {
            underlying.Write(buffer, offset, Remainder(count));
        }
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return underlying.WriteAsync(buffer, offset, Remainder(count), cancellationToken);
        }
        public override async Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
        {
            // have to restrict copying to the length of the bounded stream
            var buf = new byte[bufferSize];
            while (Position < length)
            {
                var read = await ReadAsync(buf, 0, bufferSize);
                await destination.WriteAsync(buf, 0, read);
            }
        }
        public override int ReadByte()
        {
            return Position == length ? -1 : underlying.ReadByte();
        }
        public override void WriteByte(byte value)
        {
            if (Position == length) throw new InvalidOperationException("Cannot write past the end of the stream.");
            underlying.WriteByte(value);
        }

        int Remainder(int count)
        {
            Contract.Requires(count >= 0);
            Contract.Ensures(Contract.Result<int>() >= 0);
            return Math.Min(count, Math.Max(0, (int)(length - Position)));
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
        public override int ReadTimeout
        {
            get { return underlying.ReadTimeout; }
            set { underlying.ReadTimeout = value; }
        }
        public override int WriteTimeout
        {
            get { return underlying.WriteTimeout; }
            set { underlying.WriteTimeout = value; }
        }
        #endregion
    }
}
