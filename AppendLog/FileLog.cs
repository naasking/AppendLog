using System;
using System.Security.AccessControl;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Runtime.Remoting;

namespace AppendLog
{
    /// <summary>
    /// A streaming file-based implementation of <see cref="IAppendLog"/>.
    /// </summary>
    public sealed class FileLog : IAppendLog
    {
        //FIXME: use 128 bit transaction ids, and store the "base" offset at the beginning of the db file.

        //FIXME: implement file rollover for compaction purposes, ie. can delete old entries by starting to
        //write to a different file than the one we're reading from, initialized with a new base offset. We
        //can then either copy the entries starting at the new base into the new file, or change the read
        //logic to also open the new file when it's done with the read file. The latter is preferable.
        string path;
        FileStream writer;
        long next;
        byte[] buf;

        //FIXME: creating FileStreams is expensive, so perhaps have a BoundedStream pool for readers?

        //FIXME: currently seek around too much which causes an order of magnitude slowdown. Change log
        //format to literal append-only with fixed-size segments, so given any file length we can compute
        //the last flushed segment and check for garbage. Segments are of two types, internal | complete.
        //Complete segments correspond to a full transaction, and consist of a sequence of internal
        //segments. Last flused segment may be internal or complete. If complete, that's the last
        //committed transaction. If internal, we step back until we hit the first complete segment,
        //truncate the log file to that point. We need some sort of magic number for each segment
        //header to verify whether the segment is garbage.

        // current log file version number
        internal const long VERSION = 0x01;
        // The size of an entry header.
        internal const int EHDR_SIZE = sizeof(int);
        // The size of the log file header which consists of Int64 version # followed by the last committed transaction id.
        internal const long LHDR_SIZE = sizeof(long) + sizeof(long);
        // The size of the log file header which consists of Int64 version # followed by the last committed transaction id.
        internal const int TXID_POS = sizeof(long);
        
        /// <summary>
        /// An async FileLog constructor.
        /// </summary>
        /// <param name="path">The path to the log file.</param>
        /// <returns>An <see cref="FileLog"/> instance.</returns>
        public static async Task<FileLog> Create(string path)
        {
            if (path == null) throw new ArgumentNullException("path");
            path = Path.GetFullPath(path);
            // check the file's version number if file exists, else write it out
            long next;
            var fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 4096, true);
            var buf = new byte[LHDR_SIZE];
            if (fs.Length < LHDR_SIZE)
            {
                VERSION.WriteId(buf);
                await fs.WriteAsync(buf, 0, TXID_POS);
                LHDR_SIZE.WriteId(buf);
                await fs.WriteAsync(buf, 0, TXID_POS);
                await fs.FlushAsync();
                next = LHDR_SIZE;
            }
            else
            {
                await fs.ReadAsync(buf, 0, buf.Length);
                var vers = buf.GetNextId();
                if (vers != VERSION)
                {
                    fs.Dispose();
                    throw new NotSupportedException(string.Format("File log expects version {0}.{1}.{2} but found version {3}.{4}.{5}",
                        Major(VERSION), Minor(VERSION), Revision(VERSION), Major(vers), Minor(vers), Revision(vers)));
                }
                next = buf.GetNextId(sizeof(long));
            }
            return new FileLog { path = path, next = next, writer = fs, buf = buf };
        }

        /// <summary>
        /// The first transaction in the log.
        /// </summary>
        public TransactionId First
        {
            get { return new TransactionId { Id = LHDR_SIZE }; }
        }

        /// <summary>
        /// Enumerate the sequence of transactions since <paramref name="last"/>.
        /// </summary>
        /// <param name="last">The last event seen.</param>
        /// <returns>A sequence of transactions since the given event.</returns>
        public IEventEnumerator Replay(TransactionId last)
        {
            return new EventEnumerator(this, last);
        }

        /// <summary>
        /// Atomically append data to the durable store.
        /// </summary>
        /// <param name="async">True if the stream should support efficient asynchronous operations, false otherwise.</param>
        /// <param name="transaction">The transaction being written.</param>
        /// <returns>A stream for writing.</returns>
        /// <remarks>
        /// The <paramref name="async"/> parameter is largely optional, in that it's safe to simply
        /// provide 'false' and everything will still work.
        /// </remarks>
        public Stream Append(out TransactionId tx)
        {
            Monitor.Enter(writer);
            tx = new TransactionId { Id = next };
            return new AtomicAppender(this, writer, next, buf);
        }

        public void Dispose()
        {
            // we could track outstanding write stream and dispose of it, but there's nothing
            // dangerous or incorrect about letting writers finish in their own time
            writer.Close();
        }

        #region Internals
        static long Major(long x)    { return 0x1FFFFF & (x >> (64 - 21)); }
        static long Minor(long x)    { return 0x1FFFFF & (x >> (64 - 42)); }
        static long Revision(long x) { return 0x1FFFFF & x; }

        sealed class EventEnumerator : IEventEnumerator
        {
            FileLog log;
            FileStream file;
            byte[] buf;
            long end;
            int length;
            public TransactionId Transaction { get; internal set; }
            public Stream Stream { get; internal set; }

            ~EventEnumerator()
            {
                Dispose();
            }

            public EventEnumerator(FileLog slog, TransactionId last)
            {
                file = new FileStream(slog.path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
                log = slog;
                buf = new byte[TXID_POS];
                Transaction = last.Id == 0 ? log.First : last;
                file.Position = TXID_POS;
            }

            public async Task<bool> MoveNext()
            {
                if (file == null) throw new ObjectDisposedException("IEventEnumerator");
                if (Stream == null)
                {
                    // on first run, load the last transaction which terminates the enumeration
                    await file.ReadAsync(buf, 0, TXID_POS);
                    end = buf.GetNextId();
                    file.Seek(Transaction.Id, SeekOrigin.Begin);
                }
                Debug.Assert(file.Position <= end);  // if pos > end, then read/seek logic is messed up
                if (file.Position == end)
                    return false;
                Transaction = new TransactionId { Id = file.Position };
                await file.ReadAsync(buf, 0, EHDR_SIZE);
                length = buf.GetLength();
                Stream = new BoundedStream(log, file.Position, length);
                file.Seek(length, SeekOrigin.Current);
                return true;
            }

            public void Dispose()
            {
                var x = Interlocked.Exchange(ref file, null);
                if (x != null) x.Dispose();
            }
        }
        
        /// <summary>
        /// A file stream that updates the transaction identifiers embedded in a file upon close.
        /// </summary>
        /// <remarks>
        /// This class ensures that only a single writer accesses the file at any one time. Since
        /// it's append-only, we allow multiple readers to access the file; they are bounded above
        /// by the file's internal transaction identifier.
        /// 
        /// The format of the log data is simply a sequence of records:
        /// +---------------+---------------+
        /// | 32-bit length | length * byte |
        /// +---------------+---------------+
        /// </remarks>
        sealed class AtomicAppender : Stream
        {
            long start;
            byte[] buf;
            FileStream underlying;
            FileLog log;

            // open the file with exclusive write access and using a 4KB buffer
            public AtomicAppender(FileLog log, FileStream underlying, long start, byte[] buf)
            {
                this.log = log;
                this.buf = buf;
                this.underlying = underlying;
                underlying.Position = this.start = start + EHDR_SIZE;
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                var newpos = origin == SeekOrigin.Begin   ? start + offset:
                             origin == SeekOrigin.Current ? Position + offset:
                                                            Length + offset;
                if (newpos <= start) throw new ArgumentException("Cannot seek before the beginning of the log.", "offset");
                return underlying.Seek(newpos, SeekOrigin.Begin);
            }

            public override long Position
            {
                get { return underlying.Position - start; }
                set { Seek(value, SeekOrigin.Begin); }
            }

            public override long Length
            {
                get { return underlying.Length - start; }
            }

            public override void SetLength(long value)
            {
                underlying.SetLength(value + start);
            }

            public override void Close()
            {
                var length = (int)(underlying.Position - start);
                if (length > 0)
                {
                    // write out the 32-bit length block
                    length.WriteLength(buf);
                    underlying.Seek(start - EHDR_SIZE, SeekOrigin.Begin);
                    underlying.Write(buf, 0, EHDR_SIZE);

                    //NOTE FS Atomicity: http://danluu.com/file-consistency/
                    //Moral: the OS can reorder writes such that the write at 0 could happen first, even if the write
                    //is called 2nd. If a crash/power loss happens btw the write at 0 and the writes to the end, the
                    //header is then just pointing at garbage. So call flush to sync data to disk, then write header.
                    underlying.Flush(); // or Flush(true)?

                    // the block is now successfully persisted, so write out the new txid in the log header
                    var txid = start + length;
                    txid.WriteId(buf);
                    underlying.Seek(TXID_POS, SeekOrigin.Begin); // write txid after version #
                    underlying.Write(buf, 0, TXID_POS);
                    underlying.Flush();
                    base.Close();

                    // update the cached 'next' txid, then release the lock on the underlying stream
                    Interlocked.Exchange(ref log.next, txid);
                }
                else
                {
                    base.Close();
                }
                Monitor.Exit(underlying);
            }

            #region Delegated operations
            public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
            {
                return underlying.BeginRead(buffer, offset, count, callback, state);
            }
            public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
            {
                return underlying.BeginWrite(buffer, offset, count, callback, state);
            }
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
            public override int Read(byte[] buffer, int offset, int count)
            {
                return underlying.Read(buffer, offset, count);
            }
            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                return underlying.ReadAsync(buffer, offset, count, cancellationToken);
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
            public override void Write(byte[] buffer, int offset, int count)
            {
                underlying.Write(buffer, offset, count);
            }
            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                return underlying.WriteAsync(buffer, offset, count, cancellationToken);
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

        /// <summary>
        /// A file stream that's bounded.
        /// </summary>
        sealed class BoundedStream : FileStream
        {
            long start;
            long end;

            public BoundedStream(FileLog log, long start, int length)
                : base(log.path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)
            {
                this.start = start;
                this.end = start + length;
                base.Seek(start, SeekOrigin.Begin);
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                var newpos = origin == SeekOrigin.Begin   ? start + offset:
                             origin == SeekOrigin.Current ? Position + offset:
                                                            Length + offset;
                if (newpos < start) throw new ArgumentException("Cannot seek before the beginning of the log.", "offset");
                if (newpos >= end) throw new ArgumentException("Cannot seek past the end of the log.", "offset");
                return base.Seek(offset, origin);
            }

            public override long Position
            {
                get { return base.Position; }
                set
                {
                    if (value < start) throw new ArgumentException("Cannot seek before the end of the log.", "offset");
                    if (value >= end) throw new ArgumentException("Cannot seek past the end of the log.", "offset");
                    base.Position = value;
                }
            }

            public override long Length
            {
                get { return end - start; }
            }

            public override void SetLength(long value)
            {
                base.SetLength(value + start);
            }

            public override int Read(byte[] array, int offset, int count)
            {
                return base.Read(array, offset, (int)Math.Min(end - Position, count));
            }

            public override IAsyncResult BeginRead(byte[] array, int offset, int numBytes, AsyncCallback userCallback, object stateObject)
            {
                return base.BeginRead(array, offset, (int)Math.Min(end - Position, numBytes), userCallback, stateObject);
            }

            public override int ReadByte()
            {
                if (Position >= end) throw new ArgumentException("Cannot seek past the end of the log.");
                return base.ReadByte();
            }
        }
        #endregion
    }
}