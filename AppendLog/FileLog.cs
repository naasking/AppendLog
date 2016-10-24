using System;
using System.Security.AccessControl;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.IO.MemoryMappedFiles;
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
        long next;
        FileStream header;
        FileStream writer;

        //FIXME: creating FileStreams is expensive, so perhaps have a BoundedStream pool for readers?

        // current log file version number
        internal static readonly Version VERSION = new Version(0, 0, 0, 1);
        // The size of an entry header.
        internal const int EHDR_SIZE = sizeof(int);
        // The size of the log file header which consists of Int64 version # followed by the last committed transaction id.
        internal const int LHDR_SIZE = 4 + 4 + 4; // major + minor + rev
        //internal const long LHDR_SIZE = 16 + 4 + 4 + 4; // Guid + major + minor + rev
        // The size of the log file header which consists of Int64 version # followed by the last committed transaction id.
        internal const int TXID_POS = LHDR_SIZE + sizeof(long);

        ~FileLog()
        {
            Dispose();
        }
        
        /// <summary>
        /// An async FileLog constructor.
        /// </summary>
        /// <param name="path">The path to the log file.</param>
        /// <returns>An <see cref="FileLog"/> instance.</returns>
        public static async Task<FileLog> Create(string path)
        {
            if (path == null) throw new ArgumentNullException("path");
            path = string.Intern(Path.GetFullPath(path));
            // check the file's version number if file exists, else write it out
            long next;
            var header = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            var writer = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite, 4096, true);
            var buf = new byte[TXID_POS];
            Version vers;
            if (header.Length < TXID_POS)
            {
                next = TXID_POS;
                vers = VERSION;
                buf.Write(vers.Major, 0);
                buf.Write(vers.Minor, 4);
                buf.Write(vers.Revision, 8);
                buf.Write(next, 12);
                await header.WriteAsync(buf, 0, TXID_POS);
                await header.FlushAsync();
            }
            else
            {
                await header.ReadAsync(buf, 0, TXID_POS);
                var major = buf.ReadInt32(0);
                var minor = buf.ReadInt32(4);
                var rev = buf.ReadInt32(8);
                vers = new Version(major, minor, 0, rev);
                if (vers != VERSION)
                {
                    header.Dispose();
                    writer.Dispose();
                    throw new NotSupportedException(string.Format("File log expects version {0} but found version {1}", VERSION, vers));
                }
                next = buf.ReadInt32(12);
            }
            //var writer = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite, 4096, true);
            return new FileLog { path = path, next = next, header = header, writer = writer };
        }

        /// <summary>
        /// The first transaction in the log.
        /// </summary>
        public TransactionId First
        {
            get { return new TransactionId(TXID_POS, path); }
        }

        /// <summary>
        /// Enumerate the sequence of transactions since <paramref name="last"/>.
        /// </summary>
        /// <param name="last">The last event seen.</param>
        /// <returns>A sequence of transactions since the given event.</returns>
        public IEventEnumerator Replay(TransactionId last)
        {
            if (!ReferenceEquals(path, last.Path))
                throw new ArgumentException("last", "The given transaction is not for this log");
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
        public IDisposable Append(out Stream output, out TransactionId tx)
        {
            Monitor.Enter(writer);
            tx = new TransactionId(next, path);
            output = new BoundedStream(this, writer, next, int.MaxValue);
            return new Appender(this);
        }

        public void Dispose()
        {
            // we could track outstanding write stream and dispose of it, but there's nothing
            // dangerous or incorrect about letting writers finish in their own time
            var x = Interlocked.Exchange(ref header, null);
            if (x != null)
            {
                x.Close();
                writer.Close();
            }
        }

        #region Internals
        sealed class Appender : IDisposable
        {
            internal FileStream writer;
            internal FileLog log;
            internal FileStream header;
            internal byte[] buf;
            public Appender(FileLog log)
            {
                this.log = log;
                this.writer = log.writer;
                this.header = log.header;
                this.buf = new byte[sizeof(long)];
            }
            public void Dispose()
            {
                var x = Interlocked.Exchange(ref writer, null);
                if (x != null)
                {
                    var length = (int)(x.Length - log.next);
                    if (length > 0)
                    {
                        if (x.Length != x.Position)
                            x.Seek(log.next + length, SeekOrigin.Begin);
                        buf.Write(length, 0);
                        x.Write(buf, 0, EHDR_SIZE);
                        x.Flush();
                        log.next += length + EHDR_SIZE;
                        buf.Write(log.next);
                        header.Seek(LHDR_SIZE, SeekOrigin.Begin);
                        header.Write(buf, 0, sizeof(long));
                        header.Flush();
                    }
                    Monitor.Exit(x);
                }
            }
        }

        sealed class EventEnumerator : IEventEnumerator
        {
            FileLog log;
            long length;
            FileStream file;
            byte[] buf = new byte[EHDR_SIZE];
            Stack<int> lengths = new Stack<int>();
            public TransactionId Transaction { get; internal set; }
            public Stream Stream { get; internal set; }

            ~EventEnumerator()
            {
                Dispose();
            }

            public EventEnumerator(FileLog log, TransactionId last)
            {
                this.log = log;
                this.file = new FileStream(log.path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, true);
                this.length = last.Id;
                this.Transaction = default(TransactionId);
            }

            public async Task<bool> MoveNext()
            {
                if (log == null) throw new ObjectDisposedException("IEventEnumerator");
                var txid = Transaction.Id + length;
                if (txid == log.next || lengths.Count == 0 && await NoPreviousEntries(txid, log.next))
                    return false;
                length = lengths.Peek() + EHDR_SIZE;
                Transaction = new TransactionId(txid, log.path);
                Stream = new BoundedStream(log, file, txid, (int)length - EHDR_SIZE);
                return true;
            }

            public void Dispose()
            {
                var x = Interlocked.Exchange(ref log, null);
                if (x != null) file.Dispose();
            }

            async Task<bool> NoPreviousEntries(long last, long next)
            {
                while (last != next)
                {
                    file.Seek(next - EHDR_SIZE, SeekOrigin.Begin);
                    await file.ReadAsync(buf, 0, EHDR_SIZE);
                    var x = buf.ReadInt32();
                    lengths.Push(x);
                    next -= x + EHDR_SIZE;
                }
                return lengths.Count == 0;
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
        sealed class BoundedStream : Stream
        {
            long start;
            int length;
            FileStream underlying;
            FileLog log;
            
            public BoundedStream(FileLog log, FileStream underlying, long start, int length)
            {
                this.log = log;
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
                var pos = origin == SeekOrigin.Begin   ? start + offset:
                          origin == SeekOrigin.Current ? Position + offset:
                                                         Length + offset;
                if (pos <= start) throw new ArgumentException("Cannot seek before the beginning of the log.", "offset");
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
        #endregion
    }
}