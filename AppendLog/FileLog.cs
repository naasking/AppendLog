using System;
using System.Security.AccessControl;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics.Contracts;

namespace AppendLog
{
    /// <summary>
    /// A streaming file-based implementation of <see cref="IAppendLog"/>.
    /// </summary>
    public sealed class FileLog : IAppendLog
    {
        readonly string path;
        long next;
        FileStream writer;
        readonly byte[] writeBuffer;
        SemaphoreSlim sem = new SemaphoreSlim(1);
        
        // current log file version number
        internal static readonly Version VERSION = new Version(0, 0, 0, 1);
        // The size of an entry header block.
        internal const int EHDR_SIZE = sizeof(int);
        // The size of the log file header which consists of 3 * Int32 for the version #.
        internal const int LHDR_MAJOR = 0;  // offset of major vers#
        internal const int LHDR_MINOR = 4;  // offset of minor vers#
        internal const int LHDR_REV   = 8;  // offset of revision vers#
        internal const int LHDR_SIZE = 24; // major + minor + rev + padding(6) + txid[62]
        internal const int LHDR_TX = 16;    // the start of the tx buffer

        FileLog(string path, FileStream writer, long next)
        {
            Contract.Requires(!string.IsNullOrEmpty(path));
            Contract.Requires(writer != null);
            Contract.Requires(next >= LHDR_TX);
            this.path = path;
            this.writer = writer;
            this.next = next;
            this.writeBuffer = new byte[sizeof(long)];
        }
        
        ~FileLog()
        {
            Dispose();
        }

        [ContractInvariantMethod]
        void Invariants()
        {
            Contract.Invariant(!string.IsNullOrEmpty(path));
            Contract.Invariant(next >= LHDR_TX);
            Contract.Invariant(writeBuffer != null);
            Contract.Invariant(writeBuffer.Length >= sizeof(long));
        }

        /// <summary>
        /// An async FileLog constructor.
        /// </summary>
        /// <param name="path">The path to the log file.</param>
        /// <param name="useAsync">Instruct the underlying IO subsystem to optimize for async I/O.</param>
        /// <returns>An <see cref="FileLog"/> instance.</returns>
        public static async Task<FileLog> Create(string path, bool useAsync)
        {
            if (path == null) throw new ArgumentNullException("path");
            path = string.Intern(Path.GetFullPath(path));
            var writer = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 4096, useAsync);
            var buf = new byte[LHDR_SIZE];
            // check the file's version number if file exists, else write it out and initialize the log
            long next;
            Version vers;
            if (writer.Length < LHDR_SIZE)
            {
                next = LHDR_SIZE;
                vers = VERSION;
                buf.Write(vers.Major, LHDR_MAJOR);
                buf.Write(vers.Minor, LHDR_MINOR);
                buf.Write(vers.Revision, LHDR_REV);
                buf.Write(next, LHDR_TX);
                await writer.WriteAsync(buf, 0, LHDR_SIZE);
                await writer.FlushAsync();
            }
            else
            {
                await writer.ReadAsync(buf, 0, LHDR_SIZE);
                var major = buf.ReadInt32(LHDR_MAJOR);
                var minor = buf.ReadInt32(LHDR_MINOR);
                var rev = buf.ReadInt32(LHDR_REV);
                vers = new Version(major, minor, 0, rev);
                if (vers != VERSION)
                {
                    writer.Dispose();
                    throw new NotSupportedException(string.Format("File log expects version {0} but found version {1}", VERSION, vers));
                }
                next = buf.ReadInt32(LHDR_TX);
            }
            return new FileLog(path, writer, next);
        }

        /// <summary>
        /// The first transaction in the log.
        /// </summary>
        public TransactionId First
        {
            get { return new TransactionId(LHDR_SIZE, path); }
        }

        /// <summary>
        /// Enumerate the sequence of transactions since <paramref name="last"/>.
        /// </summary>
        /// <param name="last">The last event seen.</param>
        /// <returns>A sequence of transactions since the given event.</returns>
        public ILogEnumerator Replay(TransactionId last)
        {
            if (!ReferenceEquals(path, last.Path))
                throw new ArgumentException("last", "The given transaction is not for this log");
            return new LogEnumerator(this, last);
        }

        /// <summary>
        /// Atomically append data to the durable store.
        /// </summary>
        /// <returns>A stream for writing.</returns>
        public async Task<AppendRequest> Append()
        {
            var x = writer;
            await sem.WaitAsync();
            if (writer == null)
            {
                sem.Release();
                throw new ObjectDisposedException(string.Format("FileLog ({0}) has been disposed.", path));
            }
            x.Seek(next, SeekOrigin.Begin);
            var transaction = new TransactionId(next, path);
            var output = new BoundedStream(x, next, int.MaxValue);
            output.Disposed += FinishAppend;
            return new AppendRequest(output, transaction);
        }

        public void Dispose()
        {
            // we could track outstanding write stream and dispose of it, but there's nothing
            // dangerous or incorrect about letting writers finish in their own time
            sem.Wait();
            var x = writer;
            if (x != null)
            {
                writer = null;
                x.Close();
                GC.SuppressFinalize(this);
            }
        }

        #region Internals
        void FinishAppend(Stream stream)
        {
            var x = writer;
            if (x == null) return;
            var length = (int)(x.Length - next);
            if (length > 0)
            {
                // ensure stream points to the end of the written block
                if (x.Length != x.Position)
                    x.Seek(0, SeekOrigin.End);
                // write the entry length into the EHDR block
                writeBuffer.Write(length, 0);
                x.Write(writeBuffer, 0, EHDR_SIZE);
                x.Flush(true);
                next += length + EHDR_SIZE;
                writeBuffer.Write(next);
                // write out position of new entry in the header
                x.Seek(LHDR_TX, SeekOrigin.Begin);
                x.Write(writeBuffer, 0, sizeof(long));
                x.Flush(true);
                sem.Release();
                //FIXME: power loss here before disk write could lose the last tx, so 
                //FIXME: it might be possible to get better write parallelism if we could release
                //the semaphore just before calling flush. Other writers are only appending after all.
            }
            else
                sem.Release();
        }

        sealed class LogEnumerator : ILogEnumerator
        {
            FileLog log;
            long length;
            readonly FileStream file;
            readonly byte[] buf = new byte[EHDR_SIZE];
            // stack of block lengths from known last position to end of file
            readonly Stack<int> lengths = new Stack<int>();
            public TransactionId Transaction { get; internal set; }
            public Stream Stream { get; internal set; }

            ~LogEnumerator()
            {
                Dispose();
            }

            [ContractInvariantMethod]
            void Invariants()
            {
                Contract.Invariant(file != null);
                Contract.Invariant(length >= 0);
            }

            public LogEnumerator(FileLog log, TransactionId last)
            {
                Contract.Requires(log != null);
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
                Stream = new BoundedStream(file, txid, (int)length - EHDR_SIZE);
                return true;
            }

            public void Dispose()
            {
                var x = Interlocked.Exchange(ref log, null);
                if (x != null)
                {
                    file.Dispose();
                    GC.SuppressFinalize(this);
                }
            }

            async Task<bool> NoPreviousEntries(long last, long next)
            {
                while (last != next)
                {
                    file.Seek(next - EHDR_SIZE, SeekOrigin.Begin);
                    await file.ReadAsync(buf, 0, EHDR_SIZE);
                    var x = buf.ReadInt32();
                    if (x < 0) throw new InvalidOperationException(string.Format("Found a negative length {0} in FileLog ({1})", x, log.path));
                    lengths.Push(x);
                    next -= x + EHDR_SIZE;
                }
                return lengths.Count == 0;
            }
        }
        #endregion
    }
}