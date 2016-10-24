using System;
using System.Security.AccessControl;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;

namespace AppendLog
{
    /// <summary>
    /// A streaming file-based implementation of <see cref="IAppendLog"/>.
    /// </summary>
    public sealed class FileLog : IAppendLog
    {
        string path;
        long next;
        FileStream writer;
        byte[] buf = new byte[sizeof(long)];
        
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
        
        ~FileLog()
        {
            Dispose();
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
            // check the file's version number if file exists, else write it out
            long next;
            var writer = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 4096, useAsync);
            var buf = new byte[LHDR_SIZE];
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
            return new FileLog { path = path, next = next, writer = writer };
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
        public IEventEnumerator Replay(TransactionId last)
        {
            if (!ReferenceEquals(path, last.Path))
                throw new ArgumentException("last", "The given transaction is not for this log");
            return new EventEnumerator(this, last);
        }

        /// <summary>
        /// Atomically append data to the durable store.
        /// </summary>
        /// <param name="transaction">The transaction being written.</param>
        /// <param name="transaction">The transaction being written.</param>
        /// <returns>A stream for writing.</returns>
        /// <remarks>
        /// The <paramref name="async"/> parameter is largely optional, in that it's safe to simply
        /// provide 'false' and everything will still work.
        /// </remarks>
        public IDisposable Append(out Stream output, out TransactionId transaction)
        {
            Monitor.Enter(writer);
            transaction = new TransactionId(next, path);
            writer.Seek(next, SeekOrigin.Begin);
            output = new BoundedStream(writer, next, int.MaxValue);
            return new Appender(this) { buf = buf };
        }

        [Conditional("DEBUG")]
        public void Stats()
        {
            Console.WriteLine("FileLog ({0}): {1} bytes", Path.GetFileName(path), writer.Length);
        }

        public void Dispose()
        {
            // we could track outstanding write stream and dispose of it, but there's nothing
            // dangerous or incorrect about letting writers finish in their own time
            var x = Interlocked.Exchange(ref writer, null);
            if (x != null)
            {
                x.Close();
                //writer.Close();
                GC.SuppressFinalize(this);
            }
        }

        #region Internals
        sealed class Appender : IDisposable
        {
            internal FileStream writer;
            internal FileLog log;
            internal byte[] buf;
            public Appender(FileLog log)
            {
                this.log = log;
                this.writer = log.writer;
                //this.buf = new byte[sizeof(long)];
            }
            ~Appender()
            {
                Dispose();
            }
            public void Dispose()
            {
                var x = Interlocked.Exchange(ref writer, null);
                if (x != null)
                {
                    var length = (int)(x.Length - log.next);
                    if (length > 0)
                    {
                        // ensure stream points to the end of the written block
                        if (x.Length != x.Position)
                            x.Seek(0, SeekOrigin.End);
                        // write the entry length into the EHDR block
                        buf.Write(length, 0);
                        x.Write(buf, 0, EHDR_SIZE);
                        x.Flush();
                        log.next += length + EHDR_SIZE;
                        buf.Write(log.next);
                        // write out the new entry to the header
                        x.Seek(LHDR_TX, SeekOrigin.Begin);
                        x.Write(buf, 0, sizeof(long));
                        x.Flush();
                    }
                    Monitor.Exit(x);
                    GC.SuppressFinalize(this);
                }
            }
        }

        sealed class EventEnumerator : IEventEnumerator
        {
            FileLog log;
            long length;
            FileStream file;
            byte[] buf = new byte[EHDR_SIZE];
            // stack of block lengths from known last position to end of file
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
                    lengths.Push(x);
                    next -= x + EHDR_SIZE;
                }
                return lengths.Count == 0;
            }
        }
        #endregion
    }
}