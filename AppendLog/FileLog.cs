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
        //FIXME: use 128 bit transaction ids, and store the "base" offset at the beginning of the db file.

        //FIXME: implement file rollover for compaction purposes, ie. can delete old entries by starting to
        //write to a different file than the one we're reading from, initialized with a new base offset. We
        //can then either copy the entries starting at the new base into the new file, or change the read
        //logic to also open the new file when it's done with the read file. The latter is preferable.
        string path;

        // current log file version number
        const long VERSION = 0x01;
        // The size of an entry header.
        const int EHDR_SIZE = sizeof(int);
        // The size of the log file header which consists of Int64 version # followed by the last committed transaction id.
        const long LHDR_SIZE = sizeof(long) + sizeof(long);
        // The size of the log file header which consists of Int64 version # followed by the last committed transaction id.
        const int TXID_POS = sizeof(long);

        public FileLog(string path)
        {
            if (path == null) throw new ArgumentNullException("path");
            this.path = Path.GetFullPath(path);
            // check the file's version number if file exists, else write it out
            using (var fs = File.Open(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None))
            {
                var buf = new byte[LHDR_SIZE];
                if (fs.Length <= LHDR_SIZE)
                {
                    VERSION.WriteId(buf);
                    fs.Write(buf, 0, TXID_POS);
                    LHDR_SIZE.WriteId(buf);
                    fs.Write(buf, 0, TXID_POS);
                    fs.Flush(true);
                }
                else
                {
                    fs.Read(buf, 0, buf.Length);
                    var vers = buf.GetNextId();
                    if (vers != VERSION)
                    {
                        throw new NotSupportedException(string.Format("File log expects version {0}.{1}.{2} but found version {3}.{4}.{5}",
                            Major(VERSION), Minor(VERSION), Revision(VERSION), Major(vers), Minor(vers), Revision(vers)));
                    }
                }
            }
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
        public Stream Append(bool async, out TransactionId transaction)
        {
            return new TransactedStream(this, async, out transaction);
        }

        public void Dispose()
        {
            // we could track outstanding write stream and dispose of it, but there's nothing
            // dangerous or incorrect about letting writers finish in their own time
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
                    file.Position = Transaction.Id;
                }
                //NOTE: I'm assuming that reading/writing a small array at a time is atomic across threads and processes.
                //I open the write stream appropriately for atomic writes, but should test this extensively.
                Debug.Assert(file.Position <= end);  // if pos > end, then read/seek logic is messed up
                if (file.Position == end)
                    return false;
                Transaction = new TransactionId { Id = file.Position };
                await file.ReadAsync(buf, 0, EHDR_SIZE);
                length = buf.GetLength();
                Stream = new BoundedStream(log, file.Position, length);
                return true;
            }

            public void Dispose()
            {
                var x = Interlocked.Exchange(ref file, null);
                if (x != null) x.Dispose();
            }
        }
        
        /// <summary>
        /// A file stream that updates the transaction identifiers embedded in file upon close.
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
        sealed class TransactedStream : FileStream
        {
            long start;
            byte[] buf;

            // open the file with exclusive write access and using a 4KB buffer
            public TransactedStream(FileLog log, bool async, out TransactionId txid)
                : base(log.path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, 4096, async)
            {
                this.buf = new byte[TXID_POS];
                // read the log header to initialize the stream
                base.Position = TXID_POS;
                Read(buf, 0, TXID_POS);
                var next = buf.GetNextId();
                Position = next;
                txid = new TransactionId { Id = next };
                // seek past the entry header to start writing
                base.Position = this.start = next + EHDR_SIZE;
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                var newpos = origin == SeekOrigin.Begin   ? start + offset:
                             origin == SeekOrigin.Current ? Position + offset:
                                                            Length + offset;
                if (newpos <= start) throw new ArgumentException("Cannot seek before the end of the log.", "offset");
                return base.Seek(offset, origin);
            }

            public override long Position
            {
                get { return base.Position; }
                set
                {
                    if (value <= start) throw new ArgumentOutOfRangeException("value", "Cannot seek before the end of the log.");
                    base.Position = value;
                }
            }

            public override void Close()
            {
                //NOTE FS Atomicity: http://danluu.com/file-consistency/
                //Moral: the OS can reorder writes such that the write at 0 could happen first, even if the write
                //is called 2nd. If a crash/power loss happens btw the write at 0 and the writes to the end, the
                //header is then just pointing at garbage. So call flush to sync data to disk, then write header.
                Flush(true);

                // write out the 32-bit length block
                var length = (int)(Position - start);
                length.WriteLength(buf);
                base.Position = start - EHDR_SIZE;
                Write(buf, 0, EHDR_SIZE);

                // write out the new 64-bit txid in the log header, just after the version number
                var txid = start + length;
                txid.WriteId(buf);
                base.Position = TXID_POS; // write txid after version #
                Write(buf, 0, TXID_POS);
                
                // wait until all data is written to disk
                Flush(true);
                base.Close();
            }
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
                base.Position = start;
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                var newpos = origin == SeekOrigin.Begin   ? start + offset:
                             origin == SeekOrigin.Current ? Position + offset:
                                                            Length + offset;
                if (newpos < start) throw new ArgumentException("Cannot seek before the end of the log.", "offset");
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