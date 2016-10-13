using System;
using System.Security.AccessControl;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

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

        const long VERSION = 0x01;

        public FileLog(string path)
        {
            if (path == null) throw new ArgumentNullException("path");
            this.path = Path.GetFullPath(path);
            // check the file's version number if file exists, else write it out
            using (var fs = File.Open(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None))
            {
                var buf = new byte[sizeof(ulong)];
                if (fs.Length == 0)
                {
                    VERSION.WriteId(buf);
                    fs.Write(buf, 0, buf.Length);
                }
                else
                {
                    fs.Read(buf, 0, buf.Length);
                    var vers = buf.FillNextId();
                    if (vers != VERSION)
                    {
                        throw new NotSupportedException(string.Format("File log expects version {0}.{1}.{2} but found version {3}.{4}.{5}",
                            Major(VERSION), Minor(VERSION), Revision(VERSION), Major(vers), Minor(vers), Revision(vers)));
                    }
                }
            }
        }

        public TransactionId First
        {
            get { return new TransactionId { Id = sizeof(ulong) }; }
        }

        public IEventEnumerator Replay(TransactionId lastEvent)
        {
            return new EventEnumerator(this, lastEvent);
        }

        public Stream Append(bool async, out TransactionId transaction)
        {
            return new TransactedStream(this, async, out transaction);
        }

        public void Dispose()
        {
            // should track outstanding write stream and dispose of it?
        }

        #region Internals
        static long Major(long x)    { return 0x1FFFFF & (x >> (64 - 21)); }
        static long Minor(long x)    { return 0x1FFFFF & (x >> (64 - 42)); }
        static long Revision(long x) { return 0x1FFFFF & x; }

        sealed class EventEnumerator : IEventEnumerator
        {
            FileLog log;
            FileStream file;
            TransactionId next;
            public TransactionId Transaction { get; internal set; }
            public Stream Stream { get; internal set; }

            ~EventEnumerator()
            {
                Dispose();
            }

            public EventEnumerator(FileLog slog, TransactionId lastEvent)
            {
                file = log.Open();
                log = slog;
                // skip version header if default TransactionId
                next = lastEvent.Id == 0 ? log.First : lastEvent;
                file.Position = next.Id;
            }

            public async Task<bool> MoveNext()
            {
                if (file == null) throw new ObjectDisposedException("IEventEnumerator");
                var tmp = new byte[sizeof(long)];
                await file.ReadAsync(tmp, 0, sizeof(long));
                var end = tmp.FillNextId();
                if (next.Id >= end)
                    return false;
                //FIXME: I'm assuming that reading/writing an array buffer at a time is atomic across threads and processes.
                //I open the write stream appropriately for atomic writes, but should test this extensively.
                await file.ReadAsync(tmp, 0, sizeof(int));
                var length = tmp[0] | tmp[1] << 8 | tmp[2] << 16 | tmp[3] << 24;
                Stream = new BoundedStream(log, file.Position, length);
                Transaction = next;
                next = new TransactionId { Id = Transaction.Id + length };
                return true;
            }

            public void Dispose()
            {
                var x = Interlocked.Exchange(ref file, null);
                if (x != null) x.Dispose();
            }
        }
        
        FileStream Open()
        {
            return new FileStream(path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
        }

        static long ReadNextId(Stream file, byte[] x)
        {
            // read the 64-bit value designating the stream length
            file.Read(x, 0, sizeof(long));
            return x.FillNextId();
        }
        
        /// <summary>
        /// A file stream that updates the transaction identifiers embedded in file upon close.
        /// </summary>
        /// <remarks>
        /// This class ensures that only a single writer accesses the file at any one time. Since
        /// it's append-only, we allow multiple readers to access the file; they are bounded above
        /// by the file's internal transaction identifier.
        /// </remarks>
        sealed class TransactedStream : FileStream
        {
            long nextId;
            byte[] buf;

            public TransactedStream(FileLog log, bool async, out TransactionId txid)
                : base(log.path, FileMode.Append, FileAccess.ReadWrite, FileShare.Read, 4096, async)
            {
                // open the file with exclusive write access and in async mode
                this.buf = new byte[sizeof(long)];
                this.nextId = ReadNextId(this, buf);
                txid = new TransactionId { Id = nextId };
                base.Position = nextId + sizeof(int); // seek past the length header
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                //FIXME: not sure if I really need this since I open the file in AppendData/Append mode w/ sequential scan
                var newpos = origin == SeekOrigin.Begin   ? nextId + offset:
                             origin == SeekOrigin.Current ? Position + offset:
                                                            Length + offset;
                if (newpos <= nextId + sizeof(int)) throw new ArgumentException("Cannot seek before the end of the log.", "offset");
                return base.Seek(offset, origin);
            }

            public override long Position
            {
                get { return base.Position; }
                set
                {
                    //FIXME: not sure if I really need this since I open the file in AppendData/Append mode
                    if (value <= nextId + sizeof(int)) throw new ArgumentOutOfRangeException("value", "Cannot seek before the end of the log.");
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

                // write out the 32-bit block length
                var length = (int)(Position - nextId);
                buf[0] = (byte)(length         & 0xFFFF);
                buf[1] = (byte)((length >> 8)  & 0xFFFF);
                buf[2] = (byte)((length >> 16) & 0xFFFF);
                buf[3] = (byte)((length >> 24) & 0xFFFF);
                base.Position = nextId;
                Write(buf, 0, sizeof(int));

                // write out the new 64-bit txid
                // position is not at the beginning of the file, so we shouldn't suffer from CLR bug fixed in .net 4.5:
                // https://connect.microsoft.com/VisualStudio/feedback/details/792434/flush-true-does-not-always-flush-when-it-should
                var txid = nextId + length;
                txid.WriteId(buf);
                base.Position = sizeof(long); // skip version #
                Write(buf, 0, sizeof(long));
                
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
                numBytes = (int)Math.Min(end - Position, numBytes);
                return base.BeginRead(array, offset, numBytes, userCallback, stateObject);
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