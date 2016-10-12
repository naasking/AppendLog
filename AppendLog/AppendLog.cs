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
    public sealed class StreamedFileLog : IAppendLog
    {
        //FIXME: use 128 bit transaction ids, and store the "base" offset at the beginning of the db file.

        //FIXME: implement file rollover for compaction purposes, ie. can delete old entries by starting to
        //write to a different file than the one we're reading from, initialized with a new base offset. We
        //can then either copy the entries starting at the new base into the new file, or change the read
        //logic to also open the new file when it's done with the read file. The latter is preferable.

        //FIXME: I should place a db version number at the beginning so I can perform future upgrades if needed.
        //Also, this means the atomic update to the nextId doesn't take place at Pos=0, so no need to adjust it
        //before flush.

        //FIXME: need to add CRC to the headers to mitigate corruption issues.
        string path;

        public StreamedFileLog(string path)
        {
            if (path == null) throw new ArgumentNullException("path");
            this.path = Path.GetFullPath(path);
        }

        sealed class EventEnumerator : IEventEnumerator
        {
            FileStream file;
            TransactionId next;
            public TransactionId Transaction { get; internal set; }
            public Stream Stream { get; internal set; }

            public EventEnumerator(StreamedFileLog log, TransactionId lastEvent)
            {
                file = log.Open();
                next = lastEvent;
                file.Position = next.Id;
            }

            public async Task<bool> MoveNext()
            {
                if (file == null) throw new ObjectDisposedException("IEventEnumerator");
                var tmp = new byte[sizeof(long)];
                await file.ReadAsync(tmp, 0, sizeof(long));
                var end = FillNextId(tmp);
                if (next.Id >= end)
                    return false;
                //FIXME: I'm assuming that reading/writing an array buffer at a time is atomic across threads and processes.
                //I open the write stream appropriately for atomic writes, but should test this extensively.
                await file.ReadAsync(tmp, 0, sizeof(int));
                //FIXME: replace in-memory buffering with a bounded stream, ie.
                // new BoundedStream(file, start:next.Id+header, end:next.Id+header+length);
                var length = tmp[0] | tmp[1] << 8 | tmp[2] << 16 | tmp[3] << 24;
                var data = new byte[length];
                var read = 0;
                do read += await file.ReadAsync(data, 0, length);
                while (read < length);
                Stream = new MemoryStream(data, false);
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

        public IEventEnumerator Replay(TransactionId lastEvent)
        {
            return new EventEnumerator(this, lastEvent);
        }
        
        FileStream Open()
        {
            return new FileStream(path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
        }

        static long ReadNextId(Stream file, byte[] x)
        {
            // read the 64-bit value designating the stream length
            file.Read(x, 0, sizeof(long));
            return FillNextId(x);
        }

        static long FillNextId(byte[] x)
        {
            return x[0] | x[1] << 8 | x[2] << 16 | x[3] << 24
                 | x[4] << 32 | x[5] << 40 | x[6] << 48 | x[7] << 56;
        }

        static TransactionId WriteId(long id, byte[] x)
        {
            x[0] = (byte)(id         & 0xFFFF);
            x[1] = (byte)((id >> 8)  & 0xFFFF);
            x[2] = (byte)((id >> 16) & 0xFFFF);
            x[3] = (byte)((id >> 24) & 0xFFFF);
            x[4] = (byte)((id >> 32) & 0xFFFF);
            x[5] = (byte)((id >> 40) & 0xFFFF);
            x[6] = (byte)((id >> 48) & 0xFFFF);
            x[7] = (byte)((id >> 56) & 0xFFFF);
            return new TransactionId { Id = id };
        }

        public Stream Append()
        {
            return new TransactedStream(this);
        }

        public void Dispose()
        {
            // should track outstanding write stream and dispose of it?
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
            StreamedFileLog log;
            long nextId;
            byte[] buf;

            public TransactedStream(StreamedFileLog log)
                : base(log.path, FileMode.Append, FileSystemRights.AppendData, FileShare.Read, 4070, FileOptions.SequentialScan)
            {
                // opened stream using atomic writes:
                // http://stackoverflow.com/questions/1862309/how-can-i-do-an-atomic-write-append-in-c-or-how-do-i-get-files-opened-with-the
                this.log = log;
                this.buf = new byte[sizeof(long)];
                this.nextId = ReadNextId(this, buf);
                base.Position = nextId + sizeof(int); // seek past the length header
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                //FIXME: not sure if I really need this since I open the file in AppendData/Append mode w/ sequential scan
                var newpos = origin == SeekOrigin.Begin   ? offset:
                             origin == SeekOrigin.Current ? Position + offset:
                                                            Length + offset;
                if (newpos <= nextId + sizeof(int)) throw new ArgumentOutOfRangeException("offset", "Cannot seek before the end of the log.");
                return base.Seek(offset, origin);
            }

            public override long Position
            {
                get { return base.Position; }
                set
                {
                    //FIXME: not sure if I really need this since I open the file in AppendData/Append mode
                    if (value <= nextId + +sizeof(int)) throw new ArgumentOutOfRangeException("value", "Cannot seek before the end of the log.");
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
                var txid = nextId + length;
                WriteId(txid, buf);
                base.Position = 0;
                Write(buf, 0, sizeof(long));

                // ensure position is not at the beginning of the file due to CLR bug fixed in .net 4.5:
                // https://connect.microsoft.com/VisualStudio/feedback/details/792434/flush-true-does-not-always-flush-when-it-should
                ++base.Position;

                // wait until all data is written to disk
                Flush(true);
                base.Close();
            }
        }
    }
}