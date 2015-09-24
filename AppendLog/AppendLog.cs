using System;
using System.Security.AccessControl;
using System.Threading;
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
        string path;

        public StreamedFileLog(string path)
        {
            if (path == null) throw new ArgumentNullException("path");
            this.path = Path.GetFullPath(path);
        }

        public IEnumerable<KeyValuePair<TransactionId, Stream>> Replay(TransactionId lastEvent)
        {
            using (var file = Open())
            {
                var tmp = new byte[sizeof(long)];
                var end = ReadNextId(file, tmp);
                file.Position = lastEvent.Id;
                //FIXME: I'm assuming that reading/writing an array buffer at a time is atomic across threads and processes.
                //I open the write stream appropriately for atomic writes, but should test this extensively.
                while (file.Position < end)
                {
                    file.Read(tmp, 0, sizeof(int));
                    var length = tmp[0] | tmp[1] << 8 | tmp[2] << 16 | tmp[3] << 24;
                    var data = new byte[length];
                    file.Read(data, 0, length);
                    yield return new KeyValuePair<TransactionId, Stream>(lastEvent, new MemoryStream(data, false));
                    lastEvent.Id += length;
                }
            }
        }

        public TransactionId ReplayTo(TransactionId lastEvent, Stream output)
        {
            const int header = sizeof(long) + sizeof(int);
            TransactionId finalId;
            using (var file = Open())
            {
                var buf = new byte[4096];
                var nextId = ReadNextId(file, buf);
                finalId = new TransactionId { Id = nextId };
                file.Position = lastEvent.Id;
                var read = sizeof(long);
                while (file.Position < nextId)
                {
                    finalId = WriteId(file.Position, buf);
                    // keep reading until the full length header is read
                    do read += file.Read(buf, read, header - read);
                    while (read < header);
                    var length = buf[sizeof(long)]           | buf[sizeof(long) + 1] << 8
                               | buf[sizeof(long) + 2] << 16 | buf[sizeof(long) + 3] << 24;
                    // read and write either the full length, or whatever will fit into the buffer
                    var rem = Math.Min(length, buf.Length - read);
                    read += file.Read(buf, read, rem);
                    output.Write(buf, 0, read);
                    // loop until all bytes read/writtten, but don't subtract the header from the length
                    for (length -= read - header; length > 0; length -= read)
                    {
                        read = file.Read(buf, 0, Math.Min(length, buf.Length));
                        output.Write(buf, 0, read);
                    }
                }
            }
            return finalId;
        }

        FileStream Open()
        {
            return new FileStream(path, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
        }

        static long ReadNextId(Stream file, byte[] x)
        {
            // read the 64-bit value designating the stream length
            file.Read(x, 0, sizeof(long));
            return x[0]       | x[1] << 8  | x[2] << 16 | x[3] << 24
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