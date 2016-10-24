using System;
using System.Security.AccessControl;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Runtime.Remoting;
using AppendLog.Internals;
using Biby;

namespace AppendLog
{
    /// <summary>
    /// A streaming file-based implementation of <see cref="IAppendLog"/>.
    /// </summary>
    /// <remarks>
    /// Change log format is a sequence of fixed-size blocks, so given any file length we can compute
    /// the last flushed block and work backwards to find the last successfully committed transaction.
    /// 
    /// Blocks are of two types, internal | final. Final blocks terminate a full transaction, and
    /// consist of a sequence of internal blocks. Blocks on disk at the end of the file may be internal
    /// or final, depending on whether the last transaction successfully committed.
    /// 
    /// If the last block is final, that's the last block of the last transaction. If it's internal,
    /// we step back until we hit a final block, then we truncate the log file to that point.
    /// 
    /// The Log file format looks roughly like this:
    /// +---------+---------+--------+-----------+--------+-----------+
    /// | VERSION | MAGIC # | Data...|BlockHeader| Data...|BlockHeader| ...
    /// +---------+---------+--------+-----------+--------+-----------+
    /// Magic # is currently a <see cref="Guid"/>.
    /// </remarks>
    public sealed class FileLog : IAppendLog
    {
        readonly string path;
        FileStream writer;
        long next;
        readonly byte[] buf;
        readonly LogHeader header;

        //FIXME: creating FileStreams is expensive, so perhaps keep a pool of BoundedStreams for readers? OR
        //replace file readers with a single memory mapped file with RO-view streams

        //FIXME: format also supports arbitrary concurrent writers by extending the block header with
        //a 64-bit txid designating the first block. Each writer then obtains the next free block to
        //write via log.GetNextBlock() which does: Interlocked.Increment(ref blockno) << 9
        //
        //However, this would violate the current property that once we find a final block at the file's
        //end, all transactions prior to that entry are complete, eg. some transactions may have been
        //interrupted before flush completed.
        //
        //To solve this, the block header would have to encode some sort of consistency property,
        //designating a block where all other writers are complete so the old conditions hold before this
        //mark point. Upon opening the log file, we'd only probe backwards to this checkpoint.
        //
        //Another problem is that we have to leave the file open for writing by other processes, so
        //we'd have to use FileStream.Lock to exclude other writers (which may be expensive), or we'd
        //need a lockfile for the database to ensure only the current process can write to it.

        // The size of an entry header.
        internal const int EHDR_SIZE = sizeof(int);
        // The size of the log file header which consists of Int64 version # followed by the last committed transaction id.
        internal const long LHDR_SIZE = sizeof(long) + sizeof(long);
        // The size of the log file header which consists of Int64 version # followed by the last committed transaction id.
        internal const int TXID_POS = sizeof(long);
        // the current file format version #
        static readonly Version VERSION = new Version(0, 0, 0, 1);

        FileLog(string path, LogHeader header, FileStream writer, long next, byte[] buf)
        {
            Contract.Requires(buf != null);
            Contract.Requires(next > 0);
            Contract.Requires(!string.IsNullOrEmpty(path));
            Contract.Requires(writer != null);
            Contract.Requires(buf.Length >= BlockHeader.Size);
            Contract.Requires(next <= writer.Length);
            //Contract.Ensures(this.writer != null);
            //Contract.Ensures(this.buf != null);
            this.path = path;
            this.header = header;
            this.writer = writer;
            this.next = next;
            this.buf = buf;
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
            var fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 4096, true);
            var buf = new byte[BlockHeader.Size];
            LogHeader header;
            if (fs.Length < LogHeader.Size)
            {
                header = new LogHeader(VERSION, Guid.NewGuid());
                header.CopyTo(buf, 0);
                await fs.WriteAsync(buf, 0, LogHeader.Size);
                next = BlockHeader.BlockSize;
                await fs.FlushAsync();
            }
            else
            {
                await fs.ReadAsync(buf, 0, LogHeader.Size);
                header = new LogHeader(buf, 0);
                if (header.Version != VERSION)
                {
                    fs.Dispose();
                    throw new NotSupportedException(
                        string.Format("File log expects version {0} but found version {1}", VERSION, header.Version));
                }
                next = await LastBlock(path, fs, buf, header.Id);
                fs.SetLength(next = fs.Position); // eliminate incomplete transactions
            }
            return new FileLog(path, header, fs, next, buf);
        }

        [ContractInvariantMethod]
        void Invariants()
        {
            Contract.Invariant(buf.Length >= BlockHeader.Size);
            Contract.Invariant(next > 0);
            Contract.Invariant(writer == null || next <= writer.Length);
            Contract.Invariant(!string.IsNullOrEmpty(path));
        }

        // find the last completed transaction
        static async Task<long> LastBlock(string path, FileStream fs, byte[] buf, Guid header)
        {
            Contract.Requires(buf != null);
            Contract.Requires(path != null);
            Contract.Requires(fs != null);
            Contract.Ensures(Contract.Result<long>() > 0);
            BlockHeader blk;
            var probe = BlockHeader.Last(fs.Length);
            do
            {
                fs.Position = probe;
                probe -= BlockHeader.BlockSize;
                await fs.ReadAsync(buf, 0, BlockHeader.Size);
                blk = new BlockHeader(path, buf, 0);
            } while (blk.Type != BlockType.Final);
            if (blk.Id != header)
                throw new InvalidDataException(string.Format("Expected log GUID {0} but block {1:X} has a GUID {2}.", header, fs.Position - BlockHeader.Size, blk.Id));
            return fs.Position;
        }

        /// <summary>
        /// The first transaction in the log.
        /// </summary>
        public TransactionId First
        {
            get { return new TransactionId(BlockHeader.BlockSize, path); }
        }

        /// <summary>
        /// Enumerate the sequence of transactions since <paramref name="last"/>.
        /// </summary>
        /// <param name="last">The last event seen.</param>
        /// <returns>A sequence of transactions since the given event.</returns>
        public IEventEnumerator Replay(TransactionId last)
        {
            if (!ReferenceEquals(last.Path, path))
                throw new ArgumentException(string.Format("The given TransactionId({0}) does not designate this log ({1}).", last, First), "last");
            if (writer == null)
                throw new ObjectDisposedException("The FileLog has been disposed.");
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
            var x = writer;
            if (x == null) throw new ObjectDisposedException("FileLog has been disposed.");
            Monitor.Enter(x);
            if (writer == null) { Monitor.Exit(x); throw new ObjectDisposedException("FileLog has been disposed."); }
            tx = new TransactionId(next, path);
            return new BoundedStream(this, writer, next, buf);
        }

        public void Dispose()
        {
            // we could track outstanding write stream and dispose of it, but there's nothing
            // dangerous or incorrect about letting writers finish in their own time
            var x = Interlocked.Exchange(ref writer, null);
            if (x != null) x.Close();
        }

        #region Internals
        sealed class EventEnumerator : IEventEnumerator
        {
            readonly FileLog log;
            FileStream file;
            readonly byte[] buf;
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
                Contract.Requires(slog != null);
                Contract.Requires(slog.writer != null);
                Contract.Requires(ReferenceEquals(slog.path, last.Path));
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
                    end = buf.GetInt64();
                    file.Seek(Transaction.Id, SeekOrigin.Begin);
                }
                Debug.Assert(file.Position <= end);  // if pos > end, then read/seek logic is messed up
                if (file.Position == end)
                    return false;
                Transaction = new TransactionId(file.Position, log.path);
                await file.ReadAsync(buf, 0, EHDR_SIZE);
                length = buf.GetInt32();
                Stream = new BoundedStream2(log, file.Position, length);
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
        /// </remarks>
        sealed class BoundedStream : Stream
        {
            readonly long start;
            readonly byte[] buf;
            readonly FileStream file;
            readonly FileLog log;

            // open the file with exclusive write access and using a 4KB buffer
            public BoundedStream(FileLog log, FileStream file, long start, byte[] buf)
            {
                Contract.Requires(log != null);
                Contract.Requires(file != null);
                Contract.Requires(buf != null);
                Contract.Requires(buf.Length >= BlockHeader.Size);
                Contract.Requires(512 <= start);
                Contract.Requires(start <= file.Length);
                this.log = log;
                this.buf = buf;
                this.file = file;
                file.Position = this.start = start;
            }

            [ContractInvariantMethod]
            void Invariants()
            {
                Contract.Invariant(start >= 512);
                Contract.Invariant(file != null);
                Contract.Invariant(log != null);
                Contract.Invariant(buf != null);
                Contract.Invariant(buf.Length >= BlockHeader.Size);
                Contract.Invariant(start <= file.Position);
                Contract.Invariant(Position <= file.Position);
                Contract.Invariant(start <= file.Length);
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                var lpos = origin == SeekOrigin.Begin     ? offset:
                           origin == SeekOrigin.Current   ? file.Position + offset:
                                                            file.Length + offset;
                var pos = BlockHeader.ToAbsolute(start, lpos);
                if (pos <= start) throw new ArgumentException("Cannot seek before the beginning of the log.", "offset");
                pos = file.Seek(pos, SeekOrigin.Begin);
                return BlockHeader.ToRelative(start, pos);
            }

            public override long Position
            {
                get { return BlockHeader.ToRelative(start, file.Position); }
                set { Seek(value, SeekOrigin.Begin); }
            }

            public override long Length
            {
                get { return BlockHeader.ToRelative(start, file.Length); }
            }
            
            public override void SetLength(long value)
            {
                file.SetLength(value + start);
            }
            
            public override void Close()
            {
                // if the current position is at the very beginning of a block, then we should rewind
                // and overwrite the header block to finalize
                if (CanWrite)
                {
                    file.Seek(file.Length, SeekOrigin.Begin);
                    if (file.Length % BlockHeader.BlockSize == 0)
                        Seek(-BlockHeader.BlockSize, SeekOrigin.Current);
                    NextHeader(BlockType.Final);
                    file.Flush(true);

                    // update the cached 'next' txid, then release the lock on the underlying stream
                    Interlocked.Exchange(ref log.next, file.Position);
                    Monitor.Exit(file);
                }
                base.Close();
            }

            short GetCount(BlockType type)
            {
                Contract.Requires(Enum.IsDefined(typeof(BlockType), type));
                Contract.Ensures(Contract.Result<short>() >= 0);
                return type == BlockType.Internal
                     ? (short)((file.Position - start) / BlockHeader.BlockSize)
                     : (short)(file.Position - BlockHeader.Last(file.Position) + BlockHeader.BlockSize);
            }

            async Task WriteBlockHeaderAsync(BlockType type)
            {
                var count = GetCount(type);
                var hdr = new BlockHeader(log.header.Id, log.path, start, count, type);
                hdr.CopyTo(buf, 0);
                await file.WriteAsync(buf, 0, BlockHeader.Size);
            }

            void WriteBlockHeader(BlockType type)
            {
                var count = GetCount(type);
                var hdr = new BlockHeader(log.header.Id, log.path, start, count, type);
                hdr.CopyTo(buf, 0);
                file.Write(buf, 0, BlockHeader.Size);
            }

            long NextHeader(BlockType? type = null)
            {
                var hdr = BlockHeader.Current(file.Position);
                Contract.Assume(hdr <= file.Position);
                if (hdr == file.Position)
                {
                    if (type == null)
                        // seek past header block when reading
                        file.Seek(hdr + BlockHeader.Size, SeekOrigin.Begin);
                    else
                        WriteBlockHeader(type.Value); // write header when writing
                    hdr += BlockHeader.BlockSize;
                }
                return hdr;
            }

            public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
            {
                return file.BeginRead(buffer, offset, count, callback, state);
            }
            public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback, object state)
            {
                return file.BeginWrite(buffer, offset, count, callback, state);
            }
            public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
            {
                return file.CopyToAsync(destination, bufferSize, cancellationToken);
            }
            public override void EndWrite(IAsyncResult asyncResult)
            {
                file.EndWrite(asyncResult);
            }
            public override int Read(byte[] buffer, int offset, int count)
            {
                return file.Read(buffer, offset, count);
            }
            public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                // read blocks skipping headers until count exhausted
                var read = 0;
                do
                {
                    var hdr = NextHeader();
                    var actual = Math.Min(count, (int)(hdr - file.Position));
                    var x = await file.ReadAsync(buffer, offset, actual, cancellationToken);
                    count -= x;
                    offset += x;
                    read += x;
                } while (count >= 0);
                return read;
            }
            public override int ReadByte()
            {
                NextHeader();
                return file.ReadByte();
            }
            public override void Write(byte[] buffer, int offset, int count)
            {
                do
                {
                    var hdr = NextHeader(BlockType.Internal);
                    var actual = Math.Min(count, (int)(hdr - file.Position));
                    file.Write(buffer, offset, count);
                    offset += actual;
                    count -= actual;
                } while (count >= 0);
            }
            public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                do
                {
                    var hdr = NextHeader(BlockType.Internal);
                    var actual = Math.Min(count, (int)(hdr - file.Position));
                    await file.WriteAsync(buffer, offset, actual, cancellationToken);
                    count -= actual;
                    offset += actual;
                } while (count >= 0);
            }
            public override void WriteByte(byte value)
            {
                NextHeader(BlockType.Internal);
                file.WriteByte(value);
            }

            #region Delegated operations
            public override bool CanRead
            {
                get { return file.CanRead; }
            }
            public override bool CanWrite
            {
                get { return file.CanWrite; }
            }
            public override bool CanSeek
            {
                get { return file.CanSeek; }
            }
            public override ObjRef CreateObjRef(Type requestedType)
            {
                return file.CreateObjRef(requestedType);
            }
            public override int EndRead(IAsyncResult asyncResult)
            {
                return base.EndRead(asyncResult);
            }
            public override void Flush()
            {
                file.Flush();
            }
            public override Task FlushAsync(CancellationToken cancellationToken)
            {
                return file.FlushAsync(cancellationToken);
            }
            public override object InitializeLifetimeService()
            {
                return file.InitializeLifetimeService();
            }
            public override int ReadTimeout
            {
                get { return file.ReadTimeout; }
                set { file.ReadTimeout = value; }
            }
            public override int WriteTimeout
            {
                get { return file.WriteTimeout; }
                set { file.WriteTimeout = value; }
            }
            #endregion
        }

        /// <summary>
        /// A file stream that's bounded.
        /// </summary>
        sealed class BoundedStream2 : FileStream
        {
            readonly long start;    // absolute start position
            readonly int length;    // number of bytes in this substream

            public BoundedStream2(FileLog log, long start, int length)
                : base(log.path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)
            {
                Contract.Requires(log != null);
                Contract.Requires(start > 0);
                Contract.Requires(length > 0);
                this.start = start;
                this.length = length;
                base.Seek(start, SeekOrigin.Begin);
            }

            [ContractInvariantMethod]
            void Invariants()
            {
                Contract.Invariant(start > 0);
                Contract.Invariant(length > 0);
                Contract.Invariant(start <= base.Position);
                Contract.Invariant(Length >= Position);
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                var rpos = origin == SeekOrigin.Begin   ? start + offset:
                           origin == SeekOrigin.Current ? Position + offset:
                                                          Length + offset;
                if (rpos < start) throw new ArgumentException("Cannot seek before the beginning of the log.", "offset");
                var apos = BlockHeader.ToAbsolute(start, rpos);
                if (apos - start >= length) throw new ArgumentException("Cannot seek past the end of the log.", "offset");
                apos = base.Seek(apos, SeekOrigin.Begin);
                Contract.Assume(apos >= start);
                return BlockHeader.ToRelative(start, apos);
            }

            public override long Position
            {
                get { return BlockHeader.ToRelative(start, base.Position); }
                set { Seek(value, SeekOrigin.Begin); }
            }

            public override long Length
            {
                get { return length; }
            }

            public override void SetLength(long value)
            {
                base.SetLength(BlockHeader.ToAbsolute(start, value));
            }

            public override int Read(byte[] array, int offset, int count)
            {
                //FIXME: need to read base in chunks
                return base.Read(array, offset, (int)Math.Min(Length - Position, count));
            }

            public override IAsyncResult BeginRead(byte[] array, int offset, int numBytes, AsyncCallback userCallback, object stateObject)
            {
                //FIXME: need to read base in chunks
                return base.BeginRead(array, offset, (int)Math.Min(Length - Position, numBytes), userCallback, stateObject);
            }

            public override int ReadByte()
            {
                if (Position == Length) throw new ArgumentException("Cannot seek past the end of the log.");
                return base.ReadByte();
            }
        }
        #endregion
    }
}