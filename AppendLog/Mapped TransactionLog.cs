using System;
using System.Threading;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace EventSourcing
{
    /// <summary>
    /// A file-based implementation of <see cref="ITransactionLog"/>.
    /// </summary>
    public sealed class MappedFileLog : ITransactionLog
    {
        MemoryMappedFile file;

        public MappedFileLog(string path)
        {
            this.file = MemoryMappedFile.CreateOrOpen(path, long.MaxValue, MemoryMappedFileAccess.ReadWrite, MemoryMappedFileOptions.DelayAllocatePages, null, HandleInheritability.None);
        }

        //FIXME: replay/append should support both view streams and view accessors? Streams are generally used for serialization.
        //FIXME: mmap file may be unsuitable: https://connect.microsoft.com/VisualStudio/feedback/details/792434/flush-true-does-not-always-flush-when-it-should

        public IEnumerable<KeyValuePair<TransactionId, Stream>> Replay(TransactionId lastEvent)
        {
            using (var view = file.CreateViewAccessor())
            {
                var end = view.ReadInt64(0);
                var pos = lastEvent.Id;
                while (lastEvent.Id < end)
                {
                    var length = view.ReadInt32(lastEvent.Id);
                    yield return new KeyValuePair<TransactionId, Stream>(lastEvent, file.CreateViewStream(lastEvent.Id, length, MemoryMappedFileAccess.Read));
                    lastEvent.Id += length;
                }
            }
        }

        public Stream Append()
        {
            long end;
            using (var view = file.CreateViewAccessor())
                end = view.ReadInt64(0);
            //FIXME: this needs to update the base txid on dispose/close
            return file.CreateViewStream(end, long.MaxValue - end, MemoryMappedFileAccess.ReadWrite);
        }

        public void Dispose()
        {
            var x = Interlocked.Exchange(ref file, null);
            if (x != null) x.Dispose();
        }

        ///// <summary>
        ///// A file stream that updates the transaction identifiers embedded in file upon close.
        ///// </summary>
        ///// <remarks>
        ///// This class ensures that only a single writer accesses the file at any one time. Since
        ///// it's append-only, we allow multiple readers to access the file; they are bounded above
        ///// by the file's internal transaction identifier.
        ///// </remarks>
        //sealed class TransactedStream : FileStream
        //{
        //    FileLog log;
        //    long nextId;

        //    public TransactedStream(FileLog log)
        //        : base(log.path, FileMode.Append, FileAccess.Write, FileShare.Read)
        //    {
        //        this.log = log;
        //        this.nextId = ReadNextId(this);
        //    }

        //    public override void Close()
        //    {
        //        // write out the 32-bit block length
        //        var id = Position + sizeof(int);
        //        var length = (int)(id - nextId);
        //        Seek(nextId, SeekOrigin.Begin);
        //        WriteByte((byte)(length & 0xFFFF));
        //        WriteByte((byte)((length >> 8) & 0xFFFF));
        //        WriteByte((byte)((length >> 16) & 0xFFFF));
        //        WriteByte((byte)((length >> 24) & 0xFFFF));

        //        base.Close();
        //    }
        //}
    }
}