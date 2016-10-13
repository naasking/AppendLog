using System;
using System.Threading.Tasks;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AppendLog
{
    /// <summary>
    /// A transaction identifier.
    /// </summary>
    public struct TransactionId : IEquatable<TransactionId>
    {
        long id;

        /// <summary>
        /// The integral representation of a transaction identifier.
        /// </summary>
        internal long Id
        {
            get { return Math.Max(id, sizeof(long)); }
            set { id = value; }
        }

        /// <summary>
        /// The first transaction id possible.
        /// </summary>
        public static TransactionId First
        {
            get { return new TransactionId { id = sizeof(long) }; }
        }

        public bool Equals(TransactionId other)
        {
            return Id == other.Id;
        }

        public override bool Equals(object obj)
        {
            return obj is TransactionId && Equals((TransactionId)obj);
        }

        public override int GetHashCode()
        {
            return typeof(TransactionId).GetHashCode() ^ Id.GetHashCode();
        }

        public override string ToString()
        {
            return id.ToString("X");
        }

        public static bool operator ==(TransactionId left, TransactionId right)
        {
            return left.Id == right.Id;
        }

        public static bool operator !=(TransactionId left, TransactionId right)
        {
            return left.Id != right.Id;
        }
    }

    /// <summary>
    /// An event enumerator.
    /// </summary>
    public interface IEventEnumerator : IDisposable
    {
        /// <summary>
        /// The current transaction.
        /// </summary>
        TransactionId Transaction { get; }

        /// <summary>
        /// The stream encapsulating the event.
        /// </summary>
        Stream Stream { get; }

        /// <summary>
        /// Asynchronously advance the enumerator to the next event.
        /// </summary>
        /// <returns></returns>
        Task<bool> MoveNext();
    }

    /// <summary>
    /// Interface for an atomic, durable transaction log.
    /// </summary>
    public interface IAppendLog : IDisposable
    {
        /// <summary>
        /// The first transaction.
        /// </summary>
        TransactionId First { get;  }

        /// <summary>
        /// Enumerate the sequence of transactions since <paramref name="lastEvent"/>.
        /// </summary>
        /// <param name="lastEvent">The last event seen.</param>
        /// <returns>A sequence of transactions since the given event.</returns>
        /// <remarks>
        /// The format of the output data is simply a sequence of records:
        /// +-------------+---------------+---------------+
        /// | 64-bit TxId | 32-bit length | length * byte |
        /// +-------------+---------------+---------------+
        /// </remarks>
        IEventEnumerator Replay(TransactionId lastEvent);
        
        /// <summary>
        /// Atomically append data to the durable store.
        /// </summary>
        /// <returns>A stream for writing.</returns>
        Stream Append();
    }

    /// <summary>
    /// Extensions on <see cref="IAppendLog"/>.
    /// </summary>
    public static class AppendLogs
    {
        /// <summary>
        /// Replay a log to an output stream.
        /// </summary>
        /// <param name="log"></param>
        /// <param name="lastEvent"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        public static async Task<TransactionId> ReplayTo(this IAppendLog log, TransactionId lastEvent, Stream output)
        {
            using (var ie = log.Replay(lastEvent))
            {
                var buf = new byte[sizeof(long)];
                while (await ie.MoveNext())
                {
                    ie.Transaction.Id.WriteId(buf);
                    await output.WriteAsync(buf, 0, buf.Length);
                    ie.Stream.Length.WriteId(buf);
                    await output.WriteAsync(buf, 0, buf.Length);
                    await ie.Stream.CopyToAsync(output);
                    lastEvent = ie.Transaction;
                }
            }
            return lastEvent;
        }

        /// <summary>
        /// Replay a log to an output stream.
        /// </summary>
        /// <param name="log"></param>
        /// <param name="lastEvent"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        public static async Task<TransactionId> ReplayTo(this IAppendLog log, TransactionId lastEvent, IAppendLog target)
        {
            using (var ie = log.Replay(lastEvent))
            {
                var buf = new byte[sizeof(long)];
                while (await ie.MoveNext())
                {
                    using (var output = target.Append())
                    {
                        lastEvent = ie.Transaction;
                        lastEvent.Id.WriteId(buf);
                        await output.WriteAsync(buf, 0, buf.Length);
                        await ie.Stream.CopyToAsync(output);
                    }
                }
            }
            return lastEvent;
        }

        /// <summary>
        /// Replay events using a callback.
        /// </summary>
        /// <param name="log"></param>
        /// <param name="lastEvent"></param>
        /// <param name="forEach"></param>
        /// <returns></returns>
        public static async Task Replay(this IAppendLog log, TransactionId lastEvent, Func<TransactionId, Stream, Task<bool>> forEach)
        {
            using (var ie = log.Replay(lastEvent))
            {
                while (await ie.MoveNext())
                {
                    if (!await forEach(ie.Transaction, ie.Stream))
                        return;
                }
            }
        }

        internal static long FillNextId(this byte[] x)
        {
            return x[0] | x[1] << 8 | x[2] << 16 | x[3] << 24
                 | x[4] << 32 | x[5] << 40 | x[6] << 48 | x[7] << 56;
        }

        internal static TransactionId WriteId(this long id, byte[] x)
        {
            x[0] = (byte)(id & 0xFFFF);
            x[1] = (byte)((id >> 8) & 0xFFFF);
            x[2] = (byte)((id >> 16) & 0xFFFF);
            x[3] = (byte)((id >> 24) & 0xFFFF);
            x[4] = (byte)((id >> 32) & 0xFFFF);
            x[5] = (byte)((id >> 40) & 0xFFFF);
            x[6] = (byte)((id >> 48) & 0xFFFF);
            x[7] = (byte)((id >> 56) & 0xFFFF);
            return new TransactionId { Id = id };
        }
    }
}
