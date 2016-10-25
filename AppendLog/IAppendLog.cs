using System;
using System.Threading.Tasks;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics.Contracts;
using System.Runtime.InteropServices;

namespace AppendLog
{
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
        /// The stream containing the current event.
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
        /// The first transaction in the log.
        /// </summary>
        TransactionId First { get; }

        /// <summary>
        /// Enumerate the sequence of transactions since <paramref name="last"/>.
        /// </summary>
        /// <param name="last">The last event seen.</param>
        /// <returns>A sequence of transactions since the given event.</returns>
        IEventEnumerator Replay(TransactionId last);
        
        /// <summary>
        /// Atomically append data to the durable store.
        /// </summary>
        /// <param name="transaction">The transaction being written.</param>
        /// <returns>A stream for appending to the log.</returns>
        IDisposable Append(out Stream output, out TransactionId transaction);
    }
    
    /// <summary>
    /// Extensions on <see cref="IAppendLog"/>.
    /// </summary>
    public static class AppendLogs
    {
        ///// <summary>
        ///// Replay a log to an output stream.
        ///// </summary>
        ///// <param name="log"></param>
        ///// <param name="lastEvent"></param>
        ///// <param name="output"></param>
        ///// <returns></returns>
        //public static async Task<TransactionId> ReplayTo(this IAppendLog log, TransactionId lastEvent, IAppendLog target)
        //{
        //    using (var ie = log.Replay(lastEvent))
        //    {
        //        var buf = new byte[sizeof(long)];
        //        while (await ie.MoveNext())
        //        {
        //            TransactionId tx;
        //            Stream output;
        //            using (target.Append(out output, out tx))
        //            {
        //                lastEvent = ie.Transaction;
        //                Debug.Assert(tx == lastEvent);
        //                buf.Write(lastEvent.Id);
        //                await output.WriteAsync(buf, 0, buf.Length);
        //                await ie.Stream.CopyToAsync(output);
        //            }
        //        }
        //    }
        //    return lastEvent;
        //}

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

        #region Internal marshalling to/from byte arrays in big endian format
        public static long ReadInt64(this byte[] x, int i = 0)
        {
            Contract.Requires(x != null);
            Contract.Requires(i + 7 < x.Length);
            Contract.Requires(i >= 0);
            unchecked
            {
                return (long)(BitConverter.IsLittleEndian
                     ? (ulong)x[0 + i] << 56 | (ulong)x[1 + i] << 48 | (ulong)x[2 + i] << 40 | (ulong)x[3 + i] << 32 | (ulong)x[4 + i] << 24 | (ulong)x[5 + i] << 16 | (ulong)x[6 + i] <<  8 | (ulong)x[7 + i]
                     : (ulong)x[0 + i]       | (ulong)x[1 + i] <<  8 | (ulong)x[2 + i] << 16 | (ulong)x[3 + i] << 24 | (ulong)x[4 + i] << 32 | (ulong)x[5 + i] << 40 | (ulong)x[6 + i] << 48 | (ulong)x[7 + i] << 56);
            }
        }

        public static int ReadInt32(this byte[] x, int i = 0)
        {
            Contract.Requires(x != null);
            Contract.Requires(i + 3 < x.Length);
            Contract.Requires(i >= 0);
            unchecked
            {
                return (int)(BitConverter.IsLittleEndian
                     ? (uint)x[0 + i] << 24 | (uint)x[1 + i] << 16 | (uint)x[2 + i] <<  8 | (uint)x[3 + i]
                     : (uint)x[0 + i]       | (uint)x[1 + i] <<  8 | (uint)x[2 + i] << 16 | (uint)x[3 + i] << 24);
            }
        }

        public static void Write(this byte[] x, int len, int i = 0)
        {
            Contract.Requires(x != null);
            Contract.Requires(i + 3 < x.Length);
            Contract.Requires(i >= 0);
            unchecked
            {
                if (BitConverter.IsLittleEndian)
                {
                    x[0 + i] = (byte)((len >> 24) & 0xFFFF);
                    x[1 + i] = (byte)((len >> 16) & 0xFFFF);
                    x[2 + i] = (byte)((len >>  8) & 0xFFFF);
                    x[3 + i] = (byte)(len & 0xFFFF);
                }
                else
                {
                    x[0 + i] = (byte)(len & 0xFFFF);
                    x[1 + i] = (byte)((len >>  8) & 0xFFFF);
                    x[2 + i] = (byte)((len >> 16) & 0xFFFF);
                    x[3 + i] = (byte)((len >> 24) & 0xFFFF);
                }
            }
        }

        public static void Write(this byte[] x, long id, int i = 0)
        {
            Contract.Requires(x != null);
            Contract.Requires(i + 7 < x.Length);
            Contract.Requires(i >= 0);
            unchecked
            {
                if (BitConverter.IsLittleEndian)
                {
                    x[0 + i] = (byte)((id >> 56) & 0xFFFF);
                    x[1 + i] = (byte)((id >> 48) & 0xFFFF);
                    x[2 + i] = (byte)((id >> 40) & 0xFFFF);
                    x[3 + i] = (byte)((id >> 32) & 0xFFFF);
                    x[4 + i] = (byte)((id >> 24) & 0xFFFF);
                    x[5 + i] = (byte)((id >> 16) & 0xFFFF);
                    x[6 + i] = (byte)((id >>  8) & 0xFFFF);
                    x[7 + i] = (byte)(id & 0xFFFF);
                }
                else
                {
                    x[0 + i] = (byte)(id & 0xFFFF);
                    x[1 + i] = (byte)((id >>  8) & 0xFFFF);
                    x[2 + i] = (byte)((id >> 16) & 0xFFFF);
                    x[3 + i] = (byte)((id >> 24) & 0xFFFF);
                    x[4 + i] = (byte)((id >> 32) & 0xFFFF);
                    x[5 + i] = (byte)((id >> 40) & 0xFFFF);
                    x[6 + i] = (byte)((id >> 48) & 0xFFFF);
                    x[7 + i] = (byte)((id >> 56) & 0xFFFF);
                }
            }
        }
        #endregion
    }
}
