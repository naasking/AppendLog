using System;
using System.Threading.Tasks;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
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
        /// <remarks>
        /// The <paramref name="async"/> parameter is largely optional, in that it's safe to simply
        /// provide 'false' and everything should still work.
        /// </remarks>
        Stream Append(out TransactionId transaction);
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
        //            using (var output = target.Append(out tx))
        //            {
        //                lastEvent = ie.Transaction;
        //                Debug.Assert(tx == lastEvent);
        //                lastEvent.Id.WriteId(buf);
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
        internal static long GetNextId(this byte[] x, int i = 0)
        {
            return BitConverter.IsLittleEndian
                 ? x[7 + i] | x[6 + i] << 8 | x[5 + i] << 16 | x[4 + i] << 24 | x[3 + i] << 32 | x[2 + i] << 40 | x[1 + i] << 48 | x[0 + i] << 56
                 : x[0 + i] | x[1 + i] << 8 | x[2 + i] << 16 | x[3 + i] << 24 | x[4 + i] << 32 | x[5 + i] << 40 | x[6 + i] << 48 | x[7 + i] << 56;
        }
        
        internal static int GetLength(this byte[] x, int i = 0)
        {
            return BitConverter.IsLittleEndian
                 ? x[3 + i] | x[2 + i] << 8 | x[1 + i] << 16 | x[0 + i] << 24
                 : x[0 + i] | x[1 + i] << 8 | x[2 + i] << 16 | x[3 + i] << 24;
        }

        internal static void WriteLength(this int len, byte[] x)
        {
            if (BitConverter.IsLittleEndian)
            {
                x[3] = (byte)(len         & 0xFFFF);
                x[2] = (byte)((len >>  8) & 0xFFFF);
                x[1] = (byte)((len >> 16) & 0xFFFF);
                x[0] = (byte)((len >> 24) & 0xFFFF);
            }
            else
            {
                x[0] = (byte)(len         & 0xFFFF);
                x[1] = (byte)((len >>  8) & 0xFFFF);
                x[2] = (byte)((len >> 16) & 0xFFFF);
                x[3] = (byte)((len >> 24) & 0xFFFF);
            }
        }

        internal static void WriteId(this long id, byte[] x)
        {
            if (BitConverter.IsLittleEndian)
            {
                x[7] = (byte)(id         & 0xFFFF);
                x[6] = (byte)((id >>  8) & 0xFFFF);
                x[5] = (byte)((id >> 16) & 0xFFFF);
                x[4] = (byte)((id >> 24) & 0xFFFF);
                x[3] = (byte)((id >> 32) & 0xFFFF);
                x[2] = (byte)((id >> 40) & 0xFFFF);
                x[1] = (byte)((id >> 48) & 0xFFFF);
                x[0] = (byte)((id >> 56) & 0xFFFF);
            }
            else
            {
                x[0] = (byte)(id         & 0xFFFF);
                x[1] = (byte)((id >>  8) & 0xFFFF);
                x[2] = (byte)((id >> 16) & 0xFFFF);
                x[3] = (byte)((id >> 24) & 0xFFFF);
                x[4] = (byte)((id >> 32) & 0xFFFF);
                x[5] = (byte)((id >> 40) & 0xFFFF);
                x[6] = (byte)((id >> 48) & 0xFFFF);
                x[7] = (byte)((id >> 56) & 0xFFFF);
            }
        }
        #endregion
    }
}
