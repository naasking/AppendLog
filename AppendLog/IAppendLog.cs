using System;
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
    /// Interface for an atomic, durable transaction log.
    /// </summary>
    public interface IAppendLog : IDisposable
    {
        /// <summary>
        /// Enumerate the sequence of transactions since <paramref name="lastEvent"/>.
        /// </summary>
        /// <param name="lastEvent">The last event seen.</param>
        /// <returns>A sequence of transactions since the given event.</returns>
        IEnumerable<KeyValuePair<TransactionId, Stream>> Replay(TransactionId lastEvent);

        /// <summary>
        /// Replay the log to a stream.
        /// </summary>
        /// <param name="lastEvent">The event at which to replay.</param>
        /// <param name="output">The stream to write to.</param>
        /// <returns>The last replayed transaction.</returns>
        /// <remarks>
        /// The format of the output data is simply a sequence of records:
        /// +-------------+---------------+---------------+
        /// | 64-bit TxId | 32-bit length | length * byte |
        /// +-------------+---------------+---------------+
        /// </remarks>
        //FIXME: or should I just bite the bullet and standardize the internal format using TxId's that are functions of stream position?
        TransactionId ReplayTo(TransactionId lastEvent, Stream output);

        /// <summary>
        /// Atomically append data to the durable store.
        /// </summary>
        /// <returns>A stream for writing.</returns>
        Stream Append();
    }
}
