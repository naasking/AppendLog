using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        public long Id
        {
            get { return Math.Max(id, FileLog.LHDR_SIZE); }
            internal set { id = value; }
        }

        /// <summary>
        /// The first transaction id possible.
        /// </summary>
        public static TransactionId First
        {
            get { return new TransactionId { id = FileLog.LHDR_SIZE }; }
        }

        /// <summary>
        /// Check transaction identifier for equality.
        /// </summary>
        /// <param name="other">The transaction to compare against.</param>
        /// <returns>True if they are equal, false otherwise.</returns>
        public bool Equals(TransactionId other)
        {
            return Id == other.Id;
        }

        /// <summary>
        /// Check transaction identifier for equality.
        /// </summary>
        /// <param name="obj">The transaction to compare against.</param>
        /// <returns>True if they are equal, false otherwise.</returns>
        public override bool Equals(object obj)
        {
            return obj is TransactionId && Equals((TransactionId)obj);
        }

        /// <summary>
        /// Compute the transaction's hash code.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return typeof(TransactionId).GetHashCode() ^ Id.GetHashCode();
        }

        /// <summary>
        /// Return a string representation of the transaction.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return Id.ToString("X");
        }

        /// <summary>
        /// Compare for equality.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <returns></returns>
        public static bool operator ==(TransactionId left, TransactionId right)
        {
            return left.Id == right.Id;
        }

        /// <summary>
        /// Compare for equality.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <returns></returns>
        public static bool operator !=(TransactionId left, TransactionId right)
        {
            return left.Id != right.Id;
        }
    }
}
