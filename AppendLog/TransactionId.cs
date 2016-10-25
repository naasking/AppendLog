using System;
using System.Diagnostics.Contracts;
using System.Runtime.Serialization;

namespace AppendLog
{
    /// <summary>
    /// A transaction identifier.
    /// </summary>
    [Serializable]
    public struct TransactionId : IEquatable<TransactionId>, ISerializable
    {
        readonly long id;
        readonly string path;

        internal TransactionId(long id, string path)
        {
            this.id = Math.Max(id, FileLog.LHDR_SIZE);
            this.path = path;
        }

        /// <summary>
        /// The integral representation of a transaction identifier.
        /// </summary>
        public long Id
        {
            get { return id; }
        }

        /// <summary>
        /// The log file designated by this transaction.
        /// </summary>
        public string Path
        {
            get { return path; }
        }
        
        /// <summary>
        /// Check transaction identifier for equality.
        /// </summary>
        /// <param name="other">The transaction to compare against.</param>
        /// <returns>True if they are equal, false otherwise.</returns>
        public bool Equals(TransactionId other)
        {
            return Id == other.Id && ReferenceEquals(path, other.path);
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
            return string.Format("{0}:{1:X}", System.IO.Path.GetFileName(path), Id);
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

        #region Serialization interface
        TransactionId(SerializationInfo info, StreamingContext context)
        {
            Contract.Requires(info != null);
            id = info.GetInt64(nameof(id));
            var x = info.GetString(nameof(path));
            if (x == null) throw new ArgumentNullException("path");
            path = string.Intern(System.IO.Path.GetFullPath(x));
        }

        void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue(nameof(id), Id);
            info.AddValue(nameof(path), System.IO.Path.GetFileName(path));
        }
        #endregion
    }
}
