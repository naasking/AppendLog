using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics.Contracts;
using Biby;

namespace AppendLog.Internals
{
    /// <summary>
    /// The type of the block.
    /// </summary>
    public enum BlockType : byte
    {
        /// <summary>
        /// An internal block designating the middle of a transaction stream.
        /// </summary>
        Internal = 0,

        /// <summary>
        /// The final block designating the end of a transaction stream.
        /// </summary>
        Final = 1,
    }

    /// <summary>
    /// The header describing a block in the log file.
    /// </summary>
    public struct BlockHeader
    {
        /// <summary>
        /// Construct a block header.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="path"></param>
        /// <param name="txid"></param>
        /// <param name="count"></param>
        /// <param name="type"></param>
        public BlockHeader(Guid id, string path, long txid, short count, BlockType type)
        {
            Contract.Requires(!string.IsNullOrEmpty(path));
            Contract.Requires(count >= 0);
            Contract.Requires(txid > 0);
            Contract.Requires(Enum.IsDefined(typeof(BlockType), type));
            Id = id;
            Transaction = new TransactionId(txid, path);
            Count = count;
            Type = type;
        }

        /// <summary>
        /// Construct a header from the given bytes.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="buf"></param>
        /// <param name="i"></param>
        public BlockHeader(string path, byte[] buf, int i)
        {
            Contract.Requires(buf != null);
            Contract.Requires(!string.IsNullOrEmpty(path));
            Contract.Requires(0 <= i && i + Size < buf.Length);
            Id = buf.GetGuid(i);
            var txid = buf.GetInt64(i + 16);
            if (txid <= 0) throw new ArgumentException("Invalid TransactionId: " + txid.ToString("X"));
            Transaction = new TransactionId(txid, path);
            var masked = buf.GetUInt16(i + 16 + 8);
            Type = (BlockType)(masked >> 15);
            Count = unchecked((short)(masked & 0x7F));
        }

        /// <summary>
        /// The log's magic number.
        /// </summary>
        public Guid Id { get; private set; }

        /// <summary>
        /// The Transaction this block belongs to.
        /// </summary>
        public TransactionId Transaction { get; private set; }

        /// <summary>
        /// If a final block, counts the number of bytes in the final block.
        /// If an internal block, counts the number of prior blocks.
        /// </summary>
        public short Count { get; private set; }

        /// <summary>
        /// The block type.
        /// </summary>
        public BlockType Type { get; private set; }

        /// <summary>
        /// Copy the block header to a buffer.
        /// </summary>
        /// <param name="buf"></param>
        /// <param name="i"></param>
        public void CopyTo(byte[] buf, int i)
        {
            Contract.Requires(buf != null);
            Contract.Requires(0 <= i);
            Contract.Requires(i + Size < buf.Length);
            Id.CopyTo(buf, i);
            Transaction.Id.CopyTo(buf, i + 16);
            var masked = unchecked((ushort)((ushort)Count | ((ushort)Type) << 15));
            masked.CopyTo(buf, i + 16 + 8);
        }

        /// <summary>
        /// Compute the position of the last header relative to the given position.
        /// </summary>
        /// <param name="apos">Absolute position.</param>
        /// <returns></returns>
        [Pure]
        public static long Last(long apos)
        {
            Contract.Requires(apos >= BlockSize);
            Contract.Ensures(Contract.Result<long>() >= apos);
            return apos + BlockSize * (apos / (BlockSize - Size)) - Size;
        }

        /// <summary>
        /// Compute the position of the current/upcoming header relative to the given position.
        /// </summary>
        /// <param name="apos">Absolute position.</param>
        /// <returns></returns>
        [Pure]
        public static long Current(long apos)
        {
            Contract.Requires(apos >= BlockSize);
            Contract.Ensures(Contract.Result<long>() >= apos);
            Contract.Ensures(Contract.Result<long>() > Last(apos));
            return Last(apos) + BlockSize;
        }

        /// <summary>
        /// Translate a relative offset to an absolute offset.
        /// </summary>
        /// <param name="start">The base address.</param>
        /// <param name="rpos">The relative stream position.</param>
        /// <returns>The absolute stream position, accounting for block headers.</returns>
        [Pure]
        public static long ToAbsolute(long start, long rpos)
        {
            Contract.Requires(start >= 0);
            Contract.Requires(rpos >= 0);
            Contract.Ensures(Contract.Result<long>() >= rpos);
            Contract.Ensures(Contract.Result<long>() >= start);
            Contract.Ensures(rpos == ToRelative(start, Contract.Result<long>()));
            //Contract.Assume(rpos == ToRelative(start, start + rpos + Size * rpos / (BlockSize - Size)));
            return start + rpos + Size * (rpos / (BlockSize - Size));
        }

        /// <summary>
        /// Translate an absolute offset to a relative offset.
        /// </summary>
        /// <param name="start">The base address.</param>
        /// <param name="apos">The absolute stream position.</param>
        /// <returns>The absolute stream position, accounting for block headers.</returns>
        [Pure]
        public static long ToRelative(long start, long apos)
        {
            Contract.Requires(start >= 0);
            Contract.Requires(apos >= start);
            Contract.Ensures(Contract.Result<long>() <= apos);
            Contract.Ensures(Contract.Result<long>() >= 0);
            //Contract.Ensures(apos == ToAbsolute(start, Contract.Result<long>()));
            //Contract.Assume(0 <= apos - start - Size * apos / BlockSize);
            return apos - start - Size * ((apos - start) / BlockSize);
        }

        /// <summary>
        /// The number of bytes in each block.
        /// </summary>
        public const int BlockSize = 512;
        const int BlockMask = BlockSize - 1;

        /// <summary>
        /// The number of bytes accounting for the header.
        /// </summary>
        public const int Size = 16 + 8 + 2; //GUID + TxId + Count:16
    }

    /// <summary>
    /// The log header.
    /// </summary>
    public struct LogHeader
    {
        /// <summary>
        /// Initialize the log header.
        /// </summary>
        /// <param name="version"></param>
        /// <param name="id"></param>
        public LogHeader(Version version, Guid id)
        {
            Version = version;
            Id = id;
        }

        /// <summary>
        /// Initialize a log header.
        /// </summary>
        /// <param name="buf"></param>
        /// <param name="i"></param>
        public LogHeader(byte[] buf, int i)
        {
            Contract.Requires(buf != null);
            Contract.Requires(0 <= i && i + Size < buf.Length);
            var major = buf.GetInt32(i);
            var minor = buf.GetInt32(i + 4);
            var rev = buf.GetInt32(i + 8);
            if (major < 0 || minor < 0 || rev < 0)
                throw new ArgumentException(string.Format("Invalid version number {0}.{1}.{2}", major, minor, rev));
            Version = new Version(major, minor, 0, rev);
            Id = buf.GetGuid(i + 12);
        }

        /// <summary>
        /// The log version number.
        /// </summary>
        public Version Version { get; private set; }

        /// <summary>
        /// The log's magic number.
        /// </summary>
        public Guid Id { get; private set; }

        /// <summary>
        /// Copy the header to a buffer.
        /// </summary>
        /// <param name="buf"></param>
        /// <param name="i"></param>
        public void CopyTo(byte[] buf, int i)
        {
            Contract.Requires(buf != null);
            Contract.Requires(0 <= i && i + Size < buf.Length);
            Version.Major.CopyTo(buf, i);
            Version.Minor.CopyTo(buf, i + 4);
            Version.Revision.CopyTo(buf, i + 8);
            Id.CopyTo(buf, i + 12);
        }

        public const int Size = 12 + 16; // version + guid
    }
}
