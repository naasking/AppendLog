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
        public BlockHeader(Guid id, string path, long txid, short count, BlockType type)
        {
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
            Contract.Requires(path != null);
            Contract.Requires(0 <= i && i + Size < buf.Length);
            Id = buf.GetGuid(i);
            Transaction = new TransactionId(buf.GetInt64(i + 16), path);
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
            Contract.Requires(0 <= i && i + Size < buf.Length);
            Id.CopyTo(buf, i);
            Transaction.Id.WriteId(buf, i + 16);
            var masked = unchecked((ushort)((ushort)Count | ((ushort)Type) << 15));
            masked.CopyTo(buf, i + 16 + 8);
        }

        /// <summary>
        /// Compute the position of the header
        /// </summary>
        /// <param name="pos"></param>
        /// <returns></returns>
        public long HeaderFor(long pos)
        {
            return pos & BlockMask;
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
        /// Initialize the log hedaer.
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
            Contract.Requires(0 <= i && i + 27 < buf.Length);
            Version = new Version(buf.GetInt32(i), buf.GetInt32(i + 4), 0, buf.GetInt32(i + 8));
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
            Contract.Requires(0 <= i && i + 27 < buf.Length);
            Version.Major.CopyTo(buf, i);
            Version.Minor.CopyTo(buf, i + 4);
            Version.Revision.CopyTo(buf, i + 8);
            Id.CopyTo(buf, i + 12);
        }
    }
}
