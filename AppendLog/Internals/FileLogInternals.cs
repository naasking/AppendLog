using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        /// Construct a header from the given bytes.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="buf"></param>
        /// <param name="i"></param>
        public BlockHeader(string path, byte[] buf, int i)
        {
            Id = new Guid(BitConverter.ToInt32(buf, i), BitConverter.ToInt16(buf, i + 4), BitConverter.ToInt16(buf, i + 6),
                          buf[i + 8], buf[i + 9], buf[i + 10], buf[i + 11],
                          buf[i + 12], buf[i + 13], buf[i + 14], buf[i + 15]);
            Transaction = new TransactionId(buf.GetNextId(i + 16), path);
            var masked = BitConverter.ToUInt16(buf, i + 16 + 8);
            Type = (BlockType)(masked >> 15);
            Count = unchecked((ushort)(masked & 0x7F));
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
        /// If an internal block, counts the number of prior blocks.
        /// If a final block, counts the number of bytes in the final block.
        /// </summary>
        public ushort Count { get; private set; }

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
            Id.ToByteArray().CopyTo(buf, i);
            Transaction.Id.WriteId(buf, i + 16);
            var masked = Count | ((byte)Type) << 15;
            BitConverter.GetBytes(masked).CopyTo(buf, i);
        }

        /// <summary>
        /// Compute the position of the header
        /// </summary>
        /// <param name="pos"></param>
        /// <returns></returns>
        public long HeaderFor(long pos)
        {
            return pos & SizeMask;
        }

        /// <summary>
        /// The number of bytes in each block.
        /// </summary>
        public const int Size = 512;

        /// <summary>
        /// The number of bytes accounting for the header.
        /// </summary>
        public const int HeaderSize = 16 + 8 + 2; //GUID + TxId + Count:16
        const int SizeMask = (2 << 9) - 1;
    }

    /// <summary>
    /// The log header.
    /// </summary>
    public struct LogHeader
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="buf"></param>
        /// <param name="i"></param>
        public LogHeader(byte[] buf, int i)
        {
            var vers = buf.GetNextId(i);
            Version = new Version(BitConverter.ToInt32(buf, i), BitConverter.ToInt32(buf, i + 4), 0, BitConverter.ToInt32(buf, i + 8));
            i += 12;
            Id = new Guid(BitConverter.ToInt32(buf, i), BitConverter.ToInt16(buf, i + 4), BitConverter.ToInt16(buf, i + 6),
                          buf[i + 8], buf[i + 9], buf[i + 10], buf[i + 11],
                          buf[i + 12], buf[i + 13], buf[i + 14], buf[i + 15]);
        }

        /// <summary>
        /// The log version number.
        /// </summary>
        public Version Version { get; private set; }

        /// <summary>
        /// The log's magic number.
        /// </summary>
        public Guid Id { get; private set; }

        public void CopyTo(byte[] buf, int i)
        {
            //var vers = 
            Id.ToByteArray().CopyTo(buf, i);
        }

        static long Major(long x) { return 0x1FFFFF & (x >> (64 - 21)); }
        static long Major(int x) { return x << (64 - 21); }
        static long Minor(long x) { return 0x1FFFFF & (x >> (64 - 42)); }
        static long Minor(int x) { return x << (64 - 42); }
        static long Revision(long x) { return 0x1FFFFF & x; }
        static long Revision(int x) { return 0x1FFFFF & x; }
    }
}
