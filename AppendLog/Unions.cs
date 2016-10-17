using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;

namespace Biby
{
    /// <summary>
    /// Represents a 16-bit union.
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    public struct Union16 : IEquatable<Union16>
    {
        /// <summary>
        /// The unsigned fragment of the union.
        /// </summary>
        [FieldOffset(0)]
        [CLSCompliant(false)]
        public ushort Unsigned;

        /// <summary>
        /// The signed fragment of the union.
        /// </summary>
        [FieldOffset(0)]
        public short Signed;

        /// <summary>
        /// Construct a flat 16-bit union from a signed value.
        /// </summary>
        /// <param name="value">The signed value.</param>
        public Union16(short value) : this() { Signed = value; }

        /// <summary>
        /// Construct a flat 16-bit union from an unsigned value.
        /// </summary>
        /// <param name="value">The unsigned value.</param>
        [CLSCompliant(false)]
        public Union16(ushort value) : this() { Unsigned = value; }

        /// <summary>
        /// Compare for equality.
        /// </summary>
        /// <param name="other">The instance to compare.</param>
        /// <returns>True if equal.</returns>
        [System.Diagnostics.Contracts.Pure]
        public bool Equals(Union16 other)
        {
            return Unsigned == other.Unsigned;
        }

        /// <summary>
        /// Compare for equality.
        /// </summary>
        /// <param name="obj">The instance to compare.</param>
        /// <returns>True if equal.</returns>
        public override bool Equals(object obj)
        {
            return obj is Union16
                && Equals((Union16)obj);
        }

        /// <summary>
        /// Compute a hash code.
        /// </summary>
        /// <returns>The hash code for this value.</returns>
        public override int GetHashCode()
        {
            return typeof(Union16).GetHashCode()
                 ^ Unsigned.GetHashCode();
        }

        /// <summary>
        /// Compare for equality.
        /// </summary>
        /// <param name="left">The left value.</param>
        /// <param name="right">The right value.</param>
        /// <returns>True if equal.</returns>
        public static bool operator ==(Union16 left, Union16 right)
        {
            return left.Equals(right);
        }

        /// <summary>
        /// Compare for inequality.
        /// </summary>
        /// <param name="left">The left value.</param>
        /// <param name="right">The right value.</param>
        /// <returns>True if not equal.</returns>
        public static bool operator !=(Union16 left, Union16 right)
        {
            return !(left == right);
        }
    }

    /// <summary>
    /// Represents a 32-bit union.
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    public struct Union32 : IEquatable<Union32>
    {
        /// <summary>
        /// The unsigned fragment of the union.
        /// </summary>
        [FieldOffset(0)]
        [CLSCompliant(false)]
        public uint Unsigned;

        /// <summary>
        /// The signed fragment of the union.
        /// </summary>
        [FieldOffset(0)]
        public int Signed;

        /// <summary>
        /// The single fragment of the union.
        /// </summary>
        [FieldOffset(0)]
        public float Single;

        /// <summary>
        /// Construct a flat 32-bit union from a signed value.
        /// </summary>
        /// <param name="value">The signed value.</param>
        public Union32(int value) : this() { Signed = value; }

        /// <summary>
        /// Construct a flat 32-bit union from an unsigned value.
        /// </summary>
        /// <param name="value">The unsigned value.</param>
        [CLSCompliant(false)]
        public Union32(uint value) : this() { Unsigned = value; }

        /// <summary>
        /// Construct a flat 32-bit union from an unsigned value.
        /// </summary>
        /// <param name="value">The single-precision floating point value.</param>
        public Union32(float value) : this() { Single = value; }

        /// <summary>
        /// Compare for equality.
        /// </summary>
        /// <param name="other">The instance to compare.</param>
        /// <returns>True if equal.</returns>
        [System.Diagnostics.Contracts.Pure]
        public bool Equals(Union32 other)
        {
            return Unsigned == other.Unsigned;
        }

        /// <summary>
        /// Compare for equality.
        /// </summary>
        /// <param name="obj">The instance to compare.</param>
        /// <returns>True if equal.</returns>
        public override bool Equals(object obj)
        {
            return obj is Union32
                && Equals((Union32)obj);
        }

        /// <summary>
        /// Compute a hash code.
        /// </summary>
        /// <returns>The hash code for this value.</returns>
        public override int GetHashCode()
        {
            return typeof(Union32).GetHashCode()
                 ^ Unsigned.GetHashCode();
        }

        /// <summary>
        /// Compare for equality.
        /// </summary>
        /// <param name="left">The left value.</param>
        /// <param name="right">The right value.</param>
        /// <returns>True if equal.</returns>
        public static bool operator ==(Union32 left, Union32 right)
        {
            return left.Equals(right);
        }

        /// <summary>
        /// Compare for inequality.
        /// </summary>
        /// <param name="left">The left value.</param>
        /// <param name="right">The right value.</param>
        /// <returns>True if not equal.</returns>
        public static bool operator !=(Union32 left, Union32 right)
        {
            return !(left == right);
        }
    }

    /// <summary>
    /// Represents a 64-bit union.
    /// </summary>
    [StructLayout(LayoutKind.Explicit)]
    public struct Union64 : IEquatable<Union64>
    {
        /// <summary>
        /// The unsigned fragment of the union.
        /// </summary>
        [FieldOffset(0)]
        [CLSCompliant(false)]
        public ulong Unsigned;

        /// <summary>
        /// The signed fragment of the union.
        /// </summary>
        [FieldOffset(0)]
        public long Signed;

        /// <summary>
        /// The double-precision floating point fragment of the union.
        /// </summary>
        [FieldOffset(0)]
        public double Double;

        /// <summary>
        /// Construct a flat 64-bit union from a signed value.
        /// </summary>
        /// <param name="value">The signed value.</param>
        public Union64(long value) : this() { Signed = value; }

        /// <summary>
        /// Construct a flat 64-bit union from an unsigned value.
        /// </summary>
        /// <param name="value">The unsigned value.</param>
        [CLSCompliant(false)]
        public Union64(ulong value) : this() { Unsigned = value; }

        /// <summary>
        /// Construct a flat 64-bit union from a double-precision floating point value.
        /// </summary>
        /// <param name="value">The double-precision floating point value.</param>
        public Union64(double value) : this() { Double = value; }

        /// <summary>
        /// Compare for equality.
        /// </summary>
        /// <param name="other">The instance to compare.</param>
        /// <returns>True if equal.</returns>
        [System.Diagnostics.Contracts.Pure]
        public bool Equals(Union64 other)
        {
            return Unsigned == other.Unsigned;
        }

        /// <summary>
        /// Compare for equality.
        /// </summary>
        /// <param name="obj">The instance to compare.</param>
        /// <returns>True if equal.</returns>
        public override bool Equals(object obj)
        {
            return obj is Union64
                && Equals((Union64)obj);
        }

        /// <summary>
        /// Compute a hash code.
        /// </summary>
        /// <returns>The hash code for this value.</returns>
        public override int GetHashCode()
        {
            return typeof(Union64).GetHashCode()
                 ^ Unsigned.GetHashCode();
        }

        /// <summary>
        /// Compare for equality.
        /// </summary>
        /// <param name="left">The left value.</param>
        /// <param name="right">The right value.</param>
        /// <returns>True if equal.</returns>
        public static bool operator ==(Union64 left, Union64 right)
        {
            return left.Equals(right);
        }

        /// <summary>
        /// Compare for inequality.
        /// </summary>
        /// <param name="left">The left value.</param>
        /// <param name="right">The right value.</param>
        /// <returns>True if not equal.</returns>
        public static bool operator !=(Union64 left, Union64 right)
        {
            return !(left == right);
        }
    }
}