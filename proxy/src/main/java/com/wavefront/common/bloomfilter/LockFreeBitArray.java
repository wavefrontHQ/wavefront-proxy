package com.wavefront.common.bloomfilter;

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.RoundingMode;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongArray;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Models a lock-free array of bits.
 *
 * <p>We use this instead of java.util.BitSet because we need access to the array of longs and we
 * need compare-and-swap.
 */
final class LockFreeBitArray {
    private static final int LONG_ADDRESSABLE_BITS = 6;
    final AtomicLongArray data;
    private final LongAddable bitCount;

    LockFreeBitArray(long bits) {
        this(new long[Ints.checkedCast(LongMath.divide(bits, 64, RoundingMode.CEILING))]);
    }

    // Used by serialization
    LockFreeBitArray(long[] data) {
        checkArgument(data.length > 0, "data length is zero!");
        this.data = new AtomicLongArray(data);
        this.bitCount = LongAddables.create();
        long bitCount = 0;
        for (long value : data) {
            bitCount += Long.bitCount(value);
        }
        this.bitCount.add(bitCount);
    }

    boolean get(long bitIndex) {
        return (data.get((int) (bitIndex >>> LONG_ADDRESSABLE_BITS)) & (1L << bitIndex)) != 0;
    }

    /**
     * Careful here: if threads are mutating the atomicLongArray while this method is executing, the
     * final long[] will be a "rolling snapshot" of the state of the bit array. This is usually good
     * enough, but should be kept in mind.
     */
    public static long[] toPlainArray(AtomicLongArray atomicLongArray) {
        long[] array = new long[atomicLongArray.length()];
        for (int i = 0; i < array.length; ++i) {
            array[i] = atomicLongArray.get(i);
        }
        return array;
    }

    /**
     * Number of bits
     */
    long bitSize() {
        return (long) data.length() * Long.SIZE;
    }

    /**
     * Number of set bits (1s).
     *
     * <p>Note that because of concurrent set calls and uses of atomics, this bitCount is a (very)
     * close *estimate* of the actual number of bits set. It's not possible to do better than an
     * estimate without locking. Note that the number, if not exactly accurate, is *always*
     * underestimating, never overestimating.
     */
    long bitCount() {
        return bitCount.sum();
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (o instanceof LockFreeBitArray) {
            LockFreeBitArray lockFreeBitArray = (LockFreeBitArray) o;
            // TODO(lowasser): avoid allocation here
            return Arrays.equals(toPlainArray(data), toPlainArray(lockFreeBitArray.data));
        }
        return false;
    }

    @Override
    public int hashCode() {
        // TODO(lowasser): avoid allocation here
        return Arrays.hashCode(toPlainArray(data));
    }
}
