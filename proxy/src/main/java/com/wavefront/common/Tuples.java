package com.wavefront.common;


import com.google.common.base.Charsets;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Tuples {

    public static final byte[] NIL_BYTES = {0, -1};
    public static final byte[] ZERO_BYTES = new byte[]{0};

    private static final ThreadLocal<DecodeResult> DECODE_RESULT_THREAD_LOCAL =
            new ThreadLocal<DecodeResult>() {
                @Override
                protected DecodeResult initialValue() {
                    return new DecodeResult();
                }
            };
    private static final ThreadLocal<byte[]> NINE_BYTES_THREAD_LOCAL =
            new ThreadLocal<byte[]>() {
                @Override
                protected byte[] initialValue() {
                    return new byte[9];
                }
            };

    private static final long[] size_limits_long;

    static {
        size_limits_long = new long[8];
        for (int i = 0; i < 8; i++) {
            size_limits_long[i] = BigInteger.ONE.shiftLeft(i * 8).subtract(BigInteger.ONE).longValue();
        }
    }

    public static byte[] getBytes(int skip, byte[] bytes, int num, int[] count) {
        Object o = getObject(skip, bytes, num, count);
        return (byte[]) o;
    }

    public static byte[] getBytes(int skip, byte[] bytes, int num, int[] count, int[] offset) {
        Object o = getObject(skip, bytes, num, count, offset);
        return (byte[]) o;
    }

    public static Object getObject(int skip, byte[] bytes, int num, int[] count) {
        return getObject(skip, bytes, num, count, null);
    }

    public static Object getObject(int skip, byte[] bytes, int num, int[] count, int[] offset) {
        int end = skip;
        int thisend = 0;
        int length = bytes.length;
        Object o = null;
        int i = 0;
        while (true) {
            DecodeResult dr = decode(bytes, end, i == num);
            end = dr.end;
            if (i++ == num) {
                o = dr.get();
                thisend = end;
                if (count == null) {
                    break;
                }
            }
            if (end == length) break;
        }
        if (count != null) count[0] = i;
        if (offset != null) offset[0] = thisend;
        return o;
    }

    private static DecodeResult decode(byte bytes[], int offset, boolean decode) {
        byte current = bytes[offset];
        int next = offset + 1;
        if (current == 0)
            return DECODE_RESULT_THREAD_LOCAL.get().set(next, null);
        if (current == 1) {
            int k = findTerminator(bytes, next);
            byte[] value = null;
            if (decode) {
                value = Arrays.copyOfRange(bytes, next, k);
                value = replace(value, NIL_BYTES, ZERO_BYTES);
            }
            return DECODE_RESULT_THREAD_LOCAL.get().set(k + 1, value);
        }
        if (current == 2) {
            int l = findTerminator(bytes, next);
            String s = null;
            if (decode) {
                byte[] value = Arrays.copyOfRange(bytes, next, l);
                value = replace(value, NIL_BYTES, ZERO_BYTES);
                s = new String(value, Charsets.UTF_8);
            }
            return DECODE_RESULT_THREAD_LOCAL.get().set(l + 1, s);
        }
        if (current >= 12 && current <= 28) {
            long value = 0;
            boolean positive = current >= 20;
            int length = positive ? current - 20 : 20 - current;
            int newNext = next + length;
            if (decode) {
                byte b[] = NINE_BYTES_THREAD_LOCAL.get();
                Arrays.fill(b, (byte) 0);
                if (bytes.length < newNext)
                    throw new RuntimeException("Invalid tuple (possible truncation)");
                System.arraycopy(bytes, next, b, 9 - length, length);
                if (!positive) {
                    for (int k1 = 9 - length; k1 < 9; k1++)
                        b[k1] = (byte) (~b[k1]);
                }
                value = (b[1] & 0xFFL) << 56
                        | (b[2] & 0xFFL) << 48
                        | (b[3] & 0xFFL) << 40
                        | (b[4] & 0xFFL) << 32
                        | (b[5] & 0xFFL) << 24
                        | (b[6] & 0xFFL) << 16
                        | (b[7] & 0xFFL) << 8
                        | (b[8] & 0xFFL);
                if (!positive) {
                    value = -value;
                }
            }
            return DECODE_RESULT_THREAD_LOCAL.get().set(newNext, value);
        } else {
            throw new IllegalArgumentException((new StringBuilder()).append("Unknown tuple data type ").append(current)
                    .append(" at index ").append(offset).toString());
        }
    }

    private static boolean regionEquals(byte abyte0[], int i, byte abyte1[]) {
        if (abyte0 == null)
            if (i == 0)
                return abyte1 == null;
            else
                throw new IllegalArgumentException("start index after end of src");
        if (abyte1 == null)
            return false;
        if (i >= abyte0.length)
            throw new IllegalArgumentException("start index after end of src");
        if (abyte0.length < i + abyte1.length)
            return false;
        for (int j = 0; j < abyte1.length; j++)
            if (abyte1[j] != abyte0[i + j])
                return false;

        return true;
    }

    public static byte[] replace(ByteArrayOutputStream baos, byte array[], byte search[], byte replace[]) {
        boolean returnBytes = baos == null;
        int i = 0;
        int j = 0;
        while (i <= array.length - search.length) {
            if (regionEquals(array, i, search)) {
                if (baos == null) {
                    baos = new ByteArrayOutputStream(array.length * 2);
                }
                baos.write(array, j, i - j);
                try {
                    baos.write(replace);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
                i += search.length;
                j = i;
            } else {
                i++;
            }
        }
        if (baos == null) {
            return array;
        } else {
            baos.write(array, j, array.length - j);
            return returnBytes ? baos.toByteArray() : null;
        }
    }

    public static byte[] replace(byte array[], byte search[], byte replace[]) {
        int i = 0;
        int j = 0;
        int k = 0;
        byte[] toReturn = null;
        while (i <= array.length - search.length) {
            if (regionEquals(array, i, search)) {
                if (toReturn == null) {
                    double ratio = (double) replace.length / search.length;
                    if (ratio < 1) ratio = 1;
                    toReturn = new byte[(int) Math.ceil(array.length * ratio)];
                }
                System.arraycopy(array, j, toReturn, k, i - j);
                k += i - j;
                System.arraycopy(replace, 0, toReturn, k, replace.length);
                k += replace.length;
                i += search.length;
                j = i;
            } else {
                i++;
            }
        }
        if (toReturn == null) return array;
        System.arraycopy(array, j, toReturn, k, array.length - j);
        k += array.length - j;
        if (k != toReturn.length) {
            byte[] shrunk = new byte[k];
            System.arraycopy(toReturn, 0, shrunk, 0, k);
            return shrunk;
        }
        return toReturn;
    }

    private static final Object MISSING = new Object();

    private static class DecodeResult {
        int end;
        Object o;
        long val;

        DecodeResult() {
        }

        DecodeResult set(int var1, Object var2) {
            this.end = var1;
            this.o = var2;
            return this;
        }

        DecodeResult set(int var1, long var2) {
            this.end = var1;
            this.val = var2;
            o = MISSING;
            return this;
        }

        boolean isLong() {
            return o == MISSING;
        }

        Object get() {
            return o == MISSING ? val : o;
        }
    }

    /**
     * Gets the index of the first element after the next occurrence of the byte sequence [nm]
     *
     * @param v     the bytes to scan through
     * @param start the index at which to start the scan
     * @return the index after the next occurrence of [nm]
     */
    static int findTerminator(byte[] v, int start) {
        return findTerminator(v, start, v.length);
    }

    /**
     * Gets the index of the first element after the next occurrence of the byte sequence [nm]
     *
     * @param v     the bytes to scan through
     * @param start the index at which to start the scan
     * @param end   the index at which to stop the search (exclusive)
     * @return the index after the next occurrence of [nm]
     */
    static int findTerminator(byte[] v, int start, int end) {
        int pos = start;
        while (true) {
            pos = findNext(v, pos, end);
            if (pos < 0)
                return end;
            if (pos + 1 == end || v[pos + 1] != (byte) -1)
                return pos;
            pos += 2;
        }
    }

    /**
     * Scan through an array of bytes to find the first occurrence of a specific value.
     *
     * @param src   array to scan. Must not be {@code null}.
     * @param start the index at which to start the search. If this is at or after
     *              the end of {@code src}, the result will always be {@code -1}.
     * @param end   the index one past the last entry at which to search
     * @return return the location of the first instance of {@code value}, or
     * {@code -1} if not found.
     */
    static int findNext(byte[] src, int start, int end) {
        for (int i = start; i < end; i++) {
            if (src[i] == (byte) 0)
                return i;
        }
        return -1;
    }
}
