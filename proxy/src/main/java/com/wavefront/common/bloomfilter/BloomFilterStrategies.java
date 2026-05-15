package com.wavefront.common.bloomfilter;

import com.google.common.hash.Hashing;
import com.wavefront.common.MurmurHash3;


/**
 * Collections of strategies of generating the k * log(M) bits required for an element to be mapped
 * to a BloomFilter of M bits and k hash functions. These strategies are part of the serialized form
 * of the Bloom filters that use them, thus they must be preserved as is (no updates allowed, only
 * introduction of new versions).
 *
 * Important: the order of the constants cannot change, and they cannot be deleted - we depend on
 * their ordinal for BloomFilter serialization.
 *
 * @author Dimitris Andreou
 * @author Kurt Alfred Kluever
 */
enum BloomFilterStrategies implements ReadOnlyBloomFilter.Strategy {
    /**
     * See "Less Hashing, Same Performance: Building a Better Bloom Filter" by Adam Kirsch and Michael
     * Mitzenmacher. The paper argues that this trick doesn't significantly deteriorate the
     * performance of a Bloom filter (yet only needs two 32bit hash functions).
     */
    MURMUR128_MITZ_32() {

        private final ThreadLocal<MurmurHash3.LongPair> LONG_PAIR_THREAD_LOCAL =
                ThreadLocal.withInitial(MurmurHash3.LongPair::new);

        @Override
        public boolean mightContain(byte[] object, int numHashFunctions, LockFreeBitArray bits) {
            long bitSize = bits.bitSize();
            MurmurHash3.LongPair output = LONG_PAIR_THREAD_LOCAL.get();
            MurmurHash3.murmurhash3_x64_128(object, 0, object.length, 0, output);
            int hash1 = (int) output.val1;
            int hash2 = (int) (output.val1 >>> 32);

            for (int i = 1; i <= numHashFunctions; i++) {
                int combinedHash = hash1 + (i * hash2);
                // Flip all the bits if it's negative (guaranteed positive number)
                if (combinedHash < 0) {
                    combinedHash = ~combinedHash;
                }
                if (!bits.get(combinedHash % bitSize)) {
                    return false;
                }
            }
            return true;
        }
    },
    /**
     * This strategy uses all 128 bits of {@link Hashing#murmur3_128} when hashing. It looks different
     * than the implementation in MURMUR128_MITZ_32 because we're avoiding the multiplication in the
     * loop and doing a (much simpler) += hash2. We're also changing the index to a positive number by
     * AND'ing with Long.MAX_VALUE instead of flipping the bits.
     */
    MURMUR128_MITZ_64() {

        private final ThreadLocal<MurmurHash3.LongPair> LONG_PAIR_THREAD_LOCAL =
                ThreadLocal.withInitial(MurmurHash3.LongPair::new);

        @Override
        public boolean mightContain(byte[] object, int numHashFunctions, LockFreeBitArray bits) {
            long bitSize = bits.bitSize();
            MurmurHash3.LongPair output = LONG_PAIR_THREAD_LOCAL.get();
            MurmurHash3.murmurhash3_x64_128(object, 0, object.length, 0, output);
            long hash1 = output.val1;
            long hash2 = output.val2;

            long combinedHash = hash1;
            for (int i = 0; i < numHashFunctions; i++) {
                // Make the combined hash positive and indexable
                if (!bits.get((combinedHash & Long.MAX_VALUE) % bitSize)) {
                    return false;
                }
                combinedHash += hash2;
            }
            return true;
        }
    }
}
