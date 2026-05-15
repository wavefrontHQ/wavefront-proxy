package com.wavefront.agent.sampler;

import com.wavefront.common.MurmurHash3;
import edu.emory.mathcs.backport.java.util.Collections;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ReadOnlyAbstractWindowingBloomFilters {

    private final Map<Integer, ReadOnlyManagedBloomFilter> bloomFiltersByShard;
    private int shardCount;

    private ReadOnlyAbstractWindowingBloomFilters(Map<Integer, ReadOnlyManagedBloomFilter> bloomFiltersByShard, int shardCount) {
        this.bloomFiltersByShard = bloomFiltersByShard;
        this.shardCount = shardCount;
    }

    @Nullable
    static ReadOnlyAbstractWindowingBloomFilters fromShardBytes(
            @Nullable Map<Integer, byte[]> bloomFilterShardToBytes, int maxShardSize) throws IOException {
        if (bloomFilterShardToBytes == null || bloomFilterShardToBytes.isEmpty()) {
            return null;
        }

        Map<Integer, ReadOnlyManagedBloomFilter> bloomFiltersByShard = new HashMap<>();

        for (Map.Entry<Integer, byte[]> shardEntry : bloomFilterShardToBytes.entrySet()) {
            Integer shardId = shardEntry.getKey();
            if (shardId == null || shardId < 0) {
                continue;
            }
            ReadOnlyManagedBloomFilter bloomFilter = ReadOnlyManagedBloomFilter.fromBytes(shardEntry.getValue());
            if (bloomFilter != null) {
                bloomFiltersByShard.put(shardId, bloomFilter);
            }
        }

        if (bloomFiltersByShard.isEmpty()) {
            return null;
        }

        return new ReadOnlyAbstractWindowingBloomFilters(
                Collections.unmodifiableMap(bloomFiltersByShard), maxShardSize);

    }

    /**
     * Shard first membership check
     */
    boolean mightContain(byte[] element) {
        if (bloomFiltersByShard.isEmpty()) {
            return false;
        }

        int shard = getShard(element);
        ReadOnlyManagedBloomFilter shardBloomFilter = bloomFiltersByShard.get(shard);
        return shardBloomFilter != null && shardBloomFilter.mightContain(element);
    }

    private int getShard(byte[] element) {
        return Math.abs(MurmurHash3.murmurhash3_x86_32(element, 0, element.length, 0) % shardCount);
    }
}
