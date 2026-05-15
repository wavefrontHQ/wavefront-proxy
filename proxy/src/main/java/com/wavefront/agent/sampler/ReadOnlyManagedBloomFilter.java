package com.wavefront.agent.sampler;


import com.google.common.collect.Lists;
import com.wavefront.common.Tuples;
import com.wavefront.common.bloomfilter.ReadOnlyBloomFilter;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

public class ReadOnlyManagedBloomFilter {
    private final List<ReadOnlyBloomFilter> readOnlyUnderlying;

    private ReadOnlyManagedBloomFilter(List<ReadOnlyBloomFilter> underlying) {
        this.readOnlyUnderlying = underlying;
    }

    /**
     * Deserializes shard to construct a read only ManagedBloomFilter from its bytes representation.
     */

    public static ReadOnlyManagedBloomFilter fromBytes(@Nullable byte[] bytes) throws IOException {
        // Note: Encoding is <n> byte-arrays for the base BloomFilters followed by three longs
        // TODO replace size with a reasonable guess
        List<ReadOnlyBloomFilter> filters = Lists.newArrayListWithExpectedSize(4);
        int[] offset = {0};

        while (offset[0] < bytes.length) {
            Object next = Tuples.getObject(offset[0], bytes, 0, null, offset);

            if (next instanceof byte[]) {
                // Serialized bloomfilter
                ByteArrayInputStream bais = new ByteArrayInputStream((byte[]) next);
                ReadOnlyBloomFilter bf = ReadOnlyBloomFilter.readFrom(bais);
                filters.add(bf);
            } else if (next instanceof Long) {
                // Expecting three longs at EOS  encoding numInsertions, targetInsertions, targetFpp
                try {
                    return new ReadOnlyManagedBloomFilter(filters);
                } catch (NullPointerException e) {
                    throw new IOException("Decode exception, expected a Long", e);
                }
            } else {
                throw new IOException("Decode expection, expected Long or byte[], got " +
                        next.getClass().getCanonicalName());
            }
        }
        throw new IOException("Invalid format");
    }

    boolean mightContain(byte[] key) {
        // iterate in reverse order since the larger filter is towards the end.
        for (int i = readOnlyUnderlying.size() - 1; i >= 0; i--) {
            ReadOnlyBloomFilter filter = readOnlyUnderlying.get(i);
            if (filter.mightContain(key)) {
                return true;
            }
        }
        return false;
    }
}
