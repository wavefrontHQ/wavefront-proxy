package com.wavefront.agent.api;

import com.wavefront.api.BloomFilterAPI;
import com.wavefront.api.agent.BloomFilterDTO;

import java.util.UUID;

/**
 * No-op API for BloomFilter.
 */
public class NoopBloomFilterAPI implements BloomFilterAPI {
    @Override
    public BloomFilterDTO getBloomFilters(UUID proxyId, String authorization, long epochDay, int lookbackDays, String name) {
        return null;
    }
}
