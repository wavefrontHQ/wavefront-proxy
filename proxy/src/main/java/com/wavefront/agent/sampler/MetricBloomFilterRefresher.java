package com.wavefront.agent.sampler;

import com.google.common.annotations.VisibleForTesting;
import com.wavefront.api.BloomFilterAPI;
import com.wavefront.api.agent.BloomFilterDTO;
import com.wavefront.common.NamedThreadFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.ClientErrorException;

/**
 * Refreshes bloom filter configuration from backend API.
 */
public class MetricBloomFilterRefresher {
    private static final Logger logger = Logger.getLogger(MetricBloomFilterRefresher.class.getCanonicalName());
    public static final int DEFAULT_REFRESH_MINUTES = 5;
    public static final int DEFAULT_LOOKBACK_DAYS = 1;

    private static final String DEFAULT_BLOOM_FILTER_NAME = "CUSTOMER_SERIES";

    private final BloomFilterAPI bloomFilterAPI;
    private final UUID proxyId;
    private final String authorization;
    private final String bloomFilterName;
    private final int refreshMinutes;
    private final int lookbackDays;
    private final MetricBloomFilterSampler sampler;
    private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(
                    new NamedThreadFactory("metric-bloom-filter-refresh"));

    private volatile boolean bloomFilterEndpointUnsupported = false;

    /**
     * Refresher when provided number of lookback days.
     */
    public MetricBloomFilterRefresher(BloomFilterAPI bloomFilterAPI, UUID proxyId, String authorization,
                                      int refreshMinutes, int lookbackDays, MetricBloomFilterSampler sampler) {
        this(bloomFilterAPI, proxyId, authorization, DEFAULT_BLOOM_FILTER_NAME, refreshMinutes, lookbackDays, sampler);
    }

    public MetricBloomFilterRefresher(BloomFilterAPI bloomFilterAPI, UUID proxyId, String authorization,
                                      String bloomFilterName, int refreshMinutes, int lookbackDays, MetricBloomFilterSampler sampler) {
        this.bloomFilterAPI = bloomFilterAPI;
        this.proxyId = proxyId;
        this.authorization = authorization;
        this.bloomFilterName = bloomFilterName;
        this.refreshMinutes = Math.max(refreshMinutes, DEFAULT_REFRESH_MINUTES);
        this.lookbackDays = Math.max(lookbackDays, DEFAULT_LOOKBACK_DAYS);
        this.sampler = sampler;
    }

    /**
     * Start periodic refreshes with an immediate initial fetch.
     */
    public void start() {
        executor.scheduleAtFixedRate(this::refresh, 0, refreshMinutes, TimeUnit.MINUTES);
    }

    /**
     * Stop periodic refreshes.
     */
    public void shutdown() {
        executor.shutdown();
    }

    @VisibleForTesting
    void refresh() {
        if (bloomFilterEndpointUnsupported) {
            return;
        }
        try {
            long epochDay = TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis());
            BloomFilterDTO bloomFilterDTO = bloomFilterAPI.getBloomFilters(proxyId, authorization, epochDay, lookbackDays, bloomFilterName);
            sampler.updateBloomFilters(bloomFilterDTO);
        } catch (ClientErrorException ex) {
            if (ex.getResponse().getStatus() == 404 || ex.getResponse().getStatus() == 405) {
                bloomFilterEndpointUnsupported = true;
                logger.warning("'BloomFilterAPI getBloomFilters' endpoint not found; disabling refresh.");
            }
        } catch (Exception ex) {
            logger.log(Level.WARNING, "Unable to refresh bloom filter payload.", ex);
        }
    }
}