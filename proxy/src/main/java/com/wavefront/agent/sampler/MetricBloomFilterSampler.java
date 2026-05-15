package com.wavefront.agent.sampler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.wavefront.api.agent.BloomFilterDTO;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import wavefront.report.ReportPoint;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Metric Bloom Filter Sampling logic to determine if point should be sampled out.
 */
public class MetricBloomFilterSampler {
    private static final Logger logger = Logger.getLogger(MetricBloomFilterSampler.class.getCanonicalName());

    private static final String BLOOMFILTER_SAMPLER_METRIC_NAMESPACE = "points.bloomfilter.sampler";
    final Counter queriedSeriesBloomFilterHitCounter =
            Metrics.newCounter(new MetricName(BLOOMFILTER_SAMPLER_METRIC_NAMESPACE, "", "queried_series_hit"));
    final Counter unqueriedSeriesBloomFilterMissCounter =
            Metrics.newCounter(new MetricName(BLOOMFILTER_SAMPLER_METRIC_NAMESPACE, "", "queried_series_miss"));
    final Counter unqueriedSeriesSampledOutCounter =
            Metrics.newCounter(new MetricName(BLOOMFILTER_SAMPLER_METRIC_NAMESPACE, "", "unqueried_series_sampled_out"));
    final Counter unqueriedSeriesKeptCounter =
            Metrics.newCounter(new MetricName(BLOOMFILTER_SAMPLER_METRIC_NAMESPACE, "", "unqueried_series_kept"));
    final Counter bloomFilterUnavailableCounter =
            Metrics.newCounter(new MetricName(BLOOMFILTER_SAMPLER_METRIC_NAMESPACE, "", "bloomfilter_unavailable"));
    final Counter missingTrackedTagKeysCounter =
            Metrics.newCounter(new MetricName(BLOOMFILTER_SAMPLER_METRIC_NAMESPACE, "", "missing_tracked_tag_keys"));
    final Counter dryRunWouldSampleOutCounter =
            Metrics.newCounter(new MetricName(BLOOMFILTER_SAMPLER_METRIC_NAMESPACE, "", "dry_run_would_sample_out"));

    private final AtomicReference<List<ReadOnlyAbstractWindowingBloomFilters>> bloomFiltersRef = new AtomicReference<>(Collections.emptyList());
    private final AtomicReference<List<String>> trackedTagKeysRef = new AtomicReference<>(Collections.emptyList());
    private final AtomicInteger nonQueriedKeepPercent = new AtomicInteger();
    private volatile boolean dryRunEnabled = false;
    private volatile boolean forceDisableSampling = false;

    /**
     * Returns true if point should be sampled out.
     */
    public boolean shouldSampleOut(ReportPoint point) {
        if (point == null) return false;
        List<String> trackedTagKeys = trackedTagKeysRef.get();
        // if point does not appear in  metric, sample with only metric name
        boolean pointHasTrackedTag = hasTrackedTag(point.getAnnotations(), trackedTagKeys);
        // takes metric and converts string to byte[] to call mightContain
        byte[] bloomFilterLookupKeyBytes = toBloomFilterLookupKeyBytes(point, (pointHasTrackedTag) ? trackedTagKeys : null);
        List<ReadOnlyAbstractWindowingBloomFilters> dayBloomFilters = bloomFiltersRef.get();
        if (dayBloomFilters.isEmpty()) {
            bloomFilterUnavailableCounter.inc();
            return false;
        }
        for (int dayIndex = 0; dayIndex < dayBloomFilters.size(); dayIndex++) {
            ReadOnlyAbstractWindowingBloomFilters bloomFilter = dayBloomFilters.get(dayIndex);
            // if filter hits, this  means metric is queried and will be kept (should NOT be sampled)
            if (bloomFilter.mightContain(bloomFilterLookupKeyBytes)) {
                queriedSeriesBloomFilterHitCounter.inc();
                return false;
            }
        }
        unqueriedSeriesBloomFilterMissCounter.inc();

        boolean shouldKeep = shouldKeepBySamplingModuloAndMinute(bloomFilterLookupKeyBytes, point.getTimestamp());
        if (shouldKeep) {
            unqueriedSeriesKeptCounter.inc();
        } else {
            if (dryRunEnabled) {
                dryRunWouldSampleOutCounter.inc();
                return false;
            }
            unqueriedSeriesSampledOutCounter.inc();
        }
        return !shouldKeep;
    }

    /**
     * Apply bloom filter to shards.
     */
    public void updateBloomFilters(@Nullable BloomFilterDTO bloomFilterDTO) {
        if (bloomFilterDTO == null) {
            clearBloomFilters();
            return;
        }

        List<ReadOnlyAbstractWindowingBloomFilters> bloomFiltersByDay = new ArrayList<>();

        // array mapping of shards to byte arrays, most recent day is index 0 and move backwards
        Map<Integer, byte[]>[] bloomFilterShardToBytes = bloomFilterDTO.bloomFilterShardToBytes;

        if (bloomFilterShardToBytes == null || bloomFilterShardToBytes.length == 0) {
            logger.warning("Received empty bloom filter payload from backend, clearing local bll.oom filters.");
            clearBloomFilters();
            return;
        }

        // if one bloom filter fails, continues checking rest of bloom filters
        for (int dayIndex = 0; dayIndex < bloomFilterDTO.bloomFilterShardToBytes.length; dayIndex++) {
            Map<Integer, byte[]> bloomFilterDay = bloomFilterShardToBytes[dayIndex];
            if (bloomFilterDay == null) continue;
            try {
                // read filter from the shards given at API
                ReadOnlyAbstractWindowingBloomFilters bloomFilters =
                        ReadOnlyAbstractWindowingBloomFilters.fromShardBytes(bloomFilterDay, bloomFilterDTO.maxShardSize);
                // collector filters from day
                if (bloomFilters != null) {
                    bloomFiltersByDay.add(bloomFilters);
                }
            } catch (IOException ex) {
                logger.log(Level.WARNING, "Unable to deserialize bloom filter payload for day index" + dayIndex + ".", ex);

            }
        }
        String[] sampledTagKeys = getSampledTagKeys(bloomFilterDTO);
        if (!hasAnyTrackedTagKeys(sampledTagKeys)) {
            missingTrackedTagKeysCounter.inc();
        }
        setTrackedTagKeys(sampledTagKeys);
        bloomFiltersRef.set(Collections.unmodifiableList(bloomFiltersByDay));
    }

    /**
     * Set sampling percentage probability for unqueried series
     */
    public void setNonQueriedKeepPercentFromSamplingRate(double samplingRate) {
        double keepPercent = 1 - samplingRate;
        double clampKeepPercent = Math.max(0.0d, Math.min(1.0d, keepPercent));
        this.nonQueriedKeepPercent.set((int) Math.round(clampKeepPercent * 100d));
    }

    public void setDryRunEnabled(boolean dryRunEnabled) {
        this.dryRunEnabled = dryRunEnabled;
    }

    public void setForceDisableSampling(boolean forceDisableSampling) {
        this.forceDisableSampling = forceDisableSampling;
    }

    public boolean isForceDisableSampling() {
        return forceDisableSampling;
    }

    /**
     * Reset filters.
     */
    private void clearBloomFilters() {
        trackedTagKeysRef.set(Collections.emptyList());
        bloomFiltersRef.set(Collections.emptyList());
    }

    /**
     * Adds additional sampleTagKeys to total set of allTagKeys.
     */
    private void addTrackedTagKeys(@Nullable String[] sampledTagKeys, List<String> allTagKeys) {
        if (sampledTagKeys == null) {
            return;
        }
        for (String sampledTagKey : sampledTagKeys) {
            if (!Strings.isNullOrEmpty(sampledTagKey)) {
                allTagKeys.add(sampledTagKey);
            }
        }
    }

    /**
     * Checks if tag=value has tag that appears in trackedTagKeys. Metrics whose tags are NOT in set will not continue to sampling logic.
     */
    private static boolean hasTrackedTag(@Nullable Map<String, String> annotations, List<String> trackedTagKeys) {
        if (annotations == null || annotations.isEmpty() || trackedTagKeys == null || trackedTagKeys.isEmpty()) {
            return false;
        }

        for (String trackedTagKey : trackedTagKeys) {
            if (annotations.containsKey(trackedTagKey)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasAnyTrackedTagKeys(@Nullable String[] trackedTagKeys) {
        if (trackedTagKeys == null) {
            return false;
        }
        for (String trackedTagKey : trackedTagKeys) {
            if (!Strings.isNullOrEmpty(trackedTagKey)) {
                return true;
            }
        }
        return false;
    }

    @Nullable
    private static String[] getSampledTagKeys(BloomFilterDTO bloomFilterDTO) {
        if (bloomFilterDTO == null) return null;
        return bloomFilterDTO.sampledTagKeys;
    }

    /**
     * Canonicalize a metric series key as: metric + host + sorted tracked key=value annotations.
     */
    @VisibleForTesting
    static byte[] toBloomFilterLookupKeyBytes(ReportPoint point, @Nullable List<String> trackedTagKeys) {
        StringBuilder builder = new StringBuilder();
        builder.append("m|"); // for metric
        builder.append(point.getMetric());
        if (trackedTagKeys != null && !trackedTagKeys.isEmpty()) {
            Map<String, String> annotations = point.getAnnotations();
            for (String key : trackedTagKeys) {
                if (annotations.containsKey(key)) {
                    builder
                            .append("|")
                            .append(key)
                            .append("=")
                            .append(annotations.get(key));
                    break;
                }
            }
        }
        return builder.toString().getBytes(StandardCharsets.UTF_8);
    }

    void setTrackedTagKeys(@Nullable String[] sampledTagKeys) {
        List<String> trackedTagKeys = new ArrayList<>();
        addTrackedTagKeys(sampledTagKeys, trackedTagKeys);
        trackedTagKeysRef.set(Collections.unmodifiableList(trackedTagKeys));
    }

    /**
     * Sampling logic to determine if point should be kept. Determined by point time stamp and modulo logic.
     */
    private boolean shouldKeepBySamplingModuloAndMinute(byte[] canonicalSeriesKey, long timestampMillis) {
        int keepPercent = nonQueriedKeepPercent.get();
        // warning logs in case extreme percent was set.
        if (keepPercent <= 0) return false;
        if (keepPercent >= 100) {
            logger.warning("Sampling percentage set at 0%. No points will be sampled out.");
            return true;
        }

        int modulo = percentToModulo(keepPercent);
        int seriesHash = Arrays.hashCode(canonicalSeriesKey);
        long seriesModulo = Math.floorMod(seriesHash, modulo);
        // computes time bucket
        long minuteBucket = TimeUnit.MILLISECONDS.toMinutes(timestampMillis);
        long minuteModulo = Math.floorMod(minuteBucket, modulo);
        // For 50% this computes to if both are even (0) or both odd (1) else compares if in same remainder bucket.
        return seriesModulo == minuteModulo;
    }

    /**
     * Converts percent to a modulo base (Example: 50% -> 2, 25% -> 4, 10% -> 10)
     */
    static int percentToModulo(int keepPercent) {
        return Math.max(1, (int) Math.ceil(100.0d / keepPercent));
    }

    private String pointToString(ReportPoint point, String dryRunLookupKey) {
        return String.format("metric=%s, host=%s, timestamp=%d, key=%s", point.getMetric(), point.getHost(), point.getTimestamp(), dryRunLookupKey);
    }
}
