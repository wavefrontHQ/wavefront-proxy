package com.wavefront.integrations.metrics;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.codahale.metrics.Counter;
import com.codahale.metrics.DeltaCounter;
import com.codahale.metrics.MetricAttribute;

/**
 * <p>
 * This specialized version of the {@link WavefrontReporter} supports adding custom point-tags
 * based on the dropwizard metric name. It expects metrics with custom point-tags to include
 * key/value pairs within '[' and ']' within the metric name. These tags will be parsed before uploading
 * to Wavefront and added to the reporter's global {@link #pointTags}.
 * </p><p>
 * For example, a metric with the name: {@code myapp.my_counter[key1:value1,key2:value2]} will
 * upload a metric named: {@code myapp.my_counter} , and add point tags {@code key1:value1} and
 * {@code key2:value2} to the point tags already associated with the reporter.
 * </p>
 */
public class WavefrontPointTagsPerMetricReporter extends WavefrontReporter {

    public WavefrontPointTagsPerMetricReporter(WavefrontReporter.Builder builder, String proxyHostname, int proxyPort) {
        super(builder.registry, proxyHostname, proxyPort, builder.clock, builder.prefix, builder.source, builder.pointTags,
                builder.rateUnit, builder.durationUnit, builder.filter, builder.includeJvmMetrics, builder.disabledMetricAttributes);
    }

    @Override
    protected void reportCounter(String name, Counter counter) throws IOException {
        if (counter instanceof DeltaCounter) {
            super.reportCounter(name, counter);
        } else {
            String metricName = parseName(name);
            Map<String, String> perMetricTags = parseTags(name);
            this.wavefront.send(prefixAndSanitize(metricName, "count"), counter.getCount(), this.clock.getTime() / 1000, this.source, combineTags(perMetricTags));
        }
    }

    @Override
    protected void sendIfEnabled(MetricAttribute type, String name, double value, long timestamp) throws IOException {
        if (!getDisabledMetricAttributes().contains(type)) {
            String metricName = parseName(name);
            Map<String, String> perMetricTags = parseTags(name);
            this.wavefront.send(prefixAndSanitize(metricName, type.getCode()), value, timestamp, this.source, combineTags(perMetricTags));
        }
    }

    public static String parseName(String metricName) {
        int index = metricName.indexOf('[');
        if (index == -1) {
            return metricName;
        }
        return metricName.substring(0, index);
    }

    public static Map<String, String> parseTags(String metricName) {
        int index = metricName.indexOf('[');
        if (index == -1) {
            return Collections.emptyMap();
        }
        String tagStr = metricName.substring(index + 1, metricName.length() - 1);
        String[] pairs = tagStr.split(",");
        Map<String, String> tags = new HashMap<>();
        for (String p : pairs) {
            String[] t = p.split(":");
            tags.put(sanitize(t[0]), sanitize(t[1]));
        }
        return tags;
    }

    private Map<String, String> combineTags(Map<String, String> metricTags) {
        Map<String, String> combinedTags = new HashMap<>();
        combinedTags.putAll(this.pointTags);
        combinedTags.putAll(metricTags);
        return combinedTags;
    }
}
