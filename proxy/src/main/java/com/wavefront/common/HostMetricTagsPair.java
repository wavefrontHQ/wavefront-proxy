package com.wavefront.common;

import com.google.common.base.Preconditions;

import java.util.Map;

import javax.annotation.Nullable;

/**
 * Tuple class to store combination of { host, metric, tags } Two or more tuples with the
 * same value of { host, metric and tags } are considered equal and will have the same
 * hashcode.
 *
 * @author Jia Deng (djia@vmware.com).
 */
public class HostMetricTagsPair {
    public final String metric;
    public final String host;
    @Nullable
    private final Map<String, String> tags;

    public HostMetricTagsPair(String host, String metric, @Nullable Map<String, String> tags) {
        Preconditions.checkNotNull(host, "HostMetricTagsPair.host cannot be null");
        Preconditions.checkNotNull(metric, "HostMetricTagsPair.metric cannot be null");
        this.metric = metric.trim();
        this.host = host.trim().toLowerCase();
        this.tags = tags;
    }

    public String getHost() {
        return host;
    }

    public String getMetric() {
        return metric;
    }

    @Nullable
    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HostMetricTagsPair that = (HostMetricTagsPair) o;

        if (!metric.equals(that.metric) || !host.equals(that.host)) return false;
        return tags != null ? tags.equals(that.tags) : that.tags == null;
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + metric.hashCode();
        result = 31 * result + (tags != null ? tags.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("[host: %s, metric: %s, tags: %s]", host, metric, tags);
    }
}
