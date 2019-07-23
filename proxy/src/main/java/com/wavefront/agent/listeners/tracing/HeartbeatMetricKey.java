package com.wavefront.agent.listeners.tracing;

import java.util.Map;

import javax.annotation.Nonnull;

/**
 * Composition class that makes up the heartbeat metric.
 *
 * @author Sushant Dewan (sushant@wavefront.com).
 */
public class HeartbeatMetricKey {
  @Nonnull
  private final String application;
  @Nonnull
  private final String service;
  @Nonnull
  private final String cluster;
  @Nonnull
  private final String shard;
  @Nonnull
  private final String source;
  @Nonnull
  private final Map<String, String> customTags;

  HeartbeatMetricKey(@Nonnull String application, @Nonnull String service,
                     @Nonnull String cluster, @Nonnull String shard,
                     @Nonnull String source, @Nonnull Map<String, String> customTags) {
    this.application = application;
    this.service = service;
    this.cluster = cluster;
    this.shard = shard;
    this.source = source;
    this.customTags = customTags;
  }

  @Nonnull
  String getApplication() {
    return application;
  }

  @Nonnull
  String getService() {
    return service;
  }

  @Nonnull
  String getCluster() {
    return cluster;
  }

  @Nonnull
  String getShard() {
    return shard;
  }

  @Nonnull
  String getSource() {
    return source;
  }

  @Nonnull
  Map<String, String> getCustomTags() {
    return customTags;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HeartbeatMetricKey other = (HeartbeatMetricKey) o;
    return application.equals(other.application) && service.equals(other.service) &&
        cluster.equals(other.cluster) && shard.equals(other.shard) &&
        source.equals(other.source) && customTags.equals(other.customTags);
  }

  @Override
  public int hashCode() {
    return application.hashCode() + 31 * service.hashCode() + 31 * cluster.hashCode() +
        31 * shard.hashCode() + 31 * source.hashCode() + 31 * customTags.hashCode();
  }
}
