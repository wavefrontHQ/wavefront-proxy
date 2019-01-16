package com.wavefront.agent.listeners.tracing;

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
  private String shard;
  @Nonnull
  private String source;

  public HeartbeatMetricKey(String application, String service, String cluster, String shard,
                            String source) {
    this.application = application;
    this.service = service;
    this.cluster = cluster;
    this.shard = shard;
    this.source = source;
  }

  public String getApplication() {
    return application;
  }

  public String getService() {
    return service;
  }

  public String getCluster() {
    return cluster;
  }

  public String getShard() {
    return shard;
  }

  public String getSource() {
    return source;
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
        cluster.equals(other.cluster) && shard.equals(other.shard) && source.equals(other.source);
  }

  @Override
  public int hashCode() {
    return application.hashCode() + 31 * service.hashCode() + 31 * cluster.hashCode() +
        31 * shard.hashCode() + 31 * source.hashCode();
  }
}
