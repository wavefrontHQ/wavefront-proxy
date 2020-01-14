package com.wavefront.agent.channel;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetAddress;
import java.time.Duration;
import java.util.function.Function;

/**
 * Convert {@link InetAddress} to {@link String}, either by performing reverse DNS lookups (cached, as
 * the name implies), or by converting IP addresses into their string representation.
 *
 * @author vasily@wavefront.com
 */
public class CachingHostnameLookupResolver implements Function<InetAddress, String> {

  private final Function<InetAddress, String> resolverFunc;
  private final LoadingCache<InetAddress, String> rdnsCache;
  private final boolean disableRdnsLookup;

  /**
   * Create a new instance with default cache settings:
   * - max 5000 elements in the cache
   * - 5 minutes refresh TTL
   * - 1 hour expiry TTL
   *
   * @param disableRdnsLookup if true, simply return a string representation of the IP address
   * @param metricName        if specified, use this metric for the cache size gauge.
   */
  public CachingHostnameLookupResolver(boolean disableRdnsLookup, @Nullable MetricName metricName) {
    this(disableRdnsLookup, metricName, 5000, Duration.ofMinutes(5), Duration.ofHours(1));
  }

  /**
   * Create a new instance with specific cache settings:
   *
   * @param disableRdnsLookup if true, simply return a string representation of the IP address.
   * @param metricName        if specified, use this metric for the cache size gauge.
   * @param cacheSize         max cache size.
   * @param cacheRefreshTtl   trigger cache refresh after specified duration
   * @param cacheExpiryTtl    expire items after specified duration
   */
  public CachingHostnameLookupResolver(boolean disableRdnsLookup, @Nullable MetricName metricName,
                                       int cacheSize, Duration cacheRefreshTtl,
                                       Duration cacheExpiryTtl) {
    this(InetAddress::getHostAddress, disableRdnsLookup, metricName, cacheSize, cacheRefreshTtl,
        cacheExpiryTtl);
  }

  @VisibleForTesting
  CachingHostnameLookupResolver(@Nonnull Function<InetAddress, String> resolverFunc,
                                boolean disableRdnsLookup, @Nullable MetricName metricName,
                                int cacheSize, Duration cacheRefreshTtl, Duration cacheExpiryTtl) {
    this.resolverFunc = resolverFunc;
    this.disableRdnsLookup = disableRdnsLookup;
    this.rdnsCache = disableRdnsLookup ? null : Caffeine.newBuilder().
        maximumSize(cacheSize).
        refreshAfterWrite(cacheRefreshTtl).
        expireAfterAccess(cacheExpiryTtl).
        build(InetAddress::getHostName);

    if (metricName != null) {
      Metrics.newGauge(metricName, new Gauge<Long>() {
        @Override
        public Long value() {
          return disableRdnsLookup ? 0 : rdnsCache.estimatedSize();
        }
      });
    }
  }

  @Override
  public String apply(InetAddress addr) {
    return disableRdnsLookup ? resolverFunc.apply(addr) : rdnsCache.get(addr);
  }
}
