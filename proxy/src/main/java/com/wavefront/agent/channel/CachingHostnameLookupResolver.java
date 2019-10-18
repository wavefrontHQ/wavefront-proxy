package com.wavefront.agent.channel;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.function.Function;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandlerContext;

/**
 * Convert {@link InetAddress} to {@link String}, either by performing reverse DNS lookups (cached, as
 * the name implies), or by converting IP addresses into their string representation.
 *
 * @author vasily@wavefront.com
 */
public class CachingHostnameLookupResolver implements Function<InetAddress, String> {

  private final LoadingCache<InetAddress, String> rdnsCache;
  private final boolean disableRdnsLookup;

  /**
   * Create a new instance with all default settings:
   * - rDNS lookup enabled
   * - no cache size metric
   * - max 5000 elements in the cache
   * - 5 minutes refresh TTL
   * - 1 hour expiry TTL
   */
  public CachingHostnameLookupResolver() {
    this(false, null);
  }

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
   * @param maxSize           max cache size.
   * @param cacheRefreshTtl   trigger cache refresh after specified duration
   * @param cacheExpiryTtl    expire items after specified duration
   */
  public CachingHostnameLookupResolver(boolean disableRdnsLookup, @Nullable MetricName metricName,
                                       int maxSize, Duration cacheRefreshTtl, Duration cacheExpiryTtl) {
    this.disableRdnsLookup = disableRdnsLookup;
    this.rdnsCache = disableRdnsLookup ? null : Caffeine.newBuilder().
        maximumSize(maxSize).
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
    return disableRdnsLookup ? addr.getHostAddress() : rdnsCache.get(addr);
  }

  public static InetAddress getRemoteAddress(ChannelHandlerContext ctx) {
    return ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress();
  }
}
