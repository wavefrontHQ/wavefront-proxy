package com.wavefront.agent;

import com.google.common.collect.Lists;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.wavefront.metrics.ExpectedAgentMetric;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

/**
 * Given a raw Graphite/Wavefront line, look for any host tag, and add it if implicit.
 *
 * Differences from GraphiteHostAnnotator:
 * - sharable
 * - lazy load - does not proactively perform rDNS lookups unless needed
 * - can be applied to HTTP payloads
 *
 * @author vasily@wavefront.com
 */
@ChannelHandler.Sharable
public class CachingGraphiteHostAnnotator {
  private final LoadingCache<InetAddress, String> rdnsCache;
  private final boolean disableRdnsLookup;
  private final List<String> sourceTags;

  public CachingGraphiteHostAnnotator(@Nullable final List<String> customSourceTags, boolean disableRdnsLookup) {
    this.disableRdnsLookup = disableRdnsLookup;
    this.sourceTags = Lists.newArrayListWithExpectedSize(customSourceTags == null ? 4 : customSourceTags.size() + 4);
    this.sourceTags.add("source=");
    this.sourceTags.add("source\"=");
    this.sourceTags.add("host=");
    this.sourceTags.add("host\"=");
    if (customSourceTags != null) {
      this.sourceTags.addAll(customSourceTags.stream().map(customTag -> customTag + "=").collect(Collectors.toList()));
    }

    this.rdnsCache = disableRdnsLookup ? null : Caffeine.newBuilder()
        .maximumSize(5000)
        .refreshAfterWrite(5, TimeUnit.MINUTES)
        .build(InetAddress::getHostName);

    Metrics.newGauge(ExpectedAgentMetric.RDNS_CACHE_SIZE.metricName, new Gauge<Long>() {
      @Override
      public Long value() {
        return disableRdnsLookup ? 0 : rdnsCache.estimatedSize();
      }
    });
  }

  public String apply(ChannelHandlerContext ctx, String msg) {
    for (String tag : sourceTags) {
      int strIndex = msg.indexOf(tag);
      // if a source tags is found and is followed by a non-whitespace tag value, add without change
      if (strIndex > -1 && msg.length() - strIndex - tag.length() > 0 && msg.charAt(strIndex + tag.length()) > ' ') {
        return msg;
      }
    }
    InetAddress remote = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress();
    return msg + " source=\"" + (disableRdnsLookup ? remote.getHostAddress() : rdnsCache.get(remote)) + "\"";
  }
}
