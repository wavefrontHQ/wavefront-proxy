package com.wavefront.agent.handlers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.google.common.util.concurrent.RecyclableRateLimiterImpl;
import com.google.common.util.concurrent.RecyclableRateLimiterWithMetrics;
import com.wavefront.agent.config.ProxyConfig;

import javax.annotation.Nullable;

/**
 * Basic {@link RecyclableRateLimiterFactory} implementation that currently only
 * considers {@link com.wavefront.data.ReportableEntityType} and ignores different handles
 *
 * @author vasily@wavefront.com
 */
public class RecyclableRateLimiterFactoryImpl implements RecyclableRateLimiterFactory {
  /**
   * What we consider "unlimited".
   */
  public static final int NO_RATE_LIMIT = 10_000_000;
  public static final RecyclableRateLimiter UNLIMITED = RecyclableRateLimiterImpl.create(
      NO_RATE_LIMIT, 10);
  private static final double DEFAULT_SOURCE_TAG_RATE_LIMIT = 5.0;
  private static final double DEFAULT_EVENT_RATE_LIMIT = 5.0;

  private final RecyclableRateLimiter pushRateLimiter;
  private final RecyclableRateLimiter histogramRateLimiter;
  private final RecyclableRateLimiter sourceTagRateLimiter;
  private final RecyclableRateLimiter spanRateLimiter;
  private final RecyclableRateLimiter spanLogRateLimiter;
  private final RecyclableRateLimiter eventRateLimiter;

  /**
   * Create new instance based on proxy parameters from {@link ProxyConfig}
   *
   * @param proxyConfig proxy configuration
   */
  public RecyclableRateLimiterFactoryImpl(ProxyConfig proxyConfig) {
    this(proxyConfig.getPushRateLimit() > 0 ?
            new RecyclableRateLimiterWithMetrics(RecyclableRateLimiterImpl.
                create(proxyConfig.getPushRateLimit(),
                    proxyConfig.getPushRateLimitMaxBurstSeconds()), "limiter") :
            null,
        proxyConfig.getPushRateLimitSourceTags() > 0 ?
            new RecyclableRateLimiterWithMetrics(RecyclableRateLimiterImpl.
                create(DEFAULT_SOURCE_TAG_RATE_LIMIT,
                    proxyConfig.getPushRateLimitMaxBurstSeconds()), "limiter.sourceTags") :
            null,
        proxyConfig.getPushRateLimitHistograms() > 0 ?
            new RecyclableRateLimiterWithMetrics(RecyclableRateLimiterImpl.
                create(proxyConfig.getPushRateLimitHistograms(),
                    proxyConfig.getPushRateLimitMaxBurstSeconds()), "limiter.histograms") :
            null,
        proxyConfig.getPushRateLimitSpans() > 0 ?
            new RecyclableRateLimiterWithMetrics(RecyclableRateLimiterImpl.
                create(proxyConfig.getPushRateLimitSpans(),
                    proxyConfig.getPushRateLimitMaxBurstSeconds()), "limiter.spans") :
            null,
        proxyConfig.getPushRateLimitSpanLogs() > 0 ?
            new RecyclableRateLimiterWithMetrics(RecyclableRateLimiterImpl.
                create(proxyConfig.getPushRateLimitSpanLogs(),
                    proxyConfig.getPushRateLimitMaxBurstSeconds()), "limiter.spanLogs") :
            null,
        proxyConfig.getPushRateLimitEvents() > 0 ?
            new RecyclableRateLimiterWithMetrics(RecyclableRateLimiterImpl.
                create(DEFAULT_EVENT_RATE_LIMIT,
                    proxyConfig.getPushRateLimitMaxBurstSeconds()), "limiter.events") :
            null);
  }

  /**
   * Create new instance with specific rate limiters.
   *
   * @param pushRateLimiter      rate limiter for metrics
   * @param histogramRateLimiter rate limiter for histograms
   * @param sourceTagRateLimiter rate limiter for source tags
   * @param spanRateLimiter      rate limiter for spans
   * @param spanLogRateLimiter   rate limiter for span logs
   * @param eventRateLimiter     rate limiter for events
   */
  @VisibleForTesting
  protected RecyclableRateLimiterFactoryImpl(@Nullable RecyclableRateLimiter pushRateLimiter,
                                             @Nullable RecyclableRateLimiter histogramRateLimiter,
                                             @Nullable RecyclableRateLimiter sourceTagRateLimiter,
                                             @Nullable RecyclableRateLimiter spanRateLimiter,
                                             @Nullable RecyclableRateLimiter spanLogRateLimiter,
                                             @Nullable RecyclableRateLimiter eventRateLimiter) {
    this.pushRateLimiter = pushRateLimiter;
    this.histogramRateLimiter = histogramRateLimiter;
    this.sourceTagRateLimiter = sourceTagRateLimiter;
    this.spanRateLimiter = spanRateLimiter;
    this.spanLogRateLimiter = spanLogRateLimiter;
    this.eventRateLimiter = eventRateLimiter;
  }

  @Nullable
  @Override
  public RecyclableRateLimiter getRateLimiter(HandlerKey handlerKey) {
    switch (handlerKey.getEntityType()) {
      case POINT:
      case DELTA_COUNTER:
        return pushRateLimiter;
      case HISTOGRAM:
        return histogramRateLimiter;
      case SOURCE_TAG:
        return sourceTagRateLimiter;
      case TRACE:
        return spanRateLimiter;
      case TRACE_SPAN_LOGS:
        return spanLogRateLimiter;
      case EVENT:
        return eventRateLimiter;
      default:
        throw new IllegalArgumentException("Unexpected entity type " +
            handlerKey.getEntityType().name() + " for " + handlerKey.getHandle());
    }
  }
}
