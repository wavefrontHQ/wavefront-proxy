package com.wavefront.agent.handlers;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.google.common.util.concurrent.RecyclableRateLimiterImpl;

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

  private final RecyclableRateLimiter pushRateLimiter;
  private final RecyclableRateLimiter histogramRateLimiter;
  private final RecyclableRateLimiter sourceTagRateLimiter;
  private final RecyclableRateLimiter spanRateLimiter;
  private final RecyclableRateLimiter spanLogRateLimiter;
  private final RecyclableRateLimiter eventRateLimiter;

  public RecyclableRateLimiterFactoryImpl(RecyclableRateLimiter pushRateLimiter,
                                          RecyclableRateLimiter histogramRateLimiter,
                                          RecyclableRateLimiter sourceTagRateLimiter,
                                          RecyclableRateLimiter spanRateLimiter,
                                          RecyclableRateLimiter spanLogRateLimiter,
                                          RecyclableRateLimiter eventRateLimiter) {
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
