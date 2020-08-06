package com.wavefront.agent.handlers;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.data.EntityProperties;
import com.wavefront.agent.data.EntityPropertiesFactory;
import com.wavefront.common.EvictingRingBuffer;
import com.wavefront.common.Managed;
import com.wavefront.common.SynchronizedEvictingRingBuffer;
import com.wavefront.data.ReportableEntityType;

/**
 * Experimental: use automatic traffic shaping (set rate limiter based on recently received
 * per second rates, heavily biased towards last 5 minutes)
 *
 * @author vasily@wavefront.com.
 */
public class TrafficShapingRateLimitAdjuster extends TimerTask implements Managed {
  private static final Logger log =
      Logger.getLogger(TrafficShapingRateLimitAdjuster.class.getCanonicalName());
  private static final int MIN_RATE_LIMIT = 10; // 10 pps
  private static final double TOLERANCE_PERCENT = 5.0;

  private final EntityPropertiesFactory entityProps;
  private final double headroom;
  private final Map<ReportableEntityType, EvictingRingBuffer<Long>> perEntityStats =
      new EnumMap<>(ReportableEntityType.class);
  private final Timer timer;
  private final int windowSeconds;

  /**
   * @param entityProps       entity properties factory (to control rate limiters)
   * @param windowSeconds     size of the moving time window to average point rate
   * @param headroom          headroom multiplier
   */
  public TrafficShapingRateLimitAdjuster(EntityPropertiesFactory entityProps,
                                         int windowSeconds, double headroom) {
    this.windowSeconds = windowSeconds;
    Preconditions.checkArgument(headroom >= 1.0, "headroom can't be less than 1!");
    Preconditions.checkArgument(windowSeconds > 0, "windowSeconds needs to be > 0!");
    this.entityProps = entityProps;
    this.headroom = headroom;
    this.timer = new Timer("traffic-shaping-adjuster-timer");
  }

  @Override
  public void run() {
    for (ReportableEntityType type : ReportableEntityType.values()) {
      EntityProperties props = entityProps.get(type);
      long rate = props.getTotalReceivedRate();
      EvictingRingBuffer<Long> stats = perEntityStats.computeIfAbsent(type, x ->
          new SynchronizedEvictingRingBuffer<>(windowSeconds));
      if (rate > 0 || stats.size() > 0) {
        stats.add(rate);
        if (stats.size() >= 60) { // need at least 1 minute worth of stats to enable the limiter
          RecyclableRateLimiter rateLimiter = props.getRateLimiter();
          adjustRateLimiter(type, stats, rateLimiter);
        }
      }
    }
  }

  @Override
  public void start() {
    timer.scheduleAtFixedRate(this, 1000, 1000);
  }

  @Override
  public void stop() {
    timer.cancel();
  }

  @VisibleForTesting
  void adjustRateLimiter(ReportableEntityType type, EvictingRingBuffer<Long> sample,
                         RecyclableRateLimiter rateLimiter) {
    List<Long> samples = sample.toList();
    double suggestedLimit = MIN_RATE_LIMIT + (samples.stream().mapToLong(i -> i).sum() /
        (double) samples.size()) * headroom;
    double currentRate = rateLimiter.getRate();
    if (Math.abs(currentRate - suggestedLimit) > currentRate * TOLERANCE_PERCENT / 100) {
      log.fine("Setting rate limit for " + type.toString() + " to " + suggestedLimit);
      rateLimiter.setRate(suggestedLimit);
    }
  }
}
