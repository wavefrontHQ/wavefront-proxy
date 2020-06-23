package com.wavefront.agent.handlers;

import java.util.EnumMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.RecyclableRateLimiter;
import com.wavefront.agent.data.EntityPropertiesFactory;
import com.wavefront.common.Managed;
import com.wavefront.data.ReportableEntityType;
import com.yammer.metrics.stats.ExponentiallyDecayingSample;
import com.yammer.metrics.stats.Sample;

/**
 * Experimental: use automatic traffic shaping (set rate limiter based on last 5 minutes of
 * received rates
 *
 * @author vasily@wavefront.com.
 */
public class TrafficShapingRateLimitAdjuster extends TimerTask implements Managed {
  private static final int DEFAULT_SAMPLE_SIZE = 1028;
  private static final double DEFAULT_ALPHA = 0.015;
  private static final int MIN_RATE_LIMIT = 10; // 10 pps
  private static final double HEADROOM = 1.25;

  private final ReportableEntityHandlerFactoryImpl handlerFactory;
  private final EntityPropertiesFactory entityProps;
  private final Map<ReportableEntityType, Sample> perEntityStats =
      new EnumMap<>(ReportableEntityType.class);
  private final Map<ReportableEntityType, AtomicLong> perEntitySamples =
      new EnumMap<>(ReportableEntityType.class);
  private final Timer timer;

  public TrafficShapingRateLimitAdjuster(ReportableEntityHandlerFactoryImpl handlerFactory,
                                         EntityPropertiesFactory entityProps) {
    this.handlerFactory = handlerFactory;
    this.entityProps = entityProps;
    this.timer = new Timer("traffic-shaping-adjuster-timer");
  }

  @Override
  public void run() {
    for (ReportableEntityType type : ReportableEntityType.values()) {
      AtomicLong samples = perEntitySamples.computeIfAbsent(type, k -> new AtomicLong(0));
      long rate = handlerFactory.getReceivedRate(type);
      if (rate > 0 || samples.get() > 0) {
        samples.incrementAndGet();
        Sample sample = perEntityStats.computeIfAbsent(type, x ->
            new ExponentiallyDecayingSample(DEFAULT_SAMPLE_SIZE, DEFAULT_ALPHA));
        sample.update(rate);
        if (samples.get() >= 300) {
          RecyclableRateLimiter rateLimiter = entityProps.get(type).getRateLimiter();
          double suggestedLimit = (MIN_RATE_LIMIT + sample.getSnapshot().getMedian()) * HEADROOM;
          if (Math.abs(rateLimiter.getRate() - suggestedLimit) > rateLimiter.getRate() * 0.05) {
            rateLimiter.setRate(suggestedLimit);
          }
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
}
