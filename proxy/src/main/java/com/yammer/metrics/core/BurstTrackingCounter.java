package com.yammer.metrics.core;

import com.wavefront.common.EvictingRingBuffer;
import com.yammer.metrics.Metrics;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A counter that accurately tracks burst rate, 1-minute rate and 5-minute rate, with 1s precision.
 *
 * @author vasily@wavefront.com
 */
public class BurstTrackingCounter extends Counter implements Metric {
  private static final MetricsRegistry SHARED_REGISTRY = new MetricsRegistry();
  private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(4);

  private final Counter delegate;
  private final Histogram burstRateHistogram;
  private long previousCount = 0;
  private long currentRate = 0;
  private final EvictingRingBuffer<Long> perSecondStats = new EvictingRingBuffer<>(300, false, 0L);

  /**
   * @param metricName metric name for the counter
   */
  public BurstTrackingCounter(MetricName metricName) {
    this.delegate = Metrics.newCounter(metricName);
    this.burstRateHistogram = SHARED_REGISTRY.newHistogram(BurstTrackingCounter.class,
            metricName.getGroup() + "-max-burst-rate");
    EXECUTOR.scheduleAtFixedRate(() -> {
      long currentCount = this.delegate.count();
      this.currentRate = currentCount - this.previousCount;
      this.burstRateHistogram.update(this.currentRate);
      this.previousCount = currentCount;
      this.perSecondStats.add(this.currentRate);
    }, 1, 1, TimeUnit.SECONDS);
  }

  /**
   * Get histogram of 1s burst rates.
   *
   * @return burst rate histogram
   */
  public Histogram getBurstRateHistogram() {
    return burstRateHistogram;
  }

  /**
   * Get most recent 1-second rate.
   *
   * @return rate
   */
  public long getCurrentRate() {
    return currentRate;
  }

  /**
   * Get highest burst rate and reset the histogram.
   * .
   * @return burst rate
   */
  public Double getMaxBurstRateAndClear() {
    Double maxValue = burstRateHistogram.max();
    burstRateHistogram.clear();
    return maxValue;
  }

  /**
   * Get 1-minute rate in human-readable form.
   *
   * @return 1-minute rate as string
   */
  public String getOneMinutePrintableRate() {
    return getPrintableRate(getOneMinuteCount());
  }

  /**
   * Get 4-minute rate in human-readable form.
   *
   * @return 4-minute rate as string
   */
  public String getFiveMinutePrintableRate() {
    return getPrintableRate(getFiveMinuteCount() / 5);
  }

  /**
   * Get delta value of the counter between now and 1 minute ago.
   *
   * @return 1-minute delta value
   */
  public long getOneMinuteCount() {
    return perSecondStats.toList().subList(240, 300).stream().mapToLong(i -> i).sum();
  }

  /**
   * Get delta value of the counter between now and 5 minutes ago.
   *
   * @return 5-minute delta value
   */
  public long getFiveMinuteCount() {
    return perSecondStats.toList().stream().mapToLong(i -> i).sum();
  }

  /**
   * Convert a per minute count to human-readable per second rate.
   *
   * @param count counter value.
   * @return human readable string
   */
  public static String getPrintableRate(long count) {
    // round rate to the nearest integer, unless it's < 1
    return count < 60 && count > 0 ? "<1" : String.valueOf((count + 30) / 60);
  }

  @Override
  public void inc() {
    delegate.inc();
  }

  @Override
  public void inc(long n) {
    delegate.inc(n);
  }

  @Override
  public void dec() {
    delegate.dec();
  }

  @Override
  public void dec(long n) {
    delegate.dec(n);
  }

  @Override
  public long count() {
    return delegate.count();
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  @Override
  public <T> void processWith(MetricProcessor<T> processor, MetricName name, T context)
      throws Exception {
    delegate.processWith(processor, name, context);
  }
}
