package com.yammer.metrics.core;

import com.wavefront.agent.SharedMetricsRegistry;
import com.wavefront.common.EvictingRingBuffer;
import com.yammer.metrics.Metrics;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * TODO (VV): javadoc
 *
 * @author vasily@wavefront.com
 */
public class BurstTrackingCounter extends Counter implements Metric {
  private static final SharedMetricsRegistry SHARED_REGISTRY = SharedMetricsRegistry.getInstance();
  private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(4);

  private final Counter delegate;
  private final Histogram burstRateHistogram;
  private long previousCount = 0;
  private long currentRate = 0;
  private final EvictingRingBuffer<Long> perSecondStats = new EvictingRingBuffer<>(300, false, 0L);

  /**
   * TODO (VV): javadoc
   *
   * @param delegateMetricName
   */
  public BurstTrackingCounter(MetricName delegateMetricName) {
    this.delegate = Metrics.newCounter(delegateMetricName);
    this.burstRateHistogram = SHARED_REGISTRY.newHistogram(BurstTrackingCounter.class,
            delegateMetricName.getGroup() + "-max-burst-rate");
    EXECUTOR.scheduleAtFixedRate(() -> {
      long currentCount = this.delegate.count();
      this.currentRate = currentCount - this.previousCount;
      this.burstRateHistogram.update(this.currentRate);
      this.previousCount = currentCount;
      this.perSecondStats.add(this.currentRate);
    }, 1, 1, TimeUnit.SECONDS);
  }

  public Histogram getBurstRateHistogram() {
    return burstRateHistogram;
  }

  public long getCurrentRate() {
    return currentRate;
  }

  public Double getMaxBurstRateAndClear() {
    Double maxValue = burstRateHistogram.max();
    burstRateHistogram.clear();
    return maxValue;
  }

  public String getOneMinutePrintableRate() {
    return getPrintableRate(getOneMinuteCount());
  }

  public String getFiveMinutePrintableRate() {
    return getPrintableRate(getFiveMinuteCount() / 5);
  }

  public long getOneMinuteCount() {
    return perSecondStats.toList().subList(240, 300).stream().mapToLong(i -> i).sum();
  }

  public long getFiveMinuteCount() {
    return perSecondStats.toList().stream().mapToLong(i -> i).sum();
  }

  public static String getPrintableRate(long count) {
    // round rate to the nearest integer, unless it's < 1
    return count < 60 ? "<1" : String.valueOf((count + 30) / 60);
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
