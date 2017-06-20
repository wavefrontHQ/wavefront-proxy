package com.wavefront.agent.histogram;

import com.google.common.annotations.VisibleForTesting;

import com.wavefront.agent.PointHandler;
import com.wavefront.agent.histogram.accumulator.AccumulationCache;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import wavefront.report.ReportPoint;

import static java.lang.System.nanoTime;

/**
 * Dispatch task for marshalling "ripe" digests for shipment to the agent to a point handler.
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class PointHandlerDispatcher implements Runnable {
  private final static Logger logger = Logger.getLogger(PointHandlerDispatcher.class.getCanonicalName());

  private final Counter dispatchCounter = Metrics.newCounter(
      new MetricName("histogram.accumulator", "", "dispatched"));
  private final Counter dispatchErrorCounter = Metrics.newCounter(
      new MetricName("histogram.accumulator", "", "dispatch_errors"));
  private final Histogram accumulatorSize = Metrics.newHistogram(
      new MetricName("histogram.accumulator", "", "size"));
  private final Histogram dispatchProcessTime = Metrics.newHistogram(
      new MetricName("histogram.accumulator", "", "dispatch_process_nanos"));
  private final Histogram dispatchLagMillis = Metrics.newHistogram(
      new MetricName("histogram.accumulator", "", "dispatch_lag_millis"));

  private final AccumulationCache digests;
  private final PointHandler output;
  private final TimeProvider clock;
  private final Integer dispatchLimit;

  public PointHandlerDispatcher(AccumulationCache digests, PointHandler output, @Nullable Integer dispatchLimit) {
    this(digests, output, System::currentTimeMillis, dispatchLimit);
  }

  @VisibleForTesting
  PointHandlerDispatcher(
      AccumulationCache digests,
      PointHandler output,
      TimeProvider clock,
      @Nullable Integer dispatchLimit) {
    this.digests = digests;
    this.output = output;
    this.clock = clock;
    this.dispatchLimit = dispatchLimit;
  }

  @Override
  public void run() {
    try {
      accumulatorSize.update(digests.size());
      AtomicInteger dispatchedCount = new AtomicInteger(0);

      long startNanos = nanoTime();
      Iterator<Utils.HistogramKey> index = digests.getRipeDigestsIterator(this.clock);
      while (index.hasNext()) {
        digests.compute(index.next(), (k, v) -> {
          if (v == null) {
            index.remove();
            return null;
          }
          try {
            ReportPoint out = Utils.pointFromKeyAndDigest(k, v);
            output.reportPoint(out, k.toString());
            dispatchCounter.inc();
          } catch (Exception e) {
            dispatchErrorCounter.inc();
            logger.log(Level.SEVERE, "Failed dispatching entry " + k, e);
          }
          dispatchLagMillis.update(System.currentTimeMillis() - v.getDispatchTimeMillis());
          index.remove();
          dispatchedCount.incrementAndGet();
          return null;
        });
        if (dispatchLimit != null && dispatchedCount.get() >= dispatchLimit)
          break;
      }
      dispatchProcessTime.update(nanoTime() - startNanos);
    } catch (Exception e) {
      logger.log(Level.SEVERE, "PointHandlerDispatcher error", e);
    }
  }
}
