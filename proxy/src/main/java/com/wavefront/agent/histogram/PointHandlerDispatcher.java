package com.wavefront.agent.histogram;

import com.google.common.annotations.VisibleForTesting;

import com.tdunning.math.stats.AgentDigest;
import com.wavefront.agent.PointHandler;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;

import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import sunnylabs.report.ReportPoint;

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

  private final ConcurrentMap<Utils.HistogramKey, AgentDigest> digests;
  private final PointHandler output;
  private final TimeProvider clock;

  public PointHandlerDispatcher(ConcurrentMap<Utils.HistogramKey, AgentDigest> digests, PointHandler output) {
    this(digests, output, System::currentTimeMillis);
  }

  @VisibleForTesting
  PointHandlerDispatcher(
      ConcurrentMap<Utils.HistogramKey, AgentDigest> digests,
      PointHandler output,
      TimeProvider clock) {
    this.digests = digests;
    this.output = output;
    this.clock = clock;
  }

  @Override
  public void run() {
    accumulatorSize.update(digests.size());

    long startNanos = nanoTime();
    for (Utils.HistogramKey key : digests.keySet()) {
      digests.compute(key, (k, v) -> {
        if (v == null) {
          return null;
        }
        // Remove and add to shipping queue
        if (v.getDispatchTimeMillis() < clock.millisSinceEpoch()) {
          try {
            ReportPoint out = Utils.pointFromKeyAndDigest(k, v);
            output.reportPoint(out, k.toString());
            dispatchCounter.inc();
          } catch (Exception e) {
            dispatchErrorCounter.inc();
            logger.log(Level.SEVERE, "Failed dispatching entry " + k, e);
          }
          dispatchLagMillis.update(clock.millisSinceEpoch() - v.getDispatchTimeMillis());
          return null;
        }
        return v;
      });
    }
    dispatchProcessTime.update(nanoTime() - startNanos);
  }
}
