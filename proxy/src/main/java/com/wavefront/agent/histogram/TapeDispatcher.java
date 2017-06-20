package com.wavefront.agent.histogram;

import com.google.common.annotations.VisibleForTesting;

import com.squareup.tape.ObjectQueue;
import com.tdunning.math.stats.AgentDigest;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import wavefront.report.ReportPoint;

/**
 * Dispatch task for marshalling "ripe" digests for shipment to the agent into a (Tape) ObjectQueue
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class TapeDispatcher implements Runnable {
  private final static Logger logger = Logger.getLogger(TapeDispatcher.class.getCanonicalName());

  private final Counter dispatchCounter = Metrics.newCounter(new MetricName("histogram", "", "dispatched"));


  private final ConcurrentMap<Utils.HistogramKey, AgentDigest> digests;
  private final ObjectQueue<ReportPoint> output;
  private final TimeProvider clock;

  public TapeDispatcher(ConcurrentMap<Utils.HistogramKey, AgentDigest> digests, ObjectQueue<ReportPoint> output) {
    this(digests, output, System::currentTimeMillis);
  }

  @VisibleForTesting
  TapeDispatcher(ConcurrentMap<Utils.HistogramKey, AgentDigest> digests, ObjectQueue<ReportPoint> output, TimeProvider clock) {
    this.digests = digests;
    this.output = output;
    this.clock = clock;
  }

  @Override
  public void run() {

    for (Utils.HistogramKey key : digests.keySet()) {
      digests.compute(key, (k, v) -> {
        if (v == null) {
          return null;
        }
        // Remove and add to shipping queue
        if (v.getDispatchTimeMillis() < clock.millisSinceEpoch()) {
          try {
            ReportPoint out = Utils.pointFromKeyAndDigest(k, v);
            output.add(out);
            dispatchCounter.inc();

          } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed dispatching entry " + k, e);
          }
          return null;
        }
        return v;
      });
    }
  }
}
