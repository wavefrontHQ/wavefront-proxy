package com.wavefront.agent.histogram;

import com.wavefront.common.MessageDedupingLogger;
import com.wavefront.common.TimeProvider;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.histogram.accumulator.Accumulator;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import wavefront.report.ReportPoint;

/**
 * Dispatch task for marshalling "ripe" digests for shipment to the agent to a point handler.
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class PointHandlerDispatcher implements Runnable {
  private final static Logger logger = Logger.getLogger(
      PointHandlerDispatcher.class.getCanonicalName());
  private static final Logger featureDisabledLogger = new MessageDedupingLogger(logger, 2, 0.2);

  private final Counter dispatchCounter;
  private final Counter dispatchErrorCounter;
  private final Counter dispatchProcessTime;

  private final Accumulator digests;
  private final AtomicLong digestsSize = new AtomicLong(0);
  private final ReportableEntityHandler<ReportPoint, String> output;
  private final TimeProvider clock;
  private final Supplier<Boolean> histogramDisabled;
  private final Integer dispatchLimit;

  public PointHandlerDispatcher(Accumulator digests,
                                ReportableEntityHandler<ReportPoint, String> output,
                                TimeProvider clock,
                                Supplier<Boolean> histogramDisabled,
                                @Nullable Integer dispatchLimit,
                                @Nullable Granularity granularity) {
    this.digests = digests;
    this.output = output;
    this.clock = clock;
    this.histogramDisabled = histogramDisabled;
    this.dispatchLimit = dispatchLimit;

    String prefix = "histogram.accumulator." + HistogramUtils.granularityToString(granularity);
    this.dispatchCounter = Metrics.newCounter(new MetricName(prefix, "", "dispatched"));
    this.dispatchErrorCounter = Metrics.newCounter(new MetricName(prefix, "", "dispatch_errors"));
    Metrics.newGauge(new MetricName(prefix, "", "size"), new Gauge<Long>() {
      @Override
      public Long value() {
        return digestsSize.get();
      }
    });
    this.dispatchProcessTime = Metrics.newCounter(new MetricName(prefix, "",
        "dispatch_process_millis"));
  }

  @Override
  public void run() {
    try {
      AtomicInteger dispatchedCount = new AtomicInteger(0);

      long startMillis = System.currentTimeMillis();
      digestsSize.set(digests.size()); // update size before flushing, so we show a higher value
      Iterator<HistogramKey> index = digests.getRipeDigestsIterator(this.clock);
      while (index.hasNext()) {
        digests.compute(index.next(), (k, v) -> {
          if (v == null) {
            index.remove();
            return null;
          }
          if (histogramDisabled.get()) {
            featureDisabledLogger.info("Histogram feature is not enabled on the server!");
            dispatchErrorCounter.inc();
          } else {
            try {
              ReportPoint out = HistogramUtils.pointFromKeyAndDigest(k, v);
              output.report(out);
              dispatchCounter.inc();
            } catch (Exception e) {
              dispatchErrorCounter.inc();
              logger.log(Level.SEVERE, "Failed dispatching entry " + k, e);
            }
          }
          index.remove();
          dispatchedCount.incrementAndGet();
          return null;
        });
        if (dispatchLimit != null && dispatchedCount.get() >= dispatchLimit)
          break;
      }
      dispatchProcessTime.inc(System.currentTimeMillis() - startMillis);
    } catch (Exception e) {
      logger.log(Level.SEVERE, "PointHandlerDispatcher error", e);
    }
  }
}
