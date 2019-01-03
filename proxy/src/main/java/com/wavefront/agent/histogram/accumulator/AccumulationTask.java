package com.wavefront.agent.histogram.accumulator;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import com.squareup.tape.ObjectQueue;
import com.tdunning.math.stats.AgentDigest;
import com.wavefront.agent.PointHandler;
import com.wavefront.agent.histogram.Utils;
import com.wavefront.data.Validation;
import com.wavefront.ingester.Decoder;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import wavefront.report.Histogram;
import wavefront.report.ReportPoint;

import static com.wavefront.agent.histogram.Utils.Granularity.fromMillis;
import static java.lang.System.nanoTime;

/**
 * Histogram accumulation task. Parses {@link ReportPoint} based on the passed in {@link Decoder} from its input queue
 * and accumulates them in {@link AgentDigest}.
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class AccumulationTask implements Runnable {
  private static final Logger logger = Logger.getLogger(AccumulationTask.class.getCanonicalName());

  private final ObjectQueue<List<String>> input;
  private final AccumulationCache digests;
  private final Decoder<String> decoder;
  private final List<ReportPoint> points = Lists.newArrayListWithExpectedSize(1);
  private final PointHandler blockedPointsHandler;
  private final Validation.Level validationLevel;
  private final long ttlMillis;
  private final Utils.Granularity granularity;
  private final short compression;

  // Metrics
  private final Counter eventCounter;
  private final Counter histogramCounter;
  private final Counter ignoredCounter;
  private final com.yammer.metrics.core.Histogram batchProcessTime;
  private final com.yammer.metrics.core.Histogram histogramBinCount;
  private final com.yammer.metrics.core.Histogram histogramSampleCount;


  public AccumulationTask(ObjectQueue<List<String>> input,
                          AccumulationCache digests,
                          Decoder<String> decoder,
                          PointHandler blockedPointsHandler,
                          Validation.Level validationLevel,
                          long ttlMillis,
                          @Nullable Utils.Granularity granularity,
                          short compression) {
    this.input = input;
    this.digests = digests;
    this.decoder = decoder;
    this.blockedPointsHandler = blockedPointsHandler;
    this.validationLevel = validationLevel;
    this.ttlMillis = ttlMillis;
    this.granularity = granularity == null ? Utils.Granularity.DAY : granularity;
    this.compression = compression;

    String metricNamespace = "histogram.accumulator." + Utils.Granularity.granularityToString(granularity);
    eventCounter = Metrics.newCounter(new MetricName(metricNamespace, "", "sample_added"));
    histogramCounter = Metrics.newCounter(new MetricName(metricNamespace, "", "histogram_added"));
    ignoredCounter = Metrics.newCounter(new MetricName(metricNamespace, "", "ignored"));
    batchProcessTime = Metrics.newHistogram(new MetricName(metricNamespace, "", "batch_process_nanos"));
    histogramBinCount = Metrics.newHistogram(new MetricName(metricNamespace, "", "histogram_bins"));
    histogramSampleCount = Metrics.newHistogram(new MetricName(metricNamespace, "", "histogram_samples"));
  }

  @Override
  public void run() {
    while (input.size() > 0 && !Thread.currentThread().isInterrupted()) {
      List<String> lines = input.peek();
      if (lines == null) { // remove corrupt data
        input.remove();
        continue;
      }

      long startNanos = nanoTime();
      for (String line : lines) {
        try {
          // Ignore empty lines
          if ((line = line.trim()).isEmpty()) {
            continue;
          }

          // Parse line
          points.clear();
          try {
            decoder.decodeReportPoints(line, points, "c");
          } catch (Exception e) {
            final Throwable cause = Throwables.getRootCause(e);
            String errMsg = "WF-300 Cannot parse: \"" + line + "\", reason: \"" + e.getMessage() + "\"";
            if (cause != null && cause.getMessage() != null) {
              errMsg = errMsg + ", root cause: \"" + cause.getMessage() + "\"";
            }
            throw new IllegalArgumentException(errMsg);
          }

          // now have the point, continue like in PointHandlerImpl
          ReportPoint event = points.get(0);

          Validation.validatePoint(
              event,
              line,
              validationLevel);

          if (event.getValue() instanceof Double) {
            // Get key
            Utils.HistogramKey histogramKey = Utils.makeKey(event, granularity);
            double value = (Double) event.getValue();
            eventCounter.inc();

            // atomic update
            digests.put(histogramKey, value, compression, ttlMillis);
          } else if (event.getValue() instanceof Histogram) {
            Histogram value = (Histogram) event.getValue();
            Utils.Granularity granularity = fromMillis(value.getDuration());

            histogramBinCount.update(value.getCounts().size());
            histogramSampleCount.update(value.getCounts().stream().mapToLong(x->x).sum());

            // Key
            Utils.HistogramKey histogramKey = Utils.makeKey(event, granularity);
            histogramCounter.inc();

            // atomic update
            digests.put(histogramKey, value, compression, ttlMillis);
          }
        } catch (Exception e) {
          if (!(e instanceof IllegalArgumentException)) {
            logger.log(Level.SEVERE, "Unexpected error while parsing/accumulating sample: " + e.getMessage(), e);
          }
          ignoredCounter.inc();
          if (StringUtils.isNotEmpty(e.getMessage())) {
            blockedPointsHandler.handleBlockedPoint(e.getMessage());
          }
        }
      } // end point processing
      input.remove();
      batchProcessTime.update(nanoTime() - startNanos);
    } // end batch processing
  }

  @Override
  public String toString() {
    return "AccumulationTask{" +
        "input=" + input +
        ", digests=" + digests +
        ", decoder=" + decoder +
        ", points=" + points +
        ", blockedPointsHandler=" + blockedPointsHandler +
        ", validationLevel=" + validationLevel +
        ", ttlMillis=" + ttlMillis +
        ", granularity=" + granularity +
        ", compression=" + compression +
        ", accumulationCounter=" + eventCounter +
        ", ignoredCounter=" + ignoredCounter +
        '}';
  }
}


