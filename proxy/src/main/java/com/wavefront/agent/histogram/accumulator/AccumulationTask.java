package com.wavefront.agent.histogram.accumulator;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import com.squareup.tape.ObjectQueue;
import com.tdunning.math.stats.AgentDigest;
import com.tdunning.math.stats.TDigest;
import com.wavefront.agent.PointHandler;
import com.wavefront.agent.Validation;
import com.wavefront.agent.histogram.Utils;
import com.wavefront.ingester.Decoder;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.WavefrontHistogram;

import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import sunnylabs.report.Histogram;
import sunnylabs.report.ReportPoint;

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
  private final ConcurrentMap<Utils.HistogramKey, AgentDigest> digests;
  private final Decoder<String> decoder;
  private final List<ReportPoint> points = Lists.newArrayListWithExpectedSize(1);
  private final PointHandler blockedPointsHandler;
  private final Validation.Level validationLevel;
  private final long ttlMillis;
  private final Utils.Granularity granularity;
  private final short compression;

  // Metrics
  private final Counter binCreatedCounter = Metrics.newCounter(new MetricName("histogram.accumulator", "", "bin_created"));
  private final Counter eventCounter = Metrics.newCounter(new MetricName("histogram.accumulator", "", "sample_added"));
  private final Counter histogramCounter = Metrics.newCounter(new MetricName("histogram.accumulator", "", "histogram_added"));
  private final Counter ignoredCounter = Metrics.newCounter(new MetricName("histogram.accumulator", "", "ignored"));
  private final WavefrontHistogram batchProcessTime = WavefrontHistogram.get(new MetricName("histogram.accumulator", "", "batch_process_nanos"));
  private final WavefrontHistogram histogramBinCount = WavefrontHistogram.get(new MetricName("histogram.accumulator", "", "histogram_bins"));
  private final WavefrontHistogram histogramSampleCount = WavefrontHistogram.get(new MetricName("histogram.accumulator", "", "histogram_samples"));



  public AccumulationTask(ObjectQueue<List<String>> input,
                          ConcurrentMap<Utils.HistogramKey, AgentDigest> digests,
                          Decoder<String> decoder,
                          PointHandler blockedPointsHandler,
                          Validation.Level validationLevel,
                          long ttlMillis,
                          Utils.Granularity granularity,
                          short compression) {
    this.input = input;
    this.digests = digests;
    this.decoder = decoder;
    this.blockedPointsHandler = blockedPointsHandler;
    this.validationLevel = validationLevel;
    this.ttlMillis = ttlMillis;
    this.granularity = granularity;
    this.compression = compression;
  }

  private static void add(final TDigest target, final Histogram source) {
    List<Double> means = source.getBins();
    List<Integer> counts = source.getCounts();

    if (means != null && counts != null) {
      int len = Math.min(means.size(), counts.size());

      for (int i = 0; i < len; ++i) {
        Integer count = counts.get(i);
        Double mean = means.get(i);

        if (count != null && count > 0 && mean != null && Double.isFinite(mean)) {
          target.add(mean, count);
        }
      }
    }
  }

  @Override
  public void run() {
    List<String> lines;
    while ((lines = input.peek()) != null) {
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

          // need the granularity here
          Validation.validatePoint(
              event,
              granularity.name(),
              line,
              validationLevel);

          if (event.getValue() instanceof Double) {
            // Get key
            Utils.HistogramKey histogramKey = Utils.makeKey(event, granularity);
            double value = (Double) event.getValue();
            eventCounter.inc();

            // atomic update
            digests.compute(histogramKey, (k, v) -> {
              if (v == null) {
                binCreatedCounter.inc();
                AgentDigest t = new AgentDigest(compression, System.currentTimeMillis() + ttlMillis);
                t.add(value);
                return t;
              } else {
                v.add(value);
                return v;
              }
            });
          } else if (event.getValue() instanceof Histogram) {
            Histogram value = (Histogram) event.getValue();
            Utils.Granularity granularity = fromMillis(value.getDuration());

            histogramBinCount.update(value.getCounts().size());
            histogramSampleCount.update(value.getCounts().stream().mapToLong(x->x).sum());

            // Key
            Utils.HistogramKey histogramKey = Utils.makeKey(event, granularity);
            histogramCounter.inc();

            // atomic update
            digests.compute(histogramKey, (k, v) -> {
              if (v == null) {
                binCreatedCounter.inc();
                AgentDigest t = new AgentDigest(compression, System.currentTimeMillis() + ttlMillis);
                add(t, value);
                return t;
              } else {
                add(v, value);
                return v;
              }
            });
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
        ", histogramCounter=" + binCreatedCounter +
        ", accumulationCounter=" + eventCounter +
        ", ignoredCounter=" + ignoredCounter +
        '}';
  }
}


