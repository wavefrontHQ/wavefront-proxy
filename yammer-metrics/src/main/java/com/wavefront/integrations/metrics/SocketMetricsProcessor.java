package com.wavefront.integrations.metrics;

import com.wavefront.common.MetricsToTimeseries;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.metrics.ReconnectingSocket;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.WavefrontHistogram;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * Yammer MetricProcessor that sends metrics to a TCP Socket in Wavefront-format.
 *
 * This sends a DIFFERENT metrics taxonomy than the Wavefront "dropwizard" metrics reporters.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class SocketMetricsProcessor implements MetricProcessor<Void> {

  protected static final Logger logger = Logger.getLogger(SocketMetricsProcessor.class.getCanonicalName());

  /**
   * Once a histogram is reported, we'll continue to report it (even if {@link #sendEmptyHistograms} is true). Similar
   * to {@link #sendZeroCounters}, {@link #sendEmptyHistograms} is used to prevent unused metrics from hitting being
   * emitted (thus wasting resources). Once a histogram has seen values, we'll track it. Weak hash map so that if the
   * histogram is out-of-scope, we won't hold a reference.
   */
  private final Set<Histogram> reportedHistograms = Collections.newSetFromMap(new WeakHashMap<>());

  private ReconnectingSocket metricsSocket, histogramsSocket;
  private final Supplier<Long> timeSupplier;
  private final boolean sendZeroCounters;
  private final boolean sendEmptyHistograms;
  private final boolean prependGroupName;
  private final boolean clear;

  /**
   * @param hostname               Host of the WF-graphite telemetry sink.
   * @param port                   Port of the WF-graphite telemetry sink.
   * @param wavefrontHistogramPort Port of the WF histogram sink.
   * @param timeSupplier           Gets the epoch timestamp in milliseconds.
   * @param prependGroupName       If true, metrics have their group name prepended when flushed.
   * @param clear                  If true, clear histograms and timers after flush.
   */
  SocketMetricsProcessor(String hostname, int port, int wavefrontHistogramPort, Supplier<Long> timeSupplier,
                         boolean prependGroupName, boolean clear, boolean sendZeroCounters, boolean sendEmptyHistograms)
      throws IOException {
    this.timeSupplier = timeSupplier;
    this.sendZeroCounters = sendZeroCounters;
    this.sendEmptyHistograms = sendEmptyHistograms;
    this.metricsSocket = new ReconnectingSocket(hostname, port);
    this.histogramsSocket = new ReconnectingSocket(hostname, wavefrontHistogramPort);
    this.prependGroupName = prependGroupName;
    this.clear = clear;
  }

  private String getName(MetricName name) {
    if (prependGroupName && name.getGroup() != null && !name.getGroup().equals("")) {
      return name.getGroup() + "." + name.getName();
    }
    return name.getName();
  }

  /**
   * @return " k1=v1 k2=v2 ..." if metricName is an instance of TaggedMetricName. "" otherwise.
   */
  private String tagsForMetricName(MetricName metricName) {
    if (metricName instanceof TaggedMetricName) {
      TaggedMetricName taggedMetricName = (TaggedMetricName) metricName;
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<String, String> entry : taggedMetricName.getTags().entrySet()) {
        sb.append(" ").append(entry.getKey()).append("=\"").append(entry.getValue()).append("\"");
      }
      return sb.toString();
    } else {
      return "";
    }
  }

  private void writeMetric(MetricName metricName, String nameSuffix, double value) throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append("\"").append(getName(metricName));
    if (nameSuffix != null && !nameSuffix.equals("")) {
      sb.append(".").append(nameSuffix);
    }
    sb.append("\" ").append(value).append(tagsForMetricName(metricName));
    metricsSocket.write(sb.append("\n").toString());
  }

  private void writeMetered(MetricName name, Metered metered) throws Exception {
    for (Map.Entry<String, Double> entry : MetricsToTimeseries.explodeMetered(metered).entrySet()) {
      writeMetric(name, entry.getKey(), entry.getValue());
    }
  }

  private void writeSummarizable(MetricName name, Summarizable summarizable) throws Exception {
    for (Map.Entry<String, Double> entry : MetricsToTimeseries.explodeSummarizable(summarizable).entrySet()) {
      writeMetric(name, entry.getKey(), entry.getValue());
    }
  }

  private void writeSampling(MetricName name, Sampling sampling) throws Exception {
    for (Map.Entry<String, Double> entry : MetricsToTimeseries.explodeSampling(sampling).entrySet()) {
      writeMetric(name, entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void processMeter(MetricName name, Metered meter, Void context) throws Exception {
    writeMetered(name, meter);
  }

  @Override
  public void processCounter(MetricName name, Counter counter, Void context) throws Exception {
    if (!sendZeroCounters && counter.count() == 0) return;
    writeMetric(name, null, counter.count());
  }

  @Override
  public void processHistogram(MetricName name, Histogram histogram, Void context) throws Exception {
    if (!sendEmptyHistograms) {
      if (histogram.count() == 0) {
        if (!reportedHistograms.contains(histogram)) {
          // we may skip this.
          return;
        }
      } else {
        // histogram is not empty and we would skip sending empty ones if it becomes empty in the future. track that
        // we have seen this before.
        reportedHistograms.add(histogram);
      }
    }
    if (histogram instanceof WavefrontHistogram) {
      StringBuilder sb = new StringBuilder();
      sb.append("!M ").append(timeSupplier.get() / 1000);
      WavefrontHistogram wavefrontHistogram = (WavefrontHistogram) histogram;
      for (WavefrontHistogram.MinuteBin minuteBin : wavefrontHistogram.bins(clear)) {
        sb.append(" #").append(minuteBin.getDist().size()).append(" ").append(minuteBin.getDist().quantile(.5));
      }
      sb.append(" \"").append(getName(name)).append("\"").append(tagsForMetricName(name)).append("\n");
      histogramsSocket.write(sb.toString());
    } else {
      writeMetric(name, "count", histogram.count());
      writeSampling(name, histogram);
      writeSummarizable(name, histogram);
      if (clear) histogram.clear();
    }
  }

  @Override
  public void processTimer(MetricName name, Timer timer, Void context) throws Exception {
    MetricName samplingName, rateName;
    if (name instanceof TaggedMetricName) {
      TaggedMetricName taggedMetricName = (TaggedMetricName) name;
      samplingName = new TaggedMetricName(
          taggedMetricName.getGroup(), taggedMetricName.getName() + ".duration", taggedMetricName.getTags());
      rateName = new TaggedMetricName(
          taggedMetricName.getGroup(), taggedMetricName.getName() + ".rate", taggedMetricName.getTags());
    } else {
      samplingName = new MetricName(name.getGroup(), name.getType(), name.getName() + ".duration");
      rateName = new MetricName(name.getGroup(), name.getType(), name.getName() + ".rate");
    }

    writeSummarizable(samplingName, timer);
    writeSampling(samplingName, timer);
    writeMetered(rateName, timer);

    if (clear) timer.clear();
  }

  @Override
  public void processGauge(MetricName name, Gauge<?> gauge, Void context) throws Exception {
    writeMetric(name, null, Double.valueOf(gauge.value().toString()));
  }

  public void flush() throws IOException {
    metricsSocket.flush();
    histogramsSocket.flush();
  }
}
