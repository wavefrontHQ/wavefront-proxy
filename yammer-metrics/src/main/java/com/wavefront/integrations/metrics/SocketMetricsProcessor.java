package com.wavefront.integrations.metrics;

import com.tdunning.math.stats.Centroid;
import com.wavefront.common.MetricsToTimeseries;
import com.wavefront.common.MinuteBin;
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
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Yammer MetricProcessor that sends metrics to a TCP Socket in Wavefront-format.
 *
 * This sends a DIFFERENT metrics taxonomy than the Wavefront "dropwizard" metrics reporters.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class SocketMetricsProcessor implements MetricProcessor<Void> {

  private ReconnectingSocket metricsSocket, histogramsSocket;
  private final Supplier<Long> timeSupplier;
  private final boolean sendZeroCounters;
  private final boolean sendEmptyHistograms;
  private final boolean prependGroupName;
  private final boolean clear;

  private static final Pattern SIMPLE_NAMES = Pattern.compile("[^a-zA-Z0-9_.\\-~]");

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
      return sanitize(name.getGroup() + "." + name.getName());
    }
    return sanitize(name.getName());
  }

  private static String sanitize(String name) {
    return SIMPLE_NAMES.matcher(name).replaceAll("_");
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
    sb.append("\" ").append(value).append(" ").append(timeSupplier.get() / 1000).append(tagsForMetricName(metricName));
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
    if (histogram instanceof WavefrontHistogram) {
      WavefrontHistogram wavefrontHistogram = (WavefrontHistogram) histogram;
      List<MinuteBin> bins = wavefrontHistogram.bins(clear);
      if (bins.isEmpty()) return; // don't send empty histograms.
      for (MinuteBin minuteBin : bins) {
        StringBuilder sb = new StringBuilder();
        sb.append("!M ").append(minuteBin.getMinMillis() / 1000);
        for (Centroid c : minuteBin.getDist().centroids()) {
          sb.append(" #").append(c.count()).append(" ").append(c.mean());
        }
        sb.append(" \"").append(getName(name)).append("\"").append(tagsForMetricName(name)).append("\n");
        histogramsSocket.write(sb.toString());
      }
    } else {
      if (!sendEmptyHistograms && histogram.count() == 0) {
        // send count still but skip the others.
        writeMetric(name, "count", 0);
      } else {
        writeMetric(name, "count", histogram.count());
        writeSampling(name, histogram);
        writeSummarizable(name, histogram);
        if (clear) histogram.clear();
      }
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
