package com.wavefront.integrations.metrics;

import com.tdunning.math.stats.Centroid;
import com.wavefront.common.MetricsToTimeseries;
import com.wavefront.common.TaggedMetricName;
import com.yammer.metrics.core.*;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;

abstract class WavefrontMetricsProcessor implements MetricProcessor<Void> {
  private static final Pattern SIMPLE_NAMES = Pattern.compile("[^a-zA-Z0-9_.\\-~]");
  private final boolean prependGroupName;
  private final boolean clear;
  private final boolean sendZeroCounters;
  private final boolean sendEmptyHistograms;

  /**
   * @param prependGroupName    If true, metrics have their group name prepended when flushed.
   * @param clear               If true, clear histograms and timers after flush.
   * @param sendZeroCounters    If true, send counters with a value of zero.
   * @param sendEmptyHistograms If true, send histograms that are empty.
   */
  WavefrontMetricsProcessor(boolean prependGroupName, boolean clear, boolean sendZeroCounters, boolean sendEmptyHistograms) {
    this.prependGroupName = prependGroupName;
    this.clear = clear;
    this.sendZeroCounters = sendZeroCounters;
    this.sendEmptyHistograms = sendEmptyHistograms;
  }

  /**
   * @param name       The MetricName to write
   * @param nameSuffix The metric suffix to send
   * @param value      The value of the metric to send
   */
  abstract void writeMetric(MetricName name, @Nullable String nameSuffix, double value) throws Exception;

  /**
   * @param name      The MetricName to write
   * @param histogram the Histogram data to write
   * @param context   Unused
   */
  abstract void writeHistogram(MetricName name, WavefrontHistogram histogram, Void context) throws Exception;

  abstract void flush() throws Exception;

  /**
   * @param name          The name of the metric.
   * @param nameSuffix    The nameSuffix to append to the metric if specified.
   * @param timeSupplier  The supplier of the time time for the timestamp
   * @param value         The value of the metric.
   * @return              A new line terminated string in the wavefront line format.
   */
  String toWavefrontMetricLine(MetricName name, String nameSuffix, Supplier<Long> timeSupplier, double value) {
    StringBuilder sb = new StringBuilder();
    sb.append("\"").append(getName(name));

    if (nameSuffix != null && !nameSuffix.equals(""))
      sb.append(".").append(nameSuffix);

    String tags = tagsForMetricName(name);
    sb.append("\" ").append(value).append(" ").append(timeSupplier.get() / 1000).append(tags);
    return sb.append("\n").toString();
  }

  /**
   * @param name       The name of the histogram.
   * @param histogram  The histogram data.
   * @return           A list of new line terminated strings in the wavefront line format.
   */
  List<String> toWavefrontHistogramLines(MetricName name, WavefrontHistogram histogram) {
    List<WavefrontHistogram.MinuteBin> bins = histogram.bins(clear);

    if (bins.isEmpty()) return Collections.emptyList();

    List<String> histogramLines = new ArrayList<>();
    String tags = tagsForMetricName(name);

    for (WavefrontHistogram.MinuteBin minuteBin : bins) {
      StringBuilder sb = new StringBuilder();
      sb.append("!M ").append(minuteBin.getMinMillis() / 1000);
      for (Centroid c : minuteBin.getDist().centroids()) {
        sb.append(" #").append(c.count()).append(" ").append(c.mean());
      }
      sb.append(" \"").append(getName(name)).append("\"").append(tags).append("\n");
      histogramLines.add(sb.toString());
    }
    return histogramLines;
  }

  /**
   * @param name       The name of the histogram.
   * @param histogram  The histogram data.
   * @return           A single string entity containing all of the wavefront histogram data.
   */
  String toBatchedWavefrontHistogramLines(MetricName name, WavefrontHistogram histogram) {
    List<WavefrontHistogram.MinuteBin> bins = histogram.bins(clear);

    if (bins.isEmpty()) return "";

    String tags = tagsForMetricName(name);

    StringBuilder sb = new StringBuilder();
    for (WavefrontHistogram.MinuteBin minuteBin : bins) {
      sb.append("!M ").append(minuteBin.getMinMillis() / 1000);
      for (Centroid c : minuteBin.getDist().centroids()) {
        sb.append(" #").append(c.count()).append(" ").append(c.mean());
      }
      sb.append(" \"").append(getName(name)).append("\"").append(tags).append("\n");
    }
    return sb.toString();
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

  /**
   * @return " k1=v1 k2=v2 ..." if metricName is an instance of TaggedMetricName. "" otherwise.
   */
  private String tagsForMetricName(MetricName name) {
    if (name instanceof TaggedMetricName) {
      TaggedMetricName taggedMetricName = (TaggedMetricName) name;
      return tagsToLineFormat(taggedMetricName.getTags());
    }
    return "";
  }

  private String getName(MetricName name) {
    if (prependGroupName && name.getGroup() != null && !name.getGroup().equals("")) {
      return sanitize(name.getGroup() + "." + name.getName());
    }
    return sanitize(name.getName());
  }

  private String sanitize(String name) {
    return SIMPLE_NAMES.matcher(name).replaceAll("_");
  }

  private void clear(@Nullable Histogram histogram, @Nullable Timer timer) {
    if (!clear) return;
    if (histogram != null) histogram.clear();
    if (timer != null) timer.clear();
  }

  private String tagsToLineFormat(Map<String, String> tags) {
    if (tags.isEmpty()) return "";

    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      sb.append(" ").append(entry.getKey()).append("=\"").append(entry.getValue()).append("\"");
    }
    return sb.toString();
  }

  @Override
  public void processMeter(MetricName name, Metered meter, Void context) throws Exception {
    writeMetered(name, meter);
  }

  @Override
  public void processCounter(MetricName name, Counter counter, Void context) throws Exception {
    if (!sendZeroCounters && counter.count() == 0) return;

    // handle delta counters
    if (counter instanceof DeltaCounter) {
      long count = DeltaCounter.processDeltaCounter((DeltaCounter) counter);
      writeMetric(name, null, count);
    } else {
      writeMetric(name, null, counter.count());
    }
  }

  @Override
  public void processHistogram(MetricName name, Histogram histogram, Void context) throws Exception {
    if (histogram instanceof WavefrontHistogram) {
      writeHistogram(name, (WavefrontHistogram) histogram, context);
    } else {
      if (!sendEmptyHistograms && histogram.count() == 0) {
        writeMetric(name, "count", 0);
      } else {
        writeMetric(name, "count", histogram.count());
        writeSampling(name, histogram);
        writeSummarizable(name, histogram);
        clear(histogram, null);
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

    clear(null, timer);
  }

  @Override
  public void processGauge(MetricName name, Gauge<?> gauge, Void context) throws Exception {
    if (gauge.value() != null) {
      writeMetric(name, null, Double.valueOf(gauge.value().toString()));
    }
  }
}
