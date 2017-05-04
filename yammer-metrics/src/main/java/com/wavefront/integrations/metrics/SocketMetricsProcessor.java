package com.wavefront.integrations.metrics;

import com.wavefront.common.MetricsToTimeseries;
import com.wavefront.common.TaggedMetricName;
import com.wavefront.metrics.ReconnectingSocket;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.WavefrontHistogram;

import java.io.IOException;
import java.util.Map;
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
  private ReconnectingSocket metricsSocket, histogramsSocket;
  private final Supplier<Long> timeSupplier;
  private final boolean prependGroupName;

  SocketMetricsProcessor(String hostname, int port, int wavefrontHistogramPort, Supplier<Long> timeSupplier,
                         boolean prependGroupName)
      throws IOException {
    this.timeSupplier = timeSupplier;
    this.metricsSocket = new ReconnectingSocket(hostname, port);
    this.histogramsSocket = new ReconnectingSocket(hostname, wavefrontHistogramPort);
    this.prependGroupName = prependGroupName;
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
    writeMetric(name, null, counter.count());
  }

  @Override
  public void processHistogram(MetricName name, Histogram histogram, Void context) throws Exception {
    if (histogram instanceof WavefrontHistogram) {
      StringBuilder sb = new StringBuilder();
      sb.append("!M ").append(timeSupplier.get() / 1000);
      WavefrontHistogram wavefrontHistogram = (WavefrontHistogram) histogram;
      for (WavefrontHistogram.MinuteBin minuteBin : wavefrontHistogram.bins(false)) {
        sb.append(" #").append(minuteBin.getDist().size()).append(" ").append(minuteBin.getDist().quantile(.5));
      }
      sb.append(" \"").append(getName(name)).append("\"").append(tagsForMetricName(name)).append("\n");
      histogramsSocket.write(sb.toString());
    } else {
      writeMetric(name, "count", histogram.count());
      writeSampling(name, histogram);
      writeSummarizable(name, histogram);
    }
  }

  @Override
  public void processTimer(MetricName name, Timer timer, Void context) throws Exception {
    MetricName samplingName = new MetricName(name.getGroup(), name.getType(), name.getName() + ".duration");
    writeSummarizable(samplingName, timer);
    writeSampling(samplingName, timer);
    MetricName rateName = new MetricName(name.getGroup(), name.getType(), name.getName() + ".rate");
    writeMetered(rateName, timer);
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
