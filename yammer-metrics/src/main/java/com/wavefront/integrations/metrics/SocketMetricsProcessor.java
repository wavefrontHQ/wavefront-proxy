package com.wavefront.integrations.metrics;

import com.google.common.base.Throwables;

import com.wavefront.common.MetricsToTimeseries;
import com.wavefront.common.TaggedMetricName;
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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * Yammer MetricProcessor that sends metrics to a TCP Socket in Wavefront-format.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class SocketMetricsProcessor implements MetricProcessor<Void> {
  protected static final Logger logger = Logger.getLogger(SocketMetricsProcessor.class.getCanonicalName());
  private String hostname;
  private int port, wavefrontHistogramPort;
  private BufferedOutputStream metricsStream, histogramsStream;
  private Supplier<Long> timeSupplier;

  SocketMetricsProcessor(String hostname, int port, int wavefrontHistogramPort, Supplier<Long> timeSupplier)
      throws IOException {
    this.hostname = hostname;
    this.port = port;
    this.wavefrontHistogramPort = wavefrontHistogramPort;
    this.timeSupplier = timeSupplier;
    resetHistogramsSocket();
    resetMetricsSocket();
  }

  private interface Func {
    void run() throws IOException;
  }

  private void resetMetricsSocket() throws IOException {
    metricsStream = new BufferedOutputStream(new Socket(hostname, port).getOutputStream());
  }

  private void resetHistogramsSocket() throws IOException {
    histogramsStream = new BufferedOutputStream(new Socket(hostname, wavefrontHistogramPort).getOutputStream());
  }

  /**
   * Try to send the given message. On failure, reset and try again. If _that_ fails, just rethrow the exception.
   */
  private void write(String message, OutputStream stream, Func reset) throws Exception {
    try {
      stream.write(message.getBytes());
      stream.flush();
    } catch (Exception e) {
      try {
        reset.run();
        logger.info("Successfully reset connection to " + hostname);
        stream.write(message.getBytes());
      } catch (Exception e2) {
        throw Throwables.propagate(e2);
      }
    }
  }

  private void writeMetric(String message) throws Exception {
    write(message, metricsStream, this::resetMetricsSocket);
  }

  private void writeHistogram(String message) throws Exception {
    write(message, histogramsStream, this::resetHistogramsSocket);
  }


  /**
   * @return " k1=v1 k2=v2 ..." if metricName is an instance of TaggedMetricName. "" otherwise.
   */
  private String tagsForMetricName(MetricName metricName) {
    if (metricName instanceof TaggedMetricName) {
      TaggedMetricName taggedMetricName = (TaggedMetricName) metricName;
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<String, String> entry : taggedMetricName.getTags().entrySet()) {
        sb.append(" ").append(entry.getKey()).append("=").append(entry.getValue());
      }
      return sb.toString();
    } else {
      return "";
    }
  }

  private void writeMetric(MetricName metricName, String nameSuffix, double value) throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append(metricName.getName());
    if (nameSuffix != null && !nameSuffix.equals("")) {
      sb.append(".").append(nameSuffix);
    }
    sb.append(" ").append(value).append(tagsForMetricName(metricName));
    writeMetric(sb.append("\n").toString());
  }

  private void writeExplodedMetric(MetricName name, Metric metric) throws Exception {
    if (metric instanceof Metered) {
      for (Map.Entry<String, Double> entry : MetricsToTimeseries.explodeMetered((Metered) metric).entrySet()) {
        writeMetric(name, entry.getKey(), entry.getValue());
      }
    }

    if (metric instanceof Summarizable) {
      for (Map.Entry<String, Double> entry : MetricsToTimeseries.explodeSummarizable((Summarizable) metric).entrySet()) {
        writeMetric(name, entry.getKey(), entry.getValue());
      }
    }

    if (metric instanceof Sampling) {
      for (Map.Entry<String, Double> entry : MetricsToTimeseries.explodeSampling((Sampling) metric).entrySet()) {
        writeMetric(name, entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  public void processMeter(MetricName name, Metered meter, Void context) throws Exception {
    writeExplodedMetric(name, meter);
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
      sb.append(" ").append(name.getName()).append(tagsForMetricName(name)).append("\n");
      writeHistogram(sb.toString());
    } else {
      writeMetric(name, "count", histogram.count());
      writeExplodedMetric(name, histogram);
    }
  }

  @Override
  public void processTimer(MetricName name, Timer timer, Void context) throws Exception {
    writeExplodedMetric(name, timer);
  }

  @Override
  public void processGauge(MetricName name, Gauge<?> gauge, Void context) throws Exception {
    writeMetric(name, null, Double.valueOf(gauge.value().toString()));
  }
}
