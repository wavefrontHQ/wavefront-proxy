package com.wavefront.integrations.metrics;

import com.wavefront.metrics.ReconnectingSocket;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.WavefrontHistogram;

import javax.annotation.Nullable;
import javax.net.SocketFactory;
import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Yammer MetricProcessor that sends metrics to a TCP Socket in Wavefront-format.
 * <p>
 * This sends a DIFFERENT metrics taxonomy than the Wavefront "dropwizard" metrics reporters.
 *
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class SocketMetricsProcessor extends WavefrontMetricsProcessor {

  private ReconnectingSocket metricsSocket, histogramsSocket;
  private final Supplier<Long> timeSupplier;

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
    this(hostname, port, wavefrontHistogramPort, timeSupplier, prependGroupName, clear, sendZeroCounters,
        sendEmptyHistograms, null);
  }

  /**
   * @param hostname                   Host of the WF-graphite telemetry sink.
   * @param port                       Port of the WF-graphite telemetry sink.
   * @param wavefrontHistogramPort     Port of the WF histogram sink.
   * @param timeSupplier               Gets the epoch timestamp in milliseconds.
   * @param prependGroupName           If true, metrics have their group name prepended when flushed.
   * @param clear                      If true, clear histograms and timers after flush.
   * @param connectionTimeToLiveMillis Connection TTL, with expiration checked after each flush. When null,
   *                                   TTL is not enforced.
   */
  SocketMetricsProcessor(String hostname, int port, int wavefrontHistogramPort, Supplier<Long> timeSupplier,
                         boolean prependGroupName, boolean clear, boolean sendZeroCounters, boolean sendEmptyHistograms,
                         @Nullable Long connectionTimeToLiveMillis)
      throws IOException {
    super(prependGroupName, clear, sendZeroCounters, sendEmptyHistograms);
    this.timeSupplier = timeSupplier;
    this.metricsSocket = new ReconnectingSocket(hostname, port, SocketFactory.getDefault(), connectionTimeToLiveMillis,
        timeSupplier);
    this.histogramsSocket = new ReconnectingSocket(hostname, wavefrontHistogramPort, SocketFactory.getDefault(),
        connectionTimeToLiveMillis, timeSupplier);
  }


  @Override
  protected void writeMetric(MetricName name, @Nullable String nameSuffix, double value) throws Exception {
    String line = toWavefrontMetricLine(name, nameSuffix, timeSupplier, value);
    metricsSocket.write(line);
  }

  @Override
  protected void writeHistogram(MetricName name, WavefrontHistogram histogram, Void context) throws Exception {
    List<String> histogramLines = toWavefrontHistogramLines(name, histogram);
    for (String histogramLine : histogramLines) {
      histogramsSocket.write(histogramLine);
    }
  }

  @Override
  protected void flush() throws IOException {
    metricsSocket.flush();
    histogramsSocket.flush();
  }
}
