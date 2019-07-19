package com.wavefront.integrations.metrics;

import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.WavefrontHistogram;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Yammer MetricProcessor that sends metrics via an HttpClient. Provides support for sending to a secondary/backup
 * destination.
 * <p>
 * This sends a DIFFERENT metrics taxonomy than the Wavefront "dropwizard" metrics reporters.
 *
 * @author Mike McMahon (mike@wavefront.com)
 */
public class HttpMetricsProcessor extends WavefrontMetricsProcessor {

  private final Logger log = Logger.getLogger(HttpMetricsProcessor.class.getCanonicalName());
  private final Supplier<Long> timeSupplier;
  private final CloseableHttpAsyncClient asyncClient;
  private final HttpHost metricHost;
  private final HttpHost histogramHost;
  private final HttpHost secondaryMetricHost;
  private final HttpHost secondaryHistogramHost;

  HttpMetricsProcessor(String hostname, int port, int wavefrontHistogramPort,
                       @Nullable String secondaryHostname, int secondaryPort, int secondaryWavefrontHistogramPort,
                       int maxConnections, Supplier<Long> timeSupplier, boolean prependGroupName,
                       boolean clear, boolean sendZeroCounters, boolean sendEmptyHistograms) throws IOReactorException {
    super(prependGroupName, clear, sendZeroCounters, sendEmptyHistograms);
    this.timeSupplier = timeSupplier;

    ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
    PoolingNHttpClientConnectionManager connectionManager = new PoolingNHttpClientConnectionManager(ioReactor);
    // Two possible routes
    connectionManager.setMaxTotal(maxConnections * 2);
    // Max per route is half of that
    connectionManager.setDefaultMaxPerRoute(maxConnections);
    this.asyncClient = HttpAsyncClients.custom().
        setConnectionManager(connectionManager).
        build();
    this.asyncClient.start();

    this.metricHost = new HttpHost(hostname, port);

    // Proxy supports histos on the same port as telemetry now
    if (port == wavefrontHistogramPort) {
      this.histogramHost = this.metricHost;
    } else {
      this.histogramHost = new HttpHost(hostname, wavefrontHistogramPort);
    }

    if (StringUtils.isNotBlank(secondaryHostname)) {
      this.secondaryMetricHost = new HttpHost(secondaryHostname, secondaryPort);

      if (secondaryPort == secondaryWavefrontHistogramPort) {
        this.secondaryHistogramHost = this.secondaryMetricHost;
      } else {
        this.secondaryHistogramHost = new HttpHost(secondaryHostname, secondaryWavefrontHistogramPort);
      }
    } else {
      this.secondaryMetricHost = null;
      this.secondaryHistogramHost = null;
    }
  }

  HttpMetricsProcessor(String hostname, int port, int wavefrontHistogramPort,
                       int maxConnections, Supplier<Long> timeSupplier, boolean prependGroupName,
                       boolean clear, boolean sendZeroCounters, boolean sendEmptyHistograms) throws IOReactorException {
    this(hostname, port, wavefrontHistogramPort, null, -1, -1,
        maxConnections, timeSupplier, prependGroupName, clear, sendZeroCounters, sendEmptyHistograms);
  }

  void postLine(HttpHost host, String line) throws Exception {
    HttpPost post = new HttpPost(host.toHostString());
    HttpEntity entity = new StringEntity(line);
    post.setEntity(entity);

    asyncClient.execute(post, new FutureCallback<HttpResponse>() {
      @Override
      public void completed(HttpResponse result) {

      }

      @Override
      public void failed(Exception ex) {

      }

      @Override
      public void cancelled() {

      }
    });
  }

  @Override
  void writeMetric(MetricName name, String nameSuffix, double value) throws Exception {
    String line = toWavefrontMetricLine(name, nameSuffix, timeSupplier, value);

    postLine(metricHost, line);

    if (this.secondaryMetricHost != null)
      postLine(secondaryMetricHost, line);
  }

  @Override
  void writeHistogram(MetricName name, WavefrontHistogram histogram, Void context) throws Exception {
    List<String> histogramLines = toWavefrontHistogramLines(name, histogram);
    StringBuilder payload = new StringBuilder();
    for (String histogramLine : histogramLines) {
      payload.append(histogramLine);
    }

    postLine(histogramHost, payload.toString());
    if (secondaryHistogramHost != null)
      postLine(secondaryHistogramHost, payload.toString());
  }

  @Override
  void flush() {
    // Nothing to flush in an Http client.
  }
}
