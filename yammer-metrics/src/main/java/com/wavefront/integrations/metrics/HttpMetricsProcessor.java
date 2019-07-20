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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
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

  private final Map<String, AtomicInteger> inflightRequestsPerRoute = new ConcurrentHashMap<>();
  private final int maxConnectionsPerRoute;
  private final int metricBatchSize;
  private final int histogramBatchSize;

  private final LinkedBlockingQueue<String> metricBuffer;
  private final LinkedBlockingQueue<String> histogramBuffer;

  // Qeueus are only used when the secondary endpoint fails and we need to buffer just those points
  private final LinkedBlockingQueue<String> secondaryMetricBuffer;
  private final LinkedBlockingQueue<String> secondaryHistogramBuffer;

  private final ScheduledExecutorService executor;
  private long lastFlush = 0L;
  private long lastHistoFlush = 0L;
  private final long flushInterval = 1000L;
  private final long histoFlushInterval = 1000L;

  // Secondary
  private long secondaryLastFlush = 0L;
  private long secondaryLastHistoFlush = 0L;
  private final long secondaryFlushInterval = 1000L;
  private final long secondaryHistoFlushInterval = 1000L;

  public static class Builder {
    private int metricsQueueSize = 50_000;
    private int metricsBatchSize = 10_000;
    private int histogramQueueSize = 5_000;
    private int histogramBatchSize = 1_000;
    private long flushInterval = 1_000;

    private boolean prependGroupName = false;
    private boolean clear = false;
    private boolean sendZeroCounters = true;
    private boolean sendEmptyHistograms = true;

    private String hostname;
    private int metricsPort = 2878;
    private int histogramPort = 2878;
    private String secondaryHostname;
    private int secondaryMetricsPort = 2878;
    private int secondaryHistogramPort = 2878;
    private int maxConnectionsPerRoute = 10;
    private Supplier<Long> timeSupplier = System::currentTimeMillis;
    private String name;

    public Builder withHost(String hostname) {
      this.hostname = hostname;
      return this;
    }

    public Builder withPorts(int metricsPort, int histogramPort) {
      this.metricsPort = metricsPort;
      this.histogramPort = histogramPort;
      return this;
    }

    public Builder withSecondaryHostname(String hostname) {
      this.secondaryHostname = hostname;
      return this;
    }

    public Builder withSecondaryPorts(int metricsPort, int histogramPort) {
      this.secondaryMetricsPort = metricsPort;
      this.secondaryHistogramPort = histogramPort;
      return this;
    }

    public Builder withMetricsQueueOptions(int batchSize, int queueSize) {
      this.metricsBatchSize = batchSize;
      this.metricsQueueSize = queueSize;
      return this;
    }

    public Builder withHistogramQueueOptions(int batchSize, int queueSize) {
      this.histogramBatchSize = batchSize;
      this.histogramQueueSize = queueSize;
      return this;
    }

    public Builder withMaxConnectionsPerRoute(int maxConnectionsPerRoute) {
      this.maxConnectionsPerRoute = maxConnectionsPerRoute;
      return this;
    }

    public Builder withTimeSupplier(Supplier<Long> timeSupplier) {
      this.timeSupplier = timeSupplier;
      return this;
    }

    public Builder withPrependedGroupNames(boolean prependGroupName) {
      this.prependGroupName = prependGroupName;
      return this;
    }

    public Builder clearHistogramsAndTimers(boolean clear) {
      this.clear = clear;
      return this;
    }

    public Builder sendZeroCounters(boolean send) {
      this.sendZeroCounters = send;
      return this;
    }

    public Builder sendEmptyHistograms(boolean send) {
      this.sendEmptyHistograms = send;
      return this;
    }

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public HttpMetricsProcessor build() throws IOReactorException {
      if (this.metricsBatchSize > this.metricsQueueSize || this.histogramBatchSize > this.histogramQueueSize)
        throw new IllegalArgumentException("Batch size cannot be larger than queue sizes");

      return new HttpMetricsProcessor(this);
    }
  }

  HttpMetricsProcessor(Builder builder) throws IOReactorException {
      super(builder.prependGroupName, builder.clear, builder.sendZeroCounters, builder.sendEmptyHistograms);

      this.metricBatchSize = builder.metricsBatchSize;
      this.histogramBatchSize = builder.histogramBatchSize;

      this.metricBuffer = new LinkedBlockingQueue<>(builder.metricsQueueSize);
      this.histogramBuffer = new LinkedBlockingQueue<>(builder.histogramQueueSize);

      this.timeSupplier = builder.timeSupplier;

      int maxInflightRequests = builder.maxConnectionsPerRoute;
      this.maxConnectionsPerRoute = builder.maxConnectionsPerRoute;

      // Proxy supports histos on the same port as telemetry so we can reuse the same route
      this.metricHost = new HttpHost(builder.hostname, builder.metricsPort);
      this.inflightRequestsPerRoute.put(metricHost.toHostString(), new AtomicInteger());
      if (builder.metricsPort == builder.histogramPort) {
        this.histogramHost = this.metricHost;
        this.inflightRequestsPerRoute.put(this.metricHost.toHostString(), new AtomicInteger());
      } else {
        this.histogramHost = new HttpHost(builder.hostname, builder.histogramPort);
        this.inflightRequestsPerRoute.put(this.histogramHost.toHostString(), new AtomicInteger());
        maxInflightRequests += builder.maxConnectionsPerRoute;
      }

      // Secondary / backup endpoint
      if (StringUtils.isNotBlank(builder.secondaryHostname)) {
        this.secondaryMetricBuffer = new LinkedBlockingQueue<>(builder.metricsQueueSize);
        this.secondaryHistogramBuffer = new LinkedBlockingQueue<>(builder.histogramQueueSize);
        this.secondaryMetricHost = new HttpHost(builder.secondaryHostname, builder.secondaryMetricsPort);
        this.inflightRequestsPerRoute.put(this.secondaryMetricHost.toHostString(), new AtomicInteger());
        maxInflightRequests += builder.maxConnectionsPerRoute;

        if (builder.secondaryMetricsPort == builder.secondaryHistogramPort) {
          this.secondaryHistogramHost = this.secondaryMetricHost;
        } else {
          this.secondaryHistogramHost = new HttpHost(builder.secondaryHostname, builder.secondaryHistogramPort);
          this.inflightRequestsPerRoute.put(this.secondaryHistogramHost.toHostString(), new AtomicInteger());
          maxInflightRequests += builder.maxConnectionsPerRoute;
        }
      } else {
        this.secondaryMetricHost = null;
        this.secondaryMetricBuffer = null;
        this.secondaryHistogramHost = null;
        this.secondaryHistogramBuffer = null;
      }

      ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
      PoolingNHttpClientConnectionManager connectionManager = new PoolingNHttpClientConnectionManager(ioReactor);
      // maxInflightRequests == total number of routes * maxConnectionsPerRoute
    connectionManager.setMaxTotal(maxInflightRequests);
    connectionManager.setDefaultMaxPerRoute(builder.maxConnectionsPerRoute);
    this.asyncClient = HttpAsyncClients.custom().
        setConnectionManager(connectionManager).build();
    this.asyncClient.start();

    this.executor = new ScheduledThreadPoolExecutor(1);
    this.executor.schedule(this::postLine, 0L, TimeUnit.MILLISECONDS);
  }

  void post(HttpHost destination, final List<String> points, final LinkedBlockingQueue<String> returnEnvelope,
            final AtomicInteger inflightRequests) {
    HttpPost metricPost = new HttpPost(destination.toHostString());
    StringBuilder sb = new StringBuilder();
    for (String point : points)
      sb.append(point).append("\n");

    HttpEntity body;
    try {
      body = new StringEntity(sb.toString());
      metricPost.setEntity(body);
    } catch (UnsupportedEncodingException ex) {
      log.log(Level.SEVERE, "Malformed points and unable to send. Adding back to the queue.", ex);
      inflightRequests.decrementAndGet();
      for (String point : points) {
        if (!returnEnvelope.offer(point))
          log.log(Level.SEVERE, "Unable to add points back to buffer after failure, buffer is full");
      }
      return;
    }

    this.asyncClient.execute(metricPost, new FutureCallback<HttpResponse>() {
      @Override
      public void completed(HttpResponse result) {
        inflightRequests.decrementAndGet();
      }

      @Override
      public void failed(Exception ex) {
        log.log(Level.WARNING, "Failed to write to the endpoint. Adding points back into the buffer", ex);
        inflightRequests.decrementAndGet();
        for (String point : points) {
          if (!returnEnvelope.offer(point))
            log.log(Level.SEVERE, "Unable to add points back to buffer after failure, buffer is full");
        }
      }

      @Override
      public void cancelled() {
        log.log(Level.WARNING, "POST was cancelled. Adding points back into the buffer");
        inflightRequests.decrementAndGet();
        for (String point : points) {
          if (!returnEnvelope.offer(point))
            log.log(Level.SEVERE, "Unable to add points back to buffer after failure, buffer is full");
        }
      }
    });
  }

  private void postLine() {
    lastFlush = System.currentTimeMillis();
    lastHistoFlush = lastFlush;
    secondaryLastFlush = lastFlush;
    secondaryLastHistoFlush = lastFlush;

    while (true) {
      // Metrics
      final AtomicInteger inflightMetricsRequests = this.inflightRequestsPerRoute.get(this.metricHost.toHostString());
      if (System.currentTimeMillis() - lastFlush >= flushInterval || this.metricBuffer.size() > 0 && ((inflightMetricsRequests.get() < this.maxConnectionsPerRoute) ||
          this.metricBuffer.size() > this.metricBatchSize)) {
        inflightMetricsRequests.getAndIncrement();
        List<String> points = new ArrayList<>();
        int taken = this.metricBuffer.drainTo(points, this.metricBatchSize);
        post(metricHost, points, metricBuffer, inflightMetricsRequests);
        lastFlush = System.currentTimeMillis();
      }

      final AtomicInteger inflightHistoRequests = this.inflightRequestsPerRoute.get(this.histogramHost.toHostString());
      if (System.currentTimeMillis() - lastHistoFlush >= histoFlushInterval && this.histogramBuffer.size() > 0 && ((inflightHistoRequests.get() < this.maxConnectionsPerRoute) ||
          this.histogramBuffer.size() > this.histogramBatchSize)) {
        inflightHistoRequests.getAndIncrement();
        List<String> histograms = new ArrayList<>();
        int taken = this.histogramBuffer.drainTo(histograms, this.histogramBatchSize);
        post(histogramHost, histograms, histogramBuffer, inflightHistoRequests);
        lastHistoFlush = System.currentTimeMillis();
      }

      if (this.secondaryMetricHost != null) {
        final AtomicInteger inflightSecondaryMetricRequests = this.inflightRequestsPerRoute.get(this.secondaryMetricHost.toHostString());
        if (System.currentTimeMillis() - secondaryLastFlush >= secondaryFlushInterval || this.secondaryMetricBuffer.size() > 0 && ((inflightSecondaryMetricRequests.get() < this.maxConnectionsPerRoute) ||
            this.secondaryMetricBuffer.size() > this.metricBatchSize)) {
          inflightSecondaryMetricRequests.incrementAndGet();
          List<String> points = new ArrayList<>();
          int taken = this.secondaryMetricBuffer.drainTo(points, this.metricBatchSize);
          post(secondaryMetricHost, points, secondaryMetricBuffer, inflightSecondaryMetricRequests);
          secondaryLastFlush = System.currentTimeMillis();
        }
        final AtomicInteger inflightSecondaryHistoRequests = this.inflightRequestsPerRoute.get(this.secondaryHistogramHost.toHostString());
        if (System.currentTimeMillis() - secondaryLastHistoFlush >= secondaryHistoFlushInterval || this.secondaryHistogramBuffer.size() > 0 && ((inflightSecondaryHistoRequests.get() < this.maxConnectionsPerRoute) ||
            this.secondaryMetricBuffer.size() > this.metricBatchSize)) {
          inflightSecondaryHistoRequests.incrementAndGet();
          List<String> histograms = new ArrayList<>();
          int taken = this.secondaryHistogramBuffer.drainTo(histograms, this.histogramBatchSize);
          post(secondaryHistogramHost, histograms, secondaryHistogramBuffer, inflightSecondaryHistoRequests);
          secondaryLastHistoFlush = System.currentTimeMillis();
        }
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException ex) {

      }
    }
  }

  @Override
  void writeMetric(MetricName name, String nameSuffix, double value) {
    String line = toWavefrontMetricLine(name, nameSuffix, timeSupplier, value);

    if (!this.metricBuffer.offer(line)) {
      log.log(Level.SEVERE, "Metric buffer is full, points are being dropped.");
    }

    if (this.secondaryMetricBuffer != null && !this.secondaryMetricBuffer.offer(line)) {
      log.log(Level.SEVERE, "Secondary Metric buffer is full, points are being dropped.");
    }
  }

  @Override
  void writeHistogram(MetricName name, WavefrontHistogram histogram, Void context) {
    List<String> histogramLines = toWavefrontHistogramLines(name, histogram);

    StringBuilder payload = new StringBuilder();
    for (String histogramLine : histogramLines) {
      payload.append(histogramLine);
    }

    if (!this.histogramBuffer.offer(payload.toString())) {
      log.log(Level.SEVERE, "Histogram buffer is full, distributions are being dropped.");
    }

    if (this.secondaryHistogramBuffer != null && !this.secondaryHistogramBuffer.offer(payload.toString())) {
      log.log(Level.SEVERE, "Secondary Histogram buffer is full, distributions are being dropped.");
    }
  }

  @Override
  void flush() {

  }
}
