package com.wavefront.integrations.metrics;

import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.WavefrontHistogram;
import org.apache.commons.lang.StringUtils;
import org.apache.http.*;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestWrapper;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.protocol.HttpContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

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
  private final String name;
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

  // Queues are only used when the secondary endpoint fails and we need to buffer just those points
  private final LinkedBlockingQueue<String> secondaryMetricBuffer;
  private final LinkedBlockingQueue<String> secondaryHistogramBuffer;

  private final ScheduledExecutorService executor;

  private final boolean gzip;

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
    private boolean gzip = true;

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

    public Builder withGZIPCompression(boolean gzip) {
      this.gzip = gzip;
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

    this.name = builder.name;
    this.metricBatchSize = builder.metricsBatchSize;
    this.histogramBatchSize = builder.histogramBatchSize;
    this.gzip = builder.gzip;

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

    HttpAsyncClientBuilder asyncClientBuilder = HttpAsyncClients.custom().
        setConnectionManager(connectionManager);

    if (gzip) {
      RequestConfig requestConfig = RequestConfig.custom().
          setContentCompressionEnabled(gzip)
          .build();
      asyncClientBuilder.setDefaultRequestConfig(requestConfig);
      asyncClientBuilder.setDefaultHeaders(Collections.singleton(new BasicHeader("Accept-Encoding", "gzip")));
      asyncClientBuilder.addInterceptorFirst((HttpRequestInterceptor) (request, context) -> {
        HttpEntity entity = ((HttpEntityEnclosingRequest) request).getEntity();
        ((HttpEntityEnclosingRequest) request).setEntity(new GzipCompressingEntity(entity));
      });
    }

    this.asyncClient = asyncClientBuilder.build();
    this.asyncClient.start();

    int threadPoolWorkers = 2;
    if (secondaryMetricHost != null)
      threadPoolWorkers = 4;

    this.executor = new ScheduledThreadPoolExecutor(threadPoolWorkers);

    this.executor.scheduleWithFixedDelay(this::postMetric, 0L, 50L, TimeUnit.MILLISECONDS);
    this.executor.scheduleWithFixedDelay(this::postHistogram, 0L, 50L, TimeUnit.MILLISECONDS);
    if (secondaryMetricHost != null) {
      this.executor.scheduleWithFixedDelay(this::postSecondaryMetric, 0L, 50L, TimeUnit.MILLISECONDS);
      this.executor.scheduleWithFixedDelay(this::postSecondaryHistogram, 0L, 50L, TimeUnit.MILLISECONDS);
    }
  }

  void post(final String name, HttpHost destination, final List<String> points, final LinkedBlockingQueue<String> returnEnvelope,
            final AtomicInteger inflightRequests) {

    HttpPost metricPost = new HttpPost(destination.toHostString());
    StringBuilder sb = new StringBuilder();
    for (String point : points)
      sb.append(point).append("\n");

    EntityBuilder entityBuilder = EntityBuilder.create().
        setContentType(ContentType.TEXT_PLAIN).
        setText(sb.toString());
    metricPost.setEntity(entityBuilder.build());

    this.asyncClient.execute(metricPost, new FutureCallback<HttpResponse>() {
      @Override
      public void completed(HttpResponse result) {
        inflightRequests.decrementAndGet();
      }
      @Override
      public void failed(Exception ex) {
        log.log(Level.WARNING, name + " Failed to write to the endpoint. Adding points back into the buffer", ex);
        inflightRequests.decrementAndGet();
        for (String point : points) {
          if (!returnEnvelope.offer(point))
            log.log(Level.SEVERE, name + " Unable to add points back to buffer after failure, buffer is full");
        }
      }

      @Override
      public void cancelled() {
        log.log(Level.WARNING, name + " POST was cancelled. Adding points back into the buffer");
        inflightRequests.decrementAndGet();
        for (String point : points) {
          if (!returnEnvelope.offer(point))
            log.log(Level.SEVERE, name + " Unable to add points back to buffer after failure, buffer is full");
        }
      }
    });
  }

  public void shutdown() {
    executor.shutdown();
    try {
      asyncClient.close();
    } catch (IOException ex) {
      log.log(Level.WARNING, "Failure in closing the async client", ex);
    }
  }

  public void shutdown(Long timeout, TimeUnit unit) throws InterruptedException {
    executor.shutdown();
    executor.awaitTermination(timeout, unit);
    try {
      asyncClient.close();
    } catch (IOException ex) {
      log.log(Level.WARNING, "Failure in closing the async client", ex);
    }
  }

  private void watch(LinkedBlockingQueue<String> buffer, int batchSize, AtomicInteger inflightRequests, HttpHost route) {
    Thread.currentThread().setName(name + "-postMetric");
    try {
      String peeked;
      String name = "[" + route.toHostString() + "] ";
      while ((peeked = buffer.poll(1, TimeUnit.SECONDS)) != null) {
        if (inflightRequests.get() < this.maxConnectionsPerRoute ||
            buffer.size() > batchSize) {
          inflightRequests.incrementAndGet();
          List<String> points = new ArrayList<>();
          points.add(peeked);
          int taken = buffer.drainTo(points, batchSize);
          post(name, route, points, buffer, inflightRequests);
        }
      }
    } catch (InterruptedException ex) {
      throw new RuntimeException("Interrupted, shutting down..", ex);
    }
  }

  private void postMetric() {
    final AtomicInteger inflightRequests = inflightRequestsPerRoute.get(metricHost.toHostString());
    watch(metricBuffer, metricBatchSize, inflightRequests, metricHost);
  }
  private void postHistogram() {
    final AtomicInteger inflightRequests = inflightRequestsPerRoute.get(histogramHost.toHostString());
    watch(histogramBuffer, histogramBatchSize, inflightRequests, histogramHost);
  }
  private void postSecondaryMetric() {
    if (secondaryMetricHost != null) {
      final AtomicInteger inflightRequests = inflightRequestsPerRoute.get(secondaryMetricHost.toHostString());
      watch(secondaryMetricBuffer, metricBatchSize, inflightRequests, secondaryMetricHost);
    }
  }
  private void postSecondaryHistogram() {
    if (secondaryMetricHost != null) {
      final AtomicInteger inflightRequests = inflightRequestsPerRoute.get(secondaryHistogramHost.toHostString());
      watch(secondaryHistogramBuffer, histogramBatchSize, inflightRequests, secondaryHistogramHost);
    }
  }

  @Override
  void writeMetric(MetricName name, String nameSuffix, double value) {
    String line = toWavefrontMetricLine(name, nameSuffix, timeSupplier, value);

    if (!this.metricBuffer.offer(line)) {
      log.log(Level.SEVERE, "Metric buffer is full, points are being dropped.");
    }

    if (this.secondaryMetricBuffer != null) {
       if (!this.secondaryMetricBuffer.offer(line))
        log.log(Level.SEVERE, "Secondary Metric buffer is full, points are being dropped.");
    }
  }

  @Override
  void writeHistogram(MetricName name, WavefrontHistogram histogram, Void context) {
    String wavefrontHistogramLines = toBatchedWavefrontHistogramLines(name, histogram);

    if (!this.histogramBuffer.offer(wavefrontHistogramLines)) {
      log.log(Level.SEVERE, "Histogram buffer is full, distributions are being dropped.");
    }

    if (this.secondaryHistogramBuffer != null) {
      if (!this.secondaryHistogramBuffer.offer(wavefrontHistogramLines))
        log.log(Level.SEVERE, "Secondary Histogram buffer is full, distributions are being dropped.");
    }
  }

  @Override
  void flush() {

  }
}
