package com.wavefront.integrations.metrics;

import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.WavefrontHistogram;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.http.*;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.protocol.HTTP;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
  private final Map<String, LinkedBlockingQueue<FutureCallback<HttpResponse>>> inflightCompletablesPerRoute = new ConcurrentHashMap<>();
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

    name = builder.name;
    metricBatchSize = builder.metricsBatchSize;
    histogramBatchSize = builder.histogramBatchSize;
    gzip = builder.gzip;

    metricBuffer = new LinkedBlockingQueue<>(builder.metricsQueueSize);
    histogramBuffer = new LinkedBlockingQueue<>(builder.histogramQueueSize);

    timeSupplier = builder.timeSupplier;

    int maxInflightRequests = builder.maxConnectionsPerRoute;
    maxConnectionsPerRoute = builder.maxConnectionsPerRoute;

    // Proxy supports histos on the same port as telemetry so we can reuse the same route
    metricHost = new HttpHost(builder.hostname, builder.metricsPort);
    inflightRequestsPerRoute.put(metricHost.toHostString(), new AtomicInteger());
    inflightCompletablesPerRoute.put(metricHost.toHostString(), new LinkedBlockingQueue<>(maxConnectionsPerRoute));

    if (builder.metricsPort == builder.histogramPort) {
      histogramHost = metricHost;
      inflightRequestsPerRoute.put(metricHost.toHostString(), new AtomicInteger());
    } else {
      histogramHost = new HttpHost(builder.hostname, builder.histogramPort);
      inflightRequestsPerRoute.put(histogramHost.toHostString(), new AtomicInteger());
      maxInflightRequests += builder.maxConnectionsPerRoute;
    }
    inflightCompletablesPerRoute.put(histogramHost.toHostString(), new LinkedBlockingQueue<>(maxConnectionsPerRoute));

    // Secondary / backup endpoint
    if (StringUtils.isNotBlank(builder.secondaryHostname)) {
      secondaryMetricBuffer = new LinkedBlockingQueue<>(builder.metricsQueueSize);
      secondaryHistogramBuffer = new LinkedBlockingQueue<>(builder.histogramQueueSize);

      secondaryMetricHost = new HttpHost(builder.secondaryHostname, builder.secondaryMetricsPort);
      inflightRequestsPerRoute.put(secondaryMetricHost.toHostString(), new AtomicInteger());
      maxInflightRequests += builder.maxConnectionsPerRoute;

      if (builder.secondaryMetricsPort == builder.secondaryHistogramPort) {
        secondaryHistogramHost = secondaryMetricHost;
      } else {
        secondaryHistogramHost = new HttpHost(builder.secondaryHostname, builder.secondaryHistogramPort);
        inflightRequestsPerRoute.put(secondaryHistogramHost.toHostString(), new AtomicInteger());
        maxInflightRequests += builder.maxConnectionsPerRoute;
      }
      inflightCompletablesPerRoute.put(secondaryMetricHost.toHostString(), new LinkedBlockingQueue<>(maxConnectionsPerRoute));
      inflightCompletablesPerRoute.put(secondaryHistogramHost.toHostString(), new LinkedBlockingQueue<>(maxConnectionsPerRoute));
    } else {
      secondaryMetricHost = null;
      secondaryMetricBuffer = null;
      secondaryHistogramHost = null;
      secondaryHistogramBuffer = null;
    }

    ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
    PoolingNHttpClientConnectionManager connectionManager = new PoolingNHttpClientConnectionManager(ioReactor);
    // maxInflightRequests == total number of routes * maxConnectionsPerRoute
    connectionManager.setMaxTotal(maxInflightRequests);
    connectionManager.setDefaultMaxPerRoute(builder.maxConnectionsPerRoute);

    HttpAsyncClientBuilder asyncClientBuilder = HttpAsyncClients.custom().
        setConnectionManager(connectionManager);

//    if (gzip) {
//      RequestConfig requestConfig = RequestConfig.custom().
//          setContentCompressionEnabled(true)
//          .build();
//      asyncClientBuilder.setDefaultRequestConfig(requestConfig);
//      asyncClientBuilder.setDefaultHeaders(Collections.singleton(new BasicHeader("Accept-Encoding", "gzip")));
//      asyncClientBuilder.addInterceptorFirst((HttpRequestInterceptor) (request, context) -> {
//        HttpEntity entity = ((HttpEntityEnclosingRequest) request).getEntity();
//        GzipCompressingEntity zippedEntity = new GzipCompressingEntity(entity);
//        request.removeHeaders(HTTP.CONTENT_ENCODING);
//        request.addHeader(zippedEntity.getContentEncoding());
//        ((HttpEntityEnclosingRequest) request).setEntity(zippedEntity);
//      });
//    }

    asyncClient = asyncClientBuilder.build();
    asyncClient.start();

    int threadPoolWorkers = 2;
    if (secondaryMetricHost != null)
      threadPoolWorkers = 4;

    executor = new ScheduledThreadPoolExecutor(threadPoolWorkers);

    executor.scheduleWithFixedDelay(this::postMetric, 0L, 50L, TimeUnit.MILLISECONDS);
    executor.scheduleWithFixedDelay(this::postHistogram, 0L, 50L, TimeUnit.MILLISECONDS);
    if (secondaryMetricHost != null) {
      executor.scheduleWithFixedDelay(this::postSecondaryMetric, 0L, 50L, TimeUnit.MILLISECONDS);
      executor.scheduleWithFixedDelay(this::postSecondaryHistogram, 0L, 50L, TimeUnit.MILLISECONDS);
    }
  }

  void post(final String name, HttpHost destination, final List<String> points, final LinkedBlockingQueue<String> returnEnvelope,
            final AtomicInteger inflightRequests) {

    HttpPost post = new HttpPost(destination.toString());
    StringBuilder sb = new StringBuilder();
    for (String point : points)
      sb.append(point);

    EntityBuilder entityBuilder = EntityBuilder.create().
        setContentType(ContentType.TEXT_PLAIN);
    if (gzip) {
      try {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(stream);
        gzip.write(sb.toString().getBytes());
        gzip.finish();
        entityBuilder.setBinary(stream.toByteArray());
        entityBuilder.chunked();
        entityBuilder.setContentEncoding("gzip");
      } catch (IOException ex) {
        log.log(Level.SEVERE, "Unable compress points, returning to the buffer", ex);
        for (String point : points) {
          if (!returnEnvelope.offer(point))
            log.log(Level.SEVERE, name + " Unable to add points back to buffer after failure, buffer is full");
        }
      }
    } else {
      entityBuilder.setText(sb.toString());
    }
    post.setEntity(entityBuilder.build());

    final LinkedBlockingQueue<FutureCallback<HttpResponse>> processingQueue =
        inflightCompletablesPerRoute.get(destination.toHostString());
    final FutureCallback<HttpResponse> completable = new FutureCallback<HttpResponse>() {
      @Override
      public void completed(HttpResponse result) {
        inflightRequests.decrementAndGet();
        processingQueue.poll();
      }
      @Override
      public void failed(Exception ex) {
        log.log(Level.WARNING, name + " Failed to write to the endpoint. Adding points back into the buffer", ex);
        inflightRequests.decrementAndGet();
        for (String point : points) {
          if (!returnEnvelope.offer(point))
            log.log(Level.SEVERE, name + " Unable to add points back to buffer after failure, buffer is full");
        }
        processingQueue.poll();
      }

      @Override
      public void cancelled() {
        log.log(Level.WARNING, name + " POST was cancelled. Adding points back into the buffer");
        inflightRequests.decrementAndGet();
        for (String point : points) {
          if (!returnEnvelope.offer(point))
            log.log(Level.SEVERE, name + " Unable to add points back to buffer after failure, buffer is full");
        }
        processingQueue.poll();
      }
    };

    // wait until a thread completes before issuing the next batch immediately
    processingQueue.offer(completable);
    inflightRequests.incrementAndGet();
    this.asyncClient.execute(post, completable);
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

  private void watch(LinkedBlockingQueue<String> buffer, int batchSize, AtomicInteger inflightRequests, HttpHost route,
                     String threadIdentifier) {
    Thread.currentThread().setName(name + "-" + threadIdentifier);
    try {
      String peeked;
      String friendlyRoute = "[" + route.toHostString() + "] ";
      while ((peeked = buffer.poll(1, TimeUnit.SECONDS)) != null) {
        List<String> points = new ArrayList<>();
        points.add(peeked);
        int taken = buffer.drainTo(points, batchSize);
        post(friendlyRoute, route, points, buffer, inflightRequests);
      }
    } catch (InterruptedException ex) {
      throw new RuntimeException("Interrupted, shutting down..", ex);
    }
  }

  private void postMetric() {
    final AtomicInteger inflightRequests = inflightRequestsPerRoute.get(metricHost.toHostString());
    watch(metricBuffer, metricBatchSize, inflightRequests, metricHost, "postMetric");
  }
  private void postHistogram() {
    final AtomicInteger inflightRequests = inflightRequestsPerRoute.get(histogramHost.toHostString());
    watch(histogramBuffer, histogramBatchSize, inflightRequests, histogramHost, "postHistogram");
  }
  private void postSecondaryMetric() {
    final AtomicInteger inflightRequests = inflightRequestsPerRoute.get(secondaryMetricHost.toHostString());
    watch(secondaryMetricBuffer, metricBatchSize, inflightRequests, secondaryMetricHost, "postSecondaryMetric");
  }
  private void postSecondaryHistogram() {
    final AtomicInteger inflightRequests = inflightRequestsPerRoute.get(secondaryHistogramHost.toHostString());
    watch(secondaryHistogramBuffer, histogramBatchSize, inflightRequests, secondaryHistogramHost, "postSecondaryHistogram");
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
