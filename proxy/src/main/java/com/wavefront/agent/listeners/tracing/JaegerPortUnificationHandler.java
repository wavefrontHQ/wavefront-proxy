package com.wavefront.agent.listeners.tracing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.listeners.AbstractHttpOnlyHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.agent.sampler.SpanSampler;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.internal.reporter.WavefrontInternalReporter;
import com.wavefront.sdk.common.Pair;
import com.wavefront.sdk.common.WavefrontSender;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import io.jaegertracing.thriftjava.Batch;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TDeserializer;

import wavefront.report.Span;
import wavefront.report.SpanLogs;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.wavefront.agent.channel.ChannelUtils.errorMessageWithRootCause;
import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;
import static com.wavefront.agent.listeners.tracing.JaegerThriftUtils.processBatch;
import static com.wavefront.internal.SpanDerivedMetricsUtils.TRACING_DERIVED_PREFIX;
import static com.wavefront.internal.SpanDerivedMetricsUtils.reportHeartbeats;

/**
 * Handler that processes Jaeger Thrift trace data over HTTP and converts them to Wavefront format.
 *
 * @author Han Zhang (zhanghan@vmware.com)
 */
public class JaegerPortUnificationHandler extends AbstractHttpOnlyHandler implements Runnable,
    Closeable {
  protected static final Logger logger =
      Logger.getLogger(JaegerPortUnificationHandler.class.getCanonicalName());

  private final static String JAEGER_COMPONENT = "jaeger";
  private final static String DEFAULT_SOURCE = "jaeger";

  private final ReportableEntityHandler<Span, String> spanHandler;
  private final ReportableEntityHandler<SpanLogs, String> spanLogsHandler;
  @Nullable
  private final WavefrontSender wfSender;
  @Nullable
  private final WavefrontInternalReporter wfInternalReporter;
  private final Supplier<Boolean> traceDisabled;
  private final Supplier<Boolean> spanLogsDisabled;
  private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
  private final SpanSampler sampler;
  private final String proxyLevelApplicationName;
  private final Set<String> traceDerivedCustomTagKeys;

  private final Counter discardedTraces;
  private final Counter discardedBatches;
  private final Counter processedBatches;
  private final Counter failedBatches;
  private final Counter discardedSpansBySampler;
  private final Set<Pair<Map<String, String>, String>> discoveredHeartbeatMetrics;
  private final ScheduledExecutorService scheduledExecutorService;

  private final static String JAEGER_VALID_PATH = "/api/traces/";
  private final static String JAEGER_VALID_HTTP_METHOD = "POST";

  public JaegerPortUnificationHandler(String handle,
                                      final TokenAuthenticator tokenAuthenticator,
                                      final HealthCheckManager healthCheckManager,
                                      ReportableEntityHandlerFactory handlerFactory,
                                      @Nullable WavefrontSender wfSender,
                                      Supplier<Boolean> traceDisabled,
                                      Supplier<Boolean> spanLogsDisabled,
                                      @Nullable Supplier<ReportableEntityPreprocessor> preprocessor,
                                      SpanSampler sampler,
                                      @Nullable String traceJaegerApplicationName,
                                      Set<String> traceDerivedCustomTagKeys) {
    this(handle, tokenAuthenticator, healthCheckManager,
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE, handle)),
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE_SPAN_LOGS, handle)),
        wfSender, traceDisabled, spanLogsDisabled, preprocessor, sampler,
        traceJaegerApplicationName, traceDerivedCustomTagKeys);
  }

  @VisibleForTesting
  JaegerPortUnificationHandler(String handle,
                               final TokenAuthenticator tokenAuthenticator,
                               final HealthCheckManager healthCheckManager,
                               ReportableEntityHandler<Span, String> spanHandler,
                               ReportableEntityHandler<SpanLogs, String> spanLogsHandler,
                               @Nullable WavefrontSender wfSender,
                               Supplier<Boolean> traceDisabled,
                               Supplier<Boolean> spanLogsDisabled,
                               @Nullable Supplier<ReportableEntityPreprocessor> preprocessor,
                               SpanSampler sampler,
                               @Nullable String traceJaegerApplicationName,
                               Set<String> traceDerivedCustomTagKeys) {
    super(tokenAuthenticator, healthCheckManager, handle);
    this.spanHandler = spanHandler;
    this.spanLogsHandler = spanLogsHandler;
    this.wfSender = wfSender;
    this.traceDisabled = traceDisabled;
    this.spanLogsDisabled = spanLogsDisabled;
    this.preprocessorSupplier = preprocessor;
    this.sampler = sampler;
    this.proxyLevelApplicationName = StringUtils.isBlank(traceJaegerApplicationName) ?
        "Jaeger" : traceJaegerApplicationName.trim();
    this.traceDerivedCustomTagKeys = traceDerivedCustomTagKeys;
    this.discardedTraces = Metrics.newCounter(
        new MetricName("spans." + handle, "", "discarded"));
    this.discardedBatches = Metrics.newCounter(
        new MetricName("spans." + handle + ".batches", "", "discarded"));
    this.processedBatches = Metrics.newCounter(
        new MetricName("spans." + handle + ".batches", "", "processed"));
    this.failedBatches = Metrics.newCounter(
        new MetricName("spans." + handle + ".batches", "", "failed"));
    this.discardedSpansBySampler = Metrics.newCounter(
        new MetricName("spans." + handle, "", "sampler.discarded"));
    this.discoveredHeartbeatMetrics = Sets.newConcurrentHashSet();
    this.scheduledExecutorService = Executors.newScheduledThreadPool(1,
        new NamedThreadFactory("jaeger-heart-beater"));
    scheduledExecutorService.scheduleAtFixedRate(this, 1, 1, TimeUnit.MINUTES);

    if (wfSender != null) {
      wfInternalReporter = new WavefrontInternalReporter.Builder().
          prefixedWith(TRACING_DERIVED_PREFIX).withSource(DEFAULT_SOURCE).reportMinuteDistribution().
          build(wfSender);
      // Start the reporter
      wfInternalReporter.start(1, TimeUnit.MINUTES);
    } else {
      wfInternalReporter = null;
    }
  }

  @Override
  protected void handleHttpMessage(final ChannelHandlerContext ctx,
                                   final FullHttpRequest request) throws URISyntaxException {
    URI uri = new URI(request.uri());
    String path = uri.getPath().endsWith("/") ? uri.getPath() : uri.getPath() + "/";

    // Validate Uri Path and HTTP method of incoming Jaeger spans.
    if (!path.equals(JAEGER_VALID_PATH)) {
      writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST, "Unsupported URL path.", request);
      logWarning("Requested URI path '" + path + "' is not supported.", null, ctx);
      return;
    }
    if (!request.method().toString().equalsIgnoreCase(JAEGER_VALID_HTTP_METHOD)) {
      writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST, "Unsupported Http method.", request);
      logWarning("Requested http method '" + request.method().toString() +
          "' is not supported.", null, ctx);
      return;
    }

    HttpResponseStatus status;
    StringBuilder output = new StringBuilder();

    try {
      byte[] bytesArray = new byte[request.content().nioBuffer().remaining()];
      request.content().nioBuffer().get(bytesArray, 0, bytesArray.length);
      Batch batch = new Batch();
      new TDeserializer().deserialize(batch, bytesArray);

      processBatch(batch, output, DEFAULT_SOURCE, proxyLevelApplicationName, spanHandler,
          spanLogsHandler, wfInternalReporter, traceDisabled, spanLogsDisabled,
          preprocessorSupplier, sampler, traceDerivedCustomTagKeys,
          discardedTraces, discardedBatches, discardedSpansBySampler, discoveredHeartbeatMetrics);
      status = HttpResponseStatus.ACCEPTED;
      processedBatches.inc();
    } catch (Exception e) {
      failedBatches.inc();
      output.append(errorMessageWithRootCause(e));
      status = HttpResponseStatus.BAD_REQUEST;
      logger.log(Level.WARNING, "Jaeger HTTP batch processing failed", Throwables.getRootCause(e));
    }
    writeHttpResponse(ctx, status, output, request);
  }

  @Override
  public void run() {
    try {
      reportHeartbeats(wfSender, discoveredHeartbeatMetrics, JAEGER_COMPONENT);
    } catch (IOException e) {
      logger.log(Level.WARNING, "Cannot report heartbeat metric to wavefront");
    }
  }

  @Override
  public void close() {
    scheduledExecutorService.shutdownNow();
  }
}
