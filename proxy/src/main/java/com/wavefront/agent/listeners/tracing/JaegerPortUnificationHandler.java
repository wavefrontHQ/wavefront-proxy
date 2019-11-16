package com.wavefront.agent.listeners.tracing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.wavefront.agent.auth.TokenAuthenticatorBuilder;
import com.wavefront.agent.channel.ChannelUtils;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.listeners.AbstractHttpOnlyHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.internal.reporter.WavefrontInternalReporter;
import com.wavefront.sdk.common.WavefrontSender;
import com.wavefront.sdk.entities.tracing.sampling.Sampler;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.wavefront.agent.channel.ChannelUtils.writeExceptionText;
import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;
import static com.wavefront.agent.listeners.tracing.JaegerThriftUtils.processBatch;
import static com.wavefront.agent.listeners.tracing.SpanDerivedMetricsUtils.TRACING_DERIVED_PREFIX;
import static com.wavefront.agent.listeners.tracing.SpanDerivedMetricsUtils.reportHeartbeats;

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

  private final String handle;
  private final ReportableEntityHandler<Span> spanHandler;
  private final ReportableEntityHandler<SpanLogs> spanLogsHandler;
  @Nullable
  private final WavefrontSender wfSender;
  @Nullable
  private final WavefrontInternalReporter wfInternalReporter;
  private final Supplier<Boolean> traceDisabled;
  private final Supplier<Boolean> spanLogsDisabled;
  private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
  private final Sampler sampler;
  private final boolean alwaysSampleErrors;
  private final String proxyLevelApplicationName;
  private final Set<String> traceDerivedCustomTagKeys;

  private final Counter discardedTraces;
  private final Counter discardedBatches;
  private final Counter processedBatches;
  private final Counter failedBatches;
  private final Counter discardedSpansBySampler;
  private final ConcurrentMap<HeartbeatMetricKey, Boolean> discoveredHeartbeatMetrics;
  private final ScheduledExecutorService scheduledExecutorService;

  private final static String JAEGER_VALID_PATH = "/api/traces/";
  private final static String JAEGER_VALID_HTTP_METHOD = "POST";

  @SuppressWarnings("unchecked")
  public JaegerPortUnificationHandler(String handle,
                                      final HealthCheckManager healthCheckManager,
                                      ReportableEntityHandlerFactory handlerFactory,
                                      @Nullable WavefrontSender wfSender,
                                      Supplier<Boolean> traceDisabled,
                                      Supplier<Boolean> spanLogsDisabled,
                                      @Nullable Supplier<ReportableEntityPreprocessor> preprocessor,
                                      Sampler sampler,
                                      boolean alwaysSampleErrors,
                                      @Nullable String traceJaegerApplicationName,
                                      Set<String> traceDerivedCustomTagKeys) {
    this(handle, healthCheckManager,
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE, handle)),
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE_SPAN_LOGS, handle)),
        wfSender, traceDisabled, spanLogsDisabled, preprocessor, sampler, alwaysSampleErrors,
        traceJaegerApplicationName, traceDerivedCustomTagKeys);
  }

  @VisibleForTesting
  JaegerPortUnificationHandler(String handle,
                               final HealthCheckManager healthCheckManager,
                               ReportableEntityHandler<Span> spanHandler,
                               ReportableEntityHandler<SpanLogs> spanLogsHandler,
                               @Nullable WavefrontSender wfSender,
                               Supplier<Boolean> traceDisabled,
                               Supplier<Boolean> spanLogsDisabled,
                               @Nullable Supplier<ReportableEntityPreprocessor> preprocessor,
                               Sampler sampler,
                               boolean alwaysSampleErrors,
                               @Nullable String traceJaegerApplicationName,
                               Set<String> traceDerivedCustomTagKeys) {
    super(TokenAuthenticatorBuilder.create().build(), healthCheckManager, handle);
    this.handle = handle;
    this.spanHandler = spanHandler;
    this.spanLogsHandler = spanLogsHandler;
    this.wfSender = wfSender;
    this.traceDisabled = traceDisabled;
    this.spanLogsDisabled = spanLogsDisabled;
    this.preprocessorSupplier = preprocessor;
    this.sampler = sampler;
    this.alwaysSampleErrors = alwaysSampleErrors;
    this.proxyLevelApplicationName = StringUtils.isBlank(traceJaegerApplicationName) ?
        "Jaeger" : traceJaegerApplicationName.trim();
    this.traceDerivedCustomTagKeys =  traceDerivedCustomTagKeys;
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
    this.discoveredHeartbeatMetrics =  new ConcurrentHashMap<>();
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
                                   final FullHttpRequest incomingRequest) {
    URI uri = ChannelUtils.parseUri(ctx, incomingRequest);
    if (uri == null) return;

    String path = uri.getPath().endsWith("/") ? uri.getPath() : uri.getPath() + "/";

    // Validate Uri Path and HTTP method of incoming Jaeger spans.
    if (!path.equals(JAEGER_VALID_PATH)) {
      writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST, "Unsupported URL path.", incomingRequest);
      logWarning("WF-400: Requested URI path '" + path + "' is not supported.", null, ctx);
      return;
    }
    if (!incomingRequest.method().toString().equalsIgnoreCase(JAEGER_VALID_HTTP_METHOD)) {
      writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST, "Unsupported Http method.", incomingRequest);
      logWarning("WF-400: Requested http method '" + incomingRequest.method().toString() +
          "' is not supported.", null, ctx);
      return;
    }

    HttpResponseStatus status;
    StringBuilder output = new StringBuilder();

    try {
      byte[] bytesArray = new byte[incomingRequest.content().nioBuffer().remaining()];
      incomingRequest.content().nioBuffer().get(bytesArray, 0, bytesArray.length);
      Batch batch = new Batch();
      new TDeserializer().deserialize(batch, bytesArray);

      processBatch(batch, output, DEFAULT_SOURCE, proxyLevelApplicationName, spanHandler,
          spanLogsHandler, wfInternalReporter, traceDisabled, spanLogsDisabled,
          preprocessorSupplier, sampler, alwaysSampleErrors, traceDerivedCustomTagKeys,
          discardedTraces, discardedBatches, discardedSpansBySampler, discoveredHeartbeatMetrics);
      status = HttpResponseStatus.ACCEPTED;
      processedBatches.inc();
    } catch (Exception e) {
      failedBatches.inc();
      writeExceptionText(e, output);
      status = HttpResponseStatus.BAD_REQUEST;
      logger.log(Level.WARNING, "Jaeger HTTP batch processing failed", Throwables.getRootCause(e));
    }
    writeHttpResponse(ctx, status, output, incomingRequest);
  }

  @Override
  public void run() {
    try {
      reportHeartbeats(JAEGER_COMPONENT, wfSender, discoveredHeartbeatMetrics);
    } catch (IOException e) {
      logger.log(Level.WARNING, "Cannot report heartbeat metric to wavefront");
    }
  }

  @Override
  public void close() throws IOException {
    scheduledExecutorService.shutdownNow();
  }
}
