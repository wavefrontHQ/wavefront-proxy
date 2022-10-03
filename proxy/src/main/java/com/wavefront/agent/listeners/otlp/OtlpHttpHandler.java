package com.wavefront.agent.listeners.otlp;

import static com.wavefront.agent.ProxyContext.queuesManager;
import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;
import static com.wavefront.agent.listeners.FeatureCheckUtils.SPAN_DISABLED;
import static com.wavefront.agent.listeners.FeatureCheckUtils.isFeatureDisabled;
import static com.wavefront.internal.SpanDerivedMetricsUtils.reportHeartbeats;

import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.Status;
import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.channel.HealthCheckManager;
import com.wavefront.agent.core.handlers.ReportableEntityHandler;
import com.wavefront.agent.core.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.listeners.AbstractHttpOnlyHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.agent.sampler.SpanSampler;
import com.wavefront.common.NamedThreadFactory;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.internal.reporter.WavefrontInternalReporter;
import com.wavefront.sdk.common.Pair;
import com.wavefront.sdk.common.WavefrontSender;
import com.wavefront.sdk.common.annotation.NonNull;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
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
import java.util.logging.Logger;
import javax.annotation.Nullable;
import wavefront.report.ReportPoint;
import wavefront.report.Span;
import wavefront.report.SpanLogs;

public class OtlpHttpHandler extends AbstractHttpOnlyHandler implements Closeable, Runnable {
  private static final Logger logger = Logger.getLogger(OtlpHttpHandler.class.getCanonicalName());
  private final String defaultSource;
  private final Set<Pair<Map<String, String>, String>> discoveredHeartbeatMetrics;
  @Nullable private final WavefrontInternalReporter internalReporter;
  @Nullable private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
  private final Pair<SpanSampler, Counter> spanSamplerAndCounter;
  private final ScheduledExecutorService scheduledExecutorService;
  private final ReportableEntityHandler<Span> spanHandler;
  @Nullable private final WavefrontSender sender;
  private final ReportableEntityHandler<SpanLogs> spanLogsHandler;
  private final Set<String> traceDerivedCustomTagKeys;
  private final ReportableEntityHandler<ReportPoint> metricsHandler;
  private final ReportableEntityHandler<ReportPoint> histogramHandler;
  private final Counter receivedSpans;
  private final Pair<Supplier<Boolean>, Counter> spansDisabled;
  private final Pair<Supplier<Boolean>, Counter> spanLogsDisabled;
  private final boolean includeResourceAttrsForMetrics;

  public OtlpHttpHandler(
      ReportableEntityHandlerFactory handlerFactory,
      @Nullable TokenAuthenticator tokenAuthenticator,
      @Nullable HealthCheckManager healthCheckManager,
      int port,
      @Nullable WavefrontSender wfSender,
      @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      SpanSampler sampler,
      Supplier<Boolean> spansFeatureDisabled,
      Supplier<Boolean> spanLogsFeatureDisabled,
      String defaultSource,
      Set<String> traceDerivedCustomTagKeys,
      boolean includeResourceAttrsForMetrics) {
    super(tokenAuthenticator, healthCheckManager, port);
    this.includeResourceAttrsForMetrics = includeResourceAttrsForMetrics;
    this.spanHandler =
        handlerFactory.getHandler(port, queuesManager.initQueue(ReportableEntityType.TRACE));
    this.spanLogsHandler =
        handlerFactory.getHandler(
            port, queuesManager.initQueue(ReportableEntityType.TRACE_SPAN_LOGS));
    this.metricsHandler =
        handlerFactory.getHandler(port, queuesManager.initQueue(ReportableEntityType.POINT));
    this.histogramHandler =
        handlerFactory.getHandler(port, queuesManager.initQueue(ReportableEntityType.HISTOGRAM));
    this.sender = wfSender;
    this.preprocessorSupplier = preprocessorSupplier;
    this.defaultSource = defaultSource;
    this.traceDerivedCustomTagKeys = traceDerivedCustomTagKeys;

    this.discoveredHeartbeatMetrics = Sets.newConcurrentHashSet();
    this.receivedSpans = Metrics.newCounter(new MetricName("spans." + port, "", "received.total"));
    this.spanSamplerAndCounter =
        Pair.of(
            sampler, Metrics.newCounter(new MetricName("spans." + port, "", "sampler.discarded")));
    this.spansDisabled =
        Pair.of(
            spansFeatureDisabled,
            Metrics.newCounter(new MetricName("spans." + port, "", "discarded")));
    this.spanLogsDisabled =
        Pair.of(
            spanLogsFeatureDisabled,
            Metrics.newCounter(new MetricName("spanLogs." + port, "", "discarded")));

    this.scheduledExecutorService =
        Executors.newScheduledThreadPool(1, new NamedThreadFactory("otlp-http-heart-beater"));
    scheduledExecutorService.scheduleAtFixedRate(this, 1, 1, TimeUnit.MINUTES);

    this.internalReporter = OtlpTraceUtils.createAndStartInternalReporter(sender);
  }

  @Override
  protected void handleHttpMessage(ChannelHandlerContext ctx, FullHttpRequest request)
      throws URISyntaxException {
    URI uri = new URI(request.uri());
    String path = uri.getPath().endsWith("/") ? uri.getPath() : uri.getPath() + "/";
    try {
      switch (path) {
        case "/v1/traces/":
          ExportTraceServiceRequest traceRequest =
              ExportTraceServiceRequest.parseFrom(request.content().nioBuffer());
          long spanCount = OtlpTraceUtils.getSpansCount(traceRequest);
          receivedSpans.inc(spanCount);

          if (isFeatureDisabled(spansDisabled._1, SPAN_DISABLED, spansDisabled._2, spanCount)) {
            HttpResponse response = makeErrorResponse(Code.FAILED_PRECONDITION, SPAN_DISABLED);
            writeHttpResponse(ctx, response, request);
            return;
          }

          OtlpTraceUtils.exportToWavefront(
              traceRequest,
              spanHandler,
              spanLogsHandler,
              preprocessorSupplier,
              spanLogsDisabled,
              spanSamplerAndCounter,
              defaultSource,
              discoveredHeartbeatMetrics,
              internalReporter,
              traceDerivedCustomTagKeys);
          break;
        case "/v1/metrics/":
          ExportMetricsServiceRequest metricRequest =
              ExportMetricsServiceRequest.parseFrom(request.content().nioBuffer());
          OtlpMetricsUtils.exportToWavefront(
              metricRequest,
              metricsHandler,
              histogramHandler,
              preprocessorSupplier,
              defaultSource,
              includeResourceAttrsForMetrics);
          break;
        default:
          /*
           We use HTTP 200 for success and HTTP 400 for errors, mirroring what we found in
           OTel Collector's OTLP Receiver code.
          */
          writeHttpResponse(
              ctx, HttpResponseStatus.BAD_REQUEST, "unknown endpoint " + path, request);
          return;
      }

      writeHttpResponse(ctx, HttpResponseStatus.OK, "", request);
    } catch (InvalidProtocolBufferException e) {
      logWarning("WF-300: Failed to handle incoming OTLP request", e, ctx);
      HttpResponse response = makeErrorResponse(Code.INVALID_ARGUMENT, e.getMessage());
      writeHttpResponse(ctx, response, request);
    }
  }

  @Override
  public void run() {
    try {
      reportHeartbeats(sender, discoveredHeartbeatMetrics, "otlp");
    } catch (IOException e) {
      logger.warning("Cannot report heartbeat metric to wavefront");
    }
  }

  @Override
  public void close() throws IOException {
    scheduledExecutorService.shutdownNow();
  }

  /*
  Build an OTLP HTTP error response per the spec:
  https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/otlp.md#otlphttp-response
   */
  private HttpResponse makeErrorResponse(Code rpcCode, String msg) {
    Status pbStatus = Status.newBuilder().setCode(rpcCode.getNumber()).setMessage(msg).build();
    ByteBuf content = Unpooled.copiedBuffer(pbStatus.toByteArray());

    HttpHeaders headers =
        new DefaultHttpHeaders()
            .set(HttpHeaderNames.CONTENT_TYPE, "application/x-protobuf")
            .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());

    HttpResponseStatus httpStatus =
        (rpcCode == Code.NOT_FOUND) ? HttpResponseStatus.NOT_FOUND : HttpResponseStatus.BAD_REQUEST;

    return new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1, httpStatus, content, headers, new DefaultHttpHeaders());
  }
}
