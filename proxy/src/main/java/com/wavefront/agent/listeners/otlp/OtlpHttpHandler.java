package com.wavefront.agent.listeners.otlp;

import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

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

import java.io.Closeable;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import wavefront.report.Span;
import wavefront.report.SpanLogs;

import static com.wavefront.agent.channel.ChannelUtils.errorMessageWithRootCause;
import static com.wavefront.agent.channel.ChannelUtils.writeHttpResponse;
import static com.wavefront.internal.SpanDerivedMetricsUtils.TRACING_DERIVED_PREFIX;
import static com.wavefront.internal.SpanDerivedMetricsUtils.reportHeartbeats;

public class OtlpHttpHandler extends AbstractHttpOnlyHandler implements Closeable, Runnable {
  private final static Logger logger = Logger.getLogger(OtlpHttpHandler.class.getCanonicalName());
  private final String defaultSource;
  private final Set<Pair<Map<String, String>, String>> discoveredHeartbeatMetrics;
  private final WavefrontInternalReporter internalReporter;
  private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
  private final Pair<SpanSampler, Counter> spanSamplerAndCounter;
  private final ScheduledExecutorService scheduledExecutorService;
  private final ReportableEntityHandler<Span, String> spanHandler;
  @Nullable
  private final WavefrontSender sender;
  private final ReportableEntityHandler<SpanLogs, String> spanLogsHandler;
  private final Set<String> traceDerivedCustomTagKeys;

  public OtlpHttpHandler(ReportableEntityHandlerFactory handlerFactory,
                         @Nullable TokenAuthenticator tokenAuthenticator,
                         @Nullable HealthCheckManager healthCheckManager,
                         @Nullable String handle,
                         @Nullable WavefrontSender wfSender,
                         @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
                         SpanSampler sampler,
                         String defaultSource,
                         Set<String> traceDerivedCustomTagKeys) {
    super(tokenAuthenticator, healthCheckManager, handle);
    this.spanHandler = handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE, handle));
    this.spanLogsHandler =
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE_SPAN_LOGS, handle));
    this.sender = wfSender;
    this.preprocessorSupplier = preprocessorSupplier;
    this.spanSamplerAndCounter = Pair.of(sampler,
        Metrics.newCounter(new MetricName("spans." + handle, "", "sampler.discarded")));
    this.defaultSource = defaultSource;
    this.traceDerivedCustomTagKeys = traceDerivedCustomTagKeys;
    this.discoveredHeartbeatMetrics = Sets.newConcurrentHashSet();

    this.scheduledExecutorService =
        Executors.newScheduledThreadPool(1, new NamedThreadFactory("otlp-http-heart-beater"));
    scheduledExecutorService.scheduleAtFixedRate(this, 1, 1, TimeUnit.MINUTES);

    if (wfSender != null) {
      internalReporter = new WavefrontInternalReporter.Builder().
          prefixedWith(TRACING_DERIVED_PREFIX).withSource(defaultSource).reportMinuteDistribution().
          build(wfSender);
      internalReporter.start(1, TimeUnit.MINUTES);
    } else {
      internalReporter = null;
    }

  }

  @Override
  protected void handleHttpMessage(ChannelHandlerContext ctx, FullHttpRequest request) throws URISyntaxException {
//  TODO:  if request.path == "/v1/traces"
//        else if request.p[ath == "/v1/metrics"
//        eslse blow up
    try {
      ExportTraceServiceRequest otlpRequest =
          ExportTraceServiceRequest.parseFrom(request.content().nioBuffer());
      OtlpProtobufUtils.exportToWavefront(
          otlpRequest, spanHandler, spanLogsHandler, preprocessorSupplier, spanSamplerAndCounter,
          defaultSource, discoveredHeartbeatMetrics, internalReporter, traceDerivedCustomTagKeys
      );
      /*
      We use HTTP 200 for success and HTTP 400 for errors, mirroring what we found in
      OTel Collector's OTLP Receiver code.
     */
      writeHttpResponse(ctx, HttpResponseStatus.OK, "", request);
    } catch (InvalidProtocolBufferException e) {
      logWarning("WF-300: Failed to handle incoming OTLP request", e, ctx);
      writeHttpResponse(ctx, HttpResponseStatus.BAD_REQUEST, errorMessageWithRootCause(e), request);
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
}
