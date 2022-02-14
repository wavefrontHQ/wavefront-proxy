package com.wavefront.agent.listeners.otlp;

import com.google.common.collect.Sets;

import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import wavefront.report.Span;
import wavefront.report.SpanLogs;

import static com.wavefront.internal.SpanDerivedMetricsUtils.TRACING_DERIVED_PREFIX;
import static com.wavefront.internal.SpanDerivedMetricsUtils.reportHeartbeats;

public class OtlpGrpcTraceHandler extends TraceServiceGrpc.TraceServiceImplBase implements Closeable, Runnable {
  protected static final Logger logger =
      Logger.getLogger(OtlpGrpcTraceHandler.class.getCanonicalName());
  private final ReportableEntityHandler<Span, String> spanHandler;
  private final ReportableEntityHandler<SpanLogs, String> spanLogsHandler;
  @Nullable
  private final WavefrontSender wfSender;
  private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
  private final Pair<SpanSampler, Counter> spanSamplerAndCounter;
  private final String defaultSource;
  private final WavefrontInternalReporter internalReporter;
  private final Set<Pair<Map<String, String>, String>> discoveredHeartbeatMetrics;
  private final Set<String> traceDerivedCustomTagKeys;
  private final ScheduledExecutorService scheduledExecutorService;


  public OtlpGrpcTraceHandler(String handle,
                              ReportableEntityHandler<Span, String> spanHandler,
                              ReportableEntityHandler<SpanLogs, String> spanLogsHandler,
                              @Nullable WavefrontSender wfSender,
                              @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
                              SpanSampler sampler,
                              String defaultSource,
                              Set<String> traceDerivedCustomTagKeys) {
    this.spanHandler = spanHandler;
    this.spanLogsHandler = spanLogsHandler;
    this.wfSender = wfSender;
    this.preprocessorSupplier = preprocessorSupplier;
    this.spanSamplerAndCounter = Pair.of(sampler,
        Metrics.newCounter(new MetricName("spans." + handle, "", "sampler.discarded")));
    this.defaultSource = defaultSource;
    this.traceDerivedCustomTagKeys = traceDerivedCustomTagKeys;
    this.discoveredHeartbeatMetrics = Sets.newConcurrentHashSet();
    this.scheduledExecutorService =
        Executors.newScheduledThreadPool(1, new NamedThreadFactory("otlp-grpc-heart-beater"));
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

  public OtlpGrpcTraceHandler(String handle,
                              ReportableEntityHandlerFactory handlerFactory,
                              @Nullable WavefrontSender wfSender,
                              @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
                              SpanSampler sampler,
                              String defaultSource,
                              Set<String> traceDerivedCustomTagKeys) {
    this(handle, handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE, handle)),
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE_SPAN_LOGS, handle)),
        wfSender, preprocessorSupplier, sampler, defaultSource, traceDerivedCustomTagKeys);
  }

  @Override
  public void export(ExportTraceServiceRequest request,
                     StreamObserver<ExportTraceServiceResponse> responseObserver) {
    OtlpProtobufUtils.exportToWavefront(
        request, spanHandler, spanLogsHandler, preprocessorSupplier, spanSamplerAndCounter,
        defaultSource, discoveredHeartbeatMetrics, internalReporter, traceDerivedCustomTagKeys
    );
    responseObserver.onNext(ExportTraceServiceResponse.getDefaultInstance());
    responseObserver.onCompleted();
  }

  @Override
  public void run() {
    try {
      reportHeartbeats(wfSender, discoveredHeartbeatMetrics, "otlp");
    } catch (IOException e) {
      logger.warning("Cannot report heartbeat metric to wavefront");
    }
  }

  @Override
  public void close() {
    scheduledExecutorService.shutdownNow();
  }

}
