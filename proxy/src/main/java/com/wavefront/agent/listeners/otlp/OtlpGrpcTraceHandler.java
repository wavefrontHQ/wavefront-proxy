package com.wavefront.agent.listeners.otlp;

import static com.wavefront.agent.listeners.FeatureCheckUtils.SPAN_DISABLED;
import static com.wavefront.agent.listeners.FeatureCheckUtils.isFeatureDisabled;
import static com.wavefront.internal.SpanDerivedMetricsUtils.reportHeartbeats;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.wavefront.agent.core.handlers.ReportableEntityHandler;
import com.wavefront.agent.core.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.core.queues.QueuesManager;
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
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
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
import wavefront.report.Span;
import wavefront.report.SpanLogs;

public class OtlpGrpcTraceHandler extends TraceServiceGrpc.TraceServiceImplBase
    implements Closeable, Runnable {
  protected static final Logger logger =
      Logger.getLogger(OtlpGrpcTraceHandler.class.getCanonicalName());
  private final ReportableEntityHandler<Span, String> spanHandler;
  private final ReportableEntityHandler<SpanLogs, String> spanLogsHandler;
  @Nullable private final WavefrontSender wfSender;
  @Nullable private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
  private final Pair<SpanSampler, Counter> spanSamplerAndCounter;
  private final Pair<Supplier<Boolean>, Counter> spansDisabled;
  private final Pair<Supplier<Boolean>, Counter> spanLogsDisabled;
  private final String defaultSource;
  @Nullable private final WavefrontInternalReporter internalReporter;
  private final Set<Pair<Map<String, String>, String>> discoveredHeartbeatMetrics;
  private final Set<String> traceDerivedCustomTagKeys;
  private final ScheduledExecutorService scheduledExecutorService;
  private final Counter receivedSpans;

  @VisibleForTesting
  public OtlpGrpcTraceHandler(
      int port,
      ReportableEntityHandler<Span, String> spanHandler,
      ReportableEntityHandler<SpanLogs, String> spanLogsHandler,
      @Nullable WavefrontSender wfSender,
      @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      SpanSampler sampler,
      Supplier<Boolean> spansFeatureDisabled,
      Supplier<Boolean> spanLogsFeatureDisabled,
      String defaultSource,
      Set<String> traceDerivedCustomTagKeys) {
    this.spanHandler = spanHandler;
    this.spanLogsHandler = spanLogsHandler;
    this.wfSender = wfSender;
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
        Executors.newScheduledThreadPool(1, new NamedThreadFactory("otlp-grpc-heart-beater"));
    scheduledExecutorService.scheduleAtFixedRate(this, 1, 1, TimeUnit.MINUTES);

    this.internalReporter = OtlpTraceUtils.createAndStartInternalReporter(wfSender);
  }

  public OtlpGrpcTraceHandler(
      int port,
      ReportableEntityHandlerFactory handlerFactory,
      @Nullable WavefrontSender wfSender,
      @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      SpanSampler sampler,
      Supplier<Boolean> spansFeatureDisabled,
      Supplier<Boolean> spanLogsFeatureDisabled,
      String defaultSource,
      Set<String> traceDerivedCustomTagKeys) {
    this(
        port,
        handlerFactory.getHandler(
            String.valueOf(port), QueuesManager.initQueue(ReportableEntityType.TRACE)),
        handlerFactory.getHandler(
            String.valueOf(port), QueuesManager.initQueue(ReportableEntityType.TRACE_SPAN_LOGS)),
        wfSender,
        preprocessorSupplier,
        sampler,
        spansFeatureDisabled,
        spanLogsFeatureDisabled,
        defaultSource,
        traceDerivedCustomTagKeys);
  }

  @Override
  public void export(
      ExportTraceServiceRequest request,
      StreamObserver<ExportTraceServiceResponse> responseObserver) {
    long spanCount = OtlpTraceUtils.getSpansCount(request);
    receivedSpans.inc(spanCount);

    if (isFeatureDisabled(spansDisabled._1, SPAN_DISABLED, spansDisabled._2, spanCount)) {
      Status grpcError = Status.FAILED_PRECONDITION.augmentDescription(SPAN_DISABLED);
      responseObserver.onError(grpcError.asException());
      return;
    }

    OtlpTraceUtils.exportToWavefront(
        request,
        spanHandler,
        spanLogsHandler,
        preprocessorSupplier,
        spanLogsDisabled,
        spanSamplerAndCounter,
        defaultSource,
        discoveredHeartbeatMetrics,
        internalReporter,
        traceDerivedCustomTagKeys);

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
