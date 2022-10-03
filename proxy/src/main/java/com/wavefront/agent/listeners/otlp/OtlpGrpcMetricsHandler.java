package com.wavefront.agent.listeners.otlp;

import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.data.ReportableEntityType;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import wavefront.report.ReportPoint;

public class OtlpGrpcMetricsHandler extends MetricsServiceGrpc.MetricsServiceImplBase {

  private final ReportableEntityHandler<ReportPoint, String> pointHandler;
  private final ReportableEntityHandler<ReportPoint, String> histogramHandler;
  private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
  private final String defaultSource;
  private final boolean includeResourceAttrsForMetrics;
  private final boolean includeOtlpAppTagsOnMetrics;

  /**
   * Create new instance.
   *
   * @param pointHandler
   * @param histogramHandler
   * @param preprocessorSupplier
   * @param defaultSource
   * @param includeResourceAttrsForMetrics
   */
  public OtlpGrpcMetricsHandler(
      ReportableEntityHandler<ReportPoint, String> pointHandler,
      ReportableEntityHandler<ReportPoint, String> histogramHandler,
      Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      String defaultSource,
      boolean includeResourceAttrsForMetrics,
      boolean includeOtlpAppTagsOnMetrics) {
    super();
    this.pointHandler = pointHandler;
    this.histogramHandler = histogramHandler;
    this.preprocessorSupplier = preprocessorSupplier;
    this.defaultSource = defaultSource;
    this.includeResourceAttrsForMetrics = includeResourceAttrsForMetrics;
    this.includeOtlpAppTagsOnMetrics = includeOtlpAppTagsOnMetrics;
  }

  public OtlpGrpcMetricsHandler(
      String handle,
      ReportableEntityHandlerFactory handlerFactory,
      @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      String defaultSource,
      boolean includeResourceAttrsForMetrics,
      boolean includeOtlpAppTagsOnMetrics) {
    this(
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.POINT, handle)),
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.HISTOGRAM, handle)),
        preprocessorSupplier,
        defaultSource,
        includeResourceAttrsForMetrics,
        includeOtlpAppTagsOnMetrics);
  }

  public void export(
      ExportMetricsServiceRequest request,
      StreamObserver<ExportMetricsServiceResponse> responseObserver) {
    OtlpMetricsUtils.exportToWavefront(
        request,
        pointHandler,
        histogramHandler,
        preprocessorSupplier,
        defaultSource,
        includeResourceAttrsForMetrics,
        includeOtlpAppTagsOnMetrics);
    responseObserver.onNext(ExportMetricsServiceResponse.getDefaultInstance());
    responseObserver.onCompleted();
  }
}
