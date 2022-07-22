package com.wavefront.agent.listeners.otlp;

import static com.wavefront.agent.ProxyContext.queuesManager;

import com.wavefront.agent.core.handlers.ReportableEntityHandler;
import com.wavefront.agent.core.handlers.ReportableEntityHandlerFactory;
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

  private final ReportableEntityHandler<ReportPoint> pointHandler;
  private final ReportableEntityHandler<ReportPoint> histogramHandler;
  private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
  private final String defaultSource;
  private final boolean includeResourceAttrsForMetrics;

  /** Create new instance. */
  public OtlpGrpcMetricsHandler(
      ReportableEntityHandler<ReportPoint> pointHandler,
      ReportableEntityHandler<ReportPoint> histogramHandler,
      Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      String defaultSource,
      boolean includeResourceAttrsForMetrics) {
    super();
    this.pointHandler = pointHandler;
    this.histogramHandler = histogramHandler;
    this.preprocessorSupplier = preprocessorSupplier;
    this.defaultSource = defaultSource;
    this.includeResourceAttrsForMetrics = includeResourceAttrsForMetrics;
  }

  public OtlpGrpcMetricsHandler(
      int port,
      ReportableEntityHandlerFactory handlerFactory,
      @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      String defaultSource,
      boolean includeResourceAttrsForMetrics) {
    this(
        handlerFactory.getHandler(
            String.valueOf(port), queuesManager.initQueue(ReportableEntityType.POINT)),
        handlerFactory.getHandler(
            String.valueOf(port), queuesManager.initQueue(ReportableEntityType.HISTOGRAM)),
        preprocessorSupplier,
        defaultSource,
        includeResourceAttrsForMetrics);
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
        includeResourceAttrsForMetrics);
    responseObserver.onNext(ExportMetricsServiceResponse.getDefaultInstance());
    responseObserver.onCompleted();
  }
}
