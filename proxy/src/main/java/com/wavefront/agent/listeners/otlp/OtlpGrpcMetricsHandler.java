package com.wavefront.agent.listeners.otlp;

import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.data.ReportableEntityType;

import javax.annotation.Nullable;
import java.util.function.Supplier;

import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import wavefront.report.ReportPoint;

public class OtlpGrpcMetricsHandler extends MetricsServiceGrpc.MetricsServiceImplBase {

    private final ReportableEntityHandler<ReportPoint, String> handler;
    private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
    private final String defaultSource;

    /**
     * Create new instance.
     * @param handler
     * @param preprocessorSupplier
     */
    public OtlpGrpcMetricsHandler(ReportableEntityHandler<ReportPoint, String> handler,
                                  Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
                                  String defaultSource) {
        super();
        this.handler = handler;
        this.preprocessorSupplier = preprocessorSupplier;
        this.defaultSource = defaultSource;
    }
    public OtlpGrpcMetricsHandler(String handle,
                                  ReportableEntityHandlerFactory handlerFactory,
                                  @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier, String defaultSource) {
        this(handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.POINT, handle)),
            preprocessorSupplier, defaultSource);
    }

    public void export(ExportMetricsServiceRequest request, StreamObserver<ExportMetricsServiceResponse> responseObserver) {
        OtlpProtobufPointUtils.exportToWavefront(request, handler, preprocessorSupplier, defaultSource);
        responseObserver.onNext(ExportMetricsServiceResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

}
