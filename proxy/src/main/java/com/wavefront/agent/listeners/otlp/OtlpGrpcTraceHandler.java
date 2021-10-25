package com.wavefront.agent.listeners.otlp;

import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.agent.sampler.SpanSampler;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.sdk.common.WavefrontSender;

import java.util.function.Supplier;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;

public class OtlpGrpcTraceHandler extends TraceServiceGrpc.TraceServiceImplBase {
  protected static final Logger logger = Logger.getLogger(OtlpGrpcTraceHandler.class.getCanonicalName());
  private final ReportableEntityHandler<wavefront.report.Span, String> spanHandler;
  @Nullable
  private final WavefrontSender wfSender;
  private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
  private final SpanSampler sampler;


  public OtlpGrpcTraceHandler(String handle,
                              ReportableEntityHandler<wavefront.report.Span, String> spanHandler,
                              @Nullable WavefrontSender wfSender,
                              @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
                              SpanSampler sampler) {
    this.spanHandler = spanHandler;
    this.wfSender = wfSender;
    this.preprocessorSupplier = preprocessorSupplier;
    this.sampler = sampler;
  }

  public OtlpGrpcTraceHandler(String handle,
                              ReportableEntityHandlerFactory handlerFactory,
                              @Nullable WavefrontSender wfSender,
                              @Nullable Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
                              SpanSampler sampler) {
    this(handle,
        handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE, handle)),
        wfSender, preprocessorSupplier, sampler);
  }

  @Override
  public void export(ExportTraceServiceRequest request,
                     StreamObserver<ExportTraceServiceResponse> responseObserver) {
    OtlpProtobufUtils.exportToWavefront(request, spanHandler, preprocessorSupplier);
    responseObserver.onNext(ExportTraceServiceResponse.getDefaultInstance());
    responseObserver.onCompleted();
  }
}
