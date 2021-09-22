package com.wavefront.agent.listeners;

import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.data.ReportableEntityType;

import java.util.logging.Logger;

import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;

public class OtlpGrpcHandler extends TraceServiceGrpc.TraceServiceImplBase {
  protected static final Logger logger = Logger.getLogger(OtlpGrpcHandler.class.getCanonicalName());
  private final ReportableEntityHandler<wavefront.report.Span, String> spanHandler;

  public OtlpGrpcHandler(String handle, ReportableEntityHandler<wavefront.report.Span, String> spanHandler) {
    this.spanHandler = spanHandler;
  }

  public OtlpGrpcHandler(String handle, ReportableEntityHandlerFactory handlerFactory) {
    this(handle, handlerFactory.getHandler(HandlerKey.of(ReportableEntityType.TRACE, handle)));
  }

  @Override
  public void export(ExportTraceServiceRequest request,
                     StreamObserver<ExportTraceServiceResponse> responseObserver) {
    logger.info("Received an OTLP Request: " + request);
    // TODO: we should write a function to convert OPTL span list to wf span list
    for (wavefront.report.Span wfspan: OtlpUtils.otlpSpanExportRequestParseToWFSpan(request)) {
      spanHandler.report(wfspan);
    }
    responseObserver.onNext(ExportTraceServiceResponse.getDefaultInstance());
    responseObserver.onCompleted();
  }

}
