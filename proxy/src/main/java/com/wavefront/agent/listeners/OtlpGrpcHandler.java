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
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;

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
    for (ResourceSpans resourceSpans : request.getResourceSpansList()) {
      for (InstrumentationLibrarySpans instrumentationLibrarySpans :
          resourceSpans.getInstrumentationLibrarySpansList()) {
        for (Span otlpSpan : instrumentationLibrarySpans.getSpansList()) {
          wavefront.report.Span wfSpan = wavefront.report.Span.newBuilder().
              setName(otlpSpan.getName()).
              setSpanId(otlpSpan.getSpanId().toString()).
              setTraceId(otlpSpan.getTraceId().toString()).
              setStartMillis(otlpSpan.getStartTimeUnixNano() / 1000).
              setDuration((otlpSpan.getEndTimeUnixNano() - otlpSpan.getStartTimeUnixNano()) / 1000).
              setSource("open-telemetry").
              setCustomer("wf-proxy").build();
          logger.info("Transformed OTLP into WF span: " + wfSpan);
          spanHandler.report(wfSpan);
        }
      }
    }
    responseObserver.onNext(ExportTraceServiceResponse.getDefaultInstance());
    responseObserver.onCompleted();
  }

}
