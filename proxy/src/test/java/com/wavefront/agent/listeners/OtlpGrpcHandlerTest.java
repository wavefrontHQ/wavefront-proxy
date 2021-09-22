package com.wavefront.agent.listeners;

import com.google.protobuf.ByteString;

import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;

import org.easymock.EasyMock;
import org.junit.Test;

import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;

/**
 * @author Xiaochen Wang (xiaochenw@vmware.com).
 * @author Glenn Oppegard (goppegard@vmware.com).
 */
public class OtlpGrpcHandlerTest {
  private final ReportableEntityHandler<wavefront.report.Span, String> mockSpanHandler =
      MockReportableEntityHandlerFactory.getMockTraceHandler();
  private final long startTime = System.currentTimeMillis();

  @Test
  public void testMinimalSpan() {
    EasyMock.reset(mockSpanHandler);
    byte[] spanIdBytes = {0x9, 0x9, 0x9, 0x9, 0x9, 0x9, 0x9, 0x9};
    byte[] traceIdBytes = {0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1};

    Span otelSpan = Span.newBuilder()
        .setName("root")
        .setSpanId(ByteString.copyFrom(spanIdBytes))
        .setTraceId(ByteString.copyFrom(traceIdBytes))
        .setStartTimeUnixNano(startTime * 1000)
        .setEndTimeUnixNano(startTime * 1000 + 50000)
        .build();

    wavefront.report.Span wfSpan = wavefront.report.Span.newBuilder()
        .setName("root")
        .setSpanId("00000000-0000-0000-0909-090909090909")
        .setTraceId("01010101-0101-0101-0101-010101010101")
        .setStartMillis(startTime)
        .setDuration(50)
        .setSource("open-telemetry")
        .setCustomer("wf-proxy")
        .build();
    mockSpanHandler.report(EasyMock.eq(wfSpan));
    EasyMock.expectLastCall();

    EasyMock.replay(mockSpanHandler);

    OtlpGrpcHandler otlpGrpcHandler = new OtlpGrpcHandler("9876", mockSpanHandler);
    ResourceSpans resourceSpans = ResourceSpans.newBuilder().
        addInstrumentationLibrarySpans(
            InstrumentationLibrarySpans.
                newBuilder().
                addSpans(otelSpan).
                build()).
        build();
    ExportTraceServiceRequest request =
        ExportTraceServiceRequest.newBuilder().addResourceSpans(resourceSpans).build();
    otlpGrpcHandler.export(request, emptyStreamObserver);
    EasyMock.verify(mockSpanHandler);
  }

  private final StreamObserver<ExportTraceServiceResponse> emptyStreamObserver = new StreamObserver<ExportTraceServiceResponse>() {
    @Override
    public void onNext(ExportTraceServiceResponse postSpansResponse) {
    }

    @Override
    public void onError(Throwable throwable) {
    }

    @Override
    public void onCompleted() {
    }
  };
}
