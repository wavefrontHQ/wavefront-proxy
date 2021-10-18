package com.wavefront.agent.listeners.otlp;

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
public class OtlpGrpcTraceHandlerTest {
  private final ReportableEntityHandler<wavefront.report.Span, String> mockSpanHandler =
      MockReportableEntityHandlerFactory.getMockTraceHandler();

  @Test
  public void testMinimalSpan() {
    EasyMock.reset(mockSpanHandler);


    Span otelSpan = OtlpTestHelpers.otlpSpanGenerator().build();
    wavefront.report.Span wfSpan = OtlpTestHelpers.wfSpanGenerator(null).build();

    mockSpanHandler.report(wfSpan);
    EasyMock.expectLastCall();

    EasyMock.replay(mockSpanHandler);

    OtlpGrpcTraceHandler otlpGrpcTraceHandler = new OtlpGrpcTraceHandler("9876", mockSpanHandler,
        null, null, null);
    ResourceSpans resourceSpans = ResourceSpans.newBuilder().
        addInstrumentationLibrarySpans(
            InstrumentationLibrarySpans.
                newBuilder().
                addSpans(otelSpan).
                build()).
        build();
    ExportTraceServiceRequest request =
        ExportTraceServiceRequest.newBuilder().addResourceSpans(resourceSpans).build();
    otlpGrpcTraceHandler.export(request, emptyStreamObserver);
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
