package com.wavefront.agent.listeners.otlp;

import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.Arrays;

import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.trace.v1.Span;
import wavefront.report.Annotation;

import static org.junit.Assert.assertEquals;

/**
 * @author Xiaochen Wang (xiaochenw@vmware.com).
 * @author Glenn Oppegard (goppegard@vmware.com).
 */
public class OtlpGrpcTraceHandlerTest {
  private final ReportableEntityHandler<wavefront.report.Span, String> mockSpanHandler =
      MockReportableEntityHandlerFactory.getMockTraceHandler();
  private final ReportableEntityHandler<wavefront.report.SpanLogs, String> mockSpanLogsHandler =
      MockReportableEntityHandlerFactory.getMockTraceSpanLogsHandler();

  @Test
  public void testMinimalSpanAndEvent() {
    // 1. Arrange
    EasyMock.reset(mockSpanHandler, mockSpanLogsHandler);
    Capture<wavefront.report.Span> actualSpan = Capture.newInstance();
    Capture<wavefront.report.SpanLogs> actualLogs = Capture.newInstance();
    mockSpanHandler.report(EasyMock.capture(actualSpan));
    mockSpanLogsHandler.report(EasyMock.capture(actualLogs));
    EasyMock.replay(mockSpanHandler, mockSpanLogsHandler);

    Span.Event otlpEvent = OtlpTestHelpers.otlpSpanEvent();
    Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().addEvents(otlpEvent).build();
    ExportTraceServiceRequest otlpRequest = OtlpTestHelpers.otlpTraceRequest(otlpSpan);

    // 2. Act
    OtlpGrpcTraceHandler otlpGrpcTraceHandler = new OtlpGrpcTraceHandler("9876", mockSpanHandler,
        mockSpanLogsHandler, null, null, null, "test-source");
    otlpGrpcTraceHandler.export(otlpRequest, emptyStreamObserver);

    // 3. Assert
    EasyMock.verify(mockSpanHandler, mockSpanLogsHandler);

    wavefront.report.Span expectedSpan =
        OtlpTestHelpers.wfSpanGenerator(Arrays.asList(new Annotation("_spanLogs", "true"))).build();
    wavefront.report.SpanLogs expectedLogs =
        OtlpTestHelpers.wfSpanLogsGenerator(expectedSpan).build();

    OtlpTestHelpers.assertWFSpanEquals(expectedSpan, actualSpan.getValue());
    assertEquals(expectedLogs, actualLogs.getValue());
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
